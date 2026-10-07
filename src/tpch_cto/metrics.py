"""Metric definitions. All queries use these functions (or gold tables built with them).

- revenue = extended_price * (1 - discount), no tax, by order date
- active customer as of D = has an order in (D - 12 months, D]
- periods are compared only if both are complete (fully inside the data range)
"""

import calendar
from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

GRAINS = ("week", "month", "quarter", "year")
ACTIVE_WINDOW_MONTHS = 12
REVENUE_DROP_ALERT_PCT = -20.0
MONEY = "decimal(20,4)"


def net_revenue(extended_price: Column, discount: Column) -> Column:
    return (extended_price * (F.lit(1) - discount)).cast(MONEY)


def discount_amount(extended_price: Column, discount: Column) -> Column:
    return (extended_price * discount).cast(MONEY)


def net_revenue_with_tax(extended_price: Column, discount: Column, tax: Column) -> Column:
    return (extended_price * (F.lit(1) - discount) * (F.lit(1) + tax)).cast(MONEY)


# periods

def shift_period(start: Column, grain: str, n: int) -> Column:
    if grain == "week":
        return F.date_add(start, 7 * n)
    return F.add_months(start, {"month": 1, "quarter": 3, "year": 12}[grain] * n)


def period_label(start: Column, grain: str) -> Column:
    if grain == "week":
        return F.date_format(start, "yyyy-MM-dd")
    if grain == "month":
        return F.date_format(start, "yyyy-MM")
    if grain == "quarter":
        return F.concat(F.year(start).cast("string"), F.lit("-Q"), F.quarter(start).cast("string"))
    return F.year(start).cast("string")


def data_bounds(df: DataFrame, date_col: str = "order_date") -> tuple[date, date]:
    row = df.agg(F.min(date_col).alias("lo"), F.max(date_col).alias("hi")).first()
    return row["lo"], row["hi"]


def revenue_by_period(orders, grain, date_col="order_date", value_col="net_revenue", bounds=None):
    lo, hi = bounds or data_bounds(orders, date_col)
    start = F.col("period_start")
    return (
        orders.groupBy(F.trunc(F.col(date_col), grain).alias("period_start"))  # weeks start Monday
        .agg(F.count(F.lit(1)).alias("order_count"), F.sum(value_col).alias("revenue"))
        .withColumn("period_end", F.date_sub(shift_period(start, grain, 1), 1))
        .withColumn("period_label", period_label(start, grain))
        .withColumn("is_complete", (start >= F.lit(lo)) & (F.col("period_end") <= F.lit(hi)))
        .select("period_start", "period_end", "period_label", "is_complete", "order_count",
                "revenue")
    )


def period_over_period(periods, grain, periods_back=1, suffix=""):
    """Adds prev_revenue and change_pct; NULL if either period is partial."""
    prev_col, change_col = f"prev_revenue{suffix}", f"change_pct{suffix}"
    previous = periods.filter("is_complete").select(
        F.col("period_start").alias("_prev_start"), F.col("revenue").alias(prev_col)
    )
    return (
        periods.withColumn("_prev_start", shift_period(F.col("period_start"), grain, -periods_back))
        .join(previous, "_prev_start", "left")
        .withColumn(prev_col, F.when(F.col("is_complete"), F.col(prev_col)))
        .withColumn(
            change_col,
            F.round(100 * (F.col("revenue") - F.col(prev_col)) / F.col(prev_col), 2).cast("double"),
        )
        .drop("_prev_start")
    )


def compare_periods(periods: DataFrame, current_label: str, previous_label: str) -> dict:
    rows = {
        r["period_label"]: r
        for r in periods.filter(F.col("period_label").isin(current_label, previous_label)).collect()
    }
    for label in (current_label, previous_label):
        if label not in rows:
            raise ValueError(f"no data for period {label}")
        if not rows[label]["is_complete"]:
            raise ValueError(f"{label} is a partial period, can't compare it")
    cur, prev = rows[current_label]["revenue"], rows[previous_label]["revenue"]
    return {
        "current_period": current_label,
        "current_revenue": cur,
        "previous_period": previous_label,
        "previous_revenue": prev,
        "change": cur - prev,
        "change_pct": round(float(100 * (cur - prev) / prev), 2),
    }


def revenue_drop_alerts(periods, threshold_pct=REVENUE_DROP_ALERT_PCT, change_col="change_pct"):
    return periods.filter(F.col(change_col) <= F.lit(threshold_pct))


# active customers

def add_months(d: date, n: int) -> date:
    # same as spark add_months
    y, m = divmod(d.month - 1 + n, 12)
    year, month = d.year + y, m + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def active_customers_as_of(orders, as_of, months=ACTIVE_WINDOW_MONTHS, date_col="order_date",
                           customer_col="customer_id", bounds=None) -> dict:
    as_of = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of
    lo, hi = bounds or data_bounds(orders, date_col)
    after = add_months(as_of, -months)
    count = (
        orders.filter((F.col(date_col) > F.lit(after)) & (F.col(date_col) <= F.lit(as_of)))
        .select(customer_col).distinct().count()
    )
    return {
        "as_of": as_of,
        "window_first_day": after + timedelta(days=1),
        "window_last_day": as_of,
        "active_customers": count,
        "is_complete_window": after >= lo - timedelta(days=1) and as_of <= hi,
    }


def active_customers_monthly(orders, months=ACTIVE_WINDOW_MONTHS, date_col="order_date",
                             customer_col="customer_id", bounds=None):
    """Active customers at each month end. An order in month m counts for months m..m+11."""
    lo, hi = bounds or data_bounds(orders, date_col)
    customer_months = orders.select(
        F.col(customer_col).alias("customer_id"), F.trunc(F.col(date_col), "month").alias("order_month")
    ).distinct()
    return (
        customer_months.withColumn("_k", F.explode(F.sequence(F.lit(0), F.lit(months - 1))))
        .withColumn("snapshot_month", F.add_months(F.col("order_month"), F.col("_k")))
        .filter(F.col("snapshot_month") <= F.lit(hi.replace(day=1)))
        .groupBy("snapshot_month")
        .agg(F.countDistinct("customer_id").alias("active_customers"))
        .withColumn("as_of", F.last_day("snapshot_month"))
        .withColumn(
            "is_complete",
            (F.add_months(F.col("as_of"), -months) >= F.lit(lo - timedelta(days=1)))
            & (F.col("as_of") <= F.lit(hi)),
        )
        .select("snapshot_month", "as_of", "is_complete", "active_customers")
    )


# contribution

def contribution_by_period(sales, key_col, grain="year", date_col="order_date",
                           value_col="net_revenue"):
    per_key = sales.groupBy(
        F.trunc(F.col(date_col), grain).alias("period_start"), key_col
    ).agg(F.sum(value_col).alias("revenue"))
    w = Window.partitionBy("period_start")
    return (
        per_key.withColumn(
            "share_pct", F.round(100 * F.col("revenue") / F.sum("revenue").over(w), 4).cast("double")
        )
        .withColumn("rank", F.row_number().over(w.orderBy(F.desc("revenue"), key_col)))
        .withColumn("period_label", period_label(F.col("period_start"), grain))
    )


def top_n_share(sales, key_col, n=10, grain="month", date_col="order_date",
                value_col="net_revenue"):
    ranked = contribution_by_period(sales, key_col, grain, date_col, value_col)
    return (
        ranked.groupBy("period_start")
        .agg(
            F.sum(F.when(F.col("rank") <= n, F.col("revenue"))).alias(f"top{n}_revenue"),
            F.sum("revenue").alias("total_revenue"),
        )
        .withColumn(
            f"top{n}_share_pct",
            F.round(100 * F.col(f"top{n}_revenue") / F.col("total_revenue"), 4).cast("double"),
        )
    )
