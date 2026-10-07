from datetime import date, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from tpch_cto import metrics


def orders_df(spark, rows):
    return spark.createDataFrame(
        [(i, c, date.fromisoformat(d), Decimal(str(r))) for i, c, d, r in rows],
        "order_id long, customer_id long, order_date date, net_revenue decimal(20,4)",
    )


def daily(spark, first, last, per_day=100):
    d0, d1 = date.fromisoformat(first), date.fromisoformat(last)
    rows = [(i, 1, (d0 + timedelta(days=i)).isoformat(), per_day) for i in range((d1 - d0).days + 1)]
    return orders_df(spark, rows)


def test_net_revenue_excludes_tax(spark):
    df = spark.createDataFrame([(Decimal("100.00"), Decimal("0.10"), Decimal("0.05"))],
                               "p decimal(18,2), d decimal(18,2), t decimal(18,2)")
    row = df.select(
        metrics.net_revenue(F.col("p"), F.col("d")).alias("net"),
        metrics.net_revenue_with_tax(F.col("p"), F.col("d"), F.col("t")).alias("taxed"),
    ).first()
    assert row["net"] == Decimal("90.0000")
    assert row["taxed"] == Decimal("94.5000")


def test_weeks_start_monday_and_partial_weeks_are_flagged(spark):
    # 1992-01-01 is a Wednesday, 1992-01-19 a Sunday
    weeks = metrics.revenue_by_period(daily(spark, "1992-01-01", "1992-01-19"), "week")
    got = {r["period_label"]: (r["is_complete"], r["order_count"]) for r in weeks.collect()}
    assert got == {
        "1991-12-30": (False, 5),
        "1992-01-06": (True, 7),
        "1992-01-13": (True, 7),
    }


def test_period_over_period_never_compares_a_partial_period(spark):
    weeks = metrics.period_over_period(
        metrics.revenue_by_period(daily(spark, "1992-01-01", "1992-01-22"), "week"), "week"
    )
    got = {r["period_label"]: r["change_pct"] for r in weeks.collect()}
    assert got["1991-12-30"] is None  # partial itself
    assert got["1992-01-06"] is None  # previous week is partial
    assert got["1992-01-13"] == 0.0  # full vs full
    assert got["1992-01-20"] is None  # partial (data ends Wed)


def test_compare_periods_refuses_partial(spark):
    quarters = metrics.revenue_by_period(daily(spark, "1995-10-01", "1996-05-15"), "quarter")
    result = metrics.compare_periods(quarters, "1996-Q1", "1995-Q4")
    assert result["current_period"] == "1996-Q1"
    with pytest.raises(ValueError, match="partial"):
        metrics.compare_periods(quarters, "1996-Q2", "1996-Q1")
    with pytest.raises(ValueError, match="no data"):
        metrics.compare_periods(quarters, "1997-Q1", "1996-Q4")


def test_revenue_drop_alert_fires_at_threshold(spark):
    rows = [(1, 1, "1992-01-06", 100), (2, 1, "1992-01-13", 79), (3, 1, "1992-01-20", 100),
            (4, 1, "1992-01-26", 1)]
    weeks = metrics.period_over_period(metrics.revenue_by_period(orders_df(spark, rows), "week"), "week")
    fired = [r["period_label"] for r in metrics.revenue_drop_alerts(weeks).collect()]
    assert fired == ["1992-01-13"]


def test_active_window_is_half_open(spark):
    rows = [
        (1, 1, "1997-08-02", 1),  # exactly 12 months before: outside
        (2, 2, "1997-08-03", 1),  # first day inside
        (3, 3, "1998-08-02", 1),  # as-of day: inside
        (4, 4, "1998-08-03", 1),  # after as-of: outside
        (5, 2, "1998-01-01", 1),  # same customer twice: counted once
        (6, 9, "1992-01-01", 1),
    ]
    result = metrics.active_customers_as_of(orders_df(spark, rows), "1998-08-02")
    assert result["active_customers"] == 2
    assert result["window_first_day"] == date(1997, 8, 3)
    assert result["is_complete_window"] is True


def test_monthly_series_matches_point_in_time(spark):
    rows = [(i, i % 7, (date(1992, 1, 1) + timedelta(days=11 * i)).isoformat(), 1) for i in range(250)]
    orders = orders_df(spark, rows)
    series = {r["as_of"]: r for r in metrics.active_customers_monthly(orders).collect()}
    for as_of in [date(1992, 12, 31), date(1993, 6, 30), date(1994, 2, 28)]:
        point = metrics.active_customers_as_of(orders, as_of)
        assert series[as_of]["active_customers"] == point["active_customers"]
        assert series[as_of]["is_complete"] == point["is_complete_window"]
    assert series[date(1992, 11, 30)]["is_complete"] is False  # window starts before the data


def test_add_months_matches_spark(spark):
    for d in [date(1998, 3, 31), date(1996, 2, 29), date(1998, 8, 2)]:
        spark_result = spark.range(1).select(F.add_months(F.lit(d), -12)).first()[0]
        assert metrics.add_months(d, -12) == spark_result


def test_top_n_share(spark):
    sales = spark.createDataFrame(
        [(k, date(1995, 1, 5), Decimal(v)) for k, v in [(1, 50), (2, 30), (3, 20)]],
        "supplier_id long, order_date date, net_revenue decimal(20,4)",
    )
    row = metrics.top_n_share(sales, "supplier_id", n=2).first()
    assert row["top2_share_pct"] == 80.0
