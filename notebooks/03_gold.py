# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold
# MAGIC Star schema + aggregates for the CTO questions. All revenue numbers come from `tpch_cto.metrics`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from pyspark.sql import functions as F

from tpch_cto import metrics
from tpch_cto.config import from_widgets

cfg = from_widgets(dbutils)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('gold')}")


def silver(name):
    return spark.table(cfg.table("silver", name))


def gold(name):
    return spark.table(cfg.table("gold", name))


def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("gold", name))
    print(f"gold.{name}: {gold(name).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions
# MAGIC Nation and region are put into customer/supplier (star, not snowflake).

# COMMAND ----------

geo = (
    silver("nation").alias("n")
    .join(silver("region").alias("r"), "region_id")
    .select("nation_id", F.col("n.name").alias("nation"), F.col("r.name").alias("region"))
)

save(
    silver("customer").join(geo, "nation_id", "left")
    .select("customer_id", "name", "market_segment", "nation", "region", "account_balance"),
    "dim_customer",
)
save(
    silver("supplier").join(geo, "nation_id", "left")
    .select("supplier_id", "name", "nation", "region", "account_balance"),
    "dim_supplier",
)
save(
    silver("part").select(
        "part_id", "name", "manufacturer", "brand", "type", "container", "size", "retail_price"
    ),
    "dim_part",
)

# COMMAND ----------

lo = silver("orders").agg(F.min("order_date")).first()[0]
hi = silver("lineitem").agg(F.max("receipt_date")).first()[0]

save(
    spark.sql(f"""
        SELECT
          CAST(date_format(d, 'yyyyMMdd') AS INT)      AS date_key,
          d                                            AS date,
          year(d)                                      AS year,
          quarter(d)                                   AS quarter,
          concat(year(d), '-Q', quarter(d))            AS quarter_label,
          month(d)                                     AS month,
          date_format(d, 'yyyy-MM')                    AS month_label,
          CAST(date_trunc('week', d) AS DATE)          AS week_start,
          weekofyear(d)                                AS iso_week,
          dayofweek(d)                                 AS day_of_week,
          date_format(d, 'EEEE')                       AS day_name,
          dayofweek(d) IN (1, 7)                       AS is_weekend
        FROM (SELECT explode(sequence(DATE'{lo}', DATE'{hi}', INTERVAL 1 DAY)) AS d)
    """),
    "dim_date",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts
# MAGIC `fct_sales` - one row per order line (needed for supplier/part revenue).
# MAGIC `fct_orders` - one row per order.

# COMMAND ----------

price, discount, tax = F.col("l.extended_price"), F.col("l.discount"), F.col("l.tax")

fct_sales = (
    silver("lineitem").alias("l")
    .join(silver("orders").alias("o"), "order_id")
    .select(
        "order_id",
        "l.line_number",
        "o.customer_id",
        "l.supplier_id",
        "l.part_id",
        F.date_format("o.order_date", "yyyyMMdd").cast("int").alias("order_date_key"),
        "o.order_date",
        "o.order_priority",
        "o.order_status",
        "l.ship_date",
        "l.commit_date",
        "l.receipt_date",
        "l.ship_mode",
        "l.quantity",
        price.alias("gross_amount"),
        discount,
        tax,
        metrics.discount_amount(price, discount).alias("discount_amount"),
        metrics.net_revenue(price, discount).alias("net_revenue"),
        metrics.net_revenue_with_tax(price, discount, tax).alias("net_revenue_with_tax"),
    )
)
save(fct_sales, "fct_sales")

save(
    gold("fct_sales")
    .groupBy("order_id", "customer_id", "order_date", "order_date_key", "order_priority",
             "order_status")
    .agg(
        F.count(F.lit(1)).alias("line_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("gross_amount").alias("gross_amount"),
        F.sum("discount_amount").alias("discount_amount"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("net_revenue_with_tax").alias("net_revenue_with_tax"),
    ),
    "fct_orders",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates
# MAGIC Revenue by week/month/quarter/year. `change_pct` is only filled when both periods are complete
# MAGIC (first week, Aug 1998 and 1998-Q3 are partial).

# COMMAND ----------

orders = gold("fct_orders")
bounds = metrics.data_bounds(orders)
print(f"order dates: {bounds[0]} .. {bounds[1]}")

for grain in metrics.GRAINS:
    periods = metrics.period_over_period(
        metrics.revenue_by_period(orders, grain, bounds=bounds), grain
    )
    if grain == "month":
        periods = metrics.period_over_period(periods, grain, periods_back=12, suffix="_yoy")
    save(periods, f"agg_revenue_{grain}")

save(metrics.active_customers_monthly(orders, bounds=bounds), "agg_active_customers_monthly")

# COMMAND ----------

sales = gold("fct_sales")

save(
    metrics.contribution_by_period(sales, "supplier_id", "year")
    .join(gold("dim_supplier").select("supplier_id", F.col("name").alias("supplier_name"),
                                      "nation", "region"), "supplier_id", "left")
    .withColumnRenamed("period_label", "year"),
    "agg_supplier_revenue_yearly",
)
save(
    metrics.contribution_by_period(sales, "part_id", "year")
    .join(gold("dim_part").select("part_id", F.col("name").alias("part_name"), "brand", "type"),
          "part_id", "left")
    .withColumnRenamed("period_label", "year"),
    "agg_part_revenue_yearly",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## kpi_company_health
# MAGIC Monthly: revenue (+ YoY), active customers (12m), top-10 supplier share.

# COMMAND ----------

monthly = gold("agg_revenue_month").select(
    F.col("period_start").alias("month"),
    F.col("period_label").alias("month_label"),
    F.col("is_complete").alias("revenue_complete"),
    "revenue",
    F.col("change_pct_yoy").alias("revenue_yoy_pct"),
)
active = gold("agg_active_customers_monthly").select(
    F.col("snapshot_month").alias("month"),
    F.col("active_customers").alias("active_customers_12m"),
    F.col("is_complete").alias("active_window_complete"),
)
concentration = metrics.top_n_share(sales, "supplier_id", n=10, grain="month").select(
    F.col("period_start").alias("month"),
    F.col("top10_share_pct").alias("top10_supplier_share_pct"),
)

save(
    monthly.join(active, "month", "left").join(concentration, "month", "left").orderBy("month"),
    "kpi_company_health",
)
display(gold("kpi_company_health").orderBy(F.desc("month")).limit(14))
