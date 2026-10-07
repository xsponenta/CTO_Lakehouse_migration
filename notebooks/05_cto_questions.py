# Databricks notebook source
# MAGIC %md
# MAGIC # 05 CTO questions
# MAGIC Definitions (see README / `metrics.py`):
# MAGIC - revenue = `extended_price * (1 - discount)`, by order date
# MAGIC - active customer as of D = at least one order in `(D - 12 months, D]`
# MAGIC - only complete periods are compared

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

from tpch_cto import metrics
from tpch_cto.config import from_widgets

cfg = from_widgets(dbutils)


def gold(name):
    return spark.table(cfg.table("gold", name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Revenue 1996-Q1 vs 1995-Q4

# COMMAND ----------

quarters = gold("agg_revenue_quarter")
q1 = metrics.compare_periods(quarters, "1996-Q1", "1995-Q4")

print(f"1996-Q1 revenue   {q1['current_revenue']:>22,.2f}")
print(f"1995-Q4 revenue   {q1['previous_revenue']:>22,.2f}")
print(f"change            {q1['change']:>22,.2f}   ({q1['change_pct']:+.2f}%)")

# COMMAND ----------

display(
    quarters.filter("period_start BETWEEN '1995-01-01' AND '1996-12-31'")
    .select("period_label", "is_complete", "order_count", "revenue", "change_pct")
    .orderBy("period_start")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Active customers as of 1998-08-02
# MAGIC 1998-08-02 is the last order date. Window: 1997-08-03 .. 1998-08-02.

# COMMAND ----------

q2 = metrics.active_customers_as_of(gold("fct_orders"), "1998-08-02")
customer_base = gold("dim_customer").count()

print(q2)
print(f"{q2['active_customers']:,} of {customer_base:,} customers are active "
      f"({100 * q2['active_customers'] / customer_base:.1f}%)")

# COMMAND ----------

display(
    gold("agg_active_customers_monthly")
    .filter("is_complete")
    .select("as_of", "active_customers")
    .orderBy("as_of")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Alert for a 20% week-over-week drop
# MAGIC Rule: latest complete week vs previous complete week, alert if `change_pct <= -20`.
# MAGIC Query: `sql/alerts/revenue_wow_drop.sql` (used as a Databricks SQL Alert).

# COMMAND ----------

with open(os.path.join(os.getcwd(), "..", "sql", "alerts", "revenue_wow_drop.sql")) as f:
    alert_sql = f.read()
print(alert_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC **Real data** - no alert expected.

# COMMAND ----------

spark.sql(f"USE {cfg.schema('gold')}")
display(spark.sql(alert_sql))

weekly = gold("agg_revenue_week")
history = metrics.revenue_drop_alerts(weekly)
print("weeks that would have fired:", history.count())
display(weekly.filter("is_complete").select("period_start", "revenue", "change_pct").orderBy("period_start"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reduced data** - remove ~25% of orders in the last complete week and run the same query.

# COMMAND ----------

orders = gold("fct_orders")
bounds = metrics.data_bounds(orders)
last_week = weekly.filter("is_complete").orderBy(F.desc("period_start")).first()
print("reduced week:", last_week["period_start"], "-", last_week["period_end"])

in_last_week = F.col("order_date").between(last_week["period_start"], last_week["period_end"])
reduced_orders = orders.filter(~(in_last_week & (F.pmod(F.hash("order_id"), F.lit(4)) == 0)))

reduced_weekly = metrics.period_over_period(
    metrics.revenue_by_period(reduced_orders, "week", bounds=bounds), "week"
)
reduced_weekly.createOrReplaceTempView("agg_revenue_week_reduced")

display(spark.sql(alert_sql.replace("agg_revenue_week", "agg_revenue_week_reduced")))
display(metrics.revenue_drop_alerts(reduced_weekly).select("period_label", "prev_revenue", "revenue", "change_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Why only complete periods** - August 1998 has only 2 days of data. Compared naively with July
# MAGIC it looks like a ~90% drop.

# COMMAND ----------

display(
    gold("agg_revenue_month")
    .filter("period_start >= '1998-05-01'")
    .withColumn("naive_prev", F.lag("revenue").over(Window.orderBy("period_start")))
    .withColumn("naive_change_pct",
                F.round(100 * (F.col("revenue") - F.col("naive_prev")) / F.col("naive_prev"), 2))
    .select("period_label", "is_complete", "revenue", "naive_change_pct", "change_pct")
    .orderBy("period_start")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Three metrics for company health
# MAGIC
# MAGIC | metric | why |
# MAGIC |---|---|
# MAGIC | Net revenue (monthly, YoY) | size of the business and growth; YoY removes seasonality |
# MAGIC | Active customers (trailing 12 months) | how many customers the revenue depends on; can fall while revenue still looks fine |
# MAGIC | Top-10 supplier share of revenue | dependency on a few suppliers (risk) |
# MAGIC
# MAGIC On TPC-H all three are pretty flat because the data generator is uniform.

# COMMAND ----------

display(
    gold("kpi_company_health")
    .filter("revenue_complete AND active_window_complete")
    .select("month_label", "revenue", "revenue_yoy_pct", "active_customers_12m",
            "top10_supplier_share_pct")
    .orderBy("month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top suppliers and products (last complete year)

# COMMAND ----------

last_full_year = (
    gold("agg_revenue_year").filter("is_complete").agg(F.max("period_label")).first()[0]
)
print("last complete year:", last_full_year)

display(
    gold("agg_supplier_revenue_yearly")
    .filter((F.col("year") == last_full_year) & (F.col("rank") <= 10))
    .select("rank", "supplier_id", "supplier_name", "nation", "region", "revenue", "share_pct")
    .orderBy("rank")
)

# COMMAND ----------

display(
    gold("agg_part_revenue_yearly")
    .filter((F.col("year") == last_full_year) & (F.col("rank") <= 10))
    .select("rank", "part_id", "part_name", "brand", "type", "revenue", "share_pct")
    .orderBy("rank")
)
