# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Monitoring
# MAGIC Saves headline metrics to `ops.metric_history` and checks alert rules.
# MAGIC If an alert fires and `fail_on_alert = true` the task fails (job sends a failure email).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from pyspark.sql import functions as F

from tpch_cto import metrics
from tpch_cto.config import from_widgets

dbutils.widgets.dropdown("fail_on_alert", "true", ["true", "false"])
cfg = from_widgets(dbutils)
fail_on_alert = dbutils.widgets.get("fail_on_alert") == "true"

ACTIVE_CUSTOMERS_DROP_PCT = -5.0
QUARANTINE_RATE_PCT = 1.0
TOP10_SUPPLIER_SHARE_PCT = 5.0


def gold(name):
    return spark.table(cfg.table("gold", name))

# COMMAND ----------

week = (
    gold("agg_revenue_week").filter("is_complete AND change_pct IS NOT NULL")
    .orderBy(F.desc("period_start")).first()
)

active = (
    gold("agg_active_customers_monthly").filter("is_complete")
    .orderBy(F.desc("as_of")).limit(2).collect()
)
active_now, active_before = active[0], active[1]
active_change = 100 * (active_now["active_customers"] - active_before["active_customers"]) / active_before["active_customers"]

health = (
    gold("kpi_company_health").filter("revenue_complete")
    .orderBy(F.desc("month")).first()
)

load = spark.table(cfg.table("ops", "silver_load_log"))
latest_load = load.filter(F.col("run_ts") == load.agg(F.max("run_ts")).first()[0])
q = latest_load.agg(F.sum("quarantined").alias("q"), F.sum("silver_rows").alias("s")).first()
quarantine_rate = 100 * q["q"] / (q["q"] + q["s"])

snapshot = [
    ("weekly_revenue", str(week["period_start"]), float(week["revenue"])),
    ("weekly_revenue_wow_pct", str(week["period_start"]), float(week["change_pct"])),
    ("active_customers_12m", str(active_now["as_of"]), float(active_now["active_customers"])),
    ("active_customers_mom_pct", str(active_now["as_of"]), float(active_change)),
    ("top10_supplier_share_pct", str(health["month"]), float(health["top10_supplier_share_pct"])),
    ("quarantine_rate_pct", "latest load", float(quarantine_rate)),
]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('ops')}")
metric_df = spark.createDataFrame(snapshot, "metric string, period string, value double")
metric_df.withColumn("run_ts", F.current_timestamp()).write.mode("append").saveAsTable(
    cfg.table("ops", "metric_history")
)
display(metric_df)

# COMMAND ----------

alerts = [
    ("revenue dropped >= 20% week over week",
     week["change_pct"] <= metrics.REVENUE_DROP_ALERT_PCT,
     f"week of {week['period_start']}: {week['change_pct']:+.2f}%"),
    (f"active customers dropped >= {abs(ACTIVE_CUSTOMERS_DROP_PCT):.0f}% month over month",
     active_change <= ACTIVE_CUSTOMERS_DROP_PCT,
     f"as of {active_now['as_of']}: {active_change:+.2f}%"),
    (f"top-10 suppliers hold more than {TOP10_SUPPLIER_SHARE_PCT:.0f}% of revenue",
     health["top10_supplier_share_pct"] > TOP10_SUPPLIER_SHARE_PCT,
     f"{health['month']}: {health['top10_supplier_share_pct']:.4f}%"),
    (f"more than {QUARANTINE_RATE_PCT:.0f}% of rows quarantined",
     quarantine_rate > QUARANTINE_RATE_PCT,
     f"{quarantine_rate:.4f}%"),
]

for name, fired, detail in alerts:
    print("ALERT" if fired else "ok", name, detail)

fired = [name for name, f, _ in alerts if f]
if fired and fail_on_alert:
    raise RuntimeError(f"{len(fired)} alert(s) fired: {fired}")
