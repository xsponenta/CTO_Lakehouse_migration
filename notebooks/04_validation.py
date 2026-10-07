# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Validation
# MAGIC Checks across all layers. Results go to `ops.dq_check_results`, the job fails if any check fails.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from decimal import Decimal

from pyspark.sql import functions as F

from tpch_cto import metrics
from tpch_cto.config import from_widgets
from tpch_cto.silver_spec import SILVER_TABLES

cfg = from_widgets(dbutils)
TOLERANCE = Decimal("0.01")


def layer(layer_name, name):
    return spark.table(cfg.table(layer_name, name))


results = []


def check(name, passed, detail=""):
    results.append((name, bool(passed), str(detail)))
    print("PASS" if passed else "FAIL", name, detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row counts: bronze (deduplicated) = silver + quarantine

# COMMAND ----------

for spec in SILVER_TABLES:
    typed = layer("bronze", spec.source).select(
        *[F.expr(e).alias(c) for c, e in spec.columns.items()]
    )
    deduped = typed.dropDuplicates().count()
    silver_rows = layer("silver", spec.name).count()
    quarantined = layer("silver", f"quarantine_{spec.name}").count()
    check(
        f"rows accounted for: {spec.name}",
        deduped == silver_rows + quarantined,
        f"{deduped:,} = {silver_rows:,} silver + {quarantined:,} quarantined",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PK unique, FK resolve

# COMMAND ----------

for spec in SILVER_TABLES:
    t = layer("silver", spec.name)
    total, distinct = t.count(), t.select(*spec.primary_key).distinct().count()
    check(f"unique primary key: {spec.name}", total == distinct, f"{total:,} rows, {distinct:,} keys")

    for fk in spec.foreign_keys:
        parent = layer("silver", fk.parent).select(
            *[F.col(p).alias(c) for p, c in zip(fk.parent_columns, fk.columns)]
        )
        orphans = t.join(parent, fk.columns, "left_anti").count()
        check(
            f"foreign key resolves: {spec.name}.({', '.join(fk.columns)}) -> {fk.parent}",
            orphans == 0,
            f"{orphans:,} orphans",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ship date not before order date

# COMMAND ----------

early = (
    layer("silver", "lineitem").join(layer("silver", "orders"), "order_id")
    .filter(F.col("ship_date") < F.col("order_date"))
    .count()
)
check("no line shipped before its order date", early == 0, f"{early:,} lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revenue reconciliation
# MAGIC bronze = duplicates + quarantine + silver, and silver = every gold table.

# COMMAND ----------

def revenue(df, price="extended_price", discount="discount"):
    return df.agg(F.sum(metrics.net_revenue(F.col(price), F.col(discount)))).first()[0] or Decimal(0)


def total(df, column):
    return df.agg(F.sum(column)).first()[0] or Decimal(0)


lineitem_spec = next(s for s in SILVER_TABLES if s.name == "lineitem")
bronze_li = layer("bronze", "lineitem")
bronze_typed = bronze_li.select(*[F.expr(e).alias(c) for c, e in lineitem_spec.columns.items()])

bronze_rev = revenue(bronze_li, "l_extendedprice", "l_discount")
duplicate_rev = bronze_rev - revenue(bronze_typed.dropDuplicates())
quarantine_rev = revenue(layer("silver", "quarantine_lineitem"))
silver_rev = revenue(layer("silver", "lineitem"))

ledger = [
    ("bronze.lineitem (as received)", bronze_rev),
    ("  - duplicates removed", duplicate_rev),
    ("  - quarantined", quarantine_rev),
    ("silver.lineitem", silver_rev),
    ("gold.fct_sales", total(layer("gold", "fct_sales"), "net_revenue")),
    ("gold.fct_orders", total(layer("gold", "fct_orders"), "net_revenue")),
]
ledger += [
    (f"gold.agg_revenue_{g}", total(layer("gold", f"agg_revenue_{g}"), "revenue"))
    for g in metrics.GRAINS
]

display(spark.createDataFrame(
    [(name, float(v), float(v - silver_rev)) for name, v in ledger],
    "layer string, net_revenue double, diff_vs_silver double",
))

gap = bronze_rev - duplicate_rev - quarantine_rev - silver_rev
check("bronze = duplicates + quarantine + silver", abs(gap) <= TOLERANCE, f"gap {gap}")
for name, value in ledger[4:]:
    check(f"silver revenue = {name}", abs(value - silver_rev) <= TOLERANCE, f"diff {value - silver_rev}")

sales_rows, silver_rows = layer("gold", "fct_sales").count(), layer("silver", "lineitem").count()
check("fct_sales keeps every silver line", sales_rows == silver_rows, f"{sales_rows:,} vs {silver_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Active customers: monthly table = point-in-time function

# COMMAND ----------

orders = layer("gold", "fct_orders")
for month_end in ["1995-12-31", "1997-06-30", "1998-07-31"]:
    point = metrics.active_customers_as_of(orders, month_end)["active_customers"]
    series = (
        layer("gold", "agg_active_customers_monthly")
        .filter(F.col("as_of") == F.lit(month_end).cast("date"))
        .first()["active_customers"]
    )
    check(f"active customers agree as of {month_end}", point == series, f"{point:,} vs {series:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. No comparisons with partial periods

# COMMAND ----------

for g in metrics.GRAINS:
    leaked = (
        layer("gold", f"agg_revenue_{g}")
        .filter(~F.col("is_complete") & F.col("change_pct").isNotNull())
        .count()
    )
    check(f"no change_pct on a partial {g}", leaked == 0, f"{leaked} rows")

try:
    metrics.compare_periods(layer("gold", "agg_revenue_quarter"), "1998-Q3", "1998-Q2")
    check("compare_periods refuses a partial quarter", False, "1998-Q3 was compared")
except ValueError as e:
    check("compare_periods refuses a partial quarter", True, str(e)[:60] + "...")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('ops')}")
(
    spark.createDataFrame(results, "check string, passed boolean, detail string")
    .withColumn("run_ts", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(cfg.table("ops", "dq_check_results"))
)

failed = [name for name, passed, _ in results if not passed]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
if failed:
    raise AssertionError(f"{len(failed)} validation checks failed: {failed}")
