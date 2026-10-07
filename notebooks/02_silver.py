# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver
# MAGIC For each table: cast and rename columns, drop duplicates, check rules, send failed rows to
# MAGIC `quarantine_<table>`. Table definitions and rules are in `src/tpch_cto/silver_spec.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from pyspark.sql import functions as F

from tpch_cto.config import from_widgets
from tpch_cto.quality import helper_columns, split_valid, with_fk_flag, with_pk_count
from tpch_cto.silver_spec import SILVER_TABLES

cfg = from_widgets(dbutils)


def bronze(name):
    return spark.table(cfg.table("bronze", name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling
# MAGIC Used to pick the allowed values and ranges for the rules (they match the TPC-H spec).

# COMMAND ----------

display(
    bronze("lineitem").select(
        F.min("l_quantity").alias("min_quantity"),
        F.max("l_quantity").alias("max_quantity"),
        F.min("l_discount").alias("min_discount"),
        F.max("l_discount").alias("max_discount"),
        F.min("l_tax").alias("min_tax"),
        F.max("l_tax").alias("max_tax"),
        F.min("l_extendedprice").alias("min_extended_price"),
        F.sum(F.when(F.col("l_receiptdate") < F.col("l_shipdate"), 1).otherwise(0))
        .alias("received_before_shipped"),
    )
)

# COMMAND ----------

CATEGORICALS = [
    ("region", "r_name"),
    ("customer", "c_mktsegment"),
    ("orders", "o_orderstatus"),
    ("orders", "o_orderpriority"),
    ("lineitem", "l_returnflag"),
    ("lineitem", "l_linestatus"),
    ("lineitem", "l_shipmode"),
    ("lineitem", "l_shipinstruct"),
]

profile = None
for table, column in CATEGORICALS:
    counts = (
        bronze(table)
        .groupBy(F.col(column).alias("value"))
        .count()
        .select(F.lit(table).alias("table"), F.lit(column).alias("column"), "value", "count")
    )
    profile = counts if profile is None else profile.unionByName(counts)

display(profile.orderBy("table", "column", "value"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build
# MAGIC Full rebuild. Besides table rules every table gets `pk_not_null`, `unique_pk` and `fk_*` rules.

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema('silver')} CASCADE")
spark.sql(f"CREATE SCHEMA {cfg.schema('silver')}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('ops')}")

load_log = []
for spec in SILVER_TABLES:
    business = list(spec.columns)
    typed = bronze(spec.source).select(
        *[F.expr(expr).alias(col) for col, expr in spec.columns.items()], "_ingested_at"
    )
    bronze_rows = typed.count()
    deduped = typed.dropDuplicates(business)
    duplicates = bronze_rows - deduped.count()

    checked = with_pk_count(deduped, spec.primary_key)
    rules = {
        "pk_not_null": " AND ".join(f"{c} IS NOT NULL" for c in spec.primary_key),
        "unique_pk": "_pk_count = 1",
        **spec.rules,
    }
    for fk in spec.foreign_keys:
        parent = spark.table(cfg.table("silver", fk.parent))
        checked = with_fk_flag(checked, fk.name, fk.columns, parent, fk.parent_columns)
        rules[f"fk_{fk.name}"] = f"_fk_{fk.name}"

    good, bad = split_valid(checked, rules)
    helpers = helper_columns(good)
    good.drop(*helpers).select(*business, "_ingested_at").write.saveAsTable(
        cfg.table("silver", spec.name)
    )
    bad.drop(*helpers).write.saveAsTable(cfg.table("silver", f"quarantine_{spec.name}"))

    silver_rows = spark.table(cfg.table("silver", spec.name)).count()
    quarantined = spark.table(cfg.table("silver", f"quarantine_{spec.name}")).count()
    load_log.append((spec.name, bronze_rows, duplicates, silver_rows, quarantined))
    print(f"silver.{spec.name}: {silver_rows:,} rows, {quarantined:,} quarantined, "
          f"{duplicates:,} duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constraints
# MAGIC PK/FK are not enforced in Unity Catalog, but we need them for the ER diagram (we check them
# MAGIC ourselves above and in 04_validation). CHECK constraints are enforced by Delta.
# MAGIC `lineitem -> partsupp` is a composite FK on `(part_id, supplier_id)`.

# COMMAND ----------

for spec in SILVER_TABLES:
    table = cfg.table("silver", spec.name)
    for col in spec.primary_key:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")
    spark.sql(
        f"ALTER TABLE {table} ADD CONSTRAINT pk_{spec.name} "
        f"PRIMARY KEY ({', '.join(spec.primary_key)})"
    )
    for rule, cond in spec.rules.items():
        spark.sql(
            f"ALTER TABLE {table} ADD CONSTRAINT chk_{spec.name}_{rule} "
            f"CHECK (coalesce(({cond}), false))"
        )

for spec in SILVER_TABLES:
    for fk in spec.foreign_keys:
        spark.sql(
            f"ALTER TABLE {cfg.table('silver', spec.name)} "
            f"ADD CONSTRAINT fk_{spec.name}_{fk.name} FOREIGN KEY ({', '.join(fk.columns)}) "
            f"REFERENCES {cfg.table('silver', fk.parent)} ({', '.join(fk.parent_columns)})"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load summary
# MAGIC Appended to `ops.silver_load_log` every run.

# COMMAND ----------

log_df = spark.createDataFrame(
    load_log,
    "table string, bronze_rows long, duplicates_removed long, silver_rows long, quarantined long",
).withColumn("run_ts", F.current_timestamp())
log_df.write.mode("append").saveAsTable(cfg.table("ops", "silver_load_log"))

display(
    log_df.withColumn(
        "quarantined_pct",
        F.round(100 * F.col("quarantined") / (F.col("silver_rows") + F.col("quarantined")), 4),
    )
)

# COMMAND ----------

failed_rules = None
for spec in SILVER_TABLES:
    q = (
        spark.table(cfg.table("silver", f"quarantine_{spec.name}"))
        .groupBy("_failed_rules")
        .count()
        .withColumn("table", F.lit(spec.name))
    )
    failed_rules = q if failed_rules is None else failed_rules.unionByName(q)

display(failed_rules.select("table", "_failed_rules", "count").orderBy("table", F.desc("count")))
