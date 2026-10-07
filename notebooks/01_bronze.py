# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze
# MAGIC Copy all 8 TPC-H tables as is + `_ingested_at`, `_source`, `_batch_id`.
# MAGIC Previous loads are kept in Delta history.
# MAGIC
# MAGIC `simulate_defects = true` breaks some rows on purpose to test silver rules.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

# COMMAND ----------

from uuid import uuid4

from pyspark.sql import functions as F

from tpch_cto.config import TPCH_TABLES, from_widgets
from tpch_cto.defects import simulate_defects

dbutils.widgets.dropdown("simulate_defects", "false", ["false", "true"])
cfg = from_widgets(dbutils)
simulate = dbutils.widgets.get("simulate_defects") == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('bronze')}")

# COMMAND ----------

batch_id = str(uuid4())

for name in TPCH_TABLES:
    df = spark.table(cfg.source_table(name))
    if simulate:
        df = simulate_defects(name, df)
    (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(cfg.source_table(name)))
        .withColumn("_batch_id", F.lit(batch_id))
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.table("bronze", name))
    )
    print(f"bronze.{name} done")

# COMMAND ----------

display(
    spark.createDataFrame(
        [
            (
                name,
                spark.table(cfg.source_table(name)).count(),
                spark.table(cfg.table("bronze", name)).count(),
            )
            for name in TPCH_TABLES
        ],
        "table string, source_rows long, bronze_rows long",
    )
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('bronze', 'orders')}").limit(5))
