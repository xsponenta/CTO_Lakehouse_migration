from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from tpch_cto.quality import split_valid, with_fk_flag, with_pk_count
from tpch_cto.silver_spec import SILVER_TABLES

D = Decimal
BRONZE = {
    "region": [{"r_regionkey": 1, "r_name": "ASIA", "r_comment": "x"}],
    "nation": [{"n_nationkey": 8, "n_name": "INDIA", "n_regionkey": 1, "n_comment": "x"}],
    "supplier": [{"s_suppkey": 7, "s_name": "Supplier#7", "s_address": "a", "s_nationkey": 8,
                  "s_phone": "p", "s_acctbal": D("10.00"), "s_comment": "x"}],
    "customer": [{"c_custkey": 3, "c_name": "Customer#3", "c_address": "a", "c_nationkey": 8,
                  "c_phone": "p", "c_acctbal": D("-5.00"), "c_mktsegment": "BUILDING",
                  "c_comment": "x"}],
    "part": [{"p_partkey": 5, "p_name": "n", "p_mfgr": "Manufacturer#1", "p_brand": "Brand#32",
              "p_type": "t", "p_size": 1, "p_container": "SM BOX", "p_retailprice": D("900.00"),
              "p_comment": "x"}],
    "partsupp": [{"ps_partkey": 5, "ps_suppkey": 7, "ps_availqty": 10, "ps_supplycost": D("1.00"),
                  "ps_comment": "x"}],
    "orders": [{"o_orderkey": 1, "o_custkey": 3, "o_orderstatus": "F", "o_totalprice": D("100.00"),
                "o_orderdate": date(1996, 1, 2), "o_orderpriority": "1-URGENT", "o_clerk": "c",
                "o_shippriority": 0, "o_comment": "x"}],
    "lineitem": [{"l_orderkey": 1, "l_partkey": 5, "l_suppkey": 7, "l_linenumber": 1,
                  "l_quantity": D("2.00"), "l_extendedprice": D("100.00"), "l_discount": D("0.05"),
                  "l_tax": D("0.02"), "l_returnflag": "N", "l_linestatus": "O",
                  "l_shipdate": date(1996, 1, 5), "l_commitdate": date(1996, 1, 9),
                  "l_receiptdate": date(1996, 1, 10), "l_shipinstruct": "NONE",
                  "l_shipmode": "AIR", "l_comment": "x"}],
}

# one broken row per table
BREAK = {
    "region": {"r_regionkey": 2, "r_name": "atlantis"},
    "nation": {"n_nationkey": 9, "n_regionkey": 99},
    "supplier": {"s_suppkey": 8, "s_nationkey": None},
    "customer": {"c_custkey": 4, "c_mktsegment": "  building"},
    "part": {"p_partkey": 6, "p_brand": ""},
    "partsupp": {"ps_suppkey": 99},
    "orders": {"o_orderkey": 2, "o_custkey": None},
    "lineitem": {"l_linenumber": 2, "l_suppkey": 99, "l_discount": D("0.50")},
}


def test_every_spec_passes_good_rows_and_quarantines_bad_ones(spark):
    silver = {}
    for spec in SILVER_TABLES:
        good_row = BRONZE[spec.source][0]
        rows = [good_row, {**good_row, **BREAK[spec.source]}]
        bronze = spark.createDataFrame(rows).withColumn("_ingested_at", F.current_timestamp())

        typed = bronze.select(*[F.expr(e).alias(c) for c, e in spec.columns.items()], "_ingested_at")
        checked = with_pk_count(typed.dropDuplicates(list(spec.columns)), spec.primary_key)
        rules = {"unique_pk": "_pk_count = 1", **spec.rules}
        for fk in spec.foreign_keys:
            checked = with_fk_flag(checked, fk.name, fk.columns, silver[fk.parent], fk.parent_columns)
            rules[f"fk_{fk.name}"] = f"_fk_{fk.name}"

        good, bad = split_valid(checked, rules)
        assert good.count() == 1, spec.name
        assert bad.count() == 1, spec.name
        silver[spec.name] = good
