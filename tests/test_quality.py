from tpch_cto.config import Config
from tpch_cto.quality import helper_columns, split_valid, with_fk_flag, with_pk_count


def test_null_rule_result_is_quarantined_not_lost(spark):
    df = spark.createDataFrame([(1, "BUILDING"), (2, None), (3, "building")], "id int, seg string")
    good, bad = split_valid(df, {"valid_segment": "seg IN ('BUILDING')"})
    assert good.count() + bad.count() == df.count()
    assert sorted(r["id"] for r in bad.collect()) == [2, 3]
    assert {r["_failed_rules"] for r in bad.collect()} == {"valid_segment"}


def test_failed_rules_lists_every_failure(spark):
    df = spark.createDataFrame([(1, -5, None)], "id int, qty int, name string")
    _, bad = split_valid(df, {"positive_qty": "qty > 0", "has_name": "name IS NOT NULL"})
    assert bad.first()["_failed_rules"] == "positive_qty,has_name"


def test_composite_foreign_key_needs_both_columns(spark):
    partsupp = spark.createDataFrame([(1, 10), (2, 20)], "part_id long, supplier_id long")
    lines = spark.createDataFrame([(1, 10), (1, 20), (None, 10)], "part_id long, supplier_id long")
    flagged = with_fk_flag(lines, "partsupp", ["part_id", "supplier_id"], partsupp,
                           ["part_id", "supplier_id"])
    good, bad = split_valid(flagged, {"fk_partsupp": "_fk_partsupp"})
    assert [(r["part_id"], r["supplier_id"]) for r in good.collect()] == [(1, 10)]
    assert bad.count() == 2


def test_duplicate_primary_key_fails(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], "id int, v string")
    checked = with_pk_count(df, ["id"])
    good, _ = split_valid(checked, {"unique_pk": "_pk_count = 1"})
    assert [r["id"] for r in good.collect()] == [2]
    assert helper_columns(good) == ["_pk_count"]


def test_config_names():
    cfg = Config(catalog="workspace", prefix="cto", source="samples.tpch")
    assert cfg.table("silver", "orders") == "workspace.cto_silver.orders"
    assert cfg.source_table("orders") == "samples.tpch.orders"
