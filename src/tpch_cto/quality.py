from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def split_valid(df: DataFrame, rules: dict[str, str]) -> tuple[DataFrame, DataFrame]:
    """Split df into (good, bad). NULL rule result counts as a failure."""
    flags = {name: f"_rule_{name}" for name in rules}
    tagged = df.select(
        "*", *[F.coalesce(F.expr(cond), F.lit(False)).alias(flags[n]) for n, cond in rules.items()]
    )
    all_pass = reduce(lambda a, b: a & b, [F.col(f) for f in flags.values()], F.lit(True))
    failed = F.concat_ws(",", *[F.when(~F.col(flags[n]), F.lit(n)) for n in rules])

    good = tagged.filter(all_pass).drop(*flags.values())
    bad = (
        tagged.filter(~all_pass)
        .withColumn("_failed_rules", failed)
        .withColumn("_quarantined_at", F.current_timestamp())
        .drop(*flags.values())
    )
    return good, bad


def with_pk_count(df: DataFrame, primary_key: list[str]) -> DataFrame:
    return df.withColumn("_pk_count", F.count(F.lit(1)).over(Window.partitionBy(*primary_key)))


def with_fk_flag(df, name, columns, parent, parent_columns):
    """Adds _fk_<name> = true if the key exists in parent (works for composite keys)."""
    keys = (
        parent.select(*[F.col(p).alias(c) for p, c in zip(parent_columns, columns)])
        .distinct()
        .withColumn(f"_fk_{name}", F.lit(True))
    )
    return df.join(keys, on=columns, how="left")


def helper_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c == "_pk_count" or c.startswith("_fk_")]
