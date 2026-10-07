"""Optional fake damage for bronze, to test that silver rules catch it."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

N = 20

DEFECTS = {
    "customer": ("c_custkey", 0, [
        ("c_mktsegment", lambda c: F.concat(F.lit("  "), F.lower(c))),
    ]),
    "orders": ("o_orderkey", 0, [
        ("o_custkey", lambda c: F.lit(None).cast("bigint")),
        ("o_totalprice", lambda c: -F.abs(c)),
    ]),
    # skip the first orders so lineitem damage doesn't overlap with orders damage
    "lineitem": ("l_orderkey", 200, [
        ("l_quantity", lambda c: -F.abs(c)),
        ("l_discount", lambda c: c + F.lit(0.5)),
        ("l_receiptdate", lambda c: F.date_sub(c, 60)),
    ]),
}


def simulate_defects(table: str, df: DataFrame) -> DataFrame:
    if table not in DEFECTS:
        return df
    key, skip, damage = DEFECTS[table]

    keys = [r[0] for r in df.select(key).distinct().orderBy(key)
            .limit(skip + N * (len(damage) + 1)).collect()][skip:]

    for i, (column, fn) in enumerate(damage):
        hit = keys[i * N:(i + 1) * N]
        df = df.withColumn(column, F.when(F.col(key).isin(hit), fn(F.col(column)))
                           .otherwise(F.col(column)))

    # last slice of keys gets duplicated
    return df.unionByName(df.filter(F.col(key).isin(keys[len(damage) * N:])))
