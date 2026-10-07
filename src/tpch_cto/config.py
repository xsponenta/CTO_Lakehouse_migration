from dataclasses import dataclass

DEFAULTS = {
    "catalog": "workspace",
    "prefix": "cto",
    "source": "samples.tpch",
}

# parents first, so foreign keys can be checked against already built tables
TPCH_TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")


@dataclass(frozen=True)
class Config:
    catalog: str
    prefix: str
    source: str

    def schema(self, layer: str) -> str:
        return f"{self.catalog}.{self.prefix}_{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def source_table(self, name: str) -> str:
        return f"{self.source}.{name}"


def from_widgets(dbutils) -> Config:
    for name, default in DEFAULTS.items():
        dbutils.widgets.text(name, default)
    return Config(**{name: dbutils.widgets.get(name).strip() for name in DEFAULTS})
