"""Silver tables: columns, keys and quality rules.

Allowed values and ranges come from profiling the source (see 02_silver).
"""

from dataclasses import dataclass, field

REGIONS = ("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
MARKET_SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
ORDER_STATUSES = ("F", "O", "P")
ORDER_PRIORITIES = ("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
RETURN_FLAGS = ("A", "N", "R")
LINE_STATUSES = ("F", "O")
SHIP_MODES = ("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")
SHIP_INSTRUCTIONS = ("COLLECT COD", "DELIVER IN PERSON", "NONE", "TAKE BACK RETURN")

QUANTITY_RANGE = (1, 50)
DISCOUNT_RANGE = (0.00, 0.10)
TAX_RANGE = (0.00, 0.08)


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def _not_blank(column: str) -> str:
    return f"{column} IS NOT NULL AND {column} <> ''"


@dataclass(frozen=True)
class ForeignKey:
    name: str
    columns: list[str]
    parent: str
    parent_columns: list[str]


@dataclass(frozen=True)
class TableSpec:
    name: str
    source: str
    primary_key: list[str]
    columns: dict[str, str]  # silver column -> SQL expression on bronze
    rules: dict[str, str]
    foreign_keys: list[ForeignKey] = field(default_factory=list)


SILVER_TABLES: list[TableSpec] = [
    TableSpec(
        name="region",
        source="region",
        primary_key=["region_id"],
        columns={
            "region_id": "CAST(r_regionkey AS INT)",
            "name": "TRIM(r_name)",
            "comment": "r_comment",
        },
        rules={
            "has_name": _not_blank("name"),
            "valid_region": _in("name", REGIONS),
        },
    ),
    TableSpec(
        name="nation",
        source="nation",
        primary_key=["nation_id"],
        columns={
            "nation_id": "CAST(n_nationkey AS INT)",
            "name": "TRIM(n_name)",
            "region_id": "CAST(n_regionkey AS INT)",
            "comment": "n_comment",
        },
        rules={
            "has_name": _not_blank("name"),
            "canonical_name": "name = upper(name)",
        },
        foreign_keys=[ForeignKey("region", ["region_id"], "region", ["region_id"])],
    ),
    TableSpec(
        name="supplier",
        source="supplier",
        primary_key=["supplier_id"],
        columns={
            "supplier_id": "CAST(s_suppkey AS BIGINT)",
            "name": "TRIM(s_name)",
            "address": "TRIM(s_address)",
            "nation_id": "CAST(s_nationkey AS INT)",
            "phone": "TRIM(s_phone)",
            "account_balance": "CAST(s_acctbal AS DECIMAL(18,2))",
            "comment": "s_comment",
        },
        rules={
            "has_name": _not_blank("name"),
            "has_balance": "account_balance IS NOT NULL",
        },
        foreign_keys=[ForeignKey("nation", ["nation_id"], "nation", ["nation_id"])],
    ),
    TableSpec(
        name="customer",
        source="customer",
        primary_key=["customer_id"],
        columns={
            "customer_id": "CAST(c_custkey AS BIGINT)",
            "name": "TRIM(c_name)",
            "address": "TRIM(c_address)",
            "nation_id": "CAST(c_nationkey AS INT)",
            "phone": "TRIM(c_phone)",
            "account_balance": "CAST(c_acctbal AS DECIMAL(18,2))",
            "market_segment": "TRIM(c_mktsegment)",
            "comment": "c_comment",
        },
        rules={
            "has_name": _not_blank("name"),
            "valid_segment": _in("market_segment", MARKET_SEGMENTS),
        },
        foreign_keys=[ForeignKey("nation", ["nation_id"], "nation", ["nation_id"])],
    ),
    TableSpec(
        name="part",
        source="part",
        primary_key=["part_id"],
        columns={
            "part_id": "CAST(p_partkey AS BIGINT)",
            "name": "TRIM(p_name)",
            "manufacturer": "TRIM(p_mfgr)",
            "brand": "TRIM(p_brand)",
            "type": "TRIM(p_type)",
            "size": "CAST(p_size AS INT)",
            "container": "TRIM(p_container)",
            "retail_price": "CAST(p_retailprice AS DECIMAL(18,2))",
            "comment": "p_comment",
        },
        rules={
            "has_name": _not_blank("name"),
            "has_brand": _not_blank("brand"),
            "has_manufacturer": _not_blank("manufacturer"),
            "positive_retail_price": "retail_price > 0",
        },
    ),
    TableSpec(
        name="partsupp",
        source="partsupp",
        primary_key=["part_id", "supplier_id"],
        columns={
            "part_id": "CAST(ps_partkey AS BIGINT)",
            "supplier_id": "CAST(ps_suppkey AS BIGINT)",
            "available_qty": "CAST(ps_availqty AS INT)",
            "supply_cost": "CAST(ps_supplycost AS DECIMAL(18,2))",
            "comment": "ps_comment",
        },
        rules={
            "positive_supply_cost": "supply_cost > 0",
            "non_negative_qty": "available_qty >= 0",
        },
        foreign_keys=[
            ForeignKey("part", ["part_id"], "part", ["part_id"]),
            ForeignKey("supplier", ["supplier_id"], "supplier", ["supplier_id"]),
        ],
    ),
    TableSpec(
        name="orders",
        source="orders",
        primary_key=["order_id"],
        columns={
            "order_id": "CAST(o_orderkey AS BIGINT)",
            "customer_id": "CAST(o_custkey AS BIGINT)",
            "order_status": "TRIM(o_orderstatus)",
            "total_price": "CAST(o_totalprice AS DECIMAL(18,2))",
            "order_date": "CAST(o_orderdate AS DATE)",
            "order_priority": "TRIM(o_orderpriority)",
            "clerk": "TRIM(o_clerk)",
            "ship_priority": "CAST(o_shippriority AS INT)",
            "comment": "o_comment",
        },
        rules={
            "has_order_date": "order_date IS NOT NULL",
            "positive_total": "total_price > 0",
            "valid_status": _in("order_status", ORDER_STATUSES),
            "valid_priority": _in("order_priority", ORDER_PRIORITIES),
        },
        foreign_keys=[ForeignKey("customer", ["customer_id"], "customer", ["customer_id"])],
    ),
    TableSpec(
        name="lineitem",
        source="lineitem",
        primary_key=["order_id", "line_number"],
        columns={
            "order_id": "CAST(l_orderkey AS BIGINT)",
            "line_number": "CAST(l_linenumber AS INT)",
            "part_id": "CAST(l_partkey AS BIGINT)",
            "supplier_id": "CAST(l_suppkey AS BIGINT)",
            "quantity": "CAST(l_quantity AS DECIMAL(18,2))",
            "extended_price": "CAST(l_extendedprice AS DECIMAL(18,2))",
            "discount": "CAST(l_discount AS DECIMAL(18,2))",
            "tax": "CAST(l_tax AS DECIMAL(18,2))",
            "return_flag": "TRIM(l_returnflag)",
            "line_status": "TRIM(l_linestatus)",
            "ship_date": "CAST(l_shipdate AS DATE)",
            "commit_date": "CAST(l_commitdate AS DATE)",
            "receipt_date": "CAST(l_receiptdate AS DATE)",
            "ship_instruct": "TRIM(l_shipinstruct)",
            "ship_mode": "TRIM(l_shipmode)",
            "comment": "l_comment",
        },
        rules={
            "quantity_in_range": "quantity BETWEEN {} AND {}".format(*QUANTITY_RANGE),
            "positive_extended_price": "extended_price > 0",
            "discount_in_range": "discount BETWEEN {} AND {}".format(*DISCOUNT_RANGE),
            "tax_in_range": "tax BETWEEN {} AND {}".format(*TAX_RANGE),
            "valid_return_flag": _in("return_flag", RETURN_FLAGS),
            "valid_line_status": _in("line_status", LINE_STATUSES),
            "valid_ship_mode": _in("ship_mode", SHIP_MODES),
            "valid_ship_instruct": _in("ship_instruct", SHIP_INSTRUCTIONS),
            "has_dates": "ship_date IS NOT NULL AND commit_date IS NOT NULL"
            " AND receipt_date IS NOT NULL",
            "received_after_shipped": "receipt_date >= ship_date",
        },
        foreign_keys=[
            ForeignKey("order", ["order_id"], "orders", ["order_id"]),
            ForeignKey("partsupp", ["part_id", "supplier_id"], "partsupp", ["part_id", "supplier_id"]),
        ],
    ),
]

SPECS = {s.name: s for s in SILVER_TABLES}
