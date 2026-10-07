# CTO Lakehouse migration

Group assignment 1 for the Big Data course. Team: Ihor Ivanyshyn, Nazarii Dizhak, Ivan Shynkarenko.

Medallion lakehouse (bronze / silver / gold) on Databricks for TPC-H data (`samples.tpch`).
Our profile is **Strategist / CTO**.

Presentation: _TODO_

## Structure

```
notebooks/
  01_bronze.py         raw copy + metadata
  02_silver.py         types, dedup, quality rules, quarantine, PK/FK
  03_gold.py           star schema + aggregates
  04_validation.py     checks across layers
  05_cto_questions.py  answers to the CTO questions
  06_monitoring.py     metric history + alert rules
src/tpch_cto/
  config.py            catalog / schema names
  silver_spec.py       silver tables and rules
  quality.py           split into good rows / quarantine
  metrics.py           metric definitions (revenue, active customers, periods)
  defects.py           optional broken data for testing
sql/                   queries for the dashboard and the alert
tests/                 local tests
databricks.yml         job (bronze -> silver -> gold -> validation -> monitoring)
```

## How to run

**Databricks:** Workspace → Create → Git folder → paste the repo URL. Run notebooks 01–06 in order.

Widgets:
- `catalog` (default `workspace`)
- `prefix` (default `cto`), which gives the schemas `cto_bronze`, `cto_silver`, `cto_gold` and `cto_ops`
- `source` (default `samples.tpch`)

To run on another workspace or on other (pre-prod) data, only these values need to change.

**As a job:**

```bash
databricks bundle deploy
databricks bundle run cto_lakehouse_pipeline
```

**Locally:**

```bash
uv sync
uv run pytest
```

## Layers

**Bronze** – all 8 tables as they are, plus `_ingested_at`, `_source` and `_batch_id`.
Set `simulate_defects=true` to break some rows and check that silver catches them.

**Silver (3NF)** – one table per entity at the source grain, with renamed columns and proper types.
For each table we:
1. remove duplicates;
2. check the rules;
3. put failed rows into `quarantine_<table>` together with the names of the failed rules.

Failed rows are not deleted. A rule that returns NULL counts as failed.

On the silver tables we declare:
- `PRIMARY KEY` / `FOREIGN KEY`, which give us the ER diagram. Unity Catalog doesn't enforce them, so we check them in code.
- `CHECK` constraints for the row rules.

`lineitem (part_id, supplier_id) → partsupp` is a composite FK, checked with a join on both columns.

Rule bounds come from profiling the source (first cells of `02_silver`) and match the TPC-H spec:

| column | allowed |
|---|---|
| quantity | 1 – 50 |
| discount | 0.00 – 0.10 |
| tax | 0.00 – 0.08 |
| prices, supply cost, order total | > 0 |
| market_segment | AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY |
| order_status / order_priority | F, O, P / 1-URGENT … 5-LOW |
| ship_mode | AIR, FOB, MAIL, RAIL, REG AIR, SHIP, TRUCK |
| return_flag / line_status | A, N, R / F, O |
| dates | receipt_date ≥ ship_date, ship_date ≥ order_date |

ER diagram of silver (Catalog Explorer → `cto_silver` → table → View relationships):

![ER diagram](docs/er_silver.png)

**Gold**

| table | grain |
|---|---|
| dim_date, dim_customer, dim_supplier, dim_part | one row per day / entity |
| fct_sales | order line |
| fct_orders | order |
| agg_revenue_week / month / quarter / year | period |
| agg_active_customers_monthly | month end |
| agg_supplier_revenue_yearly, agg_part_revenue_yearly | key per year |
| kpi_company_health | month |

## Metric definitions

All of these are implemented in `src/tpch_cto/metrics.py`, and every query uses them.

| metric | definition |
|---|---|
| Revenue | `extended_price * (1 - discount)`, without tax, by order date |
| Active customer as of D | at least one order in `(D - 12 months, D]` |
| Complete period | week (Mon–Sun), month, quarter or year fully inside the data range (1992-01-01 … 1998-08-02) |
| Period-over-period change | only between two complete periods, otherwise NULL |
| Revenue drop alert | latest complete week vs previous complete week ≤ -20% |
| Top-10 supplier share | revenue of the 10 biggest suppliers / total revenue in the period |

Partial periods in the data are the first week, August 1998 and 1998-Q3.

## CTO questions (`05_cto_questions`)

1. **Revenue 1996-Q1 vs 1995-Q4.**
2. **Active customers as of 1998-08-02**, counted over the window 1997-08-03 … 1998-08-02.
3. **Alert for a 20% weekly drop.** The query is `sql/alerts/revenue_wow_drop.sql`. We show it on the real data and on data with ~25% of the last week's orders removed.
4. **Company health:**
   - net revenue (YoY);
   - active customers (12 months);
   - top-10 supplier share.

## Validation (`04_validation`)

- bronze rows (after dedup) = silver + quarantine
- PKs are unique, FKs resolve
- no line ships before its order date
- revenue: bronze = duplicates + quarantine + silver, and silver = every gold table (tolerance $0.01)
- active customers: the monthly table matches the point-in-time function
- no comparisons with partial periods

Results are appended to `cto_ops.dq_check_results`. The job fails if any check fails.

## Monitoring

- `06_monitoring` saves metrics to `cto_ops.metric_history` and checks these alerts:
  - revenue week-over-week ≤ -20%
  - active customers month-over-month ≤ -5%
  - top-10 supplier share > 5%
  - quarantine rate > 1%
- `cto_ops.silver_load_log` stores quarantine counts for every load.
- Dashboard: Databricks dashboard built from the queries in `sql/dashboard/`.
- SQL Alert: `sql/alerts/revenue_wow_drop.sql`, condition `change_pct <= -20`.

## Team

| area | who |
|---|---|
| bronze, silver, quality rules | _TODO_ |
| gold, metrics, CTO questions | _TODO_ |
| validation, monitoring, dashboard, presentation | _TODO_ |

## Challenges

_TODO_
