# Olist E-Commerce Data Warehouse

## Overview

This project builds a **complete Data Warehouse** for the [Brazilian Olist e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) using **Microsoft SQL Server** and the **Medallion Architecture**.

Data flows through three layers:

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Bronze** | `bronze` | Raw ingestion — data lands exactly as it arrives from the source. |
| **Silver** | `silver` | Cleaned, typed, and standardized — the "single version of truth." |
| **Gold** | `gold` | Star Schema optimized for analytics and Power BI dashboards. |

The pipeline is fully automated through **Stored Procedures** with centralized logging, transaction safety, and built-in data quality checks.

---

## Naming Conventions

Consistent naming makes the warehouse easier to navigate and maintain. Each layer follows a clear rule:

### Bronze & Silver — Source-Centric

```
<source>_<entity>
```

- **`<source>`** — Name of the source system (e.g., `olist`).
- **`<entity>`** — Original table name, abbreviated for brevity (e.g., `cust`, `ord`, `prd`).

> **Example:** `olist_cust` — Customer data from the Olist system.

Bronze and Silver share the same naming pattern so you can trace a table across layers at a glance.

### The Surrogate Key Exception

Surrogate Keys (SK) **do not** follow the source prefix convention.

| Convention | Example | Why |
|---|---|---|
| Source prefix | `or_rev_id` | This column originates from the Olist source system. |
| No prefix | `rev_sk` | This key is **internal** to our warehouse — it has no meaning in the source. |

SKs are system-generated identifiers created by the Data Warehouse itself. Omitting the source prefix makes it immediately clear that a column is warehouse-internal, not a source field.

### Gold — Business-Centric

```
<type>_<entity>
```

- **`<type>`** — Role in the Star Schema:
  - `fact_` — Quantitative measures and foreign keys (e.g., `fact_sales`).
  - `dim_` — Descriptive attributes / dimensions (e.g., `dim_customers`, `dim_products`).
- **`<entity>`** — The business subject area.

> **Example:** `fact_sales` — Central fact table containing sales metrics and links to all dimensions.

---

## Technical Highlights

### Centralized Logging

ETL operations are tracked in a **dedicated `logging` schema**, keeping operational metadata completely separate from business data.

- Every table load records a `RUNNING` status at start, then updates to `SUCCESS` or `FAILED` on completion.
- Columns include `process_name`, `source_layer`, `target_layer`, `rows_inserted`, `start_ts`, `end_ts`, and `error_message`.
- **Why:** Isolating logs from business schemas prevents accidental coupling and gives a single place to monitor pipeline health across all layers.

### Transaction Safety

All Stored Procedures use **`SET XACT_ABORT ON`** combined with **`BEGIN TRANSACTION`** / **`COMMIT`** / **`ROLLBACK`**.

- If any single statement fails, the entire batch is rolled back automatically — no partial loads.
- **Why:** This guarantees **ACID compliance**. The Gold layer is either fully refreshed or left completely untouched, preventing data corruption and inconsistent analytics.

### Data Quality — Financial Reconciliation

A cross-table validation compares **`SUM(price + freight)`** from `olist_ord_item` against **`SUM(payment_value)`** from `olist_ord_pay` at the order level.

- A tolerance of **0.01** is applied to absorb floating-point rounding differences.
- Any order exceeding this threshold is flagged for investigation.
- **Why:** Financial figures must reconcile. Even small discrepancies, if left unchecked, compound across thousands of orders and erode trust in the data.

### Data Healing — Impossible Timestamps

During profiling, **23 rows** were discovered where the delivery date occurred **before** the purchase date — a chronological impossibility.

Rather than deleting these rows (which would lose other valid fields), the Silver layer **heals** the data:

1. The impossible timestamp is set to `NULL` (it cannot be trusted).
2. Derived columns like `delivery_lead_time` and `ord_is_late` gracefully return `NULL` instead of producing misleading negative values.

- **Why:** Silently propagating bad dates would distort delivery KPIs. Setting them to `NULL` preserves the row while clearly signaling "this value is unknown" — protecting downstream analytics from invisible errors.

---

## Project Structure

```
data-warehouse-project/
├── datasets/                  # Source CSV files (Olist dataset)
├── scripts/
│   ├── init_database.sql      # Database, schemas, and logging table
│   ├── bronze/
│   │   └── proc_load_bronze.sql
│   ├── silver/
│   │   ├── ddl_silver.sql     # Silver table definitions
│   │   └── proc_load_silver.sql
│   └── gold/
│       ├── ddl_gold.sql       # Star Schema table definitions
│       ├── ddl_gold_views.sql # Presentation views for Power BI
│       └── proc_load_gold.sql
└── tests/
    ├── quality_checks_silver.sql
    └── quality_checks_gold.sql
```

---

## How to Run

1. Execute **`init_database.sql`** to create the database, schemas, and logging table.
2. Load raw CSVs into the `bronze` schema (via `proc_load_bronze` or bulk insert).
3. Run **`EXEC silver.load_silver`** to clean and load the Silver layer.
4. Run **`EXEC gold.load_gold`** to build the Star Schema in the Gold layer.
5. Run the quality check scripts in `/tests` to validate data integrity.

---

## Tech Stack

- **Database:** Microsoft SQL Server
- **Ingestion:** Python (Pandas) — batch processing from CSV to Bronze
- **ETL:** T-SQL Stored Procedures
- **Architecture:** Medallion (Bronze → Silver → Gold)
- **Data Model:** Star Schema (Kimball methodology)
- **Visualization:** Power BI (via Gold presentation views)
