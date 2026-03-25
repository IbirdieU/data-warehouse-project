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

## Data Catalog for Gold Layer

The Gold Layer is the business-level data representation, structured using a **Star Schema** to support analytical queries and Power BI dashboards. It consists of **4 dimension tables** and **1 fact table**.

### 1. **gold.dim_customers** (Identity Resolution)

- **Purpose:** Stores one row per unique person with their current (latest) shipping address.
- **Source:** `silver.olist_cust` JOIN `silver.olist_ord`
- **Grain:** One row per `customer_unique_id` (one real-world person).
- **Logic:** `ROW_NUMBER()` partitioned by `customer_unique_id`, ordered by `purchase_ts DESC` picks the most recent order to determine current location.

| Column Name        | Data Type     | Constraint | Description                                                              |
|--------------------|---------------|------------|--------------------------------------------------------------------------|
| customer_sk        | INT           | PK         | Surrogate key uniquely identifying each customer record.                 |
| customer_unique_id | VARCHAR(50)   | NOT NULL   | Natural key: persistent buyer identifier — one row per real-world person.|
| zip_code_prefix    | CHAR(5)       | NULL       | Latest shipping ZIP code prefix (from most recent order).                |
| city               | NVARCHAR(100) | NULL       | Latest standardized city name (from most recent order).                  |
| state              | CHAR(2)       | NULL       | Latest Brazilian state abbreviation (e.g., SP, RJ, MG).                 |
| dwh_create_date    | DATETIME2     | NOT NULL   | Timestamp when this record was inserted into the gold layer.             |

---

### 2. **gold.dim_products**

- **Purpose:** Provides product attributes including category, listing details, and physical dimensions.
- **Source:** `silver.olist_prd` LEFT JOIN `silver.olist_prd_cat_map`
- **Grain:** One row per product.

| Column Name        | Data Type     | Constraint | Description                                                              |
|--------------------|---------------|------------|--------------------------------------------------------------------------|
| product_sk         | INT           | PK         | Surrogate key uniquely identifying each product record.                  |
| product_id         | VARCHAR(50)   | NOT NULL   | Natural key: original product identifier from the source system.         |
| category_name_pt   | NVARCHAR(100) | NULL       | Product category in Portuguese as it appears in the raw data.            |
| category_name_en   | NVARCHAR(100) | NULL       | Product category translated to English; NULL if no mapping exists.       |
| name_length        | INT           | NULL       | Character count of the product name on the listing.                      |
| description_length | INT           | NULL       | Character count of the product description.                              |
| photos_quantity    | INT           | NULL       | Number of photos published for this product.                             |
| weight_g           | INT           | NULL       | Product weight in grams.                                                 |
| length_cm          | INT           | NULL       | Product length in centimetres.                                           |
| height_cm          | INT           | NULL       | Product height in centimetres.                                           |
| width_cm           | INT           | NULL       | Product width in centimetres.                                            |
| dwh_create_date    | DATETIME2     | NOT NULL   | Timestamp when this record was inserted into the gold layer.             |

---

### 3. **gold.dim_sellers**

- **Purpose:** Stores seller details with standardized geographic information.
- **Source:** `silver.olist_sel`
- **Grain:** One row per seller.

| Column Name      | Data Type     | Constraint | Description                                                              |
|------------------|---------------|------------|--------------------------------------------------------------------------|
| seller_sk        | INT           | PK         | Surrogate key uniquely identifying each seller record.                   |
| seller_id        | VARCHAR(50)   | NOT NULL   | Natural key: original seller identifier from the source system.          |
| zip_code_prefix  | CHAR(5)       | NULL       | First 5 digits of the seller's postal / ZIP code.                        |
| city             | NVARCHAR(100) | NULL       | Standardized seller city name (sourced from `sel_city_std`).             |
| state            | CHAR(2)       | NULL       | Brazilian state abbreviation (e.g., SP, MG, PR).                        |
| dwh_create_date  | DATETIME2     | NOT NULL   | Timestamp when this record was inserted into the gold layer.             |

---

### 4. **gold.dim_date**

- **Purpose:** Static calendar dimension enabling time-based analysis.
- **Source:** Generated via recursive CTE (no silver source table).
- **Grain:** One row per calendar day.
- **Range:** 2016-01-01 to 2020-12-31

| Column Name      | Data Type    | Constraint | Description                                                              |
|------------------|--------------|------------|--------------------------------------------------------------------------|
| date_sk          | INT          | PK         | Surrogate key, sequential by date due to ordered INSERT.                 |
| date_id          | INT          | UNIQUE     | Natural key in YYYYMMDD format for fast filtering (e.g., 20180315).      |
| full_date        | DATE         | UNIQUE     | The actual calendar date value for direct date arithmetic.               |
| calendar_year    | INT          | NOT NULL   | Four-digit calendar year (e.g., 2018).                                   |
| quarter_number   | INT          | NOT NULL   | Calendar quarter number (1-4).                                           |
| quarter_name     | NVARCHAR(2)  | NOT NULL   | Quarter label for reports (e.g., Q1, Q4).                                |
| month_number     | INT          | NOT NULL   | Month number (1 = January, 12 = December).                               |
| month_name       | NVARCHAR(20) | NOT NULL   | Full English month name (e.g., January, November).                       |
| month_name_short | NVARCHAR(3)  | NOT NULL   | Three-letter abbreviation (e.g., Jan, Nov).                              |
| day_of_month     | INT          | NOT NULL   | Day of the month (1-31).                                                 |
| day_of_week      | INT          | NOT NULL   | ISO weekday number: 1 = Monday, 7 = Sunday (locale-safe).               |
| day_name         | NVARCHAR(20) | NOT NULL   | Full English weekday name (e.g., Monday, Friday).                        |
| day_name_short   | NVARCHAR(3)  | NOT NULL   | Three-letter abbreviation (e.g., Mon, Fri).                              |
| is_weekend       | BIT          | NOT NULL   | Convenience flag: 1 = Saturday or Sunday, 0 = weekday.                   |
| dwh_create_date  | DATETIME2    | NOT NULL   | Timestamp when this record was inserted into the gold layer.             |

---

### 5. **gold.fact_sales**

- **Purpose:** Central fact table storing transactional sales data at the order-item grain.
- **Source:** `silver.olist_ord_item` (grain driver), joined with `silver.olist_ord`, `silver.olist_ord_pay`, and `silver.olist_ord_rev`.
- **Grain:** One row per order item (`order_id` + `order_item_id`), enforced by a UNIQUE constraint.

| Column Name         | Data Type     | Constraint | Description                                                              |
|---------------------|---------------|------------|--------------------------------------------------------------------------|
| sale_sk             | INT           | PK         | Surrogate key uniquely identifying each fact row.                        |
| order_id            | VARCHAR(50)   | NOT NULL   | Degenerate dimension: original order hash from the source system.        |
| order_item_id       | INT           | NOT NULL   | Degenerate dimension: sequential item number within the order (1-based). |
| order_status        | NVARCHAR(20)  | NULL       | Order lifecycle status (e.g., delivered, shipped, canceled).             |
| customer_sk         | INT           | FK         | Foreign key to `dim_customers`; identifies the purchasing customer.      |
| product_sk          | INT           | FK         | Foreign key to `dim_products`; identifies the purchased product.         |
| seller_sk           | INT           | FK         | Foreign key to `dim_sellers`; identifies the fulfilling seller.          |
| purchase_date_sk    | INT           | FK         | Foreign key to `dim_date`; date the order was placed.                    |
| price               | DECIMAL(18,2) | NULL       | Selling price of this specific order item.                               |
| freight_value       | DECIMAL(18,2) | NULL       | Freight / shipping cost attributed to this item.                         |
| total_payment_value | DECIMAL(18,2) | NULL       | Total amount paid for the entire order across all payment methods. **Note:** order-level metric at item grain — use `MAX()` when aggregating across items. |
| review_score        | INT           | NULL       | Customer satisfaction rating (1-5). Deduplicated to one review per order. |
| is_late             | BIT           | NULL       | Delivery flag: 1 = late, 0 = on time or early.                          |
| delivery_lead_time  | INT           | NULL       | Days between purchase and delivery. NULL if not yet delivered or if the date was healed due to chronological impossibility. |
| dwh_create_date     | DATETIME2     | NOT NULL   | Timestamp when this record was inserted into the gold layer.             |

---

## Technical Highlights

### Centralized Logging

ELT operations are tracked in a **dedicated `logging` schema**, keeping operational metadata completely separate from business data.

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
- **ELT:** T-SQL Stored Procedures
- **Architecture:** Medallion (Bronze → Silver → Gold)
- **Data Model:** Star Schema (Kimball methodology)
- **Visualization:** Power BI (via Gold presentation views)
