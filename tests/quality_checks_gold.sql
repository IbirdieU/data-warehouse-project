/*
===============================================================================
Data Quality Check Script : Gold Layer (Star Schema)
Database : RetailWarehouse
Schema   : gold
===============================================================================
Purpose:
    Validates the integrity of the Star Schema after each gold layer load.
    Covers five categories:
        1. Entity Integrity       — SK uniqueness & nulls across all dim tables
        2. Fact Integrity         — PK & grain uniqueness on fact_sales
        3. Referential Integrity  — orphaned FK values in fact_sales
        4. Value Domain & Logic   — business rule validation on measures
        5. Reconciliation         — row count & financial totals vs. silver source

Status Legend:
    PASS    — check passed, no issues found
    FAIL    — hard failure, data defect that must be investigated
    WARNING — soft warning, expected divergence (e.g. INNER JOIN exclusions)

Usage:
    Run after EXEC gold.load_gold to validate the full load.
    Results are ordered: FAIL first → WARNING → PASS.
===============================================================================
*/

USE RetailWarehouse;
GO

WITH DQ_Report AS (

    -- ====================================================================
    -- 1. Entity Integrity: gold.dim_customers
    -- ====================================================================

    -- Check 1.1: SK Completeness (customer_sk must never be NULL — IDENTITY)
    SELECT
        'gold.dim_customers'            AS TableName,
        'Completeness - SK customer_sk' AS CheckName,
        SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS FailedCount,
        CASE WHEN SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END AS Status,
        'Surrogate key customer_sk contains NULL values'      AS ErrorMsg
    FROM gold.dim_customers

    UNION ALL

    -- Check 1.2: SK Uniqueness (customer_sk must be unique)
    SELECT 'gold.dim_customers', 'Uniqueness - SK customer_sk',
        COUNT(*) - COUNT(DISTINCT customer_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT customer_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate surrogate keys found in customer_sk'
    FROM gold.dim_customers

    UNION ALL

    -- Check 1.3: NK Completeness (customer_id must not be NULL)
    SELECT 'gold.dim_customers', 'Completeness - NK customer_id',
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Natural key customer_id contains NULL values'
    FROM gold.dim_customers

    UNION ALL

    -- ====================================================================
    -- 2. Entity Integrity: gold.dim_products
    -- ====================================================================

    -- Check 2.1: SK Completeness
    SELECT 'gold.dim_products', 'Completeness - SK product_sk',
        SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN product_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Surrogate key product_sk contains NULL values'
    FROM gold.dim_products

    UNION ALL

    -- Check 2.2: SK Uniqueness
    SELECT 'gold.dim_products', 'Uniqueness - SK product_sk',
        COUNT(*) - COUNT(DISTINCT product_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT product_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate surrogate keys found in product_sk'
    FROM gold.dim_products

    UNION ALL

    -- Check 2.3: NK Completeness
    SELECT 'gold.dim_products', 'Completeness - NK product_id',
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Natural key product_id contains NULL values'
    FROM gold.dim_products

    UNION ALL

    -- Check 2.4: Validity - English category name should never be NULL
    -- (proc_load_gold uses ISNULL(pcm_cat_name_en, 'Unknown') so NULL here means mapping logic broke)
    SELECT 'gold.dim_products', 'Completeness - category_name_en',
        SUM(CASE WHEN category_name_en IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN category_name_en IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'category_name_en is NULL — ISNULL fallback to Unknown may have failed'
    FROM gold.dim_products

    UNION ALL

    -- ====================================================================
    -- 3. Entity Integrity: gold.dim_sellers
    -- ====================================================================

    -- Check 3.1: SK Completeness
    SELECT 'gold.dim_sellers', 'Completeness - SK seller_sk',
        SUM(CASE WHEN seller_sk IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN seller_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Surrogate key seller_sk contains NULL values'
    FROM gold.dim_sellers

    UNION ALL

    -- Check 3.2: SK Uniqueness
    SELECT 'gold.dim_sellers', 'Uniqueness - SK seller_sk',
        COUNT(*) - COUNT(DISTINCT seller_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT seller_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate surrogate keys found in seller_sk'
    FROM gold.dim_sellers

    UNION ALL

    -- Check 3.3: NK Completeness
    SELECT 'gold.dim_sellers', 'Completeness - NK seller_id',
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Natural key seller_id contains NULL values'
    FROM gold.dim_sellers

    UNION ALL

    -- ====================================================================
    -- 4. Entity Integrity: gold.dim_date
    -- ====================================================================

    -- Check 4.1: SK Completeness
    SELECT 'gold.dim_date', 'Completeness - SK date_sk',
        SUM(CASE WHEN date_sk IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN date_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Surrogate key date_sk contains NULL values'
    FROM gold.dim_date

    UNION ALL

    -- Check 4.2: SK Uniqueness
    SELECT 'gold.dim_date', 'Uniqueness - SK date_sk',
        COUNT(*) - COUNT(DISTINCT date_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT date_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate surrogate keys found in date_sk'
    FROM gold.dim_date

    UNION ALL

    -- Check 4.3: Expected row count (2016-01-01 to 2020-12-31 = 1827 days)
    SELECT 'gold.dim_date', 'Completeness - Expected Row Count (1827)',
        ABS(COUNT(*) - 1827),
        CASE WHEN COUNT(*) <> 1827 THEN 'FAIL' ELSE 'PASS' END,
        'dim_date does not contain the expected 1827 days (2016-01-01 to 2020-12-31)'
    FROM gold.dim_date

    UNION ALL

    -- ====================================================================
    -- 5. Fact Integrity: gold.fact_sales
    -- ====================================================================

    -- Check 5.1: PK Completeness (sale_sk must never be NULL — IDENTITY)
    SELECT 'gold.fact_sales', 'Completeness - PK sale_sk',
        SUM(CASE WHEN sale_sk IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN sale_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key sale_sk contains NULL values'
    FROM gold.fact_sales

    UNION ALL

    -- Check 5.2: PK Uniqueness (sale_sk must be unique)
    SELECT 'gold.fact_sales', 'Uniqueness - PK sale_sk',
        COUNT(*) - COUNT(DISTINCT sale_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT sale_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found in sale_sk'
    FROM gold.fact_sales

    UNION ALL

    -- Check 5.3: Grain Uniqueness (one row per order item: order_id + order_item_id)
    SELECT 'gold.fact_sales', 'Uniqueness - Grain (order_id, order_item_id)',
        COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '||', CAST(order_item_id AS VARCHAR(10)))),
        CASE WHEN COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '||', CAST(order_item_id AS VARCHAR(10)))) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Grain violated: duplicate (order_id, order_item_id) combinations found'
    FROM gold.fact_sales

    UNION ALL

    -- ====================================================================
    -- 6. Referential Integrity: Orphaned FK values in gold.fact_sales
    -- ====================================================================

    -- Check 6.1: Orphaned customer_sk
    SELECT 'gold.fact_sales', 'Referential Integrity - customer_sk',
        COUNT(f.sale_sk),
        CASE WHEN COUNT(f.sale_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'fact_sales rows with customer_sk not found in dim_customers'
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers dc ON f.customer_sk = dc.customer_sk
    WHERE dc.customer_sk IS NULL

    UNION ALL

    -- Check 6.2: Orphaned product_sk
    SELECT 'gold.fact_sales', 'Referential Integrity - product_sk',
        COUNT(f.sale_sk),
        CASE WHEN COUNT(f.sale_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'fact_sales rows with product_sk not found in dim_products'
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products dp ON f.product_sk = dp.product_sk
    WHERE dp.product_sk IS NULL

    UNION ALL

    -- Check 6.3: Orphaned seller_sk
    SELECT 'gold.fact_sales', 'Referential Integrity - seller_sk',
        COUNT(f.sale_sk),
        CASE WHEN COUNT(f.sale_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'fact_sales rows with seller_sk not found in dim_sellers'
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_sellers ds ON f.seller_sk = ds.seller_sk
    WHERE ds.seller_sk IS NULL

    UNION ALL

    -- Check 6.4: Orphaned purchase_date_sk
    SELECT 'gold.fact_sales', 'Referential Integrity - purchase_date_sk',
        COUNT(f.sale_sk),
        CASE WHEN COUNT(f.sale_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'fact_sales rows with purchase_date_sk not found in dim_date'
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_date dd ON f.purchase_date_sk = dd.date_sk
    WHERE dd.date_sk IS NULL

    UNION ALL

    -- ====================================================================
    -- 7. Value Domain & Logic Checks: gold.fact_sales
    -- ====================================================================

    -- Check 7.1: Validity - review_score must be between 1 and 5 (NULL is allowed)
    SELECT 'gold.fact_sales', 'Validity - review_score Range (1-5)',
        SUM(CASE WHEN review_score IS NOT NULL AND (review_score < 1 OR review_score > 5) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN review_score IS NOT NULL AND (review_score < 1 OR review_score > 5) THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'review_score is outside the valid range of 1 to 5'
    FROM gold.fact_sales

    UNION ALL

    -- Check 7.2: Validity - price must not be negative
    SELECT 'gold.fact_sales', 'Validity - price Not Negative',
        SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Negative price values found in fact_sales'
    FROM gold.fact_sales

    UNION ALL

    -- Check 7.3: Validity - freight_value must not be negative
    SELECT 'gold.fact_sales', 'Validity - freight_value Not Negative',
        SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Negative freight_value found in fact_sales'
    FROM gold.fact_sales

    UNION ALL

    -- Check 7.4: Completeness - order_status must not be NULL or empty
    SELECT 'gold.fact_sales', 'Completeness - order_status Not NULL or Empty',
        SUM(CASE WHEN order_status IS NULL OR LEN(TRIM(order_status)) = 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN order_status IS NULL OR LEN(TRIM(order_status)) = 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'order_status contains NULL or empty values'
    FROM gold.fact_sales

    UNION ALL

    -- Check 7.5: Validity - total_payment_value must not be negative
    SELECT 'gold.fact_sales', 'Validity - total_payment_value Not Negative',
        SUM(CASE WHEN total_payment_value < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN total_payment_value < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Negative total_payment_value found in fact_sales'
    FROM gold.fact_sales

    UNION ALL

    -- ====================================================================
    -- 8. Data Volume & Financial Reconciliation (Gold vs. Silver)
    -- ====================================================================

    -- Check 8.1: Row Count Reconciliation
    -- fact_sales uses INNER JOINs on all dimensions — orders with no matching
    -- customer, product, seller, or date are excluded. A difference is expected
    -- for dirty source data but should be monitored as a WARNING, not a hard FAIL.
    SELECT
        'gold vs silver', 'Reconciliation - Row Count (fact_sales vs olist_ord_item)',
        ABS(
            (SELECT COUNT(*) FROM gold.fact_sales) -
            (SELECT COUNT(*) FROM silver.olist_ord_item)
        ),
        CASE WHEN ABS(
                (SELECT COUNT(*) FROM gold.fact_sales) -
                (SELECT COUNT(*) FROM silver.olist_ord_item)
             ) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Row count differs between gold.fact_sales and silver.olist_ord_item — review INNER JOIN exclusions'
    FROM (SELECT 1 AS x) t

    UNION ALL

    -- Check 8.2: Financial Reconciliation — SUM(price) must match SUM(oi_price)
    -- A difference here indicates rows were dropped or prices were mutated during load.
    -- Unlike the row count, a price mismatch cannot be explained by JOIN exclusions
    -- alone and should be treated as a hard FAIL.
    SELECT
        'gold vs silver', 'Reconciliation - SUM(price) vs SUM(oi_price)',
        CAST(ABS(
            ISNULL((SELECT SUM(price)    FROM gold.fact_sales),       0) -
            ISNULL((SELECT SUM(oi_price) FROM silver.olist_ord_item), 0)
        ) AS INT),
        CASE WHEN ABS(
                ISNULL((SELECT SUM(price)    FROM gold.fact_sales),       0) -
                ISNULL((SELECT SUM(oi_price) FROM silver.olist_ord_item), 0)
             ) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Total price sum differs between gold.fact_sales and silver.olist_ord_item'
    FROM (SELECT 1 AS x) t

    UNION ALL

    -- ====================================================================
    -- 9. Advanced Integrity: Date Continuity (dim_date)
    -- Execution order matters here:
    --   9.1 checks full_date UNIQUENESS first.
    --   9.2 checks CONTINUITY second.
    -- Reason: the continuity formula (DATEDIFF + 1) - COUNT(*) returns 0
    -- whenever duplicates exactly cancel out missing days, producing a false
    -- PASS. Uniqueness must be confirmed before continuity is meaningful.
    -- ====================================================================

    -- Check 9.1: full_date Uniqueness (prerequisite for continuity check)
    SELECT
        'gold.dim_date', 'Uniqueness - full_date (Continuity Prerequisite)',
        COUNT(*) - COUNT(DISTINCT full_date),
        CASE WHEN COUNT(*) - COUNT(DISTINCT full_date) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate full_date values found — continuity check (9.2) result is unreliable until resolved'
    FROM gold.dim_date

    UNION ALL

    -- Check 9.2: No Calendar Gaps (valid only when Check 9.1 = PASS)
    -- DATEDIFF from MIN to MAX + 1 must equal COUNT(*).
    -- With uniqueness guaranteed, any difference is a true gap in the calendar.
    -- FailedCount = exact number of missing days.
    SELECT
        'gold.dim_date', 'Continuity - No Calendar Gaps',
        (DATEDIFF(DAY, MIN(full_date), MAX(full_date)) + 1) - COUNT(*),
        CASE WHEN (DATEDIFF(DAY, MIN(full_date), MAX(full_date)) + 1) <> COUNT(*) THEN 'FAIL' ELSE 'PASS' END,
        'Calendar gaps detected — recursive CTE may have skipped one or more days'
    FROM gold.dim_date

    UNION ALL

    -- ====================================================================
    -- 10. Advanced Integrity: Natural Key Uniqueness (All Dimensions)
    -- Duplicate NKs cause Data Fan-out: one fact row joins to multiple dim
    -- rows, multiplying financial totals silently in every BI report.
    -- ====================================================================

    -- Check 10.1: customer_id must be unique in dim_customers
    SELECT 'gold.dim_customers', 'Uniqueness - NK customer_id (Fan-out Guard)',
        COUNT(*) - COUNT(DISTINCT customer_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT customer_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate customer_id found — fact joins will fan-out and inflate financial totals'
    FROM gold.dim_customers

    UNION ALL

    -- Check 10.2: product_id must be unique in dim_products
    SELECT 'gold.dim_products', 'Uniqueness - NK product_id (Fan-out Guard)',
        COUNT(*) - COUNT(DISTINCT product_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT product_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate product_id found — fact joins will fan-out and inflate financial totals'
    FROM gold.dim_products

    UNION ALL

    -- Check 10.3: seller_id must be unique in dim_sellers
    SELECT 'gold.dim_sellers', 'Uniqueness - NK seller_id (Fan-out Guard)',
        COUNT(*) - COUNT(DISTINCT seller_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT seller_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate seller_id found — fact joins will fan-out and inflate financial totals'
    FROM gold.dim_sellers

    UNION ALL

    -- ====================================================================
    -- 11. Advanced Integrity: Data Freshness (Latency Check)
    -- ====================================================================

    -- Check 11.1: fact_sales must have been loaded within the last 24 hours
    -- FailedCount = hours elapsed since the most recent load
    -- Status      = WARNING (not FAIL) because stale data is an ops concern,
    --               not a structural data defect
    SELECT
        'gold.fact_sales', 'Freshness - Data Latency (< 24 hours)',
        DATEDIFF(HOUR, MAX(dwh_create_date), GETDATE()),
        CASE WHEN MAX(dwh_create_date) < DATEADD(HOUR, -24, GETDATE()) THEN 'WARNING' ELSE 'PASS' END,
        'Most recent load in fact_sales is older than 24 hours — ETL pipeline may not have run'
    FROM gold.fact_sales

    UNION ALL

    -- ====================================================================
    -- 12. Advanced Integrity: Future Date Sanity (fact_sales)
    -- ====================================================================

    -- Check 12.1: No purchase dates in the future
    -- Joins fact to dim_date and compares full_date against today.
    -- A future date indicates a source system clock error or bad data entry.
    SELECT
        'gold.fact_sales', 'Validity - No Future purchase_date_sk',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'fact_sales contains purchase_date_sk values that map to a future calendar date'
    FROM gold.fact_sales f
    INNER JOIN gold.dim_date dd ON f.purchase_date_sk = dd.date_sk
    WHERE dd.full_date > CAST(GETDATE() AS DATE)

)

SELECT * FROM DQ_Report
ORDER BY
    CASE Status
        WHEN 'FAIL'    THEN 1   -- Hard failures first
        WHEN 'WARNING' THEN 2   -- Soft warnings second
        ELSE                3   -- PASS last
    END,
    TableName,
    CheckName;
