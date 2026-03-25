/*
===============================================================================
Stored Procedure : Load Gold Layer (Silver -> Gold)
Database         : RetailWarehouse
Schema           : gold
===============================================================================
Purpose:
    Performs the full ETL load from the 'silver' schema into the 'gold' Star
    Schema. Each execution is a complete refresh: all gold tables are cleared
    and reloaded from silver in a single atomic transaction.

Execution Order (respects FK dependency chain):
    1. Clear   : DELETE fact_sales → DELETE dim_* → RESEED identities
    2. Dims     : dim_customers → dim_products → dim_sellers
                  (dim_date is static; populated once at DDL time — skipped here)
    3. Fact     : fact_sales  (requires all dims to be loaded first for SK lookups)

Transaction Strategy:
    All inserts are wrapped in a single BEGIN TRANSACTION / COMMIT block.
    Any failure triggers a full ROLLBACK, leaving the gold layer untouched.

FK Constraint Note:
    TRUNCATE is blocked on tables referenced by FK constraints (SQL Server rule).
    Solution: DELETE the fact table first (nothing references it), then DELETE
    the dimension tables (now safe), then RESEED IDENTITY counters back to 0.

Logging:
    Each table load is recorded in logging.load_log with INSERT (RUNNING) → UPDATE (SUCCESS).
    A FAILED record is written at procedure level if the transaction is rolled back.

Usage:
    EXEC gold.load_gold
===============================================================================
*/

USE RetailWarehouse;
GO

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    BEGIN TRY
    SET NOCOUNT ON;
    SET XACT_ABORT ON; --Auto Rollback
    DECLARE @start_time       DATETIME,
            @end_time         DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time   DATETIME;
    DECLARE @rows_inserted    INT,
            @log_id           INT;

    SET @batch_start_time = GETDATE();

    PRINT '================================================';
    PRINT 'Loading Gold Layer...';
    PRINT '================================================';

    -- =========================================================================
    -- Begin single atomic transaction for the entire Gold refresh
    -- =========================================================================
    BEGIN TRANSACTION;

    -- =========================================================================
    -- STEP 0: Clear Gold Layer
    -- =========================================================================
    -- Fact first (it holds the FK references; nothing references the fact table).
    -- Then dimensions (safe once fact rows are gone).
    -- RESEED resets IDENTITY counters so surrogate keys start at 1 on next load.
    -- =========================================================================
    PRINT '================================================';
    PRINT 'Step 0: Clearing Gold Layer';
    PRINT '================================================';

    PRINT '>>>  Deleting Table: gold.fact_sales';
    DELETE FROM gold.fact_sales;
    DBCC CHECKIDENT ('gold.fact_sales',    RESEED, 0) WITH NO_INFOMSGS;

    PRINT '>>>  Deleting Table: gold.dim_customers';
    DELETE FROM gold.dim_customers;
    DBCC CHECKIDENT ('gold.dim_customers', RESEED, 0) WITH NO_INFOMSGS;

    PRINT '>>>  Deleting Table: gold.dim_products';
    DELETE FROM gold.dim_products;
    DBCC CHECKIDENT ('gold.dim_products',  RESEED, 0) WITH NO_INFOMSGS;

    PRINT '>>>  Deleting Table: gold.dim_sellers';
    DELETE FROM gold.dim_sellers;
    DBCC CHECKIDENT ('gold.dim_sellers',   RESEED, 0) WITH NO_INFOMSGS;

    PRINT '>>>  Gold layer cleared. Identities reseeded to 0.';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- DIMENSION 1: gold.dim_date
    -- Source  : Generated via recursive CTE (no silver source table)
    -- Range   : 2016-01-01 → 2020-12-31
    --
    -- dim_date is a static calendar table — its content never changes between
    -- ETL runs. The IF NOT EXISTS guard skips the insert on subsequent runs,
    -- making the procedure safe to re-execute without duplicating date rows.
    -- It is NOT deleted in the clear step above for the same reason.
    -- =========================================================================
    IF NOT EXISTS (SELECT 1 FROM gold.dim_date)
    BEGIN
        INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
        VALUES ('dim_date', 'generated', 'gold', 'RUNNING');
        SET @log_id = SCOPE_IDENTITY();
        SET @start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Step 1: Loading gold.dim_date';
        PRINT '================================================';
        PRINT '>>>  Inserting Data Into: gold.dim_date';

        WITH date_series AS (
            -- Anchor member: first date in range
            SELECT CAST('2016-01-01' AS DATE) AS d
            UNION ALL
            -- Recursive member: advance one day at a time until end of range
            SELECT DATEADD(DAY, 1, d)
            FROM   date_series
            WHERE  d < '2020-12-31'
        )
        INSERT INTO gold.dim_date (
            date_id,    full_date,
            calendar_year,
            quarter_number,    quarter_name,
            month_number,      month_name,     month_name_short,
            day_of_month,        day_of_week,    day_name,    day_name_short,
            is_weekend
        )
        SELECT
            -- NK: integer YYYYMMDD for fast filtering (e.g. 20180315)
            CAST(FORMAT(d, 'yyyyMMdd') AS INT)                          AS date_id,
            d                                                           AS full_date,

            YEAR(d)                                                     AS year,

            DATEPART(QUARTER, d)                                        AS quarter,
            N'Q' + CAST(DATEPART(QUARTER, d) AS NVARCHAR(1))           AS quarter_name,

            MONTH(d)                                                    AS month,
            DATENAME(MONTH, d)                                          AS month_name,
            LEFT(DATENAME(MONTH, d), 3)                                 AS month_name_short,

            DAY(d)                                                      AS day,

            -- ISO-safe weekday: 1=Mon … 7=Sun, independent of SET DATEFIRST locale
            (( DATEPART(WEEKDAY, d) + @@DATEFIRST - 2 ) % 7) + 1       AS day_of_week,
            DATENAME(WEEKDAY, d)                                        AS day_name,
            LEFT(DATENAME(WEEKDAY, d), 3)                               AS day_name_short,

            -- Weekend flag: 6=Sat, 7=Sun in ISO numbering above
            CASE
                WHEN (( DATEPART(WEEKDAY, d) + @@DATEFIRST - 2 ) % 7) + 1 IN (6, 7)
                THEN 1 ELSE 0
            END                                                         AS is_weekend

        FROM date_series
        OPTION (MAXRECURSION 2000); -- Override default 100 to cover multi-year range

        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time      = GETDATE();
        UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

        PRINT '>>>  Rows Inserted: '  + CAST(@rows_inserted AS VARCHAR(20));
        PRINT '>>>  Load Duration: '  + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
        PRINT '>>>  -------------';
    END
    ELSE
        PRINT '>>>  gold.dim_date already populated — skipped.';

    -- =========================================================================
    -- DIMENSION 2: gold.dim_customers
    -- Source  : silver.olist_cust
    -- Mapping : cst_cust_id        -> customer_id       (Natural Key)
    --           cst_cust_unique_id -> customer_unique_id
    --           cst_zip_code_prefix-> zip_code_prefix
    --           cst_city_std       -> city               (cleaned column, NOT cst_city_raw)
    --           cst_state          -> state
    -- SK      : customer_sk generated by IDENTITY(1,1) automatically
    -- =========================================================================
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('dim_customers', 'silver', 'gold', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Step 2: Loading gold.dim_customers';
    PRINT '================================================';
    PRINT '>>>  Inserting Data Into: gold.dim_customers';

    INSERT INTO gold.dim_customers (
        customer_id,
        customer_unique_id,
        zip_code_prefix,
        city,
        state
    )
    SELECT
        cst_cust_id,           -- NK: one entry per order in the Olist model
        cst_cust_unique_id,    -- Persistent buyer identifier across orders
        cst_zip_code_prefix,   -- Postal code prefix (5 chars)
        cst_city_std,          -- Standardised city name from silver (NOT cst_city_raw)
        cst_state              -- State abbreviation
    FROM silver.olist_cust;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time      = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: '  + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: '  + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- DIMENSION 3: gold.dim_products
    -- Source  : silver.olist_prd  LEFT JOIN  silver.olist_prd_cat_map
    -- Mapping : prd_prd_id        -> product_id         (Natural Key)
    --           prd_cat_name      -> category_name_pt   (Portuguese original)
    --           pcm_cat_name_en   -> category_name_en   (English translation)
    --           prd_name_len      -> name_length
    --           prd_desc_len      -> description_length
    --           prd_photos_qty    -> photos_quantity
    --           prd_weight_g      -> weight_g
    --           prd_len_cm        -> length_cm
    --           prd_height_cm     -> height_cm
    --           prd_width_cm      -> width_cm
    -- LEFT JOIN: preserves products with no category translation (NULL category_name_en)
    -- SK      : product_sk generated by IDENTITY(1,1) automatically
    -- =========================================================================
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('dim_products', 'silver', 'gold', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Step 3: Loading gold.dim_products';
    PRINT '================================================';
    PRINT '>>>  Inserting Data Into: gold.dim_products';

    INSERT INTO gold.dim_products (
        product_id,
        category_name_pt,
        category_name_en,
        name_length,
        description_length,
        photos_quantity,
        weight_g,
        length_cm,
        height_cm,
        width_cm
    )
    SELECT
        p.prd_prd_id,                                    -- NK: product identifier
        ISNULL(p.prd_cat_name,  'nao informado'),        -- Original Portuguese category
        ISNULL(m.pcm_cat_name_en, 'unknown'),            -- English translation; 'Unknown' if no mapping
        p.prd_name_len,                                  -- Character count of product name
        p.prd_desc_len,                                  -- Character count of product description
        p.prd_photos_qty,                                -- Number of listing photos
        p.prd_weight_g,                                  -- Weight in grams
        p.prd_len_cm,                                    -- Length in centimetres
        p.prd_height_cm,                                 -- Height in centimetres
        p.prd_width_cm                                   -- Width in centimetres
    FROM silver.olist_prd p
    LEFT JOIN silver.olist_prd_cat_map m
        ON p.prd_cat_name = m.pcm_cat_name;              -- Match Portuguese name to translation table

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time      = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: '  + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: '  + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- DIMENSION 4: gold.dim_sellers
    -- Source  : silver.olist_sel
    -- Mapping : sel_sel_id         -> seller_id         (Natural Key)
    --           sel_zip_code_prefix-> zip_code_prefix
    --           sel_city_std       -> city               (cleaned column, NOT sel_city_raw)
    --           sel_state          -> state
    -- SK      : seller_sk generated by IDENTITY(1,1) automatically
    -- =========================================================================
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('dim_sellers', 'silver', 'gold', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Step 4: Loading gold.dim_sellers';
    PRINT '================================================';
    PRINT '>>>  Inserting Data Into: gold.dim_sellers';

    INSERT INTO gold.dim_sellers (
        seller_id,
        zip_code_prefix,
        city,
        state
    )
    SELECT
        sel_sel_id,            -- NK: seller identifier
        sel_zip_code_prefix,   -- Postal code prefix
        sel_city_std,          -- Standardised city name (NOT sel_city_raw)
        sel_state              -- State abbreviation
    FROM silver.olist_sel;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time      = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: '  + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: '  + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- FACT TABLE: gold.fact_sales
    -- Grain   : one row per order item  (order_id + order_item_id)
    -- Source  : silver.olist_ord_item   (grain driver)
    -- Joins   :
    --   INNER  silver.olist_ord          → order header (status, dates, is_late)
    --   INNER  gold.dim_customers        → resolve customer_sk via customer_id = ord_cust_id
    --   INNER  gold.dim_products         → resolve product_sk  via product_id  = oi_prd_id
    --   INNER  gold.dim_sellers          → resolve seller_sk   via seller_id   = oi_sel_id
    --   INNER  gold.dim_date             → resolve purchase_date_sk via YYYYMMDD date_id
    --   LEFT   payment subquery          → order-level total via GROUP BY + SUM
    --   LEFT   review subquery           → latest review score per order via ROW_NUMBER
    --
    -- Join type rationale:
    --   INNER on all dimensions: acts as a data quality gate — any order item
    --   missing a matching customer, product, seller, or date is excluded.
    --   Since dims are sourced from the same silver tables, mismatches indicate
    --   upstream data issues that should be investigated, not silently kept.
    --   LEFT on payments & reviews: not every order has a payment record or
    --   review — LEFT JOIN preserves the item row with NULL measures.
    --
    -- Payment logic:
    --   silver.olist_ord_pay has one row per payment instrument per order
    --   (e.g., credit card + voucher = 2 rows). GROUP BY op_ord_id with
    --   SUM(op_pay_val) collapses all payment rows into one row per order,
    --   which LEFT JOINs cleanly to the item grain.
    --
    -- Review logic:
    --   silver.olist_ord_rev stores ALL source records (surrogate key rev_sk prevents
    --   PK violations from duplicate or_rev_id values in the source).
    --   ROW_NUMBER PARTITION BY or_ord_id enforces one review per order at the gold
    --   grain level, ordered by or_rev_create_dt DESC.
    -- =========================================================================
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('fact_sales', 'silver', 'gold', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Step 5: Loading gold.fact_sales';
    PRINT '================================================';
    PRINT '>>>  Inserting Data Into: gold.fact_sales';

    INSERT INTO gold.fact_sales (
        order_id,
        order_item_id,
        order_status,
        customer_sk,
        product_sk,
        seller_sk,
        purchase_date_sk,
        price,
        freight_value,
        total_payment_value,
        review_score,
        is_late,
        delivery_lead_time
    )
    SELECT
        -- Degenerate dimension keys (traceability back to silver)
        oi.oi_ord_id                AS order_id,
        oi.oi_ord_item_id           AS order_item_id,
        o.ord_status                AS order_status,

        -- Surrogate key lookups (resolved from freshly loaded dim tables above)
        dc.customer_sk,             -- Resolved via: dim_customers.customer_id = ord_cust_id
        dp.product_sk,              -- Resolved via: dim_products.product_id   = oi_prd_id
        ds.seller_sk,               -- Resolved via: dim_sellers.seller_id     = oi_sel_id
        dd.date_sk                  AS purchase_date_sk, -- Resolved via: dim_date.date_id = YYYYMMDD(ord_purchase_ts)

        -- Financial measures
        oi.oi_price                 AS price,
        oi.oi_freight_val           AS freight_value,
        pay.total_payment_value,    -- Order-level total stamped on every item row (see payment CTE below)

        -- Satisfaction measure
        rev.or_rev_score            AS review_score,

        -- Delivery performance flag (derived in silver layer: 1=late, 0=on time)
        o.ord_is_late               AS is_late,

        -- Delivery lead time (days between purchase and customer delivery; NULL if not delivered)
        o.delivery_lead_time

    FROM silver.olist_ord_item oi

    -- Order header: provides customer reference, timestamps, status, and is_late flag
    INNER JOIN silver.olist_ord o
        ON o.ord_ord_id = oi.oi_ord_id

    -- Dimension SK lookups — INNER JOIN acts as a data quality gate
    INNER JOIN gold.dim_customers dc
        ON dc.customer_id = o.ord_cust_id

    INNER JOIN gold.dim_products dp
        ON dp.product_id = oi.oi_prd_id

    INNER JOIN gold.dim_sellers ds
        ON ds.seller_id = oi.oi_sel_id

    -- Date SK: convert purchase timestamp to YYYYMMDD integer to match dim_date.date_id
    INNER JOIN gold.dim_date dd
        ON dd.date_id = CAST(FORMAT(o.ord_purchase_ts, 'yyyyMMdd') AS INT)

    -- Payment aggregation:
    --   GROUP BY op_ord_id collapses all payment methods into one row per order,
    --   SUM(op_pay_val) computes the order-level total across all payment methods.
    LEFT JOIN (
        SELECT
            op_ord_id,
            SUM(op_pay_val) AS total_payment_value
        FROM silver.olist_ord_pay
        GROUP BY op_ord_id
    ) pay
        ON pay.op_ord_id = oi.oi_ord_id

    -- Review: silver stores all records (full audit trail via surrogate key rev_sk).
    -- ROW_NUMBER picks the latest review per order — grain enforcement belongs in gold.
    LEFT JOIN (
        SELECT
            or_ord_id,
            or_rev_score,
            ROW_NUMBER() OVER (
                PARTITION BY or_ord_id
                ORDER BY or_rev_create_dt DESC,  -- Most recently created review first
                         or_rev_ans_ts   DESC   -- Tie-break: latest response timestamp
            ) AS rn
        FROM silver.olist_ord_rev
    ) rev
        ON  rev.or_ord_id = oi.oi_ord_id
        AND rev.rn        = 1;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time      = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: '  + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: '  + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- Commit the full transaction — all five tables loaded successfully
    -- =========================================================================
    COMMIT TRANSACTION;

    SET @batch_end_time = GETDATE();
    PRINT '=========================================='
    PRINT 'Loading Gold Layer is Completed';
    PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(50)) + ' seconds';
    PRINT '=========================================='

    END TRY

    -- =========================================================================
    -- Error handling
    -- =========================================================================
    BEGIN CATCH

        -- Roll back everything if any single statement failed
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING GOLD LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: '  + CAST(ERROR_NUMBER() AS VARCHAR(50));
        PRINT 'Error State: '   + CAST(ERROR_STATE()  AS VARCHAR(50));
        PRINT '=========================================='

        -- Log the failure at procedure level so the broken batch is traceable
        INSERT INTO logging.load_log (process_name, source_layer, target_layer, status, end_ts, rows_inserted, error_message)
        VALUES ('GOLD_PROCEDURE', 'silver', 'gold', 'FAILED', GETDATE(), 0, ERROR_MESSAGE());

    END CATCH
END
GO
