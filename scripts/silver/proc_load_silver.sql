/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
        	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleaned data from Bronze into Silver tables
===============================================================================
Usage Example :
    EXEC silver.load_silver
===============================================================================
*/
USE RetailWarehouse;
GO 

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
    SET NOCOUNT ON;
    SET XACT_ABORT ON; --Auto Rollback
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @rows_inserted INT, @log_id INT;
    SET @batch_start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Loading Silver Layer...';
    PRINT '================================================';

    -- =========================================================================
    -- Begin single atomic transaction for the entire Silver refresh
    -- =========================================================================
    BEGIN TRANSACTION;

    -- ========== TABLE 1: silver.olist_geo ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_geo', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_geo';
	TRUNCATE TABLE silver.olist_geo;
	PRINT '>>>  Inserting Data Into: silver.olist_geo';
    
    WITH CTE_GeoCoordinates AS (
        -- Calculate the average coordinates per Zip Code
        SELECT 
            CAST(geo_zip_code_prefix AS CHAR(5)) AS geo_zip_code_prefix,
            CAST(AVG(CAST(geo_lat AS DECIMAL(18,15))) AS DECIMAL(18,15)) as avg_lat,
            CAST(AVG(CAST(geo_lng AS DECIMAL(18,15))) AS DECIMAL(18,15)) as avg_lng
        FROM bronze.olist_geo
        GROUP BY geo_zip_code_prefix
    ),
    CTE_CityWinner AS (
        -- Find the most frequent name per Zip Code
        SELECT 
            CAST(geo_zip_code_prefix AS CHAR(5)) AS geo_zip_code_prefix,
            NULLIF(TRIM(geo_city), '') AS geo_city,
            CAST(geo_state AS CHAR(2)) AS geo_state
        FROM (
            SELECT 
                geo_zip_code_prefix,
                geo_city,
                geo_state,
                ROW_NUMBER() OVER (
                    PARTITION BY geo_zip_code_prefix 
                    ORDER BY COUNT(*) DESC
                ) as rn
            FROM bronze.olist_geo
            GROUP BY geo_zip_code_prefix, geo_city, geo_state
        ) t
        WHERE rn = 1
    )

    INSERT INTO silver.olist_geo (geo_zip_code_prefix, geo_lat, geo_lng, geo_city, geo_state)
    SELECT 
        coord.geo_zip_code_prefix,
        coord.avg_lat,
        coord.avg_lng,
        -- Standardize to English (Remove Accents) + Ghost String Cleanup
        CAST(TRANSLATE(NULLIF(TRIM(winner.geo_city), ''), 
            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 
            'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC') AS VARCHAR(100)) as geo_city,
        winner.geo_state
    FROM CTE_GeoCoordinates coord
    INNER JOIN CTE_CityWinner winner ON coord.geo_zip_code_prefix = winner.geo_zip_code_prefix;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 2: silver.olist_cust ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_cust', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
	PRINT '>>>  Truncating Table: silver.olist_cust';
	TRUNCATE TABLE silver.olist_cust;
	PRINT '>>>  Inserting Data Into: silver.olist_cust';

    INSERT INTO silver.olist_cust (cst_cust_id, cst_cust_unique_id, cst_zip_code_prefix, cst_city_raw, cst_city_std, cst_state)
    SELECT 
        -- Fail-Fast: Explicit CAST to VARCHAR(50) for all IDs
        CAST(bc.cst_cust_id AS VARCHAR(50)),
        CAST(bc.cst_cust_unique_id AS VARCHAR(50)),
        -- Fixed-Length: Zip codes as CHAR(5)
        CAST(bc.cst_zip_code_prefix AS CHAR(5)),
        -- Ghost Strings: NULLIF + TRIM on raw city name
        CAST(NULLIF(TRIM(bc.cst_city), '') AS VARCHAR(100)) AS cst_city_raw,
        -- Standardization Logic: Use geo as source of truth, fallback to original
        CAST(NULLIF(TRIM(COALESCE(sg.geo_city, bc.cst_city)), '') AS VARCHAR(100)) AS cst_city_std,
        -- Fixed-Length: States as CHAR(2)
        CAST(COALESCE(sg.geo_state, bc.cst_state) AS CHAR(2)) AS cst_state

    FROM bronze.olist_cust bc
    LEFT JOIN silver.olist_geo sg ON CAST(bc.cst_zip_code_prefix AS CHAR(5)) = sg.geo_zip_code_prefix;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 3: silver.olist_ord_item ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_ord_item', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord_item';
	TRUNCATE TABLE silver.olist_ord_item;
	PRINT '>>>  Inserting Data Into: silver.olist_ord_item';
    
    INSERT INTO silver.olist_ord_item (oi_ord_id, oi_ord_item_id, oi_prd_id, oi_sel_id, oi_ship_limit_dt, oi_price, oi_freight_val)
    SELECT 
        -- Fail-Fast: All IDs as VARCHAR(50)(Except order item id as INT)
        CAST(oi_ord_id AS VARCHAR(50)), 
        CAST(oi_ord_item_id AS INT), 
        CAST(oi_prd_id AS VARCHAR(50)), 
        CAST(oi_sel_id AS VARCHAR(50)), 
        -- Safe Measures: CAST for datetime
        CAST(oi_ship_limit_dt AS DATETIME), 
        -- Safe Measures: CAST for numeric values
        CAST(oi_price AS DECIMAL(18,2)), 
        CAST(oi_freight_val AS DECIMAL(18,2)) 
    FROM bronze.olist_ord_item;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 4: silver.olist_ord_pay ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_ord_pay', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord_pay';
	TRUNCATE TABLE silver.olist_ord_pay;
	PRINT '>>>  Inserting Data Into: silver.olist_ord_pay';
    
    INSERT INTO silver.olist_ord_pay (op_ord_id, op_pay_seq, op_pay_type, op_pay_inst, op_pay_val)
    SELECT 
        -- Fail-Fast: IDs as VARCHAR(50)
        CAST(op_ord_id AS VARCHAR(50)), 
        CAST(op_pay_seq AS INT),
        -- Ghost Strings: Text cleanup
        CAST(NULLIF(TRIM(op_pay_type), '') AS VARCHAR(100)), 
        -- Safe Measures: Numeric cast
        CAST(op_pay_inst AS INT), 
        CAST(op_pay_val AS DECIMAL(18,2)) 
    FROM bronze.olist_ord_pay
    -- Validation: Filter out unrecognized payment types
    WHERE NULLIF(TRIM(op_pay_type), '') IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined');

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 5: silver.olist_ord_rev ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_ord_rev', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord_rev';
	TRUNCATE TABLE silver.olist_ord_rev;
	PRINT '>>>  Inserting Data Into: silver.olist_ord_rev';

    INSERT INTO silver.olist_ord_rev (or_rev_id, or_ord_id, or_rev_score, or_rev_cmt_title, or_rev_cmt_msg, or_rev_create_dt, or_rev_ans_ts)
    SELECT
        -- Fail-Fast: IDs as VARCHAR(50)
        CAST(or_rev_id AS VARCHAR(50)),
        CAST(or_ord_id AS VARCHAR(50)),
        -- Safe Measures: Numeric cast
        CAST(or_rev_score AS INT),
        -- Ghost Strings: Text cleanup
        CAST(NULLIF(TRIM(or_rev_cmt_title), '') AS NVARCHAR(200)),
        CAST(NULLIF(TRIM(or_rev_cmt_msg),   '') AS NVARCHAR(MAX)),
        -- Safe Measures: DATETIME cast
        CAST(or_rev_create_dt AS DATETIME),
        CAST(or_rev_ans_ts    AS DATETIME)
    FROM bronze.olist_ord_rev;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 6: silver.olist_ord ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_ord', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord';
	TRUNCATE TABLE silver.olist_ord;
	PRINT '>>>  Inserting Data Into: silver.olist_ord';
    
    -- CTE: cast all dates once, then apply cleansing rules on top
    WITH ord_casted AS (
        SELECT
            CAST(ord_ord_id      AS VARCHAR(50))   AS ord_ord_id,
            CAST(ord_cust_id     AS VARCHAR(50))   AS ord_cust_id,
            CAST(NULLIF(TRIM(ord_status), '') AS NVARCHAR(20)) AS ord_status,
            CAST(ord_purchase_ts    AS DATETIME)   AS ord_purchase_ts,
            CAST(ord_approved_ts    AS DATETIME)   AS ord_approved_ts,
            CAST(ord_del_carrier_dt AS DATETIME)   AS ord_del_carrier_dt,
            CAST(ord_del_cust_dt    AS DATETIME)   AS ord_del_cust_dt,
            CAST(ord_est_del_dt     AS DATETIME)   AS ord_est_del_dt
        FROM bronze.olist_ord
        WHERE NULLIF(TRIM(ord_status), '') IN (
            'delivered', 'shipped', 'unavailable', 'canceled',
            'invoiced', 'approved', 'processing', 'created'
        )
    ),
    -- Data Cleansing: NULL out logically impossible timestamps
    ord_cleansed AS (
        SELECT
            ord_ord_id,
            ord_cust_id,
            ord_status,
            ord_purchase_ts,
            ord_approved_ts,
            -- Cleansing: carrier pickup before approval is impossible → NULL
            CASE
                WHEN ord_del_carrier_dt < ord_approved_ts THEN NULL
                ELSE ord_del_carrier_dt
            END AS ord_del_carrier_dt,
            -- Cleansing: delivery before carrier pickup is impossible → NULL
            CASE
                WHEN ord_del_cust_dt < ord_del_carrier_dt THEN NULL
                ELSE ord_del_cust_dt
            END AS ord_del_cust_dt,
            ord_est_del_dt
        FROM ord_casted
    )
    INSERT INTO silver.olist_ord (ord_ord_id, ord_cust_id, ord_status, ord_purchase_ts, ord_approved_ts, ord_del_carrier_dt, ord_del_cust_dt, ord_est_del_dt, ord_is_late, delivery_lead_time)
    SELECT
        ord_ord_id,
        ord_cust_id,
        ord_status,
        ord_purchase_ts,
        ord_approved_ts,
        ord_del_carrier_dt,       -- Already cleansed above
        ord_del_cust_dt,          -- Already cleansed above
        ord_est_del_dt,
        -- Derived: ord_is_late (uses cleansed ord_del_cust_dt)
        CASE
            WHEN ord_del_cust_dt IS NULL THEN NULL
            WHEN ord_est_del_dt  IS NULL THEN NULL
            WHEN ord_est_del_dt < ord_del_cust_dt THEN 1
            ELSE 0
        END AS ord_is_late,
        -- Derived: delivery_lead_time in days (uses cleansed ord_del_cust_dt)
        CASE
            WHEN ord_purchase_ts IS NOT NULL AND ord_del_cust_dt IS NOT NULL
            THEN DATEDIFF(DAY, ord_purchase_ts, ord_del_cust_dt)
            ELSE NULL
        END AS delivery_lead_time
    FROM ord_cleansed;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 7: silver.olist_prd ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_prd', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_prd';
	TRUNCATE TABLE silver.olist_prd;
	PRINT '>>>  Inserting Data Into: silver.olist_prd';
    
    INSERT INTO silver.olist_prd (prd_prd_id, prd_cat_name, prd_name_len, prd_desc_len, prd_photos_qty, prd_weight_g, prd_len_cm, prd_height_cm, prd_width_cm)
    SELECT 
        -- Fail-Fast: IDs as VARCHAR(50)
        CAST(prd_prd_id AS VARCHAR(50)), 
        -- Ghost Strings: Category name cleanup
        CAST(NULLIF(TRIM(prd_cat_name), '') AS VARCHAR(100)), 
        -- Safe Measures: Numeric casts
        CAST(prd_name_len AS INT), 
        CAST(prd_desc_len AS INT), 
        CAST(prd_photos_qty AS INT), 
        CAST(prd_weight_g AS INT), 
        CAST(prd_len_cm AS INT), 
        CAST(prd_height_cm AS INT), 
        CAST(prd_width_cm AS INT) 
    FROM bronze.olist_prd;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 8: silver.olist_sel ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_sel', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_sel';
	TRUNCATE TABLE silver.olist_sel;
	PRINT '>>>  Inserting Data Into: silver.olist_sel';
    
    INSERT INTO silver.olist_sel (sel_sel_id, sel_zip_code_prefix, sel_city_raw, sel_city_std, sel_state)
    SELECT 
        -- Fail-Fast: IDs as VARCHAR(50)
        CAST(bs.sel_sel_id AS VARCHAR(50)),
        -- Fixed-Length: Zip codes as CHAR(5)
        CAST(bs.sel_zip_code_prefix AS CHAR(5)),
        -- Ghost Strings: Raw city cleanup
        CAST(NULLIF(TRIM(bs.sel_city), '') AS VARCHAR(100)),                          
        -- Standardization Logic: Use geo as source of truth, fallback to original
        CAST(NULLIF(TRIM(COALESCE(sg.geo_city, bs.sel_city)), '') AS VARCHAR(100)) AS sel_city_std,
        -- Fixed-Length: States as CHAR(2)
        CAST(COALESCE(sg.geo_state, bs.sel_state) AS CHAR(2)) AS sel_state

    FROM bronze.olist_sel bs
    LEFT JOIN silver.olist_geo sg ON CAST(bs.sel_zip_code_prefix AS CHAR(5)) = sg.geo_zip_code_prefix;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- ========== TABLE 9: silver.olist_prd_cat_map ==========
    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status)
    VALUES ('olist_prd_cat_map', 'bronze', 'silver', 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_prd_cat_map';
	TRUNCATE TABLE silver.olist_prd_cat_map;
	PRINT '>>>  Inserting Data Into: silver.olist_prd_cat_map';
    
    INSERT INTO silver.olist_prd_cat_map (pcm_cat_name, pcm_cat_name_en)
    SELECT 
        -- Ghost Strings: Text cleanup for both category names
        CAST(NULLIF(TRIM(pcm_cat_name), '') AS VARCHAR(100)), 
        CAST(NULLIF(TRIM(pcm_cat_name_en), '') AS VARCHAR(100)) 
    FROM bronze.olist_prd_cat_map;

    SET @rows_inserted = @@ROWCOUNT;
    SET @end_time = GETDATE();
    UPDATE logging.load_log SET status = 'SUCCESS', end_ts = @end_time, rows_inserted = @rows_inserted WHERE log_id = @log_id;

    PRINT '>>>  Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR(20));
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS VARCHAR(50)) + ' seconds';
    PRINT '>>>  -------------';

    -- =========================================================================
    -- Commit the full transaction — all five tables loaded successfully
    -- =========================================================================
    COMMIT TRANSACTION;

    SET @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Silver Layer is Completed';
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
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS VARCHAR(50));
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS VARCHAR(50));
	PRINT '=========================================='

    INSERT INTO logging.load_log (process_name, source_layer, target_layer, status, end_ts, rows_inserted, error_message)
    VALUES ('SILVER_PROCEDURE', 'bronze', 'silver', 'FAILED', GETDATE(), 0, ERROR_MESSAGE());
    END CATCH
END
GO