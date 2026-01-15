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
    "EXEC silver.load_silver"
===============================================================================
*/
USE RetailWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Loading Silver Layer...';
    PRINT '================================================';

    SET @start_time = GETDATE();
	PRINT '>>>  Truncating Table: silver.olist_cust';
	TRUNCATE TABLE silver.olist_cust;
	PRINT '>>>  Inserting Data Into: silver.olist_cust';

    INSERT INTO silver.olist_cust (cst_cust_id, cst_cust_unique_id, cst_zip_code_prefix, cst_city, cst_state)
    SELECT 
        cst_cust_id, 
        cst_cust_unique_id, 
        TRY_CAST(cst_zip_code_prefix AS INT), 
        cst_city, cst_state
    FROM bronze.olist_cust;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_geo';
	TRUNCATE TABLE silver.olist_geo;
	PRINT '>>>  Inserting Data Into: silver.olist_geo';
    
    INSERT INTO silver.olist_geo (geo_zip_code_prefix, geo_lat, geo_lng, geo_city, geo_state)
    SELECT 
        TRY_CAST(geo_zip_code_prefix AS INT),
        TRY_CAST(geo_lat AS DECIMAL(18,15)), 
        TRY_CAST(geo_lng AS DECIMAL(18,15)), 
        geo_city, 
        geo_state 
    FROM bronze.olist_geo;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord_item';
	TRUNCATE TABLE silver.olist_ord_item;
	PRINT '>>>  Inserting Data Into: silver.olist_ord_item';
    
    INSERT INTO silver.olist_ord_item (oi_ord_id, oi_ord_item_id, oi_prd_id, oi_sel_id, oi_ship_limit_dt, oi_price, oi_freight_val)
    SELECT 
        oi_ord_id, 
        TRY_CAST(oi_ord_item_id AS INT), 
        oi_prd_id, 
        oi_sel_id, 
        TRY_CAST(oi_ship_limit_dt AS DATETIME), 
        TRY_CAST(oi_price AS DECIMAL(18,2)), 
        TRY_CAST(oi_freight_val AS DECIMAL(18,2)) 
    FROM bronze.olist_ord_item;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord_pay';
	TRUNCATE TABLE silver.olist_ord_pay;
	PRINT '>>>  Inserting Data Into: silver.olist_ord_pay';
    
    INSERT INTO silver.olist_ord_pay (op_ord_id, op_pay_seq, op_pay_type, op_pay_inst, op_pay_val)
    SELECT 
        op_ord_id, 
        TRY_CAST(op_pay_seq AS INT), 
        op_pay_type, 
        TRY_CAST(op_pay_inst AS INT), 
        TRY_CAST(op_pay_val AS DECIMAL(18,2)) 
    FROM bronze.olist_ord_pay;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_rev';
	TRUNCATE TABLE silver.olist_ord_rev;
	PRINT '>>>  Inserting Data Into: silver.olist_rev';

    
    INSERT INTO silver.olist_ord_rev (or_rev_id, or_ord_id, or_rev_score, or_rev_cmt_title, or_rev_cmt_msg, or_rev_create_dt, or_rev_ans_ts)
    SELECT 
        or_rev_id, 
        or_ord_id, 
        TRY_CAST(or_rev_score AS INT), 
        or_rev_cmt_title, 
        or_rev_cmt_msg, 
        TRY_CAST(or_rev_create_dt AS DATETIME), 
        TRY_CAST(or_rev_ans_ts AS DATETIME) 
    FROM (
            SELECT 
                or_rev_id,
                or_ord_id,
                or_rev_score,
                or_rev_cmt_title, 
                or_rev_cmt_msg,
                or_rev_create_dt,
                or_rev_ans_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY or_rev_id 
                    ORDER BY TRY_CAST(or_rev_ans_ts AS DATETIME) DESC
                ) as rn
            FROM bronze.olist_ord_rev  
    ) t
    WHERE t.rn = 1

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_ord';
	TRUNCATE TABLE silver.olist_ord;
	PRINT '>>>  Inserting Data Into: silver.olist_ord';
    
    INSERT INTO silver.olist_ord (ord_ord_id, ord_cust_id, ord_status, ord_purchase_ts, ord_approved_ts, ord_del_carrier_dt, ord_del_cust_dt, ord_est_del_dt)
    SELECT 
        ord_ord_id, 
        ord_cust_id, 
        ord_status, 
        TRY_CAST(ord_purchase_ts AS DATETIME), 
        TRY_CAST(ord_approved_ts AS DATETIME), 
        TRY_CAST(ord_del_carrier_dt AS DATETIME), 
        TRY_CAST(ord_del_cust_dt AS DATETIME),
        TRY_CAST(ord_est_del_dt AS DATETIME) 
    FROM bronze.olist_ord;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_prd';
	TRUNCATE TABLE silver.olist_prd;
	PRINT '>>>  Inserting Data Into: silver.olist_prd';
    
    INSERT INTO silver.olist_prd (prd_prd_id, prd_cat_name, prd_name_len, prd_desc_len, prd_photos_qty, prd_weight_g, prd_len_cm, prd_height_cm, prd_width_cm)
    SELECT 
        prd_prd_id, 
        prd_cat_name, 
        TRY_CAST(prd_name_len AS INT), 
        TRY_CAST(prd_desc_len AS INT), 
        TRY_CAST(prd_photos_qty AS INT), 
        TRY_CAST(prd_weight_g AS INT), 
        TRY_CAST(prd_len_cm AS INT), 
        TRY_CAST(prd_height_cm AS INT), 
        TRY_CAST(prd_width_cm AS INT) 
    FROM bronze.olist_prd;

    SET @end_time = GETDATE();
    PRINT '>>> Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
   
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_sel';
	TRUNCATE TABLE silver.olist_sel;
	PRINT '>>>  Inserting Data Into: silver.olist_sel';
    
    INSERT INTO silver.olist_sel (sel_sel_id, sel_zip_code_prefix, sel_city, sel_state)
    SELECT 
        sel_sel_id, 
        TRY_CAST(sel_zip_code_prefix AS INT), 
        sel_city, 
        sel_state 
    FROM bronze.olist_sel;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';
    
    SET @start_time = GETDATE();
    PRINT '>>>  Truncating Table: silver.olist_prd_cat_map';
	TRUNCATE TABLE silver.olist_prd_cat_map;
	PRINT '>>>  Inserting Data Into: silver.olist_prd_cat_map';
    
    INSERT INTO silver.olist_prd_cat_map (pcm_cat_name, pcm_cat_name_en)
    SELECT 
        pcm_cat_name, 
        pcm_cat_name_en 
    FROM bronze.olist_prd_cat_map;

    SET @end_time = GETDATE();
    PRINT '>>>  Load Duration: ' + CAST(CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS DECIMAL(6,2)) AS NVARCHAR) + ' seconds';
    PRINT '>>>  -------------';

    SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
    END TRY
    BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
GO