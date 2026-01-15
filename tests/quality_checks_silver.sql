/*
===============================================================================
Data Quality Check Script for Silver Layer 
===============================================================================
*/

USE RetailWarehouse;
GO

WITH DQ_Report AS (
    
    -- ====================================================================
    -- 1. silver.olist_cust (Customers)
    -- ====================================================================
    
    -- Check 1.1: Uniqueness on Primary Key (cst_cust_id)
    SELECT 
        'silver.olist_cust' AS TableName,
        'Uniqueness - PK cst_cust_id' AS CheckName,
        COUNT(cst_cust_id) - COUNT(DISTINCT cst_cust_id) AS FailedCount,
        CASE WHEN COUNT(cst_cust_id) - COUNT(DISTINCT cst_cust_id) > 0 THEN 'FAIL' ELSE 'PASS' END AS Status,
        'Duplicate primary keys found' AS ErrorMsg
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.2: Completeness (State should not be null)
    SELECT 
        'silver.olist_cust',
        'Completeness - cst_state',
        SUM(CASE WHEN cst_state IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN cst_state IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'State is NULL'
    FROM silver.olist_cust

    UNION ALL

    -- ====================================================================
    -- 2. silver.olist_geo (Geolocation)
    -- ====================================================================

    -- Check 2.1: Validity (Latitude Range -90 to 90)
    SELECT 
        'silver.olist_geo',
        'Validity - Latitude Range',
        SUM(CASE WHEN geo_lat < -90 OR geo_lat > 90 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_lat < -90 OR geo_lat > 90 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Latitude out of valid range'
    FROM silver.olist_geo

    UNION ALL

     -- Check 2.2: Validity (Longitude Range -180 to 180)
    SELECT 
        'silver.olist_geo',
        'Validity - Longitude Range',
        SUM(CASE WHEN geo_lng < -180 OR geo_lng > 180 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_lng < -180 OR geo_lng > 180 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Longitude out of valid range'
    FROM silver.olist_geo

    UNION ALL

    -- ====================================================================
    -- 3. silver.olist_ord_item (Order Items)
    -- ====================================================================

    -- Check 3.1: Uniqueness on Composite Key (oi_ord_id + oi_ord_item_id)
    SELECT 
        'silver.olist_ord_item',
        'Uniqueness - Composite PK (oi_ord_id + oi_ord_item_id)',
        (SELECT COUNT(*) FROM silver.olist_ord_item) 
        - (SELECT COUNT(*) FROM (SELECT DISTINCT oi_ord_id, oi_ord_item_id FROM silver.olist_ord_item) t),
        CASE WHEN (SELECT COUNT(*) FROM silver.olist_ord_item) 
                - (SELECT COUNT(*) FROM (SELECT DISTINCT oi_ord_id, oi_ord_item_id FROM silver.olist_ord_item) t) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    
    UNION ALL

    -- Check 3.2: Validity (Price should not be negative)
    SELECT 
        'silver.olist_ord_item',
        'Validity - Negative Price',
        SUM(CASE WHEN oi_price < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN oi_price < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Price is negative'
    FROM silver.olist_ord_item

    UNION ALL

     -- Check 3.3: Validity (Freight value should not be negative)
    SELECT 
        'silver.olist_ord_item',
        'Validity - Negative Freight',
        SUM(CASE WHEN oi_freight_val < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN oi_freight_val < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Freight value is negative'
    FROM silver.olist_ord_item

    UNION ALL

    -- ====================================================================
    -- 4. silver.olist_ord_pay (Order Payments)
    -- ====================================================================

    -- Check 4.1: Uniqueness on Composite Key (op_ord_id + op_pay_seq)
    SELECT 
        'silver.olist_ord_pay',
        'Uniqueness - Composite PK (op_ord_id + op_pay_seq)',
        (SELECT COUNT(*) FROM silver.olist_ord_pay) 
        - (SELECT COUNT(*) FROM (SELECT DISTINCT op_ord_id, op_pay_seq FROM silver.olist_ord_pay) t),
        CASE WHEN (SELECT COUNT(*) FROM silver.olist_ord_pay) 
        - (SELECT COUNT(*) FROM (SELECT DISTINCT op_ord_id, op_pay_seq FROM silver.olist_ord_pay) t) > 0
        THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate composite keys found'

    UNION ALL

    -- ====================================================================
    -- 5. silver.olist_ord_rev (Reviews)
    -- ====================================================================

    -- Check 5.1: Uniqueness on Primary Key (or_rev_id)
    SELECT 
        'silver.olist_ord_rev',
        'Uniqueness - PK or_rev_id',
        COUNT(or_rev_id) - COUNT(DISTINCT or_rev_id),
        CASE WHEN COUNT(or_rev_id) - COUNT(DISTINCT or_rev_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found' -- <--- Added this line
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.2: Validity (Review Score must be between 1 and 5)
    SELECT 
        'silver.olist_ord_rev',
        'Validity - Score Range (1-5)',
        SUM(CASE WHEN or_rev_score < 1 OR or_rev_score > 5 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN or_rev_score < 1 OR or_rev_score > 5 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Review score is out of range 1-5'
    FROM silver.olist_ord_rev

    UNION ALL

    -- ====================================================================
    -- 6. silver.olist_ord (Orders)
    -- ====================================================================

    -- Check 6.1: Uniqueness on Primary Key (ord_ord_id)
    SELECT 
        'silver.olist_ord',
        'Uniqueness - PK ord_ord_id',
        COUNT(ord_ord_id) - COUNT(DISTINCT ord_ord_id),
        CASE WHEN COUNT(ord_ord_id) - COUNT(DISTINCT ord_ord_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_ord

    UNION ALL

    -- Check 6.2: Validity (Date Logic: Delivery Date < Purchase Date is impossible)
    SELECT 
        'silver.olist_ord',
        'Validity - Delivery vs Purchase Date',
        SUM(CASE WHEN ord_del_cust_dt < ord_purchase_ts THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_del_cust_dt < ord_purchase_ts THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Delivery date is earlier than Purchase date'
    FROM silver.olist_ord

    UNION ALL

    -- ====================================================================
    -- 7. silver.olist_prd (Products)
    -- ====================================================================

    -- Check 7.1: Uniqueness on Primary Key (prd_prd_id)
    SELECT 
        'silver.olist_prd',
        'Uniqueness - PK prd_prd_id',
        COUNT(prd_prd_id) - COUNT(DISTINCT prd_prd_id),
        CASE WHEN COUNT(prd_prd_id) - COUNT(DISTINCT prd_prd_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.2: Validity (Physical dimensions > 0 if provided)
    SELECT 
        'silver.olist_prd',
        'Validity - Product Weight',
        SUM(CASE WHEN prd_weight_g <= 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN prd_weight_g <= 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Product weight is zero or negative'
    FROM silver.olist_prd

    UNION ALL 

    -- ====================================================================
    -- 8. silver.olist_sel (Sellers)
    -- ====================================================================

    -- Check 8.1: Uniqueness on Primary Key (sel_sel_id)
    SELECT 
        'silver.olist_sel',
        'Uniqueness - PK sel_sel_id',
        COUNT(sel_sel_id) - COUNT(DISTINCT sel_sel_id),
        CASE WHEN COUNT(sel_sel_id) - COUNT(DISTINCT sel_sel_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_sel
)

SELECT * FROM DQ_Report
ORDER BY Status, TableName;