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
    
    -- Check 1.1: PK Completeness (cst_cust_id should never be NULL)
    SELECT 
        'silver.olist_cust' AS TableName,
        'Completeness - PK cst_cust_id' AS CheckName,
        SUM(CASE WHEN cst_cust_id IS NULL THEN 1 ELSE 0 END) AS FailedCount,
        CASE WHEN SUM(CASE WHEN cst_cust_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END AS Status,
        'Primary key cst_cust_id contains NULL values' AS ErrorMsg
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.2: Uniqueness on Primary Key (cst_cust_id)
    SELECT 'silver.olist_cust', 'Uniqueness - PK cst_cust_id',
        COUNT(*) - COUNT(DISTINCT cst_cust_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT cst_cust_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.3: Completeness (State should not be null)
    SELECT 'silver.olist_cust', 'Completeness - cst_state',
        SUM(CASE WHEN cst_state IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN cst_state IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'State is NULL'
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.4: Format (Zip Code must be exactly 5 characters)
    SELECT 'silver.olist_cust', 'Format - cst_zip_code_prefix length',
        SUM(CASE WHEN LEN(cst_zip_code_prefix) <> 5 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN LEN(cst_zip_code_prefix) <> 5 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Zip code prefix is not 5 characters'
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.5: Format (State must be exactly 2 characters)
    SELECT 'silver.olist_cust', 'Format - cst_state length',
        SUM(CASE WHEN LEN(cst_state) <> 2 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN LEN(cst_state) <> 2 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'State code is not 2 characters'
    FROM silver.olist_cust

    UNION ALL

    -- Check 1.6: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_cust', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN (cst_city_raw IS NOT NULL AND LEN(TRIM(cst_city_raw)) = 0)
                  OR (cst_city_std IS NOT NULL AND LEN(TRIM(cst_city_std)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (cst_city_raw IS NOT NULL AND LEN(TRIM(cst_city_raw)) = 0)
                            OR (cst_city_std IS NOT NULL AND LEN(TRIM(cst_city_std)) = 0) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_cust

    UNION ALL
    -- Check 1.7: Integrity (cst_zip_code_prefix should exist in silver.olist_geo)
    SELECT 'silver.olist_cust', 'Integrity - Customer Zip Reference (cst_zip_code_prefix)',
        COUNT(c.cst_cust_id),
        CASE WHEN COUNT(c.cst_cust_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Customer zip prefix not found in olist_geo'
    FROM silver.olist_cust c
    LEFT JOIN silver.olist_geo g ON c.cst_zip_code_prefix = g.geo_zip_code_prefix
    WHERE g.geo_zip_code_prefix IS NULL

    UNION ALL

    -- ====================================================================
    -- 2. silver.olist_geo (Geolocation)
    -- ====================================================================

    -- Check 2.1: PK Completeness (geo_zip_code_prefix should not be NULL)
    SELECT 'silver.olist_geo', 'Completeness - PK geo_zip_code_prefix',
        SUM(CASE WHEN geo_zip_code_prefix IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_zip_code_prefix IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key geo_zip_code_prefix contains NULL values'
    FROM silver.olist_geo

    UNION ALL

    -- Check 2.2: Validity (Latitude Range -90 to 90)
    SELECT 'silver.olist_geo', 'Validity - Latitude Range',
        SUM(CASE WHEN geo_lat < -90 OR geo_lat > 90 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_lat < -90 OR geo_lat > 90 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Latitude out of valid range'
    FROM silver.olist_geo

    UNION ALL

    -- Check 2.3: Validity (Longitude Range -180 to 180)
    SELECT 'silver.olist_geo', 'Validity - Longitude Range',
        SUM(CASE WHEN geo_lng < -180 OR geo_lng > 180 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_lng < -180 OR geo_lng > 180 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Longitude out of valid range'
    FROM silver.olist_geo

    UNION ALL

    -- Check 2.4: Uniqueness (Each Zip Prefix must be UNIQUE - The 1 Zip = 1 Row Rule)
    SELECT 'silver.olist_geo', 'Uniqueness - PK geo_zip_code_prefix',
        COUNT(*) - COUNT(DISTINCT geo_zip_code_prefix),
        CASE WHEN COUNT(*) - COUNT(DISTINCT geo_zip_code_prefix) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate zip codes found in master geo table'
    FROM silver.olist_geo

    UNION ALL

    -- Check 2.5: Completeness (Latitude/Longitude should not be null)
    SELECT 'silver.olist_geo', 'Completeness - Coordinates',
        SUM(CASE WHEN geo_lat IS NULL OR geo_lng IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN geo_lat IS NULL OR geo_lng IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Lat/Lng coordinates are NULL'
    FROM silver.olist_geo

    UNION ALL

    -- Check 2.6: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_geo', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN (geo_city IS NOT NULL AND LEN(TRIM(geo_city)) = 0)
                  OR (geo_state IS NOT NULL AND LEN(TRIM(geo_state)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (geo_city IS NOT NULL AND LEN(TRIM(geo_city)) = 0)
                            OR (geo_state IS NOT NULL AND LEN(TRIM(geo_state)) = 0) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_geo

    UNION ALL

    -- ====================================================================
    -- 3. silver.olist_ord_item (Order Items)
    -- ====================================================================

    -- Check 3.1: PK Completeness (oi_ord_id and oi_ord_item_id should not be NULL)
    SELECT 'silver.olist_ord_item', 'Completeness - Composite PK (oi_ord_id, oi_ord_item_id)',
        SUM(CASE WHEN oi_ord_id IS NULL OR oi_ord_item_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN oi_ord_id IS NULL OR oi_ord_item_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Composite primary key contains NULL values'
    FROM silver.olist_ord_item

    UNION ALL

    -- Check 3.2: Uniqueness on Composite Key (oi_ord_id + oi_ord_item_id)
    SELECT 'silver.olist_ord_item', 'Uniqueness - Composite PK (oi_ord_id + oi_ord_item_id)',
        COUNT(*) - COUNT(DISTINCT CONCAT(oi_ord_id, '|', oi_ord_item_id)),
        CASE WHEN COUNT(*) - COUNT(DISTINCT CONCAT(oi_ord_id, '|', oi_ord_item_id)) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate composite primary keys found'
    FROM silver.olist_ord_item

    UNION ALL

    -- Check 3.3: Integrity (oi_ord_id should exist in silver.olist_ord)
    SELECT 'silver.olist_ord_item', 'Integrity - Order Reference (oi_ord_id)',
        COUNT(oi.oi_ord_id),
        CASE WHEN COUNT(oi.oi_ord_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Order ID in olist_ord_item not found in olist_ord'
    FROM silver.olist_ord_item oi
    LEFT JOIN silver.olist_ord o ON oi.oi_ord_id = o.ord_ord_id
    WHERE o.ord_ord_id IS NULL

    UNION ALL

    -- Check 3.4: Integrity (oi_prd_id should exist in silver.olist_prd)
    SELECT 'silver.olist_ord_item', 'Integrity - Product Reference (oi_prd_id)',
        COUNT(oi.oi_prd_id),
        CASE WHEN COUNT(oi.oi_prd_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Product ID in olist_ord_item not found in olist_prd'
    FROM silver.olist_ord_item oi
    LEFT JOIN silver.olist_prd p ON oi.oi_prd_id = p.prd_prd_id
    WHERE oi.oi_prd_id IS NOT NULL AND p.prd_prd_id IS NULL

    UNION ALL

    -- Check 3.5: Integrity (oi_sel_id should exist in silver.olist_sel)
    SELECT 'silver.olist_ord_item', 'Integrity - Seller Reference (oi_sel_id)',
        COUNT(oi.oi_sel_id),
        CASE WHEN COUNT(oi.oi_sel_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Seller ID in olist_ord_item not found in olist_sel'
    FROM silver.olist_ord_item oi
    LEFT JOIN silver.olist_sel s ON oi.oi_sel_id = s.sel_sel_id
    WHERE oi.oi_sel_id IS NOT NULL AND s.sel_sel_id IS NULL

    UNION ALL

    -- Check 3.6: Validity (Price should not be negative)
    SELECT 'silver.olist_ord_item', 'Validity - Negative Price',
        SUM(CASE WHEN oi_price < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN oi_price < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Price is negative'
    FROM silver.olist_ord_item

    UNION ALL

    -- Check 3.7: Validity (Freight value should not be negative)
    SELECT 'silver.olist_ord_item', 'Validity - Negative Freight',
        SUM(CASE WHEN oi_freight_val < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN oi_freight_val < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Freight value is negative'
    FROM silver.olist_ord_item

    UNION ALL

    -- ====================================================================
    -- 4. silver.olist_ord_pay (Order Payments)
    -- ====================================================================

    -- Check 4.1: PK Completeness (op_ord_id and op_pay_seq should not be NULL)
    SELECT 'silver.olist_ord_pay', 'Completeness - Composite PK (op_ord_id, op_pay_seq)',
        SUM(CASE WHEN op_ord_id IS NULL OR op_pay_seq IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN op_ord_id IS NULL OR op_pay_seq IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Composite primary key contains NULL values'
    FROM silver.olist_ord_pay

    UNION ALL

    -- Check 4.2: Uniqueness on Composite Key (op_ord_id + op_pay_seq)
    SELECT 'silver.olist_ord_pay', 'Uniqueness - Composite PK (op_ord_id + op_pay_seq)',
        COUNT(*) - COUNT(DISTINCT CONCAT(op_ord_id, '|', op_pay_seq)),
        CASE WHEN COUNT(*) - COUNT(DISTINCT CONCAT(op_ord_id, '|', op_pay_seq)) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate composite primary keys found'
    FROM silver.olist_ord_pay

    UNION ALL

    -- Check 4.3: Integrity (op_ord_id should exist in silver.olist_ord)
    SELECT 'silver.olist_ord_pay', 'Integrity - Order Reference (op_ord_id)',
        COUNT(op.op_ord_id),
        CASE WHEN COUNT(op.op_ord_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Order ID in olist_ord_pay not found in olist_ord'
    FROM silver.olist_ord_pay op
    LEFT JOIN silver.olist_ord o ON op.op_ord_id = o.ord_ord_id
    WHERE o.ord_ord_id IS NULL

    UNION ALL

    -- Check 4.4: Validity (Payment value should not be negative)
    SELECT 'silver.olist_ord_pay', 'Validity - Negative Payment Value',
        SUM(CASE WHEN op_pay_val < 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN op_pay_val < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Payment value is negative'
    FROM silver.olist_ord_pay

    UNION ALL

    -- Check 4.5: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_ord_pay', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN op_pay_type IS NOT NULL AND LEN(TRIM(op_pay_type)) = 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN op_pay_type IS NOT NULL AND LEN(TRIM(op_pay_type)) = 0 THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_ord_pay

    UNION ALL

    -- Check 4.6: Validity - Allowed Payment Types
     SELECT 'silver.olist_ord_pay', 'Validity - Payment Type Domain',
         SUM(CASE WHEN op_pay_type NOT IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined') THEN 1 ELSE 0 END),
         CASE WHEN SUM(CASE WHEN op_pay_type NOT IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined') THEN 1 ELSE 0 END) > 0 
              THEN 'WARNING' ELSE 'PASS' END,
         'Found unexpected payment types'
     FROM silver.olist_ord_pay

     UNION ALL

    -- ====================================================================
    -- 5. silver.olist_ord_rev (Reviews)
    -- ====================================================================

    -- Check 5.1: SK Completeness (rev_sk must never be NULL — IDENTITY column)
    SELECT 'silver.olist_ord_rev', 'Completeness - SK rev_sk',
        SUM(CASE WHEN rev_sk IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN rev_sk IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Surrogate key rev_sk contains NULL values'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.2: SK Uniqueness (rev_sk must be unique across the entire table)
    SELECT 'silver.olist_ord_rev', 'Uniqueness - SK rev_sk',
        COUNT(*) - COUNT(DISTINCT rev_sk),
        CASE WHEN COUNT(*) - COUNT(DISTINCT rev_sk) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate surrogate keys found in rev_sk'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.3: NK Sanity (or_rev_id must not be NULL — primary source reference)
    SELECT 'silver.olist_ord_rev', 'Completeness - NK or_rev_id',
        SUM(CASE WHEN or_rev_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN or_rev_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Natural key or_rev_id contains NULL values'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.4: Composite Uniqueness (or_rev_id + or_ord_id must be unique)
    SELECT 'silver.olist_ord_rev', 'Uniqueness - Composite (or_rev_id, or_ord_id)',
        COUNT(*) - COUNT(DISTINCT CONCAT(or_rev_id, '||', or_ord_id)),
        CASE WHEN COUNT(*) - COUNT(DISTINCT CONCAT(or_rev_id, '||', or_ord_id)) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Exact duplicate found: same or_rev_id linked to same or_ord_id more than once'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.5: Integrity (or_ord_id must exist in silver.olist_ord)
    SELECT 'silver.olist_ord_rev', 'Integrity - Order Reference (or_ord_id)',
        COUNT(r.or_ord_id),
        CASE WHEN COUNT(r.or_ord_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Order ID in olist_ord_rev not found in olist_ord'
    FROM silver.olist_ord_rev r
    LEFT JOIN silver.olist_ord o ON r.or_ord_id = o.ord_ord_id
    WHERE r.or_ord_id IS NOT NULL AND o.ord_ord_id IS NULL

    UNION ALL

    -- Check 5.6: Validity (Review Score must be between 1 and 5)
    SELECT 'silver.olist_ord_rev', 'Validity - Score Range (1-5)',
        SUM(CASE WHEN or_rev_score < 1 OR or_rev_score > 5 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN or_rev_score < 1 OR or_rev_score > 5 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Review score is out of range 1-5'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.7: Validity (Date Logic: Creation Date must be before Answer Date)
    SELECT 'silver.olist_ord_rev', 'Validity - Timeline Consistency',
        SUM(CASE WHEN or_rev_ans_ts < or_rev_create_dt THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN or_rev_ans_ts < or_rev_create_dt THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Answer date is earlier than creation date'
    FROM silver.olist_ord_rev

    UNION ALL

    -- Check 5.8: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_ord_rev', 'Format - Ghost Strings in Comments',
        SUM(CASE WHEN (or_rev_cmt_msg   IS NOT NULL AND LEN(TRIM(or_rev_cmt_msg))   = 0)
                   OR (or_rev_cmt_title IS NOT NULL AND LEN(TRIM(or_rev_cmt_title)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (or_rev_cmt_msg   IS NOT NULL AND LEN(TRIM(or_rev_cmt_msg))   = 0)
                              OR (or_rev_cmt_title IS NOT NULL AND LEN(TRIM(or_rev_cmt_title)) = 0) THEN 1 ELSE 0 END) > 0
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace strings that should be NULL'
    FROM silver.olist_ord_rev

    UNION ALL

    -- ====================================================================
    -- 6. silver.olist_ord (Orders)
    -- ====================================================================

    -- Check 6.1: PK Completeness (ord_ord_id should not be NULL)
    SELECT 'silver.olist_ord', 'Completeness - PK ord_ord_id',
        SUM(CASE WHEN ord_ord_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_ord_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key ord_ord_id contains NULL values'
    FROM silver.olist_ord

    UNION ALL

    -- Check 6.2: Uniqueness on Primary Key (ord_ord_id)
    SELECT 'silver.olist_ord', 'Uniqueness - PK ord_ord_id',
        COUNT(ord_ord_id) - COUNT(DISTINCT ord_ord_id),
        CASE WHEN COUNT(ord_ord_id) - COUNT(DISTINCT ord_ord_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_ord

    UNION ALL

    -- Check 6.3: Integrity (ord_cust_id should exist in silver.olist_cust)
    SELECT 'silver.olist_ord', 'Integrity - Customer Reference (ord_cust_id)',
        COUNT(o.ord_cust_id),
        CASE WHEN COUNT(o.ord_cust_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Customer ID in olist_ord not found in olist_cust'
    FROM silver.olist_ord o
    LEFT JOIN silver.olist_cust c ON o.ord_cust_id = c.cst_cust_id
    WHERE o.ord_cust_id IS NOT NULL AND c.cst_cust_id IS NULL

    UNION ALL

    -- ----------------------------------------------------------------
    -- Checks 6.4–6.7: Order Lifecycle Timeline Validation
    -- Business flow: Purchase → Approved → Carrier Pickup → Delivered
    -- Each check only evaluates rows where BOTH timestamps are NOT NULL.
    -- NULL timestamps (canceled, in-transit) are silently skipped.
    -- ----------------------------------------------------------------

    -- Check 6.4: Purchase → Approved (approved_ts must be >= purchase_ts)
    -- WARNING: clock drift or batch-processing delays can cause slight inversions
    SELECT 'silver.olist_ord', 'Validity - Approved Date >= Purchase Date',
        SUM(CASE WHEN ord_approved_ts < ord_purchase_ts THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_approved_ts < ord_purchase_ts THEN 1 ELSE 0 END) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Approved timestamp is earlier than Purchase timestamp'
    FROM silver.olist_ord
    WHERE ord_approved_ts IS NOT NULL AND ord_purchase_ts IS NOT NULL

    UNION ALL

    -- Check 6.5: Approved → Carrier Pickup (carrier_dt must be >= approved_ts)
    -- WARNING: carrier scan may be logged slightly before approval settles
    SELECT 'silver.olist_ord', 'Validity - Carrier Pickup >= Approved Date',
        SUM(CASE WHEN ord_del_carrier_dt < ord_approved_ts THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_del_carrier_dt < ord_approved_ts THEN 1 ELSE 0 END) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Carrier pickup date is earlier than Approved timestamp'
    FROM silver.olist_ord
    WHERE ord_del_carrier_dt IS NOT NULL AND ord_approved_ts IS NOT NULL

    UNION ALL

    -- Check 6.6: Carrier Pickup → Delivered to Customer (cust_dt must be >= carrier_dt)
    -- FAIL: delivering before the carrier picks up is physically impossible
    SELECT 'silver.olist_ord', 'Validity - Delivered Date >= Carrier Pickup',
        SUM(CASE WHEN ord_del_cust_dt < ord_del_carrier_dt THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_del_cust_dt < ord_del_carrier_dt THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Customer delivery date is earlier than Carrier pickup date'
    FROM silver.olist_ord
    WHERE ord_del_cust_dt IS NOT NULL AND ord_del_carrier_dt IS NOT NULL

    UNION ALL

    -- Check 6.7: Purchase → Estimated Delivery (est_del_dt must be >= purchase_ts)
    -- FAIL: an estimated delivery before the purchase was placed is logically impossible
    SELECT 'silver.olist_ord', 'Validity - Estimated Delivery >= Purchase Date',
        SUM(CASE WHEN ord_est_del_dt < ord_purchase_ts THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN ord_est_del_dt < ord_purchase_ts THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Estimated delivery date is earlier than Purchase timestamp'
    FROM silver.olist_ord
    WHERE ord_est_del_dt IS NOT NULL AND ord_purchase_ts IS NOT NULL

    UNION ALL

    -- Check 6.8: Business Logic Flag - ord_is_late (Should be 1 if delivered after estimated date)
    SELECT 'silver.olist_ord', 'Business Logic - ord_is_late Flag Accuracy',
        SUM(CASE WHEN (ord_is_late = 1 AND ord_del_cust_dt <= ord_est_del_dt) 
                  OR (ord_is_late = 0 AND ord_del_cust_dt > ord_est_del_dt) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (ord_is_late = 1 AND ord_del_cust_dt <= ord_est_del_dt) 
                            OR (ord_is_late = 0 AND ord_del_cust_dt > ord_est_del_dt) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'ord_is_late flag does not match actual delivery vs estimated date'
    FROM silver.olist_ord
    WHERE ord_del_cust_dt IS NOT NULL AND ord_est_del_dt IS NOT NULL AND ord_is_late IS NOT NULL

    UNION ALL

    -- Check 6.9: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_ord', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN (ord_status IS NOT NULL AND LEN(TRIM(ord_status)) = 0)
                  OR (ord_cust_id IS NOT NULL AND LEN(TRIM(ord_cust_id)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (ord_status IS NOT NULL AND LEN(TRIM(ord_status)) = 0)
                            OR (ord_cust_id IS NOT NULL AND LEN(TRIM(ord_cust_id)) = 0) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_ord

    UNION ALL

    -- Check 6.10: Integrity - Order Item Reference (ord_ord_id should exist in olist_ord_item)
    SELECT 'silver.olist_ord', 'Integrity - Order Item Reference (ord_ord_id)',
        COUNT(o.ord_ord_id),
        CASE WHEN COUNT(o.ord_ord_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Order ID in olist_ord not found in olist_ord_item'
    FROM silver.olist_ord o
    LEFT JOIN silver.olist_ord_item oi ON o.ord_ord_id = oi.oi_ord_id
    WHERE oi.oi_ord_id IS NULL

    UNION ALL

    -- Check 6.11: Integrity - Order Payment Reference (ord_ord_id should exist in olist_ord_pay)
    SELECT 'silver.olist_ord', 'Integrity - Order Payment Reference (ord_ord_id)',
        COUNT(o.ord_ord_id),
        CASE WHEN COUNT(o.ord_ord_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Order ID in olist_ord not found in olist_ord_pay'
    FROM silver.olist_ord o
    LEFT JOIN silver.olist_ord_pay op ON o.ord_ord_id = op.op_ord_id
    WHERE op.op_ord_id IS NULL

    UNION ALL

    -- Check 6.12: Validity - Allowed Order Statuses
    SELECT 
        'silver.olist_ord', 'Validity - Order Status Domain',
        SUM(CASE 
            WHEN ord_status NOT IN ('delivered', 'shipped', 'unavailable', 'canceled', 'invoiced', 'approved', 'processing', 'created') 
            THEN 1 ELSE 0 END),
        CASE 
            WHEN SUM(CASE 
                WHEN ord_status NOT IN ('delivered', 'shipped', 'unavailable', 'canceled', 'invoiced', 'approved', 'processing', 'created') 
                THEN 1 ELSE 0 END) > 0 
            THEN 'WARNING' 
            ELSE 'PASS' 
        END,
        'Found unexpected order statuses'
    FROM silver.olist_ord

    UNION ALL

    -- ====================================================================
    -- 7. silver.olist_prd (Products)
    -- ====================================================================

    -- Check 7.1: PK Completeness (prd_prd_id should not be NULL)
    SELECT 'silver.olist_prd', 'Completeness - PK prd_prd_id',
        SUM(CASE WHEN prd_prd_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN prd_prd_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key prd_prd_id contains NULL values'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.2: Uniqueness on Primary Key (prd_prd_id)
    SELECT 'silver.olist_prd', 'Uniqueness - PK prd_prd_id',
        COUNT(*) - COUNT(DISTINCT prd_prd_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT prd_prd_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.3: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_prd', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN prd_cat_name IS NOT NULL AND LEN(TRIM(prd_cat_name)) = 0 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN prd_cat_name IS NOT NULL AND LEN(TRIM(prd_cat_name)) = 0 THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.4: Completeness - Product Name (prd_cat_name should not be NULL)
    SELECT 'silver.olist_prd', 'Completeness - prd_cat_name',
        SUM(CASE WHEN prd_cat_name IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN prd_cat_name IS NULL THEN 1 ELSE 0 END) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Product name is NULL'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.5: Validity - Product Dimensions and Photo Quantity (Should be positive if not NULL)
    SELECT 'silver.olist_prd', 'Validity - Product Dimensions and Photo Quantity',
        SUM(CASE WHEN (prd_weight_g IS NOT NULL AND prd_weight_g <= 0) 
                 OR (prd_len_cm IS NOT NULL AND prd_len_cm <= 0)
                 OR (prd_height_cm IS NOT NULL AND prd_height_cm <= 0)
                 OR (prd_width_cm IS NOT NULL AND prd_width_cm <= 0)
                 OR (prd_photos_qty IS NOT NULL AND prd_photos_qty < 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (prd_weight_g IS NOT NULL AND prd_weight_g <= 0) 
                           OR (prd_len_cm IS NOT NULL AND prd_len_cm <= 0)
                           OR (prd_height_cm IS NOT NULL AND prd_height_cm <= 0)
                           OR (prd_width_cm IS NOT NULL AND prd_width_cm <= 0)
                           OR (prd_photos_qty IS NOT NULL AND prd_photos_qty < 0) THEN 1 ELSE 0 END) > 0 
             THEN 'WARNING' ELSE 'PASS' END,
        'Product dimensions or photo quantity are zero or negative'
    FROM silver.olist_prd

    UNION ALL

    -- Check 7.6: Integrity - Product Category (prd_cat_name should exist in mapping)
    SELECT 'silver.olist_prd', 'Integrity - Product Category Reference (prd_cat_name)',
        COUNT(p.prd_prd_id),
        CASE WHEN COUNT(p.prd_prd_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Product category not found in olist.prd_cat_map'
    FROM silver.olist_prd p
    LEFT JOIN silver.olist_prd_cat_map cm ON p.prd_cat_name = cm.pcm_cat_name
    WHERE p.prd_cat_name IS NOT NULL AND cm.pcm_cat_name IS NULL

    UNION ALL

    -- ====================================================================
    -- 8. silver.olist_sel (Sellers)
    -- ====================================================================

    -- Check 8.1: PK Completeness (sel_sel_id should not be NULL)
    SELECT 'silver.olist_sel', 'Completeness - PK sel_sel_id',
        SUM(CASE WHEN sel_sel_id IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN sel_sel_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key sel_sel_id contains NULL values'
    FROM silver.olist_sel

    UNION ALL

    -- Check 8.2: Uniqueness on Primary Key (sel_sel_id)
    SELECT 'silver.olist_sel', 'Uniqueness - PK sel_sel_id',
        COUNT(*) - COUNT(DISTINCT sel_sel_id),
        CASE WHEN COUNT(*) - COUNT(DISTINCT sel_sel_id) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate primary keys found'
    FROM silver.olist_sel

    UNION ALL

    -- Check 8.3: Format - Seller Zip Code Prefix length
    SELECT 'silver.olist_sel', 'Format - sel_zip_code_prefix length',
        SUM(CASE WHEN LEN(sel_zip_code_prefix) <> 5 THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN LEN(sel_zip_code_prefix) <> 5 THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Seller zip prefix is not 5 characters'
    FROM silver.olist_sel

    UNION ALL

    -- Check 8.4: Integrity - Seller Zip not found in Geolocation (Warning only)
    SELECT 'silver.olist_sel', 'Integrity - Seller Zip Reference (sel_zip_code_prefix)',
        COUNT(s.sel_sel_id),
        CASE WHEN COUNT(s.sel_sel_id) > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Seller zip prefix not found in olist_geo'
    FROM silver.olist_sel s
    LEFT JOIN silver.olist_geo g ON s.sel_zip_code_prefix = g.geo_zip_code_prefix
    WHERE g.geo_zip_code_prefix IS NULL

    UNION ALL

    -- Check 8.5: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_sel', 'Format - Ghost Strings in Text Columns',
        SUM(CASE WHEN (sel_city_raw IS NOT NULL AND LEN(TRIM(sel_city_raw)) = 0)
                  OR (sel_city_std IS NOT NULL AND LEN(TRIM(sel_city_std)) = 0)
                  OR (sel_state IS NOT NULL AND LEN(TRIM(sel_state)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (sel_city_raw IS NOT NULL AND LEN(TRIM(sel_city_raw)) = 0)
                            OR (sel_city_std IS NOT NULL AND LEN(TRIM(sel_city_std)) = 0)
                            OR (sel_state IS NOT NULL AND LEN(TRIM(sel_state)) = 0) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_sel

    UNION ALL

    -- ====================================================================
    -- 9. silver.olist_prd_cat_map (Product Category Mapping)
    -- ====================================================================

    -- Check 9.1: Ghost Strings (Empty or whitespace-only strings in text columns)
    SELECT 'silver.olist_prd_cat_map', 'Format - Ghost Strings in Category Names',
        SUM(CASE WHEN (pcm_cat_name IS NOT NULL AND LEN(TRIM(pcm_cat_name)) = 0)
                  OR (pcm_cat_name_en IS NOT NULL AND LEN(TRIM(pcm_cat_name_en)) = 0) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (pcm_cat_name IS NOT NULL AND LEN(TRIM(pcm_cat_name)) = 0)
                            OR (pcm_cat_name_en IS NOT NULL AND LEN(TRIM(pcm_cat_name_en)) = 0) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Found whitespace-only strings that should be NULL'
    FROM silver.olist_prd_cat_map

    UNION ALL

    -- Check 9.2: PK Completeness (pcm_cat_name should not be NULL - if it's the PK)
    SELECT 'silver.olist_prd_cat_map', 'Completeness - PK pcm_cat_name',
        SUM(CASE WHEN pcm_cat_name IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN pcm_cat_name IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Primary key pcm_cat_name contains NULL values'
    FROM silver.olist_prd_cat_map

    UNION ALL

    -- Check 9.3: Uniqueness on Primary Key (pcm_cat_name must be unique)
    SELECT 'silver.olist_prd_cat_map', 'Uniqueness - PK pcm_cat_name',
        COUNT(*) - COUNT(DISTINCT pcm_cat_name),
        CASE WHEN COUNT(*) - COUNT(DISTINCT pcm_cat_name) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Duplicate category names found in mapping'
    FROM silver.olist_prd_cat_map

    UNION ALL

    -- Check 9.4: Completeness - English Category Name (pcm_cat_name_en should not be NULL)
    SELECT 'silver.olist_prd_cat_map', 'Completeness - pcm_cat_name_en',
        SUM(CASE WHEN pcm_cat_name_en IS NULL THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN pcm_cat_name_en IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL' ELSE 'PASS' END,
        'English category name is NULL'
    FROM silver.olist_prd_cat_map

    UNION ALL

    -- Check 9.5: Format - Category Name Consistency (Both names should have content)
    SELECT 'silver.olist_prd_cat_map', 'Format - Category Name Consistency',
        SUM(CASE WHEN (pcm_cat_name IS NULL AND pcm_cat_name_en IS NOT NULL) 
                 OR (pcm_cat_name IS NOT NULL AND pcm_cat_name_en IS NULL) THEN 1 ELSE 0 END),
        CASE WHEN SUM(CASE WHEN (pcm_cat_name IS NULL AND pcm_cat_name_en IS NOT NULL) 
                           OR (pcm_cat_name IS NOT NULL AND pcm_cat_name_en IS NULL) THEN 1 ELSE 0 END) > 0 
             THEN 'FAIL' ELSE 'PASS' END,
        'Category name and English name should both be NULL or both have values'
    FROM silver.olist_prd_cat_map

    UNION ALL

    -- ====================================================================
    -- 10. Cross-Table: Financial Integrity (olist_ord_item vs olist_ord_pay)
    -- ====================================================================
    -- Business Rule:
    --   For each order, the total paid (SUM of all payment methods) must match
    --   the total charged (SUM of item price + freight across all order items).
    --   A tolerance of 0.01 absorbs float rounding; anything beyond that is a
    --   genuine discrepancy in the source data.
    --
    -- FULL OUTER JOIN: catches orders that exist in one table but not the other
    --   - Order in items but not payments → customer was never charged
    --   - Order in payments but not items → payment with no line items

    -- Check 10.1: Financial Reconciliation — Item Totals vs Payment Totals
    SELECT
        'silver.cross_table' AS TableName,
        'Financial Reconciliation (Item vs Payment)' AS CheckName,
        COUNT(*) AS FailedCount,
        CASE WHEN COUNT(*) > 0 THEN 'WARNING' ELSE 'PASS' END AS Status,
        'Discrepancy > 0.01 detected between SUM(price + freight) and SUM(payment_value) per order' AS ErrorMsg
    FROM (
        -- CTE 1: Total item amount per order (price + freight for all items)
        SELECT
            COALESCE(item.order_id, pay.order_id) AS order_id,
            item.total_item_amt,
            pay.total_payment_amt
        FROM (
            SELECT
                oi_ord_id                              AS order_id,
                SUM(ISNULL(oi_price, 0) + ISNULL(oi_freight_val, 0)) AS total_item_amt
            FROM silver.olist_ord_item
            GROUP BY oi_ord_id
        ) item
        -- FULL OUTER JOIN to catch orphaned orders on either side
        FULL OUTER JOIN (
            SELECT
                op_ord_id                              AS order_id,
                SUM(ISNULL(op_pay_val, 0))             AS total_payment_amt
            FROM silver.olist_ord_pay
            GROUP BY op_ord_id
        ) pay
            ON item.order_id = pay.order_id
        -- Filter: only keep orders with a material discrepancy
        WHERE ABS(
                ISNULL(item.total_item_amt,    0) -
                ISNULL(pay.total_payment_amt,  0)
              ) > 0.01
    ) mismatched_orders
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

