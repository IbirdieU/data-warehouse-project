/*
===============================================================================
DDL Script: Create Gold Views (Virtual Data Mart) with Stable Surrogate Keys
Target Schema: gold
Source Schema: silver
Description: 
    Creates Logical Views for Dimensions and Facts.
    Implements deterministic surrogate keys generated from source Primary Keys.
===============================================================================
*/

USE RetailWarehouse;
GO

-- =============================================================================
-- 1. Dimension View: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    -- Generate a surrogate key based on the primary key to ensure deterministic ordering
    ROW_NUMBER() OVER (ORDER BY cst_cust_id) AS customer_key,
    
    -- Business/Natural Key
    cst_cust_id         AS customer_id,
    cst_cust_unique_id  AS customer_unique_id,
    
    -- Attributes
    cst_city_raw        AS city,
    cst_state           AS state,
    cst_zip_code_prefix AS zip_code,
    
    -- Metadata
    dwh_create_date     AS create_date
FROM silver.olist_cust;
GO

-- =============================================================================
-- 2. Dimension View: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    -- Generate a surrogate key based on the product primary key
    ROW_NUMBER() OVER (ORDER BY p.prd_prd_id) AS product_key,
    
    -- Business/Natural Key
    p.prd_prd_id        AS product_id,
    
    -- Attributes
    p.prd_cat_name      AS category_name_pt, -- Original Portuguese category name
    ISNULL(m.pcm_cat_name_en, 'Unknown') AS category_name, -- Mapped English category name
    p.prd_weight_g      AS weight_g,
    p.prd_photos_qty    AS photos_quantity,
    
    -- Metadata
    p.dwh_create_date   AS create_date
FROM silver.olist_prd p
LEFT JOIN silver.olist_prd_cat_map m 
    ON p.prd_cat_name = m.pcm_cat_name;
GO

-- =============================================================================
-- 3. Dimension View: gold.dim_sellers
-- =============================================================================
IF OBJECT_ID('gold.dim_sellers', 'V') IS NOT NULL DROP VIEW gold.dim_sellers;
GO

CREATE VIEW gold.dim_sellers AS
SELECT
    -- Generate a surrogate key based on the seller primary key
    ROW_NUMBER() OVER (ORDER BY sel_sel_id) AS seller_key,
    
    -- Business/Natural Key
    sel_sel_id          AS seller_id,
    
    -- Attributes
    sel_city            AS city,
    sel_state           AS state,
    sel_zip_code_prefix AS zip_code,
    
    -- Metadata
    dwh_create_date     AS create_date
FROM silver.olist_sel;
GO

-- =============================================================================
-- 4. Fact View: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    -- Primary Key for the Fact Table (Composite Key from source)
    oi.oi_ord_id        AS order_id,
    oi.oi_ord_item_id   AS order_item_id,
    
    -- Foreign Keys connecting to Dimensions (Using Natural Keys for performance in Views)
    -- Note: In a physical table implementation, these would often be the Surrogate Keys.
    o.ord_cust_id       AS customer_id,      -- Links to gold.dim_customers.customer_id
    oi.oi_prd_id        AS product_id,       -- Links to gold.dim_products.product_id
    oi.oi_sel_id        AS seller_id,        -- Links to gold.dim_sellers.seller_id
    
    -- Transaction Dates
    o.ord_purchase_ts   AS purchase_date,
    o.ord_approved_ts   AS approved_date,
    o.ord_del_cust_dt   AS delivered_date,
    o.ord_est_del_dt    AS estimated_delivery_date,
    
    -- Measures (Numerical values for analysis)
    oi.oi_price         AS price,
    oi.oi_freight_val   AS freight_value,
    (oi.oi_price + oi.oi_freight_val) AS total_amount, -- Calculated total
    
    -- Status
    o.ord_status        AS order_status
FROM silver.olist_ord_item oi
-- Join with Order Header to get customer reference and dates
LEFT JOIN silver.olist_ord o 
    ON oi.oi_ord_id = o.ord_ord_id;
GO