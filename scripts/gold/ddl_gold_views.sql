/*
===============================================================================
DDL Script : Create Gold Views (Presentation Layer for BI Tools)
Database   : RetailWarehouse
Schema     : gold
===============================================================================
Purpose:
    Creates thin presentation views on top of the gold physical tables.
    These views are the single entry point for Power BI / reporting tools.

    Views provide:
        - Business-friendly column names (no technical prefixes)
        - NULL handling with default display values
        - Stable _sk columns for BI relationship mapping

Naming Convention:
    gold.view_dim_<entity>   — dimension views
    gold.view_fact_<entity>  — fact views
===============================================================================
*/

USE RetailWarehouse;
GO

-- =============================================================================
-- 1. gold.view_dim_customers
--    Source : gold.dim_customers
-- =============================================================================
CREATE OR ALTER VIEW gold.view_dim_customers AS
SELECT
    customer_sk,                                    -- Surrogate key (PK for BI relationships)
    customer_unique_id,                             -- NK: one row per real-world person
    zip_code_prefix     AS zip_code,                -- Latest shipping ZIP code (5 digits)
    city                AS customer_city,            -- Latest customer city (standardised)
    state               AS customer_state            -- Latest Brazilian state abbreviation
FROM gold.dim_customers;
GO

-- =============================================================================
-- 2. gold.view_dim_products
--    Source : gold.dim_products
-- =============================================================================
CREATE OR ALTER VIEW gold.view_dim_products AS
SELECT
    product_sk,                                     -- Surrogate key (PK for BI relationships)
    product_id,                                     -- Business key: product identifier
    category_name_pt    AS category_portuguese,      -- Portuguese category (NULL-safe)
    category_name_en    AS category_english,          -- English category (NULL-safe)
    name_length,                                    -- Product name character count
    description_length,                             -- Product description character count
    photos_quantity,                                -- Number of listing photos
    weight_g            AS weight_grams,             -- Weight in grams
    length_cm,                                      -- Length in centimetres
    height_cm,                                      -- Height in centimetres
    width_cm                                        -- Width in centimetres
FROM gold.dim_products;
GO

-- =============================================================================
-- 3. gold.view_dim_sellers
--    Source : gold.dim_sellers
-- =============================================================================
CREATE OR ALTER VIEW gold.view_dim_sellers AS
SELECT
    seller_sk,                                      -- Surrogate key (PK for BI relationships)
    seller_id,                                      -- Business key: seller identifier
    zip_code_prefix     AS zip_code,                -- Postal code (5 digits)
    city                AS seller_city,              -- Seller city (standardised)
    state               AS seller_state              -- Brazilian state abbreviation
FROM gold.dim_sellers;
GO

-- =============================================================================
-- 4. gold.view_dim_date
--    Source : gold.dim_date
--    All calendar attributes exposed for Power BI Time Intelligence
-- =============================================================================
CREATE OR ALTER VIEW gold.view_dim_date AS
SELECT
    date_sk,                                        -- Surrogate key (PK for BI relationships)
    date_id,                                        -- Integer date YYYYMMDD (e.g. 20180315)
    full_date           AS calendar_date,           -- Calendar date value
    calendar_year,                                  -- Calendar year (e.g. 2018)
    quarter_number,                                 -- Quarter number (1-4)
    quarter_name,                                   -- Quarter label (Q1, Q2, Q3, Q4)
    month_number        AS month_number,            -- Month number (1-12)
    month_name,                                     -- Full month name (e.g. January)
    month_name_short,                               -- Abbreviated month (e.g. Jan)
    day_of_month        AS day_of_month,            -- Day of month (1-31)
    day_of_week,                                    -- ISO weekday (1=Mon, 7=Sun)
    day_name,                                       -- Full weekday name (e.g. Monday)
    day_name_short,                                 -- Abbreviated weekday (e.g. Mon)
    is_weekend                                      -- Weekend flag (1=Sat/Sun, 0=weekday)
FROM gold.dim_date;
GO

-- =============================================================================
-- 5. gold.view_fact_sales
--    Source : gold.fact_sales
--    Grain  : one row per order item
-- =============================================================================
CREATE OR ALTER VIEW gold.view_fact_sales AS
SELECT
    -- Primary key
    sale_sk,                                        -- Surrogate key (PK)

    -- Degenerate dimensions — Order
    order_id,                                       -- Order identifier
    order_item_id,                                  -- Item sequence within order (1-based)
    order_status,                                   -- Order lifecycle status

    -- Degenerate dimensions — Shipping Location (Point-in-Time)
    shipping_zip_code_prefix,                       -- ZIP code of shipping address for this order
    shipping_city,                                  -- City of shipping address
    shipping_state,                                 -- State of shipping address

    -- Foreign keys (BI relationship mapping)
    customer_sk,                                    -- FK -> view_dim_customers
    product_sk,                                     -- FK -> view_dim_products
    seller_sk,                                      -- FK -> view_dim_sellers
    purchase_date_sk,                               -- FK -> view_dim_date

    -- Financial measures
    price,                                          -- Item selling price
    freight_value,                                  -- Freight cost for this item
    total_payment_value,                            -- Order-level total payment (all methods)

    -- Satisfaction measure
    review_score,                                   -- Customer rating (1-5, NULL if no review)

    -- Delivery performance
    is_late,                                        -- Late delivery flag (1=late, 0=on time)
    delivery_lead_time                              -- Days between purchase and customer delivery (NULL if not delivered)
FROM gold.fact_sales;
GO
