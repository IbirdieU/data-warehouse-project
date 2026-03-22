/*
===============================================================================
DDL Script : Create Gold Schema - Star Schema (Physical Tables)
Database   : RetailWarehouse
Target     : gold schema
Source     : silver schema
===============================================================================
Architecture:
    Dimensions : dim_date | dim_customers | dim_products | dim_sellers
    Fact       : fact_sales  (grain: one row per order item)

Change Log:
    2026-03-11  Initial physical-table implementation (Star Schema)
===============================================================================
*/

USE RetailWarehouse;
GO

-- =============================================================================
-- 0. Schema
-- Create the gold schema if it does not already exist.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- =============================================================================
-- 1. Drop existing objects
--    Fact first (holds FK references), then dimensions.
-- =============================================================================

-- Physical tables (fact before dimensions due to FK dependencies)
IF OBJECT_ID('gold.fact_sales',    'U') IS NOT NULL DROP TABLE gold.fact_sales;
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL DROP TABLE gold.dim_customers;
IF OBJECT_ID('gold.dim_products',  'U') IS NOT NULL DROP TABLE gold.dim_products;
IF OBJECT_ID('gold.dim_sellers',   'U') IS NOT NULL DROP TABLE gold.dim_sellers;
IF OBJECT_ID('gold.dim_date',      'U') IS NOT NULL DROP TABLE gold.dim_date;
GO


-- =============================================================================
-- 2. Dimension: gold.dim_customers
--    Source : silver.olist_cust
--    Grain  : one row per cst_cust_id (unique per order in Olist model)
-- =============================================================================
CREATE TABLE gold.dim_customers (
    -- Surrogate Key
    customer_sk         INT          IDENTITY(1,1) NOT NULL,  -- System-generated surrogate PK; stable across source changes

    -- Natural Key
    customer_id         VARCHAR(50)               NOT NULL,   -- NK: maps one-to-one with cst_cust_id; unique per order, not per real-world buyer
    
    -- Descriptive Attributes
    customer_unique_id  VARCHAR(50)               NULL,       -- Persistent buyer identifier that groups all orders by the same real-world customer (cst_cust_unique_id)
    zip_code_prefix     CHAR(5)                   NULL,       -- First 5 digits of the customer's postal / ZIP code
    city                NVARCHAR(100)             NULL,       -- Standardised city name (silver column: cst_city_std)
    state               CHAR(2)                   NULL,       -- Brazilian state abbreviation, e.g. SP, RJ, MG
    
    -- Metadata
    dwh_create_date     DATETIME2    DEFAULT GETDATE() NOT NULL,  -- Timestamp when this record was inserted into the gold layer

    CONSTRAINT PK_dim_customers PRIMARY KEY (customer_sk)
);
GO

-- =============================================================================
-- 3. Dimension: gold.dim_products
--    Source : silver.olist_prd  LEFT JOIN  silver.olist_prd_cat_map
--    Grain  : one row per product
-- =============================================================================
CREATE TABLE gold.dim_products (
    -- Surrogate Key
    product_sk          INT          IDENTITY(1,1) NOT NULL,  -- System-generated surrogate PK; stable across source changes
    
    -- Natural Key
    
    product_id          VARCHAR(50)               NOT NULL,   -- NK: original product identifier from the source system (prd_prd_id)
    
    -- Descriptive Attributes — Category
    category_name_pt    NVARCHAR(100)             NULL,       -- Product category in Portuguese as it appears in the raw data (prd_cat_name)
    category_name_en    NVARCHAR(100)             NULL,       -- Product category translated to English via the mapping table (pcm_cat_name_en); NULL if no mapping exists
    
    -- Descriptive Attributes — Listing
    name_length         INT                       NULL,       -- Character count of the product name on the listing (prd_name_len)
    description_length  INT                       NULL,       -- Character count of the product description (prd_desc_len)
    photos_quantity     INT                       NULL,       -- Number of photos published for this product (prd_photos_qty)
    
    -- Descriptive Attributes — Physical Dimensions
    weight_g            INT                       NULL,       -- Product weight in grams (prd_weight_g)
    length_cm           INT                       NULL,       -- Product length in centimetres (prd_len_cm)
    height_cm           INT                       NULL,       -- Product height in centimetres (prd_height_cm)
    width_cm            INT                       NULL,       -- Product width in centimetres (prd_width_cm)
   
    -- Metadata
    dwh_create_date     DATETIME2    DEFAULT GETDATE() NOT NULL,  -- Timestamp when this record was inserted into the gold layer

    CONSTRAINT PK_dim_products PRIMARY KEY (product_sk)
);
GO

-- =============================================================================
-- 4. Dimension: gold.dim_sellers
--    Source : silver.olist_sel
--    Grain  : one row per seller
-- =============================================================================
CREATE TABLE gold.dim_sellers (
    -- Surrogate Key
    seller_sk           INT          IDENTITY(1,1) NOT NULL,  -- System-generated surrogate PK; stable across source changes
    
    -- Natural Key
    seller_id           VARCHAR(50)               NOT NULL,   -- NK: original seller identifier from the source system (sel_sel_id)
    
    -- Descriptive Attributes
    zip_code_prefix     CHAR(5)                   NULL,       -- First 5 digits of the seller's postal / ZIP code (sel_zip_code_prefix)
    city                NVARCHAR(100)             NULL,       -- Standardised seller city name (silver column: sel_city_std)
    state               CHAR(2)                   NULL,       -- Brazilian state abbreviation, e.g. SP, MG, PR
    
    -- Metadata
    dwh_create_date     DATETIME2    DEFAULT GETDATE() NOT NULL,  -- Timestamp when this record was inserted into the gold layer

    CONSTRAINT PK_dim_sellers PRIMARY KEY (seller_sk)
);
GO


-- =============================================================================
-- 5. Dimension: gold.dim_date
--    Source : Generated (no silver table; derived from a date series CTE)
--    Grain  : one row per calendar day
--    Range  : 2016-01-01 → 2020-12-31  (covers full Olist dataset + buffer)
-- =============================================================================
CREATE TABLE gold.dim_date (
    -- Surrogate Key
    date_sk             INT          IDENTITY(1,1) NOT NULL,  -- System-generated surrogate PK; sequential by date due to ordered INSERT
    
    -- Natural Key
    date_id             INT                        NOT NULL,  -- NK: integer date in YYYYMMDD format for fast, human-readable filtering (e.g. 20180315)
    full_date           DATE                       NOT NULL,  -- The actual calendar date value for direct date arithmetic
    
    -- Year Attributes
    calendar_year       INT                        NOT NULL,  -- Four-digit calendar year (e.g. 2018)
    
    -- Quarter Attributes
    quarter_number      INT                        NOT NULL,  -- Calendar quarter number (1–4)
    quarter_name        NVARCHAR(2)                NOT NULL,  -- Quarter label for reports (e.g. Q1, Q4)
    
    -- Month Attributes
    month_number        INT                        NOT NULL,  -- Month number (1=January … 12=December)
    month_name          NVARCHAR(20)               NOT NULL,  -- Full English month name (e.g. January, November)
    month_name_short    NVARCHAR(3)                NOT NULL,  -- Three-letter English abbreviation (e.g. Jan, Nov)
    
    -- Day Attributes
    day_of_month        INT                        NOT NULL,  -- Day of the month (1–31)
    day_of_week         INT                        NOT NULL,  -- ISO weekday number: 1=Monday … 7=Sunday (locale-safe calculation)
    day_name            NVARCHAR(20)               NOT NULL,  -- Full English weekday name (e.g. Monday, Friday)
    day_name_short      NVARCHAR(3)                NOT NULL,  -- Three-letter English abbreviation (e.g. Mon, Fri)
    is_weekend          BIT                        NOT NULL,  -- Convenience flag: 1 = Saturday or Sunday, 0 = weekday
    
    -- Metadata
    dwh_create_date     DATETIME2    DEFAULT GETDATE() NOT NULL,  -- Timestamp when this record was inserted into the gold layer

    CONSTRAINT PK_dim_date       PRIMARY KEY (date_sk),
    CONSTRAINT UQ_dim_date_id    UNIQUE (date_id),     -- Guarantees one row per YYYYMMDD integer key
    CONSTRAINT UQ_dim_date_full  UNIQUE (full_date)    -- Guarantees one row per calendar date
);
GO


-- =============================================================================
-- 6. Fact Table: gold.fact_sales
--    Source  : silver.olist_ord_item  (grain driver — one row per order item)
--              silver.olist_ord       (order header: dates, status, is_late)
--              silver.olist_ord_pay   (payments: SUM aggregated to order level)
--              silver.olist_ord_rev   (review score: one review per order)
--    Grain   : one row per order item  (order_id + order_item_id)
-- =============================================================================
CREATE TABLE gold.fact_sales (
    -- Surrogate Key
    sale_sk                 INT           IDENTITY(1,1) NOT NULL,  -- System-generated surrogate PK; uniquely identifies each fact row
    
    -- Degenerate Dimensions (natural keys kept for traceability, no dim table)
    order_id                VARCHAR(50)               NOT NULL,    -- Degenerate dimension: original order hash from the source system (oi_ord_id)
    order_item_id           INT                       NOT NULL,    -- Degenerate dimension: sequential item number within the order (1-based, oi_ord_item_id)
    order_status            NVARCHAR(20)              NULL,        -- Order lifecycle status at load time (e.g. delivered, shipped, canceled)

    -- Foreign Keys → Dimension Surrogate Keys
    customer_sk             INT                       NOT NULL,    -- FK → gold.dim_customers.customer_sk; identifies the purchasing customer
    product_sk              INT                       NOT NULL,    -- FK → gold.dim_products.product_sk;  identifies the purchased product
    seller_sk               INT                       NOT NULL,    -- FK → gold.dim_sellers.seller_sk;    identifies the fulfilling seller
    purchase_date_sk        INT                       NOT NULL,    -- FK → gold.dim_date.date_sk;         date the order was placed by the customer
    
    -- Measures — Financial
    price                   DECIMAL(18,2)             NULL,        -- Selling price of this specific order item (silver: oi_price)
    freight_value           DECIMAL(18,2)             NULL,        -- Freight / shipping cost attributed to this item (silver: oi_freight_val)
    total_payment_value     DECIMAL(18,2)             NULL,        -- Total amount paid for the entire order across all payment methods
                                                                   --   Source : SUM(op_pay_val) from silver.olist_ord_pay grouped by order_id
                                                                   --   Grain note: this is an order-level metric stored at item grain.
                                                                   --   When aggregating across items, use MAX() or divide by item count
                                                                   --   to avoid inflating totals across multi-item orders.
    
    -- Measures — Customer Satisfaction
    review_score            INT                       NULL,        -- Customer satisfaction rating for the order (1 = worst … 5 = best); source: silver.olist_ord_rev.or_rev_score

    -- Measures — Delivery Performance
    is_late                 BIT                       NULL,        -- Delivery flag derived in silver layer: 1 = actual delivery exceeded estimated date, 0 = on time or early

    -- Metadata
    dwh_create_date         DATETIME2     DEFAULT GETDATE() NOT NULL,  -- Timestamp when this record was inserted into the gold layer

    CONSTRAINT PK_fact_sales        PRIMARY KEY (sale_sk),
    CONSTRAINT UQ_fact_sales_grain  UNIQUE      (order_id, order_item_id),   -- Enforces grain integrity: exactly one row per order item

    -- Logical Foreign Key constraints
    -- These document the Star Schema relationships and are enforced by SQL Server.
    -- If ETL load order cannot guarantee dimension rows exist first, add WITH NOCHECK.
    CONSTRAINT FK_fact_sales_customer  FOREIGN KEY (customer_sk)      REFERENCES gold.dim_customers (customer_sk),
    CONSTRAINT FK_fact_sales_product   FOREIGN KEY (product_sk)       REFERENCES gold.dim_products  (product_sk),
    CONSTRAINT FK_fact_sales_seller    FOREIGN KEY (seller_sk)        REFERENCES gold.dim_sellers   (seller_sk),
    CONSTRAINT FK_fact_sales_date      FOREIGN KEY (purchase_date_sk) REFERENCES gold.dim_date      (date_sk)
);
GO