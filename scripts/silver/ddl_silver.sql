/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

USE RetailWarehouse;

-- 1.customers
IF OBJECT_ID('silver.olist_cust', 'U') IS NOT NULL
    DROP TABLE silver.olist_cust;
GO

CREATE TABLE silver.olist_cust (
    cst_cust_id         VARCHAR(50) NOT NULL,  -- PRIMARY KEY
    cst_cust_unique_id  VARCHAR(50),
    cst_zip_code_prefix CHAR(5),
    cst_city_raw        NVARCHAR(100),
    cst_city_std        NVARCHAR(100),
    cst_state           CHAR(2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_cust PRIMARY KEY (cst_cust_id)
);
GO

-- 2.geolocation
IF OBJECT_ID('silver.olist_geo', 'U') IS NOT NULL
    DROP TABLE silver.olist_geo;
GO

CREATE TABLE silver.olist_geo (
    geo_zip_code_prefix CHAR(5) NOT NULL, -- PRIMARY KEY
    geo_lat             DECIMAL(18,15),
    geo_lng             DECIMAL(18,15),
    geo_city            NVARCHAR(100),
    geo_state           CHAR(2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_geo PRIMARY KEY (geo_zip_code_prefix)
);
GO

-- 3.order items
IF OBJECT_ID('silver.olist_ord_item', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_item;
GO

CREATE TABLE silver.olist_ord_item (
    oi_ord_id           VARCHAR(50) NOT NULL,      -- COMPOSITE KEY (part 1)
    oi_ord_item_id      INT NOT NULL,              -- COMPOSITE KEY (part 2)
    oi_prd_id           VARCHAR(50),
    oi_sel_id           VARCHAR(50),
    oi_ship_limit_dt    DATETIME,
    oi_price            DECIMAL(18,2),
    oi_freight_val      DECIMAL(18,2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_item PRIMARY KEY (oi_ord_id, oi_ord_item_id),
    CONSTRAINT CHK_ord_item_price CHECK (oi_price >= 0),
    CONSTRAINT CHK_ord_item_freight CHECK (oi_freight_val >= 0)
);
GO

-- 4.order payments
IF OBJECT_ID('silver.olist_ord_pay', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_pay;
GO

CREATE TABLE silver.olist_ord_pay (
    op_ord_id           VARCHAR(50) NOT NULL,      -- COMPOSITE KEY (part 1)
    op_pay_seq          INT NOT NULL,              -- COMPOSITE KEY (part 2)
    op_pay_type         NVARCHAR(50),
    op_pay_inst         INT,
    op_pay_val          DECIMAL(18,2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_pay PRIMARY KEY (op_ord_id, op_pay_seq),
    CONSTRAINT CHK_ord_pay_val CHECK (op_pay_val >= 0)
);
GO

-- 5.order reviews
IF OBJECT_ID('silver.olist_ord_rev', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_rev;
GO

CREATE TABLE silver.olist_ord_rev (
    rev_sk              INT          IDENTITY(1,1) NOT NULL,  -- SURROGATE KEY: source or_rev_id is duplicated across orders; SK avoids PK violations
    or_rev_id           VARCHAR(50)               NULL,       
    or_ord_id           VARCHAR(50)               NULL,     
    or_rev_score        INT                       NULL,
    or_rev_cmt_title    NVARCHAR(200)             NULL,
    or_rev_cmt_msg      NVARCHAR(MAX)             NULL,
    or_rev_create_dt    DATETIME                  NULL,       
    or_rev_ans_ts       DATETIME                  NULL,       
    dwh_create_date     DATETIME2    DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_rev  PRIMARY KEY (rev_sk),
    CONSTRAINT CHK_ord_rev_score CHECK (or_rev_score BETWEEN 1 AND 5)
);
GO

-- 6.orders
IF OBJECT_ID('silver.olist_ord', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord;
GO

CREATE TABLE silver.olist_ord (
    ord_ord_id          VARCHAR(50) NOT NULL,  -- PRIMARY KEY
    ord_cust_id         VARCHAR(50),
    ord_status          NVARCHAR(20),
    ord_purchase_ts     DATETIME,
    ord_approved_ts     DATETIME,
    ord_del_carrier_dt  DATETIME,
    ord_del_cust_dt     DATETIME,
    ord_est_del_dt      DATETIME,
    ord_is_late         BIT,                   -- Flag: 1=Late, 0=On Time (Derived column)
    delivery_lead_time  INT,                   -- Days between purchase and customer delivery (Derived column; NULL if not yet delivered)
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord PRIMARY KEY (ord_ord_id)
);
GO

-- 7.products
IF OBJECT_ID('silver.olist_prd', 'U') IS NOT NULL
    DROP TABLE silver.olist_prd;
GO

CREATE TABLE silver.olist_prd (
    prd_prd_id          VARCHAR(50) NOT NULL,  -- PRIMARY KEY
    prd_cat_name        NVARCHAR(50),
    prd_name_len        INT,
    prd_desc_len        INT,
    prd_photos_qty      INT,
    prd_weight_g        INT,
    prd_len_cm          INT,
    prd_height_cm       INT,
    prd_width_cm        INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_prd PRIMARY KEY (prd_prd_id),
    CONSTRAINT CHK_prd_weight CHECK (prd_weight_g >= 0),
    CONSTRAINT CHK_prd_len CHECK (prd_len_cm >= 0),
    CONSTRAINT CHK_prd_height CHECK (prd_height_cm >= 0),
    CONSTRAINT CHK_prd_width CHECK (prd_width_cm >= 0)
);
GO

-- 8.sellers
IF OBJECT_ID('silver.olist_sel', 'U') IS NOT NULL
    DROP TABLE silver.olist_sel;
GO

CREATE TABLE silver.olist_sel (
    sel_sel_id          VARCHAR(50) NOT NULL,  -- PRIMARY KEY
    sel_zip_code_prefix CHAR(5),
    sel_city_raw        NVARCHAR(100),
    sel_city_std        NVARCHAR(100),
    sel_state           CHAR(2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_sel PRIMARY KEY (sel_sel_id)
);
GO

-- 9.category name mapping
IF OBJECT_ID('silver.olist_prd_cat_map', 'U') IS NOT NULL
    DROP TABLE silver.olist_prd_cat_map;
GO

CREATE TABLE silver.olist_prd_cat_map (
    pcm_cat_name    NVARCHAR(100) NOT NULL, -- PRIMARY KEY
    pcm_cat_name_en NVARCHAR(100) NOT NULL, 
    
    CONSTRAINT PK_olist_prd_cat_map PRIMARY KEY (pcm_cat_name)
);
GO
