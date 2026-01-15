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
    cst_cust_id         NVARCHAR(50),
    cst_cust_unique_id  NVARCHAR(50),
    cst_zip_code_prefix INT,
    cst_city            NVARCHAR(100),
    cst_state           NVARCHAR(10),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_cust PRIMARY KEY (cst_cust_id)
);
GO

-- 2.geolocation
IF OBJECT_ID('silver.olist_geo', 'U') IS NOT NULL
    DROP TABLE silver.olist_geo;
GO

CREATE TABLE silver.olist_geo (
    geo_zip_code_prefix INT,
    geo_lat             DECIMAL(18,15),
    geo_lng             DECIMAL(18,15),
    geo_city            NVARCHAR(100),
    geo_state           NVARCHAR(50),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

-- 3.order items--
IF OBJECT_ID('silver.olist_ord_item', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_item;
GO

CREATE TABLE silver.olist_ord_item (
    oi_ord_id           NVARCHAR(50),
    oi_ord_item_id      INT,
    oi_prd_id           NVARCHAR(50),
    oi_sel_id           NVARCHAR(50),
    oi_ship_limit_dt    DATETIME,
    oi_price            DECIMAL(18,2),
    oi_freight_val      DECIMAL(18,2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_item PRIMARY KEY (oi_ord_id, oi_ord_item_id)
);
GO

-- 4.order payments
IF OBJECT_ID('silver.olist_ord_pay', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_pay;
GO

CREATE TABLE silver.olist_ord_pay (
    op_ord_id           NVARCHAR(50),
    op_pay_seq          INT,
    op_pay_type         NVARCHAR(50),
    op_pay_inst         INT,
    op_pay_val          DECIMAL(18,2),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_pay PRIMARY KEY (op_ord_id,op_pay_seq)
);
GO

-- 5.order reviews
IF OBJECT_ID('silver.olist_ord_rev', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord_rev;
GO

CREATE TABLE silver.olist_ord_rev (
    or_rev_id           NVARCHAR(50),
    or_ord_id           NVARCHAR(50),
    or_rev_score        INT,
    or_rev_cmt_title    NVARCHAR(MAX),
    or_rev_cmt_msg      NVARCHAR(MAX),
    or_rev_create_dt    DATETIME,
    or_rev_ans_ts       DATETIME,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord_rev PRIMARY KEY (or_rev_id)
);
GO

-- 6.orders
IF OBJECT_ID('silver.olist_ord', 'U') IS NOT NULL
    DROP TABLE silver.olist_ord;
GO

CREATE TABLE silver.olist_ord (
    ord_ord_id          NVARCHAR(50),
    ord_cust_id         NVARCHAR(50),
    ord_status          NVARCHAR(20),
    ord_purchase_ts     DATETIME,
    ord_approved_ts     DATETIME,
    ord_del_carrier_dt  DATETIME,
    ord_del_cust_dt     DATETIME,
    ord_est_del_dt      DATETIME,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_ord PRIMARY KEY (ord_ord_id)
);
GO

-- 7.products
IF OBJECT_ID('silver.olist_prd', 'U') IS NOT NULL
    DROP TABLE silver.olist_prd;
GO

CREATE TABLE silver.olist_prd (
    prd_prd_id          NVARCHAR(50),
    prd_cat_name        NVARCHAR(50),
    prd_name_len        INT,
    prd_desc_len        INT,
    prd_photos_qty      INT,
    prd_weight_g        INT,
    prd_len_cm          INT,
    prd_height_cm       INT,
    prd_width_cm        INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_prd PRIMARY KEY (prd_prd_id)
);
GO

-- 8.sellers
IF OBJECT_ID('silver.olist_sel', 'U') IS NOT NULL
    DROP TABLE silver.olist_sel;
GO

CREATE TABLE silver.olist_sel (
    sel_sel_id          NVARCHAR(50),
    sel_zip_code_prefix INT,
    sel_city            NVARCHAR(100),
    sel_state           NVARCHAR(10),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()

    CONSTRAINT PK_olist_sel PRIMARY KEY (sel_sel_id)
);
GO

-- 9.category name mapping
IF OBJECT_ID('silver.olist_prd_cat_map', 'U') IS NOT NULL
    DROP TABLE silver.olist_prd_cat_map;
GO

CREATE TABLE silver.olist_prd_cat_map (
    pcm_cat_name        NVARCHAR(100),
    pcm_cat_name_en     NVARCHAR(100),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO


