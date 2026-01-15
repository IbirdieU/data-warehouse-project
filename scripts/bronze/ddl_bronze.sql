/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE RetailWarehouse;

---1.customers---
IF OBJECT_ID('bronze.olist_cust', 'U') IS NOT NULL
    DROP TABLE bronze.olist_cust;
GO

CREATE TABLE bronze.olist_cust (
    cst_cust_id         NVARCHAR(MAX),
    cst_cust_unique_id  NVARCHAR(MAX),
    cst_zip_code_prefix NVARCHAR(MAX),
    cst_city            NVARCHAR(MAX),
    cst_state           NVARCHAR(MAX)
);
GO

---2.geolocation---
IF OBJECT_ID('bronze.olist_geo', 'U') IS NOT NULL
    DROP TABLE bronze.olist_geo;
GO

CREATE TABLE bronze.olist_geo (
    geo_zip_code_prefix NVARCHAR(MAX),
    geo_lat             NVARCHAR(MAX),
    geo_lng             NVARCHAR(MAX),
    geo_city            NVARCHAR(MAX),
    geo_state           NVARCHAR(MAX)
);
GO

---3.order items---
IF OBJECT_ID('bronze.olist_ord_item', 'U') IS NOT NULL
    DROP TABLE bronze.olist_ord_item;
GO

CREATE TABLE bronze.olist_ord_item (
    oi_ord_id           NVARCHAR(MAX),
    oi_ord_item_id      NVARCHAR(MAX),
    oi_prd_id           NVARCHAR(MAX),
    oi_sel_id        NVARCHAR(MAX),
    oi_ship_limit_dt    NVARCHAR(MAX),
    oi_price            NVARCHAR(MAX),
    oi_freight_val      NVARCHAR(MAX)
);
GO

---4.order payments---
IF OBJECT_ID('bronze.olist_ord_pay', 'U') IS NOT NULL
    DROP TABLE bronze.olist_ord_pay;
GO

CREATE TABLE bronze.olist_ord_pay (
    op_ord_id           NVARCHAR(MAX),
    op_pay_seq          NVARCHAR(MAX),
    op_pay_type         NVARCHAR(MAX),
    op_pay_inst         NVARCHAR(MAX),
    op_pay_val          NVARCHAR(MAX)

);
GO

---5.order reviews---
IF OBJECT_ID('bronze.olist_ord_rev', 'U') IS NOT NULL
    DROP TABLE bronze.olist_ord_rev;
GO

CREATE TABLE bronze.olist_ord_rev (
    or_rev_id           NVARCHAR(MAX),
    or_ord_id           NVARCHAR(MAX),
    or_rev_score        NVARCHAR(MAX),
    or_rev_cmt_title   NVARCHAR(MAX),
    or_rev_cmt_msg      NVARCHAR(MAX),
    or_rev_create_dt    NVARCHAR(MAX),
    or_rev_ans_ts       NVARCHAR(MAX)
);
GO

---6.orders---
IF OBJECT_ID('bronze.olist_ord', 'U') IS NOT NULL
    DROP TABLE bronze.olist_ord;
GO

CREATE TABLE bronze.olist_ord (
    ord_ord_id          NVARCHAR(MAX),
    ord_cust_id         NVARCHAR(MAX),
    ord_status          NVARCHAR(MAX),
    ord_purchase_ts     NVARCHAR(MAX),
    ord_approved_ts     NVARCHAR(MAX),
    ord_del_carrier_dt  NVARCHAR(MAX),
    ord_del_cust_dt     NVARCHAR(MAX),
    ord_est_del_dt      NVARCHAR(MAX)
);
GO

---7.products---
IF OBJECT_ID('bronze.olist_prd', 'U') IS NOT NULL
    DROP TABLE bronze.olist_prd;
GO

CREATE TABLE bronze.olist_prd (
    prd_prd_id          NVARCHAR(MAX),
    prd_cat_name        NVARCHAR(MAX),
    prd_name_len        NVARCHAR(MAX),
    prd_desc_len        NVARCHAR(MAX),
    prd_photos_qty      NVARCHAR(MAX),
    prd_weight_g        NVARCHAR(MAX),
    prd_len_cm          NVARCHAR(MAX),
    prd_height_cm       NVARCHAR(MAX),
    prd_width_cm        NVARCHAR(MAX)
);
GO

---8.sellers---
IF OBJECT_ID('bronze.olist_sel', 'U') IS NOT NULL
    DROP TABLE bronze.olist_sel;
GO

CREATE TABLE bronze.olist_sel (
    sel_sel_id          NVARCHAR(MAX),
    sel_zip_code_prefix NVARCHAR(MAX),
    sel_city            NVARCHAR(MAX),
    sel_state           NVARCHAR(MAX)
);
GO

---9.category name mapping---
IF OBJECT_ID('bronze.olist_prd_cat_map', 'U') IS NOT NULL
    DROP TABLE bronze.olist_prd_cat_map;
GO

CREATE TABLE bronze.olist_prd_cat_map (
    pcm_cat_name        NVARCHAR(MAX),
    pcm_cat_name_en     NVARCHAR(MAX)
);
GO


