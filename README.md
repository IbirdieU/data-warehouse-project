# RetailDataWarehouse_Project

## 📖 Overview
This project builds a complete data pipeline using the Brazilian Olist e-commerce dataset. It follows the Medallion Architecture (Bronze, Silver, Gold) to clean, transform, and analyze data with Python and Microsoft SQL Server.


## 🏷️ Naming Conventions
To maintain consistency across the Medallion Architecture, the following naming rules are applied to all tables within the Data Warehouse:

### Bronze Rules
All names must start with the source system name, and table names must match their original names as closely as possible without significant renaming to ensure traceability.

<sourcesystem>_<entity>

<sourcesystem>: Name of the source system (e.g., olist).

<entity>: Original table name from the source (e.g., cust, ord).

Example: olist_cust → Raw customer data ingested from the Olist system.

### Silver Rules
Names remain aligned with the source system for consistency, but the data is cleaned, typed, and standardized. These tables are stored in the silver schema.

<sourcesystem>_<entity>

<sourcesystem>: Name of the source system (e.g., olist).

<entity>: Cleaned version of the entity name.

Example: olist_cust → Cleaned and standardized customer information in the silver layer.
