/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'RetailWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

USE master;
GO

-- Drop and recreate the 'RetailWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'RetailWarehouse')
BEGIN
    ALTER DATABASE RetailWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE RetailWarehouse;
END;
GO

-- Create the 'RetailWarehouse' database
CREATE DATABASE RetailWarehouse;
GO

USE RetailWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
