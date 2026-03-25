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
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'logging')
    EXEC('CREATE SCHEMA logging');
GO

-- =============================================================================
-- Centralized ELT Load Log
-- Pattern: INSERT (RUNNING) at start → UPDATE (SUCCESS/FAILED) at end
-- Duration: derived from end_ts - start_ts
-- =============================================================================
CREATE TABLE logging.load_log (
    log_id          INT             IDENTITY(1,1) NOT NULL,
    process_name    VARCHAR(100)    NOT NULL,           -- Table or procedure being loaded (e.g. 'olist_geo', 'SILVER_PROCEDURE')
    source_layer    VARCHAR(10)     NOT NULL,           -- Source schema (e.g. 'bronze', 'silver')
    target_layer    VARCHAR(10)     NOT NULL,           -- Target schema (e.g. 'silver', 'gold')
    status          VARCHAR(10)     NOT NULL,           -- 'RUNNING', 'SUCCESS', 'FAILED'
    start_ts        DATETIME        DEFAULT GETDATE(),  -- Timestamp when the process started
    end_ts          DATETIME        NULL,               -- Timestamp when the process finished (NULL while RUNNING)
    rows_inserted   INT             NULL,               -- Number of rows loaded (NULL while RUNNING)
    error_message   NVARCHAR(MAX)   NULL,               -- Error details (NULL on SUCCESS)

    CONSTRAINT PK_load_log        PRIMARY KEY (log_id),
    CONSTRAINT CHK_load_log_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);
GO
