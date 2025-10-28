
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.



-- Step 1 — Create the Database
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- STEP 2- CONNECT TO THE NEW DATABASE 

-- Step 3 — Create Your Schemas
CREATE OR REPLACE FUNCTION create_schemas() 
RETURNS void AS $$
BEGIN
    -- Create bronze schema
    CREATE SCHEMA IF NOT EXISTS bronze;

    -- Create silver schema
    CREATE SCHEMA IF NOT EXISTS silver;

    -- Create gold schema
    CREATE SCHEMA IF NOT EXISTS gold;
END;
$$ LANGUAGE plpgsql;

SELECT create_schemas();
