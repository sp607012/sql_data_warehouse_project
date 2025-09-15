/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new PostgreSQL database named 'datawarehouse' 
    after checking if it already exists.
    If the database exists, it must be dropped manually before running this script.
    Additionally, the script sets up three schemas within the database: 
    'bronze', 'silver', and 'gold'.

WARNING:
    Running this script after dropping the database will delete all data. 
    Proceed with caution and ensure you have proper backups before running.
===============================================================================
*/

-- NOTE: PostgreSQL does not support IF EXISTS for CREATE DATABASE or DROP DATABASE in a single script.
-- You must manually drop the database with: DROP DATABASE IF EXISTS datawarehouse;
-- Then run this script:

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- The following should be run in the 'datawarehouse' database context.

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
