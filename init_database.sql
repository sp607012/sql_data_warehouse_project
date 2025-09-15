 /*
===============================================================================
Create Database and Schemas - Improved Version
===============================================================================
Script Purpose:
    This script creates a new PostgreSQL database named 'datawarehouse'.
    - If the database exists, it is dropped first (manual confirmation recommended).
    - After creation, the script sets up three schemas: 'bronze', 'silver', and 'gold'.
    - This script is intended to be run from a psql session connected to a different database,
      like the default 'postgres' database.

WARNING:
    Dropping a database will delete all data permanently. Ensure proper backups before running.
===============================================================================
*/

-- Step 0: Connect to a safe database (not 'datawarehouse')
-- In psql, run:
-- \c postgres

-- Step 1: Drop the database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Step 2: Create the database
CREATE DATABASE datawarehouse;

-- Step 3: Connect to the newly created database
-- In psql, run:
-- \c datawarehouse

-- Step 4: Create schemas within the new database
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Step 5: Optional: confirm schemas
-- Run:
-- \dn
-- This lists all schemas in the current database

