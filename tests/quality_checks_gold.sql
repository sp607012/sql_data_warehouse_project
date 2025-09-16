/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after loading data into the Silver Layer and populating
      the Gold Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ===================================================================
-- Check 1: Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
-- ===================================================================
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ===================================================================
-- Check 2: Uniqueness of Product Key in gold.dim_products
-- Expectation: No results
-- ===================================================================
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ===================================================================
-- Check 3: Referential Integrity in gold.fact_sales
-- Ensures fact table keys exist in dimension tables
-- Expectation: No results
-- ===================================================================
SELECT f.fact_id, f.customer_key, f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
       ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;

