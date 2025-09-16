/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
<<<<<<< HEAD
    This script performs quality checks to validate the integrity, consistency, 
=======
    This script performs quality checks to validate the integrity, consistency,
>>>>>>> 337dade689eb5a2a14864ef875e41aa09a1bdf3a
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
<<<<<<< HEAD
=======
    - Run these checks after data loading Silver Layer.
>>>>>>> 337dade689eb5a2a14864ef875e41aa09a1bdf3a
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

<<<<<<< HEAD
-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
=======
-- ===================================================================
-- Checking 'gold.dim_customers'
-- ===================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT
>>>>>>> 337dade689eb5a2a14864ef875e41aa09a1bdf3a
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

<<<<<<< HEAD
-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
=======
-- ===================================================================
-- Checking 'gold.product_key'
-- ===================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results
SELECT
>>>>>>> 337dade689eb5a2a14864ef875e41aa09a1bdf3a
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

<<<<<<< HEAD
-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
=======
-- ===================================================================
-- Checking 'gold.fact_sales'
-- ===================================================================
-- Check the data model connectivity between fact and dimensions
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
>>>>>>> 337dade689eb5a2a14864ef875e41aa09a1bdf3a
