/*
====================================================================
Quality Checks - Silver Layer
====================================================================

Script Purpose:
    This script performs various quality checks for data consistency,
    accuracy, and standardization across the 'silver' schema. 

Checks Include:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run after loading the Silver Layer.
    - Investigate & resolve any discrepancies found.
====================================================================
*/

-- ===============================================================
-- Checking 'silver.crm_cust_info'
-- ===============================================================

-- ✅ Expectation: No NULLs or duplicate customer IDs
SELECT
    cst_id,
    COUNT(*) AS dup_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- ✅ Expectation: No unwanted spaces in cst_key
SELECT
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- ⚠️ Issue Check: Standardization of Marital Status
SELECT
    cst_marital_status,
    COUNT(*) AS frequency
FROM silver.crm_cust_info
GROUP BY cst_marital_status
ORDER BY frequency DESC;

-- ⚠️ Issue Check: Standardization of Gender
SELECT
    cst_gndr,
    COUNT(*) AS frequency
FROM silver.crm_cust_info
GROUP BY cst_gndr
ORDER BY frequency DESC;


-- ===============================================================
-- Checking 'silver.crm_prd_info'
-- ===============================================================

-- ✅ Expectation: No NULLs or duplicate product IDs
SELECT
    prd_id,
    COUNT(*) AS dup_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ✅ Expectation: No unwanted spaces in product name
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- ✅ Expectation: No negative or NULL product costs
SELECT
    prd_id,
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- ⚠️ Issue Check: Standardization of Product Line
SELECT
    prd_line,
    COUNT(*) AS frequency
FROM silver.crm_prd_info
GROUP BY prd_line
ORDER BY frequency DESC;

-- ✅ Expectation: No invalid date orders (start > end)
SELECT
    prd_id,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ===============================================================
-- Checking 'silver.erp_cust_az12'
-- ===============================================================

-- ✅ Expectation: Birthdates between 1924-01-01 and Today
SELECT
    cid,
    bdate
FROM silver.erp_cust_az12
WHERE bdate < DATE '1924-01-01'
   OR bdate > CURRENT_DATE;

-- ⚠️ Issue Check: Gender standardization
SELECT
    gen,
    COUNT(*) AS frequency
FROM silver.erp_cust_az12
GROUP BY gen
ORDER BY frequency DESC;


-- ===============================================================
-- Checking 'silver.erp_loc_a101'
-- ===============================================================

-- ⚠️ Issue Check: Country standardization
SELECT
    cntry,
    COUNT(*) AS frequency
FROM silver.erp_loc_a101
GROUP BY cntry
ORDER BY frequency DESC;


-- ===============================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ===============================================================

-- ✅ Expectation: No unwanted spaces in category fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- ⚠️ Issue Check: Maintenance field values
SELECT
    maintenance,
    COUNT(*) AS frequency
FROM silver.erp_px_cat_g1v2
GROUP BY maintenance
ORDER BY frequency DESC;


-- ===============================================================
-- Checking 'silver.crm_sales_details'
-- ===============================================================

-- ✅ Expectation: No invalid date orders (order > ship/due)
SELECT
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- ✅ Expectation: Sales = Quantity * Price and > 0
SELECT
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_ord_num;


-- ===============================================================
-- Checking 'bronze.crm_sales_details'
-- ===============================================================

-- ✅ Expectation: No invalid or out-of-range dates
SELECT
    sls_ord_num,
    sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LENGTH(sls_due_dt::text) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;



