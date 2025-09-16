/*   
====================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
====================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables (optional, if needed).
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_stage_silver();
====================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_stage_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;   -- overall start time
    v_end_time   TIMESTAMP;   -- overall end time
    v_duration   INTERVAL;    -- total run duration
    t_start      TIMESTAMP;   -- per-step start time
    v_rowcount   BIGINT;      -- rows processed in step
BEGIN
    -----------------------------------------------------------------
    -- Step 0: Record start
    -----------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE '🚀 Starting Silver Layer Load';
    RAISE NOTICE 'Process started at: %', v_start_time;
    RAISE NOTICE '====================================================';

    -----------------------------------------------------------------
    -- Step 1: Transform Product Data
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 1: Loading silver.crm_prd_info...';

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key FROM 7) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key ORDER BY prd_start_dt
                ) - INTERVAL '1 DAY'
                AS DATE
            )
        FROM bronze.crm_prd_info;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.crm_prd_info loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 1 (crm_prd_info): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 2: Transform Customer Data
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 2: Loading silver.crm_cust_info...';

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY cst_id ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) subq
        WHERE flag_last = 1;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.crm_cust_info loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 2 (crm_cust_info): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 3: Transform Sales Data
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 3: Loading silver.crm_sales_details...';

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_order_dt::varchar, 'YYYYMMDD') END,
            CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_ship_dt::varchar, 'YYYYMMDD') END,
            CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
                 ELSE TO_DATE(sls_due_dt::varchar, 'YYYYMMDD') END,
            CASE WHEN sls_sales <= 0 OR sls_sales IS NULL 
                   OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price <= 0 OR sls_price IS NULL
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.crm_sales_details loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 3 (crm_sales_details): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 4: ERP Location Data
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 4: Loading silver.erp_loc_az12...';

        INSERT INTO silver.erp_loc_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) ELSE cid END,
            CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_loc_az12;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.erp_loc_az12 loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 4 (erp_loc_az12): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 5: ERP Country Data
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 5: Loading silver.erp_loc_a101...';

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.erp_loc_a101 loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 5 (erp_loc_a101): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 6: ERP Product Category
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();
        RAISE NOTICE 'Step 6: Loading silver.erp_px_cat_g1v2...';

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        RAISE NOTICE '✅ silver.erp_px_cat_g1v2 loaded: % rows in % sec',
            v_rowcount, EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT;

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Error in Step 6 (erp_px_cat_g1v2): %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 7: End
    -----------------------------------------------------------------
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;

    RAISE NOTICE '====================================================';
    RAISE NOTICE '🏁 Loading Silver Layer is Complete';
    RAISE NOTICE 'Process ended at: %', v_end_time;
    RAISE NOTICE 'Total duration: % sec', EXTRACT(EPOCH FROM v_duration)::INT;
    RAISE NOTICE '====================================================';


END;
$$;

BEGIN;
  CALL silver.load_stage_silver();
COMMIT;
