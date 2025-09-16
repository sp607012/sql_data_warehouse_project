/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Loads data using the COPY command from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_stage_bronze();

===============================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_stage_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;   -- overall start time
    v_end_time   TIMESTAMP;   -- overall end time
    v_duration   INTERVAL;    -- total run duration
    t_start      TIMESTAMP;   -- per-step start time
    v_rowcount   BIGINT;      -- row count inserted
BEGIN
    -----------------------------------------------------------------
    -- Step 0: Record start
    -----------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE '🚀 Starting Bronze Layer Load';
    RAISE NOTICE 'Process started at: %', v_start_time;
    RAISE NOTICE '====================================================';

    -----------------------------------------------------------------
    -- Step 1: Truncate bronze tables
    -----------------------------------------------------------------
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    TRUNCATE TABLE bronze.erp_loc_az12;
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '✅ All bronze tables truncated.';

    -----------------------------------------------------------------
    -- Step 2: Load crm_cust_info
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_cust_info;
        CREATE TEMP TABLE stg_cust_info (LIKE bronze.crm_cust_info INCLUDING ALL);

        COPY stg_cust_info
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.crm_cust_info
        SELECT * FROM stg_cust_info;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_cust_info;

        RAISE NOTICE '✅ crm_cust_info loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading crm_cust_info: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 3: Load crm_prd_info
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_prd_info;
        CREATE TEMP TABLE stg_prd_info (LIKE bronze.crm_prd_info INCLUDING ALL);

        COPY stg_prd_info
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.crm_prd_info
        SELECT * FROM stg_prd_info;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_prd_info;

        RAISE NOTICE '✅ crm_prd_info loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading crm_prd_info: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 4: Load crm_sales_details
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_sales_details;
        CREATE TEMP TABLE stg_sales_details (LIKE bronze.crm_sales_details INCLUDING ALL);

        COPY stg_sales_details
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.crm_sales_details
        SELECT * FROM stg_sales_details;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_sales_details;

        RAISE NOTICE '✅ crm_sales_details loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading crm_sales_details: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 5: Load PX_CAT_G1V2
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_px_cat_g1v2;
        CREATE TEMP TABLE stg_px_cat_g1v2 (LIKE bronze.erp_px_cat_g1v2 INCLUDING ALL);

        COPY stg_px_cat_g1v2
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.erp_px_cat_g1v2
        SELECT * FROM stg_px_cat_g1v2;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_px_cat_g1v2;

        RAISE NOTICE '✅ PX_CAT_G1V2 loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading PX_CAT_G1V2: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 6: Load CUST_AZ12 
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_cust_az12;
        CREATE TEMP TABLE stg_cust_az12 (LIKE bronze.erp_loc_az12 INCLUDING ALL);

        COPY stg_cust_az12
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.erp_loc_az12
        SELECT * FROM stg_cust_az12;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_cust_az12;

        RAISE NOTICE '✅ CUST_AZ12 loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading CUST_AZ12: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 7: Load LOC_A101
    -----------------------------------------------------------------
    BEGIN
        t_start := clock_timestamp();

        DROP TABLE IF EXISTS stg_loc_a101;
        CREATE TEMP TABLE stg_loc_a101 (LIKE bronze.erp_loc_a101 INCLUDING ALL);

        COPY stg_loc_a101
        FROM 'D:/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ',' CSV HEADER;

        INSERT INTO bronze.erp_loc_a101
        SELECT * FROM stg_loc_a101;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        DROP TABLE stg_loc_a101;

        RAISE NOTICE '✅ LOC_A101 loaded: % rows in % sec (duration: %)',
            v_rowcount,
            EXTRACT(EPOCH FROM (clock_timestamp() - t_start))::INT,
            clock_timestamp() - t_start;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '❌ Error loading LOC_A101: %', SQLERRM;
    END;

    -----------------------------------------------------------------
    -- Step 8: End
    -----------------------------------------------------------------
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;

    RAISE NOTICE '====================================================';
    RAISE NOTICE '🏁 Bronze Layer Load Complete';
    RAISE NOTICE 'Process ended at: %', v_end_time;
    RAISE NOTICE 'Total duration: % sec (duration: %)',
        EXTRACT(EPOCH FROM v_duration)::INT,
        v_duration;
    RAISE NOTICE '====================================================';

END;
$$;

-- Run the procedure
BEGIN;
    CALL bronze.load_stage_bronze();
COMMIT;
