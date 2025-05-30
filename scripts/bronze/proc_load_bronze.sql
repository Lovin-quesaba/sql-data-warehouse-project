/*
===============================================================================
PostgreSQL Function: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This function loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV files to bronze tables.

Parameters:
    None.
    This function does not accept any parameters or return any values (returns VOID).

Usage Example:
    SELECT bronze.load_bronze();

IMPORTANT CONSIDERATIONS FOR CSV PATHS:
    The file paths (e.g., 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv')
    must be accessible by the **PostgreSQL SERVER process** on the machine
    where the PostgreSQL server is running.
    If the files are on your local machine (client), and the server is remote,
    this command will fail unless the paths are accessible from the server's perspective.
    Also, ensure the PostgreSQL server user has read permissions on these files/directories.
===============================================================================
*/
CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void -- Returns void, acting like a procedure
LANGUAGE plpgsql -- Specifies the procedural language
AS $$ -- <--- START OF PL/pgSQL BODY
DECLARE
    -- Variables for timing
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    load_duration_seconds NUMERIC;
BEGIN
    batch_start_time := NOW(); -- Get current timestamp
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- CRM_CUST_INFO
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    -- CRM_PRD_INFO
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    -- CRM_SALES_DETAILS
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ERP_LOC_A101
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    -- ERP_CUST_AZ12
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    -- ERP_PX_CAT_G1V2
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 FROM 'D:\Data Engineer Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');
    end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '>> -------------';

    batch_end_time := NOW();
    load_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', load_duration_seconds;
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '==========================================%ERROR OCCURED DURING LOADING BRONZE LAYER%Error Message: % %Error SQLSTATE: % %Error DETAIL: % ==========================================',
            E'\n', E'\n', SQLERRM, E'\n', SQLSTATE, E'\n', PG_EXCEPTION_DETAIL;
END;
$$; -- <--- END OF PL/pgSQL BODY

/*Run this query to run those function*/
SELECT bronze.load_bronze();
