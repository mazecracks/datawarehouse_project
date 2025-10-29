/*
===============================================================================
BULK LOAD DATA USING STORED PROCEDURE WITH ERROR HANDLING
===============================================================================
Script Purpose:
    
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.bulk_loads_bronze()
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    elapsed_time INTERVAL;
	
BEGIN

	-- Record the start time
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Start time: %', start_time;

	RAISE NOTICE  '====================================';
	RAISE NOTICE  'LOADING BRONZE LAYER';
	RAISE NOTICE  '====================================';
		
    -- Truncate and load crm_cust_info
	RAISE NOTICE  '====================================';
	RAISE NOTICE  'LOADING CRM TABLES';
	RAISE NOTICE  '====================================';
	
    BEGIN	
		
		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.crm_cust_info';
		RAISE NOTICE  '-------------------------------------';
        TRUNCATE TABLE bronze.crm_cust_info;
        
		RAISE NOTICE  '------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.crm_cust_info';
		RAISE NOTICE  '------------------------------------';
		COPY bronze.crm_cust_info FROM '/csv/crm/cust_info.csv' DELIMITER ',' CSV HEADER;
		
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- Truncate and load crm_prd_info
    BEGIN

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.crm_prd_info';
		RAISE NOTICE  '-------------------------------------';
		TRUNCATE TABLE bronze.crm_prd_info;
        
		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.crm_prd_info';
		RAISE NOTICE  '-------------------------------------';
		COPY bronze.crm_prd_info FROM '/csv/crm/prd_info.csv' DELIMITER ',' CSV HEADER;
		
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;


    BEGIN

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.crm_sales_details';
		RAISE NOTICE  '-------------------------------------';
		TRUNCATE TABLE bronze.crm_sales_details;

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.crm_sales_details';
		RAISE NOTICE  '-------------------------------------';
		COPY bronze.crm_sales_details FROM '/csv/crm/sales_details.csv' DELIMITER ',' CSV HEADER;
    
	EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;


    RAISE NOTICE  '====================================';
	RAISE NOTICE  'LOADING ERP TABLE';
	RAISE NOTICE  '====================================';
	
    BEGIN
        
		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.erp_loc_a101';
		RAISE NOTICE  '-------------------------------------';
		TRUNCATE TABLE bronze.erp_loc_a101;
        
		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.erp_loc_a101';
		RAISE NOTICE  '-------------------------------------';
		COPY bronze.erp_loc_a101 FROM '/csv/erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
    
	EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
    END;


    BEGIN

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.erp_cust_az12';
		RAISE NOTICE  '-------------------------------------';
        TRUNCATE TABLE bronze.erp_cust_az12;

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.erp_cust_az12';
		RAISE NOTICE  '-------------------------------------';
        COPY bronze.erp_cust_az12 FROM '/csv/erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
    
	EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    BEGIN

		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> TRUNCATING TABLE:bronze.erp_px_cat_g1v2';
		RAISE NOTICE  '-------------------------------------';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
		RAISE NOTICE  '-------------------------------------';
		RAISE NOTICE  '>> COPYING TABLE:bronze.erp_px_cat_g1v2';
		RAISE NOTICE  '-------------------------------------';
		COPY bronze.erp_px_cat_g1v2 FROM '/csv/erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
    
	EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;


	-- Record the end time
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'End time: %', end_time;

    -- Calculate and display the elapsed time
    elapsed_time := end_time - start_time;
    RAISE NOTICE 'Total execution time: %', elapsed_time;
	
	-- Final message
    RAISE NOTICE '====================================';
    RAISE NOTICE 'BRONZE LAYER LOADING COMPLETE';
    RAISE NOTICE '====================================';

END;
$$;

CALL bronze.bulk_loads_bronze();





/*
===============================================================================
BULK LOAD DATA USING FUNCTION
===============================================================================
Script Purpose:
  
===============================================================================
*/



CREATE OR REPLACE FUNCTION bulk_load_csv_data() 
RETURNS void AS $$
BEGIN
    BEGIN
	    TRUNCATE TABLE bronze.crm_cust_info; 
        COPY bronze.crm_cust_info FROM '/csv/crm/cust_info.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    BEGIN
	    TRUNCATE TABLE bronze.crm_prd_info; 
        COPY bronze.crm_prd_info FROM '/csv/crm/prd_info.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;

    BEGIN
	    TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details FROM '/csv/crm/sales_details.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

	
    BEGIN
	    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 FROM '/csv/erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

	
    BEGIN
	    TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 FROM '/csv/erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

	
    BEGIN
	    TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101 FROM '/csv/erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

END;
$$ LANGUAGE plpgsql;


SELECT bulk_load_csv_data();






