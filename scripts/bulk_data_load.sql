/*
===============================================================================
BULK LOAD DATA USING STORED PROCEDURE
===============================================================================
Script Purpose:
    
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.bulk_loads_bronze()
LANGUAGE plpgsql AS $$
BEGIN
    -- Truncate and load crm_cust_info
    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info FROM '/csv/crm/cust_info.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- Truncate and load crm_prd_info
    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info FROM '/csv/crm/prd_info.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- Repeat the same pattern for other tables
    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details FROM '/csv/crm/sales_details.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101 FROM '/csv/erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 FROM '/csv/erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 FROM '/csv/erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;

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






