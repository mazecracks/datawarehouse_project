-- data cleaning 

-- checking for duplicate values
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id = NULL

-- CHECK SPECIFIC DUPLICSATE
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29473 

--- NUMBER THE DUPLICATE USING A WINDOWS FUNCTION ROWNUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last FROM bronze.crm_cust_info
WHERE cst_id = 29466

--- CHECK SPECIFIC NON DUPLICSATE USING SUBQUERY

SELECT * FROM (

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info

) AS t WHERE flag_last = 1


--------------- DUPLICATES

SELECT * FROM (

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info

) AS t WHERE flag_last != 1 


------- CHECK WHERE THERE IS EXTRA SPACE IN THE ATTRIBUTES

SELECT * FROM bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname)
SELECT * FROM bronze.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname)



------

SELECT DISTINCT cst_gndr from bronze.crm_cust_info
SELECT DISTINCT cst_marital_status from bronze.crm_cust_info


------- data cleaning

SELECT
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     ELSE 'N/A'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'N/A'
END cst_gndr,

cst_create_date
FROM (

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL

) AS t WHERE flag_last = 1


-------------- LOAD CLEANED DATA INTO SILVER LAYER

INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)

SELECT
cst_id,
cst_key,
TRIM (cst_firstname) AS cst_firstname,
TRIM (cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     ELSE 'N/A'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'N/A'
END cst_gndr,

cst_create_date
FROM (

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL

) AS t WHERE flag_last = 1
