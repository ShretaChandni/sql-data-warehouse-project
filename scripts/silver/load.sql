/*===================================================================
  Loading crm_cust_info after transformation in the silver schema
===================================================================*/


TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname, -- Removing unwanted spaces/charatcer
TRIM(cst_lastname) AS cst_lastname, -- Removing unwanted spaces/charatcer
CASE WHEN Upper(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 WHEN Upper(TRIM(cst_material_status)) = 'M' THEN 'Married'
	 ELSE 'N/A'
END cst_material_status, -- Normalize material status values to readable format
CASE WHEN Upper(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN Upper(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'N/A'
END cst_gndr, 
cst_create_date -- Normalize gender values to readable format
FROM 
(SELECT * ,
ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(cst_id AS VARCHAR)) ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL)t WHERE flag_last = 1 -- Select the most recent record per customer
