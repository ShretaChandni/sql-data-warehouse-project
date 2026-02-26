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

	============================================================================================
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_date,
prd_end_date
)

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, --- Extract category ID
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,		   --- Extract product key
prd_nm,
ISNULL(prd_cost, 0) As prd_cost, --- Replacing null with 0
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 Else 'N/A' --- Normalization or standardization
END as prd_line, --- Map product line codes to descriptive values
CAST(prd_start_date AS DATE) AS prd_start_date,
CAST(
	LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)-1 
	As DATE) 
	AS prd_end_date --- Calculate end date as one day before the next start date (Data enrichmemt)
from bronze.crm_prd_info



