/*
=========================================================================================================================
Quality Checks
=========================================================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver'
	schemas. It includes checks for :
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid data ranges and orders.
	- Data consistency between related fields.
Usage Notes:
	- Run these checks after loading the data into the silver layer.
	- Investigate and resolve any discrepancies found during the checks.
=========================================================================================================================
*/

--- Check For Nulls or Duplicates in Primary Key
--- Expectation: No Result

Select prd_id, count(prd_id)
From silver.crm_prd_info
GROUP BY prd_id
HAVING count(prd_id) > 1 or prd_id IS NULL

--- Check for unwanted spaces
--- Expectation: No Result
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

--- Data Standardization & Consistency

select DISTINCT cst_gndr
from silver.crm_cust_info

Select Distinct prd_line
from silver.crm_prd_info

--- check for Nulls or Negatve Numbers
--- Expectation: No Results

Select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost IS Null

--- check for Invalid Date Orders
Select * from silver.crm_prd_info
where prd_end_date < prd_start_date

--- Fixing end date which is smaller than start date
select
prd_id,
prd_key,
prd_nm,
prd_start_date,
prd_end_date,
LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)-1 As prd_end_date
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

--- check for invalid dates
select 
NULLIF (sls_order_date, 0)
from bronze.crm_sales_details
where sls_order_date <= 0 
or len(sls_order_date) != 8
or sls_order_date > 20500101
or sls_order_date < 19000101

--- check data consistency: Between sales, Quantity and price 
--- >> Sales = Quantity * Price
--- >> Values must not be NULL, Zero or negative 

select DISTINCT
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * ABS(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price
-----------------------------------------------------
--- identify out-of-range dates

select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()
-----------------------------------------------------
--- Data Standardization & Consistency
Select Distinct cntry
from bronze.erp_loc_a101
order by cntry

-----------------------------------------------------
--- Check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

-----------------------------------------------------
--- Data Standardization & Consistency
Select DISTINCT 
maintenance 
from bronze.erp_px_cat_g1v2


