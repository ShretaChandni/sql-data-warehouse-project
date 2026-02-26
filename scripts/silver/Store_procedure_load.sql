/*EXEC silver.load_silver;*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==================================================================='
		PRINT 'Loading silver Layer'
		PRINT '==================================================================='

		PRINT '...................................................................'
		PRINT 'Loading CRM Layer'
		PRINT '...................................................................'

		---- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';

		---- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
			AS prd_end_date --- Calculate end date as one day before the next start date (Data enrichment)
		from bronze.crm_prd_info

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';

		---- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_date,
		sls_ship_date,
		sls_due_date,
		sls_sales,
		sls_quantity,
		sls_price
		)

		Select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_date = 0 or len(sls_order_date) != 8 then null -- Handling missing/invalid data
			else cast(cast(sls_order_date as varchar) as date) --- Handling date format
		end as sls_order_date,
		CASE WHEN sls_ship_date = 0 or len(sls_ship_date) != 8 then null -- Handling missing/invalid data
			else cast(cast(sls_ship_date as varchar) as date) --- Handling date format
		end as sls_ship_date,
		CASE WHEN sls_due_date = 0 or len(sls_due_date) != 8 then null -- Handling missing/invalid data
			else cast(cast(sls_due_date as varchar) as date) --- Handling date format
		end as sls_due_date,
		CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
				then sls_quantity * ABS(sls_price) --- Data enrichment by deriving information (business rules)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0 -- Handling missing/invalid data
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price                            --- Data enrichment by deriving information (business rules)
		end as sls_price
		from bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';

		---- Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid like 'NAS%' THEN substring(cid, 4, len(cid)) --- Remove 'NAS' prefix if present
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate 
		END AS bdate, --- Set future birthdates to NULL
		CASE WHEN TRIM(UPPER(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'N/A'
		END AS gen   --- Normalize gender values and handle unknown cases
		from bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';
	
		---- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		select 
		REPLACE(cid, '-', '') As cid,  --- Handled invalid value 
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			 ELSE TRIM(cntry)
		END AS cntry   --- Normalize and Handle missing or blank country codes
		from bronze.erp_loc_a101
	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';


		---- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ............................';
	END TRY 
	BEGIN CATCH 
		PRINT '========================================================='
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================='
	END CATCH
END
