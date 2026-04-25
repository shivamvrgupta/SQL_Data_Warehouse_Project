CREATE OR ALTER PROCEDURE silver.load_silver  as
begin
	DECLARE @start_time DATETIME, 
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME;
    BEGIN TRY
		set @batch_start_time = GETDATE();
		print '===============================================';
		print 'Loading Silver layer';
		print '===============================================';

		print '-----------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------';

        -- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: silver.crm_cust_info';
		print '-----------------------------------------------';

PRINT '>> Truncating silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info
PRINT '>> Inserting Data Into: silver.crm_cust_info';
insert into silver.crm_cust_info (
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
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,

    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,
    
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS cst_gndr,  

    cst_create_date

FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    where cst_firstname IS NOT NULL
) t 
WHERE flag_last = 1;
set @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

--Loading silver.crm_prd_info'
SET @start_time = GETDATE();

PRINT '-----------------------------------------------';
PRINT 'Loading silver.crm_prd_info';
PRINT '-----------------------------------------------';

PRINT '>> Truncating silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info
PRINT '>> Inserting Data Into: silver.crm_prd_info';
insert into silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt

)

select 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
	prd_nm,
	
	ISNULL(prd_cost, 0) as prd_cost,

	CASE WHEN (UPPER(prd_line)) = 'M' then 'Mountain'
		 WHEN (UPPER(prd_line))= 'S' then 'Other Sales'
		 WHEN (UPPER(prd_line))= 'R' then 'Road'
		 WHEN (UPPER(prd_line)) = 'T' then 'Touring'
		 ELSE 'N/A'
	END AS prd_line,

	cast(prd_start_dt as DATE) as prd_start_dt,
	cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as DATE) as prd_end_dt_test
from bronze.crm_prd_info

SET @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

--Loading silver.crm_sales_details
SET @start_time = GETDATE();

PRINT '-----------------------------------------------';
PRINT 'Loading silver.crm_sales_details';
PRINT '-----------------------------------------------';

PRINT '>> Truncating silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details
PRINT '>> Inserting Data Into: silver.crm_sales_details';
insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)


select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
		 else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,

	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then NULL
		 else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,
	
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
		 else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,

	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
		 then sls_quantity * ABS(sls_price)
		 else sls_sales
	end as sls_sales,

	sls_quantity,

case when sls_price is null or sls_price <= 0 
	 then ABS(sls_sales / sls_quantity)
	 else sls_price
end as sls_price

	 from bronze.crm_sales_details

SET @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

-- Loading silver.erp_cust_az12
SET @start_time = GETDATE();

PRINT '-----------------------------------------------';
PRINT 'Loading silver.erp_cust_az12';
PRINT '-----------------------------------------------';

PRINT '>> Truncating silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12
PRINT '>> Inserting Data Into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE
        WHEN UPPER(LEFT(LTRIM(RTRIM(gen)), 1)) = 'F' THEN 'Female'
        WHEN UPPER(LEFT(LTRIM(RTRIM(gen)), 1)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS gen
FROM bronze.erp_cust_az12;

SET @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

--Loading silver.erp_loc_a101
SET @start_time = GETDATE();

PRINT '-----------------------------------------------';
PRINT 'Loading silver.erp_loc_a101';
PRINT '-----------------------------------------------';

PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Into: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN clean_cntry = '' OR clean_cntry IS NULL THEN 'N/A'
        WHEN UPPER(clean_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(clean_cntry) IN ('US', 'USA') THEN 'United States'
        ELSE clean_cntry
    END AS cntry
FROM (
    SELECT
        cid,
        TRIM(REPLACE(cntry, CHAR(13), '')) AS clean_cntry
    FROM bronze.erp_loc_a101
) cleaned_data;

SET @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

--Loading silver.erp_px_cat_g1v2
SET @start_time = GETDATE();

PRINT '-----------------------------------------------';
PRINT 'Loading silver.erp_px_cat_g1v2';
PRINT '-----------------------------------------------';

PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

insert into silver.erp_px_cat_g1v2
(id,cat,subcat,maintenance)

select 
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2

SET @end_time = GETDATE()
PRINT '>> LoadDuration:  '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds';
PRINT'>> ----------------';

SET @batch_end_time = GETDATE()
PRINT '===============================================';
PRINT 'Loading Silver layer completed';
PRINT 'Total LoadDuration:  '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) + 'seconds';
PRINT '===============================================';

end try

BEGIN CATCH
        PRINT '===============================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===============================================';
    END CATCH

end;
go
