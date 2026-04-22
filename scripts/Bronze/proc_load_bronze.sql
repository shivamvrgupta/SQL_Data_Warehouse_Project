/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/


create or alter procedure bronze.load_bronze as 
begin

	DECLARE @start_time DATETIME, 
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME;
	BEGIN TRY
		set @batch_start_time = GETDATE();
		print '===============================================';
		print 'Loading Bronze layer';
		print '===============================================';

		print '-----------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------';

		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.crm_prd_info

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';


		bulk insert bronze.crm_prd_info
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';



		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.crm_cust_info

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';

		bulk insert bronze.crm_cust_info
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';




		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.crm_sales_details

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';

		bulk insert bronze.crm_sales_details
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';




		SET @start_time = GETDATE();
		print '-----------------------------------------------'
		print 'Loading ERP Tables'
		print '-----------------------------------------------'

		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.erp_cust_az12

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';


		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.erp_loc_a101

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';


		SET @start_time = GETDATE();
		print '-----------------------------------------------';
		print '>> Truncating Table: bronze.crm_prd_info';
		print '-----------------------------------------------';

		truncate table bronze.erp_px_cat_g1v2

		print '-----------------------------------------------';
		print '>> Inserting data into : bronze.crm_prd_info';
		print '-----------------------------------------------';

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\ishiv\Documents\SQL DWH Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)  + 'seconds' 
		print '>>--------------';


		set @batch_end_time = GETDATE();
		PRINT '=================================================='
		PRINT 'Loading Bronze Layer is Completed'
		PRINT ' -Total Load Duration: '  + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) + 'seconds';
		PRINT '=================================================='
	END TRY
	BEGIN CATCH 
	PRINT ' =================================================='
	PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT ' Error Message' + ERROR_MESSAGE();
	PRINT ' Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT ' Error message' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT ' =================================================='
	END CATCH

end

