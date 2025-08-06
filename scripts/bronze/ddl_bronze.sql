CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '=================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================';
		PRINT '------------------------------';
		PRINT 'Loadind CRM Tables';
		PRINT '------------------------------';
		
		-- Declaring the two variables to get start and end time!
		DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME, @batch_start_time DATETIME;
		
		--to calcualte the whole batch
		SET @batch_start_time = GETDATE();

		--to calculate particular table load time
		SET @layer_start_time = GETDATE();

		--Truncating and Loading the relevant CSV File into the "bronze.crm_cust_info" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.crm_cust_info = '+ CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------';
		

		--Truncating and Loading the relevant CSV File into the "bronze.crm_prd_info" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.crm_prd_info = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		--SELECT * FROM bronze.crm_prd_info;


		--Truncating and Loading the relevant CSV File into the "bronze.crm_sales_details" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.crm_sales_details = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		--SELECT * FROM bronze.crm_sales_details;

		SET @layer_end_time = GETDATE();
		PRINT '>>Total CRM Tables Load Time = ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------';


		SET @layer_start_time = GETDATE();
		PRINT '------------------------------';
		PRINT 'Loadind ERP Tables';
		PRINT '------------------------------';
		--Truncating and Loading the relevant CSV File into the "bronze.erp_cust_az12" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.erp_cust_az12 = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		--SELECT * FROM bronze.erp_cust_az12;


		--Truncating and Loading the relevant CSV File into the "bronze.erp_loc_a101" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.erp_loc_a101 = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		--SELECT * FROM bronze.erp_loc_a101;


		--Truncating and Loading the relevant CSV File into the "bronze.erp_px_cat_g1v2" table below
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'G:\SQL Course Material\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration of TABLE bronze.erp_px_cat_g1v2 = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------------------';
		SET @layer_end_time = GETDATE();
		PRINT '>>Total ERP Tables Load Time = ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------';
		--SELECT * FROM bronze.erp_px_cat_g1v2;
		PRINT '------------------------------';
		PRINT 'Loadind Completed!';
		PRINT '------------------------------';
		PRINT '>>Whole Process Duration = ' + CAST(DATEDIFF(second, @batch_start_time, @layer_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '======================================';
		PRINT 'An Error Occured While Loading Bronze Layer';
		PRINT '======================================';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);

	END CATCH
END


EXEC bronze.load_bronze;
