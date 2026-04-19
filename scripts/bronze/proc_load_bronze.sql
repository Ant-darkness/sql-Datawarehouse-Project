/*

=========================================================================================================
Stored Procedure: Load Bronze Layer ( Source -> bronze )
=========================================================================================================
Script Purpose:
This Stored Procedure loads data into the 'bronze' schema from external CSV file.
It performs the following actions
    -- Truncets the bronze tables before loading data
    -- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables

Parameters:
  None.
  This Procedure does not accept any parameters or return any values.

Usage Example
    EXEC bronze.load_bronze;

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
			PRINT '===================================================================';
			PRINT 'LOADING BRONZE LAYER';
			PRINT '===================================================================';

			PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
			PRINT 'LOADING CRM TABLES';
			PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';

			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING cust_info Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_crm\cust_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';



			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING prd_info Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';



			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING sales_details Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';


			PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
			PRINT 'LOADING ERP TABLES';
			PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';

			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING cust_az12 Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_cust_az12;
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_erp\CUST_AZ12.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';


			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING loc_q101 Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_loc_q101;
			BULK INSERT bronze.erp_loc_q101
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_erp\LOC_A101.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';


			PRINT '-------------------------------------------------------------------';
			PRINT 'LOADING px_cat_g1v2 Table';
			PRINT '-------------------------------------------------------------------';

			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Abely\Desktop\DataAnalysis\BARAA\source_erp\PX_CAT_G1V2.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT'>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';

			SET @batch_end_time = GETDATE();
			PRINT'>>> BRONZE Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@';


		END TRY
		BEGIN CATCH
			PRINT '===========================================================';
			PRINT 'ERROR OCCURIED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS VARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		END CATCH
END

