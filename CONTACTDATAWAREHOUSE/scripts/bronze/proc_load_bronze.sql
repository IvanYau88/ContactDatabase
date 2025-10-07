/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze; 
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
		DECLARE @start_time DATEtime, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Contact Exports Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.contact_exports';
		TRUNCATE TABLE bronze.contact_exports; 

		PRINT '>> Inserting Data into: bronze.contact_exports';
		BULK INSERT bronze.contact_exports
		FROM 'C:\Users\ivany\Documents\Personal\Hoya Capital\CONTACTDATAWAREHOUSE\sources\contact_exports.csv' -- Change this line based on where the source files are stored
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'second'
		PRINT '>> -------------';

		PRINT '---------------------------------------------------';
		PRINT 'Loading Mary J and David A Prospect Investor Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.m_d_contact_list';
		TRUNCATE TABLE bronze.m_d_contact_list; 
		PRINT '>> Inserting Data into: bronze.m_d_contact_list';
		BULK INSERT bronze.m_d_contact_list
		FROM 'C:\Users\ivany\Documents\Personal\Hoya Capital\CONTACTDATAWAREHOUSE\sources\m_d_contact_list.csv' -- Change this line based on where the source files are stored
		WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK 
		);

		SET @batch_end_time = GETDATE()
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
