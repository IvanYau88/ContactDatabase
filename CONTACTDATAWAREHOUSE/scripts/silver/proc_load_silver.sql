/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
     	PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Contact Exports Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.contact_exports

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.contact_exports';
		TRUNCATE TABLE silver.contact_exports;
		PRINT '>> Inserting Data Into: silver.contact_exports';

		INSERT INTO silver.contact_exports(
				FIRM_ID,
				FIRM_NAME,
				FIRM_TYPE,
				[NAME],
				ASSET_CLASS,
				EMAIL,
				TEL,
				CITY,
				"STATE",
				COUNTRY,
				ZIPCODE
		)
		SELECT  
				FIRM_ID AS FIRM_ID,
				INVESTOR AS FIRM_NAME,
				[FIRM TYPE] AS FIRM_TYPE,
				[NAME] AS NAME,
				[ASSET CLASS] AS ASSET_CLASS,
				EMAIL,
				TRIM(TEL) AS TEL,
				CASE 
						WHEN CITY IS NULL AND TRIM([STATE]) = 'Columbia' THEN 'Columbia'
						ELSE CITY
				END AS CITY,
				CASE 
						WHEN TRIM([STATE]) = 'Alabama' THEN 'AL'
						WHEN TRIM([STATE]) = 'Alaska' THEN 'AK'
						WHEN TRIM([STATE]) = 'Arizona' THEN 'AZ'
						WHEN TRIM([STATE]) = 'Arkansas' THEN 'AR'
						WHEN TRIM([STATE]) = 'California' THEN 'CA'
						WHEN TRIM([STATE]) = 'Colorado' THEN 'CO'
						WHEN TRIM([STATE]) = 'Connecticut' THEN 'CT'
						WHEN TRIM([STATE]) = 'Delaware' THEN 'DE'
						WHEN TRIM([STATE]) = 'Florida' THEN 'FL'
						WHEN TRIM([STATE]) = 'Georgia' THEN 'GA'
						WHEN TRIM([STATE]) = 'Hawaii' THEN 'HI'
						WHEN TRIM([STATE]) = 'Idaho' THEN 'ID'
						WHEN TRIM([STATE]) = 'Illinois' THEN 'IL'
						WHEN TRIM([STATE]) = 'Indiana' THEN 'IN'
						WHEN TRIM([STATE]) = 'Iowa' THEN 'IA'
						WHEN TRIM([STATE]) = 'Kansas' THEN 'KS'
						WHEN TRIM([STATE]) = 'Kentucky' THEN 'KY'
						WHEN TRIM([STATE]) = 'Louisiana' THEN 'LA'
						WHEN TRIM([STATE]) = 'Maine' THEN 'ME'
						WHEN TRIM([STATE]) = 'Maryland' THEN 'MD'
						WHEN TRIM([STATE]) = 'Massachusetts' THEN 'MA'
						WHEN TRIM([STATE]) = 'Michigan' THEN 'MI'
						WHEN TRIM([STATE]) = 'Minnesota' THEN 'MN'
						WHEN TRIM([STATE]) = 'Mississippi' THEN 'MS'
						WHEN TRIM([STATE]) = 'Missouri' THEN 'MO'
						WHEN TRIM([STATE]) = 'Montana' THEN 'MT'
						WHEN TRIM([STATE]) = 'Nebraska' THEN 'NE'
						WHEN TRIM([STATE]) = 'Nevada' THEN 'NV'
						WHEN TRIM([STATE]) = 'New Hampshire' THEN 'NH'
						WHEN TRIM([STATE]) = 'New Jersey' THEN 'NJ'
						WHEN TRIM([STATE]) = 'New Mexico' THEN 'NM'
						WHEN TRIM([STATE]) = 'New York' THEN 'NY'
						WHEN TRIM([STATE]) = 'North Carolina' THEN 'NC'
						WHEN TRIM([STATE]) = 'North Dakota' THEN 'ND'
						WHEN TRIM([STATE]) = 'Ohio' THEN 'OH'
						WHEN TRIM([STATE]) = 'Oklahoma' THEN 'OK'
						WHEN TRIM([STATE]) = 'Oregon' THEN 'OR'
						WHEN TRIM([STATE]) = 'Pennsylvania' THEN 'PA'
						WHEN TRIM([STATE]) = 'Rhode Island' THEN 'RI'
						WHEN TRIM([STATE]) = 'South Carolina' THEN 'SC'
						WHEN TRIM([STATE]) = 'South Dakota' THEN 'SD'
						WHEN TRIM([STATE]) = 'Tennessee' THEN 'TN'
						WHEN TRIM([STATE]) = 'Texas' THEN 'TX'
						WHEN TRIM([STATE]) = 'Utah' THEN 'UT'
						WHEN TRIM([STATE]) = 'Vermont' THEN 'VT'
						WHEN TRIM([STATE]) = 'Virginia' THEN 'VA'
						WHEN TRIM([STATE]) = 'Washington' THEN 'WA'
						WHEN TRIM([STATE]) = 'West Virginia' THEN 'WV'
						WHEN TRIM([STATE]) = 'Wisconsin' THEN 'WI'
						WHEN TRIM([STATE]) = 'Wyoming' THEN 'WY'
						WHEN TRIM([STATE]) = 'Washington D.C.' THEN 'DC'
						WHEN TRIM([STATE]) = 'Puerto Rico' THEN 'PR'
						WHEN TRIM([STATE]) = 'Tualatin' THEN 'OR' -- Special Case
						WHEN TRIM([STATE]) = 'Boston MA' THEN 'NY' -- special Case
						WHEN TRIM([STATE]) = 'Columbia' THEN 'MD' -- Special Case
						ELSE TRIM([STATE])
				END AS [STATE],
				CASE 
					WHEN TRIM([COUNTRY/TERRITORY]) = 'US' THEN 'United States'
					WHEN TRIM([COUNTRY/TERRITORY]) = 'UK' THEN 'United Kingdom'
					ELSE TRIM([COUNTRY/TERRITORY])
				END AS COUNTRY,
				[ZIP CODE] AS ZIPCODE
		FROM bronze.contact_exports;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading Contact Exports Tables';
		PRINT '------------------------------------------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.m_d_contact_list';
		TRUNCATE TABLE silver.m_d_contact_list;
		PRINT '>> Inserting Data Into: silver.m_d_contact_list';
		INSERT INTO silver.m_d_contact_list (
	FIRM_ID,
	FIRM_NAME,
	FIRM_TYPE,
	CITY,
	COUNTRY,
	AUM,
	"ADDRESS",
	"STATE",
	"ZIPCODE",
	EMAIL,
	TEL
)
SELECT
	[FIRM ID] AS FIRM_ID,
	TRIM([FIRM NAME]) AS FIRM_NAME,
	[FIRM TYPE] AS FIRM_TYPE,
	CASE 
		WHEN CITY = 'Suite 3150' AND TRIM([STATE/COUNTY]) = 'Chicago  IL' THEN 'Chicago' -- Move Chicago to City
		ELSE CITY
	END AS CITY,
	CASE 
		WHEN TRIM(COUNTRY) = 'US' THEN 'United States'
		WHEN TRIM(COUNTRY) = 'UK' THEN 'United Kingdom'
		ELSE TRIM(COUNTRY)
	END AS COUNTRY,
	[AUM (USD MN)] AS AUM,
	CASE 
		WHEN TRIM([ADDRESS]) = '444 North Michigan Avenue' THEN '444 North Michigan Avenue Suite 3150' -- Move suite3150 in address
		ELSE TRIM([ADDRESS])
	END AS [ADDRESS],
	CASE 
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Alabama' THEN 'AL'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Alaska' THEN 'AK'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Arizona' THEN 'AZ'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Arkansas' THEN 'AR'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'California' THEN 'CA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Colorado' THEN 'CO'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Connecticut' THEN 'CT'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Delaware' THEN 'DE'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Florida' THEN 'FL'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Georgia' THEN 'GA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Hawaii' THEN 'HI'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Idaho' THEN 'ID'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Illinois' THEN 'IL'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Indiana' THEN 'IN'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Iowa' THEN 'IA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Kansas' THEN 'KS'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Kentucky' THEN 'KY'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Louisiana' THEN 'LA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Maine' THEN 'ME'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Maryland' THEN 'MD'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Massachusetts' THEN 'MA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Michigan' THEN 'MI'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Minnesota' THEN 'MN'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Mississippi' THEN 'MS'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Missouri' THEN 'MO'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Montana' THEN 'MT'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Nebraska' THEN 'NE'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Nevada' THEN 'NV'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'New Hampshire' THEN 'NH'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'New Jersey' THEN 'NJ'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'New Mexico' THEN 'NM'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'New York' THEN 'NY'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'North Carolina' THEN 'NC'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'North Dakota' THEN 'ND'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Ohio' THEN 'OH'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Oklahoma' THEN 'OK'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Oregon' THEN 'OR'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Pennsylvania' THEN 'PA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Rhode Island' THEN 'RI'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'South Carolina' THEN 'SC'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'South Dakota' THEN 'SD'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Tennessee' THEN 'TN'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Texas' THEN 'TX'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Utah' THEN 'UT'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Vermont' THEN 'VT'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Virginia' THEN 'VA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Washington' THEN 'WA'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'West Virginia' THEN 'WV'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Wisconsin' THEN 'WI'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Wyoming' THEN 'WY'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Washington D.C.' THEN 'DC'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Puerto Rico' THEN 'PR'
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Chicago  IL' THEN 'IL' -- Special Case
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = 'Pennslyvania' THEN 'PA' -- Special Case
		WHEN TRIM(REPLACE([STATE/COUNTY], '"', '')) = '"New Jersey "' THEN 'NJ' -- Special Case
		ELSE [STATE/COUNTY]
	END AS STATE,
	[ZIP CODE],
	EMAIL,
	TEL
FROM bronze.m_d_contact_list;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
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







