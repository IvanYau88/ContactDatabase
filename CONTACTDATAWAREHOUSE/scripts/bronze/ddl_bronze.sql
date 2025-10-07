/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.contact_exports', 'U') IS NOT NULL
    DROP TABLE bronze.contact_exports;
GO

CREATE TABLE bronze.contact_exports (
FIRM_ID INT,
CONTACT_ID INT,
INVESTOR NVARCHAR(255),
"FIRM TYPE" NVARCHAR(255),
TITLE NVARCHAR(255),
"NAME" NVARCHAR(255),
"ALTERNATIVE NAME" NVARCHAR(255),
ROLE NVARCHAR(255),
"JOB TITLE" NVARCHAR(255),
"ASSET CLASS" NVARCHAR(255),
EMAIL NVARCHAR(255),
TEL NVARCHAR(255),
CITY NVARCHAR(255),
STATE NVARCHAR(255),
"COUNTRY/TERRITORY" NVARCHAR(255),
"ZIP CODE" NVARCHAR(255),
LINKEDIN NVARCHAR(255)
)
GO


IF OBJECT_ID('bronze.m_d_contact_list', 'U') IS NOT NULL
    DROP TABLE bronze.m_d_contact_list;
GO

CREATE TABLE bronze.m_d_contact_list (
"FIRM ID" INT,
BACKGROUND NVARCHAR(max),
"FIRM NAME" NVARCHAR(255),
"FUNDS COUNT" INT,
"FIRM TYPE" NVARCHAR(255),
CITY NVARCHAR(255),
COUNTRY NVARCHAR(255),
"AUM (USD MN)" NVARCHAR(255),
REGION NVARCHAR(255),
"ADDRESS" NVARCHAR(255),
"STATE/COUNTY" NVARCHAR(255),
"ZIP CODE" NVARCHAR(255),
WEBSITE NVARCHAR(255),
EMAIL NVARCHAR(255),
TEL NVARCHAR(255)
)
