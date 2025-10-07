/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.contact_exports', 'U') IS NOT NULL
    DROP TABLE silver.contact_exports;
GO

CREATE TABLE silver.contact_exports (
        FIRM_ID INT,
        FIRM_NAME NVARCHAR(255),
        FIRM_TYPE NVARCHAR(255),
        "NAME" NVARCHAR(255),
        ASSET_CLASS NVARCHAR(255),
        EMAIL NVARCHAR(255),
        TEL NVARCHAR(255),
        CITY NVARCHAR(255),
        STATE NVARCHAR(255),
        "COUNTRY" NVARCHAR(255),
        "ZIPCODE" NVARCHAR(255),
)
GO


IF OBJECT_ID('silver.m_d_contact_list', 'U') IS NOT NULL
    DROP TABLE silver.m_d_contact_list;
GO

CREATE TABLE silver.m_d_contact_list (
        FIRM_ID INT,
        FIRM_NAME NVARCHAR(255),
        FIRM_TYPE NVARCHAR(255),
        CITY NVARCHAR(255),
        COUNTRY NVARCHAR(255),
        AUM NVARCHAR(255),
        "ADDRESS" NVARCHAR(255),
        "STATE" NVARCHAR(255),
        "ZIPCODE" NVARCHAR(255),
        EMAIL NVARCHAR(255),
        TEL NVARCHAR(255)
)
