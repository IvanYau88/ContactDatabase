/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.contacts_united_states
-- =============================================================================

IF OBJECT_ID('gold.contacts_united_states', 'V') IS NOT NULL
    DROP VIEW gold.contacts_united_states;
GO

CREATE VIEW gold.contacts_united_states AS
WITH ContactExportUS AS (
        SELECT
            FIRM_ID,
            FIRM_NAME,
            FIRM_TYPE,
            NAME,
            ASSET_CLASS,
            EMAIL,
            TEL,
            CITY,
            STATE,
            ZIPCODE
        FROM silver.contact_exports
        WHERE COUNTRY = 'United States'
    ),
    MDContactUS AS (
        SELECT
            FIRM_ID,
            FIRM_NAME,
            FIRM_TYPE,
            AUM,
            ADDRESS,
            CITY,
            STATE,
            ZIPCODE,
            EMAIL,
            TEL
        FROM silver.m_d_contact_list
        WHERE COUNTRY = 'United States'
    )
    SELECT
        -- Use COALESCE to get the first non-null value from either table
        COALESCE(ce.FIRM_NAME, md.FIRM_NAME)   AS Firm_Name,
        COALESCE(ce.FIRM_TYPE, md.FIRM_TYPE)   AS Firm_Type,
        ce.NAME                                AS Contact_Name,  -- This is unique to the contacts table
        ce.ASSET_CLASS                         AS Asset_Class,   -- This is unique to the contacts table
        COALESCE(ce.EMAIL, md.EMAIL)           AS Email,
        COALESCE(ce.TEL, md.TEL)               AS Phone_Number,
        md.ADDRESS                             AS Address,
        COALESCE(ce.CITY, md.CITY)             AS City,
        COALESCE(ce.STATE, md.STATE)           AS "State",
        COALESCE(ce.ZIPCODE, md.ZIPCODE)       AS Zipcode,
        md.AUM                                 AS AUM           -- This is unique to the M and D table
    FROM ContactExportUS AS ce
    FULL OUTER JOIN MDContactUS AS md 
        ON ce.FIRM_ID = md.FIRM_ID;
GO


-- =============================================================================
-- Create View: gold.contacts_international
-- =============================================================================
IF OBJECT_ID('gold.contacts_international', 'V') IS NOT NULL
    DROP VIEW gold.contacts_international;
GO

CREATE VIEW gold.contacts_international AS
WITH ContactExportIT AS (
        SELECT
            FIRM_ID,
            FIRM_NAME,
            FIRM_TYPE,
            NAME,
            ASSET_CLASS,
            EMAIL,
            TEL,
            CITY,
            STATE,
            ZIPCODE
        FROM silver.contact_exports
        WHERE COUNTRY != 'United States'
    ),
      MDContactIT AS (
        SELECT
            FIRM_ID,
            FIRM_NAME,
            FIRM_TYPE,
            AUM,
            ADDRESS,
            CITY,
            STATE,
            ZIPCODE,
            EMAIL,
            TEL
        FROM silver.m_d_contact_list
        WHERE COUNTRY != 'United States'
    )
    SELECT
        -- Use COALESCE to get the first non-null value from either table
        COALESCE(ce.FIRM_NAME, md.FIRM_NAME)   AS Firm_Name,
        COALESCE(ce.FIRM_TYPE, md.FIRM_TYPE)   AS Firm_Type,
        ce.NAME                                AS Contact_Name,  -- This is unique to the contacts table
        ce.ASSET_CLASS                         AS Asset_Class,   -- This is unique to the contacts table
        COALESCE(ce.EMAIL, md.EMAIL)           AS Email,
        COALESCE(ce.TEL, md.TEL)               AS Phone_Number,
        md.ADDRESS                             AS Address,
        COALESCE(ce.CITY, md.CITY)             AS City,
        COALESCE(ce.STATE, md.STATE)           AS "State",
        COALESCE(ce.ZIPCODE, md.ZIPCODE)       AS Zipcode,
        md.AUM                                 AS AUM           -- This is unique to the M and D table
    FROM ContactExportIT AS ce
    FULL OUTER JOIN MDContactIT AS md 
        ON ce.FIRM_ID = md.FIRM_ID;



--Select * From gold.contacts_united_states