/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
-- ====================================================================
-- Checking 'silver.contact_exports'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT TEL
FROM silver.contact_exports
WHERE TEL != TRIM(TEL);

-- Data Standardization & Consistency
SELECT DISTINCT COUNTRY
FROM silver.contact_exports

-- Data Standardization & Consistency
SELECT DISTINCT STATE
FROM silver.contact_exports
WHERE COUNTRY = 'United States'

-- ====================================================================
-- Checking 'silver.m_d_contact_list'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT FIRM_NAME
FROM silver.m_d_contact_list
WHERE FIRM_NAME != TRIM(FIRM_NAME);

SELECT ADDRESS
FROM silver.m_d_contact_list
WHERE ADDRESS != TRIM(ADDRESS);

-- Data Standardization & Consistency
SELECT DISTINCT COUNTRY
FROM silver.m_d_contact_list


-- Data Standardization & Consistency
SELECT DISTINCT STATE
FROM silver.m_d_contact_list
WHERE COUNTRY = 'United States'
