# 🏢 ContactDatabase

A database of REIT (Real Estate Investment Trust) contacts.

---

## 📘 Overview

The ContactDatabase stores all REIT contacts that David has collected over time.
Data originates from multiple Excel files that were cleaned and standardized to match the following column schema:

Column Schema:
  - FIRM_ID
  - CONTACT_ID
  - INVESTOR
  - FIRM TYPE
  - TITLE
  - NAME
  - ALTERNATIVE NAME
  - ROLE
  - JOB TITLE
  - ASSET CLASS
  - EMAIL
  - TEL
  - CITY
  - STATE
  - COUNTRY/TERRITORY
  - ZIP CODE`


The scripts folder contains all SQL scripts used to initialize and load data into the database across the Bronze, Silver, and Gold layers.

---

## 🏗️ Initialization

Before loading data into any layer, make sure to run the init_database.sql script first.
This script sets up the required database schemas (bronze, silver, gold).

-- Run in SQL Server Management Studio (SSMS)
:run init_database.sql

---

## 🥉 Bronze Layer
Purpose

The Bronze Layer is the raw ingestion layer where data is loaded exactly as received from source files (CSV/Excel).
No transformations are applied here — it simply stages the data for cleaning.

Scripts

DDL Script: Defines the bronze tables (e.g., contact_exports, m_d_contact_list).

Load Procedure (proc_load): Loads CSV data into these tables using BULK INSERT.

How to Run

Run the DDL script to create tables:

:run create_bronze_tables.sql


Run the load procedure:

EXEC bronze.load_bronze;


This will truncate existing data and reload from:

sources/contact_exports.csv
sources/m_d_contact_list.csv

---

## 🥈 Silver Layer
Purpose

The Silver Layer is the cleaned and standardized layer.
Here, data from the Bronze Layer is transformed — duplicates removed, columns standardized, and data quality checks performed.

Scripts

DDL Script: Defines cleaned and structured Silver tables.

Load Procedure (proc_load): Cleans and loads data from Bronze → Silver.

Tests Folder: Contains scripts to validate data quality after cleaning.

How to Run
-- Create Silver tables
:run create_silver_tables.sql

-- Load and clean data
EXEC silver.load_silver;

---

## 🥇 Gold Layer
Purpose

The Gold Layer is the final presentation layer.
It produces curated datasets and views for reporting and sharing — specifically for David’s use.

Outputs

Creates two views:

gold.contacts_united_states — All U.S.-based contacts

gold.contacts_international — All international contacts

How to Run
-- Create Gold views
:run create_gold_views.sql

-- Query data
SELECT * FROM gold.contacts_united_states;
SELECT * FROM gold.contacts_international;

---

## 🗂️ Docs Folder

The docs folder includes:

what_i_need.txt — Notes describing what information each file should contain, for quick reference.

---

## ✅ Summary
Layer	Purpose	How to Run
Bronze	Raw data ingestion	Run DDL → EXEC bronze.load_bronze;
Silver	Data cleaning & transformation	Run DDL → EXEC silver.load_silver;
Gold	Final curated output	Run DDL → query gold views
