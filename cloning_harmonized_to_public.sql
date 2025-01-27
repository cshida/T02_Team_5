-- Generate SQL for duplicating tables
USE ROLE SYSADMIN;
USE DATABASE trustbankdata;

USE WAREHOUSE TRUSTBANK_DEV_WAREHOUSE;

CREATE OR REPLACE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE PUBLIC.ATM_LOOKUP CLONE Harmonized.ATM_LOOKUP;
CREATE OR REPLACE TABLE PUBLIC.BRANCH CLONE Harmonized.BRANCH;
CREATE OR REPLACE TABLE PUBLIC.BRANCH_LOOKUP CLONE Harmonized.BRANCH_LOOKUP;
CREATE OR REPLACE TABLE PUBLIC.BRANCH_PERFORMANCE CLONE Harmonized.BRANCH_PERFORMANCE;
CREATE OR REPLACE TABLE PUBLIC.CALENDER_LOOKUP CLONE Harmonized.CALENDER_LOOKUP;
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_DEMO CLONE Harmonized.CUSTOMER_DEMO;
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_LOOKUP CLONE Harmonized.CUSTOMER_LOOKUP;
CREATE OR REPLACE TABLE PUBLIC.HOUR_LOOKUP CLONE Harmonized.HOUR_LOOKUP;
CREATE OR REPLACE TABLE PUBLIC.INVESTMENTS CLONE Harmonized.INVESTMENTS;
CREATE OR REPLACE TABLE PUBLIC.LOAN CLONE Harmonized.LOAN;
CREATE OR REPLACE TABLE PUBLIC.SYSTEM_HEALTH_LOGS CLONE Harmonized.SYSTEM_HEALTH_LOGS;
CREATE OR REPLACE TABLE PUBLIC.TRANSACTIONS CLONE Harmonized.TRANSACTIONS;
CREATE OR REPLACE TABLE PUBLIC.TRANSACTIONS_TYPE_LOOKUP CLONE Harmonized.TRANSACTIONS_TYPE_LOOKUP;

CREATE OR REPLACE VIEW PUBLIC.VW_BRANCH_PERFORMANCE AS SELECT * FROM Harmonized.VW_BRANCH_PERFORMANCE;
CREATE OR REPLACE VIEW PUBLIC.VW_CUSTOMER_OVERVIEW AS SELECT * FROM Harmonized.VW_CUSTOMER_OVERVIEW;
CREATE OR REPLACE VIEW PUBLIC.VW_INVESTMENT_SUMMARY AS SELECT * FROM Harmonized.VW_INVESTMENT_SUMMARY;
CREATE OR REPLACE VIEW PUBLIC.VW_LOAN_ANALYSIS AS SELECT * FROM Harmonized.VW_LOAN_ANALYSIS;
CREATE OR REPLACE VIEW PUBLIC.VW_SCHEMA_CHANGE_LOG AS SELECT * FROM Harmonized.VW_SCHEMA_CHANGE_LOG;
CREATE OR REPLACE VIEW PUBLIC.VW_TABLE_ACCESS_STATS AS SELECT * FROM Harmonized.VW_TABLE_ACCESS_STATS;
CREATE OR REPLACE VIEW PUBLIC.VW_TRANSACTION_SUMMARY AS SELECT * FROM Harmonized.VW_TRANSACTION_SUMMARY;
CREATE OR REPLACE VIEW PUBLIC.V_CUSTOMER_TRANSACTIONS_SUMMARY AS SELECT * FROM Harmonized.V_CUSTOMER_TRANSACTIONS_SUMMARY;
