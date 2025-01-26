use role sysadmin;

use database trustbankdata;

use warehouse trustbank_dev_warehouse;

show roles;

create role if not exists trust_admin
comment = 'admin for trust bank';

create role if not exists trust_data_engineer
comment = 'data engineer for trust bank';

create role if not exists trust_analyst
comment = 'analysts for trust bank';

create role if not exists trust_read_only
comment = 'read only roles for trust bank';

grant role trust_data_engineer to role trust_admin;
grant role analyst to role trust_data_engineer;
grant role trust_read_only to role analyst;


GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE trust_admin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE trust_data_engineer;

/* GRANT USAGE OF DATABASE TO AlL ROLES */

GRANT USAGE ON DATABASE trustbankdata TO ROLE trust_admin;
GRANT USAGE ON DATABASE trustbankdata TO ROLE trust_data_engineer;
GRANT USAGE ON DATABASE trustbankdata TO ROLE trust_analyst;
GRANT USAGE ON DATABASE trustbankdata TO ROLE trust_read_only;

/* GRANT OWNERSHIP OF WAREHOUSES TO ADMIN*/

-- Grant ownership of the ETL Warehouse to trust_admin
GRANT OWNERSHIP ON WAREHOUSE trustbank_etl_warehouse TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Analytic Warehouse to trust_admin
GRANT OWNERSHIP ON WAREHOUSE trustbank_analytic_warehouse TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Reporting Warehouse to trust_admin
GRANT OWNERSHIP ON WAREHOUSE trustbank_reporting_warehouse TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Loading Warehouse to trust_admin
GRANT OWNERSHIP ON WAREHOUSE trustbank_loading_warehouse TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Development Warehouse to trust_admin
GRANT OWNERSHIP ON WAREHOUSE trustbank_dev_warehouse TO ROLE trust_admin COPY CURRENT GRANTS;



/* GRANT OWNERSHIP OF SCHEMA TO ADMIN*/

-- Grant ownership of the Raw schema to trust_admin
GRANT OWNERSHIP ON SCHEMA raw TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Harmonized schema to trust_admin
GRANT OWNERSHIP ON SCHEMA harmonized TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Analytics schema to trust_admin
GRANT OWNERSHIP ON SCHEMA analytics TO ROLE trust_admin COPY CURRENT GRANTS;

-- Grant ownership of the Public schema to trust_admin
GRANT OWNERSHIP ON SCHEMA public TO ROLE trust_admin COPY CURRENT GRANTS;


/* GRANT USGAE OF WAREHOUSE FOR EACH ROLE */

GRANT USAGE ON WAREHOUSE trustbank_etl_warehouse TO ROLE trust_admin;
GRANT USAGE ON WAREHOUSE trustbank_analytic_warehouse TO ROLE trust_admin;
GRANT USAGE ON WAREHOUSE trustbank_reporting_warehouse TO ROLE trust_admin;
GRANT USAGE ON WAREHOUSE trustbank_loading_warehouse TO ROLE trust_admin;
GRANT USAGE ON WAREHOUSE trustbank_dev_warehouse TO ROLE trust_admin;

GRANT USAGE ON WAREHOUSE trustbank_etl_warehouse TO ROLE trust_data_engineer;
GRANT USAGE ON WAREHOUSE trustbank_loading_warehouse TO ROLE trust_data_engineer;
GRANT USAGE ON WAREHOUSE trustbank_dev_warehouse TO ROLE trust_data_engineer;

GRANT USAGE ON WAREHOUSE trustbank_analytic_warehouse TO ROLE trust_analyst;
GRANT USAGE ON WAREHOUSE trustbank_reporting_warehouse TO ROLE trust_analyst;

GRANT USAGE ON WAREHOUSE trustbank_reporting_warehouse TO ROLE trust_read_only;

/* PERMISSIONS FOR ADMIN */

GRANT USAGE ON ALL SCHEMAS IN DATABASE trustbankdata TO ROLE trust_admin;

/* PERMISSIONS FOR DATA ENGINEER */

-- Permissions for Raw and Harmonize schemas
GRANT USAGE ON SCHEMA RAW TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RAW TO ROLE trust_data_engineer;

GRANT USAGE ON SCHEMA HARMONIZED TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA HARMONIZED TO ROLE trust_data_engineer;

/* PERMISSIONS FOR ANALYTICS ROLE */

-- Allow usage but no modifications in Analytics
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE trust_data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE trust_data_engineer;

GRANT USAGE ON SCHEMA Analytics TO ROLE trust_analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA Analytics TO ROLE trust_analyst;

-- Read-only access to Raw and Harmonize
GRANT USAGE ON SCHEMA Raw TO ROLE trust_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA Raw TO ROLE trust_analyst;

GRANT USAGE ON SCHEMA harmonized TO ROLE trust_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA harmonized TO ROLE trust_analyst;

/* PERMISSIONS FOR READ ONLY ROLE */

GRANT USAGE ON SCHEMA Public TO ROLE trust_read_only;
GRANT SELECT ON ALL TABLES IN SCHEMA Public TO ROLE trust_read_only;


/* FUTURE GRANTS */

-- Comprehensive future grants for Admin across all schemas in TrustBank
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TRUSTBANKDATA TO ROLE trust_admin;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE TRUSTBANKDATA TO ROLE trust_admin;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE TRUSTBANKDATA TO ROLE trust_admin;

-- Future grants for Data Engineers on the RAW and HARMONIZED schemas
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TRUSTBANKDATA TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA TRUSTBANKDATA.RAW TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA TRUSTBANKDATA.HARMONIZED TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE VIEWS IN SCHEMA TRUSTBANKDATA.RAW TO ROLE trust_data_engineer;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE VIEWS IN SCHEMA TRUSTBANKDATA.HARMONIZED TO ROLE trust_data_engineer;

-- Future grants for Analysts in the ANALYTICS schema
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TRUSTBANKDATA TO ROLE trust_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRUSTBANKDATA.ANALYTICS TO ROLE trust_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA TRUSTBANKDATA.ANALYTICS TO ROLE trust_analyst;

-- Future grants for Read-Only roles in the PUBLIC schema
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TRUSTBANKDATA TO ROLE trust_read_only;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRUSTBANKDATA.PUBLIC TO ROLE trust_read_only;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA TRUSTBANKDATA.PUBLIC TO ROLE trust_read_only;

/* The roles trust_admin and trust_data_engineer are being granted the ability to apply masking policies across the account. */CURRENT_DATE

GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE trust_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE trust_data_engineer;
