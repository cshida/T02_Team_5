-- Tasks Below

CREATE OR REPLACE TASK refresh_materialized_view_task
  WAREHOUSE = 'trustbank_reporting_warehouse'
  SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
  select * from TRUSTBANKDATA.HARMONIZED.VW_SALES_SUMMARY;
ALTER TASK TRUSTBANKDATA.HARMONIZED.refresh_materialized_view_task RESUME;



CREATE OR REPLACE TASK monitor_stream_health_transaction
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_transactions_stream;
ALTER TASK TRUSTBANKDATA.HARMONIZED.monitor_stream_health_transaction RESUME;



CREATE OR REPLACE TASK monitor_stream_health_investment
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_investments_stream;
ALTER TASK TRUSTBANKDATA.HARMONIZED.monitor_stream_health_investment RESUME;



CREATE OR REPLACE TASK monitor_stream_health_loan
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_loan_stream;
ALTER TASK TRUSTBANKDATA.HARMONIZED.monitor_stream_health_loan RESUME;



SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME=>'REFRESH_DAILY_SALES_SUMMARY_TASK',
    SCHEDULED_TIME_RANGE_START=>DATEADD('days', -7, CURRENT_TIMESTAMP()),
    SCHEDULED_TIME_RANGE_END=>CURRENT_TIMESTAMP()
    ));
