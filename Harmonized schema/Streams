-- Streams

CREATE OR REPLACE STREAM trustbankdata_harmonized_transactions_stream
  ON TABLE TRUSTBANKDATA.HARMONIZED.TRANSACTIONS;

CREATE OR REPLACE STREAM trustbankdata_harmonized_investments_stream
  ON TABLE TRUSTBANKDATA.HARMONIZED.INVESTMENTS;

CREATE OR REPLACE STREAM trustbankdata_harmonized_loan_stream
  ON TABLE TRUSTBANKDATA.HARMONIZED.LOAN;

CREATE OR REPLACE TASK monitor_stream_health_transaction
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_transactions_stream;

CREATE OR REPLACE TASK monitor_stream_health_investment
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_investments_stream;

CREATE OR REPLACE TASK monitor_stream_health_loan
  WAREHOUSE = 'trustbank_ETL_Warehouse'
  SCHEDULE = '15 MINUTE'
AS
  INSERT INTO system_health_logs SELECT 'Stream Check', CURRENT_TIMESTAMP(), COUNT(*) FROM trustbankdata_harmonized_loan_stream;


