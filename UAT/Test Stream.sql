use database trustbankdata;
use warehouse trustbank_dev_warehouse;

UPDATE Harmonized.investments
SET INVESTMENT_TYPE = 'Stocks'
WHERE INVESTMENTID = 'RI-005-1032-I414169';

SELECT * FROM Harmonized.investments_stream;

EXECUTE TASK monitor_stream_health_investment;

SELECT * FROM Harmonized.system_health_logs

use role ACCOUNTADMIN

UPDATE Harmonized.investments
SET INVESTMENT_TYPE = 'Cryptocurrency'
WHERE INVESTMENTID = 'RI-005-1032-I414169';  -- Change to another valid INVESTMENTID if needed

SELECT * FROM Harmonized.investments_stream;

EXECUTE TASK Monitor_Stream_Health_Investment

alter warehouse TRUSTBANK_DEV_WAREHOUSE resume 

use database trustbankdata
ALTER TASK Monitor_Stream_Health_Investment RESUME;
EXECUTE TASK Monitor_Stream_Health_Investment


UPDATE TRUSTBANKDATA.HARMONIZED.INVESTMENTS
SET INVESTMENT_TYPE = 'Cryptocurrency'
WHERE INVESTMENTID = 'LA-005-1024-I452769';


SELECT * FROM TRUSTBANKDATA.HARMONIZED.INVESTMENTS_STREAM;


SELECT * FROM TRUSTBANKDATA.HARMONIZED.SYSTEM_HEALTH_LOGS
ORDER BY CHECK_TIMESTAMP DESC;


INSERT INTO TRUSTBANKDATA.HARMONIZED.SYSTEM_HEALTH_LOGS 
(CHECK_TYPE, CHECK_TIMESTAMP, RESULT_COUNT, STATUS, TABLE_NAME, USER_NAME)
VALUES ('Manual Test', CURRENT_TIMESTAMP, 1, 'PASS', 'investments', CURRENT_USER());

