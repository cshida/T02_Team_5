use database trustbankdata;
use warehouse trustbank_dev_warehouse;

UPDATE Harmonized.investments
SET INVESTMENT_TYPE = 'Stocks'
WHERE INVESTMENTID = 'RI-005-1032-I414169';

SELECT * FROM Harmonized.investments_stream;

EXECUTE TASK monitor_stream_health_investment;

SELECT * FROM Harmonized.system_health_logs

