CREATE OR REPLACE TABLE ANALYTICS.FACT_TABLE AS
SELECT
    -- CUSTOMER Information
    c.CARDHOLDERID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.GENDER,
    c.BRANCH_ID,
    
    -- Loan Information (No Aggregation)
    ARRAY_AGG(DISTINCT ld.CARDHOLDER_LOAN_ID) AS LOAN_IDS,

    -- Investment Information (No Aggregation)
    ARRAY_AGG(DISTINCT i.CARDHOLDER_INVESTMENT_ID) AS INVESTMENT_IDS,
    
    -- Transaction Information
    COUNT(t.TRANSACTIONID) AS TOTAL_TRANSACTIONS,
    SUM(t.TRANSACTIONAMOUNT) AS TOTAL_TRANSACTION_AMOUNT,
    
    -- Additional Metrics
    c.BALANCE_SGD AS CUSTOMER_BALANCE,
    c.INVESTMENT_PORTFOLIO_VALUE__SGD AS INVESTMENT_PORTFOLIO_VALUE,
    
    -- ATM Information
    a.LOCATIONID
FROM 
    ANALYTICS.CUSTOMER_DIMENSION c
LEFT JOIN 
    ANALYTICS.LOAN_DIMENSION ld ON c.CARDHOLDERID = ld.CARDHOLDERID
LEFT JOIN 
    ANALYTICS.INVESTMENTS_DIMENSION i ON c.CARDHOLDERID = i.CUSTOMERID
LEFT JOIN 
    ANALYTICS.TRANSACTION_DIMENSION t ON c.CARDHOLDERID = t.CARDHOLDERID
LEFT JOIN 
    ANALYTICS.ATM_DIMENSION a ON c.ATMID = a.LOCATIONID
GROUP BY 
    c.CARDHOLDERID, c.FIRST_NAME, c.LAST_NAME, c.GENDER, c.BRANCH_ID, 
    c.BALANCE_SGD, c.INVESTMENT_PORTFOLIO_VALUE__SGD, a.LOCATIONID;

CREATE OR REPLACE TABLE TRUSTBANKDATA.ANALYTICS.CUSTOMER_DIMENSION AS
SELECT 
    -- Customer Details from CUSTOMER_DEMO and CUSTOMER_LOOKUP
    c.CARDHOLDERID,
    c.GENDER,
    c.OCCUPATION,
    c.ACCOUNTTYPE,
    c.ISPRIVATEBANKING,
    c.MARITAL_STATUS,
    c.EDUCATION_LEVEL,
    c.AGE,
    c.ADDRESS,
    c.DISTRICT,
    c.CITIZENSHIP,
    c.LANGUAGE_PREFERENCE,
    c.AGE_GROUP,
    c.INCOME_FREQUENCY,
    c.HOMEOWNERSHIP,
    cl.FIRST_NAME,
    cl.LAST_NAME,
    cl.ATMID,
    cl.BIRTH_DATE,
    cl.PREFERRED_CONTACT_METHOD,
    cl.CUSTOMERSINCE,
    cl.PHONENO,
    cl.BALANCE_SGD,
    cl.BRANCH_ID,
    cl.INVESTMENT_PORTFOLIO_VALUE__SGD,

    -- Aggregated Metrics
    COUNT(DISTINCT ln.LOAN_ID) AS TOTAL_LOANS,
    SUM(ln.LOAN_AMOUNT) AS TOTAL_LOAN_AMOUNT,
    AVG(ln.INTEREST_RATE_) AS AVG_LOAN_INTEREST_RATE,
    COUNT(DISTINCT inv.INVESTMENTID) AS TOTAL_INVESTMENTS,
    SUM(inv.INVESTMENT_AMOUNT_SGD) AS TOTAL_INVESTMENT_AMOUNT,
    AVG(inv.ACTUAL_ROI) AS AVG_INVESTMENT_ROI,
    COUNT(tr.TRANSACTIONID) AS TOTAL_TRANSACTIONS,
    SUM(tr.TRANSACTIONAMOUNT) AS TOTAL_TRANSACTION_AMOUNT

FROM 
    HARMONIZED.CUSTOMER_DEMO c
JOIN 
    HARMONIZED.CUSTOMER_LOOKUP cl
    ON c.CARDHOLDERID = cl.CARDHOLDERID
LEFT JOIN 
    TRUSTBANKDATA.PUBLIC.LOAN ln 
    ON c.CARDHOLDERID = ln.CARDHOLDERID
LEFT JOIN 
    TRUSTBANKDATA.PUBLIC.INVESTMENTS inv 
    ON c.CARDHOLDERID = inv.CUSTOMERID
LEFT JOIN 
    TRUSTBANKDATA.PUBLIC.TRANSACTIONS tr 
    ON c.CARDHOLDERID = tr.CARDHOLDERID

GROUP BY 
    c.CARDHOLDERID, c.GENDER, c.OCCUPATION, c.ACCOUNTTYPE, 
    c.ISPRIVATEBANKING, c.MARITAL_STATUS, c.EDUCATION_LEVEL, 
    c.AGE, c.ADDRESS, c.DISTRICT, c.CITIZENSHIP, 
    c.LANGUAGE_PREFERENCE, c.AGE_GROUP, c.INCOME_FREQUENCY, 
    c.HOMEOWNERSHIP, cl.FIRST_NAME, cl.LAST_NAME, cl.ATMID, 
    cl.BIRTH_DATE, cl.PREFERRED_CONTACT_METHOD, cl.CUSTOMERSINCE, 
    cl.PHONENO, cl.BALANCE_SGD, cl.BRANCH_ID, cl.INVESTMENT_PORTFOLIO_VALUE__SGD;
CREATE OR REPLACE TABLE TRUSTBANKDATA.ANALYTICS.TRANSACTION_DIMENSION AS
SELECT
    -- Transaction Details
    a.TRANSACTIONID,
    a.CARDHOLDERID,
    a.TRANSACTIONAMOUNT,
    a.DATETIMEEND,
    a.DATETIMESTART,
    a.EXISTCARDHOLDER,
    a.EXISTLOC,
    b.TRANSACTIONTYPENAME,
    a.LOCATIONID,

    -- Aggregated Metrics
    COUNT(a.TRANSACTIONID) OVER (PARTITION BY a.LOCATIONID, b.TRANSACTIONTYPENAME) AS TOTAL_TRANSACTION_COUNT,
    SUM(a.TRANSACTIONAMOUNT) OVER (PARTITION BY a.LOCATIONID, b.TRANSACTIONTYPENAME) AS TOTAL_TRANSACTION_AMOUNT,
    AVG(a.TRANSACTIONAMOUNT) OVER (PARTITION BY a.LOCATIONID, b.TRANSACTIONTYPENAME) AS AVG_TRANSACTION_AMOUNT,
    COUNT(DISTINCT a.CARDHOLDERID) OVER (PARTITION BY a.LOCATIONID) AS TOTAL_UNIQUE_CUSTOMERS,

    -- Transaction Type Breakdown
    SUM(CASE WHEN a.TRANSACTIONTYPEID = 1 THEN a.TRANSACTIONAMOUNT ELSE 0 END) 
        OVER (PARTITION BY a.LOCATIONID) AS TOTAL_DEPOSIT_AMOUNT,
    SUM(CASE WHEN a.TRANSACTIONTYPEID = 2 THEN a.TRANSACTIONAMOUNT ELSE 0 END) 
        OVER (PARTITION BY a.LOCATIONID) AS TOTAL_WITHDRAWAL_AMOUNT,
    COUNT(CASE WHEN a.TRANSACTIONTYPEID = 1 THEN 1 ELSE NULL END) 
        OVER (PARTITION BY a.LOCATIONID) AS TOTAL_DEPOSIT_COUNT,
    COUNT(CASE WHEN a.TRANSACTIONTYPEID = 2 THEN 1 ELSE NULL END) 
        OVER (PARTITION BY a.LOCATIONID) AS TOTAL_WITHDRAWAL_COUNT,

    -- Time-Based Metrics
    EXTRACT(YEAR FROM a.DATETIMESTART) AS TRANSACTION_YEAR,
    EXTRACT(MONTH FROM a.DATETIMESTART) AS TRANSACTION_MONTH,
    EXTRACT(DAY FROM a.DATETIMESTART) AS TRANSACTION_DAY,

    -- Flag-Based Metrics
    COUNT(CASE WHEN a.EXISTCARDHOLDER THEN 1 ELSE NULL END) 
        OVER (PARTITION BY a.LOCATIONID) AS EXISTING_CUSTOMER_TRANSACTIONS,
    COUNT(CASE WHEN NOT a.EXISTCARDHOLDER THEN 1 ELSE NULL END) 
        OVER (PARTITION BY a.LOCATIONID) AS NEW_CUSTOMER_TRANSACTIONS,

    -- Other Aggregates
    MAX(a.TRANSACTIONAMOUNT) OVER (PARTITION BY a.LOCATIONID) AS MAX_TRANSACTION_AMOUNT,
    MIN(a.TRANSACTIONAMOUNT) OVER (PARTITION BY a.LOCATIONID) AS MIN_TRANSACTION_AMOUNT

FROM
    HARMONIZED.TRANSACTIONS AS a
JOIN
    HARMONIZED.TRANSACTIONS_TYPE_LOOKUP AS b
    ON a.TRANSACTIONTYPEID = b.TRANSACTIONTYPEID;
