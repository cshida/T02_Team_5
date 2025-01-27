-- Create and populate ATM Dimension table
CREATE OR REPLACE TABLE ANALYTICS.ATM_DIMENSION AS
SELECT *
FROM HARMONIZED.ATM_LOOKUP;

-- Create and populate Branch Dimension table
CREATE OR REPLACE TABLE ANALYTICS.BRANCH_DIMENSION AS
SELECT *
FROM HARMONIZED.BRANCH_LOOKUP;

-- Create and populate Branch Performance table
CREATE OR REPLACE TABLE ANALYTICS.BRANCH_PERFORMANCE AS
SELECT *
FROM HARMONIZED.BRANCH_PERFORMANCE;

-- Create and populate Branch Financials table
CREATE OR REPLACE TABLE ANALYTICS.BRANCH_FINANCIALS AS
SELECT *
FROM HARMONIZED.BRANCH;

-- Create and populate Loan Dimension table
CREATE OR REPLACE TABLE ANALYTICS.LOAN_DIMENSION AS 
SELECT 
    CONCAT(l.CARDHOLDERID, '-', l.LOAN_ID) AS CARDHOLDER_LOAN_ID,
    l.cardholderid,
    l.LOAN_AMOUNT,
    l.INTEREST_RATE_,
    l.LOAN_TERM_MONTHS,
    l.MONTHLY_INCOME,
    l.CREDIT_SCORE,
    l.LOAN_REPAYMENT_STATUS,
    l.LOAN_REPAID,
    l.LOAN_BALANCE
FROM HARMONIZED.LOAN AS l;

-- Create and populate Investments Dimension table
CREATE OR REPLACE TABLE ANALYTICS.INVESTMENTS_DIMENSION AS
SELECT 
    CONCAT(i.CUSTOMERID, '-', i.INVESTMENTID) AS CARDHOLDER_INVESTMENT_ID,
    i.customerid,
    i.INVESTMENT_TYPE,
    i.INVESTMENT_AMOUNT_SGD,
    i."DateTimeStart",
    i."DateTimeMature",
    i.EXPECTED_ROI_,
    i.ACTUAL_ROI_,
    i.RISK_LEVEL,
    i.PERFORMANCE_INDICATOR,
    i."ExistCUSID"
FROM HARMONIZED.INVESTMENTS AS i;

-- Create and populate Customer Dimension table
CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_DIMENSION AS
SELECT 
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
    l.FIRST_NAME,
    l.LAST_NAME,
    l.ATMID,
    l.BIRTH_DATE,
    l.PREFERRED_CONTACT_METHOD,
    l.CUSTOMERSINCE,
    l.PHONENO,
    l.BALANCE_SGD,
    l.BRANCH_ID,
    l.INVESTMENT_PORTFOLIO_VALUE__SGD
FROM HARMONIZED.CUSTOMER_DEMO c
JOIN HARMONIZED.CUSTOMER_LOOKUP l
ON c.CARDHOLDERID = l.CARDHOLDERID;

-- Create and populate Transaction Dimension table
CREATE OR REPLACE TABLE ANALYTICS.TRANSACTION_DIMENSION AS
SELECT  
  a.TRANSACTIONID,
  a.cardholderid,
  a.TRANSACTIONAMOUNT,
  a.datetimeend,
  a.datetimestart,
  a.existcardholder,
  a.existloc,
  b.TRANSACTIONTYPENAME  
FROM HARMONIZED.TRANSACTIONS AS a
JOIN HARMONIZED.TRANSACTIONS_TYPE_LOOKUP AS b
ON a.TRANSACTIONTYPEID = b.TRANSACTIONTYPEID;

-- Create and populate Fact Table
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
    ARRAY_AGG (DISTINCT t.TRANSACTIONID) AS TRANSACTION_IDS, 
    
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

-----------------------------------------------------------------------------------------

-- Add Primary Key and Foreign Key Constraints
-----------------------------------
ALTER TABLE ANALYTICS.ATM_DIMENSION
ADD CONSTRAINT PK_ATM_ID PRIMARY KEY (LOCATIONID);
------------------------------------
ALTER TABLE ANALYTICS.BRANCH_DIMENSION
ADD CONSTRAINT PK_BRANCH_ID PRIMARY KEY (BRANCH_ID);
------------------------------------
ALTER TABLE ANALYTICS.BRANCH_PERFORMANCE
ADD CONSTRAINT PK_BRANCHID_DATE PRIMARY KEY (BRANCH_ID, DATES);

ALTER TABLE ANALYTICS.BRANCH_PERFORMANCE
ADD CONSTRAINT FK_BRANCH_ID FOREIGN KEY (BRANCH_ID)
REFERENCES ANALYTICS.BRANCH_DIMENSION (BRANCH_ID);
------------------------------------
ALTER TABLE ANALYTICS.BRANCH_FINANCIALS
ADD CONSTRAINT PK_BRANCHID_DATE PRIMARY KEY (BRANCH_ID, DATE);

ALTER TABLE ANALYTICS.BRANCH_FINANCIALS
ADD CONSTRAINT FK_BRANCH_ID FOREIGN KEY (BRANCH_ID)
REFERENCES ANALYTICS.BRANCH_DIMENSION (BRANCH_ID);
------------------------------------
ALTER TABLE ANALYTICS.LOAN_DIMENSION
ADD CONSTRAINT PK_LOAN_ID PRIMARY KEY (CARDHOLDER_LOAN_ID);
------------------------------------
ALTER TABLE ANALYTICS.INVESTMENTS_DIMENSION
ADD CONSTRAINT PK_INVESTMENT_ID PRIMARY KEY (CARDHOLDER_INVESTMENT_ID);
------------------------------------
ALTER TABLE ANALYTICS.CUSTOMER_DIMENSION
ADD CONSTRAINT PK_CARDHOLDER_ID PRIMARY KEY (CARDHOLDERID);
------------------------------------
ALTER TABLE ANALYTICS.TRANSACTION_DIMENSION
ADD CONSTRAINT PK_ID PRIMARY KEY (TRANSACTIONID);
------------------------------------
-- Add Constraints for Fact Table
ALTER TABLE ANALYTICS.FACT_TABLE
ADD CONSTRAINT PK_FACT_TABLE PRIMARY KEY (CARDHOLDERID);

ALTER TABLE ANALYTICS.FACT_TABLE
ADD CONSTRAINT FK_FACT_CUSTOMERID FOREIGN KEY (CARDHOLDERID)
REFERENCES ANALYTICS.CUSTOMER_DIMENSION (CARDHOLDERID);

ALTER TABLE ANALYTICS.FACT_TABLE
ADD CONSTRAINT FK_FACT_BRANCHID FOREIGN KEY (BRANCH_ID)
REFERENCES ANALYTICS.BRANCH_DIMENSION (BRANCH_ID);

ALTER TABLE ANALYTICS.FACT_TABLE
ADD CONSTRAINT FK_FACT_ATMID FOREIGN KEY (LOCATIONID)
REFERENCES ANALYTICS.ATM_DIMENSION (LOCATIONID);
------------------------------------
-- Fact Loan Intermediary
CREATE OR REPLACE TABLE ANALYTICS.FACT_LOAN_INTERMEDIARY AS
SELECT 
    f.CARDHOLDERID,
    CAST(ld.VALUE AS VARCHAR(511)) AS CARDHOLDER_LOAN_ID
FROM 
    ANALYTICS.FACT_TABLE f
JOIN LATERAL FLATTEN(input => f.LOAN_IDS) AS ld;

ALTER TABLE ANALYTICS.FACT_LOAN_INTERMEDIARY
ADD CONSTRAINT FK_FACT_LOAN_CARDHOLDERID FOREIGN KEY (CARDHOLDERID)
REFERENCES ANALYTICS.FACT_TABLE (CARDHOLDERID);

ALTER TABLE ANALYTICS.FACT_LOAN_INTERMEDIARY 
ADD CONSTRAINT FK_LOAN_CARDHOLDER_LOAN_ID FOREIGN KEY (CARDHOLDER_LOAN_ID)
REFERENCES ANALYTICS.LOAN_DIMENSION(CARDHOLDER_LOAN_ID);

-- Fact Investment Intermediary
CREATE OR REPLACE TABLE ANALYTICS.FACT_INVESTMENT_INTERMEDIARY AS
SELECT 
    f.CARDHOLDERID,
    CAST(i.VALUE AS VARCHAR(511)) AS CARDHOLDER_INVESTMENT_ID
FROM 
    ANALYTICS.FACT_TABLE f
JOIN LATERAL FLATTEN(input => f.INVESTMENT_IDS) AS i;

ALTER TABLE ANALYTICS.FACT_INVESTMENT_INTERMEDIARY
ADD CONSTRAINT FK_FACT_INVESTMENT_CARDHOLDERID FOREIGN KEY (CARDHOLDERID)
REFERENCES ANALYTICS.FACT_TABLE (CARDHOLDERID);

ALTER TABLE ANALYTICS.FACT_INVESTMENT_INTERMEDIARY
ADD CONSTRAINT FK_FACT_INVESTMENT_ID FOREIGN KEY (CARDHOLDER_INVESTMENT_ID)
REFERENCES ANALYTICS.INVESTMENTS_DIMENSION (CARDHOLDER_INVESTMENT_ID);

--Transaction Investment Intermediary 
CREATE OR REPLACE TABLE ANALYTICS.FACT_TRANSACTION_INTERMEDIARY AS
SELECT 
    f.CARDHOLDERID,
    CAST(i.VALUE AS VARCHAR(255)) AS TRANSACTIONID
FROM 
    ANALYTICS.FACT_TABLE f
JOIN LATERAL FLATTEN(input => f.TRANSACTION_IDS) AS i;

ALTER TABLE ANALYTICS.FACT_TRANSACTION_INTERMEDIARY
ADD CONSTRAINT FK_FACT_CARDHOLDER_ID FOREIGN KEY (CARDHOLDERID)
REFERENCES ANALYTICS.FACT_TABLE (CARDHOLDERID);

ALTER TABLE ANALYTICS.FACT_TRANSACTION_INTERMEDIARY 
ADD CONSTRAINT FK_FACT_TRANSACTION_ID FOREIGN KEY (TRANSACTIONID)
REFERENCES ANALYTICS.TRANSACTION_DIMENSION (TRANSACTIONID);