use database trustbankdata;
use schema trustbankdata.analytics;

use warehouse trustbank_loading_warehouse;



CREATE OR REPLACE DYNAMIC TABLE dt_dim_branch_lookup
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
SELECT
    Branch_ID,
    Location AS branch_location,
    No_of_Accounts AS total_accounts,
    Customer_Satisfaction AS customer_satisfaction_score,
    Average_Transaction_Time_Min AS avg_transaction_time_mins,
    RECENT_YEARLY_REVENUE_SGD AS yearly_revenue,
    RECENT_YEARLY_COST_SGD AS yearly_costs,
    Customer_Retention_Rate AS customer_retention_rate,
    CUSTOMER_RETENTION_RATE_PERCENTAGE AS customer_retention_rate_percent
FROM harmonized.branch_lookup;

CREATE OR REPLACE DYNAMIC TABLE dt_dim_customer
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
SELECT
    cl.CardHolderID,
    cl.Branch_ID,
    cl.ATMID,
    cl.First_Name AS first_name,
    cl.Last_Name AS last_name,
    cl.Gender,
    cl.Birth_Date AS birth_date,
    cl.AccountType AS account_type,
    cl.IsPrivateBanking AS is_private_banking,
    cl.Preferred_Contact_Method AS preferred_contact_method,
    cl.CustomerSince AS JoinDate,
    cl.PhoneNo AS phone_number,
    cl.Balance_SGD AS balance_sgd,
    cl.INVESTMENT_PORTFOLIO_VALUE_SGD AS investment_portfolio_value,
    cd.Occupation,
    cd.Marital_Status AS marital_status,
    cd.Education_Level AS education_level,
    cd.Age,
    cd.Address,
    cd.District,
    cd.Citizenship,
    cd.Language_Preference AS language_preference,
    cd.Age_Group AS age_group,
    cd.Income_Frequency AS income_frequency,
    cd.HomeOwnership AS home_ownership
FROM harmonized.customer_lookup cl
JOIN harmonized.customer_demo cd ON cl.CardHolderID = cd.CardHolderID;

CREATE OR REPLACE DYNAMIC TABLE dt_dim_atm
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
SELECT
    LocationID,
    Location_Name AS location_name,
    No_of_ATMs AS total_atms,
    City,
    State,
    Country,
    Installation_Date AS installation_date,
    Maintenance_Date AS maintenance_date,
    Operational_Status AS operational_status,
    Cash_Deposit_Available AS cash_deposit_available
FROM harmonized.atm_lookup;

CREATE OR REPLACE TABLE dt_dim_calendar
AS
SELECT
    Date,
    day_name
    Year,
    EDATE,
    EMONTH,
    Quarter,
    isholiday,
    DAY_OF_WEEK AS day_of_week,
FROM harmonized.calender_lookup;

CREATE OR REPLACE TABLE dt_dim_hour_lookup
AS
SELECT
    hour_key,
    hour_start_time,
    hour_end_time
FROM harmonized.hour_lookup;

CREATE OR REPLACE DYNAMIC TABLE dt_fact_branch_performance
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = full
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        Branch_ID,
        Dates,
        ATM_Usage_Count,
        AVERAGE_TRANSACTION_TIME_MINUTES,
        Customer_Satisfaction_Score,
        Compliance_Issue_Count,
        Fraudulent_Transactions_Count,
        NEW_ACCOUNTS_OPENED_COUNT,
        ACCOUNTS_CLOSED_COUNT,
        CREDIT_CARD_APPLICATIONS_RECEIVED_COUNT,
        BRANCH_OPERATIONAL_HOURS_HOURS,
        ATM_OPERATIONAL_HOURS_HOURS,
        MAINTENANCE_ISSUES_REPORTED_COUNT,
        ATM_Downtime_Hours,
        Transaction_Error_Count,
        NO_OF_COMPLAINTS_RESOLVED_COUNT
    FROM harmonized.branch_performance
)
SELECT
    db.Branch_ID,
    dd.Date,
    -- Measures
    COUNT(nk.Branch_ID) AS total_branches,
    SUM(nk.ATM_Usage_Count) AS total_atm_usage,
    AVG(nk.AVerage_Transaction_Time_minutes) AS avg_transaction_time,
    AVG(nk.Customer_Satisfaction_Score) AS avg_customer_satisfaction,
    SUM(nk.Compliance_Issue_Count) AS total_compliance_issues,
    SUM(nk.Fraudulent_Transactions_Count) AS total_fraudulent_transactions,
    SUM(nk.NEW_ACCOUNTS_OPENED_COUNT) AS total_accounts_opened,
    SUM(nk.CREDIT_CARD_APPLICATIONS_RECEIVED_COUNT) AS total_accounts_closed,
    SUM(nk.CREDIT_CARD_APPLICATIONS_RECEIVED_COUNT) AS total_credit_card_applications,
    SUM(nk.BRANCH_OPERATIONAL_HOURS_HOURS) AS total_branch_operational_hours,
    SUM(nk.ATM_Operational_Hours_Hours) AS total_atm_operational_hours,
    SUM(nk.MAINTENANCE_ISSUES_REPORTED_COUNT) AS total_maintenance_issues,
    SUM(nk.ATM_Downtime_Hours) AS total_atm_downtime,
    SUM(nk.Transaction_Error_Count) AS total_transaction_errors,
    SUM(nk.NO_OF_COMPLAINTS_RESOLVED_COUNT) AS total_complaints_resolved
FROM natural_keys nk
JOIN analytics.dt_dim_branch_lookup db ON db.Branch_ID = nk.Branch_ID
JOIN analytics.dt_dim_calendar dd ON dd.Date = nk.Dates
GROUP BY db.Branch_ID, dd.Date;

CREATE OR REPLACE DYNAMIC TABLE dt_fact_branch_finance
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        Branch_ID,
        Date,
        BRANCH_REVENUE_SGD,
        BRANCH_EXPENSES_SGD,
        Net_Income_sgd,
        AVG_DAILY_BRANCH_TRANSACTION_VALUE_SGD,
        PENDING_TRANSACTIONS_COUNT,
        BRANCH_AVG_TRANSACTION_QUEUE_LENGTH,
        DIGITAL_TRANSACTIONS_VOLUME_,
        Branch_Internet_Downtime_hours
    FROM harmonized.branch
)
SELECT
    db.Branch_ID,
    dd.Date,
    -- Measures
    SUM(nk.BRANCH_REVENUE_SGD) AS total_branch_revenue,
    SUM(nk.BRANCH_EXPENSES_SGD) AS total_branch_expenses,
    SUM(nk.Net_Income_sgd) AS total_net_income,
    AVG(nk.AVG_DAILY_BRANCH_TRANSACTION_VALUE_SGD) AS avg_daily_transaction_value,
    SUM(nk.PENDING_TRANSACTIONS_COUNT) AS total_pending_transactions,
    AVG(nk.BRANCH_AVG_TRANSACTION_QUEUE_LENGTH) AS avg_transaction_queue_length,
    AVG(nk.Digital_Transactions_Volume_) AS avg_digital_transaction_volume,
    SUM(nk.Branch_Internet_Downtime_hours) AS total_internet_downtime
FROM natural_keys nk
JOIN analytics.dt_dim_branch_lookup db ON db.Branch_ID = nk.Branch_ID
JOIN analytics.dt_dim_calendar dd ON dd.Date = nk.Date
GROUP BY db.Branch_ID, dd.Date;

CREATE OR REPLACE DYNAMIC TABLE dt_fact_loans
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        CardHolderID,
        LoanID,
        Loan_Amount,
        Interest_Rate,
        Loan_Term_Months,
        Monthly_Income,
        Credit_Score,
        Loan_Repayment_Status,
        Loan_Repaid,
        Loan_Balance
    FROM harmonized.loan
)
SELECT
    LoanID,
    dc.CardHolderID,
    -- Measures
    SUM(nk.Loan_Amount) AS total_loan_amount,
    AVG(nk.Interest_Rate) AS avg_interest_rate,
    AVG(nk.Loan_Term_Months) AS avg_loan_term,
    SUM(nk.Monthly_Income) AS total_monthly_income,
    AVG(nk.Credit_Score) AS avg_credit_score,
    COUNT(nk.Loan_Repayment_Status) AS total_loans,
    SUM(nk.Loan_Repaid) AS total_loans_repaid,
    SUM(nk.Loan_Balance) AS total_loan_balance
FROM natural_keys nk
JOIN analytics.dt_dim_customer dc ON dc.CardHolderID = nk.CardHolderID
GROUP BY dc.CardHolderID,LoanID;

CREATE OR REPLACE DYNAMIC TABLE dt_fact_investments
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        CardHolderID,
        InvestmentID,
        Investment_Type,
        INVESTMENT_AMOUNT_SGD,
        "DateTimeStart",
        "DateTimeMature",
        EXPECTED_ROI_,
        ACTUAL_ROI_,
        Risk_Level,
        Performance_Indicator
    FROM harmonized.investments
)
SELECT
    InvestmentID,
    dc.CardHolderID,
    risk_level,
    performance_indicator,
    investment_type,
    "DateTimeStart",
    "DateTimeMature",
    -- Measures
    SUM(nk.INVESTMENT_AMOUNT_SGD) AS total_investment_amount,
    AVG(nk.Expected_ROI_) AS avg_expected_roi,
    AVG(nk.Actual_ROI_) AS avg_actual_roi
FROM natural_keys nk
JOIN analytics.dt_dim_customer dc ON dc.CardHolderID = nk.CardHolderID
GROUP BY InvestmentID,
    dc.CardHolderID,
    risk_level,
    performance_indicator,
    investment_type,
    "DateTimeStart",
    "DateTimeMature";



CREATE OR REPLACE DYNAMIC TABLE dt_fact_transactions
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        t.TransactionID,
        t.CardHolderID,
        t.TransactionTypeID,
        t.Datetimestart,
        t.datetimeend,
        t.TransactionAmount,
        t.locationid,
        tl.TRANSACTIONTYPENAME AS transaction_type_name
    FROM harmonized.transactions t
    JOIN harmonized.transactions_type_lookup tl ON t.TransactionTypeID = tl.TransactionTypeID
)

SELECT
    da.LocationID as ATM_Location_ID,
    -- Measures
    COUNT(nk.TransactionID) AS total_transactions,
    SUM(nk.TransactionAmount) AS total_transaction_amount,
    AVG(nk.TransactionAmount) AS avg_transaction_amount,
    nk.transaction_type_name
FROM natural_keys nk
JOIN analytics.dt_dim_customer dc ON dc.CardHolderID = nk.CardHolderID
JOIN analytics.dt_dim_atm da ON da.LocationID = nk.LocationID
GROUP BY dc.CardHolderID, da.LocationID, nk.transaction_type_name;
CREATE OR REPLACE DYNAMIC TABLE dt_fact_transactions
TARGET_LAG = 'DOWNSTREAM'
WAREHOUSE = trustbank_dev_warehouse
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
AS
WITH natural_keys AS (
    SELECT
        t.TransactionID,
        t.CardHolderID,
        t.TransactionTypeID,
        t.Datetimestart,
        t.datetimeend,
        t.TransactionAmount,
        t.locationid,
        tl.TRANSACTIONTYPENAME AS transaction_type_name
    FROM harmonized.transactions t
    JOIN harmonized.transactions_type_lookup tl ON t.TransactionTypeID = tl.TransactionTypeID
)

SELECT
    dc.CardHolderID,  -- Include CardHolderID in the SELECT statement
    da.LocationID as ATM_Location_ID,
    -- Measures
    COUNT(nk.TransactionID) AS total_transactions,
    SUM(nk.TransactionAmount) AS total_transaction_amount,
    AVG(nk.TransactionAmount) AS avg_transaction_amount,
    nk.transaction_type_name
FROM natural_keys nk
JOIN analytics.dt_dim_customer dc ON dc.CardHolderID = nk.CardHolderID
JOIN analytics.dt_dim_atm da ON da.LocationID = nk.LocationID
GROUP BY dc.CardHolderID, da.LocationID, nk.transaction_type_name;

