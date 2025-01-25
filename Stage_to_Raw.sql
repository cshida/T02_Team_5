CREATE OR REPLACE PROCEDURE load_raw_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate and load data into rivers transactions table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."RIVERS_TRANSACTIONS";
    COPY INTO "TRUSTBANKDATA"."RAW"."RIVERS_TRANSACTIONS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('rivers_transactions.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into kano transactions table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."KANO_TRANSACTIONS";
    COPY INTO "TRUSTBANKDATA"."RAW"."KANO_TRANSACTIONS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('kano_transactions.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into hour lookup table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."HOUR_LOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."HOUR_LOOKUP"
    FROM (
        SELECT $1, $2, $3
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('hour lookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into Branch performance table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."BRANCH_PERFORMANCE";
    COPY INTO "TRUSTBANKDATA"."RAW"."BRANCH_PERFORMANCE"
    FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
    FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('Branch_performance.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into generated investment data table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."GENERATED_INVESTMENT_DATA";
    COPY INTO "TRUSTBANKDATA"."RAW"."GENERATED_INVESTMENT_DATA"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('generated_investment_data.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into fct transactions table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."FCT_TRANSACTIONS";
    COPY INTO "TRUSTBANKDATA"."RAW"."FCT_TRANSACTIONS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('fct_transactions.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into enugu transaction table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."ENUGU_TRANSACTIONS";
    COPY INTO "TRUSTBANKDATA"."RAW"."ENUGU_TRANSACTIONS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('enugu_transactions.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into customer demographics table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."CUSTOMER_DEMOGRAPHICS";
    COPY INTO "TRUSTBANKDATA"."RAW"."CUSTOMER_DEMOGRAPHICS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('customer_demographics.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into customer lookup table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."CUSTOMERLOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."CUSTOMERLOOKUP"
    FROM (
        SELECT $1, null, null, $4, $5, null, $7, $8, null, $10, $11, null, $13, null
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('customerlookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into calendar lookup table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."CALENDAR_LOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."CALENDAR_LOOKUP"
    FROM (
        SELECT $1, $2, null, $4, $5, null, null, null, null, $10
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('calendar lookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into transaction type table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."TRANSACTION_TYPE_LOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."TRANSACTION_TYPE_LOOKUP"
    FROM (
        SELECT $1, $2
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('transaction_type lookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into LOAN table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."LOAN";
    COPY INTO "TRUSTBANKDATA"."RAW"."LOAN"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('loan.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=1, -- Assuming the first row contains headers
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;

    -- Truncate and load data into BRANCH_TABLE
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."BRANCH_TABLE";
    COPY INTO "TRUSTBANKDATA"."RAW"."BRANCH_TABLE"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('Branch table.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=1, -- Assuming the first row contains headers
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;

    -- Truncate and load data into LAGOS_TRANSACTIONS table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."LAGOS_TRANSACTIONS";
    COPY INTO "TRUSTBANKDATA"."RAW"."LAGOS_TRANSACTIONS"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('lagos_transactions.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=1, -- Assuming the first row contains headers
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into branch lookup table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."BRANCH_LOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."BRANCH_LOOKUP"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('branch_lookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into branch table table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."BRANCH_TABLE";
    COPY INTO "TRUSTBANKDATA"."RAW"."BRANCH_TABLE"
    FROM (
        SELECT $1, $2, null, null, null, null, null, null, null, null, null
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('Branch table.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    -- Truncate and load data into atm location lookup table
    TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."ATM_LOCATION_LOOKUP";
    COPY INTO "TRUSTBANKDATA"."RAW"."ATM_LOCATION_LOOKUP"
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
    )
    FILES = ('atm_location lookup.csv.gz')
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=-1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;
    RETURN 'Data load for all tables completed successfully.';
END;
$$;




-- Step 1: Create the task

CREATE OR REPLACE TASK daily_task_to_call_sp
  WAREHOUSE = my_warehouse
  SCHEDULE = 'USING CRON 10 16 * * * UTC' -- Runs daily at 12:10 AM SGT
AS
call LOAD_RAW_TABLES();

-- Step 2: Enable the task
ALTER TASK daily_task_to_call_sp RESUME;
