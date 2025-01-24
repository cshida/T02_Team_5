CREATE OR REPLACE PROCEDURE load_raw_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
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