TRUNCATE TABLE "TRUSTBANKDATA"."RAW"."LOAN";
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/truncate-table
COPY INTO "TRUSTBANKDATA"."RAW"."LOAN"
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
    FROM '@"TRUSTBANKDATA"."RAW"."NEW_CSV_STAGE"'
)
FILES = ('loan.csv.gz')
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
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table