
CREATE OR REPLACE TABLE error_logs.schema_metadata (
    "Name" STRING,
    "Type" STRING,
    "Classification" STRING,
    "Owner" STRING,
    "Rows" INT,
    "Columns" INT,
    "Nullable_Columns" INT,
    "Has_Primary_Key" BOOLEAN,
    "Created" TIMESTAMP,
    "Last_Updated" TIMESTAMP,
    "Action_Group" STRING
);
CREATE TABLE IF NOT EXISTS ERROR_LOGS.error_logs_summary (
    error_logs_table VARCHAR PRIMARY KEY,
    load_row_count INT,
    error_row_count INT,
    total_row_count INT,
    error_percentage DECIMAL(10,2)
);

CREATE OR REPLACE PROCEDURE populate_schema_metadata(schema_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS

$$

    // Step 1: Fetch all tables in the schema
    var tables = [];
    var stmt = snowflake.createStatement({
        sqlText: `SELECT TABLE_NAME, TABLE_TYPE, TABLE_OWNER, CREATED, LAST_ALTERED 
                  FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_SCHEMA = '${SCHEMA_NAME}'`
    });
    var result = stmt.execute();
    while (result.next()) {
        tables.push({
            table_name: result.getColumnValue(1),
            table_type: result.getColumnValue(2),
            table_owner: result.getColumnValue(3),
            created: result.getColumnValue(4),
            last_altered: result.getColumnValue(5)
        });
    }

    // Step 2: For each table, calculate metadata
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        
        // Get row count
        var countStmt = snowflake.createStatement({
            sqlText: `SELECT COUNT(*) AS row_count 
                      FROM ${SCHEMA_NAME}.${table.table_name}`
        });
        var countResult = countStmt.execute();
        countResult.next();
        var rowCount = countResult.getColumnValue(1);

        // Get column metadata (total columns, nullable columns)
        var columnStmt = snowflake.createStatement({
            sqlText: `SELECT 
                          COUNT(*) AS total_columns,
                          SUM(CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END) AS nullable_columns
                      FROM INFORMATION_SCHEMA.COLUMNS
                      WHERE TABLE_SCHEMA = '${SCHEMA_NAME}'
                        AND TABLE_NAME = '${table.table_name}'`
        });
        var columnResult = columnStmt.execute();
        columnResult.next();
        var totalColumns = columnResult.getColumnValue(1);
        var nullableColumns = columnResult.getColumnValue(2);

        // Check if the table has a primary key
        var pkStmt = snowflake.createStatement({
            sqlText: `SELECT COUNT(*) 
                      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                      WHERE TABLE_SCHEMA = '${SCHEMA_NAME}' 
                        AND TABLE_NAME = '${table.table_name}' 
                        AND CONSTRAINT_TYPE = 'PRIMARY KEY'`
        });
        var pkResult = pkStmt.execute();
        pkResult.next();
        var hasPrimaryKey = pkResult.getColumnValue(1) > 0 ? 'TRUE' : 'FALSE';

        // Insert into schema_metadata
        var insertStmt = snowflake.createStatement({
            sqlText: `INSERT INTO schema_metadata 
                      ("Name", "Type", "Classification", "Owner", "Rows", "Columns", "Nullable_Columns", "Has_Primary_Key", "Created", "Last_Updated", "Action_Group") 
                      VALUES (?, ?, '—', ?, ?, ?, ?, ?, ?, ?, 'Action Group')`,
            binds: [
                table.table_name,          // STRING
                table.table_type,          // STRING
                table.table_owner,         // STRING
                rowCount,                  // INT
                totalColumns,              // INT
                nullableColumns,           // INT
                hasPrimaryKey,             // STRING ('TRUE' or 'FALSE')
                table.created,             // TIMESTAMP
                table.last_altered         // TIMESTAMP
            ]
        });
        insertStmt.execute();
    }

    return 'Schema metadata populated for schema: ' + SCHEMA_NAME;
$$;

CREATE OR REPLACE PROCEDURE ERROR_LOGS.UPDATE_ERROR_LOGS_SUMMARY()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
    // Step 1: Create the summary table if it doesn't exist
    var createTableSQL = `
        CREATE TABLE IF NOT EXISTS ERROR_LOGS.error_logs_summary (
            error_logs_table STRING,
            load_row_count INT,
            error_row_count INT,
            total_row_count INT,
            error_percentage DECIMAL(5,2)
        );
    `;
    var stmt = snowflake.createStatement({sqlText: createTableSQL});
    stmt.execute();

    // Step 2: Delete all existing data in the summary table to ensure fresh data
    var deleteSQL = "DELETE FROM ERROR_LOGS.error_logs_summary;";
    stmt = snowflake.createStatement({sqlText: deleteSQL});
    stmt.execute();

    // Step 3: Use MERGE to insert the new data, as there's no existing data anymore
    var mergeSQL = `
        MERGE INTO ERROR_LOGS.error_logs_summary target
        USING (
            SELECT 
                m.error_logs_table AS error_logs_table,
                m.load_row_count,
                COALESCE(e.error_row_count, 0) AS error_row_count,
                (m.load_row_count + COALESCE(e.error_row_count, 0)) AS total_row_count,
                CASE 
                    WHEN m.load_row_count = 0 THEN 0
                    ELSE (COALESCE(e.error_row_count, 0) * 100.0) / m.load_row_count
                END AS error_percentage
            FROM (
                -- Load history mapping
                SELECT
                    CASE
                        WHEN TABLE_NAME = 'ATM_LOCATION_LOOKUP' THEN 'ATM_LOCATION_LOOKUP_ERROR'
                        WHEN TABLE_NAME = 'BRANCH_LOOKUP' THEN 'BRANCH_LOOKUP_ERROR'
                        WHEN TABLE_NAME = 'BRANCH_PERFORMANCE' THEN 'BRANCH_PERFORMANCE_ERROR'
                        WHEN TABLE_NAME = 'BRANCH_TABLE' THEN 'BRANCH_ERROR'
                        WHEN TABLE_NAME = 'CALENDAR_LOOKUP' THEN 'CALENDER_LOOKUP_ERROR'
                        WHEN TABLE_NAME = 'CUSTOMERLOOKUP' THEN 'CUSTOMER_LOOKUP_ERROR'
                        WHEN TABLE_NAME = 'CUSTOMER_DEMOGRAPHICS' THEN 'CUSTOMER_DEMOGRAPHICS_ERROR'
                        WHEN TABLE_NAME IN ('ENUGU_TRANSACTIONS', 'FCT_TRANSACTIONS', 'KANO_TRANSACTIONS', 'LAGOS_TRANSACTIONS', 'RIVERS_TRANSACTIONS') 
                             THEN 'TRANSACTIONS_ERROR'
                        WHEN TABLE_NAME = 'GENERATED_INVESTMENT_DATA' THEN 'INVESTMENTS_ERROR'
                        WHEN TABLE_NAME = 'HOUR_LOOKUP' THEN 'HOUR_LOOKUP_ERROR'
                        WHEN TABLE_NAME = 'LOAN' THEN 'LOAN_ERROR'
                        ELSE NULL
                    END AS error_logs_table,
                    SUM(ROW_COUNT) AS load_row_count
                FROM INFORMATION_SCHEMA.LOAD_HISTORY
                GROUP BY 1
            ) m
            LEFT JOIN (
                -- Get error row counts
                SELECT TABLE_NAME, SUM(ROW_COUNT) AS error_row_count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'ERROR_LOGS'
                GROUP BY TABLE_NAME
            ) e
            ON m.error_logs_table = e.TABLE_NAME
            WHERE m.error_logs_table IS NOT NULL
        ) new_data
        ON target.error_logs_table = new_data.error_logs_table
        WHEN MATCHED THEN
            UPDATE SET 
                target.load_row_count = new_data.load_row_count,
                target.error_row_count = new_data.error_row_count,
                target.total_row_count = new_data.total_row_count,
                target.error_percentage = new_data.error_percentage
        WHEN NOT MATCHED THEN
            INSERT (error_logs_table, load_row_count, error_row_count, total_row_count, error_percentage)
            VALUES (new_data.error_logs_table, new_data.load_row_count, new_data.error_row_count, new_data.total_row_count, new_data.error_percentage);
    `;

    stmt = snowflake.createStatement({sqlText: mergeSQL});
    stmt.execute();

    return 'Procedure executed successfully, all data replaced.';
} catch (err) {
    return 'Error: ' + err.message;
}
$$;




CREATE OR REPLACE STREAM error_logs_stream
ON table error_logs.branch_error;

CREATE OR REPLACE TASK process_error_log_task
WAREHOUSE = 'TRUSTBANK_REPORTING_WAREHOUSE'
SCHEDULE = '1 HOUR'
WHEN SYSTEM$STREAM_HAS_DATA('error_logs_stream')
AS
BEGIN
    TRUNCATE TABLE error_logs.schema_metadata;
    CALL populate_schema_metadata('ERROR_LOGS');
    call ERROR_LOGS.UPDATE_ERROR_LOGS_SUMMARY();
END;

ALTER TASK process_error_log_task RESUME;