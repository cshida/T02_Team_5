
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
END;

ALTER TASK process_error_log_task RESUME;