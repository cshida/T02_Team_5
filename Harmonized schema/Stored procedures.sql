-- Stored Procedures

CREATE OR REPLACE PROCEDURE log_error(error_message VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO ERROR_LOGS (TIMESTAMP, ERROR_MESSAGE) VALUES (CURRENT_TIMESTAMP(), :error_message);
    RETURN 'Error logged successfully';
END;
$$;

CREATE OR REPLACE PROCEDURE adjust_warehouse_size(warehouse_name VARCHAR, target_size VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Resize warehouse according to the current workload
    EXECUTE IMMEDIATE 'ALTER WAREHOUSE ' || :warehouse_name || ' SET WAREHOUSE_SIZE = ' || :target_size;
    RETURN 'Warehouse resized successfully to ' || :target_size;
END;
$$;

CREATE OR REPLACE PROCEDURE log_schema_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO schema_change_log (change_date, details)
    SELECT CURRENT_TIMESTAMP(), 'Schema modified' WHERE EXISTS
    (SELECT * FROM information_schema.changes WHERE change_type = 'SCHEMA_MODIFIED' AND change_date = CURRENT_DATE());
    RETURN 'Schema changes logged successfully.';
END;
$$;
