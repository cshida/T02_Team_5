-- Step 1: Create the table in the target schema
CREATE TABLE target_schema.target_table AS
SELECT *
FROM source_schema.source_table;

-- Step 2: Optionally, drop the table from the source schema if no longer needed
DROP TABLE source_schema.source_table;
