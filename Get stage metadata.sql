-- Step 1: Create the metadata table (if not already created)
CREATE OR REPLACE TABLE stage_file_metadata (
    filename STRING,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Insert new file metadata into the table
INSERT INTO stage_file_metadata (filename, load_time)
SELECT DISTINCT METADATA$FILENAME, CURRENT_TIMESTAMP
FROM @NEW_CSV_STAGE
WHERE METADATA$FILENAME NOT IN (SELECT filename FROM stage_file_metadata);

-- Step 3: View the metadata table
SELECT * FROM stage_file_metadata;
