import os
import snowflake.connector

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='GECKO',
    password='3ddie1aU',
    account='BOB84066',
    warehouse='GECKO_wh',
    database='TRUSTBANKDATA'
)

cursor = conn.cursor()

# Set the schema to RAW for the session
cursor.execute("USE SCHEMA RAW;")

# Directory containing your files
data_directory = r'C:/Users/ethan/OneDrive/Desktop/bankdataset'

# Stage name, ensure it is fully qualified if within a specific schema
stage_name = "RAW.NEW_CSV_STAGE"

try:
    for filename in os.listdir(data_directory):
        # Check file extensions for CSV, ZIP, and JSON
        if filename.endswith(".csv") or filename.endswith(".zip") or filename.endswith(".json"):
            file_path = os.path.join(data_directory, filename).replace("\\", "/")  # Normalize file path
            
            # PUT command to upload file to the new stage
            put_command = f"PUT 'file://{file_path}' @{stage_name} AUTO_COMPRESS=TRUE;"
            print(f"Uploading file to stage: {put_command}")
            cursor.execute(put_command)

            print(f"File {filename} uploaded successfully to stage {stage_name}.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cursor.close()
    conn.close()
