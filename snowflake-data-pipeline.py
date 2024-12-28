import snowflake.connector
import pandas as pd

# Snowflake connection
conn = snowflake.connector.connect(
    user='USERNAME',
    password='PASSWORD',
    account='ACCOUNT_NAME',
    warehouse='WAREHOUSE_NAME',
    database='DATABASE_NAME',
    schema='SCHEMA_NAME'
)


# Step 1: Load raw data into Snowflake
def load_raw_data(file_path, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        COPY INTO {table_name}
        FROM @my_stage/{file_path}
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
    """)
    conn.commit()


# Step 2: Transform data
def transform_data():
    cursor = conn.cursor()
    cursor.execute("""
        CREATE OR REPLACE TABLE transformed_data AS
        SELECT 
            id,
            UPPER(name) AS name,
            TO_DATE(created_at) AS created_date
        FROM raw_data
        WHERE active = TRUE;
    """)
    conn.commit()


# Step 3: Schedule tasks (using Airflow or custom scheduler)
if __name__ == "__main__":
    load_raw_data('data.csv', 'raw_data')
    transform_data()
