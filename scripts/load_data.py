from dotenv import load_dotenv
import os
import snowflake.connector
import logging
logging.basicConfig(level=logging.DEBUG)

load_dotenv()

snow_flk_acc = os.getenv("SNOWFLAKE_ACCOUNT")
snow_flk_username = os.getenv("SNOWFLAKE_USER")
snow_flk_pass = os.getenv("SNOWFLAKE_PASSWORD")
snow_flk_db = os.getenv("SNOWFLAKE_DATABASE")
snow_flk_schema = os.getenv("SNOWFLAKE_SCHEMA")
snow_flk_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

conn = snowflake.connector.connect(
    user=snow_flk_username,
    password=snow_flk_pass,
    account=snow_flk_acc
)

cursor = conn.cursor()

cursor.execute(f"CREATE OR REPLACE DATABASE {snow_flk_db};")
print("Database created")

cursor.execute(f"CREATE OR REPLACE SCHEMA {snow_flk_schema};")
print("Schema created")

cursor.execute(f"""
    CREATE OR REPLACE WAREHOUSE {snow_flk_warehouse}
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 2
    AUTO_RESUME = TRUE;
""")
print("Warehouse created")

cursor.close()
conn.close()
