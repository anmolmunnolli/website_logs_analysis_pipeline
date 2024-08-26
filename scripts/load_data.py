from dotenv import load_dotenv
import os
import snowflake.connector
import logging
import pandas
from pyspark.sql import SparkSession

# logging.basicConfig(level=logging.DEBUG)

load_dotenv()

spark = SparkSession.builder \
    .appName("CSV to Snowflake") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.18.0") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv(r"C:\Users\anmol\Desktop\de\data\E_commerce_Website_Logs.csv", header=True, inferSchema=True)

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

create_table_query = '''
        CREATE TABLE ecommerce_logs (
            accessed_date TIMESTAMP,
            duration_secs INTEGER,
            network_protocol VARCHAR,
            ip VARCHAR,
            bytes INTEGER,
            accessed_from VARCHAR,
            age VARCHAR,
            gender VARCHAR,
            country VARCHAR,
            membership VARCHAR,
            language VARCHAR,
            sales FLOAT,
            returned VARCHAR,
            returned_amount FLOAT,
            pay_method VARCHAR
        );
            '''

conn.cursor().execute(create_table_query)

put_query = "PUT file:///C:/Users/anmol/Desktop/de/data/E_commerce_Website_Logs.csv @%ecommerce_logs"
conn.cursor().execute(put_query)
print("File uploaded to Snowflake stage")

# COPY INTO command to load data into the table
copy_query = """
COPY INTO ecommerce_logs
FROM @%ecommerce_logs/E_commerce_Website_Logs.csv
FILE_FORMAT = (TYPE = 'csv', FIELD_OPTIONALLY_ENCLOSED_BY='"');
"""
conn.cursor().execute(copy_query)
print("Data copied into ecommerce_logs table")


options = {
    "sfURL": f"https://{snow_flk_acc}.snowflakecomputing.com",
    "sfUser": snow_flk_username,
    "sfPassword": snow_flk_pass,
    "sfDatabase": snow_flk_db,
    "sfSchema": snow_flk_schema,
    "sfWarehouse": snow_flk_warehouse
}

# Write DataFrame to Snowflake
df.write.format("snowflake") \
    .options(**options) \
    .option("dbtable", "ecommerce_logs") \
    .mode("overwrite") \
    .save()

# Cleanup
spark.stop()

cursor.close()
conn.close()
