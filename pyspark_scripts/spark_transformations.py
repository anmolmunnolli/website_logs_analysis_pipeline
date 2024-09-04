import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Ecommerce Data Transformation") \
    .getOrCreate()

df = spark.read.format("snowflake") \
    .options(**{
        "sfURL": os.getenv("SNOWFLAKE_URL"),
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE")
    }) \
    .option("dbtable", "ecommerce_logs") \
    .load()

logger.info("Data loaded successfully")
df_transformed = df.withColumnRenamed("duration_(secs)", "duration_secs")

logger.info("Data transformed successfully")
df_transformed.show(8)
