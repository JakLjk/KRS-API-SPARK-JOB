from pyspark.sql import SparkSession, functions as F, types as T, Window

from .schemas.schema_raw_krs_api_json import schema as krs_api_schema


from config import (
    ENV_APP_NAME,
    ENV_MASTER_URL,
    ENV_TIMEZONE,
    ENV_ALLOW_SCHEMA_OVERWRITE,
    ENV_SOURCE_PQ_URL,
    ENV_SOURCE_PQ_USER,
    ENV_SOURCE_PQ_PASSWORD,
    ENV_SOURCE_PQ_SCHEMA_TABLE,
    ENV_SOURCE_PQ_DRIVER,
    ENV_SOURCE_FETCH_SIZE,
    ENV_DELTA_BRONZE_RAW_DATA,
    ENV_HADOOP_USER,
)

def krs_api_job():

    # Spark Session definition
    # Adds engines for handling postgresql and delta lake
    # Enabling Delta Lake extension to use delta-specific parser commands
    # Enabling spark-catalog to understand delta tablse 
    spark = (SparkSession.builder
        .appName(f"{ENV_APP_NAME}-BRONZE")
        .master(ENV_MASTER_URL)
        .config("spark.jars.packages","io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", ENV_TIMEZONE)
        .config("spark.executorEnv.HADOOP_USER_NAME", ENV_HADOOP_USER)
        .config("spark.driverEnv.HADOOP_USER_NAME",  ENV_HADOOP_USER)
        .getOrCreate())

    # Configuring connection string for the postgresql
    # Selecting only records marked as current from the table with raw JSONB
    JDBC_CONFIG = {
        "url":ENV_SOURCE_PQ_URL,
        "dbtable":ENV_SOURCE_PQ_SCHEMA_TABLE,
        "user":ENV_SOURCE_PQ_USER,
        "password":ENV_SOURCE_PQ_PASSWORD,
        "driver":ENV_SOURCE_PQ_DRIVER,
        "fetchsize":ENV_SOURCE_FETCH_SIZE,
        "db_data":f"SELECT id, krs_number, raw_data FROM db_data WHERE is_current = TRUE AS src"
    }
    

    raw_df = spark.read.format("jdbc").options(**JDBC_CONFIG).load()

    # Selecting columns with:
    # id - Being the surrogate id of the table that stores raw scraped data
    # company krs - being the business key of a company
    # raw data - raw JSON with data concerning the company
    raw_df = (
        raw_df
        .select(
            F.col("id").alias("source_id"),
            F.col("krs_number").alias("company_krs"),
            F.col("raw_data").alias("raw_json")
            )
    )

    # Safe measure to mitigate the risk of having two records for the same krs in
    # situation where scraping function did not mark old records as current=False 
    w = (
        Window
        .partitionBy("company_krs")
        .orderBy("source_id")
    )
    raw_df = (
        raw_df
        .withColumn("rownum", F.row_number().over(w))
        .filter(F.col("rownum")==1)
        .drop("rownum")
    )
    
    # Write to delta table
    (
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema",ENV_ALLOW_SCHEMA_OVERWRITE)
        .save(ENV_DELTA_BRONZE_RAW_DATA))
