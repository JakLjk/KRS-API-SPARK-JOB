from pyspark.sql import SparkSession, functions as F, types as T, Window
from pyspark.storagelevel import StorageLevel
import psycopg

from .schemas.schema_raw_krs_api_json import schema as krs_api_schema
from .functions.join_row_as_of import join_asof
from .functions.join_row_as_of_shares import join_asof as join_asof_shares
from .functions.load_from_delta import load_delta
from .functions.write_to_delta import write_to_delta
from .functions.orphaned_rows_valid_to import fix_open_ended_orphaned_entries

from config import(
    ENV_APP_NAME,
    ENV_MASTER_URL,
    ENV_TIMEZONE,
    ENV_HADOOP_USER,

    ENV_DELTA_SILVER_REGISTRY,
    ENV_DELTA_SILVER_HISTORY_NAMES,
    ENV_DELTA_SILVER_HISTORY_IDENTIFIERS,
    ENV_DELTA_SILVER_HISTORY_LEGAL_FORM,
    ENV_DELTA_SILVER_HISTORY_HAS_OPP_STATUS,
    ENV_DELTA_SILVER_HISTORY_BUSINESS_WITH_OTHERS,
    ENV_DELTA_SILVER_HISTORY_ADDRESS,
    ENV_DELTA_SILVER_HISTORY_HEADQUARTERS,
    ENV_DELTA_SILVER_HISTORY_WEBPAGE,
    ENV_DELTA_SILVER_HISTORY_EMAIL,
    ENV_DELTA_SILVER_HISTORY_ELECTRONIC_BAE_DELIVERY,
    ENV_DELTA_SILVER_HISTORY_MAIN_HQ_ADDRESS,
    ENV_DELTA_SILVER_HISTORY_TIME_ENTITY_CREATED,
    ENV_DELTA_SILVER_HISTORY_STATUTE_CHANGE,
    ENV_DELTA_SILVER_HISTORY_SHARE_CAPITAL_AMOUNT,
    ENV_DELTA_SILVER_HISTORY_TOTAL_SHARES_UNITS,
    ENV_DELTA_SILVER_HISTORY_SINGLE_SHARE_VALUE,
    ENV_DELTA_SILVER_HISTORY_PAID_IN_CAPITAL_PART,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SERIES_NAME,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SHARES_IN_SERIES,
    ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_PREFERENCE_INFO,

    ENV_SINK_HOST,
    ENV_SINK_PORT,
    ENV_SINK_DB,
    ENV_SINK_PQ_URL,
    ENV_SINK_PQ_USER,
    ENV_SINK_PQ_PASSWORD,
    ENV_SINK_PQ_DRIVER,

    ENV_DELTA_GOLD_STAGING,

    ENV_SPARK_EXECUTORS_MEMORY,
    ENV_SPARK_EXECUTORS_CORES,
    ENV_SPARK_EXECUTORS_OVERHEAD,
    ENV_SPARK_NETWORK_TIMEOUT,
    ENV_SPARK_SHUFFLE_MAX_RETRIES,
    ENV_SPARK_SHUFFLE_RETRY_WAIT,
    ENV_SPARK_SHUFFLE_PARTITIONS,
    ENV_SPARK_PARTITITION_SIZE,
    ENV_SPARK_ADAPTIVE_ENABLED,
    ENV_SPARK_SKEW_JOIN_ENABLED,
    ENV_SPARK_COALESCE_PARTITIONS_ENABLED,

    ENV_SAVE_TO_PGSQL_TRUNCATE,
    ENV_SAVE_TO_PGSQL_BATCHSIZE,

    ENV_ALLOW_SCHEMA_OVERWRITE,
    ENV_SPARK_CHECKPOINT_DIR
)

def krs_api_job():

    # Spark Session definition
    # Adds engines for handling postgresql and delta lake
    # Enabling Delta Lake extension to use delta-specific parser commands
    # Enabling spark-catalog to understand delta tablse 
    spark = (
        SparkSession.builder
        .appName(f"{ENV_APP_NAME}-GOLD")
        # .master(ENV_MASTER_URL)
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", ENV_TIMEZONE)
        .config("spark.executorEnv.HADOOP_USER_NAME", ENV_HADOOP_USER)
        .config("spark.driverEnv.HADOOP_USER_NAME", ENV_HADOOP_USER)
        .config("spark.executor.memory", ENV_SPARK_EXECUTORS_MEMORY)
        .config("spark.executor.cores", ENV_SPARK_EXECUTORS_CORES)
        .config("spark.executor.memoryOverhead", ENV_SPARK_EXECUTORS_OVERHEAD)
        .config("spark.network.timeout", ENV_SPARK_NETWORK_TIMEOUT)
        .config("spark.shuffle.io.maxRetries", ENV_SPARK_SHUFFLE_MAX_RETRIES)
        .config("spark.shuffle.io.retryWait", ENV_SPARK_SHUFFLE_RETRY_WAIT)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.shuffle.partitions", ENV_SPARK_SHUFFLE_PARTITIONS)
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", ENV_SPARK_PARTITITION_SIZE)
    spark.conf.set("spark.sql.adaptive.enabled", ENV_SPARK_ADAPTIVE_ENABLED)
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", ENV_SPARK_SKEW_JOIN_ENABLED)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", ENV_SPARK_COALESCE_PARTITIONS_ENABLED)

    tables = {}

    # Registry and history tables
    tables["df_registry"] = load_delta(spark, ENV_DELTA_SILVER_REGISTRY, sort_within_partition_by=["company_krs", "entry_number"])
    tables["df_hist_names"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_NAMES)
    tables["df_hist_identifiers"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_IDENTIFIERS)
    tables["df_hist_legal_form"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_LEGAL_FORM)
    tables["df_hist_has_opp_status"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_HAS_OPP_STATUS)
    tables["df_hist_does_conduct_business_with_other_entities"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_BUSINESS_WITH_OTHERS
    )

    tables["df_hist_address"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_ADDRESS)
    tables["df_hist_headquarters"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_HEADQUARTERS)
    tables["df_hist_webpage"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_WEBPAGE)
    tables["df_hist_email"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_EMAIL)
    tables["df_hist_electronic_delivery_addres_base"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_ELECTRONIC_BAE_DELIVERY
    )
    tables["df_hist_main_headquarters_address"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_MAIN_HQ_ADDRESS
    )

    tables["df_hist_time_for_which_the_entity_was_created"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_TIME_ENTITY_CREATED
    )
    tables["df_hist_conclusion_or_amendment_of_the_statue_agreement"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_STATUTE_CHANGE
    )

    tables["df_hist_share_capital_amount"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_SHARE_CAPITAL_AMOUNT)
    tables["df_hist_total_number_of_shares_units"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_TOTAL_SHARES_UNITS)
    tables["df_hist_single_share_value"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_SINGLE_SHARE_VALUE)
    tables["df_hist_paid_in_capital_part"] = load_delta(spark, ENV_DELTA_SILVER_HISTORY_PAID_IN_CAPITAL_PART)

    tables["df_hist_share_issues_series_name"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SERIES_NAME
    )
    tables["df_hist_share_issues_shares_in_series"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_SHARES_IN_SERIES
    )
    tables["df_hist_share_issues_preference_info"] = load_delta(
        spark, ENV_DELTA_SILVER_HISTORY_SHARE_ISSUES_PREFERENCE_INFO
    )

    spark.sparkContext.setCheckpointDir(ENV_SPARK_CHECKPOINT_DIR)


    print("Partitions:", tables["df_registry"].rdd.getNumPartitions())


    for name, df in tables.items():
        if name not in ("df_registry"):
            tables[name] = fix_open_ended_orphaned_entries(df)

    base_df = tables["df_registry"].select(
        F.col("source_id"),
        F.col("company_krs"),
        F.col("entry_number"),
        F.col("entry_date"),
        F.col("court_designation"),
        F.col("case_file_designation")
    )


    base_df = join_asof(base_df, tables["df_hist_names"], ["company_name"])
    base_df = join_asof(base_df, tables["df_hist_identifiers"], ["nip_number", "regon_number"])
    base_df = join_asof(base_df, tables["df_hist_legal_form"], ["legal_form"])
    base_df = join_asof(base_df, tables["df_hist_has_opp_status"], ["has_opp_status"])
    base_df = join_asof(
        base_df,
        tables["df_hist_does_conduct_business_with_other_entities"], 
        ["does_conduct_business_with_other_entities"]
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_address"], 
        [
            "country", 
            "street", 
            "house_number", 
            "flat_number", 
            "post_office",
            "postal_code",
            "city"
        ], 
        "address"
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_headquarters"], 
        [
            "country", 
            "municipality", 
            "county", 
            "city", 
            "voivodeship"
        ], 
        "headquarters"
    )
    base_df = join_asof(base_df, tables["df_hist_webpage"], ["webpage"])
    base_df = join_asof(base_df, tables["df_hist_email"], ["email"])
    base_df = join_asof(
        base_df, 
        tables["df_hist_electronic_delivery_addres_base"], 
        ["electronic_bae_delivery"]
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_main_headquarters_address"], 
        [
            "country",
            "street",
            "house_number",
            "flat_number",
            "post_office",
            "postal_code",
            "city",
            "voivodeship"
        ],
        "mainHeadquarters"
    )
    base_df = join_asof(
        base_df,
        tables["df_hist_time_for_which_the_entity_was_created"], 
        ["time_for_which_entity_was_created"]
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_share_capital_amount"], 
        ["currency", "share_capital_amount"],
        "shareCapitalAmount"
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_total_number_of_shares_units"], 
        ["total_number_of_shares_units"]
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_single_share_value"], 
        ["currency", "single_share_value"],
        "singleShareValue"
    )
    base_df = join_asof(
        base_df, 
        tables["df_hist_paid_in_capital_part"], 
        ["currency", "paid_in_capital_amount"],
        "paidInCapitalPart"
    )

    base_df = base_df.persist(StorageLevel.DISK_ONLY) 
    base_df = base_df.checkpoint(eager=True)


    w = Window.partitionBy()


    # Shares DataFrame
    shares_df = tables["df_hist_share_issues_series_name"]
    shares_df = join_asof_shares(
        shares_df,
        tables["df_hist_share_issues_shares_in_series"],
        ["shares_in_series"]
    )
    shares_df = join_asof_shares(
        shares_df,
        tables["df_hist_share_issues_preference_info"],
        ["preference_info"]
    )

    # Amendments DataFrame
    amendments_df = tables["df_hist_conclusion_or_amendment_of_the_statue_agreement"]





    w = (
        Window
        .partitionBy("company_krs")
        .orderBy(F.col("entry_number").desc_nulls_last())
    )

    current_commpany_info_df = (
        base_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn")==1)
        .drop("rn")
    )


    write_to_delta(base_df, ENV_DELTA_GOLD_STAGING, ENV_ALLOW_SCHEMA_OVERWRITE)

    CONNECTION_INFO = {
        "user":ENV_SINK_PQ_USER,
        "password":ENV_SINK_PQ_PASSWORD,
        "driver":ENV_SINK_PQ_DRIVER}
    
    (spark.read.format("delta").load(ENV_DELTA_GOLD_STAGING)
      .coalesce(4)
      .write
      .mode("overwrite")
      .option("truncate", ENV_SAVE_TO_PGSQL_TRUNCATE)
      .option("batchsize", ENV_SAVE_TO_PGSQL_BATCHSIZE)
      .jdbc(ENV_SINK_PQ_URL, "stg_full_history", properties=CONNECTION_INFO))
    
    (shares_df
      .write
      .mode("overwrite")
      .option("truncate", ENV_SAVE_TO_PGSQL_TRUNCATE)
      .option("batchsize", ENV_SAVE_TO_PGSQL_BATCHSIZE)
      .jdbc(ENV_SINK_PQ_URL, "stg_shares_issues", properties=CONNECTION_INFO))
    
    (amendments_df
      .coalesce(4)
      .write
      .mode("overwrite")
      .option("truncate", ENV_SAVE_TO_PGSQL_TRUNCATE)
      .option("batchsize", ENV_SAVE_TO_PGSQL_BATCHSIZE)
      .jdbc(ENV_SINK_PQ_URL, "stg_amendments", properties=CONNECTION_INFO))

    (current_commpany_info_df
      .coalesce(4)
      .write
      .mode("overwrite")
      .option("truncate", ENV_SAVE_TO_PGSQL_TRUNCATE)
      .option("batchsize", ENV_SAVE_TO_PGSQL_BATCHSIZE)
      .jdbc(ENV_SINK_PQ_URL, "stg_current_company_info", properties=CONNECTION_INFO))
    

    spark.catalog.clearCache()

    conn = None
    try:
        conn = psycopg.connect(
            host=ENV_SINK_HOST,
            port=ENV_SINK_PORT,
            dbname=ENV_SINK_DB,
            user=ENV_SINK_PQ_USER,
            password=ENV_SINK_PQ_PASSWORD,
            autocommit=True
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT load_stg(
                    'stg_current_company_info',
                    'stg_full_history',
                    'stg_shares_issues',
                    'stg_amendments'
                );
            """)
            status=cur.fetchone()[0]
            print(f"PSQL Status: {status}")
    except Exception as e:
        raise
    finally:
        if conn is not None:
            conn.close()
