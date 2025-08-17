# KRS API Spark Pipeline [Medallion structure]
[![Spark](https://img.shields.io/badge/spark-4.0.0-orange)](#)
[![Hadoop 3](https://img.shields.io/badge/Hadoop-3-ffcc00?logo=apachehadoop&logoColor=black)](#)
[![Python](https://img.shields.io/badge/python-3.11-3776AB)](#)

## About project
This project implements an end-to-end Spark pipeline that ingests Krajowy Rejestr Sądowy (KRS) JSON data, normalizes it into history tables, and produces a Gold layer for analytics and PostgreSQL. 

### Pipeline jobs:
- Bronze: extract raw JSON from PostgreSQL 
- Delta Silver: parse and standardize deeply nested KRS JSON into curated history tables 
- Gold: as-of joins across history > flattened full history > PostgreSQL 

# Features 
- Delta Lake tables with schema evolution and partition-aware writes 
- Handles both domestic and foreign company JSON formats Join-as-of helper to select correct historical values 
- Configurable Spark runtime (adaptive execution, shuffle partitions, executor memory/overhead) 
- Runs standalone or orchestrated via Apache Airflow 
# Quickstart 
- Build or pull a Spark + Hadoop Docker image with Delta + PostgreSQL JDBC included. You can also use Cloud-Based spark session, located on cloud services such as MS Azure
- Start the local cluster (for example, with docker-compose up -d). 
- Run the three jobs in sequence:bronze> silver > gold. To do that, you can use simple Cron job, or Apache Airflow orchestrator.
- Check the outputs: Bronze and Silver: Delta tables in HDFS (or local FS for dev) 
- Gold: Delta + PostgreSQL table stg_full_history 
# What the Code Does 
###  Bronze – JDBC > Delta (raw JSON) 
- Reads raw JSONB records from PostgreSQL. 
- Deduplicates per company (row_number). 
- Writes raw JSON to Delta Lake. 
### Silver – Parse & Normalize history tables 
- Normalizes foreign JSON shape to match domestic schema. 
- Parses JSON into typed schema. 
- Explodes nested arrays into Delta history tables (names, identifiers, legal form, addresses, share capital, etc.). 
- Repartitions and sorts data within partitions. 
### Gold – As-of joins > single fact table > PostgreSQL 
- Uses registry entries as timeline backbone. 
- Joins in history attributes with as-of logic. 
- Persists results and exports full history to PostgreSQL. 

# How to Run spark-submit
 ```bash
spark-submit \   --master ${ENV_MASTER_URL} \   --packages io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3 \   spark_processors/krs_api_processor/bronze.py 
 ```
Run Silver and Gold in the same way. 
