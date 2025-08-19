# ✅ Keep SparkSession in each script, but don’t hardcode master.
# ✅ One Airflow task per stage (or one task with subcommands—your choice).
# ✅ Ship your helpers via --py-files (wheel/zip) or preinstall on the cluster.
# ✅ Put DAG files in Airflow’s dags/ directory (repo can hold them; deploy copies them).
# ✅ Run Airflow in Docker if you like; it’s common to add it to your Spark compose stack.
# ✅ Secrets/config live in Airflow (Variables/Connections), not hardcoded.
# ✅ If executors need an env var, set it with spark.executorEnv.MY_VAR=... (operator conf or in your builder).
# ✅ Make jobs idempotent and parameterizable (good for retries/backfills).
# ✅ Raise on error so Airflow can retry and show failures clearly.