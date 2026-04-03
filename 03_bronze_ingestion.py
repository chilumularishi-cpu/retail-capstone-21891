# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F
from datetime import datetime

def load_bronze(table_name):
    run_id = generate_run_id()
    start_ts = datetime.now()

    try:
        cfg = TABLE_CONFIG[table_name]
        file_path = cfg["file_path"]
        bronze_table = cfg["bronze_table"]
        timestamp_cols = cfg["timestamp_cols"]

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_path)
        )

        df = standardize_timestamps(df, timestamp_cols)

        df = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("source_file_name", F.lit(file_path))
        )

        if not table_exists(bronze_table):
            df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        else:
            df.write.format("delta").mode("append").saveAsTable(bronze_table)

        row_count = df.count()
        end_ts = datetime.now()
        log_run(run_id, "BRONZE", table_name, "SUCCESS", row_count, "Bronze load completed", start_ts, end_ts)

        print(f"Bronze load successful for {table_name}. Rows loaded: {row_count}")

    except Exception as e:
        end_ts = datetime.now()
        log_run(run_id, "BRONZE", table_name, "FAILED", 0, str(e), start_ts, end_ts)
        raise e

for table_name in ["customers", "products", "orders", "order_items"]:
    load_bronze(table_name)