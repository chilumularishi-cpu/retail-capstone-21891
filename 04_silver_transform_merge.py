# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

def clean_customers(df):
    return (
        df.filter(F.col("customer_id").isNotNull())
          .withColumn("customer_id", F.col("customer_id").cast("long"))
          .withColumn("name", F.trim(F.col("name")))
          .withColumn("city", F.initcap(F.trim(F.col("city"))))
          .withColumn("state", F.upper(F.trim(F.col("state"))))
    )

def clean_products(df):
    return (
        df.filter(F.col("product_id").isNotNull())
          .withColumn("product_id", F.col("product_id").cast("long"))
          .withColumn("product_name", F.trim(F.col("product_name")))
          .withColumn("category", F.initcap(F.trim(F.col("category"))))
          .withColumn("price", F.col("price").cast("double"))
    )

def clean_orders(df):
    return (
        df.filter(F.col("order_id").isNotNull())
          .withColumn("order_id", F.col("order_id").cast("long"))
          .withColumn("customer_id", F.col("customer_id").cast("long"))
          .withColumn("total_amount", F.col("total_amount").cast("double"))
          .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    )

def clean_order_items(df):
    return (
        df.filter(F.col("order_item_id").isNotNull())
          .withColumn("order_item_id", F.col("order_item_id").cast("long"))
          .withColumn("order_id", F.col("order_id").cast("long"))
          .withColumn("product_id", F.col("product_id").cast("long"))
          .withColumn("quantity", F.col("quantity").cast("int"))
          .withColumn("price", F.col("price").cast("double"))
    )

def apply_cleaning(table_name, df):
    if table_name == "customers":
        return clean_customers(df)
    elif table_name == "products":
        return clean_products(df)
    elif table_name == "orders":
        return clean_orders(df)
    elif table_name == "order_items":
        return clean_order_items(df)
    else:
        return df

def merge_to_silver(table_name):
    run_id = generate_run_id()
    start_ts = datetime.now()

    try:
        cfg = TABLE_CONFIG[table_name]
        bronze_table = cfg["bronze_table"]
        silver_table = cfg["silver_table"]
        pk = cfg["pk"]

        last_run_ts = get_last_run_ts(table_name)
        print(f"Last watermark for {table_name}: {last_run_ts}")

        bronze_df = spark.table(bronze_table)

        incremental_df = bronze_df.filter(
            (F.col("created_at") > F.lit(last_run_ts)) |
            (F.col("updated_at") > F.lit(last_run_ts))
        )

        if incremental_df.count() == 0:
            end_ts = datetime.now()
            log_run(run_id, "SILVER", table_name, "SUCCESS", 0, "No incremental data found", start_ts, end_ts)
            print(f"No new data for {table_name}")
            return

        cleaned_df = apply_cleaning(table_name, incremental_df)
        dedup_df = deduplicate_latest(cleaned_df, pk)

        if not table_exists(silver_table):
            dedup_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        else:
            delta_table = DeltaTable.forName(spark, silver_table)

            merge_condition = f"t.{pk} = s.{pk}"

            update_set = {col: F.col(f"s.{col}") for col in dedup_df.columns}
            insert_values = {col: F.col(f"s.{col}") for col in dedup_df.columns}

            (
                delta_table.alias("t")
                .merge(dedup_df.alias("s"), merge_condition)
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsert(values=insert_values)
                .execute()
            )

        max_ts_row = dedup_df.select(
            F.greatest(F.max("created_at"), F.max("updated_at")).alias("max_ts")
        ).collect()[0]

        max_ts = max_ts_row["max_ts"]
        update_watermark(table_name, max_ts)

        row_count = dedup_df.count()
        end_ts = datetime.now()
        log_run(run_id, "SILVER", table_name, "SUCCESS", row_count, "Silver merge completed", start_ts, end_ts)

        print(f"Silver merge successful for {table_name}. Rows processed: {row_count}")

    except Exception as e:
        end_ts = datetime.now()
        log_run(run_id, "SILVER", table_name, "FAILED", 0, str(e), start_ts, end_ts)
        raise e

for table_name in ["customers", "products", "orders", "order_items"]:
    merge_to_silver(table_name)

# ---------------------------------------------------------
# Build an enriched silver table for reusable joins
# ---------------------------------------------------------
run_id = generate_run_id()
start_ts = datetime.now()

try:
    customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
    products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
    orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    order_items = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_items")

    sales_enriched = (
        order_items.alias("oi")
        .join(orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "inner")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"), "left")
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("o.customer_id"),
            F.col("oi.product_id"),
            F.col("o.order_date"),
            F.col("o.order_status"),
            F.col("c.name").alias("customer_name"),
            F.col("c.city"),
            F.col("c.state"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("oi.quantity"),
            F.col("oi.price").alias("unit_price"),
            (F.col("oi.quantity") * F.col("oi.price")).alias("sales_amount"),
            F.col("o.total_amount").alias("order_total_amount"),
            F.col("o.created_at").alias("order_created_at"),
            F.col("o.updated_at").alias("order_updated_at")
        )
    )

    sales_enriched.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.sales_enriched")

    row_count = sales_enriched.count()
    end_ts = datetime.now()
    log_run(run_id, "SILVER", "sales_enriched", "SUCCESS", row_count, "Silver enriched table built", start_ts, end_ts)

except Exception as e:
    end_ts = datetime.now()
    log_run(run_id, "SILVER", "sales_enriched", "FAILED", 0, str(e), start_ts, end_ts)
    raise e