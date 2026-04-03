# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from datetime import datetime

run_id = generate_run_id()
start_ts = datetime.now()

try:
    customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
    products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
    sales_enriched = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.sales_enriched")

    # Cache reused dataframe
    # sales_enriched.cache() --Error can't use Presists on Serverless
    sales_enriched.count()

    # ----------------------------------------------------
    # DIM_CUSTOMERS
    # ----------------------------------------------------
    dim_customers = (
        customers.select(
            "customer_id",
            "name",
            "city",
            "state",
            "signup_date",
            "created_at",
            "updated_at"
        )
        .dropDuplicates(["customer_id"])
    )

    dim_customers.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_customers")

    # ----------------------------------------------------
    # DIM_PRODUCTS
    # ----------------------------------------------------
    dim_products = (
        products.select(
            "product_id",
            "product_name",
            "category",
            "price",
            "created_at",
            "updated_at"
        )
        .dropDuplicates(["product_id"])
    )

    dim_products.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_products")

    # ----------------------------------------------------
    # FACT_SALES
    # grain = one row per order_item_id
    # ----------------------------------------------------
    fact_sales = (
        sales_enriched
        .withColumn("order_date_only", F.to_date(F.col("order_date")))
        .withColumn("order_year", F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
        .withColumn("order_day", F.dayofmonth(F.col("order_date")))
    )

    fact_sales = fact_sales.repartition("order_year", "order_month")

    (
        fact_sales.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")
    )

    # ----------------------------------------------------
    # REVENUE BY STATE
    # ----------------------------------------------------
    # Ensure 'state' column exists in fact_sales
    if "state" not in fact_sales.columns:
        # Join with customers to get 'state'
        fact_sales = fact_sales.join(
            customers.select("customer_id", "state"),
            on="customer_id",
            how="left"
        )

    agg_revenue_by_state = (
        fact_sales.groupBy("state")
        .agg(
            F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("total_customers")
        )
        .orderBy(F.desc("total_revenue"))
    )

    agg_revenue_by_state.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_revenue_by_state")

    # ----------------------------------------------------
    # TOP PRODUCTS
    # ----------------------------------------------------
    # Ensure 'product_name' and 'category' exist in fact_sales
    if ("product_name" not in fact_sales.columns) or ("category" not in fact_sales.columns):
        fact_sales = fact_sales.join(
            products.select("product_id", "product_name", "category"),
            on="product_id",
            how="left"
        )

    agg_top_products = (
        fact_sales.groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("sales_amount"), 2).alias("revenue")
        )
        .orderBy(F.desc("revenue"))
    )

    agg_top_products.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_top_products")

    # ----------------------------------------------------
    # DAILY SALES
    # ----------------------------------------------------
    agg_daily_sales = (
        fact_sales.groupBy("order_date_only")
        .agg(
            F.round(F.sum("sales_amount"), 2).alias("daily_revenue"),
            F.countDistinct("order_id").alias("daily_orders"),
            F.countDistinct("customer_id").alias("daily_customers")
        )
        .orderBy("order_date_only")
    )

    agg_daily_sales.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_daily_sales")

    # ----------------------------------------------------
    # MONTHLY SALES
    # ----------------------------------------------------
    agg_monthly_sales = (
        fact_sales.groupBy("order_year", "order_month")
        .agg(
            F.round(F.sum("sales_amount"), 2).alias("monthly_revenue"),
            F.countDistinct("order_id").alias("monthly_orders"),
            F.countDistinct("customer_id").alias("monthly_customers")
        )
        .orderBy("order_year", "order_month")
    )

    agg_monthly_sales.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_monthly_sales")

    # ----------------------------------------------------
    # CATEGORY SALES
    # ----------------------------------------------------
    agg_category_sales = (
        fact_sales.groupBy("category")
        .agg(
            F.round(F.sum("sales_amount"), 2).alias("category_revenue"),
            F.sum("quantity").alias("units_sold")
        )
        .orderBy(F.desc("category_revenue"))
    )

    agg_category_sales.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_category_sales")

    row_count = fact_sales.count()
    end_ts = datetime.now()
    log_run(run_id, "GOLD", "fact_sales_and_aggregates", "SUCCESS", row_count, "Gold layer build completed", start_ts, end_ts)

    print("Gold tables built successfully")

except Exception as e:
    end_ts = datetime.now()
    log_run(run_id, "GOLD", "fact_sales_and_aggregates", "FAILED", 0, str(e), start_ts, end_ts)
    raise e