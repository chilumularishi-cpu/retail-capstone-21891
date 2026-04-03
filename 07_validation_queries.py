# Databricks notebook source
# DBTITLE 1,Row Counts
# MAGIC %sql
# MAGIC -- Counts
# MAGIC SELECT 'brz_customers_raw' AS table_name, COUNT(*) AS row_count FROM brz_21891.customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_products_raw', COUNT(*) FROM brz_21891.products_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_orders_raw', COUNT(*) FROM brz_21891.orders_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_order_items_raw', COUNT(*) FROM brz_21891.order_items_raw
# MAGIC UNION ALL
# MAGIC SELECT 'slv_customers', COUNT(*) FROM slv_21891.customers
# MAGIC UNION ALL
# MAGIC SELECT 'slv_products', COUNT(*) FROM slv_21891.products
# MAGIC UNION ALL
# MAGIC SELECT 'slv_orders', COUNT(*) FROM slv_21891.orders
# MAGIC UNION ALL
# MAGIC SELECT 'slv_order_items', COUNT(*) FROM slv_21891.order_items
# MAGIC UNION ALL
# MAGIC SELECT 'slv_sales_enriched', COUNT(*) FROM slv_21891.sales_enriched
# MAGIC UNION ALL
# MAGIC SELECT 'gld_fact_sales', COUNT(*) FROM gld_21891.fact_sales;

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Customers
# MAGIC %sql
# MAGIC -- Duplicate checks
# MAGIC SELECT customer_id, COUNT(*) cnt
# MAGIC FROM slv_21891.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Products
# MAGIC %sql
# MAGIC SELECT product_id, COUNT(*) cnt
# MAGIC FROM slv_21891.products
# MAGIC GROUP BY product_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Orders
# MAGIC %sql
# MAGIC SELECT order_id, COUNT(*) cnt
# MAGIC FROM slv_21891.orders
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Order Items
# MAGIC %sql
# MAGIC SELECT order_item_id, COUNT(*) cnt
# MAGIC FROM slv_21891.order_items
# MAGIC GROUP BY order_item_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Watermark check
# MAGIC %sql
# MAGIC -- Watermark check
# MAGIC SELECT * FROM brz_21891.pipeline_watermark;

# COMMAND ----------

# DBTITLE 1,Run logs
# MAGIC %sql
# MAGIC -- Run logs
# MAGIC SELECT * FROM brz_21891.pipeline_run_log ORDER BY run_start_ts DESC;