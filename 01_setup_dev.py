# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- 01_setup_dev.sql
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS dev_21891;
# MAGIC USE CATALOG dev_21891;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brz_21891;
# MAGIC CREATE SCHEMA IF NOT EXISTS slv_21891;
# MAGIC CREATE SCHEMA IF NOT EXISTS gld_21891;
# MAGIC
# MAGIC -- Volume for uploaded source files
# MAGIC CREATE VOLUME IF NOT EXISTS dev_21891.brz_21891.manuallyuploadedfiles;
# MAGIC
# MAGIC -- Watermark table for incremental processing
# MAGIC CREATE TABLE IF NOT EXISTS dev_21891.brz_21891.pipeline_watermark (
# MAGIC     table_name STRING,
# MAGIC     last_run_ts TIMESTAMP,
# MAGIC     status STRING,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Run log table
# MAGIC CREATE TABLE IF NOT EXISTS dev_21891.brz_21891.pipeline_run_log (
# MAGIC     run_id STRING,
# MAGIC     layer_name STRING,
# MAGIC     table_name STRING,
# MAGIC     run_status STRING,
# MAGIC     row_count BIGINT,
# MAGIC     message STRING,
# MAGIC     run_start_ts TIMESTAMP,
# MAGIC     run_end_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Seed watermark
# MAGIC MERGE INTO dev_21891.brz_21891.pipeline_watermark t
# MAGIC USING (
# MAGIC     SELECT 'customers' AS table_name, TIMESTAMP('1900-01-01 00:00:00') AS last_run_ts, 'INIT' AS status, current_timestamp() AS updated_at
# MAGIC     UNION ALL
# MAGIC     SELECT 'products', TIMESTAMP('1900-01-01 00:00:00'), 'INIT', current_timestamp()
# MAGIC     UNION ALL
# MAGIC     SELECT 'orders', TIMESTAMP('1900-01-01 00:00:00'), 'INIT', current_timestamp()
# MAGIC     UNION ALL
# MAGIC     SELECT 'order_items', TIMESTAMP('1900-01-01 00:00:00'), 'INIT', current_timestamp()
# MAGIC ) s
# MAGIC ON t.table_name = s.table_name
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (table_name, last_run_ts, status, updated_at)
# MAGIC VALUES (s.table_name, s.last_run_ts, s.status, s.updated_at);