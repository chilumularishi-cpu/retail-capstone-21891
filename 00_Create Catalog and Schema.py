# Databricks notebook source
# DBTITLE 1,CREATE CATALOG
# MAGIC %sql
# MAGIC -- CREATE CATALOG IF NOT EXISTS dev_21891; --Creating Dev Catalog
# MAGIC -- CREATE CATALOG IF NOT EXISTS uat_21891; --Creating Uat Catalog
# MAGIC -- CREATE CATALOG IF NOT EXISTS prod_21891; --Creating Prod Catalog

# COMMAND ----------

# DBTITLE 1,CREATE SCHEMA IN CATALOGS
# MAGIC %sql
# MAGIC -- Comment the Catalogs that you are not using, and run the script.
# MAGIC -- USE CATALOG dev_21891;
# MAGIC -- USE CATALOG uat_21891;
# MAGIC -- USE CATALOG prod_21891;
# MAGIC
# MAGIC --Ensure only one "USE CATALOG" syntax is enabled, and the remaining are commented out.
# MAGIC -- CREATE SCHEMA IF NOT EXISTS brz_21891;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS slv_21891;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS gld_21891;