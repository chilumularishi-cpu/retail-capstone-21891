# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY dev_21891.slv_21891.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev_21891.slv_21891.orders VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev_21891.gld_21891.fact_sales
# MAGIC TIMESTAMP AS OF current_timestamp() - INTERVAL 1 DAY;
# MAGIC --The code is giving errors as we used Vacuum in 06_delta_optimzation notebook.

# COMMAND ----------

-