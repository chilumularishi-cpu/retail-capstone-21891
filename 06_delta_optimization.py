# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- 06_delta_optimization.sql
# MAGIC -- ============================================
# MAGIC
# MAGIC USE CATALOG dev_21891;
# MAGIC
# MAGIC -- BRONZE
# MAGIC OPTIMIZE brz_21891.customers_raw ZORDER BY (customer_id);
# MAGIC OPTIMIZE brz_21891.products_raw ZORDER BY (product_id);
# MAGIC OPTIMIZE brz_21891.orders_raw ZORDER BY (order_id, customer_id);
# MAGIC OPTIMIZE brz_21891.order_items_raw ZORDER BY (order_id, product_id);
# MAGIC
# MAGIC -- SILVER
# MAGIC OPTIMIZE slv_21891.customers ZORDER BY (customer_id, state);
# MAGIC OPTIMIZE slv_21891.products ZORDER BY (product_id, category);
# MAGIC OPTIMIZE slv_21891.orders ZORDER BY (order_id, customer_id, order_date);
# MAGIC OPTIMIZE slv_21891.order_items ZORDER BY (order_id, product_id);
# MAGIC OPTIMIZE slv_21891.sales_enriched ZORDER BY (order_date, customer_id, product_id);
# MAGIC
# MAGIC -- GOLD
# MAGIC OPTIMIZE gld_21891.fact_sales ZORDER BY (order_date_only, state, product_id);
# MAGIC OPTIMIZE gld_21891.dim_customers ZORDER BY (customer_id, state);
# MAGIC OPTIMIZE gld_21891.dim_products ZORDER BY (product_id, category);
# MAGIC
# MAGIC VACUUM brz_21891.customers_raw RETAIN 168 HOURS;
# MAGIC VACUUM brz_21891.products_raw RETAIN 168 HOURS;
# MAGIC VACUUM brz_21891.orders_raw RETAIN 168 HOURS;
# MAGIC VACUUM brz_21891.order_items_raw RETAIN 168 HOURS;
# MAGIC
# MAGIC VACUUM slv_21891.customers RETAIN 168 HOURS;
# MAGIC VACUUM slv_21891.products RETAIN 168 HOURS;
# MAGIC VACUUM slv_21891.orders RETAIN 168 HOURS;
# MAGIC VACUUM slv_21891.order_items RETAIN 168 HOURS;
# MAGIC VACUUM slv_21891.sales_enriched RETAIN 168 HOURS;
# MAGIC
# MAGIC VACUUM gld_21891.fact_sales RETAIN 168 HOURS;
# MAGIC VACUUM gld_21891.dim_customers RETAIN 168 HOURS;
# MAGIC VACUUM gld_21891.dim_products RETAIN 168 HOURS;