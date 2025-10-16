# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver ➜ Gold
# MAGIC Produce business-friendly facts and aggregates.

# COMMAND ----------
catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.get()] else "hive_metastore"
schema  = dbutils.widgets.get("schema")  if "schema"  in [w.name for w in dbutils.widgets.get()] else "medallion_demo"

spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F, Window

s = spark.table(f"{catalog}.{schema}.silver__sales_enriched")
items = spark.table(f"{catalog}.{schema}.silver__items")
cats = spark.table(f"{catalog}.{schema}.silver__categories")

# Fact: daily sales by item
fact_daily_item = (s.groupBy("date","item_code","item_name")
                     .agg(F.sum("qty_sold_kg").alias("qty_kg"),
                          F.sum("gross_revenue_rmb").alias("revenue_rmb"),
                          F.sum("est_margin_rmb").alias("margin_rmb"))
                  )
fact_daily_item.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gold__fact_daily_item")

# Fact: daily sales by category
fact_daily_cat = (s.groupBy("date","category_code","category_name")
                    .agg(F.sum("qty_sold_kg").alias("qty_kg"),
                         F.sum("gross_revenue_rmb").alias("revenue_rmb"),
                         F.sum("est_margin_rmb").alias("margin_rmb"))
                 )
fact_daily_cat.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gold__fact_daily_category")

# Top 10 products per day by revenue
w = Window.partitionBy("date").orderBy(F.desc("revenue_rmb"))
top10 = (fact_daily_item
         .withColumn("rank_rev", F.row_number().over(w))
         .where(F.col("rank_rev") <= 10))
top10.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gold__top10_by_revenue")

# KPI Summary (overall period)
kpi = (s.agg(F.sum("qty_sold_kg").alias("total_qty_kg"),
             F.sum("gross_revenue_rmb").alias("total_revenue_rmb"),
             F.sum("est_margin_rmb").alias("total_margin_rmb"),
             F.countDistinct("item_code").alias("distinct_items"),
             F.countDistinct("category_code").alias("distinct_categories"))
      )
(kpi.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gold__kpi_summary"))