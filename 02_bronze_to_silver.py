# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze ➜ Silver
# MAGIC Clean and conform data types; split date/time; standardize column names; and enrich joins.

# COMMAND ----------
catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.get()] else "hive_metastore"
schema  = dbutils.widgets.get("schema")  if "schema"  in [w.name for w in dbutils.widgets.get()] else "medallion_demo"

spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F, types as T

items = spark.table(f"{catalog}.{schema}.bronze__annex1_items")
sales = spark.table(f"{catalog}.{schema}.bronze__annex2_sales")
wholesale = spark.table(f"{catalog}.{schema}.bronze__annex3_wholesale")
loss = spark.table(f"{catalog}.{schema}.bronze__annex4_lossrates")
categories = spark.table(f"{catalog}.{schema}.bronze__categories")

# Normalize columns
sales2 = (sales
          .withColumn("date", F.to_date("Date", "yyyy-MM-dd"))
          .withColumn("time", F.to_timestamp("Time"))
          .withColumn("item_code", F.col("Item Code").cast("string"))
          .withColumn("qty_sold_kg", F.col("Quantity Sold (kilo)").cast("double"))
          .withColumn("unit_price_rmb_kg", F.col("Unit Selling Price (RMB/kg)").cast("double"))
          .withColumn("sale_or_return", F.lower(F.col("Sale or Return")))
          .withColumn("has_discount", F.when(F.col("Discount (Yes/No)") == "Yes", F.lit(True)).otherwise(F.lit(False)))
          .drop("Date","Time","Item Code","Quantity Sold (kilo)","Unit Selling Price (RMB/kg)","Sale or Return","Discount (Yes/No)")
         )

items2 = (items
          .withColumn("item_code", F.col("Item Code").cast("string"))
          .withColumn("item_name", F.col("Item Name"))
          .withColumn("category_code", F.col("Category Code").cast("string"))
          .withColumn("category_name", F.col("Category Name"))
          .select("item_code","item_name","category_code","category_name")
         )

wholesale2 = (wholesale
              .withColumn("date", F.to_date("Date","yyyy-MM-dd"))
              .withColumn("item_code", F.col("Item Code").cast("string"))
              .withColumn("wholesale_price_rmb_kg", F.col("Wholesale Price (RMB/kg)").cast("double"))
              .select("date","item_code","wholesale_price_rmb_kg")
             )

loss2 = (loss
         .withColumn("item_code", F.col("Item Code").cast("string"))
         .withColumn("loss_rate_pct", F.col("Loss Rate (%)").cast("double"))
         .select("item_code","loss_rate_pct")
        )

categories2 = (categories
               .withColumn("category_code", F.col("Category Code").cast("string"))
               .withColumn("category_name", F.col("Category Name"))
               .select("category_code","category_name")
              )

# Join dimensions
sales_enriched = (sales2
                  .join(items2, "item_code", "left")
                  .join(loss2, "item_code", "left")
                  .join(wholesale2, ["date","item_code"], "left")
                  .join(categories2, "category_code", "left")
                  .withColumn("gross_revenue_rmb", F.col("qty_sold_kg") * F.col("unit_price_rmb_kg"))
                  .withColumn("est_cost_rmb", F.col("qty_sold_kg") * F.col("wholesale_price_rmb_kg") * (1 + (F.col("loss_rate_pct")/100.0)))
                  .withColumn("est_margin_rmb", F.col("gross_revenue_rmb") - F.col("est_cost_rmb"))
                  .withColumn("_process_ts", F.current_timestamp())
                 )

# Write silver tables
(items2.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(f"{catalog}.{schema}.silver__items"))
(categories2.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(f"{catalog}.{schema}.silver__categories"))
(wholesale2.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(f"{catalog}.{schema}.silver__wholesale"))
(loss2.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(f"{catalog}.{schema}.silver__lossrates"))
(sales_enriched.write.mode("overwrite").format("delta").option("overwriteSchema","true").saveAsTable(f"{catalog}.{schema}.silver__sales_enriched"))

for t in ["silver__items","silver__categories","silver__wholesale","silver__lossrates","silver__sales_enriched"]:
    spark.sql(f"OPTIMIZE {catalog}.{schema}.{t}")