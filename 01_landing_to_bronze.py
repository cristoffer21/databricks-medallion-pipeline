# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing ➜ Bronze
# MAGIC Ingest raw CSVs from the **landing** folder into Delta **bronze** tables, adding audit columns.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - bronze.annex1_items
# MAGIC - bronze.annex2_sales
# MAGIC - bronze.annex3_wholesale
# MAGIC - bronze.annex4_lossrates
# MAGIC - bronze.categories

# COMMAND ----------
# Parameters (override via job/cluster widgets if desired)
catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.get()] else "hive_metastore"
schema  = dbutils.widgets.get("schema")  if "schema"  in [w.name for w in dbutils.widgets.get()] else "medallion_demo"
landing_path = dbutils.widgets.get("landing_path") if "landing_path" in [w.name for w in dbutils.widgets.get()] else "dbfs:/FileStore/medallion_demo/landing"
bronze_path  = dbutils.widgets.get("bronze_path")  if "bronze_path"  in [w.name for w in dbutils.widgets.get()] else "dbfs:/FileStore/medallion_demo/bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

def ingest_csv(file, table_name):
    df = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(f"{landing_path}/{file}")
         )
    # add audit columns
    df = (df
          .withColumn("_ingest_file", F.input_file_name())
          .withColumn("_ingest_ts", F.current_timestamp())
    )
    target = f"{catalog}.{schema}.{table_name}"
    (df.write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(target))
    print(f"✅ wrote {target}")

ingest_csv("annex1.csv", "bronze__annex1_items")
ingest_csv("annex2.csv", "bronze__annex2_sales")
ingest_csv("annex3.csv", "bronze__annex3_wholesale")
ingest_csv("annex4.csv", "bronze__annex4_lossrates")
# the derived 5th raw table
ingest_csv("categories.csv", "bronze__categories")

# Optimize (Databricks runtimes with Delta)
for t in ["bronze__annex1_items","bronze__annex2_sales","bronze__annex3_wholesale","bronze__annex4_lossrates","bronze__categories"]:
    spark.sql(f"OPTIMIZE {catalog}.{schema}.{t}")
    spark.sql(f"VACUUM {catalog}.{schema}.{t} RETAIN 168 HOURS")  # 7 days