# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing ➜ Bronze
# MAGIC Fonte: **/Volumes/workspace/default/medallion_demo_landing**
# MAGIC Arquivos esperados: `items.csv`, `sales.csv`, `wholesale_price.csv`, `loss_rate.csv`, `categories.csv`.

# COMMAND ----------
# Widgets (padrões para seu ambiente)
dbutils.widgets.text("catalog","workspace")
dbutils.widgets.text("schema","default")
dbutils.widgets.text("landing_path","/Volumes/workspace/default/medallion_demo_landing")
dbutils.widgets.text("bronze_path","dbfs:/FileStore/medallion_demo/bronze")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
landing_path = dbutils.widgets.get("landing_path")
bronze_path  = dbutils.widgets.get("bronze_path")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

def ingest_csv(file, table_name):
    df = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(f"{landing_path}/{file}")
         )
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

# === Seus nomes de arquivo ===
ingest_csv("items.csv",            "bronze__annex1_items")
ingest_csv("sales.csv",            "bronze__annex2_sales")
ingest_csv("wholesale_price.csv",  "bronze__annex3_wholesale")
ingest_csv("loss_rate.csv",        "bronze__annex4_lossrates")
ingest_csv("categories.csv",       "bronze__categories")

# Otimizações Delta (opcional)
for t in ["bronze__annex1_items","bronze__annex2_sales","bronze__annex3_wholesale","bronze__annex4_lossrates","bronze__categories"]:
    spark.sql(f"OPTIMIZE {catalog}.{schema}.{t}")