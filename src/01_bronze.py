# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ecommerce_catalog")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("source_volume", "raw_files")

catalog = dbutils.widgets.get("catalog")
landing_schema = dbutils.widgets.get("landing_schema")
source_volume = dbutils.widgets.get("source_volume")
source_root = f"/Volumes/{catalog}/{landing_schema}/{source_volume}"

tables = [
    "customers", "products", "suppliers", "orders", "order_items",
    "payments", "shipments", "returns", "website_events"
]

for table in tables:
    path = f"{source_root}/{table}.csv"
    df = (spark.read.option("header", True).option("inferSchema", True).csv(path)
          .withColumn("_source_file", F.input_file_name())
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")))
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.bronze.{table}"))
    print(f"Loaded {table}: {df.count()} rows")