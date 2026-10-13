# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ecommerce_catalog")
catalog = dbutils.widgets.get("catalog")

orders = spark.table(f"{catalog}.silver.orders")
items = spark.table(f"{catalog}.silver.order_items")
products = spark.table(f"{catalog}.silver.products")
customers = spark.table(f"{catalog}.silver.customers")

daily_sales = (orders.filter(F.col("order_status").isin("Delivered", "Shipped"))
    .groupBy("order_date", "sales_channel")
    .agg(F.countDistinct("order_id").alias("order_count"),
         F.countDistinct("customer_id").alias("customer_count"),
         F.round(F.sum("order_total"), 2).alias("revenue"),
         F.round(F.avg("order_total"), 2).alias("average_order_value")))

product_sales = (items.join(products.select("product_id", "product_name", "category", "brand"), "product_id")
    .groupBy("product_id", "product_name", "category", "brand")
    .agg(F.sum("quantity").alias("units_sold"),
         F.round(F.sum("line_total"), 2).alias("product_revenue")))

customer_360 = (orders.groupBy("customer_id")
    .agg(F.countDistinct("order_id").alias("lifetime_orders"),
         F.round(F.sum("order_total"), 2).alias("lifetime_value"),
         F.max("order_timestamp").alias("last_order_timestamp"))
    .join(customers.select("customer_id", "first_name", "last_name", "city", "state", "customer_segment"), "customer_id", "left"))

for name, frame in {"daily_sales": daily_sales, "product_sales": product_sales, "customer_360": customer_360}.items():
    (frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{catalog}.gold.{name}"))
    print(f"Gold {name}: {frame.count()} rows")
