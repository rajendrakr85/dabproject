# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ecommerce_catalog")
catalog = dbutils.widgets.get("catalog")

customers = (spark.table(f"{catalog}.bronze.customers")
    .dropDuplicates(["customer_id"])
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("email", F.lower(F.trim("email"))))

products = (spark.table(f"{catalog}.bronze.products")
    .dropDuplicates(["product_id"])
    .withColumn("unit_cost", F.col("unit_cost").cast("decimal(18,2)"))
    .withColumn("list_price", F.col("list_price").cast("decimal(18,2)"))
    .withColumn("stock_quantity", F.col("stock_quantity").cast("int")))

orders = (spark.table(f"{catalog}.bronze.orders")
    .dropDuplicates(["order_id"])
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn("order_date", F.to_date("order_timestamp"))
    .withColumn("order_total", F.col("order_total").cast("decimal(18,2)"))
    .withColumn("subtotal", F.col("subtotal").cast("decimal(18,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(18,2)"))
    .withColumn("tax_amount", F.col("tax_amount").cast("decimal(18,2)"))
    .withColumn("shipping_fee", F.col("shipping_fee").cast("decimal(18,2)")))

items = (spark.table(f"{catalog}.bronze.order_items")
    .dropDuplicates(["order_item_id"])
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(18,2)"))
    .withColumn("line_total", F.col("line_total").cast("decimal(18,2)")))

payments = (spark.table(f"{catalog}.bronze.payments")
    .dropDuplicates(["payment_id"])
    .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)")))

shipments = (spark.table(f"{catalog}.bronze.shipments")
    .dropDuplicates(["shipment_id"])
    .withColumn("shipped_timestamp", F.to_timestamp("shipped_timestamp"))
    .withColumn("delivered_timestamp", F.to_timestamp("delivered_timestamp")))

returns = (spark.table(f"{catalog}.bronze.returns")
    .dropDuplicates(["return_id"])
    .withColumn("return_date", F.to_date("return_date"))
    .withColumn("refund_amount", F.col("refund_amount").cast("decimal(18,2)")))

events = (spark.table(f"{catalog}.bronze.website_events")
    .dropDuplicates(["event_id"])
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp")))

frames = {"customers": customers, "products": products, "orders": orders,
          "order_items": items, "payments": payments, "shipments": shipments,
          "returns": returns, "website_events": events}
for name, frame in frames.items():
    (frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{catalog}.silver.{name}"))
    print(f"Silver {name}: {frame.count()} rows")