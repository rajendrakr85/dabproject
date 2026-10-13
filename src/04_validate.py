# Databricks notebook source
dbutils.widgets.text("catalog", "ecommerce_catalog")
catalog = dbutils.widgets.get("catalog")

checks = {
    "customers_not_empty": spark.table(f"{catalog}.silver.customers").count() > 0,
    "orders_not_empty": spark.table(f"{catalog}.silver.orders").count() > 0,
    "daily_sales_not_empty": spark.table(f"{catalog}.gold.daily_sales").count() > 0,
    "order_id_not_null": spark.table(f"{catalog}.silver.orders").filter("order_id IS NULL").count() == 0,
    "order_total_non_negative": spark.table(f"{catalog}.silver.orders").filter("order_total < 0").count() == 0,
}

failed = [name for name, passed in checks.items() if not passed]
for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")
if failed:
    raise ValueError(f"Data quality checks failed: {failed}")