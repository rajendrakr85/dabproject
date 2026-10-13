# Databricks notebook source

dbutils.widgets.text("catalog", "ecommerce_catalog")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("source_volume", "raw_files")

catalog = dbutils.widgets.get("catalog").strip()
landing_schema = dbutils.widgets.get("landing_schema").strip()
source_volume = dbutils.widgets.get("source_volume").strip()

# Validate identifiers
for name in (catalog, landing_schema, source_volume):
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {name}")

# Check that the catalog already exists
available_catalogs = {
    row[0] for row in spark.sql("SHOW CATALOGS").collect()
}

if catalog not in available_catalogs:
    raise ValueError(
        f"Catalog '{catalog}' does not exist. "
        "Create it through Catalog Explorer using Default Storage."
    )

print(f"Catalog verified: {catalog}")

# Create/verify schemas
for schema_name in (landing_schema, "bronze", "silver", "gold"):
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"`{catalog}`.`{schema_name}`"
    )
    print(f"Schema verified: {catalog}.{schema_name}")

# Create/verify source volume
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    `{catalog}`.`{landing_schema}`.`{source_volume}`
    """
)

source_path = f"/Volumes/{catalog}/{landing_schema}/{source_volume}"

print(f"Volume verified: {catalog}.{landing_schema}.{source_volume}")
print(f"Source path: {source_path}")

# Confirm source files are available
display(dbutils.fs.ls(source_path))