# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC **Purpose:** One-time workspace setup for the lakehouse take-home task.
# MAGIC
# MAGIC Creates the Unity Catalog namespace (`lakehouse_demo.bronze / silver / gold`) and
# MAGIC verifies that the `retail-org` sample dataset is reachable before the pipeline runs.
# MAGIC
# MAGIC **Safe to re-run** at any time — all operations are idempotent.
# MAGIC
# MAGIC Idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG = "lakehouse_demo"
SCHEMAS = ["bronze", "silver", "gold"]

SOURCE_ROOT = "/databricks-datasets/retail-org"

# Tables the pipeline will touch — used for availability check below
EXPECTED_SOURCES = {
    "customers":    f"{SOURCE_ROOT}/customers/customers.csv",
    "products":     f"{SOURCE_ROOT}/products/products.csv",
    "sales_orders": f"{SOURCE_ROOT}/sales_orders/",
}

print(f"Target catalog : {CATALOG}")
print(f"Target schemas : {SCHEMAS}")
print(f"Source root    : {SOURCE_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create catalog and schemas
# MAGIC
# MAGIC All DDL uses `IF NOT EXISTS` — safe to re-run at any time without
# MAGIC side-effects. If the catalog or schemas already exist, the statements
# MAGIC are no-ops. `ALTER SCHEMA ... SET DBPROPERTIES` is wrapped in a
# MAGIC try/except so a privilege gap never aborts the rest of setup.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    # ALTER SCHEMA ... SET DBPROPERTIES is safe to re-run — UC silently
    # overwrites existing properties with the same values.  Wrapped in
    # try/except so that environments where the schema was created by a
    # different principal (and we lack ALTER privilege) don't abort setup.
    try:
        spark.sql(
            f"ALTER SCHEMA {CATALOG}.{schema} SET DBPROPERTIES "
            f"('layer' = '{schema}', 'owner' = 'data-engineering')"
        )
    except Exception as e:
        print(f"  [WARN] Could not set DBPROPERTIES on {schema}: {e} — continuing.")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify source data is available
# MAGIC
# MAGIC `retail-org` lives in the pre-mounted `/databricks-datasets/` location.
# MAGIC We confirm each expected path exists before the bronze pipeline depends on it —
# MAGIC fails loud and early if the dataset has moved or been renamed.

# COMMAND ----------

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

print("Top-level retail-org contents:")
for f in dbutils.fs.ls(SOURCE_ROOT):
    print(f"  {f.name}")

print("\nExpected source paths:")
missing = []
for name, path in EXPECTED_SOURCES.items():
    ok = path_exists(path)
    marker = "OK " if ok else "MISSING"
    print(f"  [{marker}] {name:<15} {path}")
    if not ok:
        missing.append(name)

if missing:
    raise RuntimeError(
        f"Source paths missing: {missing}. "
        f"Check /databricks-datasets/retail-org/ layout before running the pipeline."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Peek at source schemas
# MAGIC
# MAGIC Quick sanity check on column names, separators, and nested structure.
# MAGIC Nothing is written — this is purely diagnostic.

# COMMAND ----------

print("=" * 70)
print("customers.csv (first 500 chars)")
print("=" * 70)
print(dbutils.fs.head(EXPECTED_SOURCES["customers"], 500))

# COMMAND ----------

print("=" * 70)
print("products.csv (first 500 chars) — note ';' separator")
print("=" * 70)
print(dbutils.fs.head(EXPECTED_SOURCES["products"], 500))

# COMMAND ----------

# sales_orders/ is a folder of JSON files; inspect the first one
sales_files = dbutils.fs.ls(EXPECTED_SOURCES["sales_orders"])
print(f"Sales-orders folder contains {len(sales_files)} file(s).")
print("=" * 70)
print(f"First file: {sales_files[0].name} (first 1500 chars)")
print("=" * 70)
print(dbutils.fs.head(sales_files[0].path, 1500))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Setup complete
# MAGIC
# MAGIC - Catalog `lakehouse_demo` and three schemas exist (created or already present)
# MAGIC - All source paths verified
# MAGIC - Safe to re-run at any time — all operations are idempotent
# MAGIC - Ready to run `01_bronze_ingest`
