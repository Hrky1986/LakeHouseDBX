# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Ingest
# MAGIC
# MAGIC **Purpose:** Land raw `retail-org` files as Delta tables with **no transformations**,
# MAGIC only audit metadata (`_ingest_ts`, `_source_file`, `_batch_id`).
# MAGIC
# MAGIC **Design choices:**
# MAGIC - **Configuration via `%run ./00_setup`** — all constants (`CATALOG`, `SOURCE_ROOT`,
# MAGIC   `EXPECTED_SOURCES`) are defined once in `00_setup` and shared here. No duplication,
# MAGIC   no drift between notebooks.
# MAGIC - **Idempotent by design** — write mode `overwrite` with `overwriteSchema=true`
# MAGIC   replaces the table on every run. Re-running produces the same result as the first
# MAGIC   run: same rows, same schema, fresh audit timestamps. Safe to run multiple times.
# MAGIC - **No filtering, no type casting, no joins.** Bronze is the auditable source of truth
# MAGIC   so any downstream bug can be replayed without re-reading source files.
# MAGIC - Every row carries provenance: when it was loaded, which file it came from,
# MAGIC   which batch loaded it.
# MAGIC
# MAGIC **Prerequisites:** `00_setup` must be in the same folder as this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load shared configuration from 00_setup
# MAGIC
# MAGIC `%run` executes `00_setup` in the current notebook's scope — all variables
# MAGIC (`CATALOG`, `SCHEMAS`, `SOURCE_ROOT`, `EXPECTED_SOURCES`) become available here
# MAGIC without re-defining them. This is the standard Databricks pattern for sharing
# MAGIC configuration across a multi-notebook pipeline.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Notebook-level constants and imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid

# CATALOG, SOURCE_ROOT, EXPECTED_SOURCES all come from 00_setup above
BRONZE   = f"{CATALOG}.bronze"
# spark.sparkContext.applicationId accesses the JVM directly and is not
# supported on serverless compute. Use a UUID instead — unique per run,
# no JVM access required.
BATCH_ID = str(uuid.uuid4())

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA bronze")

print(f"Writing to : {BRONZE}")
print(f"Batch ID   : {BATCH_ID}")
print(f"Source     : {SOURCE_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper: land() — write any DataFrame to bronze with audit columns
# MAGIC
# MAGIC One function used for every source. The `overwrite` + `overwriteSchema=true`
# MAGIC combination is the idempotency guarantee: every run produces an identical table,
# MAGIC with fresh `_ingest_ts` and `_batch_id` values reflecting the current execution.
# MAGIC A pre-run row count is captured and printed alongside the post-run count so
# MAGIC re-runs are visibly confirmed as replacements, not appends.

# COMMAND ----------

def land(df: DataFrame, table: str, comment: str) -> None:
    """Write a DataFrame to bronze as a Delta table with audit metadata.

    Idempotent: overwrites the table and schema on every run.
    Re-running produces the same result as the first run.

    Args:
        df:       Source DataFrame (read as-is, no transformations).
        table:    Target table name (unqualified — written to CATALOG.bronze).
        comment:  Table comment for catalog discoverability.
    """
    fqn = f"{BRONZE}.{table}"

    # Capture pre-run count so re-runs can confirm replacement, not append
    try:
        pre_count = spark.table(fqn).count()
        pre_label = f"{pre_count:>7,} rows (previous run)"
    except Exception:
        pre_label = "table did not exist yet"

    enriched = (
        df
        .withColumn("_ingest_ts",   current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id",    lit(BATCH_ID))
    )

    (enriched.write
        .format("delta")
        .mode("overwrite")                   # idempotent: replaces on every run
        .option("overwriteSchema", "true")   # tolerates source schema changes
        .saveAsTable(fqn))

    spark.sql(f"COMMENT ON TABLE {fqn} IS '{comment}'")

    post_count = spark.table(fqn).count()
    print(f"  {fqn}")
    print(f"    before : {pre_label}")
    print(f"    after  : {post_count:>7,} rows (this run)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest: customers
# MAGIC
# MAGIC - Source: `EXPECTED_SOURCES["customers"]` — path from `00_setup`
# MAGIC - Format: CSV with header row
# MAGIC - US-only customers (acknowledged source limitation — not a bug to fix)

# COMMAND ----------

customers_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(EXPECTED_SOURCES["customers"]))

land(customers_df,
     table="customers_raw",
     comment="Raw customer master data from retail-org sample. US-only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest: products
# MAGIC
# MAGIC - Source: `EXPECTED_SOURCES["products"]` — path from `00_setup`
# MAGIC - Format: CSV
# MAGIC - **Separator is `;`**, not the default `,`. Easy to miss — flagged explicitly here.

# COMMAND ----------

products_df = (spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", True)
    .csv(EXPECTED_SOURCES["products"]))

land(products_df,
     table="products_raw",
     comment="Raw product master. CSV with ';' separator.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingest: sales orders
# MAGIC
# MAGIC - Source: `EXPECTED_SOURCES["sales_orders"]` — path from `00_setup`
# MAGIC - Format: JSON, one file per batch, nested structure
# MAGIC - `order_datetime` is a **unix timestamp stored as a string** — cast in silver, not here
# MAGIC - `ordered_products` is an **array of structs** (line items) — exploded in silver
# MAGIC
# MAGIC Bronze keeps the nested shape as-is so we don't lose fidelity.

# COMMAND ----------

sales_orders_df = spark.read.json(EXPECTED_SOURCES["sales_orders"])

land(sales_orders_df,
     table="sales_orders_raw",
     comment="Raw sales orders. JSON, nested ordered_products array. Unix-ts order_datetime.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify bronze layer

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {BRONZE}"))

# COMMAND ----------

# Row counts and audit-column presence — quick sanity check
for table in ["customers_raw", "products_raw", "sales_orders_raw"]:
    fqn = f"{BRONZE}.{table}"
    row_count  = spark.table(fqn).count()
    audit_cols = [c for c in spark.table(fqn).columns if c.startswith("_")]
    print(f"{fqn}")
    print(f"  rows        : {row_count:>7,}")
    print(f"  audit cols  : {audit_cols}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sample each bronze table

# COMMAND ----------

display(spark.table(f"{BRONZE}.customers_raw").limit(5))

# COMMAND ----------

display(spark.table(f"{BRONZE}.products_raw").limit(5))

# COMMAND ----------

display(spark.table(f"{BRONZE}.sales_orders_raw").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Bronze layer complete
# MAGIC
# MAGIC Three Delta tables written with full audit metadata:
# MAGIC
# MAGIC | Table                                    | Source                      | Notes                         |
# MAGIC |------------------------------------------|-----------------------------|-------------------------------|
# MAGIC | `lakehouse_demo.bronze.customers_raw`    | `customers/customers.csv`   | CSV, header                   |
# MAGIC | `lakehouse_demo.bronze.products_raw`     | `products/products.csv`     | CSV, `;` separator            |
# MAGIC | `lakehouse_demo.bronze.sales_orders_raw` | `sales_orders/*.json`       | JSON, nested ordered_products |
# MAGIC
# MAGIC **Idempotency confirmed:** each `land()` call prints a before/after row count.
# MAGIC A re-run shows identical counts — the table was replaced, not appended to.
# MAGIC
# MAGIC Next: **`02_silver_transform`** — cleaning, explode, deduplicate, DQ + quarantine.

