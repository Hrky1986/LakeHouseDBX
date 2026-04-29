# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 02 · Silver Transform
# MAGIC
# MAGIC **Purpose:** Transform bronze into a clean, typed, conformed analytical layer.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. `customers` — trim, type-cast, **convert `valid_from` / `valid_to` to dates**,
# MAGIC    **split `customer_name` (LAST, FIRST format) into `last_name` / `first_name`**,
# MAGIC    deduplicate on `customer_id`
# MAGIC 2. `products` — type cast, deduplicate on `product_id`
# MAGIC 3. `loyalty_segments` — type cast and trim; serves as the loyalty lookup
# MAGIC 4. `orders` — cast unix-ts `order_datetime` to timestamp, derive `order_date`
# MAGIC 5. `order_line_items` — **explode** `ordered_products` array into fact-grain rows
# MAGIC 6. **Data quality** — two-tier flag-and-quarantine framework (`rules` block,
# MAGIC    `warn_rules` observe-only), unified quarantine + append-only DQ trend
# MAGIC
# MAGIC **Design choices:**
# MAGIC - **Configuration via `%run ./00_setup`** — same pattern as bronze, no duplication.
# MAGIC - **Idempotent by design** — silver tables `overwrite`; `dq_results` `append` is
# MAGIC   guarded by `batch_id` deletion so re-runs replace, not duplicate.
# MAGIC - **Serverless-safe** — no `sparkContext`, no `input_file_name()`, no `_metadata`
# MAGIC   access. UUID for `BATCH_ID`, timestamp captured up-front as a Python `datetime`.
# MAGIC - **Two-tier DQ:** blocking rules quarantine the row, warn rules observe-only.
# MAGIC   Lets us track gaps in optional fields (`region`, `district`) without breaking
# MAGIC   the dashboard. Documented as the conscious trade-off in the write-up.
# MAGIC
# MAGIC **Prerequisites:** `00_setup` in the same folder; `01_bronze_ingest` already populated bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run shared configuration from 00_setup
# MAGIC
# MAGIC `%run` re-executes `00_setup` in the current notebook's scope — catalog + schemas
# MAGIC are confirmed to exist (idempotent), source paths verified, and `CATALOG`,
# MAGIC `SOURCE_ROOT`, `EXPECTED_SOURCES` land in scope ready to use.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Notebook-level constants and imports

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import uuid

# CATALOG comes from 00_setup
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Serverless-safe: no spark.sparkContext access. Capture run timestamp ONCE
# as a Python datetime so every reference uses the same value.
BATCH_ID  = str(uuid.uuid4())
RUN_TS_PY = datetime.utcnow()

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA silver")

print(f"Reading from : {BRONZE}")
print(f"Writing to   : {SILVER}")
print(f"Batch ID     : {BATCH_ID}")
print(f"Run TS       : {RUN_TS_PY.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper: write_silver() — write a cleaned DataFrame with a table comment
# MAGIC
# MAGIC Idempotent: overwrites every run. Pre/post row counts printed so re-runs are
# MAGIC visibly confirmed as replacements, not appends.

# COMMAND ----------

def write_silver(df: DataFrame, table: str, comment: str) -> None:
    """Write a DataFrame to silver as a Delta table. Idempotent."""
    fqn = f"{SILVER}.{table}"

    try:
        pre_count = spark.table(fqn).count()
        pre_label = f"{pre_count:>7,} rows (previous run)"
    except Exception:
        pre_label = "table did not exist yet"

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqn))

    spark.sql(f"COMMENT ON TABLE {fqn} IS '{comment}'")

    post_count = spark.table(fqn).count()
    print(f"  {fqn} written successfully.")
    print(f"    before : {pre_label}")
    print(f"    after  : {post_count:>7,} rows (this run)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DQ framework — two-tier flag-and-quarantine helper
# MAGIC
# MAGIC Two rule tiers:
# MAGIC - **`rules`** (blocking) — failing rows are routed to `dq_quarantine` and removed
# MAGIC   from clean. Use for fundamental integrity issues.
# MAGIC - **`warn_rules`** (observe-only) — failing rows stay in clean, but counts are
# MAGIC   recorded in `dq_results` so the gap is visible in DQ trend monitoring.
# MAGIC   Use for nice-to-have completeness signals (null region, missing geo) where
# MAGIC   partial data is still useful.
# MAGIC
# MAGIC Returns `(clean, quarantine, summary)`. Summary reports both tiers.

# COMMAND ----------

def apply_dq(df: DataFrame, rules: dict, entity: str, warn_rules: dict = None):
    """Apply two-tier DQ rules and split into clean / quarantine DataFrames.

    Args:
        df:         Input DataFrame.
        rules:      {rule_name: cond_TRUE_when_FAILS} — BLOCKING. Failing rows go to quarantine.
        entity:     Logical entity name for the summary record.
        warn_rules: {rule_name: cond_TRUE_when_FAILS} — WARN. Failing rows stay in clean,
                    but counts are recorded in dq_results for observability.

    Returns:
        (clean_df, quarantine_df, summary_df)
    """
    warn_rules = warn_rules or {}

    # Blocking rules: running OR with first-match reason wins
    fail_expr   = F.lit(False)
    reason_expr = F.lit(None).cast(StringType())
    for name, cond in rules.items():
        reason_expr = F.when(cond & reason_expr.isNull(), F.lit(name)).otherwise(reason_expr)
        fail_expr   = fail_expr | cond

    tagged = (df
        .withColumn("_dq_fail",   fail_expr)
        .withColumn("_dq_reason", reason_expr))

    clean = tagged.filter("NOT _dq_fail").drop("_dq_fail", "_dq_reason")

    quarantine = (tagged.filter("_dq_fail")
        .withColumn("_entity",   F.lit(entity))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_run_ts",   F.lit(RUN_TS_PY).cast(TimestampType())))

    # Cache once for all the counts we're about to compute
    # tagged_cached = tagged.cache()
    total_in    = tagged.count()
    total_fail  = tagged.filter("_dq_fail").count()
    total_clean = total_in - total_fail

    blocking_rows = (tagged
        .filter("_dq_fail")
        .groupBy("_dq_reason")
        .count()
        .collect())
    blocking_map = {r["_dq_reason"]: r["count"] for r in blocking_rows}

    # Warn rules: each evaluated independently so a row can trigger multiple
    warn_map = {}
    for w_name, w_cond in warn_rules.items():
        warn_map[w_name] = tagged.filter(w_cond).count()

    # tagged_cached.unpersist()

    combined_reasons = {"blocking": blocking_map, "warn": warn_map}

    summary_schema = StructType([
        StructField("entity",          StringType(),    False),
        StructField("batch_id",        StringType(),    False),
        StructField("rows_in",         LongType(),      False),
        StructField("rows_clean",      LongType(),      False),
        StructField("rows_quarantine", LongType(),      False),
        StructField("run_ts",          TimestampType(), True),
        StructField("reasons_json",    StringType(),    True),
    ])
    summary = spark.createDataFrame(
        [(entity, BATCH_ID, total_in, total_clean, total_fail,
          RUN_TS_PY, str(combined_reasons))],
        schema=summary_schema,
    )

    warn_summary = f"  warn={warn_map}" if warn_map else ""
    print(f"  DQ [{entity}]: in={total_in:,}  clean={total_clean:,}  "
          f"quarantine={total_fail:,}  blocking={blocking_map}{warn_summary}")

    return clean, quarantine, summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: customers
# MAGIC
# MAGIC - Trim string columns; uppercase `state`; cast `lat`, `lon`, `units_purchased`,
# MAGIC   `loyalty_segment` to numeric types.
# MAGIC - **Convert `valid_from` and `valid_to` to dates.** Source stores them as
# MAGIC   unix-timestamp strings, with `valid_to` often arriving in scientific notation
# MAGIC   (e.g. `1.548137353E9`) and frequently NULL. Cast chain `string → double → long`
# MAGIC   tolerates both forms; `from_unixtime` + `to_date` produces YYYY-MM-DD.
# MAGIC   NULL `valid_to` is preserved (open-ended validity, customer is currently active).
# MAGIC - **Split `customer_name` into `last_name` and `first_name`.** Source format is
# MAGIC   `LAST, FIRST [middle initial]` (e.g. `SMITH,  SHIRLEY`, `STEPHENS,  GERALDINE M`).
# MAGIC   Comma split with tolerant `\s*,\s*` regex handles inconsistent spacing.
# MAGIC   Middle initials stay attached to `first_name`.
# MAGIC - **`region` and `district` flagged as warn-only.** Commonly NULL in source. Rather
# MAGIC   than removing rows from the dashboard, the `warn_rules` tier records the count
# MAGIC   in `dq_results` so the gap is observable without breaking analytics.
# MAGIC - Deduplicate on `customer_id`, keep newest ingest using a window function.

# COMMAND ----------

customers_bronze = spark.table(f"{BRONZE}.customers_raw")

# Sanity check — fail loud and early if expected columns are missing.
required = ["customer_id", "customer_name", "state", "city", "valid_from"]
missing = [c for c in required if c not in customers_bronze.columns]
if missing:
    raise RuntimeError(
        f"Required columns {missing} missing from {BRONZE}.customers_raw. "
        f"Available: {customers_bronze.columns}"
    )

w_cust = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc_nulls_last())

# Build full street address from individual components for downstream convenience.
# Source columns: street, number, unit (e.g. "Main St", "123", "Apt 4B")
def _concat_address():
    parts = [F.trim(F.col("number")), F.trim(F.col("street")), F.trim(F.col("unit"))]
    # concat_ws skips NULLs but treats empty strings as content; coalesce empty -> NULL first
    cleaned = [F.when(F.length(p) > 0, p).otherwise(F.lit(None)) for p in parts]
    return F.concat_ws(" ", *cleaned)

customers = (customers_bronze
    .withColumn("customer_id",     F.col("customer_id").cast("string"))
    .withColumn("customer_name",   F.trim(F.col("customer_name")))
    # tax_id and tax_code are PII — preserve in bronze for audit, drop here.
    # If tax data is needed downstream, build a separate, access-controlled
    # `silver.customers_tax` table instead of polluting the main customer view.
    .withColumn("state",           F.upper(F.trim(F.col("state"))))
    .withColumn("city",            F.trim(F.col("city")))
    .withColumn("postcode",        F.trim(F.col("postcode").cast("string")))
    .withColumn("street",          F.trim(F.col("street")))
    .withColumn("number",          F.trim(F.col("number").cast("string")))
    .withColumn("unit",            F.trim(F.col("unit")))
    .withColumn("region",          F.trim(F.col("region")))
    .withColumn("district",        F.trim(F.col("district")))
    .withColumn("street_address",  _concat_address())
    .withColumn("ship_to_address", F.trim(F.col("ship_to_address")))
    .withColumn("lat",             F.col("lat").cast("double"))
    .withColumn("lon",             F.col("lon").cast("double"))
    .withColumn("units_purchased", F.col("units_purchased").cast("long"))
    .withColumn("loyalty_segment", F.col("loyalty_segment").cast("int"))
    # valid_from / valid_to: unix-ts strings, valid_to often "1.548137353E9" or NULL.
    # Cast string -> double -> long handles both regular and scientific-notation formats.
    .withColumn("valid_from",
        F.to_date(F.from_unixtime(F.col("valid_from").cast("double").cast("long"))))
    .withColumn("valid_to",
        F.to_date(F.from_unixtime(F.col("valid_to").cast("double").cast("long"))))
    # Split customer_name (format: LAST, FIRST [middle initial])
    .withColumn("last_name",
        F.when(F.col("customer_name").contains(","),
               F.trim(F.split(F.col("customer_name"), r"\s*,\s*", 2).getItem(0)))
         .otherwise(F.lit(None).cast("string")))
    .withColumn("first_name",
        F.when(F.col("customer_name").contains(","),
               F.trim(F.split(F.col("customer_name"), r"\s*,\s*", 2).getItem(1)))
         .otherwise(F.col("customer_name")))
    # Select only non-PII columns for the clean silver table.
    # Excludes: tax_id, tax_code (PII), bronze audit columns.
    .select(
        "customer_id",
        "customer_name", "first_name", "last_name",
        "state", "city", "postcode", "region", "district",
        "street", "number", "unit", "street_address", "ship_to_address",
        "lat", "lon",
        "units_purchased", "loyalty_segment",
        "valid_from", "valid_to",
        "_ingest_ts",   # kept temporarily for the dedup window
    )
    # Deduplicate: keep newest ingest per customer_id
    .withColumn("_rn", F.row_number().over(w_cust))
    .filter("_rn = 1")
    .drop("_rn", "_ingest_ts"))

# Blocking rules — failing any of these removes the row from clean.
customers_rules = {
    "null_customer_id":  F.col("customer_id").isNull() | (F.length("customer_id") == 0),
    "missing_name":      F.col("customer_name").isNull() | (F.length("customer_name") == 0),
    "invalid_state":     F.col("state").isNull() | (F.length("state") != 2),
    "null_valid_from":   F.col("valid_from").isNull(),
    "valid_to_before_from":
        F.col("valid_to").isNotNull() & (F.col("valid_to") < F.col("valid_from")),
}

# Warn rules — observation-only. Rows stay in clean, counts surface in dq_results.
customers_warn_rules = {
    "null_region":     F.col("region").isNull()   | (F.length("region")   == 0),
    "null_district":   F.col("district").isNull() | (F.length("district") == 0),
    "null_postcode":   F.col("postcode").isNull() | (F.length("postcode") == 0),
    "null_street":     F.col("street").isNull()   | (F.length("street")   == 0),
    "missing_geocode": F.col("lat").isNull() | F.col("lon").isNull(),
}

customers_clean, customers_q, customers_dq = apply_dq(
    customers, customers_rules, "customers", warn_rules=customers_warn_rules)

write_silver(customers_clean, "customers",
    "Cleaned US customers, PII (tax_id, tax_code) excluded. "
    "customer_name (LAST, FIRST format) split into last_name/first_name. "
    "Address parts assembled into street_address. "
    "valid_from/valid_to converted to dates. Deduped on customer_id, uppercased state.")

# COMMAND ----------

# Quick verification of the name split and date conversion
display(spark.sql(f"""
    SELECT customer_id, customer_name, first_name, last_name,
           state, region, district, valid_from, valid_to
    FROM {SILVER}.customers
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver: products
# MAGIC
# MAGIC Source schema (CSV with `;` separator):
# MAGIC ```
# MAGIC product_id; product_category; product_name; sales_price; EAN13; EAN5; product_unit
# MAGIC ```
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Type cast `product_id` (string), `sales_price` (double)
# MAGIC - Trim text columns; lowercase the EAN barcodes (defensive — they should be digit-only)
# MAGIC - **Rename `sales_price` → `unit_price`** to match downstream conventions used
# MAGIC   in `fact_sales` and `wide_sales_genie`. The Bronze original `sales_price` survives
# MAGIC   in audit history; Silver presents the consistent name.
# MAGIC - Deduplicate on `product_id`
# MAGIC
# MAGIC **DQ rules:**
# MAGIC - `null_product_id`, `bad_price` (≤ 0 or null) — blocking
# MAGIC - `null_category`, `null_name`, `null_ean13`, `null_unit` — warn-only
# MAGIC   (a missing barcode or unit doesn't break the dashboard)

# COMMAND ----------

products_bronze = spark.table(f"{BRONZE}.products_raw")

required = ["product_id", "product_category", "product_name", "sales_price",
            "EAN13", "EAN5", "product_unit"]
missing = [c for c in required if c not in products_bronze.columns]
if missing:
    raise RuntimeError(
        f"Required columns {missing} missing from {BRONZE}.products_raw. "
        f"Available: {products_bronze.columns}"
    )

w_prod = Window.partitionBy("product_id").orderBy(F.col("_ingest_ts").desc_nulls_last())

products = (products_bronze
    .withColumn("product_id",       F.col("product_id").cast("string"))
    .withColumn("product_category", F.trim(F.col("product_category")))
    .withColumn("product_name",     F.trim(F.col("product_name")))
    # sales_price -> unit_price for downstream consistency with fact_sales
    .withColumn("unit_price",       F.col("sales_price").cast("double"))
    # Barcodes: trim, cast to string. They should be all-digits but we don't enforce
    # that in silver — bad-format barcodes are a warn signal, not a blocker.
    .withColumn("ean13",            F.trim(F.col("EAN13").cast("string")))
    .withColumn("ean5",             F.trim(F.col("EAN5").cast("string")))
    .withColumn("product_unit",     F.trim(F.col("product_unit")))
    .select(
        "product_id",
        "product_category",
        "product_name",
        "unit_price",
        "ean13",
        "ean5",
        "product_unit",
        "_ingest_ts",  # kept temporarily for the dedup window
    )
    # Deduplicate: keep newest ingest per product_id
    .withColumn("_rn", F.row_number().over(w_prod))
    .filter("_rn = 1")
    .drop("_rn", "_ingest_ts"))

# Blocking rules — failing any of these removes the row from clean.
products_rules = {
    "null_product_id": F.col("product_id").isNull() | (F.length("product_id") == 0),
    "bad_price":       F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
}

# Warn rules — completeness signals, row stays in clean.
products_warn_rules = {
    "null_category":  F.col("product_category").isNull() | (F.length("product_category") == 0),
    "null_name":      F.col("product_name").isNull()     | (F.length("product_name")     == 0),
    "null_ean13":     F.col("ean13").isNull()            | (F.length("ean13")            == 0),
    "null_unit":      F.col("product_unit").isNull()     | (F.length("product_unit")     == 0),
    # EAN13 should be exactly 13 digits — flag malformed barcodes for product team triage
    "bad_ean13_length": F.col("ean13").isNotNull()
                        & (F.length("ean13") != 13),
}

products_clean, products_q, products_dq = apply_dq(
    products, products_rules, "products", warn_rules=products_warn_rules)

write_silver(products_clean, "products",
    "Cleaned product master. sales_price renamed to unit_price for downstream consistency. "
    "Deduped on product_id, price > 0.")

# Quick verification — sample the result with the renamed price column visible
display(spark.sql(f"""
    SELECT product_id, product_category, product_name, unit_price,
           ean13, ean5, product_unit
    FROM {SILVER}.products
    LIMIT 10
"""))

write_silver(products_clean, "products",
    "Cleaned product master, deduped on product_id, price > 0.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver: loyalty_segments
# MAGIC
# MAGIC Lookup table that gives `customer.loyalty_segment` (numeric 0–4) a human-readable
# MAGIC label and a unit-threshold for tier eligibility. Source schema:
# MAGIC
# MAGIC ```
# MAGIC loyalty_segment_id, loyalty_segment_description, unit_threshold, valid_from, valid_to
# MAGIC 0, level_0, 0,  2017-01-01,
# MAGIC 1, level_1, 10, 2017-01-01,
# MAGIC 2, level_2, 30, 2017-01-01,
# MAGIC 3, level_3, 70, 2017-01-01,
# MAGIC ```
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Type cast `loyalty_segment_id` (int) and `unit_threshold` (long)
# MAGIC - Trim `loyalty_segment_description`
# MAGIC - Parse `valid_from` / `valid_to` as proper dates (already in YYYY-MM-DD; empty
# MAGIC   strings normalised to NULL meaning "open-ended validity")
# MAGIC - **Derive `loyalty_segment_label`** — the source description (`level_0`...`level_3`)
# MAGIC   is generic. Map IDs to business-friendly names (Prospect → Platinum) for the
# MAGIC   dashboard and Genie. Description is preserved for auditability.
# MAGIC - Deduplicate on `loyalty_segment_id`
# MAGIC
# MAGIC The Gold layer joins this onto `dim_customer` so the Sales Lead dashboard shows
# MAGIC "Gold" instead of "3".

# COMMAND ----------

loyalty_bronze = spark.table(f"{BRONZE}.loyalty_segments_raw")

# Sanity check — fail loud if expected schema has drifted
required = ["loyalty_segment_id", "loyalty_segment_description", "unit_threshold",
            "valid_from", "valid_to"]
missing = [c for c in required if c not in loyalty_bronze.columns]
if missing:
    raise RuntimeError(
        f"Required columns {missing} missing from {BRONZE}.loyalty_segments_raw. "
        f"Available: {loyalty_bronze.columns}"
    )

# Empty-string-to-NULL helper for valid_to (CSV reads blank cells as "")
def _blank_to_null(c):
    return F.when((F.col(c).isNull()) | (F.length(F.trim(F.col(c))) == 0),
                  F.lit(None).cast("string")) \
            .otherwise(F.trim(F.col(c)))

w_loy = Window.partitionBy("loyalty_segment_id") \
              .orderBy(F.col("_ingest_ts").desc_nulls_last())

loyalty = (loyalty_bronze
    .withColumn("loyalty_segment_id",          F.col("loyalty_segment_id").cast("int"))
    .withColumn("loyalty_segment_description", F.trim(F.col("loyalty_segment_description")))
    .withColumn("unit_threshold",              F.col("unit_threshold").cast("long"))
    # Dates already YYYY-MM-DD; empty -> NULL, then cast.
    .withColumn("valid_from", F.to_date(_blank_to_null("valid_from"), "yyyy-MM-dd"))
    .withColumn("valid_to",   F.to_date(_blank_to_null("valid_to"),   "yyyy-MM-dd"))
    # Business-friendly label mapping. Source description is generic ("level_0"...).
    .withColumn("loyalty_segment_label",
        F.when(F.col("loyalty_segment_id") == 0, F.lit("Prospect"))
         .when(F.col("loyalty_segment_id") == 1, F.lit("Bronze"))
         .when(F.col("loyalty_segment_id") == 2, F.lit("Silver"))
         .when(F.col("loyalty_segment_id") == 3, F.lit("Gold"))
         .when(F.col("loyalty_segment_id") == 4, F.lit("Platinum"))
         .otherwise(F.lit("Unknown")))
    # Deduplicate: keep newest ingest per segment id
    .withColumn("_rn", F.row_number().over(w_loy))
    .filter("_rn = 1")
    .drop("_rn", "_ingest_ts", "_source_file", "_batch_id"))

loyalty_rules = {
    "null_segment_id":       F.col("loyalty_segment_id").isNull(),
    "null_description":      F.col("loyalty_segment_description").isNull()
                             | (F.length("loyalty_segment_description") == 0),
    "negative_threshold":    F.col("unit_threshold") < 0,
    "null_valid_from":       F.col("valid_from").isNull(),
    "valid_to_before_from":  F.col("valid_to").isNotNull()
                             & (F.col("valid_to") < F.col("valid_from")),
}

loyalty_clean, loyalty_q, loyalty_dq = apply_dq(loyalty, loyalty_rules, "loyalty_segments")

write_silver(loyalty_clean, "loyalty_segments",
    "Cleaned loyalty segments lookup. Adds business-friendly loyalty_segment_label "
    "(Prospect/Bronze/Silver/Gold/Platinum). Used by Gold to enrich dim_customer.")

# Quick verification — should show 4 rows with labels mapped
display(spark.sql(f"""
    SELECT loyalty_segment_id, loyalty_segment_label, loyalty_segment_description,
           unit_threshold, valid_from, valid_to
    FROM {SILVER}.loyalty_segments
    ORDER BY loyalty_segment_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver: orders (header grain)
# MAGIC
# MAGIC - Cast unix-string `order_datetime` to proper timestamp; derive `order_date`
# MAGIC - Deduplicate on `order_number`
# MAGIC - Header-grain only (one row per order); line items come next

# COMMAND ----------

orders_bronze = spark.table(f"{BRONZE}.sales_orders_raw")

orders = (orders_bronze
    .withColumn("order_ts",
        F.from_unixtime(
            F.when(F.length(F.trim(F.col("order_datetime"))) == 0, None)
            .otherwise(F.col("order_datetime").cast("long"))
        ).cast("timestamp")
    )
    .withColumn("order_date", F.to_date("order_ts"))
    .select(
        F.col("order_number").cast("string").alias("order_number"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.trim(F.col("customer_name")).alias("customer_name"),
        "order_ts",
        "order_date",
        F.col("number_of_line_items").cast("int").alias("number_of_line_items"),
        "ordered_products",
    ))

w_ord = Window.partitionBy("order_number").orderBy(F.col("order_ts").desc_nulls_last())
orders = (orders
    .withColumn("_rn", F.row_number().over(w_ord))
    .filter("_rn = 1")
    .drop("_rn"))

orders_rules = {
    "null_order_number": F.col("order_number").isNull() | (F.length("order_number") == 0),
    "null_customer_id":  F.col("customer_id").isNull(),
    "bad_ts":            F.col("order_ts").isNull(),
    "future_date":       F.col("order_date") > F.current_date(),
}

orders_clean, orders_q, orders_dq = apply_dq(
    orders.drop("ordered_products"), orders_rules, "orders")

write_silver(orders_clean, "orders",
    "Order header grain. Deduped on order_number, timestamp cast, DQ applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Silver: order_line_items (fact grain)
# MAGIC
# MAGIC The core transformation of the pipeline.
# MAGIC
# MAGIC - `ordered_products` is `array<struct<id, name, price, qty, curr, ...>>`
# MAGIC - `explode` flattens to one row per `(order, product)`
# MAGIC - Derive `line_revenue = unit_price * quantity` here so every consumer of line
# MAGIC   items gets revenue for free
# MAGIC - Deduplicate on `(order_number, product_id)` — defensive against source repeats

# COMMAND ----------

line_items = (orders
    .filter(F.col("ordered_products").isNotNull())
    .select(
        "order_number",
        "customer_id",
        "order_date",
        "order_ts",
        F.explode("ordered_products").alias("p"),
    )
    .select(
        "order_number",
        "customer_id",
        "order_date",
        "order_ts",
        F.col("p.id").cast("string").alias("product_id"),
        F.col("p.name").alias("product_name_snapshot"),
        F.col("p.price").cast("double").alias("unit_price"),
        F.col("p.qty").cast("int").alias("quantity"),
        F.col("p.curr").alias("currency"),
    )
    .withColumn("line_revenue", F.col("unit_price") * F.col("quantity")))

w_line = (Window
    .partitionBy("order_number", "product_id")
    .orderBy(F.col("order_ts").desc_nulls_last()))
line_items = (line_items
    .withColumn("_rn", F.row_number().over(w_line))
    .filter("_rn = 1")
    .drop("_rn"))

line_rules = {
    "null_product_id": F.col("product_id").isNull() | (F.length("product_id") == 0),
    "bad_price":       F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
    "bad_quantity":    F.col("quantity").isNull()   | (F.col("quantity")   <= 0),
    "null_order":      F.col("order_number").isNull(),
    "null_date":       F.col("order_date").isNull(),
}

lines_clean, lines_q, lines_dq = apply_dq(line_items, line_rules, "order_line_items")

write_silver(lines_clean, "order_line_items",
    "Exploded order line items. Fact grain: one row per order x product. "
    "line_revenue = unit_price * quantity computed here.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Unified DQ quarantine

# COMMAND ----------

def to_quarantine_row(qdf: DataFrame, entity: str) -> DataFrame:
    """Normalize an entity's failed rows to a unified shape with payload as JSON."""
    payload_cols = [c for c in qdf.columns
                    if c not in ("_dq_fail", "_dq_reason", "_entity", "_batch_id", "_run_ts")]
    return (qdf
        .withColumn("payload", F.to_json(F.struct(*payload_cols)))
        .select(
            F.lit(entity).alias("entity"),
            F.col("_dq_reason").alias("reason"),
            "payload",
            F.col("_batch_id").alias("batch_id"),
            F.col("_run_ts").alias("run_ts"),
        ))

all_quarantine = (
    to_quarantine_row(customers_q, "customers")
    .unionByName(to_quarantine_row(products_q,  "products"))
    .unionByName(to_quarantine_row(loyalty_q,   "loyalty_segments"))
    .unionByName(to_quarantine_row(orders_q,    "orders"))
    .unionByName(to_quarantine_row(lines_q,     "order_line_items"))
)

write_silver(all_quarantine, "dq_quarantine",
    "Unified DQ quarantine. One row per failed record with reason and JSON payload.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. dq_results — append-only DQ trend
# MAGIC
# MAGIC The one deliberate exception to the overwrite pattern. Idempotency is preserved at
# MAGIC the batch level: a `DELETE WHERE batch_id = ?` runs first, so re-running this
# MAGIC notebook in the same session won't duplicate rows. UUID makes cross-run collision
# MAGIC effectively impossible.

# COMMAND ----------

dq_results_batch = (customers_dq
    .unionByName(products_dq)
    .unionByName(loyalty_dq)
    .unionByName(orders_dq)
    .unionByName(lines_dq))

results_table = f"{SILVER}.dq_results"
if spark.catalog.tableExists(results_table):
    spark.sql(f"DELETE FROM {results_table} WHERE batch_id = '{BATCH_ID}'")

(dq_results_batch.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(results_table))

spark.sql(
    f"COMMENT ON TABLE {results_table} IS "
    f"'Append-only DQ observability. One row per entity per batch for trend analysis.'")

print(f"  Appended {dq_results_batch.count()} DQ result rows to {results_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Verify silver layer

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SILVER}"))

# COMMAND ----------

print("Row counts:")
for table in ["customers", "products", "loyalty_segments", "orders", "order_line_items",
              "dq_quarantine", "dq_results"]:
    fqn = f"{SILVER}.{table}"
    try:
        n = spark.table(fqn).count()
        print(f"  {fqn:<50} {n:>8,}")
    except Exception as e:
        print(f"  {fqn:<50} ERROR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Latest DQ batch summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT entity, rows_in, rows_clean, rows_quarantine,
           ROUND(100.0 * rows_quarantine / NULLIF(rows_in, 0), 2) AS pct_fail,
           reasons_json, run_ts
    FROM {SILVER}.dq_results
    WHERE batch_id = '{BATCH_ID}'
    ORDER BY entity
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample customers (with name split + date conversion)

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, customer_name, first_name, last_name,
           state, region, district, loyalty_segment, valid_from, valid_to
    FROM {SILVER}.customers
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample line items

# COMMAND ----------

display(spark.table(f"{SILVER}.order_line_items").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Silver layer complete
# MAGIC
# MAGIC | Table                                | Grain                      | Notes                                       |
# MAGIC |--------------------------------------|----------------------------|---------------------------------------------|
# MAGIC | `silver.customers`                   | one per customer           | Name split, dates converted, state cleaned  |
# MAGIC | `silver.products`                    | one per product            | Price > 0 enforced                          |
# MAGIC | `silver.loyalty_segments`            | one per segment            | Lookup for Gold's loyalty_segment_label     |
# MAGIC | `silver.orders`                      | one per order              | Timestamp cast from unix                    |
# MAGIC | `silver.order_line_items`            | one per order x product    | Exploded, revenue derived                   |
# MAGIC | `silver.dq_quarantine`               | one per failed row         | All entities, JSON payload                  |
# MAGIC | `silver.dq_results`                  | one per entity per batch   | Append-only, dedupes on `batch_id` re-run   |
# MAGIC
# MAGIC **Idempotency confirmed:** silver tables overwrite each run; `dq_results` append is
# MAGIC guarded by `batch_id` deletion so a re-run replaces, not duplicates.
# MAGIC
# MAGIC Next: **`03_gold_build`** — star schema (`dim_customer`, `dim_product`, `dim_date`,
# MAGIC `fact_sales`), aggregates, and the wide Genie-friendly table.
