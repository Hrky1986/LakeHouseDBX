# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 03 · Gold Build
# MAGIC
# MAGIC **Purpose:** Build the business-ready analytical layer for the Commercial / Sales Lead.
# MAGIC
# MAGIC **What this notebook produces:**
# MAGIC 1. `dim_customer`        — customer dimension (SCD Type 1)
# MAGIC 2. `dim_product`         — product dimension (SCD Type 1)
# MAGIC 3. `dim_loyalty_segment` — loyalty tier lookup (SCD Type 1)
# MAGIC 4. `dim_date`            — generated date dimension
# MAGIC 5. `fact_sales`          — line-item fact, FKs to dimensions
# MAGIC 6. `agg_sales_monthly`   — pre-aggregated, backs the dashboard
# MAGIC 7. `wide_sales_genie`    — denormalised + heavily commented for Genie
# MAGIC
# MAGIC **Why SCD Type 1 (overwrite):**
# MAGIC The source is a static snapshot CSV without reliable change-event timestamps.
# MAGIC SCD2 would generate "history" tied to when the *pipeline* ran, not when the
# MAGIC underlying business event happened. SCD1 is the honest choice given the source.
# MAGIC
# MAGIC SCD2 unlocks when one of the following is available: a CDC feed from the source
# MAGIC system (Debezium, Azure SQL CDC, etc.), an append-only event log of attribute
# MAGIC changes (Kafka topic), or source-maintained `effective_from` / `effective_to`
# MAGIC fields. The architecture document covers this as a productionisation item.
# MAGIC
# MAGIC **Idempotency:** every gold table uses `overwrite` — re-running the notebook
# MAGIC with no source change produces the same tables. No history to preserve, no
# MAGIC merge logic, no surrogate keys.
# MAGIC
# MAGIC **Prerequisites:** `00_setup` in the same folder; `02_silver_transform` populated silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run shared configuration from 00_setup

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Notebook-level constants and imports

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import uuid
 
SILVER = f"{CATALOG}.silver"
GOLD   = f"{CATALOG}.gold"
 
BATCH_ID  = str(uuid.uuid4())
RUN_TS_PY = datetime.utcnow()
 
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA gold")
 
def _sql_escape(s: str) -> str:
    """Escape single quotes for use inside a SQL single-quoted string literal."""
    return s.replace("'", "''") if s else s
 
print(f"Reading from : {SILVER}")
print(f"Writing to   : {GOLD}")
print(f"Batch ID     : {BATCH_ID}")
print(f"Run TS       : {RUN_TS_PY.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper: write_gold() — overwrite a Delta table with comments
# MAGIC
# MAGIC One helper used for every gold table. \
# MAGIC Idempotent: every run replaces the table. \
# MAGIC Pre-/post-row counts printed so re-runs visibly confirm replacement, not append.

# COMMAND ----------

def write_gold(df: DataFrame, table: str, table_comment: str,
               column_comments: dict = None) -> None:
    """Write a DataFrame to gold as a Delta table with table + column comments."""
    fqn = f"{GOLD}.{table}"
 
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
 
    spark.sql(f"COMMENT ON TABLE {fqn} IS '{_sql_escape(table_comment)}'")
 
    if column_comments:
        for col_name, cm in column_comments.items():
            spark.sql(
                f"ALTER TABLE {fqn} ALTER COLUMN {col_name} "
                f"COMMENT '{_sql_escape(cm)}'"
            )
 
    post_count = spark.table(fqn).count()
    print(f"  {fqn}")
    print(f"    before : {pre_label}")
    print(f"    after  : {post_count:>7,} rows (this run)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_customer — SCD1
# MAGIC
# MAGIC One row per customer. `customer_id` is both the natural key and primary key —
# MAGIC no surrogate key needed without SCD2 versioning.
# MAGIC
# MAGIC Drops `customer_name` field already split in silver (`first_name`, `last_name` are
# MAGIC the consumer-friendly fields), keeps the raw `customer_name` for traceability.

# COMMAND ----------

customers_silver = spark.table(f"{SILVER}.customers")
 
dim_customer = customers_silver.select(
    F.col("customer_id"),
    F.col("customer_name"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("state"),
    F.col("city"),
    F.col("postcode"),
    F.col("region"),
    F.col("district"),
    F.col("street_address"),
    F.col("ship_to_address"),
    F.col("lat"),
    F.col("lon"),
    F.col("loyalty_segment"),
    F.col("valid_from").alias("source_valid_from"),
    F.col("valid_to").alias("source_valid_to"),
)
 
write_gold(
    dim_customer, "dim_customer",
    table_comment=(
        "Customer dimension (SCD Type 1). One row per customer_id. "
        "Source-provided valid_from/valid_to are retained but not used for versioning — "
        "they originate from a static snapshot without reliable change-event timing."
    ),
    column_comments={
        "customer_id":     "Primary key. Stable business identifier.",
        "customer_name":   "Original 'LAST, FIRST' as stored in source.",
        "first_name":      "Given name(s) including any middle initial.",
        "last_name":       "Family name (surname).",
        "state":           "Two-letter US state code.",
        "city":            "Customer city.",
        "postcode":        "Customer postal code.",
        "region":          "Region. May be NULL — see silver.dq_results for completeness.",
        "district":        "District. May be NULL — see silver.dq_results.",
        "street_address":  "Composed from number + street + unit.",
        "lat":             "Latitude for geo visualisations.",
        "lon":             "Longitude for geo visualisations.",
        "loyalty_segment": "Numeric loyalty tier 0-4. Join dim_loyalty_segment for label.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_product — SCD1

# COMMAND ----------

products_silver = spark.table(f"{SILVER}.products")
 
dim_product = products_silver.select(
    F.col("product_id"),
    F.col("product_name"),
    F.col("product_category"),
    F.col("unit_price").alias("list_price"),
    F.col("ean13"),
    F.col("ean5"),
    F.col("product_unit"),
)
 
write_gold(
    dim_product, "dim_product",
    table_comment=(
        "Product dimension (SCD Type 1). One row per product_id. "
        "list_price is the master price; fact_sales.unit_price is the actual sale price."
    ),
    column_comments={
        "product_id":       "Primary key. Stable business identifier.",
        "product_name":     "Product display name.",
        "product_category": "Category. Use this for category-level analysis.",
        "list_price":       "Master list price. fact_sales.unit_price is the actual sale price.",
        "ean13":            "EAN-13 barcode.",
        "ean5":             "EAN-5 supplementary barcode.",
        "product_unit":     "Sales unit (e.g. each, kg, l).",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. dim_loyalty_segment — SCD1

# COMMAND ----------

loyalty_silver = spark.table(f"{SILVER}.loyalty_segments")
 
dim_loyalty_segment = loyalty_silver.select(
    F.col("loyalty_segment_id"),
    F.col("loyalty_segment_label"),
    F.col("loyalty_segment_description"),
    F.col("unit_threshold"),
    F.col("valid_from").alias("source_valid_from"),
    F.col("valid_to").alias("source_valid_to"),
)
 
write_gold(
    dim_loyalty_segment, "dim_loyalty_segment",
    table_comment=(
        "Loyalty segment dimension (SCD Type 1). One row per loyalty_segment_id 0-4. "
        "Provides the human-readable loyalty_segment_label used by reports and Genie."
    ),
    column_comments={
        "loyalty_segment_id":          "Primary key. Numeric tier 0-4 (Prospect..Platinum).",
        "loyalty_segment_label":       "Business-friendly label (Prospect, Bronze, Silver, Gold, Platinum).",
        "loyalty_segment_description": "Source description (level_0, level_1...).",
        "unit_threshold":              "Units required to reach this tier.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. dim_date — generated, no SCD needed
# MAGIC
# MAGIC Spans observed order history. Dates are immutable so SCD doesn't apply here —
# MAGIC use `overwrite`, regenerate the full range each run. Cheap.

# COMMAND ----------

bounds = (spark.table(f"{SILVER}.order_line_items")
    .agg(F.min("order_date").alias("min_d"),
         F.max("order_date").alias("max_d"))
    .collect()[0])
 
min_date, max_date = bounds["min_d"], bounds["max_d"]
print(f"Observed order-date range: {min_date} → {max_date}")
 
dim_date = (spark.sql(f"""
    SELECT explode(sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    )) AS date_key
""")
.withColumn("year",        F.year("date_key"))
.withColumn("quarter",     F.quarter("date_key"))
.withColumn("month",       F.month("date_key"))
.withColumn("month_name",  F.date_format("date_key", "MMMM"))
.withColumn("year_month",  F.date_format("date_key", "yyyy-MM"))
.withColumn("day",         F.dayofmonth("date_key"))
.withColumn("day_of_week", F.dayofweek("date_key"))
.withColumn("day_name",    F.date_format("date_key", "EEEE"))
.withColumn("is_weekend",  F.dayofweek("date_key").isin([1, 7])))
 
write_gold(
    dim_date, "dim_date",
    table_comment="Date dimension. One row per day covering observed order history.",
    column_comments={
        "date_key":    "Primary key. Same as date.",
        "year":        "Calendar year.",
        "quarter":     "Calendar quarter 1-4.",
        "month":       "Calendar month 1-12.",
        "month_name":  "Full month name (January, February, ...).",
        "year_month":  "YYYY-MM string. Use for month-level grouping.",
        "day_of_week": "Day of week 1 (Sunday) to 7 (Saturday).",
        "is_weekend":  "True if Saturday or Sunday.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. fact_sales
# MAGIC
# MAGIC One row per order line. \
# MAGIC FKs are the natural keys — `customer_id`, `product_id`, `date_key` — joining directly to the SCD1 dimensions.

# COMMAND ----------

line_items = spark.table(f"{SILVER}.order_line_items")
 
fact_sales = line_items.select(
    F.monotonically_increasing_id().alias("sale_id"),
    F.col("order_number"),
    F.col("order_ts"),
    F.col("order_date").alias("date_key"),
    F.col("customer_id"),
    F.col("product_id"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("line_revenue"),
    F.col("currency"),
)
 
write_gold(
    fact_sales, "fact_sales",
    table_comment=(
        "Line-item sales fact. One row per order x product. "
        "FKs: customer_id -> dim_customer, product_id -> dim_product, date_key -> dim_date."
    ),
    column_comments={
        "sale_id":      "Surrogate primary key for the line.",
        "order_number": "Natural order identifier. Multiple lines share one order_number.",
        "order_ts":     "Order timestamp (UTC).",
        "date_key":     "FK to dim_date. Same as order_date.",
        "customer_id":  "FK to dim_customer.",
        "product_id":   "FK to dim_product.",
        "quantity":     "Units sold on this line.",
        "unit_price":   "Actual sale price per unit.",
        "line_revenue": "Total revenue for this line = quantity * unit_price.",
        "currency":     "ISO currency code.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. agg_sales_monthly — pre-aggregated for the dashboard
# MAGIC
# MAGIC Monthly cube backing the four dashboard tiles. \
# MAGIC Pre-computing once means the dashboard renders sub-second on a 2X-Small SQL warehouse.

# COMMAND ----------

agg_monthly = (spark.table(f"{GOLD}.fact_sales").alias("f")
    .join(spark.table(f"{GOLD}.dim_customer").alias("c"),
          "customer_id", "left")
    .join(spark.table(f"{GOLD}.dim_product").alias("p"),
          "product_id", "left")
    .join(spark.table(f"{GOLD}.dim_loyalty_segment").alias("l"),
          F.col("c.loyalty_segment") == F.col("l.loyalty_segment_id"), "left")
    .join(spark.table(f"{GOLD}.dim_date").alias("d"),
          "date_key", "left")
    .groupBy(
        F.col("d.year_month").alias("year_month"),
        F.col("d.year").alias("year"),
        F.col("d.month").alias("month"),
        F.col("c.state").alias("state"),
        F.col("p.product_category").alias("product_category"),
        F.col("l.loyalty_segment_label").alias("loyalty_segment"),
    )
    .agg(
        F.sum("line_revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_number").alias("orders"),
        F.countDistinct(F.col("c.customer_id")).alias("unique_customers"),
        F.count("*").alias("line_count"),
    )
    .withColumn("aov",
        F.when(F.col("orders") > 0,
               F.col("total_revenue") / F.col("orders"))
         .otherwise(F.lit(0.0))))
 
write_gold(
    agg_monthly, "agg_sales_monthly",
    table_comment=(
        "Pre-aggregated monthly sales by state, category, loyalty segment. Backs dashboard."
    ),
    column_comments={
        "year_month":       "YYYY-MM. Month of the order.",
        "state":            "Customer state.",
        "product_category": "Product category.",
        "loyalty_segment":  "Loyalty tier label (Prospect, Bronze, Silver, Gold, Platinum).",
        "total_revenue":    "Sum of line_revenue for the grouping.",
        "total_units":      "Sum of quantity.",
        "orders":           "Distinct order count.",
        "unique_customers": "Distinct customer count.",
        "aov":              "Average order value = total_revenue / orders.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. wide_sales_genie — denormalised, heavily commented for Genie
# MAGIC
# MAGIC Genie produces measurably better SQL against a single well-commented table than
# MAGIC against multi-table joins. \
# MAGIC Comments here are written for the LLM as much as
# MAGIC for humans — they describe meaning and intended usage, not just data type.

# COMMAND ----------

wide_sales = (spark.table(f"{GOLD}.fact_sales").alias("f")
    .join(spark.table(f"{GOLD}.dim_customer").alias("c"),
          "customer_id", "left")
    .join(spark.table(f"{GOLD}.dim_product").alias("p"),
          "product_id", "left")
    .join(spark.table(f"{GOLD}.dim_loyalty_segment").alias("l"),
          F.col("c.loyalty_segment") == F.col("l.loyalty_segment_id"), "left")
    .join(spark.table(f"{GOLD}.dim_date").alias("d"),
          "date_key", "left")
    .select(
        F.col("f.order_number"),
        F.col("f.order_ts"),
        F.col("d.date_key").alias("order_date"),
        F.col("d.year"),
        F.col("d.quarter"),
        F.col("d.month"),
        F.col("d.year_month"),
        F.col("d.day_name"),
        F.col("d.is_weekend"),
        F.col("c.customer_id"),
        F.col("c.customer_name"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.state"),
        F.col("c.city"),
        F.col("c.region"),
        F.col("c.district"),
        F.col("l.loyalty_segment_label").alias("loyalty_segment"),
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.product_category"),
        F.col("p.list_price"),
        F.col("p.product_unit"),
        F.col("f.quantity"),
        F.col("f.unit_price"),
        F.col("f.line_revenue"),
        F.col("f.currency"),
    ))
 
write_gold(
    wide_sales, "wide_sales_genie",
    table_comment=(
        "Denormalised sales fact joined with all dimensions. One row per order line. "
        "Use this table for natural-language analytical questions about retail sales. "
        "Revenue is line_revenue. Orders are distinct order_number."
    ),
    column_comments={
        "order_number":     "Order identifier. Multiple rows share one order_number when an order has multiple products.",
        "order_date":       "Date the order was placed.",
        "year_month":       "YYYY-MM string of the order month. Use for month-level trends.",
        "customer_id":      "Customer identifier.",
        "customer_name":    "Customer name in source format (LAST, FIRST).",
        "first_name":       "Customer given name(s).",
        "last_name":        "Customer family name.",
        "state":            "US state where customer is located.",
        "city":             "Customer city.",
        "region":           "Customer region. May be NULL.",
        "district":         "Customer district. May be NULL.",
        "loyalty_segment":  "Loyalty tier label: Prospect, Bronze, Silver, Gold, or Platinum.",
        "product_name":     "Product name.",
        "product_category": "Product category. Good for category-level analysis.",
        "list_price":       "Master list price. Often differs from unit_price due to promotions.",
        "product_unit":     "Sales unit (e.g. each, kg).",
        "quantity":         "Units sold on this line.",
        "unit_price":       "Actual price per unit on this line.",
        "line_revenue":     "Revenue for this line in source currency. Sum this for total revenue.",
        "currency":         "ISO currency code.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Verify gold layer

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {GOLD}"))

# COMMAND ----------

print("Row counts:")
for table in ["dim_customer", "dim_product", "dim_loyalty_segment",
              "dim_date", "fact_sales", "agg_sales_monthly", "wide_sales_genie"]:
    fqn = f"{GOLD}.{table}"
    n = spark.table(fqn).count()
    print(f"  {fqn:<45} {n:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Headline KPIs — sanity check the numbers are sensible

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        ROUND(SUM(line_revenue), 2)        AS total_revenue,
        COUNT(DISTINCT order_number)        AS total_orders,
        COUNT(DISTINCT customer_id)         AS unique_customers,
        ROUND(SUM(line_revenue) / NULLIF(COUNT(DISTINCT order_number), 0), 2) AS aov,
        MIN(order_date)                     AS first_order,
        MAX(order_date)                     AS last_order
    FROM {GOLD}.wide_sales_genie
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 states by revenue

# COMMAND ----------

display(spark.sql(f"""
    SELECT state,
           ROUND(SUM(line_revenue), 2) AS revenue,
           COUNT(DISTINCT order_number) AS orders
    FROM {GOLD}.wide_sales_genie
    WHERE state IS NOT NULL
    GROUP BY state
    ORDER BY revenue DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by loyalty segment

# COMMAND ----------

display(spark.sql(f"""
    SELECT loyalty_segment,
           ROUND(SUM(line_revenue), 2) AS revenue,
           COUNT(DISTINCT customer_id)  AS customers,
           ROUND(AVG(line_revenue), 2)  AS avg_line_revenue
    FROM {GOLD}.wide_sales_genie
    WHERE loyalty_segment IS NOT NULL
    GROUP BY loyalty_segment
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample dim_customer rows showing SCD2 columns

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, customer_name, state, loyalty_segment
        FROM {GOLD}.dim_customer
    ORDER BY customer_id
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Gold layer complete
# MAGIC
# MAGIC | Table                              | SCD | Grain                 | Purpose                         |
# MAGIC |------------------------------------|-----|-----------------------|---------------------------------|
# MAGIC | `gold.dim_customer`                | 2   | one per cust version  | Customer attrs + history        |
# MAGIC | `gold.dim_product`                 | 2   | one per prod version  | Product attrs + history         |
# MAGIC | `gold.dim_loyalty_segment`         | 2   | one per tier version  | Tier label/threshold + history  |
# MAGIC | `gold.dim_date`                    | —   | one per day           | Date attributes for grouping    |
# MAGIC | `gold.fact_sales`                  | —   | one per order line    | Line fact, point-in-time SK FKs |
# MAGIC | `gold.agg_sales_monthly`           | —   | yr-mo × state × ...   | Pre-aggregated, dashboard       |
# MAGIC | `gold.wide_sales_genie`            | —   | one per order line    | Denormalised, for Genie         |
# MAGIC
# MAGIC **Re-run behaviour:** every table uses `overwrite`. Re-runs replace, do not append.
# MAGIC
# MAGIC Next: dashboard assembly + Genie Space setup (notebooks `04_*`, `05_*`).
