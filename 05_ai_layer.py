# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 05 · AI / Natural-Language Layer
# MAGIC
# MAGIC **Purpose:** Satisfy the brief's AI-layer deliverable with two working approaches.
# MAGIC
# MAGIC 1. **Genie Space** — text-to-SQL over `gold.wide_sales_genie`. Primary deliverable.
# MAGIC 2. **`ai_query()` LLM summary** — notebook cell that calls a Databricks-hosted
# MAGIC    foundation model from SQL to generate plain-English commentary from
# MAGIC    `agg_sales_monthly`. Backup demo and a real productionisation path
# MAGIC    (scheduled email summary).
# MAGIC
# MAGIC Both approaches are supported on Databricks Free Edition (serverless, quota-limited).
# MAGIC
# MAGIC **Prerequisites:** `00_setup`, `02_silver_transform`, `03_gold_build` already run.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Genie Space setup (primary deliverable)
# MAGIC
# MAGIC Genie is configured through the workspace UI, not code. This section is the
# MAGIC checklist to follow — written here as a runnable markdown cell so it lives in
# MAGIC the repo alongside the pipeline.
# MAGIC
# MAGIC ### Step 1 — Create the space
# MAGIC
# MAGIC 1. Workspace sidebar → **Genie** → **New Genie space**
# MAGIC 2. Name: `Commercial Sales Lead`
# MAGIC 3. Description:
# MAGIC    > *"Natural-language analytics over US retail sales. Ask questions about
# MAGIC    > revenue, orders, products, states, customers, and loyalty tiers."*
# MAGIC 4. **Add tables:**
# MAGIC    - `lakehouse_demo.gold.wide_sales_genie` (primary — denormalised, per-line)
# MAGIC    - `lakehouse_demo.gold.dim_customer` (optional — for customer-level questions)
# MAGIC    - `lakehouse_demo.gold.dim_product` (optional — for product-level questions)
# MAGIC    - `lakehouse_demo.gold.dim_loyalty_segment` (optional — explains tier thresholds)
# MAGIC
# MAGIC The column comments written by `03_gold_build` are what Genie actually reads
# MAGIC to understand the data — they do most of the heavy lifting. Don't skip that step.
# MAGIC
# MAGIC ### Step 2 — Add instructions (paste verbatim into the Genie space)
# MAGIC
# MAGIC > - This data covers US retail sales only. All customer states are two-letter US codes.
# MAGIC > - "Revenue" always means `SUM(line_revenue)`. "Orders" means `COUNT(DISTINCT order_number)`.
# MAGIC > - "Customers" means `COUNT(DISTINCT customer_id)`.
# MAGIC > - "Average order value" or "AOV" is `SUM(line_revenue) / COUNT(DISTINCT order_number)`.
# MAGIC > - The `loyalty_segment` column uses five labels: Prospect, Bronze, Silver, Gold, Platinum.
# MAGIC > - When asked about "growth" or "trend", aggregate by `year_month`.
# MAGIC > - When asked about "top" anything, default to `LIMIT 10` unless specified.
# MAGIC > - Always round revenue and AOV to 2 decimal places.
# MAGIC > - When asked about a customer by name, match on `first_name` and `last_name`
# MAGIC >   (silver-derived), not `customer_name` (which is in `LAST, FIRST` source format).
# MAGIC > - The `region` and `district` columns may be NULL — exclude NULLs from filters
# MAGIC >   unless the user is explicitly asking about data quality.
# MAGIC
# MAGIC ### Step 3 — Add trusted sample questions
# MAGIC
# MAGIC Trusted examples teach Genie preferred phrasing. Add at least these six:
# MAGIC
# MAGIC 1. *"What were the top 10 products by revenue last quarter?"*
# MAGIC 2. *"Show monthly revenue trend by loyalty segment for the last 12 months."*
# MAGIC 3. *"Which states have the highest average order value?"*
# MAGIC 4. *"How did Platinum customer revenue change month over month?"*
# MAGIC 5. *"What's the revenue split between weekday and weekend orders?"*
# MAGIC 6. *"Find me the top 5 customers by revenue in California."*
# MAGIC
# MAGIC ### Step 4 — Two demo queries for the submission (required: at least 2)
# MAGIC
# MAGIC The brief requires **at least 2 example queries with outputs**. Run these live
# MAGIC and screenshot the results for the write-up:
# MAGIC
# MAGIC **Query A:** *"What were our top 5 products by revenue in the most recent
# MAGIC complete month, and how does that compare to the month before?"*
# MAGIC
# MAGIC **Query B:** *"Which state has the highest Platinum customer revenue, and what
# MAGIC product category do those Platinum customers buy most?"*
# MAGIC
# MAGIC Save the screenshots to `docs/genie_examples.md` with a one-line caption each.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — `ai_query()` summary cell
# MAGIC
# MAGIC Notebook-friendly alternative. Pulls last 6 months of aggregate data, passes it
# MAGIC to a Databricks-hosted LLM, returns a bullet-point executive summary.
# MAGIC
# MAGIC **Why include this alongside Genie:**
# MAGIC - **Live demo backup** — if Genie has a bad moment during the call, pivot here.
# MAGIC - **Different integration pattern** — SQL-native LLM call, no UI dependency.
# MAGIC - **Real productionisation path** — wrap this in a scheduled Workflow and email
# MAGIC   the result to the Sales Lead every Monday at 07:00. Mention this in the write-up
# MAGIC   to show consumption-channel thinking.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pick an available foundation model
# MAGIC
# MAGIC Free Edition exposes several Databricks-hosted LLM endpoints via `ai_query`.
# MAGIC Endpoint names evolve, so before locking in a model, check what's currently
# MAGIC available in the workspace:
# MAGIC
# MAGIC - **Workspace sidebar → Serving** → look for endpoints starting with `databricks-`
# MAGIC - Common Free Edition options: `databricks-meta-llama-3-1-70b-instruct`,
# MAGIC   `databricks-llama-4-maverick`, `databricks-claude-haiku`
# MAGIC - Pick whichever responds in the workspace and update `AI_MODEL` below

# COMMAND ----------

# Update this if the workspace doesn't expose this exact endpoint name.
# Check Serving in the sidebar before running.
AI_MODEL = "databricks-meta-llama-3-1-70b-instruct"
print(f"Using model endpoint: {AI_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1 — Aggregate-level executive summary
# MAGIC
# MAGIC Aggregates the last 6 months at `year_month` grain, serialises to JSON, sends
# MAGIC to the LLM with an explicit persona prompt ("you are a sales analyst writing for
# MAGIC a Commercial Sales Lead"). Returns 3 bullet points: trend, positive signal, risk.

# COMMAND ----------

summary_df = spark.sql(f"""
    WITH recent AS (
        SELECT
            year_month,
            ROUND(SUM(total_revenue), 2)                                   AS revenue,
            SUM(orders)                                                    AS orders,
            SUM(unique_customers)                                          AS customers,
            ROUND(SUM(total_revenue) / NULLIF(SUM(orders), 0), 2)          AS aov
        FROM lakehouse_demo.gold.agg_sales_monthly
        GROUP BY year_month
        ORDER BY year_month DESC
        LIMIT 6
    )
    SELECT
        ai_query(
            '{AI_MODEL}',
            CONCAT(
                'You are a sales analyst writing for a Commercial Sales Lead. ',
                'Using the last 6 months of monthly sales data below, produce exactly 3 bullet points: ',
                '(1) headline revenue trend with specific numbers, ',
                '(2) one positive signal worth celebrating, ',
                '(3) one risk or concern that warrants attention. ',
                'Be specific with numbers. Do not hedge. Do not include preamble. ',
                'Data: ',
                to_json(collect_list(struct(year_month, revenue, orders, customers, aov)))
            )
        ) AS executive_summary
    FROM recent
""")

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2 — Per-state commentary (row-level ai_query)
# MAGIC
# MAGIC Different pattern: one LLM call per row instead of one call over an aggregate.
# MAGIC Returns a one-sentence insight per top-performing state. Demonstrates how
# MAGIC `ai_query` can be embedded in regular SELECT pipelines for enrichment.

# COMMAND ----------

state_commentary = spark.sql(f"""
    WITH top_states AS (
        SELECT state,
               ROUND(SUM(total_revenue), 2)                              AS revenue,
               SUM(orders)                                               AS orders,
               ROUND(SUM(total_revenue) / NULLIF(SUM(orders), 0), 2)     AS aov,
               SUM(unique_customers)                                     AS customers
        FROM lakehouse_demo.gold.agg_sales_monthly
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY revenue DESC
        LIMIT 5
    )
    SELECT
        state, revenue, orders, aov, customers,
        ai_query(
            '{AI_MODEL}',
            CONCAT(
                'You write one-sentence sales insights for a Commercial Sales Lead. ',
                'Be direct, specific, and actionable. No preamble. ',
                'Describe what is notable about state ', state,
                ' with revenue $', revenue,
                ', ', orders, ' orders, ',
                customers, ' unique customers, and $', aov, ' AOV. ',
                'In one sentence.'
            )
        ) AS insight
    FROM top_states
""")

display(state_commentary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3 — Loyalty tier analysis
# MAGIC
# MAGIC Third pattern: aggregate-level LLM commentary on the loyalty distribution.
# MAGIC Builds the input data from `wide_sales_genie` joined with the loyalty thresholds
# MAGIC so the LLM gets context on what each tier represents.

# COMMAND ----------

loyalty_summary = spark.sql(f"""
    WITH loyalty_perf AS (
        SELECT
            l.loyalty_segment_label,
            l.unit_threshold,
            ROUND(SUM(f.line_revenue), 2)                              AS revenue,
            COUNT(DISTINCT f.customer_id)                              AS customers,
            ROUND(SUM(f.line_revenue)
                  / NULLIF(COUNT(DISTINCT f.customer_id), 0), 2)       AS revenue_per_customer
        FROM lakehouse_demo.gold.fact_sales f
        JOIN lakehouse_demo.gold.dim_customer c
          ON f.customer_id = c.customer_id
        JOIN lakehouse_demo.gold.dim_loyalty_segment l
          ON c.loyalty_segment = l.loyalty_segment_id
        GROUP BY l.loyalty_segment_label, l.unit_threshold
    )
    SELECT
        ai_query(
            '{AI_MODEL}',
            CONCAT(
                'You write loyalty-program analysis for a Commercial Sales Lead. ',
                'Given the per-tier metrics below, produce 3 bullets: ',
                '(1) which tier delivers the most revenue per customer and why this is or is not surprising, ',
                '(2) where the biggest opportunity lies (e.g. moving Bronze customers up), ',
                '(3) one concrete recommendation. ',
                'Numbers, no hedging, no preamble. ',
                'Data: ',
                to_json(collect_list(struct(loyalty_segment_label, unit_threshold,
                                            revenue, customers, revenue_per_customer)))
            )
        ) AS loyalty_insight
    FROM loyalty_perf
""")

display(loyalty_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Live demo strategy
# MAGIC
# MAGIC **Lead with Genie.** It's the "Databricks way" and the strongest platform signal —
# MAGIC shows you understand the AI/BI story, set up proper governance through table
# MAGIC and column comments, and curated trusted-example questions.
# MAGIC
# MAGIC **Keep `ai_query()` as backup.** If Genie stumbles live (LLMs have bad days),
# MAGIC pivot to the notebook cells — they're deterministic, run in seconds, and
# MAGIC demonstrate the same "natural-language interface over gold" idea from a
# MAGIC different angle.
# MAGIC
# MAGIC **Frame the difference explicitly.** A single sentence in the live demo:
# MAGIC > *"Genie is interactive text-to-SQL for the Sales Lead themselves; ai_query
# MAGIC > is how I'd schedule a Monday-morning summary email to land in their inbox
# MAGIC > without any interaction at all. Different channels for the same stakeholder."*
# MAGIC
# MAGIC That framing is a Principal-level signal — it shows you're thinking about
# MAGIC consumption patterns and channels, not just tools.

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI layer complete
# MAGIC
# MAGIC | Requirement                                       | How satisfied                                       |
# MAGIC |---------------------------------------------------|-----------------------------------------------------|
# MAGIC | Text-to-SQL natural-language interface            | Genie Space over `wide_sales_genie` + 3 dims        |
# MAGIC | LLM-powered summary                               | `ai_query` cells over agg + facts (3 patterns)      |
# MAGIC | At least 2 example queries with outputs           | Genie Queries A + B (screenshots in `docs/`)        |
# MAGIC
# MAGIC Three different `ai_query` patterns are demonstrated:
# MAGIC 1. **Aggregate → single summary** (executive summary over 6 months of monthly aggregates)
# MAGIC 2. **Row-level enrichment** (one insight per top state, embedded in a SELECT)
# MAGIC 3. **Cross-table aggregate** (loyalty tier analysis joining fact + dims for richer context)

