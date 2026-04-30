-- Databricks notebook source
-- /// script
-- [tool.databricks.environment]
-- environment_version = "2"
-- ///
-- MAGIC %md
-- MAGIC # 04 · Dashboard Queries
-- MAGIC
-- MAGIC **Purpose:** SQL queries backing the Commercial / Sales Lead AI/BI dashboard.
-- MAGIC
-- MAGIC **How this file is used:**
-- MAGIC - Each query below becomes one *dataset* in the AI/BI Dashboard
-- MAGIC - In the dashboard editor: **Data → Add → From SQL** → paste query → name it → bind to a widget
-- MAGIC - Most queries hit `gold.agg_sales_monthly` (pre-aggregated, fast on a 2X-Small warehouse)
-- MAGIC - Top-products and customer-detail queries hit `gold.wide_sales_genie` since the
-- MAGIC   monthly aggregate doesn't carry product or customer-level detail
-- MAGIC
-- MAGIC **Sales Lead narrative** (a text tile at the top of the dashboard):
-- MAGIC > "A weekly view of where revenue is coming from, how it's trending, and which
-- MAGIC > segments and categories are driving growth. Use the state filter to drill into a
-- MAGIC > territory; use the loyalty filter to see how each tier contributes."
-- MAGIC
-- MAGIC **Dashboard parameters to add:**
-- MAGIC - `:date_from`, `:date_to` — date range
-- MAGIC - `:state` — multi-select state filter
-- MAGIC - `:loyalty_segment` — multi-select loyalty tier
-- MAGIC
-- MAGIC The queries below don't bind these parameters explicitly — bind them in the
-- MAGIC dashboard's Filter widget, which applies them to all referenced columns.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 1 — Headline KPIs (with month-over-month deltas)
-- MAGIC
-- MAGIC **Widget:** 4 counter tiles across the top — Revenue, Orders, AOV, Unique Customers
-- MAGIC **Answers the Sales Lead's question:** "How are we doing this month vs last?"
-- MAGIC
-- MAGIC Returns one row per `year_month` with current values and MoM percent deltas.
-- MAGIC In the dashboard, filter to the latest `year_month` for the headline cards;
-- MAGIC the trend version of this same data appears in Query 2.

-- COMMAND ----------

WITH monthly AS (
    SELECT
        year_month,
        SUM(total_revenue)                              AS revenue,
        SUM(orders)                                     AS orders,
        SUM(unique_customers)                           AS unique_customers,
        SUM(total_revenue) / NULLIF(SUM(orders), 0)     AS aov
    FROM lakehouse_demo.gold.agg_sales_monthly
    GROUP BY year_month
),
ranked AS (
    SELECT
        year_month,
        revenue,
        orders,
        unique_customers,
        aov,
        LAG(revenue)          OVER (ORDER BY year_month) AS prev_revenue,
        LAG(orders)           OVER (ORDER BY year_month) AS prev_orders,
        LAG(aov)              OVER (ORDER BY year_month) AS prev_aov,
        LAG(unique_customers) OVER (ORDER BY year_month) AS prev_customers
    FROM monthly
)
SELECT
    year_month,
    ROUND(revenue, 2)                                                       AS revenue,
    ROUND(100.0 * (revenue - prev_revenue)
                / NULLIF(prev_revenue, 0), 1)                               AS revenue_mom_pct,
    orders,
    ROUND(100.0 * (orders - prev_orders)
                / NULLIF(prev_orders, 0), 1)                                AS orders_mom_pct,
    ROUND(aov, 2)                                                           AS aov,
    ROUND(100.0 * (aov - prev_aov)
                / NULLIF(prev_aov, 0), 1)                                   AS aov_mom_pct,
    unique_customers,
    ROUND(100.0 * (unique_customers - prev_customers)
                / NULLIF(prev_customers, 0), 1)                             AS customers_mom_pct
FROM ranked
ORDER BY year_month DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 2 — Monthly revenue trend by loyalty segment
-- MAGIC
-- MAGIC **Widget:** Stacked area chart (or stacked bar) — X = `year_month`,
-- MAGIC Y = `revenue`, colour = `loyalty_segment`
-- MAGIC **Answers:** "Are we growing, and which segments are driving (or dragging) it?"
-- MAGIC
-- MAGIC Stacking by loyalty segment tells a richer story than a single trend line —
-- MAGIC the Sales Lead sees whether growth is broad-based or concentrated in one tier.
-- MAGIC `Prospect` is excluded since it represents pre-loyalty customers and tends to
-- MAGIC dominate the chart with low-value transactions.

-- COMMAND ----------

SELECT
    year_month,
    loyalty_segment,
    ROUND(SUM(total_revenue), 2)        AS revenue,
    SUM(orders)                         AS orders
FROM lakehouse_demo.gold.agg_sales_monthly
WHERE year_month       IS NOT NULL
  AND loyalty_segment  IS NOT NULL
  AND loyalty_segment <> 'Prospect'
GROUP BY year_month, loyalty_segment
ORDER BY year_month, loyalty_segment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 3 — Revenue by state (top 10)
-- MAGIC
-- MAGIC **Widget:** Horizontal bar chart, or US choropleth (AI/BI supports US state choropleth)
-- MAGIC **Answers:** "Where is the money?"
-- MAGIC
-- MAGIC For the choropleth, remove `LIMIT 10` so every state gets a colour. The bar-chart
-- MAGIC version below is the focused top-10 ranking.

-- COMMAND ----------

SELECT
    state,
    ROUND(SUM(total_revenue), 2)                                AS revenue,
    SUM(orders)                                                 AS orders,
    SUM(unique_customers)                                       AS unique_customers,
    ROUND(SUM(total_revenue) / NULLIF(SUM(orders), 0), 2)       AS aov
FROM lakehouse_demo.gold.agg_sales_monthly
WHERE state IS NOT NULL
GROUP BY state
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 4 — Top 10 products by revenue
-- MAGIC
-- MAGIC **Widget:** Horizontal bar chart, colour-coded by `product_category`
-- MAGIC **Answers:** "What should I push harder, and which categories are winning?"
-- MAGIC
-- MAGIC Hits `wide_sales_genie` rather than `agg_sales_monthly` because the monthly
-- MAGIC aggregate doesn't carry product-level detail. On sample volumes the query is
-- MAGIC still fast on a 2X-Small warehouse. In production, an `agg_sales_by_product`
-- MAGIC table would be added — noted in `docs/writeup.md` as a productionisation item.

-- COMMAND ----------

SELECT
    product_name,
    product_category,
    ROUND(SUM(line_revenue), 2)         AS revenue,
    SUM(quantity)                       AS units_sold,
    COUNT(DISTINCT order_number)        AS orders
FROM lakehouse_demo.gold.wide_sales_genie
WHERE product_name IS NOT NULL
GROUP BY product_name, product_category
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 5 — Category x loyalty segment heatmap
-- MAGIC
-- MAGIC **Widget:** Heatmap (or pivot table)
-- MAGIC **Answers:** "Which segment buys which category? Any cross-sell angles?"
-- MAGIC
-- MAGIC Useful for spotting concentration. If "Apparel" revenue is overwhelmingly
-- MAGIC Bronze and "Electronics" overwhelmingly Platinum, that's a real Sales Lead
-- MAGIC insight: campaigns targeting Bronze customers should lead with Apparel.

-- COMMAND ----------

SELECT
    product_category,
    loyalty_segment,
    ROUND(SUM(total_revenue), 2)        AS revenue,
    SUM(orders)                         AS orders
FROM lakehouse_demo.gold.agg_sales_monthly
WHERE product_category IS NOT NULL
  AND loyalty_segment  IS NOT NULL
GROUP BY product_category, loyalty_segment
ORDER BY product_category, revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 6 — Top customers (optional drill-down tile)
-- MAGIC
-- MAGIC **Widget:** Table with conditional formatting on revenue
-- MAGIC **Answers:** "Who are our biggest accounts? Are we retaining them?"
-- MAGIC
-- MAGIC Uses the names already split in silver. `customer_name` (raw "LAST, FIRST") is
-- MAGIC included alongside `first_name` / `last_name` for any consumer who prefers the
-- MAGIC original format. Filter by `:state` and `:loyalty_segment` in the dashboard.

-- COMMAND ----------

SELECT
    customer_id,
    first_name,
    last_name,
    state,
    city,
    loyalty_segment,
    ROUND(SUM(line_revenue), 2)         AS revenue,
    COUNT(DISTINCT order_number)        AS orders,
    ROUND(SUM(line_revenue)
        / NULLIF(COUNT(DISTINCT order_number), 0), 2) AS aov,
    MAX(order_date)                     AS last_order_date
FROM lakehouse_demo.gold.wide_sales_genie
WHERE customer_id IS NOT NULL
GROUP BY customer_id, first_name, last_name, state, city, loyalty_segment
ORDER BY revenue DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query 7 — Weekday vs weekend revenue mix
-- MAGIC
-- MAGIC **Widget:** Donut chart, or two-row summary
-- MAGIC **Answers:** "Is the business shaped by week-day patterns? Should we staff weekends differently?"
-- MAGIC
-- MAGIC Quick sanity on whether weekend traffic is a meaningful share. Useful for ops
-- MAGIC conversations even though the persona is Sales Lead.

-- COMMAND ----------

SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END  AS day_type,
    ROUND(SUM(line_revenue), 2)                             AS revenue,
    COUNT(DISTINCT order_number)                            AS orders,
    ROUND(SUM(line_revenue)
        / NULLIF(COUNT(DISTINCT order_number), 0), 2)       AS aov
FROM lakehouse_demo.gold.wide_sales_genie
GROUP BY is_weekend
ORDER BY day_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashboard assembly checklist
-- MAGIC
-- MAGIC 1. **Create dashboard:** workspace sidebar → **Dashboards** → **Create dashboard**.
-- MAGIC    Name it `Commercial Sales Lead`.
-- MAGIC 2. **Attach warehouse:** select the 2X-Small serverless SQL warehouse (Free Edition default).
-- MAGIC 3. **Create datasets:** for each query above, paste into a new dataset and name it clearly:
-- MAGIC    `ds_kpis`, `ds_trend`, `ds_states`, `ds_top_products`, `ds_heatmap`,
-- MAGIC    `ds_top_customers`, `ds_weekday_split`.
-- MAGIC 4. **Add filters:** date range, state (multi-select), loyalty segment (multi-select),
-- MAGIC    applied globally so they propagate to every visualisation.
-- MAGIC 5. **Layout (top → bottom):**
-- MAGIC    - Markdown text tile: 3-sentence Sales Lead narrative (above)
-- MAGIC    - Row of 4 KPI counter tiles bound to `ds_kpis` (latest year_month)
-- MAGIC    - Stacked area / bar chart from `ds_trend` — revenue trend by loyalty segment
-- MAGIC    - Two-column row: state choropleth or bar (`ds_states`) | top products bar (`ds_top_products`)
-- MAGIC    - Heatmap from `ds_heatmap` — category x loyalty
-- MAGIC    - Optional: top-customers table (`ds_top_customers`) and weekday/weekend donut (`ds_weekday_split`)
-- MAGIC 6. **Publish** — click **Publish** and copy the share link for the README.
-- MAGIC 7. **Export the dashboard definition** to `resources/dashboard.lvdash.json` (Edit → ⋯ → Export)
-- MAGIC    so the repo is self-contained and the hiring team can re-import it.

