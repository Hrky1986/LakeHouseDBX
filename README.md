# Senior Data Engineer — Take-Home Task

**Submission by:** Krešimir Hrkač
**Platform:** Databricks Free Edition (serverless, Unity Catalog)
**Dataset:** `/databricks-datasets/retail-org` (US retail sales, customers, products)
**Persona:** Commercial / Sales Lead

---

## What this is

An end-to-end medallion-architecture lakehouse on Databricks Free Edition. Raw retail data lands in Bronze, is cleaned and quarantined in Silver, and served via a star schema and a Genie-friendly wide table in Gold — consumed by an AI/BI dashboard for a Commercial Sales Lead persona and a Genie Space for natural-language analytics.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the diagram and rationale.

```
/databricks-datasets/retail-org
    │
    ▼
lakehouse_demo.bronze    (raw + audit metadata)
    │
    ▼
lakehouse_demo.silver    (cleaned, exploded, DQ + quarantine)
    │
    ▼
lakehouse_demo.gold      (star schema + aggregates + wide-for-Genie)
    │
    ├──▶  AI/BI Dashboard  (Commercial Sales Lead)
    ├──▶  Genie Space      (natural-language analytics)
    └──▶  ai_query()       (scheduled LLM summary)
```

## Repo layout

```
lakehouse-task/
├── README.md                          ← you are here
├── docs/
│   ├── architecture.md                ← diagram + rationale (submit-first deliverable)
│   └── writeup.md                     ← final 1–2 page write-up
├── notebooks/
│   ├── 00_setup.py                    ← catalog/schema + source-data availability check
│   ├── 01_bronze_ingest.py            ← files → bronze Delta with audit metadata
│   ├── 02_silver_transform.py         ← explode, dedup, DQ + quarantine
│   ├── 03_gold_build.py               ← star schema + aggregates + Genie wide table
│   ├── 04_dashboard_queries.sql       ← SQL behind the 4 dashboard tiles
│   └── 05_ai_layer.py                 ← Genie setup checklist + ai_query examples
└── resources/
    └── dashboard.lvdash.json          ← exported dashboard definition (optional)
```

## How to run

### Prerequisites

- A Databricks Free Edition account — sign up at <https://www.databricks.com/learn/free-edition>
- Ability to connect a Git folder (public repo or granted access)

### Setup

1. **Clone the repo into the Databricks workspace**
   Workspace → **Git folders** → **Add Git folder** → paste the repo URL → Create
2. **Open `notebooks/00_setup.py`** and run all cells
   Creates `lakehouse_demo.{bronze, silver, gold}` and verifies source paths
3. **Run the pipeline notebooks in order:**
   `01_bronze_ingest` → `02_silver_transform` → `03_gold_build`
   Each notebook is idempotent and safe to re-run

### Orchestrated execution (recommended)

**Workflow Job** with three serial tasks mapped to the three notebooks. Runs end-to-end on a fresh serverless compute in ~2–4 minutes on sample volumes.

### Dashboard + AI layer

- **Dashboard:** `04_dashboard_queries.sql`
- **Genie Space:** `05_ai_layer.py`
- **`ai_query` summary cell:** runs inline in Part B of `05_ai_layer.py`

## Access for the hiring team

- The workspace is Databricks Free Edition (serverless, quota-limited) — hiring team users can be added at the account-console level as view-only members
- A read-only shared link to the dashboard and Genie Space is included in the submission email
- All code in this repo is self-contained and reproducible on any fresh Free Edition workspace

## Live demo

Prepared for a walkthrough in the Databricks workspace:

1. Catalog Explorer tour — bronze/silver/gold + column comments
2. Run the Job live (pre-warmed warehouse)
3. Dashboard interaction with filters
4. Two Genie queries
5. Backup: `ai_query` summary cell

Estimated runtime end-to-end: ~5 minutes.

## Key design choices

- **Flag-and-quarantine data quality** over fail-the-job — partial data beats a broken dashboard
- **Star schema + denormalized wide table** — correctness and flexibility in parallel with Genie-friendliness
- **Column comments as first-class output** — they are what makes Genie usable
- **Pre-aggregated `agg_sales_monthly`** — dashboard renders sub-second on a 2X-Small warehouse
- **SCD Type 2 on dimensions** — SCD2 is for CDC
