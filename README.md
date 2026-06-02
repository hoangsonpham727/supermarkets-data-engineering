# 🛒 Australian Supermarket Price-Intelligence Pipeline

A production-style **daily price-intelligence** data pipeline built on a **100% free-tier**
stack: raw files land in **Amazon S3**, **Databricks** (Lakeflow Declarative Pipelines /
Delta Live Tables) runs a **Bronze → Silver → Gold** medallion, and **Databricks AI/BI
Dashboards** serve the analytics.

The interesting engineering problem: four retailers (**Aldi, Coles, IGA, Woolworths**)
publish the same concepts in **four different, messy schemas**. We integrate them into one
conformed, comparable, analytics-ready model.

## Why this is a realistic project
| Real-world concern | How it shows up here |
|---|---|
| Heterogeneous sources | 4 feeds, different columns, taxonomies & quirks → one conformed model |
| Messy data | Stringified Python lists, mixed currency units (`3c`, `$2.58`), BOM headers, null prices, cents/dollars data-entry errors |
| Incremental ingestion | **Auto Loader** processes only newly-landed daily files |
| Data quality as a first-class concern | DLT expectations → **drop / quarantine / pass**; bad rows kept for review, not deleted |
| Slowly-changing data | **SCD Type 2** product dimension tracks attribute changes over time |
| Cross-source comparability | Unit prices normalised to **per-kg / per-L / per-each** |
| Governance & lineage | **Unity Catalog** + DLT lineage graph |
| Reproducible infra | **Terraform** (S3 + IAM) and **Databricks Asset Bundles** (pipeline/job/dashboard as code) |
| CI/CD | **GitHub Actions**: lint + unit tests + bundle validate |

## Architecture

```
 Source CSVs (dated)          AWS S3 (raw zone, free tier)            Databricks Lakehouse (Unity Catalog)
┌──────────────────┐  land   ┌───────────────────────────────┐     ┌──────────────────────────────────────────┐
│ aldi/coles/iga/  │ ──────▶ │ raw/<retailer>/date=YYYY-MM-DD │ ──▶ │ BRONZE  raw_<retailer>   (Auto Loader)      │
│ wow  .csv (daily)│         │       /<retailer>_<date>.csv   │     │ SILVER  fact_price + dims (conform + DQ)    │
└──────────────────┘         └───────────────────────────────┘     │ GOLD    fact_price_daily + 5 marts          │
                                                                    └──────────────────────┬─────────────────────┘
                                                                                     AI/BI Dashboard
```

### Data model (Gold star schema)
- **`fact_price_daily`** — grain: *retailer × product × day*; carries `price_aud`,
  normalised unit price, `price_change_pct` (day-over-day), FKs to all dims.
- **Dimensions** — `dim_product` (SCD2), `dim_category` (canonical 2-level taxonomy),
  `dim_retailer`, `dim_date`.
- **Marts** — `mart_price_comparison`, `mart_specials_penetration`,
  `mart_category_price_index`, `mart_availability`, `mart_dq_scorecard`.

## Repository layout
```
src/common/parsers.py      # unit-price/list/brand/category parsing (pure Python, tested)
src/common/dq.py           # data-quality rule predicates (pure Python, tested)
src/ingest/land_to_s3.py   # land dated CSVs into the S3 raw zone
src/simulate/generate_days.py  # synthesize extra days (history for SCD2 / price change)
src/pipelines/bronze.py    # Auto Loader ingestion (DLT)
src/pipelines/silver.py    # conform + DQ + dimensions (DLT)
src/pipelines/gold.py      # star schema + BI marts (DLT)
tests/                     # pytest unit tests for parsers & DQ rules
infra/terraform/           # S3 bucket + IAM role for the UC storage credential
databricks.yml             # Databricks Asset Bundle (pipeline + daily job + dashboard)
dashboards/price_intelligence.lvdash.json  # AI/BI dashboard
.github/workflows/ci.yml   # lint + tests + bundle validate
```

## Setup (free tiers throughout)

### 0. Prerequisites
```bash
pip install -r requirements-dev.txt
databricks --version    # https://docs.databricks.com/dev-tools/cli
terraform -version
```

### 1. AWS S3 raw zone (Terraform)
```bash
cd infra/terraform
terraform init
terraform apply -var bucket_name=<your-unique-bucket> \
                -var unity_catalog_external_id=<from-step-2>
# outputs: raw_bucket_s3_uri, uc_role_arn
```
> The `unity_catalog_external_id` comes from the Databricks **storage credential** you
> create in step 2 — create the credential first (it generates the external ID), then
> apply Terraform, then paste `uc_role_arn` back into the credential.

### 2. Databricks Free Edition + Unity Catalog
1. Sign up for **Databricks Free Edition** (serverless; includes Unity Catalog, DLT,
   Workflows, AI/BI Dashboards).
2. Create catalog `supermarket` with schemas `bronze`, `silver`, `gold`.
3. **Connect S3:** create a UC **storage credential** (IAM role `uc_role_arn`) and an
   **external location** pointing at `s3://<bucket>/raw`.
   - ⚠️ **If Free Edition blocks external S3:** fall back to a UC **managed Volume**
     (`/Volumes/supermarket/bronze/landing/raw`) and upload the CSVs there. Auto Loader
     works against the Volume path identically — just set `source.root` accordingly in
     `databricks.yml`. **Record which path you used.**

### 3. Land the data
```bash
# real snapshot (2021-04-23)
python -m src.ingest.land_to_s3 --bucket <your-bucket> --source-dir data/raw
# (optional) synthesize a week of history, then land it too
python -m src.simulate.generate_days --start 2021-04-24 --days 7
python -m src.ingest.land_to_s3 --bucket <your-bucket> --source-dir data/synth
```

### 4. Deploy & run the pipeline
```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run price_intelligence_job -t dev
```
The daily **Workflow** (06:00 Australia/Sydney) triggers the medallion pipeline with
retries + failure email. Open the pipeline to see the Bronze→Silver→Gold lineage graph
and the DLT data-quality expectation metrics.

### 5. Dashboard
Import `dashboards/price_intelligence.lvdash.json` as an AI/BI Dashboard (adjust the
catalog name in the dataset queries if you used `supermarket_dev`).

## Testing
```bash
pytest          # 35 unit tests over the parsing & DQ rules
ruff check .    # lint
```

## End-to-end verification checklist
1. **Parsers/DQ:** `pytest` green.
2. **Landing:** `aws s3 ls s3://<bucket>/raw/ --recursive` shows date-partitioned keys.
3. **Bronze incremental:** run pipeline; land one more day; re-run → only the new files
   are ingested (row counts rise by the new files only).
4. **Silver DQ:** WOW `Unavailable` rows are **quarantined, not dropped**; the Aldi
   price/unit anomaly is flagged in `fact_price_quarantine`; expectation metrics visible
   in the pipeline UI.
5. **Gold:** `price_change_pct` matches the day-over-day drift; `mart_price_comparison`
   marks the cheapest retailer per category by normalised unit price.
6. **Dashboard:** all five tiles render; the price-index line trends across the synth days.

## Known limitations / future work
- **Product matching across retailers** is by *canonical category × normalised unit
  price*, not entity resolution — fuzzy product matching is a documented enhancement.
- **Brand extraction** for Aldi/IGA is a heuristic; a curated brand dictionary would
  improve it.
- The **synthetic days** are clearly labelled demo data; a real deployment lands genuine
  daily extracts.

---
*Data: four supermarket price snapshots scraped 2021-04-23 (Aldi 565, Coles 25,926,
IGA 1,788, Woolworths 35,973 rows).*
