# CLAUDE.md — ClickEngine

This file provides context for Claude when working inside the ClickEngine repository.

---

## Project Overview

**ClickEngine** is a production-grade PySpark/Databricks ETL processing library, part of the larger **Click Data Analyzer (CDA)** platform. It consumes streaming click and user events from Azure Event Hubs and processes them through a medallion architecture (landing → bronze → silver → gold) on ADLS Gen2, using Databricks and Unity Catalog.

This repo is one of three:

| Repo | Purpose |
|------|---------|
| `cda_infra` | Terraform infrastructure (Azure + Databricks, Unity Catalog) |
| `click_data_func` | Azure Function App — event ingestion endpoints |
| `ClickEngine` | **This repo** — PySpark/Databricks ETL library |

---

## Architecture

- **Ingestion**: Azure Function App sends user and click events to Azure Event Hubs (two separate hubs: `evh-users`, `evh-clicks`)
- **Streaming**: Spark Structured Streaming reads from Event Hubs via the `azure-eventhubs-spark` connector, authenticated via the Databricks Service Principal (AAD / Managed Identity)
- **Storage**: ADLS Gen2 with containers: `landing`, `bronze`, `silver`, `gold`
- **Catalog**: Unity Catalog (single metastore per region/tenant, shared across workspaces)
- **Deployment**: Databricks Asset Bundles (`databricks.yml`) targeting `dev`, `uat`, `prd` workspaces

### Medallion Layers

| Layer | Description |
|-------|-------------|
| Bronze | Raw parsed events from Event Hubs, written as Delta tables |
| Silver | Deduplicated, validated, enriched data; user/click joins |
| Gold | Aggregations for analytics (DAU, clicks per page, user summaries) |

---

## Repository Structure

```
ClickEngine/
├── src/
│   └── clickengine/
│       ├── __init__.py
│       ├── common/
│       │   ├── spark_session.py      # SparkSession builder (test vs prod)
│       │   ├── config.py             # Environment config loader, Event Hub config
│       ├── transformations/
│       │   ├── users.py       # Entry point: instantiates UsersPipeline, calls run()
│       │   └── clicks.py      # Entry point: instantiates ClicksPipeline, calls run()
├── test/
│   ├── conftest.py                   # Shared SparkSession fixture (local[*])
│   ├── test_users_pipeline.py
│   └── test_clicks_pipeline.py
├── databricks.yml                    # Databricks Asset Bundle config
├── pyproject.toml
├── .python-version
└── ruff.toml
```

---

## Key Design Principles
 
### Pure Functional design
ClickEngine uses a **pure functional design** — no classes, no shared state, no side effects except at the explicit I/O boundary.
 
Every function follows one rule: **take data in, return data out**. SparkSession and config are passed explicitly as arguments — never imported as globals or stored on objects.
 
```
readers/       → DataFrame source functions    — I/O boundary (read)
transformations/ → pure DataFrame → DataFrame  — no I/O, no side effects
writers/       → sink functions                — I/O boundary (write)
jobs/          → thin composers                — wires the three layers together
```
 
### Transformation functions are pure
All functions in `transformations/` take a `DataFrame` and return a `DataFrame`. They have no access to SparkSession, no file I/O, no config, no side effects. The same input always produces the same output. This makes them trivially unit-testable with a local SparkSession and a hand-crafted DataFrame.
 
```python
# transformations/users.py
def validate_users(df: DataFrame) -> DataFrame:
    return df.filter(col("user_id").isNotNull())
 
def enrich_users(df: DataFrame) -> DataFrame:
    return df.withColumn("_processed_at", current_timestamp())
 
def transform_users(df: DataFrame) -> DataFrame:
    return df.transform(validate_users).transform(enrich_users)
```
---

## Data Schemas

### User Event (from `evh-users`)
```json
{
  "event_type": "user_registration",
  "user_id": "uuid",
  "name": "string",
  "email": "string",
  "age": 25,
  "country": "US",
  "registration_date": "2025-01-15",
  "timestamp": "2025-01-15T10:30:00Z"
}
```

### Click Event (from `evh-clicks`)
```json
{
  "event_type": "click",
  "click_id": "uuid",
  "user_id": "uuid",
  "page": "/products/shoes",
  "element": "buy_button",
  "device": "mobile",
  "browser": "chrome",
  "session_id": "uuid",
  "timestamp": "2025-01-15T10:32:15Z"
}
```

---

## Development Environment

- **OS**: WSL (Ubuntu) on Windows
- **Python**: managed via `uv` — see `.python-version`
- **Package manager**: `uv` (`uv sync --extra dev` to install all dev dependencies)
- **Linter/formatter**: `ruff` (`ruff check src/`, `ruff format src/`)
- **Test runner**: `pytest` with `chispa` for DataFrame assertions
- **Databricks CLI**: used for bundle validation and deployment

### Install dev dependencies
```bash
uv sync --extra dev
```

### Run tests
```bash
pytest test/ -v
```

### Lint
```bash
ruff check src/
ruff format src/
```

---

## Deployment

Deployment uses **Databricks Asset Bundles**. The `databricks.yml` defines three targets: `dev`, `uat`, `prd`.

### Build
```bash
uv build
```

### Validate bundle
```bash
databricks bundle validate -t dev
```

### Deploy
```bash
databricks bundle deploy -t dev
```

### Run job
```bash
databricks bundle run clickengine_streaming_job -t dev
```

> **Note**: `prd` target uses a service principal (`run_as`) rather than a user identity. Always validate before deploying to `uat`/`prd`.

---

## Testing Approach

- Unit tests use a local `SparkSession` (`local[*]`) defined in `test/conftest.py`
- Use `chispa`'s `assert_df_equality` for DataFrame comparisons
- Test `transform()` on pipeline classes directly — instantiate with a local SparkSession and a test config
- PySpark is a **dev dependency only** — Databricks provides it at runtime; do not add it to `[project.dependencies]`
- Target: all pipeline `transform()` methods have test coverage before any job is deployed

---

## Infrastructure Dependencies (managed in `cda_infra`)

ClickEngine depends on the following Azure resources being provisioned:

| Resource | Purpose |
|----------|---------|
| Event Hub `evh-users` | Source stream for user events |
| Event Hub `evh-clicks` | Source stream for click events |
| ADLS Gen2 storage account | Bronze/silver/gold Delta tables + checkpoints |
| Databricks Service Principal | Job identity; needs `Azure Event Hubs Data Receiver` + `Storage Blob Data Contributor` |
| Unity Catalog metastore | Table registration and governance |
| Databricks Access Connector | Managed identity for Unity Catalog external locations |

---

## Related Repositories

- [`cda_infra`](https://github.com/Andr3w1000/cda_infra) — Terraform for all Azure + Databricks infrastructure
- `click_data_func` — Azure Function App that generates and sends events to Event Hubs