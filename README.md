# UK Property Data Pipeline (Estate)

Portfolio data engineering project: zero-cost, on-premise pipeline for UK
property price data, medallion architecture (bronze -> silver -> gold),
served through self-hosted Metabase.

## Stack

| Layer          | Tool                                    |
|----------------|------------------------------------------|
| Orchestration  | Apache Airflow 2.9.1 (Docker, LocalExecutor) |
| Storage        | PostgreSQL 16 (bronze/silver/gold schemas) |
| Transformation | dbt-core (dbt-postgres)                 |
| Data quality   | dbt tests + dbt_utils                   |
| BI / Serving   | Metabase (self-hosted)                  |

Everything runs locally via Docker Compose (e.g. on WSL Ubuntu) — no cloud account required.

## Setup

```bash
cp .env.example .env
# fill in AIRFLOW__CORE__FERNET_KEY (generate with:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
# also set NSPL_SOURCE_URL for the ons_postcode ingestion task (see below)
docker compose up -d --build
```

- Airflow UI: http://localhost:8080 (admin / admin)
- Metabase: http://localhost:3000 — connect it to the `postgres` service, db from `POSTGRES_DB`, reading only from the `gold` schema
- Trigger the `uk_property_pipeline` DAG to run the full flow

## Data sources

- **HM Land Registry Price Paid Data** — monthly bulk CSV, no auth (`ingestion/land_registry.py`)
- **postcodes.io** — free bulk postcode -> lat/long/region lookup, no key (`ingestion/postcodes.py`)
- **ONS Postcode Directory (NSPL)** — authoritative postcode -> region/LSOA/MSOA lookup, published by ONS Geography as a bulk CSV (`ingestion/ons_postcode.py`). The exact download link changes between releases — set `NSPL_SOURCE_URL` in `.env` to the current one from the [Open Geography Portal](https://geoportal.statistics.gov.uk).

Two overlapping postcode sources are deliberate: `stg_postcode_master.sql`
reconciles them, preferring ONS as authoritative and falling back to
postcodes.io for any postcode ONS doesn't resolve — a more realistic
data engineering pattern than relying on a single source.

## Medallion layers

- **bronze** — raw ingested data, untouched, in Postgres tables created by `docker/init.sql`
- **silver** (dbt `models/staging/`) — cleaned, typed, deduplicated; includes `stg_postcode_master`, the reconciled postcode lookup
- **gold** (dbt `models/marts/`) — `fct_property_prices`, `dim_region`, analytics-ready. Metabase only reads from here.

## Data quality & logging

- dbt tests (`unique`, `not_null`, `accepted_values`) run as the `dbt_test` DAG task right after `dbt_run`; a failure stops the DAG before Metabase sees bad data.
- Airflow UI has per-task logs out of the box; ingestion scripts use Python's `logging` module for row counts and drops.

## Repo structure

```
uk-property-pipeline/
├── docker-compose.yml
├── Dockerfile.airflow
├── docker/init.sql
├── .env.example
├── airflow/dags/property_pipeline_dag.py
├── ingestion/
│   ├── db.py
│   ├── land_registry.py
│   ├── postcodes.py
│   └── ons_postcode.py
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── staging/  (silver)
        │   ├── sources.yml
        │   ├── schema.yml
        │   ├── stg_land_registry.sql
        │   ├── stg_postcode_io.sql
        │   ├── stg_ons_postcode.sql
        │   └── stg_postcode_master.sql
        └── marts/    (gold)
            ├── schema.yml
            ├── dim_region.sql
            └── fct_property_prices.sql
```
