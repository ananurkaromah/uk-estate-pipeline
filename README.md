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
docker compose up -d
```

- Airflow UI: http://localhost:8080 (admin / admin)
- Metabase: http://localhost:3000 — connect it to the `postgres` service, db from `POSTGRES_DB`, reading only from the `gold` schema
- Trigger the `uk_property_pipeline` DAG to run the full flow

## Data sources

- **HM Land Registry Price Paid Data** — monthly bulk CSV, no auth (`ingestion/land_registry.py`)
- **postcodes.io** — free bulk postcode -> lat/long/region lookup, no key (`ingestion/postcodes.py`)

`docker/init.sql` also defines a `bronze.ons_postcode` table for the ONS postcode directory if you want to add that source later — no ingestion script for it yet.

## Medallion layers

- **bronze** — raw ingested data, untouched, in Postgres tables created by `docker/init.sql`
- **silver** (dbt `models/staging/`) — cleaned, typed, deduplicated
- **gold** (dbt `models/marts/`) — `fct_property_prices`, `dim_region`, analytics-ready. Metabase only reads from here.

## Data quality & logging

- dbt tests (`unique`, `not_null`) run as the `dbt_test` DAG task right after `dbt_run`; a failure stops the DAG before Metabase sees bad data.
- Airflow UI has per-task logs out of the box; ingestion scripts use Python's `logging` module for row counts and drops.

## Repo structure

```
uk-property-pipeline/
├── docker-compose.yml
├── docker/init.sql
├── .env.example
├── airflow/dags/property_pipeline_dag.py
├── ingestion/
│   ├── db.py
│   ├── land_registry.py
│   └── postcodes.py
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── staging/  (silver)
        └── marts/    (gold)
```
