"""
UK Property Data Pipeline
==========================
Medallion flow: bronze (raw ingestion) -> silver/gold (dbt) -> BI (Metabase reads gold directly).
Schedule: monthly, aligned with Land Registry's monthly data release.

Sources:
- HM Land Registry Price Paid Data (land_registry.py)
- postcodes.io bulk lookup (postcodes.py)
- ONS Postcode Directory / NSPL (ons_postcode.py) -- reconciled with
  postcodes.io in dbt (stg_postcode_master.sql)
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/ingestion")

from land_registry import run as run_land_registry  # noqa: E402
from ons_postcode import run as run_ons_postcode  # noqa: E402
from postcodes import run as run_postcodes  # noqa: E402

DBT_DIR = "/opt/airflow/dbt"

default_args = {"owner": "data-eng", "retries": 2}

with DAG(
    dag_id="uk_property_pipeline",
    description="Ingest UK property data, transform with dbt, serve via Metabase",
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["property", "portfolio"],
) as dag:

    # --- Bronze: raw ingestion ---
    ingest_land_registry = PythonOperator(
        task_id="ingest_land_registry",
        python_callable=run_land_registry,
    )

    # Needs distinct postcodes from land_registry_pp, so runs after it.
    enrich_postcodes = PythonOperator(
        task_id="enrich_postcodes",
        python_callable=run_postcodes,
    )

    # Independent full-directory download; can run in parallel with land registry.
    ingest_ons_postcode = PythonOperator(
        task_id="ingest_ons_postcode",
        python_callable=run_ons_postcode,
    )

    # --- Silver + Gold: dbt transformations ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    # --- Data quality gate: pipeline stops here if tests fail, so a broken
    # gold layer never reaches Metabase ---
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    ingest_land_registry >> enrich_postcodes
    [enrich_postcodes, ingest_ons_postcode] >> dbt_run >> dbt_test
