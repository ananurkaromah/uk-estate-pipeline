"""Shared Postgres connection helper for ingestion scripts.

Reads connection details from environment variables so the same code
works both inside the Airflow container (via AIRFLOW_CONN_ESTATE_POSTGRES /
docker-compose env) and locally against a forwarded port.
"""
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    host = os.environ.get("DW_HOST", "localhost")
    port = os.environ.get("DW_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "estate")
    password = os.environ.get("POSTGRES_PASSWORD", "estate")
    db = os.environ.get("POSTGRES_DB", "estate")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load_dataframe(df, table_name: str, schema: str = "bronze", if_exists: str = "append") -> None:
    """Load a pandas DataFrame into the given medallion schema/table."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    print(f"Loaded {len(df)} rows into {schema}.{table_name}")
