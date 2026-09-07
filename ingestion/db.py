"""Shared Postgres connection helper for ingestion scripts.
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

def ensure_schema(engine: Engine, schema: str) -> None:
    """Create the medallion schema (bronze/silver/gold) if it doesn't exist yet."""
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

def load_dataframe(df, table_name: str, schema: str = "bronze", if_exists: str = "append") -> None:
    """Load a pandas DataFrame into the given medallion schema/table.

    Used for smaller/already-in-memory loads (e.g. postcodes.py's API
    results). For large CSV files, load chunk-by-chunk instead -- see
    land_registry.py / ons_postcode.py.
    """
    engine = get_engine()
    ensure_schema(engine, schema)
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    print(f"Loaded {len(df)} rows into {schema}.{table_name}")
