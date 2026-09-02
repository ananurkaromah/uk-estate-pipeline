"""Ingest UK HM Land Registry Price Paid Data (monthly CSV, no auth) into bronze."""
import logging
from io import StringIO

import pandas as pd
import requests

from db import load_dataframe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    "/pp-monthly-update-new-version.csv"
)

COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status",
]


def run():
    logger.info("Downloading Land Registry Price Paid Data from %s", SOURCE_URL)
    resp = requests.get(SOURCE_URL, timeout=120)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), header=None, names=COLUMNS)

    before = len(df)
    df = df.dropna(subset=["postcode", "price"])
    logger.info("Dropped %d rows missing postcode/price", before - len(df))

    load_dataframe(df, table_name="land_registry_pp", schema="bronze")


if __name__ == "__main__":
    run()
