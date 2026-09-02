"""Enrich distinct postcodes from bronze.land_registry_pp using postcodes.io
(free bulk lookup, no key, rate-limited) into bronze.postcode_io."""
import logging

import pandas as pd
import requests

from db import get_engine, load_dataframe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BULK_LOOKUP_URL = "https://api.postcodes.io/postcodes"
BATCH_SIZE = 100  # postcodes.io bulk lookup limit per request


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def run():
    engine = get_engine()
    postcodes = pd.read_sql(
        "SELECT DISTINCT postcode FROM bronze.land_registry_pp WHERE postcode IS NOT NULL",
        engine,
    )["postcode"].tolist()

    logger.info("Looking up %d distinct postcodes", len(postcodes))

    rows = []
    for batch in _chunks(postcodes, BATCH_SIZE):
        resp = requests.post(BULK_LOOKUP_URL, json={"postcodes": batch}, timeout=30)
        resp.raise_for_status()
        for item in resp.json().get("result", []):
            result = item.get("result")
            if result:
                rows.append({
                    "postcode": result["postcode"],
                    "quality": result.get("quality"),
                    "eastings": result.get("eastings"),
                    "northings": result.get("northings"),
                    "country": result.get("country"),
                    "nhs_ha": result.get("nhs_ha"),
                    "longitude": result.get("longitude"),
                    "latitude": result.get("latitude"),
                    "european_electoral_region": result.get("european_electoral_region"),
                    "primary_care_trust": result.get("primary_care_trust"),
                    "region": result.get("region"),
                    "lsoa": result.get("lsoa"),
                    "msoa": result.get("msoa"),
                    "incode": result.get("codes", {}).get("incode") if result.get("incode") is None else result.get("incode"),
                    "outcode": result.get("outcode"),
                    "parliamentary_constituency": result.get("parliamentary_constituency"),
                    "admin_district": result.get("admin_district"),
                    "parish": result.get("parish"),
                    "admin_county": result.get("admin_county"),
                    "admin_ward": result.get("admin_ward"),
                    "ced": result.get("ced"),
                    "ccg": result.get("ccg"),
                    "nuts": result.get("nuts"),
                })

    df = pd.DataFrame(rows)
    logger.info("Resolved %d/%d postcodes", len(df), len(postcodes))

    load_dataframe(df, table_name="postcode_io", schema="bronze")


if __name__ == "__main__":
    run()
