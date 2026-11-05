# Snapshot: 2026-10-08 (pre-backfill)

Frozen record. Do not edit except to fix typos; corrections go in the Notes section.

- Data through: 2026-08-28
- Warehouse state: first working version of the pipeline, **before** the 2021-2025 historical backfill

## Key figures

| Metric | Value |
|---|---|
| Total transactions | 89,133 |
| Latest transaction date | 2026-08-28 |
| Median sale price | £300,000 |
| Geographic match rate | 99.9% |
| Price outliers flagged (> £10M) | 136 |

## Screenshots

### Overview and data quality
![Overview and data quality](01-overview-data-quality.png)

### Monthly price trend
![Monthly price trend](02-monthly-price-trend.png)

### Top 10 districts by CAGR
![Top 10 districts by CAGR](04-top-10-districts-cagr.png)

### Median price by property type
![Median price by property type](05-median-price-by-property-type.png)

### Transaction volume
![Transaction volume](06-transaction-volume.png)

### Property type mix
![Property type mix](07-property-type-mix.png)

### Year-over-year price growth (known issue)
![Year-over-year price growth](03-yoy-price-growth.png)

This chart was not working correctly at the time of the snapshot: the year-over-year comparison was unreliable with the limited data loaded then. It was rebuilt after the historical backfill and is kept here only as a record of that state.

## Notes

- Taken before the 2021-2025 historical backfill, so coverage is limited to the data loaded at that time. Figures are **not comparable** to later snapshots: the lower transaction count and the higher median reflect coverage, not the market.
- Taken before the stale-row fix (v1.1.1), when Gold did not yet remove cancelled transactions automatically.