# UK Property Market Intelligence Dashboard
Built on HM Land Registry Price Paid Data, geographically enriched with ONS and postcodes.io reference data, transformed with dbt (bronze → silver → gold) and orchestrated by Airflow. All figures come from gold.fct_property_prices. Price views use standard (Category A) transactions only, with likely data-entry outliers (over £10M) excluded.
Three business insight built against `gold.fct_property_prices`:
- Overview & Data Quality
- Market Trends & Hotspots 
- Market Segmentation

## Configuration

### 1.	Overview and Data Quality 
![Overview and Data Quality](doc/Metabase-Overview-and-Data-Quality.png)

**1.Total Transactions**
Number of transactions currently loaded in the gold layer.

``` 
SELECT COUNT(*) AS total_transactions FROM goldfct_property_prices;
```

**2. Latest Transaction Date**
Most recent transfer date in the loaded data (data freshness indicator).

```
SELECT MAX(transaction_date) AS latest_transaction_date FROM gold.fct_property_prices; 
```

**3. Median Price (Category A, non-outlier)**
Median sale price for standard (Category A), non-outlier transactions.

```
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
FROM gold.fct_property_prices
WHERE ppd_category_type = 'A' AND is_price_outlier = FALSE;
```

**4. Geographic Match Rate**
Share of transactions whose postcode resolved to a region via ONS NSPL (postcodes.io as fallback).

```
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE region IS NOT NULL) / COUNT(*), 1) AS geo_match_rate_pct
FROM gold.fct_property_prices;
```

**5. Price Outliers Flagged**
Transactions priced above £10M, flagged as likely data-entry errors or non-market bulk entries. Kept in the data for traceability, excluded from price charts.

```
SELECT COUNT(*) FILTER (WHERE is_price_outlier) AS flagged_outliers
FROM gold.fct_property_prices;
```

## 2.	Market Trends & Hotspots
**2.1.	Monthly Average Property Price Trends by Region**
Monthly average price by region for standard (Category A), non-outlier transactions. Only region-months with at least 30 transactions are shown, so the line may start later than the underlying data

![Monthly-Average-Property-Price-Trends-by-Region](doc/Metabase-Monthly-Average-Property-Price-Trends-by-Region.png)

```
WITH monthly AS (
    SELECT
        transaction_month,
        region,
        AVG(price) AS average_price,
        COUNT(*) AS txn_count
    FROM gold.fct_property_prices
    WHERE ppd_category_type = 'A'
      AND is_price_outlier = FALSE
      AND region IS NOT NULL
    GROUP BY transaction_month, region
)
SELECT transaction_month, region, average_price, txn_count
FROM monthly
WHERE txn_count >= 30  -- exclude thin months/regions prone to single-sale noise
ORDER BY transaction_month, region
```
**Visualization**: Line chart (x: transaction_month, series: region, y: average_price)

**2.2.	Year-over-Year Property Price Growth by Region**
Annual percentage change in average price by region (Category A, non-outlier). A YoY value is shown only when both the year and the immediately preceding year have at least 30 transactions; otherwise it is left blank rather than compared across a gap.

![Year-over-Year Property-Price-Growth-by-Region](<doc/Metabase-Year-over-Year Property-Price-Growth-by-Region.png>)

```
WITH yearly AS (
    SELECT
        region,
        DATE_TRUNC('year', transaction_month) AS yr,
        AVG(price) AS avg_price,
        COUNT(*) AS txn_count
    FROM gold.fct_property_prices
    WHERE ppd_category_type = 'A'
      AND is_price_outlier = FALSE
      AND region IS NOT NULL
    GROUP BY region, DATE_TRUNC('year', transaction_month)
    HAVING COUNT(*) >= 30
),
with_prev AS (
    SELECT
        region, yr, avg_price, txn_count,
        LAG(yr) OVER (PARTITION BY region ORDER BY yr) AS prev_yr,
        LAG(avg_price) OVER (PARTITION BY region ORDER BY yr) AS prev_avg_price
    FROM yearly
)
SELECT
    region, yr, avg_price, txn_count,
    CASE
        WHEN prev_yr = yr - INTERVAL '1 year'
        THEN ROUND(((avg_price - prev_avg_price) / prev_avg_price) * 100, 1)
    END AS yoy_pct_change
FROM with_prev
ORDER BY region, yr
```

**Visualization:** Bar chart (x: yr, series: region, y: yoy_pct_change)


**2.3.	Top 10 Districts by Annual Price Growth (CAGR)**
Postcode districts ranked by compound annual growth rate (CAGR) in average price between their first and last year with at least 5 standard (Category A, non-outlier) transactions, requiring at least two years between them so growth is comparable across districts. This reflects historical appreciation, not a forecast.

![Top-10-Districts-by-Annual-Price-Growth(CAGR)](doc/Metabase-Top-10-Districts-by-Annual-Price-Growth(CAGR).png)

```
WITH district_year AS (
    SELECT
        split_part(postcode, ' ', 1) AS postcode_district,
        region,
        EXTRACT(YEAR FROM transaction_month)::int AS yr,
        AVG(price) AS avg_price,
        COUNT(*) AS txn_count
    FROM gold.fct_property_prices
    WHERE ppd_category_type = 'A'
      AND is_price_outlier = FALSE
      AND region IS NOT NULL
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 5  -- minimum sales per district-year to be meaningful
),
bounds AS (
    SELECT postcode_district, region, MIN(yr) AS first_yr, MAX(yr) AS last_yr
    FROM district_year
    GROUP BY postcode_district, region
    HAVING MAX(yr) - MIN(yr) >= 2  -- at least 2 years apart so growth can be annualized
)
SELECT
    b.postcode_district,
    b.region,
    b.first_yr,
    b.last_yr,
    d1.avg_price AS first_year_avg,
    d2.avg_price AS last_year_avg,
    ROUND((POWER(d2.avg_price / d1.avg_price, 1.0 / (b.last_yr - b.first_yr)) - 1) * 100, 1) AS cagr_pct,
    d1.txn_count AS first_year_txn_count,
    d2.txn_count AS last_year_txn_count
FROM bounds b
JOIN district_year d1
    ON d1.postcode_district = b.postcode_district AND d1.region = b.region AND d1.yr = b.first_yr
JOIN district_year d2
    ON d2.postcode_district = b.postcode_district AND d2.region = b.region AND d2.yr = b.last_yr
ORDER BY cagr_pct DESC
LIMIT 10
```
**Visualization:** Bar chart dengan x = postcode_district dan y = cagr_pct.



**Note on Data Maturity** — Trend and growth views apply minimum transaction-count thresholds to avoid single-sale noise. History is limited to what the pipeline has loaded so far, so these views become more reliable as monthly runs accumulate or after a historical backfill.

## 3.	Market Segmentation 
** 3.1 Average Price by Property Type per Region**
Median and average transaction price by property type within each region (Category A, non-outlier), shown only for groups with at least 10 transactions (N ≥ 10). 

![Average-Price-by-Property-Type-per-Region](doc/Metabase-Average-Price-by-Property-Type-per-Region.png)

```
SELECT
    region,
    CASE property_type
        WHEN 'D' THEN 'Detached'
        WHEN 'S' THEN 'Semi-detached'
        WHEN 'T' THEN 'Terraced'
        WHEN 'F' THEN 'Flat/Maisonette'
        ELSE 'Other'
    END AS property_type_label,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
    AVG(price) AS avg_price,
    COUNT(*) AS txn_count
FROM gold.fct_property_prices
WHERE ppd_category_type = 'A'
  AND is_price_outlier = FALSE
  AND region IS NOT NULL
GROUP BY region, 2
HAVING COUNT(*) >= 10
ORDER BY region, 2
```
**Visualization:** Bar chart (x: region, series: property_type, y: avg_price)


**3.2	Transaction Volume by Region (last 12 months)**
Number of standard (Category A, non-outlier) transactions per region over the most recent 12 months of data. This measures market activity, not liquidity: counts scale with region size and are not adjusted for housing stock.

![Transaction-Volume-by-Region-(last 12 months)](<doc/Metabase-Transaction-Volume-by-Region-(last 12 months).png>)

```
SELECT
    region,
    COUNT(*) AS txn_count
FROM gold.fct_property_prices
WHERE ppd_category_type = 'A'
  AND is_price_outlier = FALSE
  AND region IS NOT NULL
  AND transaction_date >= (SELECT MAX(transaction_date) FROM gold.fct_property_prices) - INTERVAL '12 months'
GROUP BY region
ORDER BY txn_count DESC
```
**Visualization:** Bar chart (x: region, y: txn_count) 


**3.3	Property Type Mix per Region (%)**
Share of standard transactions by property type within each region, showing structural differences such as flat-heavy versus detached-heavy markets.

![Property-Type-Mix-per-Region(%)](doc/Metabase-Property-Type-Mix-per-Region(%).png)

```
WITH counts AS (
    SELECT
        region,
        CASE property_type
            WHEN 'D' THEN 'Detached'
            WHEN 'S' THEN 'Semi-detached'
            WHEN 'T' THEN 'Terraced'
            WHEN 'F' THEN 'Flat/Maisonette'
            ELSE 'Other'
        END AS property_type_label,
        COUNT(*) AS txn_count
    FROM gold.fct_property_prices
    WHERE ppd_category_type = 'A'
      AND is_price_outlier = FALSE
      AND region IS NOT NULL
    GROUP BY region, 2
)
SELECT
    region,
    property_type_label,
    txn_count,
    ROUND(100.0 * txn_count / SUM(txn_count) OVER (PARTITION BY region), 1) AS pct_of_region
FROM counts
ORDER BY region, pct_of_region DESC
```
**Visualization:** Stacked bar chart (x: region, y: pct_of_region, series: property_type)

**Note on Data Maturity** — Trend and growth views apply minimum transaction-count thresholds to avoid single-sale noise. History is limited to what the pipeline has loaded so far, so these views become more reliable as monthly runs accumulate or after a historical backfill.