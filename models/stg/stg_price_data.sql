{{ config(materialized='view') }}

SELECT DISTINCT
  date as base_date,
  ticker,
  ohlc_type,
  price,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S', fetch_time_str) AS fetch_time_jst
FROM
  {{ source('trading-' ~ target.name, 'raw_price_data') }}
WHERE
  date IS NOT NULL
