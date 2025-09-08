{{ config(materialized='view') }}

SELECT
  add_date,
  ticker,
  entry_price,
  memo
FROM
  {{ source('trading-' ~ target.name, 'raw_entry_point') }}
WHERE
  add_date IS NOT NULL
