{{ config(materialized='view') }}

SELECT
  PARSE_DATETIME('%Y/%m/%d %H:%M:%S', timestamp_jst) AS event_time_jst,
  trade_date,
  ticker,
  trade_type,
  account,
  order_count,
  price,
  memo
FROM
  {{ source('trading-' ~ target.name, 'raw_trade_history') }}
WHERE
  timestamp_jst IS NOT NULL