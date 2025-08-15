-- models/example/dwh_asset_status_history.sql
-- DWH asset status history view model

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
  `trading-prod-468212.trading.raw_trade_history`
WHERE
  timestamp_jst IS NOT NULL