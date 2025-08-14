-- models/example/dwh_asset_status_history.sql
-- DWH asset status history view model

{{ config(materialized='view') }}

SELECT
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S', event_time_jst_str) AS event_time_jst,
  ticker,
  is_monitor,
  is_watch
FROM
  `trading-prod-468212.trading.raw_asset_status_history`
WHERE
  event_time_jst_str IS NOT NULL
