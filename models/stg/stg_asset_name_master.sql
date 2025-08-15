{{ config(materialized='view') }}

SELECT
  ticker,
  asset_type,
  asset_name
FROM
  `trading-prod-468212.trading.raw_asset_name_master`
WHERE
  ticker IS NOT NULL
