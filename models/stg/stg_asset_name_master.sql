{{ config(materialized='view') }}

SELECT
  ticker,
  asset_type,
  asset_name
FROM
  {{ source('trading-prod', 'raw_asset_name_master') }}
WHERE
  ticker IS NOT NULL
