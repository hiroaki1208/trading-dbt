{{ config(materialized='view') }}

SELECT
  ticker,
  asset_type,
  asset_name
FROM
  {{ source('trading-' ~ target.name, 'raw_asset_name_master') }}
WHERE
  ticker IS NOT NULL
