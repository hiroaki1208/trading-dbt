-- entry pointのチェック
{{ config(
    materialized='incremental',
    unique_key=['ticker', 'base_date'],
    partition_by={
        "field": "base_date",
        "data_type": "date"
    }

) }}

{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

WITH

-- entry pointリスト取得
entry_point_data AS (
  SELECT
    t0.*,
    t1.asset_name,
    t1.is_active_weekend,
    if(t1.is_active_weekend, DATE('{{ date_1day_ago }}'), {{ to_prev_weekday("DATE('" + date_1day_ago + "')") }}) AS price_date_for_close
  FROM
    {{ ref('stg_entry_point') }} t0
    LEFT JOIN {{ ref('ref_ticker_info') }} t1 USING (ticker)
)

-- close価格取得
, close_price_data AS (
  SELECT *
  FROM {{ ref('dwh_close_price_history') }}
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 7 DAY) AND DATE('{{ date_1day_ago }}')
)

SELECT
  t0.entry_point >= t1.price AS is_reached_entry_price,
  t0.ticker,
  t0.asset_name,
  t0.add_date,
  t0.entry_point,
  t0.memo,
  t0.price_date_for_close AS recent_price_date,
  t1.price AS recent_price,
  DATE('{{ date_1day_ago }}') AS base_date
FROM
  entry_point_data t0
  LEFT JOIN close_price_data t1 ON t0.ticker = t1.ticker AND t0.price_date_for_close = t1.base_date
