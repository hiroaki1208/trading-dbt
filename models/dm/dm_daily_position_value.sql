-- 基準日のClose時点でのポジション&含み益を算出
{{ config(
    materialized='incremental',
    unique_key=['account', 'ticker', 'partition_date'],
    partition_by={
        "field": "partition_date",
        "data_type": "date"
    }

) }}

{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

WITH

position_data_raw AS (
  SELECT
    t0.account,
    t0.ticker,
    t1.asset_type,
    t1.asset_name,
    t1.is_active_weekend,
    if(t1.is_active_weekend, partition_date, {{ to_prev_weekday("partition_date") }}) AS price_date_for_close,
    t0.position,
    t0.avg_buy_price
  FROM
    {{ ref('dwh_daily_position') }} t0
    LEFT JOIN {{ ref('ref_ticker_info') }} t1 ON t0.ticker = t1.ticker
  WHERE
    t0.partition_date = DATE('{{ date_1day_ago }}')
)

SELECT
  t0.account,
  t0.ticker,
  t0.asset_type,
  t0.asset_name,
  t0.position,
  t0.avg_buy_price,
  t1.price AS close_price, -- 基準日時点の価格
  (t1.price - t0.avg_buy_price) * t0.position AS unrealized_value, -- 含み益
  (t1.price - t0.avg_buy_price) / t0.avg_buy_price AS unrealized_return_ratio, -- 含み益率(%)
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  position_data_raw t0
    LEFT JOIN (
      SELECT *
      FROM {{ ref('dwh_close_price_history') }}
      WHERE base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 6 DAY) AND DATE('{{ date_1day_ago }}')
    ) t1 ON t0.ticker = t1.ticker AND t0.price_date_for_close = t1.base_date
