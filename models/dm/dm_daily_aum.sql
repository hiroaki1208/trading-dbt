-- 基準日のClose時点でのAUMを算出
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

-- 1.基準日時点までのcash amountを集計
cash_amount_data AS (
  SELECT
    account,
    ticker,
    'cash' AS asset_type,
    'cash' AS asset_name,
    SUM(CASE WHEN trade_type = 'buy' THEN price * order_count ELSE -1 * price * order_count END) AS position, -- cashはposition=current_valueとしている
    1 AS value_price, -- cashの単価は1
    SUM(CASE WHEN trade_type = 'buy' THEN price * order_count ELSE -1 * price * order_count END) AS current_value
  FROM
    {{ ref('dwh_trade_history_with_cash') }}
  WHERE
    trade_date <= DATE('{{ date_1day_ago }}')
    and ticker = 'cash' -- cashの取引のみ
  GROUP BY
    account,
    ticker,
    asset_type,
    asset_name
)

-- 2.含み益評価
-- 基準日時点の終値で評価
, position_data_raw AS (
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

, position_data AS (
  SELECT
    t0.account,
    t0.ticker,
    t0.asset_type,
    t0.asset_name,
    t0.position,
    t1.price AS value_price, -- 基準日時点の価格
    t1.price * t0.position AS current_value -- 評価額
  FROM
    position_data_raw t0
    LEFT JOIN (
      SELECT *
      FROM {{ ref('dwh_close_price_history') }}
      WHERE base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 6 DAY) AND DATE('{{ date_1day_ago }}')
    ) t1 ON t0.ticker = t1.ticker AND t0.price_date_for_close = t1.base_date
)

SELECT
  account,
  ticker,
  asset_type,
  asset_name,
  position,
  value_price,
  current_value,
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  cash_amount_data
UNION ALL
SELECT
  account,
  ticker,
  asset_type,
  asset_name,
  position,
  value_price,
  current_value,
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  position_data
