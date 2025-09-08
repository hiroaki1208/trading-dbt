-- 基準日のClose時点でのPLを算出
-- 評価期間は過去全期間
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

-- 基準日時点までの全取引を集計
all_trade_data AS (
  SELECT
    account,
    ticker,
    SUM(CASE WHEN trade_type = 'buy' THEN price * order_count ELSE NULL END) AS total_buy_amount, -- 購入金額合計
    SUM(CASE WHEN trade_type = 'sell' THEN price * order_count ELSE NULL END) AS sell_amount_already -- 売却金額合計
  FROM
    {{ ref('stg_trade_history') }}
  WHERE
    trade_date <= DATE('{{ date_1day_ago }}')
    and ticker != 'cash' -- cashの取引は除外
  GROUP BY
    account,
    ticker
)

-- 含み益評価用のポジション解消取引
, position_close_data AS (
  SELECT
    t0.account,
    t0.ticker,
    t1.asset_type,
    t1.asset_name,
    t1.is_active_weekend,
    t0.position,
    if(t1.is_active_weekend, partition_date, {{ to_prev_weekday("partition_date") }}) AS price_date_for_close
  FROM
    {{ ref('dwh_daily_position') }} t0
    LEFT JOIN {{ ref('ref_ticker_info') }} t1 ON t0.ticker = t1.ticker
  WHERE
    t0.partition_date = DATE('{{ date_1day_ago }}')
)

, add_price_data AS (
  SELECT
    t0.account,
    t0.ticker,
    t0.asset_type,
    t0.asset_name,
    t0.position,
    t1.price AS close_price_for_position -- ポジション解消用の終値
  FROM
    position_close_data t0
    LEFT JOIN (
      SELECT *
      FROM {{ ref('dwh_close_price_history') }}
      WHERE base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 6 DAY) AND DATE('{{ date_1day_ago }}')
    ) t1 ON t0.ticker = t1.ticker AND t0.price_date_for_close = t1.base_date
)

SELECT
  t0.account,
  t0.ticker,
  t1.asset_type,
  t1.asset_name,
  t0.total_buy_amount,
  IFNULL(t0.sell_amount_already, 0) AS sell_amount_already,
  IFNULL(t1.position_close_amount, 0) AS position_close_amount,
  IFNULL(t0.sell_amount_already, 0) + IFNULL(t1.position_close_amount, 0) AS total_sell_amount,
  IFNULL(t0.sell_amount_already, 0) + IFNULL(t1.position_close_amount, 0) - t0.total_buy_amount AS pl_amount_all_term, -- PL変化額
  SAFE_DIVIDE( IFNULL(t0.sell_amount_already, 0) + IFNULL(t1.position_close_amount, 0), t0.total_buy_amount ) - 1 AS pl_ratio_all_term, -- PL変化率
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  all_trade_data t0
  LEFT JOIN position_close_data t1 using (account, ticker)
