-- 各日付時点でのポジション作成
-- 前提：ロングオンリー
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

-- 全ての取引を1件ずつに分解
explode_order AS (
  SELECT
    trade_date,
    ticker,
    trade_type,
    account,
    1 AS order_count,
    price
  FROM
    {{ ref('stg_trade_history') }} t0
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, t0.order_count)) AS seq
  WHERE
    trade_date <= DATE('{{ date_1day_ago }}')
    and ticker != 'cash' -- cash以外
)

-- buy,sellそれぞれに連番を付与
, add_idx AS (
  SELECT
    trade_date,
    ticker,
    trade_type,
    account,
    order_count,
    price,
    ROW_NUMBER() OVER (PARTITION BY account, ticker, trade_type ORDER BY trade_date ASC) AS idx
  FROM
    explode_order
)

-- buy,sellを1件ずつ突き合わせるデータ
, buy_sell_match_data AS (
  SELECT
    t0.account,
    t0.ticker,
    t0.trade_type AS trade_type_buy,
    t0.order_count AS order_count_buy,
    t0.price AS price_buy,
    t0.idx AS idx_buy,
    t1.trade_type AS trade_type_sell,
    t1.order_count AS order_count_sell,
    t1.price AS price_sell,
    t1.idx AS idx_sell
  FROM
    (SELECT * from add_idx where trade_type = 'buy') t0
    LEFT JOIN (SELECT * from add_idx where trade_type = 'sell') t1 USING(account, ticker, idx)
)

-- ポジションと平均購入単価作成
  SELECT
    t0.account,
    t0.ticker,
    SUM(t0.order_count_buy) AS position, -- 買いポジション量
    AVG(t0.price_buy) AS avg_buy_price, -- 平均購入単価
    DATE('{{ date_1day_ago }}') AS partition_date
  FROM
    buy_sell_match_data t0
  WHERE
    -- 買いに対して売りがないもののみ
    -- 前提：ロングオンリー
    idx_sell IS NULL
  GROUP BY
    account,
    ticker
