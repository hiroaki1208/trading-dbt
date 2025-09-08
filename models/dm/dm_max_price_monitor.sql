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

-- tickerマスタ
-- ソート順付き
ticker_list_data AS (
  SELECT
    t0.*,
    t1.asset_name,
    t1.is_active_weekend,
    if(t1.is_active_weekend, DATE('{{ date_1day_ago }}'), {{ to_prev_weekday("DATE('{{ date_1day_ago }}')") }}) AS price_date_for_close
  FROM {{ ref('dim_max_price') }} t0
    LEFT JOIN {{ ref('ref_ticker_info') }} t1 USING (ticker)
)

-- close価格取得
, close_price_data AS (
  SELECT *
  FROM {{ ref('dwh_close_price_history') }}
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 1 YEAR) AND DATE('{{ date_1day_ago }}')
)

-- tickerごとの直近3カ月以内の最高値とその日付を取得
, max_price_3month_data AS (
  SELECT
    ticker,
    MAX(price) AS max_price_3month,
    ARRAY_AGG(base_date ORDER BY price DESC LIMIT 1)[OFFSET(0)] AS max_price_date_3month
  FROM close_price_data
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 3 MONTH) AND DATE('{{ date_1day_ago }}')
  GROUP BY ticker
)

-- tickerごとの直近6カ月以内の最高値とその日付を取得
, max_price_6month_data AS (
  SELECT
    ticker,
    MAX(price) AS max_price_6month,
    ARRAY_AGG(base_date ORDER BY price DESC LIMIT 1)[OFFSET(0)] AS max_price_date_6month
  FROM close_price_data
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 6 MONTH) AND DATE('{{ date_1day_ago }}')
  GROUP BY ticker
)

-- tickerごとの直近1年以内の最高値とその日付を取得
, max_price_1year_data AS (
  SELECT
    ticker,
    MAX(price) AS max_price_1year,
    ARRAY_AGG(base_date ORDER BY price DESC LIMIT 1)[OFFSET(0)] AS max_price_date_1year
  FROM close_price_data
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 1 YEAR) AND DATE('{{ date_1day_ago }}')
  GROUP BY ticker
)

SELECT
  t0.order_group,
  t0.order_index,
  t0.ticker,
  t0.asset_name,
  t0.price_date_for_close AS recent_price_date,
  t1.price AS recent_price,
  t2.max_price_3month,
  t2.max_price_date_3month,
  t3.max_price_6month,
  t3.max_price_date_6month,
  t4.max_price_1year,
  t4.max_price_date_1year,
  DATE('{{ date_1day_ago }}') AS base_date
FROM
  ticker_list_data t0
  LEFT JOIN close_price_data t1 ON t0.ticker = t1.ticker AND t0.price_date_for_close = t1.base_date
  LEFT JOIN max_price_3month_data t2 ON t0.ticker = t2.ticker
  LEFT JOIN max_price_6month_data t3 ON t0.ticker = t3.ticker
  LEFT JOIN max_price_1year_data t4 ON t0.ticker = t4.ticker
