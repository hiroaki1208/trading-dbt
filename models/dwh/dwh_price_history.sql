{{ config(materialized='view') }}

-- 価格データの生データを整形
-- 同じticker x date x ohlc_typeの組み合わせでもレコードが複数存在する場合がある
-- その場合はfetch_time_strが最新のものを採用する

WITH

-- ticker x date x ohlc_typeごとにfetch_timeを降順に番号付け
ranked_price_data AS (
  SELECT
    base_date,
    ticker,
    ohlc_type,
    price,
    fetch_time_jst,
    ROW_NUMBER() OVER (PARTITION BY base_date, ticker, ohlc_type ORDER BY fetch_time_jst DESC) AS rn
  FROM
    {{ ref('stg_price_data') }}
)

SELECT
  base_date,
  ticker,
  ohlc_type,
  price,
  fetch_time_jst
FROM
  ranked_price_data
WHERE
  rn = 1
ORDER BY
  base_date,
  ticker,
  ohlc_type