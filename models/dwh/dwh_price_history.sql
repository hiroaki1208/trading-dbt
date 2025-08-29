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

, price_data_raw AS (
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
)

-- 追加データ
-- BTCJPYなど、直接取得できないデータを追加
, additional_data_btcjpy AS (
  SELECT
    base_date,
    'BTC-JPY' AS ticker,
    'close' AS ohlc_type,
    ROUND(
      MAX(CASE WHEN ticker = 'BTC-USD' AND ohlc_type = 'close' THEN price END) *
      MAX(CASE WHEN ticker = 'JPY=X' AND ohlc_type = 'close' THEN price END)
    , 0) AS price,
    MAX(fetch_time_jst) AS fetch_time_jst
  FROM
    price_data_raw
  WHERE
    ticker IN ('BTC-USD', 'JPY=X')
  GROUP BY
    base_date
)


, additional_data_ethjpy AS (
  SELECT
    base_date,
    'ETH-JPY' AS ticker,
    'close' AS ohlc_type,
    ROUND(
      MAX(CASE WHEN ticker = 'ETH-USD' AND ohlc_type = 'close' THEN price END) *
      MAX(CASE WHEN ticker = 'JPY=X' AND ohlc_type = 'close' THEN price END)
    , 0) AS price,
    MAX(fetch_time_jst) AS fetch_time_jst
  FROM
    price_data_raw
  WHERE
    ticker IN ('ETH-USD', 'JPY=X')
  GROUP BY
    base_date
)

SELECT * FROM price_data_raw
UNION ALL
SELECT * FROM additional_data_btcjpy
UNION ALL
SELECT * FROM additional_data_ethjpy
