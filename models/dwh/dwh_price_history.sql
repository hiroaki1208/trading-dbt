{{ config(materialized='view') }}

-- 価格データの生データを整形
-- 同じticker x date x ohlc_typeの組み合わせでもレコードが複数存在する場合がある
-- その場合はfetch_time_strが最新のものを採用する
-- 土日のデータは前の金曜日の値で補完する

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

, price_data_raw_weekday AS (
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

-- 全期間の日付範囲を生成（固定範囲）
, date_range AS (
  SELECT
    date_day,
    {{ to_prev_weekday('date_day') }} AS date_day_weekday
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2050-12-31', INTERVAL 1 DAY)) AS date_day
)

-- すべてのticker x ohlc_typeの組み合わせを取得
, ticker_ohlc_combinations AS (
  SELECT DISTINCT
    ticker,
    ohlc_type
  FROM price_data_raw_weekday
)

-- 全日付 x ticker x ohlc_typeの組み合わせを生成
, all_combinations AS (
  SELECT
    dr.date_day AS base_date,
    dr.date_day_weekday AS base_date_weekday,
    toc.ticker,
    toc.ohlc_type
  FROM date_range dr
  CROSS JOIN ticker_ohlc_combinations toc
)

-- 平日データを全日付に拡張（土日は前の金曜日の値で補完）
-- 土日の値があるtickerは、その値を採用(ex. BTC)
-- 土日の値がないtickerは、前の金曜の日付としたbase_date_weekdayにjoinすることで補完
, price_data_raw_daily AS (
  SELECT
    ac.base_date,
    ac.ticker,
    ac.ohlc_type,
    COALESCE(pwd.price, pwd_prev.price) AS price,
    COALESCE(pwd.fetch_time_jst, pwd_prev.fetch_time_jst) AS fetch_time_jst
  FROM all_combinations ac
  LEFT JOIN price_data_raw_weekday pwd
    ON ac.base_date = pwd.base_date
    AND ac.ticker = pwd.ticker
    AND ac.ohlc_type = pwd.ohlc_type
  LEFT JOIN price_data_raw_weekday pwd_prev
    ON ac.base_date_weekday = pwd_prev.base_date
    AND ac.ticker = pwd_prev.ticker
    AND ac.ohlc_type = pwd_prev.ohlc_type
  WHERE
    -- priceがNULLのdate x tickerの組み合わせは、rawにそもそもないデータなので除外
    COALESCE(pwd.price, pwd_prev.price) IS NOT NULL
)

-- 追加データ
-- BTCJPYなど、直接取得できないデータを追加
, additional_data_btcjpy AS (
  SELECT
    base_date,
    'BTC-JPY' AS ticker,
    'close' AS ohlc_type,    
    ROUND(
      MAX(CASE WHEN ticker = 'BTC-USD' THEN price END) *
      MAX(CASE WHEN ticker = 'JPY=X' THEN price END)
    , 0) AS price,
    MAX(fetch_time_jst) AS fetch_time_jst
  FROM
    price_data_raw_daily
  WHERE
    ticker IN ('BTC-USD', 'JPY=X')
    AND ohlc_type = 'close'
  GROUP BY
    base_date
)

, additional_data_ethjpy AS (
  SELECT
    base_date,
    'ETH-JPY' AS ticker,
    'close' AS ohlc_type,
    ROUND(
      MAX(CASE WHEN ticker = 'ETH-USD' THEN price END) *
      MAX(CASE WHEN ticker = 'JPY=X' THEN price END)
    , 0) AS price,
    MAX(fetch_time_jst) AS fetch_time_jst
  FROM
    price_data_raw_daily
  WHERE
    ticker IN ('ETH-USD', 'JPY=X')
    AND ohlc_type = 'close'
  GROUP BY
    base_date
)

SELECT * FROM price_data_raw_daily
UNION ALL
SELECT * FROM additional_data_btcjpy
UNION ALL
SELECT * FROM additional_data_ethjpy
