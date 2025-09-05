{{ config(
    materialized='incremental',
    unique_key=['ticker', 'base_date'],
    partition_by={
        "field": "base_date",
        "data_type": "date"
    }

) }}

{%- set date_28day_ago = var('date_28day_ago', '9999-12-31') -%}
{%- set date_3day_ago = var('date_3day_ago', '9999-12-31') -%}
{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

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
  WHERE
    base_date BETWEEN DATE('{{ date_28day_ago }}') AND DATE('{{ date_1day_ago }}')
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

-- date x ticker x ohlc_typeの組み合わせを全て作成
-- nullは最大14日までさかのぼってbackfill
-- dateはtickerによる。is_active_weekend:trueのtickerは土日も含む。falseは含まない
-- 全期間の日付範囲を生成（固定範囲）
, date_range AS (
  SELECT
    date_day,
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE('{{ date_28day_ago }}'), DATE('{{ date_1day_ago }}'), INTERVAL 1 DAY)) AS date_day
)

-- すべてのticker x ohlc_typeの組み合わせを取得
, ticker_ohlc_combinations AS (
  SELECT DISTINCT
    ticker,
    ohlc_type,
    is_active_weekend
  FROM price_data_raw_weekday
    LEFT JOIN {{ ref('ref_ticker_info') }} USING (ticker)
)

-- 全日付 x ticker x ohlc_typeの組み合わせを生成
-- is_active_weekend:trueのtickerは土日も含む、falseは平日のみ
, all_combinations AS (
  SELECT
    dr.date_day AS base_date,
    toc.ticker,
    toc.ohlc_type
  FROM date_range dr
  CROSS JOIN ticker_ohlc_combinations toc
  WHERE
    -- is_active_weekend:trueの場合は全日付、falseの場合は平日のみ
    toc.is_active_weekend = TRUE
    OR (toc.is_active_weekend = FALSE AND EXTRACT(DAYOFWEEK FROM dr.date_day) NOT IN (1, 7))
)

-- 各組み合わせに対して価格データを結合
, price_data_raw_daily AS (
  SELECT
    ac.base_date,
    ac.ticker,
    ac.ohlc_type,
    pwd.price AS price_raw,
    -- nullの場合は直近14日以内の値で補完
    LAST_VALUE(pwd.price IGNORE NULLS) OVER (
      PARTITION BY ac.ticker, ac.ohlc_type 
      ORDER BY ac.base_date 
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) AS price,
    LAST_VALUE(pwd.fetch_time_jst IGNORE NULLS) OVER (
      PARTITION BY ac.ticker, ac.ohlc_type 
      ORDER BY ac.base_date 
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) AS fetch_time_jst
  FROM all_combinations ac
  LEFT JOIN price_data_raw_weekday pwd
    ON ac.base_date = pwd.base_date
    AND ac.ticker = pwd.ticker
    AND ac.ohlc_type = pwd.ohlc_type
)

-- 追加データ
-- BTCJPYなど、直接取得できないデータを追加
, add_data_btcjpy_raw AS (
  SELECT
    btcusd.base_date,
    'BTC-JPY' AS ticker,
    'close' AS ohlc_type,
    btcusd.price AS price_raw_btc,
    usdjpy.price AS price_raw_usdjpy,
    btcusd.fetch_time_jst
  FROM
    (SELECT * FROM price_data_raw_daily WHERE ticker = 'BTC-USD' AND ohlc_type = 'close') btcusd
    LEFT JOIN (SELECT * FROM price_data_raw_daily WHERE ticker = 'JPY=X' AND ohlc_type = 'close') usdjpy USING (base_date)
)

, add_data_btcjpy AS (
  SELECT
    base_date,
    ticker,
    ohlc_type,
    price_raw_btc * LAST_VALUE(
      price_raw_usdjpy IGNORE NULLS) OVER (
      ORDER BY base_date
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) AS price,
    fetch_time_jst
  FROM
    add_data_btcjpy_raw
)

-- ETH/JPYも同様に追加
, add_data_ethjpy_raw AS (
  SELECT
    ethusd.base_date,
    'ETH-JPY' AS ticker,
    'close' AS ohlc_type,
    ethusd.price AS price_raw_eth,
    usdjpy.price AS price_raw_usdjpy,
    ethusd.fetch_time_jst
  FROM
    (SELECT * FROM price_data_raw_daily WHERE ticker = 'ETH-USD' AND ohlc_type = 'close') ethusd
    LEFT JOIN (SELECT * FROM price_data_raw_daily WHERE ticker = 'JPY=X' AND ohlc_type = 'close') usdjpy USING (base_date)
)

, add_data_ethjpy AS (
  SELECT
    base_date,
    ticker,
    ohlc_type,
    price_raw_eth * LAST_VALUE(
      price_raw_usdjpy IGNORE NULLS) OVER (
      ORDER BY base_date
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) AS price,
    fetch_time_jst
  FROM
    add_data_ethjpy_raw
)

, data_28d AS (
SELECT base_date, ticker, ohlc_type, price, fetch_time_jst FROM price_data_raw_daily
UNION ALL
SELECT base_date, ticker, ohlc_type, price, fetch_time_jst FROM add_data_btcjpy
UNION ALL
SELECT base_date, ticker, ohlc_type, price, fetch_time_jst FROM add_data_ethjpy
)

-- 更新は直近３日分のみ
SELECT * FROM data_28d
WHERE base_date BETWEEN DATE('{{ date_3day_ago }}') AND DATE('{{ date_1day_ago }}')