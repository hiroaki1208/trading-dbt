{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

WITH

-- tickerマスタ
-- ソート順付き
ticker_list_data AS (
  SELECT
    t0.*,
    t1.asset_name,
    t1.is_active_weekend,
    DATE('{{ date_1day_ago }}') AS base_date
  FROM {{ ref('dim_summary_ticker_list') }} t0
    LEFT JOIN {{ ref('ref_ticker_info') }} t1 USING (ticker)
)

-- close価格取得
, close_price_data AS (
  SELECT *
  FROM {{ ref('dwh_price_history') }}
  WHERE
    base_date BETWEEN DATE_SUB(DATE('{{ date_1day_ago }}'), INTERVAL 30 DAY) AND DATE('{{ date_1day_ago }}')
    AND ohlc_type = 'close'
)

-- 休日取引フラグを基に、N日前の日付を計算
, add_ndays_ago_date AS (
  SELECT
    t0.*,
    CASE
      WHEN t0.is_active_weekend THEN t0.base_date
      WHEN NOT t0.is_active_weekend THEN {{ to_prev_weekday('t0.base_date') }} 
    END AS recent_date,
    CASE
      WHEN t0.is_active_weekend THEN DATE_SUB(t0.base_date, INTERVAL 1 DAY)
      WHEN NOT t0.is_active_weekend AND EXTRACT(DAYOFWEEK FROM base_date) IN (1, 7) THEN DATE_SUB({{ to_prev_weekday('t0.base_date') }}, INTERVAL 1 DAY)
      WHEN NOT t0.is_active_weekend AND EXTRACT(DAYOFWEEK FROM base_date) NOT IN (1, 7) THEN {{ to_prev_weekday('DATE_SUB(t0.base_date, INTERVAL 1 DAY)') }}
    END AS prev_date,
    CASE
      WHEN t0.is_active_weekend THEN DATE_SUB(t0.base_date, INTERVAL 7 DAY)
      WHEN NOT t0.is_active_weekend THEN {{ to_prev_weekday('DATE_SUB(t0.base_date, INTERVAL 7 DAY)') }}
    END AS date_7day_ago,
    CASE
      WHEN t0.is_active_weekend THEN DATE_SUB(t0.base_date, INTERVAL 28 DAY)
      WHEN NOT t0.is_active_weekend THEN {{ to_prev_weekday('DATE_SUB(t0.base_date, INTERVAL 28 DAY)') }}
    END AS date_28day_ago,
  FROM ticker_list_data t0
)

-- N日前の価格を取得
, add_ndays_ago_price AS (
  SELECT
    t0.*,
    t1.price AS price_recent_date,
    t2.price AS price_prev_date,
    t3.price AS price_7day_ago,
    t4.price AS price_28day_ago,
    -- 直近価格との差分
    t1.price - t2.price AS diff_recent_prev,
    t1.price - t3.price AS diff_recent_7day,
    t1.price - t4.price AS diff_recent_28day,
    -- 直近価格からの変化率
    SAFE_DIVIDE(t1.price - t2.price, t2.price) AS rate_recent_prev,
    SAFE_DIVIDE(t1.price - t3.price, t3.price) AS rate_recent_7day,
    SAFE_DIVIDE(t1.price - t4.price, t4.price) AS rate_recent_28day
  FROM add_ndays_ago_date t0
    LEFT JOIN close_price_data t1 ON t0.ticker = t1.ticker AND t0.recent_date = t1.base_date
    LEFT JOIN close_price_data t2 ON t0.ticker = t2.ticker AND t0.prev_date = t2.base_date
    LEFT JOIN close_price_data t3 ON t0.ticker = t3.ticker AND t0.date_7day_ago = t3.base_date
    LEFT JOIN close_price_data t4 ON t0.ticker = t4.ticker AND t0.date_28day_ago = t4.base_date
)

SELECT * FROM add_ndays_ago_price ORDER BY order_group, order_index

-- SELECT * FROM cte3 /*
-- */
