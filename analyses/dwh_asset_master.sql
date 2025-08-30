WITH

add_rn AS (
  SELECT
    event_time_jst,
    ticker,
    is_monitor,
    is_watch,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY event_time_jst DESC) AS rn
  FROM
    {{ ref('stg_asset_status_history') }}
)

SELECT
  t0.ticker,
  t1.asset_name,
  t1.asset_type,
  t0.is_monitor,
  t0.is_watch,
  t0.event_time_jst AS latest_status_chg_time_jst
FROM
  add_rn t0
  LEFT JOIN {{ ref('stg_asset_name_master') }} t1 ON t0.ticker = t1.ticker
WHERE
  rn = 1

-- SELECT * FROM cte3 /*
-- */
