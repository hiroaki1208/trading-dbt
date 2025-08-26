-- 基準日のClose時点でのポジション&含み益を算出
{{ config(
    materialized='incremental',
    unique_key=['account', 'ticker', 'partition_date'],
    partition_by={
        "field": "partition_date",
        "data_type": "date"
    }

) }}

{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

SELECT
  t0.account,
  t0.ticker,
  t2.asset_type,
  t2.asset_name,
  t0.position,
  t0.avg_buy_price,
  t1.price AS close_price, -- 基準日時点の価格
  (t1.price - t0.avg_buy_price) * t0.position AS unrealized_value, -- 含み益
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  {{ ref('dwh_daily_position') }} t0
  LEFT JOIN (
    SELECT *
    FROM {{ ref('dwh_price_history') }}
    WHERE base_date = DATE('{{ date_1day_ago }}') and ohlc_type = 'close'
  ) t1 using (ticker)
  LEFT JOIN {{ ref('dwh_asset_master') }} t2 using (ticker)
WHERE
  t0.partition_date = DATE('{{ date_1day_ago }}')
