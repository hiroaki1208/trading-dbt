-- 基準日のClose時点でのポジション&含み益を算出
{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}
{%- set date_7day_ago = var('date_7day_ago', '9999-12-31') -%}

SELECT
  ticker,
  base_date,
  {{ to_prev_weekday("base_date") }} as adjusted_date,
  ohlc_type,
  price
FROM
  {{ ref('dwh_price_history') }}
WHERE
  partition_date BETWEEN DATE('{{ date_7day_ago }}') AND DATE('{{ date_1day_ago }}')
  and ohlc_type = 'close'
order by
  ticker, base_date desc

-- SELECT * FROM cte3 /*
-- */
