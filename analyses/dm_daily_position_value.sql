-- 基準日のClose時点でのポジション&含み益を算出
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
  (t1.price - t0.avg_buy_price) / t0.avg_buy_price AS unrealized_return_ratio, -- 含み益率(%)
  DATE('{{ date_1day_ago }}') AS partition_date
FROM
  {{ ref('dwh_price_history') }} t0
WHERE
  t0.partition_date = DATE('{{ date_1day_ago }}')

-- SELECT * FROM cte3 /*
-- */