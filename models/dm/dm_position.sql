{{ config(
    materialized='incremental',
    unique_key=['account', 'ticker', 'partition_date']
) }}

{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

-- 基準日時点でのポジション作成
-- cashにポジションという考えはないのでこの時点で除外
  SELECT
    t0.account,
    t0.ticker,
    t1.asset_type,
    t1.asset_name,
    SUM(CASE WHEN trade_type = 'buy' THEN order_count ELSE -1 * order_count END) AS position,
    DATE('{{ date_1day_ago }}') AS partition_date
  FROM
    {{ ref('dwh_trade_history_with_cash') }} t0
    left join {{ ref('dwh_asset_master') }} t1 using (ticker)
  WHERE
    trade_date <= DATE('{{ date_1day_ago }}')
    and ticker != 'cash' -- cash以外
  GROUP BY
    account,
    ticker,
    asset_type,
    asset_name
