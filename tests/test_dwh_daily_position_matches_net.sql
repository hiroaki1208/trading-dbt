-- tests/test_dwh_daily_position_matches_net.sql
{{ config(severity='error', tags=['dwh_daily_position']) }}
{%- set date_1day_ago = var('date_1day_ago', '9999-12-31') -%}

with
-- 当日の dwh 側（作成済みの行だけ対象）
d as (
  select
    account,
    ticker,
    position
  from
    {{ ref('dwh_daily_position') }}
  where
    partition_date = date('{{ date_1day_ago }}')
),

-- 取引履歴から計算する “理論上のネットポジション”
n as (
  select
    account,
    ticker,
    sum(case when trade_type = 'buy' then order_count else -1 * order_count end) as net_position
  from
    {{ ref('stg_trade_history') }}
  where
    trade_date <= date('{{ date_1day_ago }}')
    and ticker != 'cash'
  group by
    account, ticker
)

-- 不一致（または一方にしかない）を失敗として返す
select
  coalesce(d.account, n.account) as account,
  coalesce(d.ticker,  n.ticker)  as ticker,
  d.position,
  n.net_position
from
  d
  full outer join n using (account, ticker)
where
  coalesce(d.position, 0) != coalesce(n.net_position, 0)
