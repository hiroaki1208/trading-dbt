{{ config(materialized='view') }}

with

trading_raw as (
  SELECT
    PARSE_DATETIME('%Y/%m/%d %H:%M:%S', timestamp_jst) AS event_time_jst,
    trade_date,
    ticker,
    trade_type,
    account,
    order_count,
    price,
    memo
  FROM
    `trading-prod-468212.trading.raw_trade_history`
  WHERE
    timestamp_jst IS NOT NULL
)

, cash_record as (
  SELECT
    event_time_jst,
    trade_date,
    'cash' as ticker,
    if(trade_type = 'buy', 'sell', 'buy') as trade_type,
    account,
    price * order_count as order_count,
    1 as price,
    memo
  FROM
    trading_raw
  WHERE
    ticker != 'cash'
)

SELECT * FROM trading_raw
UNION ALL
SELECT * FROM cash_record
