{{ config(materialized='view') }}

SELECT base_date, ticker, ohlc_type, price, fetch_time_jst FROM {{ ref('dwh_price_history') }} WHERE ohlc_type = 'close'
