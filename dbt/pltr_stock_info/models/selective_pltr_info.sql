{{
    config(materialized='table')
}}

SELECT
    date(Date_Recorded) AS Date_Recorded,
    Open AS Open_Price,
    Close AS Close_Price,
    Volume AS Trade_Volume
FROM pltr_stock_info.pltr_historical_data