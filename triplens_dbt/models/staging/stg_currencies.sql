{{ config(
    materialized='view'
) }}

SELECT
    r.RECORD_ID AS COUNTRY_KEY,

    currency.value:code::VARCHAR AS CURRENCY_CODE,

    currency.value:name::VARCHAR AS CURRENCY_NAME,

    currency.value:symbol::VARCHAR AS CURRENCY_SYMBOL

FROM {{ source('triplens_raw', 'RAW_COUNTRIES') }} AS r,

LATERAL FLATTEN(
    INPUT => r.PAYLOAD:currencies
) AS currency