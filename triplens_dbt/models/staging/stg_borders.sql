{{ config(
    materialized='view'
) }}

SELECT
    r.RECORD_ID AS COUNTRY_KEY,

    border.value::VARCHAR AS BORDER_ALPHA_3

FROM {{ source('triplens_raw', 'RAW_COUNTRIES') }} AS r,

LATERAL FLATTEN(
    INPUT => r.PAYLOAD:borders
) AS border