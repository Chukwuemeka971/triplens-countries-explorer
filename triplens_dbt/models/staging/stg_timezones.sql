{{ config(
    materialized='view'
) }}

SELECT
    r.RECORD_ID AS COUNTRY_KEY,

    timezone.value::VARCHAR AS TIMEZONE

FROM {{ source('triplens_raw', 'RAW_COUNTRIES') }} AS r,

LATERAL FLATTEN(
    INPUT => r.PAYLOAD:timezones
) AS timezone