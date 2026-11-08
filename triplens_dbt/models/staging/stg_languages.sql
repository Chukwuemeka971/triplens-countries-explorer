{{ config(
    materialized='view'
) }}

SELECT
    r.RECORD_ID AS COUNTRY_KEY,

    language.value:name::VARCHAR AS LANGUAGE_NAME,

    language.value:iso639_1::VARCHAR AS LANGUAGE_CODE

FROM {{ source('triplens_raw', 'RAW_COUNTRIES') }} AS r,

LATERAL FLATTEN(
    INPUT => r.PAYLOAD:languages
) AS language