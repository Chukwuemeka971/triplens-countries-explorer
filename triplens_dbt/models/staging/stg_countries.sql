{{ config(
    materialized='view'
) }}

SELECT
    RECORD_ID AS COUNTRY_KEY,

    COUNTRY_NAME,

    PAYLOAD:names:official::VARCHAR AS OFFICIAL_NAME,

    PAYLOAD:codes:alpha_2::VARCHAR AS ALPHA_2,

    PAYLOAD:codes:alpha_3::VARCHAR AS ALPHA_3,

    PAYLOAD:region::VARCHAR AS REGION,

    PAYLOAD:subregion::VARCHAR AS SUBREGION,

    PAYLOAD:capitals[0]:name::VARCHAR AS CAPITAL,

    PAYLOAD:population::NUMBER AS POPULATION,

    PAYLOAD:area:kilometers::FLOAT AS AREA_KM2,

    PAYLOAD:coordinates:lat::FLOAT AS LATITUDE,

    PAYLOAD:coordinates:lng::FLOAT AS LONGITUDE,

    PAYLOAD:flag:emoji::VARCHAR AS FLAG_EMOJI,

    SOURCE,

    INGESTED_AT

FROM {{ source('triplens_raw', 'RAW_COUNTRIES') }}