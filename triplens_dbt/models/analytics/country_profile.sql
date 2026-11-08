{{ config(
    materialized='table'
) }}

WITH country_base AS (

    SELECT
        COUNTRY_KEY,
        COUNTRY_NAME,
        OFFICIAL_NAME,
        ALPHA_2,
        ALPHA_3,
        REGION,
        SUBREGION,
        CAPITAL,
        POPULATION,
        AREA_KM2,
        LATITUDE,
        LONGITUDE,
        FLAG_EMOJI,
        SOURCE,
        INGESTED_AT
    FROM {{ ref('stg_countries') }}

),

language_counts AS (

    SELECT
        COUNTRY_KEY,
        COUNT(*) AS LANGUAGE_COUNT
    FROM {{ ref('stg_languages') }}
    GROUP BY COUNTRY_KEY

),

timezone_counts AS (

    SELECT
        COUNTRY_KEY,
        COUNT(*) AS TIMEZONE_COUNT
    FROM {{ ref('stg_timezones') }}
    GROUP BY COUNTRY_KEY

),

border_counts AS (

    SELECT
        COUNTRY_KEY,
        COUNT(*) AS BORDER_COUNTRY_COUNT
    FROM {{ ref('stg_borders') }}
    GROUP BY COUNTRY_KEY

)

SELECT
    c.COUNTRY_KEY,
    c.COUNTRY_NAME,
    c.OFFICIAL_NAME,
    c.ALPHA_2,
    c.ALPHA_3,
    c.REGION,
    c.SUBREGION,
    c.CAPITAL,
    c.POPULATION,
    c.AREA_KM2,
    c.LATITUDE,
    c.LONGITUDE,
    c.FLAG_EMOJI,

    COALESCE(l.LANGUAGE_COUNT, 0) AS LANGUAGE_COUNT,
    COALESCE(t.TIMEZONE_COUNT, 0) AS TIMEZONE_COUNT,
    COALESCE(b.BORDER_COUNTRY_COUNT, 0) AS BORDER_COUNTRY_COUNT,

    c.SOURCE,
    c.INGESTED_AT

FROM country_base c

LEFT JOIN language_counts l
    ON c.COUNTRY_KEY = l.COUNTRY_KEY

LEFT JOIN timezone_counts t
    ON c.COUNTRY_KEY = t.COUNTRY_KEY

LEFT JOIN border_counts b
    ON c.COUNTRY_KEY = b.COUNTRY_KEY