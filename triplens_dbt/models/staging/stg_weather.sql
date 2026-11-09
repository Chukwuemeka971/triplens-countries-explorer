WITH raw_weather AS (

    SELECT
        RECORD_ID,
        COUNTRY_CODE,
        FORECAST_DATE,
        SOURCE,
        INGESTED_AT,
        PAYLOAD
    FROM TRIPLENS.RAW.RAW_WEATHER

),

hourly_weather AS (

    SELECT
        r.RECORD_ID,
        r.COUNTRY_CODE,
        r.FORECAST_DATE,
        r.SOURCE,
        r.INGESTED_AT,

        r.PAYLOAD:latitude::FLOAT AS LATITUDE,
        r.PAYLOAD:longitude::FLOAT AS LONGITUDE,
        r.PAYLOAD:timezone::VARCHAR AS TIMEZONE,

        hourly_time.INDEX AS HOUR_INDEX,

        hourly_time.VALUE::TIMESTAMP_NTZ
            AS WEATHER_TIMESTAMP,

        r.PAYLOAD:hourly:temperature_2m[
            hourly_time.INDEX
        ]::FLOAT AS TEMPERATURE_2M,

        r.PAYLOAD:hourly:apparent_temperature[
            hourly_time.INDEX
        ]::FLOAT AS APPARENT_TEMPERATURE,

        r.PAYLOAD:hourly:precipitation[
            hourly_time.INDEX
        ]::FLOAT AS PRECIPITATION,

        r.PAYLOAD:hourly:rain[
            hourly_time.INDEX
        ]::FLOAT AS RAIN,

        r.PAYLOAD:hourly:weather_code[
            hourly_time.INDEX
        ]::INTEGER AS WEATHER_CODE,

        r.PAYLOAD:hourly:wind_speed_10m[
            hourly_time.INDEX
        ]::FLOAT AS WIND_SPEED_10M

    FROM raw_weather AS r

    CROSS JOIN LATERAL FLATTEN(
        INPUT => r.PAYLOAD:hourly:time
    ) AS hourly_time

)

SELECT
    RECORD_ID,
    COUNTRY_CODE,
    FORECAST_DATE,
    SOURCE,
    INGESTED_AT,
    LATITUDE,
    LONGITUDE,
    TIMEZONE,
    HOUR_INDEX,
    WEATHER_TIMESTAMP,
    TEMPERATURE_2M,
    APPARENT_TEMPERATURE,
    PRECIPITATION,
    RAIN,
    WEATHER_CODE,
    WIND_SPEED_10M

FROM hourly_weather