import hashlib
import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from minio import Minio
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

from triplens.storage.minio_client import (
    download_json,
    get_minio_client,
    object_exists,
)
from triplens.storage.snowflake.snowflake_client import (
    get_snowflake_connection,
)

logger = logging.getLogger(__name__)

RAW_TABLE = "TRIPLENS.RAW.RAW_WEATHER"
SOURCE_NAME = "open-meteo"


class WeatherSnowflakeLoaderError(Exception):
    """Raised when loading weather data into Snowflake fails."""


def _validate_country_code(country_code: str) -> str:
    """
    Validate and normalize an ISO alpha-3 country code.
    """
    if not isinstance(country_code, str):
        raise WeatherSnowflakeLoaderError(
            "Country code must be a string."
        )

    normalized = country_code.strip().upper()

    if len(normalized) != 3 or not normalized.isalpha():
        raise WeatherSnowflakeLoaderError(
            f"Invalid country code: {country_code}"
        )

    return normalized


def _validate_forecast_date(forecast_date: str) -> str:
    """
    Validate an ISO-format forecast date.
    """
    if not isinstance(forecast_date, str):
        raise WeatherSnowflakeLoaderError(
            "Forecast date must be a string."
        )

    try:
        parsed_date = date.fromisoformat(
            forecast_date
        )
    except ValueError as exc:
        raise WeatherSnowflakeLoaderError(
            f"Invalid forecast date: {forecast_date}"
        ) from exc

    return parsed_date.isoformat()


def _build_weather_object_name(
    country_code: str,
    forecast_date: str,
) -> str:
    """
    Build the deterministic MinIO object path.
    """
    country_code = _validate_country_code(
        country_code
    )

    forecast_date = _validate_forecast_date(
        forecast_date
    )

    return (
        f"raw/weather/"
        f"{country_code}/"
        f"{forecast_date}.json"
    )


def _build_record_id(
    country_code: str,
    forecast_date: str,
) -> str:
    """
    Build a deterministic record ID.

    Same country + same forecast date
    always produces the same ID.
    """
    country_code = _validate_country_code(
        country_code
    )

    forecast_date = _validate_forecast_date(
        forecast_date
    )

    stable_key = (
        f"{SOURCE_NAME}:"
        f"{country_code}:"
        f"{forecast_date}"
    )

    return hashlib.sha256(
        stable_key.encode("utf-8")
    ).hexdigest()


def _validate_weather_payload(
    payload: Any,
) -> None:
    """
    Validate the basic structure of an
    Open-Meteo response.
    """
    if not isinstance(payload, dict):
        raise WeatherSnowflakeLoaderError(
            "Weather payload must be a dictionary."
        )

    if "latitude" not in payload:
        raise WeatherSnowflakeLoaderError(
            "Weather payload is missing latitude."
        )

    if "longitude" not in payload:
        raise WeatherSnowflakeLoaderError(
            "Weather payload is missing longitude."
        )

    if "hourly" not in payload:
        raise WeatherSnowflakeLoaderError(
            "Weather payload is missing hourly data."
        )

    if not isinstance(
        payload["hourly"],
        dict,
    ):
        raise WeatherSnowflakeLoaderError(
            "Weather hourly data must be a dictionary."
        )


def _get_weather_from_minio(
    country_code: str,
    forecast_date: str,
    client: Minio,
) -> dict:
    """
    Retrieve an already-cached weather response
    from MinIO.

    This function deliberately does NOT call
    the weather API.
    """
    object_name = _build_weather_object_name(
        country_code,
        forecast_date,
    )

    logger.info(
        "Reading weather data from MinIO: %s",
        object_name,
    )

    if not object_exists(
        client,
        object_name,
    ):
        raise WeatherSnowflakeLoaderError(
            "Weather object does not exist in MinIO: "
            f"{object_name}"
        )

    payload = download_json(
        client,
        object_name,
    )

    _validate_weather_payload(payload)

    return payload


def load_weather_record(
    country_code: str,
    forecast_date: str,
    minio_client: Minio | None = None,
    connection: SnowflakeConnection | None = None,
) -> str:
    """
    Load one cached weather record into
    Snowflake RAW_WEATHER using MERGE.
    """
    country_code = _validate_country_code(
        country_code
    )

    forecast_date = _validate_forecast_date(
        forecast_date
    )

    record_id = _build_record_id(
        country_code,
        forecast_date,
    )

    
    should_close_connection = connection is None

    try:
        if minio_client is None:
            minio_client = get_minio_client()

        if connection is None:
            connection = get_snowflake_connection()

        payload = _get_weather_from_minio(
            country_code,
            forecast_date,
            minio_client,
        )

        ingested_at = datetime.now(
            timezone.utc
        )

        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
        )

        merge_sql = f"""
            MERGE INTO {RAW_TABLE} AS target
            USING (
                SELECT
                    %s AS RECORD_ID,
                    %s AS COUNTRY_CODE,
                    %s AS FORECAST_DATE,
                    %s AS SOURCE,
                    %s AS INGESTED_AT,
                    PARSE_JSON(%s) AS PAYLOAD
            ) AS source

            ON target.RECORD_ID =
               source.RECORD_ID

            WHEN MATCHED THEN
                UPDATE SET
                    COUNTRY_CODE =
                        source.COUNTRY_CODE,
                    FORECAST_DATE =
                        source.FORECAST_DATE,
                    SOURCE =
                        source.SOURCE,
                    INGESTED_AT =
                        source.INGESTED_AT,
                    PAYLOAD =
                        source.PAYLOAD

            WHEN NOT MATCHED THEN
                INSERT (
                    RECORD_ID,
                    COUNTRY_CODE,
                    FORECAST_DATE,
                    SOURCE,
                    INGESTED_AT,
                    PAYLOAD
                )
                VALUES (
                    source.RECORD_ID,
                    source.COUNTRY_CODE,
                    source.FORECAST_DATE,
                    source.SOURCE,
                    source.INGESTED_AT,
                    source.PAYLOAD
                )
        """

        with connection.cursor() as cursor:
            cursor.execute(
                merge_sql,
                (
                    record_id,
                    country_code,
                    forecast_date,
                    SOURCE_NAME,
                    ingested_at,
                    payload_json,
                ),
            )

        connection.commit()

        logger.info(
            "Weather loaded into Snowflake RAW: "
            "country=%s date=%s",
            country_code,
            forecast_date,
        )

        return record_id

    except SnowflakeError as exc:
        if connection is not None:
            connection.rollback()

        logger.exception(
            "Snowflake weather load failed: "
            "country=%s date=%s",
            country_code,
            forecast_date,
        )

        raise WeatherSnowflakeLoaderError(
            "Failed to load weather into "
            f"Snowflake: {country_code} "
            f"{forecast_date}"
        ) from exc

    except WeatherSnowflakeLoaderError:
        if connection is not None:
            connection.rollback()

        raise

    except Exception as exc:
        if connection is not None:
            connection.rollback()

        logger.exception(
            "Unexpected weather loading error: "
            "country=%s date=%s",
            country_code,
            forecast_date,
        )

        raise WeatherSnowflakeLoaderError(
            "Unexpected error loading weather: "
            f"{country_code} {forecast_date}"
        ) from exc

    finally:
        if (
            should_close_connection
            and connection is not None
        ):
            connection.close()



def load_weather_records(
    country_codes: list[str],
    forecast_date: str,
    minio_client: Minio | None = None,
    connection: SnowflakeConnection | None = None,
) -> dict[str, Any]:
    """
    Load multiple cached weather records
    into Snowflake RAW_WEATHER.
    """
    if not isinstance(
        country_codes,
        list,
    ):
        raise TypeError(
            "country_codes must be a list."
        )

    if not country_codes:
        raise ValueError(
            "country_codes cannot be empty."
        )

    forecast_date = _validate_forecast_date(
        forecast_date
    )

    normalized_codes = []

    for country_code in country_codes:
        normalized_codes.append(
            _validate_country_code(
                country_code
            )
        )

    unique_codes = list(
        dict.fromkeys(normalized_codes)
    )

    logger.info(
        "Starting Snowflake RAW weather load. "
        "Requested=%d | Unique=%d",
        len(country_codes),
        len(unique_codes),
    )

    
    should_close_connection = connection is None

    successful = []
    failures = []

    try:
        if minio_client is None:
            minio_client = get_minio_client()

        if connection is None:
            connection = get_snowflake_connection()

        for country_code in unique_codes:
            try:
                record_id = load_weather_record(
                    country_code=country_code,
                    forecast_date=forecast_date,
                    minio_client=minio_client,
                    connection=connection,
                )

                successful.append(
                    {
                        "country": country_code,
                        "record_id": record_id,
                    }
                )

            except WeatherSnowflakeLoaderError as exc:
                failures.append(
                    {
                        "country": country_code,
                        "error": str(exc),
                    }
                )

                logger.exception(
                    "Weather load failed: "
                    "country=%s date=%s",
                    country_code,
                    forecast_date,
                )

        total_requested = len(
            country_codes
        )
        total_unique = len(
            unique_codes
        )
        total_succeeded = len(
            successful
        )
        total_failed = len(
            failures
        )

        if total_failed == 0:
            status = "SUCCESS"
        elif total_succeeded > 0:
            status = "PARTIAL_SUCCESS"
        else:
            status = "FAILED"

        summary = {
            "total_requested": total_requested,
            "total_unique": total_unique,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "successful_records": successful,
            "failures": failures,
            "forecast_date": forecast_date,
            "status": status,
        }

        logger.info(
            "Snowflake RAW weather load completed. "
            "Requested=%d | Unique=%d | "
            "Succeeded=%d | Failed=%d | Status=%s",
            total_requested,
            total_unique,
            total_succeeded,
            total_failed,
            status,
        )

        return summary

    finally:
        if (
            should_close_connection
            and connection is not None
        ):
            connection.close()

       