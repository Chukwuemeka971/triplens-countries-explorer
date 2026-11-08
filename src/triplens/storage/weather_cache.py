import logging
from datetime import date

from minio import Minio

from triplens.extract.weather_api import get_weather
from triplens.storage.minio_client import (
    download_json,
    get_minio_client,
    object_exists,
    upload_json,
)

logger = logging.getLogger(__name__)


class WeatherCacheError(Exception):
    """Raised when a weather cache operation fails."""


def _get_weather_object_name(
    country_code: str,
    forecast_date: str,
) -> str:
    """
    Build a deterministic MinIO object name for weather data.
    """

    if not isinstance(country_code, str) or not country_code.strip():
        raise ValueError(
            "country_code must be a non-empty string."
        )

    if not isinstance(forecast_date, str) or not forecast_date.strip():
        raise ValueError(
            "forecast_date must be a non-empty string."
        )

    try:
        date.fromisoformat(forecast_date)
    except ValueError as exc:
        raise ValueError(
            "forecast_date must use YYYY-MM-DD format."
        ) from exc

    normalized_country_code = country_code.strip().upper()

    return (
        f"raw/weather/"
        f"{normalized_country_code}/"
        f"{forecast_date}.json"
    )


def get_cached_weather(
    country_code: str,
    latitude: float,
    longitude: float,
    forecast_date: str,
    client: Minio | None = None,
) -> dict:
    """
    Retrieve weather data from MinIO cache or Open-Meteo.

    The cache is keyed by country code and forecast date.
    """

    if client is None:
        client = get_minio_client()

    object_name = _get_weather_object_name(
        country_code=country_code,
        forecast_date=forecast_date,
    )

    try:
        if object_exists(client, object_name):
            logger.info(
                "Weather cache hit: country=%s date=%s",
                country_code,
                forecast_date,
            )

            cached_weather = download_json(
                client,
                object_name,
            )

            if not isinstance(cached_weather, dict):
                raise WeatherCacheError(
                    "Cached weather response has an invalid format."
                )

            return cached_weather

        logger.info(
            "Weather cache miss: country=%s date=%s",
            country_code,
            forecast_date,
        )

        weather = get_weather(
            latitude=latitude,
            longitude=longitude,
            timezone="auto",
            forecast_days=1,
        )

        if not isinstance(weather, dict):
            raise WeatherCacheError(
                "Weather API returned an invalid response."
            )

        upload_json(
            client,
            object_name,
            weather,
        )

        logger.info(
            "Weather data cached successfully: "
            "country=%s date=%s",
            country_code,
            forecast_date,
        )

        return weather

    except WeatherCacheError:
        raise

    except Exception as exc:
        logger.exception(
            "Failed to retrieve or cache weather data: "
            "country=%s date=%s",
            country_code,
            forecast_date,
        )

        raise WeatherCacheError(
            "Failed to retrieve or cache weather data."
        ) from exc