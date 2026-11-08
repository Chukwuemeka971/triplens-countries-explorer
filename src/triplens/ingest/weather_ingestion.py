import logging
import time
from datetime import date
from typing import Any

from minio import Minio

from triplens.storage.country_cache import CountryCacheError
from triplens.storage.minio_client import (
    get_minio_client,
    object_exists,
)
from triplens.storage.weather_cache import (
    WeatherCacheError,
    _get_weather_object_name,
    get_cached_weather,
)

logger = logging.getLogger(__name__)


class WeatherIngestionError(Exception):
    """Raised when weather ingestion fails."""


def _extract_country_metadata(
    country: dict[str, Any],
) -> tuple[str, float, float]:
    """
    Extract the country code and coordinates required
    for weather ingestion.
    """

    if not isinstance(country, dict):
        raise ValueError("Country record must be a dictionary.")

    names = country.get("names", {})
    codes = country.get("codes", {})
    coordinates = country.get("coordinates", {})

    if not isinstance(names, dict):
        raise ValueError("Country names must be a dictionary.")

    if not isinstance(codes, dict):
        raise ValueError("Country codes must be a dictionary.")

    if not isinstance(coordinates, dict):
        raise ValueError("Country coordinates must be a dictionary.")

    country_name = names.get("common")
    alpha_3 = codes.get("alpha_3")
    latitude = coordinates.get("lat")
    longitude = coordinates.get("lng")

    if not isinstance(country_name, str) or not country_name.strip():
        raise ValueError("Country is missing a valid common name.")

    if not isinstance(alpha_3, str) or not alpha_3.strip():
        raise ValueError(
            f"Country '{country_name}' is missing alpha_3 code."
        )

    if not isinstance(latitude, (int, float)):
        raise ValueError(
            f"Country '{country_name}' has an invalid latitude."
        )

    if not isinstance(longitude, (int, float)):
        raise ValueError(
            f"Country '{country_name}' has an invalid longitude."
        )

    if not -90 <= latitude <= 90:
        raise ValueError(
            f"Country '{country_name}' has an invalid latitude."
        )

    if not -180 <= longitude <= 180:
        raise ValueError(
            f"Country '{country_name}' has an invalid longitude."
        )

    return (
        alpha_3.strip().upper(),
        float(latitude),
        float(longitude),
    )


def ingest_weather_for_countries(
    country_records: list[dict[str, Any]],
    forecast_date: str | None = None,
    client: Minio | None = None,
) -> dict[str, Any]:
    """
    Ingest weather data for multiple countries.

    Weather data is retrieved from the MinIO cache when available.
    Otherwise, Open-Meteo is called and the response is cached.

    Parameters
    ----------
    country_records:
        Raw country records returned by the country API.

    forecast_date:
        Date used as the weather cache key.
        Defaults to today's date.

    client:
        Optional MinIO client, primarily useful for testing.

    Returns
    -------
    dict
        Ingestion summary containing success/failure and
        cache statistics.
    """

    start_time = time.perf_counter()

    if not isinstance(country_records, list):
        raise TypeError("country_records must be a list.")

    if not country_records:
        raise ValueError("country_records cannot be empty.")

    if forecast_date is None:
        forecast_date = date.today().isoformat()

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

    if client is None:
        client = get_minio_client()

    successful_records: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    cache_hits = 0
    cache_misses = 0

    processed_country_codes: set[str] = set()

    logger.info(
        "Starting weather ingestion. "
        "Requested countries: %d | Date: %s",
        len(country_records),
        forecast_date,
    )

    for country in country_records:

        try:
            country_code, latitude, longitude = (
                _extract_country_metadata(country)
            )

            if country_code in processed_country_codes:
                logger.info(
                    "Skipping duplicate country: %s",
                    country_code,
                )
                continue

            processed_country_codes.add(country_code)

            object_name = _get_weather_object_name(
                country_code=country_code,
                forecast_date=forecast_date,
            )

            cache_hit = object_exists(
                client,
                object_name,
            )

            if cache_hit:
                cache_hits += 1
            else:
                cache_misses += 1

            weather = get_cached_weather(
                country_code=country_code,
                latitude=latitude,
                longitude=longitude,
                forecast_date=forecast_date,
                client=client,
            )

            successful_records.append(
                {
                    "country_code": country_code,
                    "latitude": latitude,
                    "longitude": longitude,
                    "forecast_date": forecast_date,
                    "weather": weather,
                }
            )

            logger.info(
                "Weather ingestion successful: "
                "country=%s date=%s cache_hit=%s",
                country_code,
                forecast_date,
                cache_hit,
            )

        except (WeatherCacheError, CountryCacheError, ValueError) as exc:

            country_name = "unknown"

            if isinstance(country, dict):
                names = country.get("names", {})

                if isinstance(names, dict):
                    country_name = names.get(
                        "common",
                        "unknown",
                    )

            failures.append(
                {
                    "country": str(country_name),
                    "error": str(exc),
                }
            )

            logger.exception(
                "Weather ingestion failed: country=%s",
                country_name,
            )

        except Exception as exc:

            failures.append(
                {
                    "country": "unknown",
                    "error": str(exc),
                }
            )

            logger.exception(
                "Unexpected weather ingestion error."
            )

    duration_seconds = round(
        time.perf_counter() - start_time,
        4,
    )

    total_requested = len(country_records)
    total_unique = len(processed_country_codes)
    total_succeeded = len(successful_records)
    total_failed = len(failures)

    status = (
        "SUCCESS"
        if total_failed == 0
        else "PARTIAL_SUCCESS"
        if total_succeeded > 0
        else "FAILED"
    )

    summary = {
        "total_requested": total_requested,
        "total_unique": total_unique,
        "total_succeeded": total_succeeded,
        "total_failed": total_failed,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "forecast_date": forecast_date,
        "duration_seconds": duration_seconds,
        "status": status,
        "successful_records": successful_records,
        "failures": failures,
    }

    logger.info(
        "Weather ingestion completed. "
        "Requested=%d | Unique=%d | "
        "Succeeded=%d | Failed=%d | "
        "Cache hits=%d | Cache misses=%d | "
        "Duration=%.4fs | Status=%s",
        total_requested,
        total_unique,
        total_succeeded,
        total_failed,
        cache_hits,
        cache_misses,
        duration_seconds,
        status,
    )

    return summary