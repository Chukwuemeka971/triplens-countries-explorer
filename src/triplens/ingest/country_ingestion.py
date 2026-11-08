import logging
import time
from typing import Any

from minio import Minio

from triplens.storage.country_cache import (
    CountryCacheError,
    get_country_with_cache_status,
)

logger = logging.getLogger(__name__)


def ingest_countries(
    country_names: list[str],
    client: Minio | None = None,
) -> dict[str, Any]:
    """
    Ingest a controlled list of countries through the cache layer.

    A failure for one country does not stop the remaining countries.

    Returns a summary containing:
    - requested and unique country counts
    - successful and failed counts
    - cache hits and misses
    - processing duration
    - successful records
    - failure details
    """
    if not isinstance(country_names, list):
        raise TypeError("country_names must be a list.")

    if not country_names:
        raise ValueError("country_names cannot be empty.")

    if any(
        not isinstance(name, str) or not name.strip()
        for name in country_names
    ):
        raise ValueError(
            "Every country name must be a non-empty string."
        )

    # Remove duplicate country names while preserving order.
    unique_names = list(
        dict.fromkeys(
            name.strip()
            for name in country_names
        )
    )

    successful_records = []
    failures = []

    cache_hits = 0
    cache_misses = 0

    start_time = time.perf_counter()

    logger.info(
        "Starting country ingestion. Requested: %d, unique: %d",
        len(country_names),
        len(unique_names),
    )

    for country_name in unique_names:
        try:
            country, cache_status = get_country_with_cache_status(
                country_name,
                client=client,
            )

            successful_records.append(country)

            if cache_status == "hit":
                cache_hits += 1
            elif cache_status == "miss":
                cache_misses += 1
            else:
                raise CountryCacheError(
                    f"Unexpected cache status: {cache_status}"
                )

            logger.info(
                "Country ingestion successful: %s | cache=%s",
                country_name,
                cache_status,
            )

        except CountryCacheError as exc:
            failures.append(
                {
                    "country": country_name,
                    "error": str(exc),
                }
            )

            logger.exception(
                "Country ingestion failed: %s",
                country_name,
            )

        except Exception as exc:
            failures.append(
                {
                    "country": country_name,
                    "error": str(exc),
                }
            )

            logger.exception(
                "Unexpected ingestion error: %s",
                country_name,
            )

    duration_seconds = time.perf_counter() - start_time

    total_requested = len(country_names)
    total_unique = len(unique_names)
    total_succeeded = len(successful_records)
    total_failed = len(failures)

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
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "duration_seconds": round(duration_seconds, 4),
        "status": status,
        "successful_records": successful_records,
        "failures": failures,
    }

    logger.info(
        "Country ingestion completed. "
        "Unique: %d | Succeeded: %d | Failed: %d | "
        "Cache hits: %d | Cache misses: %d | "
        "Duration: %.4fs | Status: %s",
        total_unique,
        total_succeeded,
        total_failed,
        cache_hits,
        cache_misses,
        duration_seconds,
        status,
    )

    return summary
