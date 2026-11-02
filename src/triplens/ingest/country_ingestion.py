
import logging
from typing import Any

from minio import Minio

from triplens.storage.country_cache import (
    CountryCacheError,
    get_cached_country,
)

logger = logging.getLogger(__name__)


def ingest_countries(
    country_names: list[str],
    client: Minio | None = None,
) -> dict[str, Any]:
    """
    Ingest a controlled list of countries through the cache layer.

    A failure for one country does not stop the remaining countries.

    Returns a summary containing successful records and failures.
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

    # Remove duplicate country names without changing their order.
    unique_names = list(
        dict.fromkeys(name.strip() for name in country_names)
    )

    successful_records = []
    failures = []

    logger.info(
        "Starting country ingestion. Requested: %d, unique: %d",
        len(country_names),
        len(unique_names),
    )

    for country_name in unique_names:
        try:
            country = get_cached_country(
                country_name,
                client=client,
            )

            successful_records.append(country)

            logger.info(
                "Country ingestion successful: %s",
                country_name,
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

    total_requested = len(country_names)
    total_unique = len(unique_names)
    total_succeeded = len(successful_records)
    total_failed = len(failures)

    summary = {
        "total_requested": total_requested,
        "total_unique": total_unique,
        "total_succeeded": total_succeeded,
        "total_failed": total_failed,
        "successful_records": successful_records,
        "failures": failures,
    }

    logger.info(
        "Country ingestion completed. "
        "Unique: %d | Succeeded: %d | Failed: %d",
        total_unique,
        total_succeeded,
        total_failed,
    )

    return summary
