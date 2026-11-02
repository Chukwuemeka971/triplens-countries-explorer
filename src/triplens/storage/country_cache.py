
import hashlib
import logging

from minio import Minio

from triplens.extract.countries_api import get_country
from triplens.storage.minio_client import (
    download_json,
    get_minio_client,
    object_exists,
    upload_json,
)

logger = logging.getLogger(__name__)


class CountryCacheError(Exception):
    """Raised when a country cache operation fails."""


def _get_cache_object_name(country_name: str) -> str:
    """
    Generate a deterministic, safe MinIO object name.

    Hashing avoids problems with spaces, special characters
    and inconsistent country-name formatting.
    """
    if not country_name or not country_name.strip():
        raise ValueError("Country name cannot be empty.")

    normalized_name = country_name.strip().casefold()

    name_hash = hashlib.sha256(
        normalized_name.encode("utf-8")
    ).hexdigest()

    return f"cache/countries/{name_hash}.json"


def get_cached_country(
    country_name: str,
    client: Minio | None = None,
) -> dict:
    """
    Retrieve a country from MinIO if cached.

    If no cached response exists, retrieve it from the API,
    upload the response to MinIO and return it.
    """
    if not country_name or not country_name.strip():
        raise ValueError("Country name cannot be empty.")

    try:
        if client is None:
            client = get_minio_client()

        object_name = _get_cache_object_name(country_name)

        # 1. Check whether the country is already cached.
        if object_exists(client, object_name):
            logger.info(
                "Country cache hit: %s",
                country_name,
            )

            cached_country = download_json(
                client,
                object_name,
            )

            if not isinstance(cached_country, dict):
                raise CountryCacheError(
                    "Cached country response has an invalid format."
                )

            return cached_country

        # 2. Cache miss: retrieve country from the API.
        logger.info(
            "Country cache miss: fetching %s from API",
            country_name,
        )

        country = get_country(country_name)

        if not isinstance(country, dict):
            raise CountryCacheError(
                "Country API returned an invalid response."
            )

        # 3. Upload the individual country response to MinIO.
        upload_json(
            client,
            object_name,
            country,
        )

        logger.info(
            "Country cached successfully: %s",
            country_name,
        )

        return country

    except CountryCacheError:
        raise

    except Exception as exc:
        raise CountryCacheError(
            f"Failed to retrieve or cache country: {country_name}"
        ) from exc
