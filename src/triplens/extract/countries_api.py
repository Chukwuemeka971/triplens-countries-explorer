
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv(
    "REST_COUNTRIES_BASE_URL",
    "https://api.restcountries.com/countries/v5",
)
API_KEY = os.getenv("REST_COUNTRIES_API_KEY")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 4
REQUEST_TIMEOUT = 30

RESPONSE_FIELDS = (
    "names.common,"
    "names.official,"
    "codes.alpha_2,"
    "codes.alpha_3,"
    "region,"
    "subregion,"
    "capitals,"
    "languages,"
    "currencies,"
    "borders,"
    "timezones,"
    "population,"
    "area.kilometers,"
    "coordinates.lat,"
    "coordinates.lng,"
    "flag.emoji"
)


class CountriesAPIError(Exception):
    """Raised when the Countries API request fails."""


def _retry_delay(response, attempt: int) -> float:
    """Calculate a bounded retry delay using Retry-After if available."""

    retry_after = response.headers.get("Retry-After")

    if retry_after:
        try:
            return min(max(float(retry_after), 0), 60)

        except ValueError:
            try:
                retry_time = parsedate_to_datetime(retry_after)

                if retry_time.tzinfo is None:
                    retry_time = retry_time.replace(
                        tzinfo=timezone.utc
                    )

                delay = (
                    retry_time - datetime.now(timezone.utc)
                ).total_seconds()

                return min(max(delay, 0), 60)

            except (TypeError, ValueError, OverflowError):
                pass

    return min(2**attempt, 30)


def _get_json(
    url: str,
    params: dict | None = None,
) -> dict:
    """Perform an authenticated GET request with bounded retries."""

    if not API_KEY:
        raise CountriesAPIError(
            "REST_COUNTRIES_API_KEY is not configured."
        )

    for attempt in range(MAX_ATTEMPTS):
        response = None

        try:
            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                },
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:

                if attempt == MAX_ATTEMPTS - 1:
                    raise CountriesAPIError(
                        f"API returned HTTP {response.status_code} "
                        f"after {MAX_ATTEMPTS} attempts."
                    )

                delay = _retry_delay(response, attempt)

                logger.warning(
                    "Retryable HTTP %s; attempt %s/%s; "
                    "waiting %.1f seconds",
                    response.status_code,
                    attempt + 1,
                    MAX_ATTEMPTS,
                    delay,
                )

                time.sleep(delay)
                continue

            response.raise_for_status()

            try:
                payload = response.json()

            except ValueError as exc:
                raise CountriesAPIError(
                    "API returned invalid JSON."
                ) from exc

            if not isinstance(payload, dict):
                raise CountriesAPIError(
                    "API response must be a JSON object."
                )

            if payload.get("errors"):
                raise CountriesAPIError(
                    str(payload["errors"])
                )

            return payload

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as exc:

            if attempt == MAX_ATTEMPTS - 1:
                raise CountriesAPIError(
                    "API request failed after repeated network errors."
                ) from exc

            delay = min(2**attempt, 30)

            logger.warning(
                "Network error; attempt %s/%s; "
                "waiting %.1f seconds",
                attempt + 1,
                MAX_ATTEMPTS,
                delay,
            )

            time.sleep(delay)

        except requests.HTTPError as exc:

            status = (
                response.status_code
                if response is not None
                else "unknown"
            )

            raise CountriesAPIError(
                f"API request returned HTTP {status}."
            ) from exc

        except requests.RequestException as exc:
            raise CountriesAPIError(
                "Unexpected API transport error."
            ) from exc

    raise CountriesAPIError("API request failed.")


def _validate_country_record(country: Any) -> str:
    """
    Validate a country record and generate a stable identifier.

    ISO alpha-3 is preferred. If unavailable, use the common name
    as a fallback identifier without inventing an ISO code.
    """

    if not isinstance(country, dict):
        raise CountriesAPIError(
            "Invalid country record: expected an object, "
            f"got {type(country).__name__}."
        )

    names = country.get("names")

    if not isinstance(names, dict):
        raise CountriesAPIError(
            "Country record is missing a valid names object."
        )

    common_name = names.get("common")

    if (
        not isinstance(common_name, str)
        or not common_name.strip()
    ):
        raise CountriesAPIError(
            "Country record is missing a valid common name."
        )

    codes = country.get("codes")

    if not isinstance(codes, dict):
        raise CountriesAPIError(
            f"Invalid codes object for {common_name}."
        )

    alpha_3 = codes.get("alpha_3")

    if isinstance(alpha_3, str) and alpha_3.strip():

        identifier = f"ISO3:{alpha_3.strip().upper()}"

        return identifier

    logger.warning(
        "Country/territory '%s' has no ISO alpha-3 code. "
        "Using its common name as a fallback identifier.",
        common_name,
    )

    return f"NAME:{common_name.strip().casefold()}"


def get_country(name: str) -> dict:
    """Retrieve a country by its common name."""

    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            "Country name must not be empty."
        )

    url = (
        f"{BASE_URL}/names.common/"
        f"{requests.utils.quote(name.strip(), safe='')}"
    )

    payload = _get_json(url)

    data = payload.get("data")

    if not isinstance(data, dict):
        raise CountriesAPIError(
            "Invalid API response: data must be an object."
        )

    objects = data.get("objects")

    if not isinstance(objects, list) or not objects:
        raise CountriesAPIError(
            f"No country records returned for {name!r}."
        )

    country = objects[0]

    if not isinstance(country, dict):
        raise CountriesAPIError(
            "Invalid country record: expected an object."
        )

    logger.info(
        "Successfully retrieved country: %s",
        name,
    )

    return country


def get_all_countries(
    page_size: int = 100,
) -> list[dict]:
    """Retrieve country records using API pagination."""

    if (
        not isinstance(page_size, int)
        or isinstance(page_size, bool)
    ):
        raise ValueError(
            "page_size must be an integer."
        )

    if not 1 <= page_size <= 100:
        raise ValueError(
            "page_size must be between 1 and 100."
        )

    countries = []
    seen_identifiers = set()
    offset = 0

    while True:

        payload = _get_json(
            BASE_URL,
            params={
                "limit": page_size,
                "offset": offset,
                "response_fields": RESPONSE_FIELDS,
            },
        )

        data = payload.get("data")

        if not isinstance(data, dict):
            raise CountriesAPIError(
                "Invalid API response: data must be an object."
            )

        page = data.get("objects")
        meta = data.get("meta")

        if (
            not isinstance(page, list)
            or not isinstance(meta, dict)
        ):
            raise CountriesAPIError(
                "Invalid API response: expected data.objects "
                "and data.meta."
            )

        more = meta.get("more")

        if not isinstance(more, bool):
            raise CountriesAPIError(
                "Invalid pagination metadata: "
                "'more' must be a boolean."
            )

        if not page:

            if more:
                raise CountriesAPIError(
                    "API reports more records but returned "
                    "an empty page."
                )

            break

        for country in page:

            identifier = _validate_country_record(country)

            if identifier in seen_identifiers:
                raise CountriesAPIError(
                    "Duplicate country identifier detected: "
                    f"{identifier}"
                )

            seen_identifiers.add(identifier)
            countries.append(country)

        logger.info(
            "Retrieved page: count=%s, offset=%s, total=%s",
            len(page),
            offset,
            meta.get("total"),
        )

        offset += len(page)

        if not more:
            break

    logger.info(
        "Country extraction complete: %s records",
        len(countries),
    )

    return countries
