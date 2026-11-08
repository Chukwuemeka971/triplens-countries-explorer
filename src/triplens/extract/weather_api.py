import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_HOURLY_VARIABLES = (
    "temperature_2m,"
    "apparent_temperature,"
    "precipitation,"
    "rain,"
    "weather_code,"
    "wind_speed_10m"
)

DEFAULT_TIMEOUT = 30


class WeatherAPIError(Exception):
    """Raised when a weather API operation fails."""


def _create_session() -> requests.Session:
    """
    Create an HTTP session with retry behaviour for transient failures.
    """

    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def _validate_coordinates(
    latitude: float,
    longitude: float,
) -> None:
    """Validate geographical coordinates."""

    if not isinstance(latitude, (int, float)):
        raise ValueError("latitude must be a number.")

    if not isinstance(longitude, (int, float)):
        raise ValueError("longitude must be a number.")

    if not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90.")

    if not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180.")


def _validate_weather_response(data: Any) -> dict[str, Any]:
    """
    Validate the basic structure of an Open-Meteo response.
    """

    if not isinstance(data, dict):
        raise WeatherAPIError(
            "Weather API returned an invalid response type."
        )

    required_fields = (
        "latitude",
        "longitude",
        "timezone",
        "hourly",
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in data
    ]

    if missing_fields:
        raise WeatherAPIError(
            "Weather API response is missing required fields: "
            + ", ".join(missing_fields)
        )

    if not isinstance(data["hourly"], dict):
        raise WeatherAPIError(
            "Weather API hourly data has an invalid format."
        )

    if "time" not in data["hourly"]:
        raise WeatherAPIError(
            "Weather API hourly response is missing time data."
        )

    return data


def get_weather(
    latitude: float,
    longitude: float,
    *,
    timezone: str = "auto",
    forecast_days: int = 1,
    hourly_variables: str = DEFAULT_HOURLY_VARIABLES,
) -> dict[str, Any]:
    """
    Retrieve weather forecast data for a geographical location.

    Parameters
    ----------
    latitude:
        Latitude of the location.

    longitude:
        Longitude of the location.

    timezone:
        Timezone used by the API. "auto" lets Open-Meteo determine it.

    forecast_days:
        Number of forecast days to retrieve.

    hourly_variables:
        Comma-separated Open-Meteo hourly variables.

    Returns
    -------
    dict
        Validated Open-Meteo response.
    """

    _validate_coordinates(latitude, longitude)

    if not isinstance(timezone, str) or not timezone.strip():
        raise ValueError("timezone must be a non-empty string.")

    if not isinstance(forecast_days, int):
        raise ValueError("forecast_days must be an integer.")

    if not 1 <= forecast_days <= 16:
        raise ValueError(
            "forecast_days must be between 1 and 16."
        )

    if not isinstance(hourly_variables, str) or not hourly_variables.strip():
        raise ValueError(
            "hourly_variables must be a non-empty string."
        )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly_variables,
        "timezone": timezone,
        "forecast_days": forecast_days,
    }

    logger.info(
        "Requesting weather data. latitude=%s longitude=%s "
        "forecast_days=%s",
        latitude,
        longitude,
        forecast_days,
    )

    try:
        with _create_session() as session:
            response = session.get(
                OPEN_METEO_URL,
                params=params,
                timeout=DEFAULT_TIMEOUT,
            )

            response.raise_for_status()

            data = response.json()

    except requests.RequestException as exc:
        logger.exception(
            "Weather API request failed. latitude=%s longitude=%s",
            latitude,
            longitude,
        )

        raise WeatherAPIError(
            "Failed to retrieve weather data."
        ) from exc

    except ValueError as exc:
        logger.exception(
            "Weather API returned invalid JSON."
        )

        raise WeatherAPIError(
            "Weather API returned invalid JSON."
        ) from exc

    validated_data = _validate_weather_response(data)

    logger.info(
        "Weather API request successful. latitude=%s longitude=%s",
        latitude,
        longitude,
    )

    return validated_data