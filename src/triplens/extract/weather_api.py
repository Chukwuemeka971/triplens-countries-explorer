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
    Validate the structural integrity of an Open-Meteo response.

    The validation checks:
    - response type
    - required top-level fields
    - hourly data structure
    - required hourly variables
    - non-empty observations
    - consistent hourly array lengths
    - valid latitude and longitude
    """

    # -----------------------------------------------------
    # RESPONSE TYPE
    # -----------------------------------------------------

    if not isinstance(data, dict):
        raise WeatherAPIError(
            "Weather API returned an invalid response type."
        )

    # -----------------------------------------------------
    # REQUIRED TOP-LEVEL FIELDS
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # COORDINATE VALIDATION
    # -----------------------------------------------------

    latitude = data["latitude"]
    longitude = data["longitude"]

    if not isinstance(latitude, (int, float)):
        raise WeatherAPIError(
            "Weather API response contains an invalid latitude."
        )

    if not isinstance(longitude, (int, float)):
        raise WeatherAPIError(
            "Weather API response contains an invalid longitude."
        )

    if not -90 <= latitude <= 90:
        raise WeatherAPIError(
            "Weather API response latitude is outside "
            "the valid range."
        )

    if not -180 <= longitude <= 180:
        raise WeatherAPIError(
            "Weather API response longitude is outside "
            "the valid range."
        )

    # -----------------------------------------------------
    # TIMEZONE VALIDATION
    # -----------------------------------------------------

    timezone = data["timezone"]

    if not isinstance(timezone, str) or not timezone.strip():
        raise WeatherAPIError(
            "Weather API response contains an invalid timezone."
        )

    # -----------------------------------------------------
    # HOURLY OBJECT
    # -----------------------------------------------------

    hourly = data["hourly"]

    if not isinstance(hourly, dict):
        raise WeatherAPIError(
            "Weather API hourly data has an invalid format."
        )

    # -----------------------------------------------------
    # REQUIRED HOURLY VARIABLES
    # -----------------------------------------------------

    required_hourly_variables = {
        "time",
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "weather_code",
        "wind_speed_10m",
    }

    missing_hourly_variables = [
        variable
        for variable in required_hourly_variables
        if variable not in hourly
    ]

    if missing_hourly_variables:
        raise WeatherAPIError(
            "Weather API response is missing required "
            "hourly variables: "
            + ", ".join(
                sorted(missing_hourly_variables)
            )
        )

    # -----------------------------------------------------
    # TIME OBSERVATIONS
    # -----------------------------------------------------

    time_values = hourly["time"]

    if not isinstance(time_values, list):
        raise WeatherAPIError(
            "Weather API hourly time values must be a list."
        )

    if not time_values:
        raise WeatherAPIError(
            "Weather API returned no hourly observations."
        )

    observation_count = len(time_values)

    # -----------------------------------------------------
    # HOURLY ARRAY VALIDATION
    # -----------------------------------------------------

    for variable in required_hourly_variables:

        values = hourly[variable]

        if not isinstance(values, list):
            raise WeatherAPIError(
                "Weather API hourly variable "
                f"'{variable}' must be a list."
            )

        if len(values) != observation_count:
            raise WeatherAPIError(
                "Weather API hourly arrays have "
                f"inconsistent lengths: "
                f"'{variable}' has {len(values)} values, "
                f"but time has {observation_count}."
            )

    # -----------------------------------------------------
    # SOURCE QUALITY PASSED
    # -----------------------------------------------------

    logger.info(
        "Weather source quality checks passed. "
        "latitude=%s longitude=%s observations=%s",
        latitude,
        longitude,
        observation_count,
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
        raise ValueError(
            "timezone must be a non-empty string."
        )

    if not isinstance(forecast_days, int):
        raise ValueError(
            "forecast_days must be an integer."
        )

    if not 1 <= forecast_days <= 16:
        raise ValueError(
            "forecast_days must be between 1 and 16."
        )

    if (
        not isinstance(hourly_variables, str)
        or not hourly_variables.strip()
    ):
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
            "Weather API request failed. "
            "latitude=%s longitude=%s",
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

    # -----------------------------------------------------
    # SOURCE DATA QUALITY GATE
    # -----------------------------------------------------

    validated_data = _validate_weather_response(data)

    logger.info(
        "Weather API request successful. "
        "latitude=%s longitude=%s",
        latitude,
        longitude,
    )

    return validated_data
