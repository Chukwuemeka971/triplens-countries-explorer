import json
from unittest.mock import MagicMock, patch

import pytest

from triplens.load.weather_snowflake_loader import (
    WeatherSnowflakeLoaderError,
    _build_record_id,
    _build_weather_object_name,
    _validate_country_code,
    _validate_forecast_date,
    _validate_weather_payload,
    load_weather_record,
    load_weather_records,
)


def valid_weather_payload():
    return {
        "latitude": 51.5,
        "longitude": -0.1,
        "timezone": "Europe/London",
        "hourly": {
            "time": [
                "2026-11-08T00:00"
            ],
            "temperature_2m": [
                8.2
            ],
        },
    }


def test_validate_country_code_returns_uppercase():
    result = _validate_country_code("gbr")

    assert result == "GBR"


def test_validate_country_code_rejects_invalid_code():
    with pytest.raises(
        WeatherSnowflakeLoaderError,
        match="Invalid country code",
    ):
        _validate_country_code("GB")


def test_validate_country_code_rejects_non_string():
    with pytest.raises(
        WeatherSnowflakeLoaderError,
        match="Country code must be a string",
    ):
        _validate_country_code(123)


def test_validate_forecast_date_returns_iso_date():
    result = _validate_forecast_date(
        "2026-11-08"
    )

    assert result == "2026-11-08"


def test_validate_forecast_date_rejects_invalid_date():
    with pytest.raises(
        WeatherSnowflakeLoaderError,
        match="Invalid forecast date",
    ):
        _validate_forecast_date(
            "08-11-2026"
        )


def test_build_weather_object_name():
    result = _build_weather_object_name(
        "gbr",
        "2026-11-08",
    )

    assert (
        result
        == "raw/weather/GBR/2026-11-08.json"
    )


def test_build_record_id_is_deterministic():
    first = _build_record_id(
        "GBR",
        "2026-11-08",
    )

    second = _build_record_id(
        "GBR",
        "2026-11-08",
    )

    assert first == second


def test_build_record_id_changes_for_different_dates():
    first = _build_record_id(
        "GBR",
        "2026-11-08",
    )

    second = _build_record_id(
        "GBR",
        "2026-11-09",
    )

    assert first != second


def test_validate_weather_payload_accepts_valid_payload():
    payload = valid_weather_payload()

    _validate_weather_payload(payload)


def test_validate_weather_payload_rejects_non_dictionary():
    with pytest.raises(
        WeatherSnowflakeLoaderError,
        match="Weather payload must be a dictionary",
    ):
        _validate_weather_payload([])


def test_validate_weather_payload_requires_hourly_data():
    payload = valid_weather_payload()
    del payload["hourly"]

    with pytest.raises(
        WeatherSnowflakeLoaderError,
        match="missing hourly data",
    ):
        _validate_weather_payload(payload)


def test_load_weather_record_success():
    minio_client = MagicMock()
    connection = MagicMock()
    cursor = MagicMock()

    minio_client.bucket_exists.return_value = True
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch(
        "triplens.load.weather_snowflake_loader.object_exists",
        return_value=True,
    ), patch(
        "triplens.load.weather_snowflake_loader.download_json",
        return_value=valid_weather_payload(),
    ):
        record_id = load_weather_record(
            country_code="GBR",
            forecast_date="2026-11-08",
            minio_client=minio_client,
            connection=connection,
        )

    assert isinstance(record_id, str)
    assert len(record_id) == 64

    cursor.execute.assert_called_once()
    connection.commit.assert_called_once()


def test_load_weather_record_rolls_back_on_failure():
    minio_client = MagicMock()
    connection = MagicMock()

    with patch(
        "triplens.load.weather_snowflake_loader.object_exists",
        return_value=True,
    ), patch(
        "triplens.load.weather_snowflake_loader.download_json",
        side_effect=Exception("MinIO failure"),
    ):
        with pytest.raises(
            WeatherSnowflakeLoaderError
        ):
            load_weather_record(
                country_code="GBR",
                forecast_date="2026-11-08",
                minio_client=minio_client,
                connection=connection,
            )

    connection.rollback.assert_called_once()


def test_load_weather_records_deduplicates_country_codes():
    with patch(
        "triplens.load.weather_snowflake_loader.load_weather_record",
        side_effect=[
            "record-gbr",
            "record-nga",
        ],
    ) as mocked_loader:

        result = load_weather_records(
            country_codes=[
                "GBR",
                "NGA",
                "GBR",
            ],
            forecast_date="2026-11-08",
            minio_client=MagicMock(),
            connection=MagicMock(),
        )

    assert result["total_requested"] == 3
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 2
    assert result["total_failed"] == 0
    assert result["status"] == "SUCCESS"

    assert mocked_loader.call_count == 2


def test_load_weather_records_handles_partial_failure():
    with patch(
        "triplens.load.weather_snowflake_loader.load_weather_record",
        side_effect=[
            "record-gbr",
            WeatherSnowflakeLoaderError(
                "Snowflake failure"
            ),
        ],
    ):

        result = load_weather_records(
            country_codes=[
                "GBR",
                "NGA",
            ],
            forecast_date="2026-11-08",
            minio_client=MagicMock(),
            connection=MagicMock(),
        )

    assert result["total_requested"] == 2
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 1
    assert result["status"] == "PARTIAL_SUCCESS"
    assert len(result["failures"]) == 1