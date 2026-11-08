import pytest

from triplens.ingest.weather_ingestion import (
    ingest_weather_for_countries,
)
from triplens.storage.weather_cache import WeatherCacheError


def make_country(
    name: str = "Nigeria",
    alpha_3: str = "NGA",
    latitude: float = 9.0,
    longitude: float = 8.0,
) -> dict:
    """Create a minimal valid country record for testing."""

    return {
        "names": {
            "common": name,
        },
        "codes": {
            "alpha_3": alpha_3,
        },
        "coordinates": {
            "lat": latitude,
            "lng": longitude,
        },
    }


def test_weather_ingestion_success(monkeypatch):
    """A valid country should be ingested successfully."""

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: False,
    )

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.get_cached_weather",
        lambda **kwargs: {
            "latitude": kwargs["latitude"],
            "longitude": kwargs["longitude"],
            "hourly": {
                "time": [
                    "2026-11-07T00:00"
                ]
            },
        },
    )

    countries = [
        make_country(),
    ]

    result = ingest_weather_for_countries(
        country_records=countries,
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_requested"] == 1
    assert result["total_unique"] == 1
    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 0
    assert result["cache_hits"] == 0
    assert result["cache_misses"] == 1
    assert result["status"] == "SUCCESS"


def test_weather_ingestion_cache_hit(monkeypatch):
    """An existing cache object should be recorded as a cache hit."""

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: True,
    )

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.get_cached_weather",
        lambda **kwargs: {
            "latitude": kwargs["latitude"],
            "longitude": kwargs["longitude"],
            "hourly": {
                "time": [
                    "2026-11-07T00:00"
                ]
            },
        },
    )

    result = ingest_weather_for_countries(
        country_records=[make_country()],
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 0
    assert result["cache_hits"] == 1
    assert result["cache_misses"] == 0
    assert result["status"] == "SUCCESS"


def test_weather_ingestion_deduplicates_countries(monkeypatch):
    """Duplicate country codes should only be processed once."""

    calls = []

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: False,
    )

    def fake_get_cached_weather(**kwargs):
        calls.append(kwargs["country_code"])

        return {
            "latitude": kwargs["latitude"],
            "longitude": kwargs["longitude"],
            "hourly": {
                "time": [
                    "2026-11-07T00:00"
                ]
            },
        }

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.get_cached_weather",
        fake_get_cached_weather,
    )

    countries = [
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
        make_country(
            name="Nigeria Duplicate",
            alpha_3="NGA",
        ),
    ]

    result = ingest_weather_for_countries(
        country_records=countries,
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_requested"] == 2
    assert result["total_unique"] == 1
    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 0
    assert calls == ["NGA"]


def test_weather_ingestion_invalid_coordinates(monkeypatch):
    """Invalid coordinates should produce a controlled failure."""

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: False,
    )

    countries = [
        make_country(
            latitude=100.0,
            longitude=8.0,
        ),
    ]

    result = ingest_weather_for_countries(
        country_records=countries,
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_requested"] == 1
    assert result["total_unique"] == 0
    assert result["total_succeeded"] == 0
    assert result["total_failed"] == 1
    assert result["status"] == "FAILED"


def test_weather_ingestion_handles_weather_failure(monkeypatch):
    """A weather API/cache failure should not crash the batch."""

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: False,
    )

    def fake_get_cached_weather(**kwargs):
        raise WeatherCacheError(
            "Weather service unavailable."
        )

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.get_cached_weather",
        fake_get_cached_weather,
    )

    result = ingest_weather_for_countries(
        country_records=[make_country()],
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_requested"] == 1
    assert result["total_unique"] == 1
    assert result["total_succeeded"] == 0
    assert result["total_failed"] == 1
    assert result["status"] == "FAILED"
    assert len(result["failures"]) == 1


def test_weather_ingestion_partial_success(monkeypatch):
    """Successful and failed countries should produce PARTIAL_SUCCESS."""

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.object_exists",
        lambda client, object_name: False,
    )

    def fake_get_cached_weather(**kwargs):

        if kwargs["country_code"] == "NGA":
            raise WeatherCacheError(
                "Weather service unavailable."
            )

        return {
            "latitude": kwargs["latitude"],
            "longitude": kwargs["longitude"],
            "hourly": {
                "time": [
                    "2026-11-07T00:00"
                ]
            },
        }

    monkeypatch.setattr(
        "triplens.ingest.weather_ingestion.get_cached_weather",
        fake_get_cached_weather,
    )

    countries = [
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
        make_country(
            name="Ghana",
            alpha_3="GHA",
            latitude=8.0,
            longitude=-1.0,
        ),
    ]

    result = ingest_weather_for_countries(
        country_records=countries,
        forecast_date="2026-11-07",
        client=object(),
    )

    assert result["total_requested"] == 2
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 1
    assert result["status"] == "PARTIAL_SUCCESS"


def test_weather_ingestion_rejects_empty_records():

    with pytest.raises(ValueError, match="country_records cannot be empty"):
        ingest_weather_for_countries(
            country_records=[],
            forecast_date="2026-11-07",
            client=object(),
        )


def test_weather_ingestion_rejects_invalid_date():

    with pytest.raises(
        ValueError,
        match="forecast_date must use YYYY-MM-DD format",
    ):
        ingest_weather_for_countries(
            country_records=[make_country()],
            forecast_date="07-11-2026",
            client=object(),
        )