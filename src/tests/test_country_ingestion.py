
import pytest

import triplens.ingest.country_ingestion as ingestion
from triplens.storage.country_cache import CountryCacheError


def test_ingest_multiple_countries_successfully(monkeypatch):
    def fake_get_cached_country(country_name, client=None):
        return {
            "name": country_name,
            "region": "Africa",
        }

    monkeypatch.setattr(
        ingestion,
        "get_cached_country",
        fake_get_cached_country,
    )

    result = ingestion.ingest_countries(
        ["Nigeria", "Ghana", "Kenya"]
    )

    assert result["total_requested"] == 3
    assert result["total_unique"] == 3
    assert result["total_succeeded"] == 3
    assert result["total_failed"] == 0
    assert len(result["successful_records"]) == 3


def test_ingestion_continues_after_country_failure(monkeypatch):
    def fake_get_cached_country(country_name, client=None):
        if country_name == "Ghana":
            raise CountryCacheError("API unavailable")

        return {"name": country_name}

    monkeypatch.setattr(
        ingestion,
        "get_cached_country",
        fake_get_cached_country,
    )

    result = ingestion.ingest_countries(
        ["Nigeria", "Ghana", "Kenya"]
    )

    assert result["total_succeeded"] == 2
    assert result["total_failed"] == 1
    assert result["failures"][0]["country"] == "Ghana"


def test_duplicate_countries_are_removed(monkeypatch):
    processed = []

    def fake_get_cached_country(country_name, client=None):
        processed.append(country_name)
        return {"name": country_name}

    monkeypatch.setattr(
        ingestion,
        "get_cached_country",
        fake_get_cached_country,
    )

    result = ingestion.ingest_countries(
        ["Nigeria", "Ghana", "Nigeria"]
    )

    assert processed == ["Nigeria", "Ghana"]
    assert result["total_requested"] == 3
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 2


def test_empty_country_list_is_rejected():
    with pytest.raises(ValueError, match="cannot be empty"):
        ingestion.ingest_countries([])


def test_invalid_country_list_is_rejected():
    with pytest.raises(TypeError):
        ingestion.ingest_countries("Nigeria")


def test_empty_country_name_is_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        ingestion.ingest_countries(["Nigeria", " "])


def test_client_is_passed_to_cache(monkeypatch):
    expected_client = object()
    received_clients = []

    def fake_get_cached_country(country_name, client=None):
        received_clients.append(client)
        return {"name": country_name}

    monkeypatch.setattr(
        ingestion,
        "get_cached_country",
        fake_get_cached_country,
    )

    ingestion.ingest_countries(
        ["Nigeria"],
        client=expected_client,
    )

    assert received_clients == [expected_client]
