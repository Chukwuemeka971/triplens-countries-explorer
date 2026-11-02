
import pytest

from triplens.storage.country_cache import (
    CountryCacheError,
    get_cached_country,
    _get_cache_object_name,
)


def test_cache_object_name_is_deterministic():
    first = _get_cache_object_name("Nigeria")
    second = _get_cache_object_name("Nigeria")

    assert first == second


def test_cache_object_name_is_case_insensitive():
    first = _get_cache_object_name("Nigeria")
    second = _get_cache_object_name("NIGERIA")

    assert first == second


def test_cache_object_name_is_unique():
    nigeria = _get_cache_object_name("Nigeria")
    ghana = _get_cache_object_name("Ghana")

    assert nigeria != ghana


def test_empty_country_name_is_rejected():
    with pytest.raises(ValueError):
        _get_cache_object_name("")


def test_cached_country_returns_existing_data(monkeypatch):
    import triplens.storage.country_cache as cache

    expected_country = {
        "name": "Nigeria",
        "region": "Africa",
    }

    monkeypatch.setattr(
        cache,
        "object_exists",
        lambda client, object_name: True,
    )

    monkeypatch.setattr(
        cache,
        "download_json",
        lambda client, object_name: expected_country,
    )

    def api_should_not_be_called(country_name):
        pytest.fail("API should not be called on a cache hit")

    monkeypatch.setattr(
        cache,
        "get_country",
        api_should_not_be_called,
    )

    result = get_cached_country(
        "Nigeria",
        client=object(),
    )

    assert result == expected_country


def test_cache_miss_fetches_and_uploads_country(monkeypatch):
    import triplens.storage.country_cache as cache

    expected_country = {
        "name": "Ghana",
        "region": "Africa",
    }

    uploaded = {}

    monkeypatch.setattr(
        cache,
        "object_exists",
        lambda client, object_name: False,
    )

    monkeypatch.setattr(
        cache,
        "get_country",
        lambda country_name: expected_country,
    )

    def fake_upload(client, object_name, data):
        uploaded["object_name"] = object_name
        uploaded["data"] = data

    monkeypatch.setattr(
        cache,
        "upload_json",
        fake_upload,
    )

    result = get_cached_country(
        "Ghana",
        client=object(),
    )

    assert result == expected_country
    assert uploaded["data"] == expected_country
    assert uploaded["object_name"].startswith(
        "cache/countries/"
    )


def test_api_failure_is_wrapped(monkeypatch):
    import triplens.storage.country_cache as cache

    monkeypatch.setattr(
        cache,
        "object_exists",
        lambda client, object_name: False,
    )

    def failing_api(country_name):
        raise RuntimeError("API unavailable")

    monkeypatch.setattr(
        cache,
        "get_country",
        failing_api,
    )

    with pytest.raises(CountryCacheError):
        get_cached_country(
            "Nigeria",
            client=object(),
        )
