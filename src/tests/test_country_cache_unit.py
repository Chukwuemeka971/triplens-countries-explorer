import hashlib
from unittest.mock import MagicMock

import pytest

import triplens.storage.country_cache as cache


class TestGetCacheObjectName:
    def test_generates_deterministic_hash_path(self):
        result = cache._get_cache_object_name("Nigeria")

        expected_hash = hashlib.sha256(
            "nigeria".encode("utf-8")
        ).hexdigest()

        assert result == (
            f"cache/countries/{expected_hash}.json"
        )

    def test_normalizes_country_name(self):
        result = cache._get_cache_object_name(
            "  Nigeria  "
        )

        expected_hash = hashlib.sha256(
            "nigeria".encode("utf-8")
        ).hexdigest()

        assert result == (
            f"cache/countries/{expected_hash}.json"
        )

    def test_country_name_is_case_insensitive(self):
        lower = cache._get_cache_object_name("nigeria")
        upper = cache._get_cache_object_name("NIGERIA")

        assert lower == upper

    @pytest.mark.parametrize(
        "country_name",
        ["", "   ", None],
    )
    def test_rejects_empty_country_name(self, country_name):
        with pytest.raises(
            ValueError,
            match="Country name cannot be empty",
        ):
            cache._get_cache_object_name(country_name)


class TestGetCachedCountry:
    def test_returns_cached_country_on_cache_hit(self):
        client = MagicMock()

        cached_country = {
            "name": {
                "common": "Nigeria",
                "official": "Federal Republic of Nigeria",
            },
            "cca3": "NGA",
        }

        object_exists_mock = MagicMock(
            return_value=True
        )
        download_json_mock = MagicMock(
            return_value=cached_country
        )

        original_object_exists = cache.object_exists
        original_download_json = cache.download_json

        cache.object_exists = object_exists_mock
        cache.download_json = download_json_mock

        try:
            result = cache.get_cached_country(
                country_name="Nigeria",
                client=client,
            )

            assert result == cached_country

            expected_object_name = (
                cache._get_cache_object_name("Nigeria")
            )

            object_exists_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

            download_json_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

        finally:
            cache.object_exists = original_object_exists
            cache.download_json = original_download_json

    def test_fetches_and_caches_country_on_cache_miss(self):
        client = MagicMock()

        country = {
            "name": {
                "common": "Nigeria",
                "official": "Federal Republic of Nigeria",
            },
            "cca3": "NGA",
        }

        original_object_exists = cache.object_exists
        original_get_country = cache.get_country
        original_upload_json = cache.upload_json

        object_exists_mock = MagicMock(
            return_value=False
        )
        get_country_mock = MagicMock(
            return_value=country
        )
        upload_json_mock = MagicMock()

        cache.object_exists = object_exists_mock
        cache.get_country = get_country_mock
        cache.upload_json = upload_json_mock

        try:
            result = cache.get_cached_country(
                country_name="Nigeria",
                client=client,
            )

            assert result == country

            expected_object_name = (
                cache._get_cache_object_name("Nigeria")
            )

            object_exists_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

            get_country_mock.assert_called_once_with(
                "Nigeria"
            )

            upload_json_mock.assert_called_once_with(
                client,
                expected_object_name,
                country,
            )

        finally:
            cache.object_exists = original_object_exists
            cache.get_country = original_get_country
            cache.upload_json = original_upload_json

    def test_rejects_invalid_cached_country_format(self):
        client = MagicMock()

        original_object_exists = cache.object_exists
        original_download_json = cache.download_json

        cache.object_exists = MagicMock(
            return_value=True
        )
        cache.download_json = MagicMock(
            return_value=["invalid", "country"]
        )

        try:
            with pytest.raises(
                cache.CountryCacheError,
                match=(
                    "Cached country response has "
                    "an invalid format"
                ),
            ):
                cache.get_cached_country(
                    country_name="Nigeria",
                    client=client,
                )

        finally:
            cache.object_exists = original_object_exists
            cache.download_json = original_download_json

    def test_rejects_invalid_country_api_response(self):
        client = MagicMock()

        original_object_exists = cache.object_exists
        original_get_country = cache.get_country
        original_upload_json = cache.upload_json

        cache.object_exists = MagicMock(
            return_value=False
        )

        get_country_mock = MagicMock(
            return_value=["invalid", "response"]
        )

        upload_json_mock = MagicMock()

        cache.get_country = get_country_mock
        cache.upload_json = upload_json_mock

        try:
            with pytest.raises(
                cache.CountryCacheError,
                match=(
                    "Country API returned an invalid response"
                ),
            ):
                cache.get_cached_country(
                    country_name="Nigeria",
                    client=client,
                )

            upload_json_mock.assert_not_called()

        finally:
            cache.object_exists = original_object_exists
            cache.get_country = original_get_country
            cache.upload_json = original_upload_json

    def test_creates_minio_client_when_client_is_not_provided(
        self,
        monkeypatch,
    ):
        fake_client = MagicMock()

        get_minio_client_mock = MagicMock(
            return_value=fake_client
        )

        monkeypatch.setattr(
            cache,
            "get_minio_client",
            get_minio_client_mock,
        )

        monkeypatch.setattr(
            cache,
            "object_exists",
            MagicMock(return_value=True),
        )

        cached_country = {
            "name": {
                "common": "Nigeria"
            }
        }

        monkeypatch.setattr(
            cache,
            "download_json",
            MagicMock(return_value=cached_country),
        )

        result = cache.get_cached_country(
            country_name="Nigeria"
        )

        assert result == cached_country

        get_minio_client_mock.assert_called_once_with()

    def test_wraps_unexpected_error(self, monkeypatch):
        client = MagicMock()

        monkeypatch.setattr(
            cache,
            "object_exists",
            MagicMock(
                side_effect=RuntimeError(
                    "MinIO unavailable"
                )
            ),
        )

        with pytest.raises(
            cache.CountryCacheError,
            match=(
                "Failed to retrieve or cache country: Nigeria"
            ),
        ):
            cache.get_cached_country(
                country_name="Nigeria",
                client=client,
            )


class TestGetCountryWithCacheStatus:
    def test_returns_hit_status_for_cached_country(self):
        client = MagicMock()

        country = {
            "name": {
                "common": "Nigeria"
            },
            "cca3": "NGA",
        }

        object_exists_mock = MagicMock(
            return_value=True
        )

        download_json_mock = MagicMock(
            return_value=country
        )

        original_object_exists = cache.object_exists
        original_download_json = cache.download_json

        cache.object_exists = object_exists_mock
        cache.download_json = download_json_mock

        try:
            result = cache.get_country_with_cache_status(
                country_name="Nigeria",
                client=client,
            )

            assert result == (country, "hit")

            expected_object_name = (
                cache._get_cache_object_name("Nigeria")
            )

            object_exists_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

            download_json_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

        finally:
            cache.object_exists = original_object_exists
            cache.download_json = original_download_json

    def test_returns_miss_status_for_api_country(self):
        client = MagicMock()

        country = {
            "name": {
                "common": "Nigeria"
            },
            "cca3": "NGA",
        }

        object_exists_mock = MagicMock(
            return_value=False
        )

        get_country_mock = MagicMock(
            return_value=country
        )

        upload_json_mock = MagicMock()

        original_object_exists = cache.object_exists
        original_get_country = cache.get_country
        original_upload_json = cache.upload_json

        cache.object_exists = object_exists_mock
        cache.get_country = get_country_mock
        cache.upload_json = upload_json_mock

        try:
            result = cache.get_country_with_cache_status(
                country_name="Nigeria",
                client=client,
            )

            assert result == (country, "miss")

            expected_object_name = (
                cache._get_cache_object_name("Nigeria")
            )

            object_exists_mock.assert_called_once_with(
                client,
                expected_object_name,
            )

            get_country_mock.assert_called_once_with(
                "Nigeria"
            )

            upload_json_mock.assert_called_once_with(
                client,
                expected_object_name,
                country,
            )

        finally:
            cache.object_exists = original_object_exists
            cache.get_country = original_get_country
            cache.upload_json = original_upload_json

    def test_rejects_invalid_cached_country(self):
        client = MagicMock()

        original_object_exists = cache.object_exists
        original_download_json = cache.download_json

        cache.object_exists = MagicMock(
            return_value=True
        )
        cache.download_json = MagicMock(
            return_value=["invalid"]
        )

        try:
            with pytest.raises(
                cache.CountryCacheError,
                match=(
                    "Cached country response has "
                    "an invalid format"
                ),
            ):
                cache.get_country_with_cache_status(
                    country_name="Nigeria",
                    client=client,
                )

        finally:
            cache.object_exists = original_object_exists
            cache.download_json = original_download_json

    def test_rejects_invalid_country_api_response(self):
        client = MagicMock()

        original_object_exists = cache.object_exists
        original_get_country = cache.get_country

        cache.object_exists = MagicMock(
            return_value=False
        )

        get_country_mock = MagicMock(
            return_value=["invalid"]
        )

        cache.get_country = get_country_mock

        try:
            with pytest.raises(
                cache.CountryCacheError,
                match=(
                    "Country API returned an invalid response"
                ),
            ):
                cache.get_country_with_cache_status(
                    country_name="Nigeria",
                    client=client,
                )

        finally:
            cache.object_exists = original_object_exists
            cache.get_country = original_get_country

    def test_creates_minio_client_when_client_is_not_provided(
        self,
        monkeypatch,
    ):
        fake_client = MagicMock()

        get_minio_client_mock = MagicMock(
            return_value=fake_client
        )

        monkeypatch.setattr(
            cache,
            "get_minio_client",
            get_minio_client_mock,
        )

        monkeypatch.setattr(
            cache,
            "object_exists",
            MagicMock(return_value=True),
        )

        country = {
            "name": {
                "common": "Nigeria"
            }
        }

        monkeypatch.setattr(
            cache,
            "download_json",
            MagicMock(return_value=country),
        )

        result = cache.get_country_with_cache_status(
            country_name="Nigeria"
        )

        assert result == (country, "hit")

        get_minio_client_mock.assert_called_once_with()

    def test_wraps_unexpected_error(self, monkeypatch):
        client = MagicMock()

        monkeypatch.setattr(
            cache,
            "object_exists",
            MagicMock(
                side_effect=RuntimeError(
                    "MinIO unavailable"
                )
            ),
        )

        with pytest.raises(
            cache.CountryCacheError,
            match=(
                "Failed to retrieve or cache country: Nigeria"
            ),
        ):
            cache.get_country_with_cache_status(
                country_name="Nigeria",
                client=client,
            )