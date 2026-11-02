
from unittest.mock import Mock, patch

import pytest

import triplens.extract.countries_api as client


def make_response(countries, offset, total, more):
    """Build a mocked paginated API response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "data": {
            "objects": countries,
            "meta": {
                "total": total,
                "count": len(countries),
                "limit": 2,
                "offset": offset,
                "more": more,
            },
        }
    }
    return response


@patch("triplens.extract.countries_api.requests.get")
def test_get_country_returns_country_record(mock_get, monkeypatch):
    monkeypatch.setattr(client, "API_KEY", "test-key")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {
            "objects": [
                {
                    "names": {"common": "Canada"},
                    "codes": {"alpha_2": "CA"},
                }
            ],
            "meta": {
                "total": 1,
                "count": 1,
                "more": False,
            },
        }
    }
    mock_get.return_value = mock_response

    result = client.get_country("Canada")

    assert result["names"]["common"] == "Canada"
    assert result["codes"]["alpha_2"] == "CA"
    mock_response.raise_for_status.assert_called_once()


@patch("triplens.extract.countries_api.requests.get")
def test_get_all_countries_paginates(mock_get, monkeypatch):
    monkeypatch.setattr(client, "API_KEY", "test-key")

    first_page = [
        {
            "names": {"common": "Canada"},
            "codes": {"alpha_3": "CAN"},
        },
        {
            "names": {"common": "Japan"},
            "codes": {"alpha_3": "JPN"},
        },
    ]

    second_page = [
        {
            "names": {"common": "Nigeria"},
            "codes": {"alpha_3": "NGA"},
        },
    ]

    mock_get.side_effect = [
        make_response(first_page, 0, 3, True),
        make_response(second_page, 2, 3, False),
    ]

    result = client.get_all_countries(page_size=2)

    assert len(result) == 3
    assert [c["codes"]["alpha_3"] for c in result] == [
        "CAN",
        "JPN",
        "NGA",
    ]
    assert mock_get.call_count == 2


@patch("triplens.extract.countries_api.requests.get")
def test_get_all_countries_rejects_duplicate_codes(
    mock_get, monkeypatch
):
    monkeypatch.setattr(client, "API_KEY", "test-key")

    duplicate_page = [
        {
            "names": {"common": "Canada"},
            "codes": {"alpha_3": "CAN"},
        },
        {
            "names": {"common": "Canada duplicate"},
            "codes": {"alpha_3": "CAN"},
        },
    ]

    mock_get.return_value = make_response(
        duplicate_page, 0, 2, False
    )

    with pytest.raises(
        client.CountriesAPIError,
        match="Duplicate country identifier",
    ):
        client.get_all_countries(page_size=2)


def test_get_all_countries_rejects_invalid_page_size():
    with pytest.raises(ValueError):
        client.get_all_countries(page_size=101)


@patch("triplens.extract.countries_api.time.sleep")
@patch("triplens.extract.countries_api.requests.get")
def test_get_json_retries_after_rate_limit(
    mock_get, mock_sleep, monkeypatch
):
    monkeypatch.setattr(client, "API_KEY", "test-key")

    limited = Mock()
    limited.status_code = 429
    limited.headers = {"Retry-After": "0"}

    successful = Mock()
    successful.status_code = 200
    successful.headers = {}
    successful.json.return_value = {
        "data": {"objects": []}
    }

    mock_get.side_effect = [limited, successful]

    result = client._get_json("https://example.com/test")

    assert result == {"data": {"objects": []}}
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(0)


def test_validate_country_record_uses_iso3_identifier():
    country = {
        "names": {"common": "Nigeria"},
        "codes": {"alpha_3": "NGA"},
    }

    result = client._validate_country_record(country)

    assert result == "ISO3:NGA"


def test_validate_country_record_uses_name_fallback(caplog):
    country = {
        "names": {"common": "Abkhazia"},
        "codes": {
            "alpha_2": "",
            "alpha_3": "",
        },
    }

    with caplog.at_level("WARNING"):
        result = client._validate_country_record(country)

    assert result == "NAME:abkhazia"
    assert "no ISO alpha-3 code" in caplog.text


def test_validate_country_record_rejects_missing_common_name():
    country = {
        "names": {},
        "codes": {"alpha_3": "NGA"},
    }

    with pytest.raises(
        client.CountriesAPIError,
        match="common name",
    ):
        client._validate_country_record(country)


def test_validate_country_record_rejects_invalid_record():
    with pytest.raises(
        client.CountriesAPIError,
        match="expected an object",
    ):
        client._validate_country_record(None)

