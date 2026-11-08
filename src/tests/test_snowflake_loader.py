import pytest

from triplens.load.snowflake_loader import (
    SnowflakeLoaderError,
    _build_record_id,
    _validate_country,
    load_countries,
    load_country,
)


def make_country(
    name: str = "Nigeria",
    alpha_3: str = "NGA",
) -> dict:
    return {
        "names": {
            "common": name,
            "official": f"Republic of {name}",
        },
        "codes": {
            "alpha_3": alpha_3,
        },
        "region": "Africa",
    }


class FakeCursor:
    """Simple fake Snowflake cursor for unit tests."""

    def __init__(self):
        self.executed_sql = None
        self.executed_parameters = None
        self.executemany_sql = None
        self.executemany_parameters = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, parameters=None):
        self.executed_sql = sql
        self.executed_parameters = parameters

    def executemany(self, sql, parameters):
        self.executemany_sql = sql
        self.executemany_parameters = parameters


class FakeConnection:
    """Simple fake Snowflake connection for unit tests."""

    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.commit_called = False
        self.rollback_called = False
        self.close_called = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True

    def close(self):
        self.close_called = True


def test_build_record_id_is_deterministic():
    country = make_country()

    first_id = _build_record_id(country)
    second_id = _build_record_id(country)

    assert first_id == second_id
    assert len(first_id) == 64


def test_build_record_id_uses_alpha3():
    nigeria = make_country(
        name="Nigeria",
        alpha_3="NGA",
    )

    ghana = make_country(
        name="Nigeria",
        alpha_3="GHA",
    )

    assert _build_record_id(nigeria) != _build_record_id(ghana)


def test_build_record_id_falls_back_to_country_name():
    country = make_country(
        name="Abkhazia",
        alpha_3="",
    )

    record_id = _build_record_id(country)

    assert isinstance(record_id, str)
    assert len(record_id) == 64


def test_validate_country_accepts_valid_country():
    country = make_country()

    _validate_country(country)


def test_validate_country_rejects_invalid_country():
    with pytest.raises(
        SnowflakeLoaderError,
        match="Country record must be a dictionary",
    ):
        _validate_country("Nigeria")


def test_validate_country_rejects_missing_name():
    country = {
        "names": {},
        "codes": {
            "alpha_3": "NGA",
        },
    }

    with pytest.raises(
        SnowflakeLoaderError,
        match="valid common name",
    ):
        _validate_country(country)


def test_load_country_executes_merge_and_commits():
    connection = FakeConnection()

    country = make_country()

    record_id = load_country(
        country,
        connection=connection,
    )

    assert isinstance(record_id, str)
    assert len(record_id) == 64

    assert connection.commit_called is True
    assert connection.rollback_called is False
    assert connection.close_called is False

    assert connection.cursor_instance.executed_sql is not None

    assert "MERGE INTO TRIPLENS.RAW.RAW_COUNTRIES" in (
        connection.cursor_instance.executed_sql
    )

    parameters = connection.cursor_instance.executed_parameters

    assert parameters[0] == record_id
    assert parameters[1] == "Nigeria"
    assert parameters[2] == "restcountries"


def test_load_country_is_idempotent_by_record_id():
    connection = FakeConnection()

    country = make_country()

    first_id = load_country(
        country,
        connection=connection,
    )

    second_id = load_country(
        country,
        connection=connection,
    )

    assert first_id == second_id

    assert connection.commit_called is True


def test_load_countries_deduplicates_records():
    connection = FakeConnection()

    countries = [
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
        make_country(
            name="Ghana",
            alpha_3="GHA",
        ),
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
    ]

    result = load_countries(
        countries,
        connection=connection,
    )

    assert result["total_requested"] == 3
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 2
    assert result["total_failed"] == 0
    assert result["status"] == "SUCCESS"

    assert connection.commit_called is True

    assert connection.cursor_instance.executemany_sql is not None

    assert "INSERT INTO TEMP_RAW_COUNTRIES" in (
        connection.cursor_instance.executemany_sql
    )

    assert len(
        connection.cursor_instance.executemany_parameters
    ) == 2

    assert connection.cursor_instance.executed_sql is not None

    assert "MERGE INTO TRIPLENS.RAW.RAW_COUNTRIES" in (
        connection.cursor_instance.executed_sql
    )


def test_load_countries_batch_contains_correct_records():
    connection = FakeConnection()

    countries = [
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
        make_country(
            name="Ghana",
            alpha_3="GHA",
        ),
    ]

    result = load_countries(
        countries,
        connection=connection,
    )

    assert result["total_requested"] == 2
    assert result["total_unique"] == 2
    assert result["total_succeeded"] == 2
    assert result["total_failed"] == 0

    batch_parameters = (
        connection.cursor_instance.executemany_parameters
    )

    assert len(batch_parameters) == 2

    country_names = {
        row[1]
        for row in batch_parameters
    }

    assert country_names == {
        "Nigeria",
        "Ghana",
    }


def test_load_countries_handles_invalid_record():
    connection = FakeConnection()

    countries = [
        make_country(
            name="Nigeria",
            alpha_3="NGA",
        ),
        {
            "codes": {
                "alpha_3": "XXX",
            },
        },
    ]

    result = load_countries(
        countries,
        connection=connection,
    )

    assert result["total_requested"] == 2
    assert result["total_unique"] == 1
    assert result["total_succeeded"] == 1
    assert result["total_failed"] == 1
    assert result["status"] == "PARTIAL_SUCCESS"


def test_load_countries_rejects_empty_list():
    with pytest.raises(
        ValueError,
        match="countries cannot be empty",
    ):
        load_countries([])


def test_load_countries_rejects_non_list():
    with pytest.raises(
        TypeError,
        match="countries must be a list",
    ):
        load_countries("Nigeria")
