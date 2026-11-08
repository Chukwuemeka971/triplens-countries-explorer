import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

from triplens.storage.snowflake.snowflake_client import (
    get_snowflake_connection,
)


logger = logging.getLogger(__name__)


class SnowflakeLoaderError(Exception):
    """Raised when loading data into Snowflake fails."""


RAW_TABLE = "TRIPLENS.RAW.RAW_COUNTRIES"
TEMP_TABLE = "TEMP_RAW_COUNTRIES"
SOURCE_NAME = "restcountries"


def _get_country_name(country: dict[str, Any]) -> str:
    """Return the common country name from a country record."""

    if not isinstance(country, dict):
        return "UNKNOWN"

    names = country.get("names")

    if not isinstance(names, dict):
        return "UNKNOWN"

    common_name = names.get("common")

    if not isinstance(common_name, str) or not common_name.strip():
        return "UNKNOWN"

    return common_name.strip()


def _build_record_id(country: dict[str, Any]) -> str:
    """
    Build a deterministic record ID for a country.

    Prefer the ISO alpha-3 code when available.

    Fall back to the common country name for territories
    that do not have an ISO alpha-3 code.
    """

    codes = country.get("codes") or {}

    if not isinstance(codes, dict):
        codes = {}

    alpha_3 = codes.get("alpha_3")

    if isinstance(alpha_3, str) and alpha_3.strip():
        stable_key = f"ISO3:{alpha_3.strip().upper()}"

    else:
        names = country.get("names") or {}

        if not isinstance(names, dict):
            raise SnowflakeLoaderError(
                "Country record has invalid names structure."
            )

        common_name = names.get("common")

        if not isinstance(common_name, str) or not common_name.strip():
            raise SnowflakeLoaderError(
                "Country record has neither alpha-3 code nor common name."
            )

        stable_key = f"NAME:{common_name.strip().casefold()}"

    return hashlib.sha256(
        f"{SOURCE_NAME}:{stable_key}".encode("utf-8")
    ).hexdigest()


def _validate_country(country: dict[str, Any]) -> None:
    """Validate the minimum structure required for RAW ingestion."""

    if not isinstance(country, dict):
        raise SnowflakeLoaderError(
            "Country record must be a dictionary."
        )

    names = country.get("names")

    if not isinstance(names, dict):
        raise SnowflakeLoaderError(
            "Country record is missing the names object."
        )

    common_name = names.get("common")

    if not isinstance(common_name, str) or not common_name.strip():
        raise SnowflakeLoaderError(
            "Country record is missing a valid common name."
        )


def load_country(
    country: dict[str, Any],
    connection: SnowflakeConnection | None = None,
) -> str:
    """
    Load one country record into Snowflake RAW.

    Uses MERGE so repeated executions are idempotent.

    Returns:
        Deterministic RECORD_ID.
    """

    _validate_country(country)

    record_id = _build_record_id(country)
    country_name = _get_country_name(country)

    ingested_at = datetime.now(timezone.utc)

    payload = json.dumps(
        country,
        ensure_ascii=False,
    )

    should_close_connection = connection is None

    try:

        if connection is None:
            connection = get_snowflake_connection()

        merge_sql = f"""
            MERGE INTO {RAW_TABLE} AS target

            USING (
                SELECT
                    %s AS RECORD_ID,
                    %s AS COUNTRY_NAME,
                    %s AS SOURCE,
                    %s AS INGESTED_AT,
                    PARSE_JSON(%s) AS PAYLOAD
            ) AS source

            ON target.RECORD_ID = source.RECORD_ID

            WHEN MATCHED THEN
                UPDATE SET
                    COUNTRY_NAME = source.COUNTRY_NAME,
                    SOURCE = source.SOURCE,
                    INGESTED_AT = source.INGESTED_AT,
                    PAYLOAD = source.PAYLOAD

            WHEN NOT MATCHED THEN
                INSERT (
                    RECORD_ID,
                    COUNTRY_NAME,
                    SOURCE,
                    INGESTED_AT,
                    PAYLOAD
                )
                VALUES (
                    source.RECORD_ID,
                    source.COUNTRY_NAME,
                    source.SOURCE,
                    source.INGESTED_AT,
                    source.PAYLOAD
                )
        """

        with connection.cursor() as cursor:
            cursor.execute(
                merge_sql,
                (
                    record_id,
                    country_name,
                    SOURCE_NAME,
                    ingested_at,
                    payload,
                ),
            )

        connection.commit()

        logger.info(
            "Country loaded into Snowflake RAW: "
            "%s | record_id=%s",
            country_name,
            record_id,
        )

        return record_id

    except SnowflakeError as exc:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "Snowflake error loading country: %s",
            country_name,
        )

        raise SnowflakeLoaderError(
            f"Failed to load country into Snowflake RAW: "
            f"{country_name}"
        ) from exc

    except Exception as exc:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "Unexpected Snowflake loading error: %s",
            country_name,
        )

        raise SnowflakeLoaderError(
            f"Unexpected error loading country: "
            f"{country_name}"
        ) from exc

    finally:

        if should_close_connection and connection is not None:
            connection.close()


def _prepare_batch(
    countries: list[dict[str, Any]],
) -> tuple[list[tuple[Any, ...]], list[dict[str, str]]]:
    """
    Validate and prepare country records for batch loading.

    Returns:
        batch_rows:
            Rows ready for Snowflake executemany().

        failures:
            Invalid input records that cannot be loaded.
    """

    unique_countries: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []

    for country in countries:

        try:

            _validate_country(country)

            record_id = _build_record_id(country)

            unique_countries[record_id] = country

        except SnowflakeLoaderError as exc:

            failures.append(
                {
                    "country": _get_country_name(country),
                    "error": str(exc),
                }
            )

    ingested_at = datetime.now(timezone.utc)

    batch_rows: list[tuple[Any, ...]] = []

    for record_id, country in unique_countries.items():

        country_name = _get_country_name(country)

        payload = json.dumps(
            country,
            ensure_ascii=False,
        )

        batch_rows.append(
            (
                record_id,
                country_name,
                SOURCE_NAME,
                ingested_at,
                payload,
            )
        )

    return batch_rows, failures


def load_countries(
    countries: list[dict[str, Any]],
    connection: SnowflakeConnection | None = None,
) -> dict[str, Any]:
    """
    Batch-load multiple country records into Snowflake RAW.

    The process is:

        1. Validate records.
        2. Deduplicate records.
        3. Insert the batch into a temporary table.
        4. MERGE the temporary table into RAW_COUNTRIES.

    This significantly reduces Snowflake round trips compared
    with executing one MERGE per country.

    The final MERGE keeps the operation idempotent.
    """

    if not isinstance(countries, list):
        raise TypeError("countries must be a list.")

    if not countries:
        raise ValueError("countries cannot be empty.")

    should_close_connection = connection is None

    batch_rows, failures = _prepare_batch(countries)

    total_requested = len(countries)
    total_unique = len(batch_rows)

    logger.info(
        "Starting batch Snowflake RAW load. "
        "Requested=%d | Unique=%d | Invalid=%d",
        total_requested,
        total_unique,
        len(failures),
    )

    if not batch_rows:

        return {
            "total_requested": total_requested,
            "total_unique": 0,
            "total_succeeded": 0,
            "total_failed": len(failures),
            "successful_records": [],
            "failures": failures,
            "status": "FAILED",
        }

    try:

        if connection is None:
            connection = get_snowflake_connection()

        # -----------------------------------------------------
        # 1. Create a temporary staging table.
        # -----------------------------------------------------

        create_temp_table_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {TEMP_TABLE} (
                RECORD_ID VARCHAR(64) NOT NULL,
                COUNTRY_NAME VARCHAR(255) NOT NULL,
                SOURCE VARCHAR(100) NOT NULL,
                INGESTED_AT TIMESTAMP_TZ NOT NULL,
                PAYLOAD VARCHAR NOT NULL
            )
        """

        with connection.cursor() as cursor:

            cursor.execute(
                create_temp_table_sql
            )

            # -------------------------------------------------
            # 2. Batch insert records into the temporary table.
            # -------------------------------------------------

            insert_sql = f"""
                INSERT INTO {TEMP_TABLE} (
                    RECORD_ID,
                    COUNTRY_NAME,
                    SOURCE,
                    INGESTED_AT,
                    PAYLOAD
                )
                VALUES(
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """

            cursor.executemany(
                insert_sql,
                batch_rows,
            )

            # -------------------------------------------------
            # 3. Merge the complete batch into RAW.
            # -------------------------------------------------

            merge_sql = f"""
                MERGE INTO {RAW_TABLE} AS target

                USING (
                    SELECT
                        RECORD_ID,
                        COUNTRY_NAME,
                        SOURCE,
                        INGESTED_AT,
                        PARSE_JSON(PAYLOAD) AS PAYLOAD
                    FROM {TEMP_TABLE}
                ) AS source

                ON target.RECORD_ID = source.RECORD_ID

                WHEN MATCHED THEN
                    UPDATE SET
                        COUNTRY_NAME = source.COUNTRY_NAME,
                        SOURCE = source.SOURCE,
                        INGESTED_AT = source.INGESTED_AT,
                        PAYLOAD = source.PAYLOAD

                WHEN NOT MATCHED THEN
                    INSERT (
                        RECORD_ID,
                        COUNTRY_NAME,
                        SOURCE,
                        INGESTED_AT,
                        PAYLOAD
                    )
                    VALUES (
                        source.RECORD_ID,
                        source.COUNTRY_NAME,
                        source.SOURCE,
                        source.INGESTED_AT,
                        source.PAYLOAD
                    )
            """

            cursor.execute(
                merge_sql
            )

        connection.commit()

        # -----------------------------------------------------
        # 4. Build successful-record metadata.
        # -----------------------------------------------------

        successful_records = [
            {
                "country": row[1],
                "record_id": row[0],
            }
            for row in batch_rows
        ]

        total_succeeded = len(successful_records)
        total_failed = len(failures)

        if total_failed == 0:
            status = "SUCCESS"

        elif total_succeeded > 0:
            status = "PARTIAL_SUCCESS"

        else:
            status = "FAILED"

        summary = {
            "total_requested": total_requested,
            "total_unique": total_unique,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "successful_records": successful_records,
            "failures": failures,
            "status": status,
        }

        logger.info(
            "Batch Snowflake RAW load completed. "
            "Requested=%d | Unique=%d | "
            "Succeeded=%d | Failed=%d | Status=%s",
            total_requested,
            total_unique,
            total_succeeded,
            total_failed,
            status,
        )

        return summary

    except SnowflakeError as exc:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "Snowflake batch load failed."
        )

        raise SnowflakeLoaderError(
            "Failed to batch load countries into Snowflake RAW."
        ) from exc

    except Exception as exc:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "Unexpected Snowflake batch loading error."
        )

        raise SnowflakeLoaderError(
            "Unexpected error during Snowflake batch load."
        ) from exc

    finally:

        if should_close_connection and connection is not None:
            connection.close()
