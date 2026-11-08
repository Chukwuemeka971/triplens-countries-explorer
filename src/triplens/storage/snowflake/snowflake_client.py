import logging
import os

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

load_dotenv()

logger = logging.getLogger(__name__)


class SnowflakeStorageError(Exception):
    """Raised when a Snowflake storage operation fails."""


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


def _validate_configuration() -> None:
    """Validate required Snowflake configuration."""

    required_settings = {
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    }

    missing_settings = [
        name
        for name, value in required_settings.items()
        if not value
    ]

    if missing_settings:
        raise SnowflakeStorageError(
            "Missing Snowflake configuration: "
            + ", ".join(missing_settings)
        )


def get_snowflake_connection() -> SnowflakeConnection:
    """Create and return a Snowflake connection."""

    _validate_configuration()

    connection_parameters = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
    }

    if SNOWFLAKE_ROLE:
        connection_parameters["role"] = SNOWFLAKE_ROLE

    try:
        connection = snowflake.connector.connect(
            **connection_parameters
        )

        logger.info(
            "Snowflake connection established: "
            "database=%s schema=%s",
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA,
        )

        return connection

    except SnowflakeError as exc:
        logger.exception("Failed to connect to Snowflake.")

        raise SnowflakeStorageError(
            "Failed to establish Snowflake connection."
        ) from exc


def test_snowflake_connection(
    connection: SnowflakeConnection,
) -> bool:
    """Verify that an existing Snowflake connection is usable."""

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    CURRENT_ACCOUNT(),
                    CURRENT_USER(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_WAREHOUSE()
                """
            )

            result = cursor.fetchone()

        logger.info("Snowflake connection test successful.")

        return result is not None

    except SnowflakeError as exc:
        logger.exception(
            "Snowflake connection test failed."
        )

        raise SnowflakeStorageError(
            "Snowflake connection test failed."
        ) from exc