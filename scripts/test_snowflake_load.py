import logging

from triplens.ingest.country_ingestion import ingest_countries
from triplens.load.snowflake_loader import load_countries
from triplens.storage.minio_client import get_minio_client
from triplens.storage.snowflake.snowflake_client import (
    get_snowflake_connection,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main() -> None:
    print("\n=== TRIPLENS SNOWFLAKE LOAD TEST ===")

    # ---------------------------------------------------------
    # 1. Connect to MinIO
    # ---------------------------------------------------------

    minio_client = get_minio_client()

    # ---------------------------------------------------------
    # 2. Retrieve countries through the existing
    #    MinIO-backed country ingestion layer.
    # ---------------------------------------------------------

    countries = ingest_countries(
        [
            "Nigeria",
            "Ghana",
            "Kenya",
        ],
        client=minio_client,
    )

    print("\n=== COUNTRY INGESTION ===")
    print(
        f"Requested: {countries['total_requested']}"
    )
    print(
        f"Unique: {countries['total_unique']}"
    )
    print(
        f"Succeeded: {countries['total_succeeded']}"
    )
    print(
        f"Failed: {countries['total_failed']}"
    )

    if countries["total_succeeded"] == 0:
        raise RuntimeError(
            "No country records were available for Snowflake loading."
        )

    # ---------------------------------------------------------
    # 3. Connect to Snowflake
    # ---------------------------------------------------------

    snowflake_connection = get_snowflake_connection()

    try:

        # -----------------------------------------------------
        # 4. Load country records into Snowflake RAW.
        # -----------------------------------------------------

        result = load_countries(
            countries["successful_records"],
            connection=snowflake_connection,
        )

        print("\n=== SNOWFLAKE RAW LOAD ===")
        print(
            f"Requested: {result['total_requested']}"
        )
        print(
            f"Unique: {result['total_unique']}"
        )
        print(
            f"Succeeded: {result['total_succeeded']}"
        )
        print(
            f"Failed: {result['total_failed']}"
        )
        print(
            f"Status: {result['status']}"
        )

        if result["failures"]:
            print("\nFailures:")

            for failure in result["failures"]:
                print(
                    f"- {failure['country']}: "
                    f"{failure['error']}"
                )

        if result["status"] == "FAILED":
            raise RuntimeError(
                "Snowflake RAW load failed."
            )

    finally:
        snowflake_connection.close()


if __name__ == "__main__":
    main()
