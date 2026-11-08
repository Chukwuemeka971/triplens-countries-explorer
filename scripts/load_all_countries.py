import logging
import time

from triplens.extract.countries_api import get_all_countries
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
    start_time = time.perf_counter()

    print("\n=== TRIPLENS FULL COUNTRY LOAD ===")

    # ---------------------------------------------------------
    # 1. Extract the complete country dataset.
    # ---------------------------------------------------------

    print("\n=== COUNTRY EXTRACTION ===")

    countries = get_all_countries()

    print(
        f"Countries extracted: {len(countries)}"
    )

    if not countries:
        raise RuntimeError(
            "Country API returned no records."
        )

    # ---------------------------------------------------------
    # 2. Connect to MinIO.
    # ---------------------------------------------------------

    minio_client = get_minio_client()

    # ---------------------------------------------------------
    # 3. Cache all country records in MinIO.
    #
    #    We use the existing country ingestion layer so
    #    validation, caching and error handling remain
    #    centralized.
    # ---------------------------------------------------------

    print("\n=== MINIO COUNTRY CACHE ===")

    from triplens.ingest.country_ingestion import (
        ingest_countries,
    )

    country_names = [
        country["names"]["common"]
        for country in countries
        if isinstance(country.get("names"), dict)
        and country["names"].get("common")
    ]

    ingestion_result = ingest_countries(
        country_names,
        client=minio_client,
    )

    print(
        f"Requested: {ingestion_result['total_requested']}"
    )
    print(
        f"Unique: {ingestion_result['total_unique']}"
    )
    print(
        f"Succeeded: {ingestion_result['total_succeeded']}"
    )
    print(
        f"Failed: {ingestion_result['total_failed']}"
    )

    if ingestion_result["total_succeeded"] == 0:
        raise RuntimeError(
            "No countries were successfully cached."
        )

    # ---------------------------------------------------------
    # 4. Load the extracted country records directly into
    #    Snowflake RAW.
    #
    #    The loader uses deterministic RECORD_ID + MERGE,
    #    making the operation idempotent.
    # ---------------------------------------------------------

    print("\n=== SNOWFLAKE RAW LOAD ===")

    snowflake_connection = get_snowflake_connection()

    try:

        load_result = load_countries(
            countries,
            connection=snowflake_connection,
        )

        print(
            f"Requested: {load_result['total_requested']}"
        )
        print(
            f"Unique: {load_result['total_unique']}"
        )
        print(
            f"Succeeded: {load_result['total_succeeded']}"
        )
        print(
            f"Failed: {load_result['total_failed']}"
        )
        print(
            f"Status: {load_result['status']}"
        )

        if load_result["failures"]:

            print("\n=== FAILURES ===")

            for failure in load_result["failures"]:
                print(
                    f"- {failure['country']}: "
                    f"{failure['error']}"
                )

        if load_result["status"] == "FAILED":
            raise RuntimeError(
                "Snowflake RAW load failed."
            )

    finally:
        snowflake_connection.close()

    # ---------------------------------------------------------
    # 5. Overall pipeline timing.
    # ---------------------------------------------------------

    duration_seconds = round(
        time.perf_counter() - start_time,
        4,
    )

    print("\n=== PIPELINE SUMMARY ===")

    print(
        f"Countries extracted: {len(countries)}"
    )

    print(
        f"Countries cached: "
        f"{ingestion_result['total_succeeded']}"
    )

    print(
        f"Countries loaded to Snowflake: "
        f"{load_result['total_succeeded']}"
    )

    print(
        f"Total duration: "
        f"{duration_seconds} seconds"
    )

    print(
        f"Final status: "
        f"{load_result['status']}"
    )


if __name__ == "__main__":
    main()
