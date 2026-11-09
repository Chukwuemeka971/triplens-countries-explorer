from datetime import date

from triplens.load.weather_snowflake_loader import (
    load_weather_records,
)
from triplens.storage.minio_client import (
    get_minio_client,
)
from triplens.storage.snowflake.snowflake_client import (
    get_snowflake_connection,
)


COUNTRY_CODES = [
    "GBR",
    "NGA",
    "GHA",
]

FORECAST_DATE = date.today().isoformat()


def main():
    print("=" * 60)
    print("TripLens Weather → Snowflake Integration Test")
    print("=" * 60)

    print(f"Forecast date: {FORECAST_DATE}")
    print(f"Countries: {COUNTRY_CODES}")
    print()

    minio_client = get_minio_client()
    snowflake_connection = get_snowflake_connection()

    try:
        print("Loading weather data from MinIO into Snowflake...")

        result = load_weather_records(
            country_codes=COUNTRY_CODES,
            forecast_date=FORECAST_DATE,
            minio_client=minio_client,
            connection=snowflake_connection,
        )

        print()
        print("LOAD RESULT")
        print("-" * 60)
        print(f"Requested:  {result['total_requested']}")
        print(f"Unique:     {result['total_unique']}")
        print(f"Succeeded:  {result['total_succeeded']}")
        print(f"Failed:     {result['total_failed']}")
        print(f"Status:     {result['status']}")

        if result["failures"]:
            print()
            print("FAILURES")
            for failure in result["failures"]:
                print(failure)

        print()
        print("Querying Snowflake RAW_WEATHER...")

        query = """
            SELECT
                COUNTRY_CODE,
                FORECAST_DATE,
                SOURCE,
                INGESTED_AT
            FROM TRIPLENS.RAW.RAW_WEATHER
            WHERE COUNTRY_CODE IN ('GBR', 'NGA', 'GHA')
              AND FORECAST_DATE = %s
            ORDER BY COUNTRY_CODE
        """

        with snowflake_connection.cursor() as cursor:
            cursor.execute(
                query,
                (FORECAST_DATE,),
            )

            rows = cursor.fetchall()

        print()
        print("SNOWFLAKE RESULTS")
        print("-" * 60)

        for row in rows:
            print(row)

        print()
        print(f"Rows found in Snowflake: {len(rows)}")

        if len(rows) != 3:
            raise RuntimeError(
                "Integration test failed: "
                f"expected 3 rows but found {len(rows)}"
            )

        print()
        print("=" * 60)
        print("INTEGRATION TEST PASSED")
        print("=" * 60)

    finally:
        snowflake_connection.close()
    


if __name__ == "__main__":
    main()