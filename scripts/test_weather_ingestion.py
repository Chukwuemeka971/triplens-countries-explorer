import logging
from datetime import date

from triplens.ingest.country_ingestion import ingest_countries
from triplens.ingest.weather_ingestion import ingest_weather_for_countries
from triplens.storage.minio_client import get_minio_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main() -> None:
    client = get_minio_client()

    countries = ingest_countries(
        [
            "Nigeria",
            "Ghana",
            "Kenya",
        ],
        client=client,
    )

    if countries["total_succeeded"] == 0:
        raise RuntimeError(
            "Country ingestion returned no successful records."
        )

    print("\n=== COUNTRY INGESTION ===")
    print(
        f"Requested: {countries['total_requested']}"
    )
    print(
        f"Succeeded: {countries['total_succeeded']}"
    )
    print(
        f"Failed: {countries['total_failed']}"
    )

    weather = ingest_weather_for_countries(
        country_records=countries["successful_records"],
        forecast_date=date.today().isoformat(),
        client=client,
    )

    print("\n=== WEATHER INGESTION ===")
    print(
        f"Requested: {weather['total_requested']}"
    )
    print(
        f"Unique: {weather['total_unique']}"
    )
    print(
        f"Succeeded: {weather['total_succeeded']}"
    )
    print(
        f"Failed: {weather['total_failed']}"
    )
    print(
        f"Cache hits: {weather['cache_hits']}"
    )
    print(
        f"Cache misses: {weather['cache_misses']}"
    )
    print(
        f"Duration: {weather['duration_seconds']} seconds"
    )
    print(
        f"Status: {weather['status']}"
    )


if __name__ == "__main__":
    main()