from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


# =========================================================
# AIRFLOW OBSERVABILITY CALLBACKS
# =========================================================


def task_failure_callback(context):
    """
    Logs useful information whenever an Airflow task fails.
    """

    task_instance = context.get("task_instance")

    if task_instance is None:
        print("AIRFLOW TASK FAILURE: task instance unavailable.")
        return

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    exception = context.get("exception")

    print("=" * 70)
    print("AIRFLOW TASK FAILURE")
    print(f"DAG ID: {dag_id}")
    print(f"TASK ID: {task_id}")
    print(f"RUN ID: {run_id}")
    print(f"EXCEPTION: {exception}")
    print("=" * 70)


def task_retry_callback(context):
    """
    Logs useful information whenever Airflow retries a task.
    """

    task_instance = context.get("task_instance")

    if task_instance is None:
        print("AIRFLOW TASK RETRY: task instance unavailable.")
        return

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    try_number = task_instance.try_number

    print("=" * 70)
    print("AIRFLOW TASK RETRY")
    print(f"DAG ID: {dag_id}")
    print(f"TASK ID: {task_id}")
    print(f"RUN ID: {run_id}")
    print(f"CURRENT TRY: {try_number}")
    print("=" * 70)


def dag_failure_callback(context):
    """
    Logs useful information whenever the overall DAG run fails.
    """

    dag_run = context.get("dag_run")

    if dag_run is None:
        print("AIRFLOW DAG FAILURE: DAG run unavailable.")
        return

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    print("=" * 70)
    print("AIRFLOW DAG FAILURE")
    print(f"DAG ID: {dag_id}")
    print(f"RUN ID: {run_id}")
    print("The TripLens pipeline did not complete successfully.")
    print("=" * 70)


# =========================================================
# DAG DEFINITION
# =========================================================


@dag(
    dag_id="triplens_country_pipeline",
    description=(
        "TripLens multi-source country and weather ingestion "
        "pipeline"
    ),
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": task_failure_callback,
        "on_retry_callback": task_retry_callback,
    },
    on_failure_callback=dag_failure_callback,
    tags=[
        "triplens",
        "data-engineering",
        "countries",
        "weather",
        "dbt",
    ],
)
def triplens_country_pipeline():

    # =====================================================
    # COUNTRY EXTRACTION + MINIO CACHE
    # =====================================================

    @task(
        execution_timeout=timedelta(minutes=15)
    )
    def extract_and_cache_countries() -> dict:

        from triplens.extract.countries_api import (
            get_all_countries,
        )
        from triplens.storage.country_cache import (
            get_cached_country,
        )
        from triplens.storage.minio_client import (
            get_minio_client,
        )

        client = get_minio_client()

        print(
            "Starting REST Countries extraction..."
        )

        countries = get_all_countries()

        print(
            f"Countries extracted: {len(countries)}"
        )

        # -------------------------------------------------
        # SOURCE DATA QUALITY GATE
        # -------------------------------------------------

        minimum_country_records = 200

        print(
            "Running REST Countries source data quality "
            "checks..."
        )

        # Check 1: minimum record count
        if len(countries) < minimum_country_records:

            raise RuntimeError(
                "Country source quality check failed: "
                f"expected at least "
                f"{minimum_country_records} records, "
                f"but received {len(countries)}."
            )

        # Check 2: every record must be a dictionary
        invalid_record_types = [
            index
            for index, country in enumerate(countries)
            if not isinstance(country, dict)
        ]

        if invalid_record_types:

            raise RuntimeError(
                "Country source quality check failed: "
                f"{len(invalid_record_types)} records "
                "are not dictionaries."
            )

        # Check 3: every record must have a valid
        # common country name
        missing_country_names = []

        for index, country in enumerate(countries):

            names = country.get("names")

            if not isinstance(names, dict):

                missing_country_names.append(index)
                continue

            common_name = names.get("common")

            if (
                not isinstance(common_name, str)
                or not common_name.strip()
            ):

                missing_country_names.append(index)

        if missing_country_names:

            raise RuntimeError(
                "Country source quality check failed: "
                f"{len(missing_country_names)} records "
                "are missing a valid common country name."
            )

        # Check 4: codes must be a dictionary when present
        invalid_codes_structures = []

        for index, country in enumerate(countries):

            codes = country.get("codes")

            if codes is not None and not isinstance(
                codes,
                dict,
            ):

                invalid_codes_structures.append(index)

        if invalid_codes_structures:

            raise RuntimeError(
                "Country source quality check failed: "
                f"{len(invalid_codes_structures)} records "
                "contain an invalid codes structure."
            )

        # ISO3 is intentionally NOT required.
        #
        # Four legitimate special territories in the
        # current REST Countries response do not have
        # alpha-3 codes:
        #
        # - Abkhazia
        # - Northern Cyprus
        # - Somaliland
        # - South Ossetia

        missing_iso3_count = 0

        for country in countries:

            codes = country.get("codes") or {}
            alpha_3 = codes.get("alpha_3")

            if (
                not isinstance(alpha_3, str)
                or not alpha_3.strip()
            ):

                missing_iso3_count += 1

        print(
            "Country source quality checks passed."
        )

        print(
            f"Records received: {len(countries)}"
        )

        print(
            f"Minimum required records: "
            f"{minimum_country_records}"
        )

        print(
            f"Records missing ISO3: "
            f"{missing_iso3_count}"
        )

        # -------------------------------------------------
        # CACHE COUNTRIES
        # -------------------------------------------------

        cached_count = 0
        failed_count = 0

        for country in countries:

            country_name = (
                country.get("names", {}).get("common")
            )

            if not country_name:

                failed_count += 1
                continue

            try:

                get_cached_country(
                    country_name,
                    client=client,
                )

                cached_count += 1

            except Exception as exc:

                failed_count += 1

                print(
                    f"Failed to cache "
                    f"{country_name}: {exc}"
                )

        summary = {
            "countries_extracted": len(countries),
            "countries_cached": cached_count,
            "countries_failed": failed_count,
            "missing_iso3": missing_iso3_count,
            "quality_gate": "PASSED",
        }

        print(
            "Country extraction and caching completed."
        )

        print(summary)

        if failed_count > 0:

            raise RuntimeError(
                f"Country caching failed for "
                f"{failed_count} countries."
            )

        return summary

    # =====================================================
    # COUNTRY → SNOWFLAKE RAW
    # =====================================================

    @task(
        execution_timeout=timedelta(minutes=15)
    )
    def load_countries_to_snowflake(
        ingestion_summary: dict,
    ) -> dict:

        from triplens.extract.countries_api import (
            get_all_countries,
        )
        from triplens.load.snowflake_loader import (
            load_countries,
        )

        print(
            "Starting Snowflake RAW country load..."
        )

        countries = get_all_countries()

        print(
            "Countries retrieved for Snowflake load: "
            f"{len(countries)}"
        )

        result = load_countries(countries)

        print(
            "Snowflake RAW country load completed."
        )

        print(result)

        if result["total_failed"] > 0:

            raise RuntimeError(
                "Snowflake RAW country load "
                "completed with failures."
            )

        return {
            "countries_extracted": (
                ingestion_summary[
                    "countries_extracted"
                ]
            ),
            "countries_cached": (
                ingestion_summary[
                    "countries_cached"
                ]
            ),
            "countries_loaded": (
                result[
                    "total_succeeded"
                ]
            ),
            "status": result["status"],
        }

    # =====================================================
    # WEATHER INGESTION
    # =====================================================

    @task(
        execution_timeout=timedelta(minutes=30)
    )
    def ingest_weather(
        country_load_summary: dict,
    ) -> dict:

        from triplens.extract.countries_api import (
            get_all_countries,
        )
        from triplens.ingest.weather_ingestion import (
            ingest_weather_for_countries,
        )
        from triplens.storage.minio_client import (
            get_minio_client,
        )

        print(
            "Starting Open-Meteo weather ingestion..."
        )

        countries = get_all_countries()

        print(
            "Countries retrieved for weather processing: "
            f"{len(countries)}"
        )

        weather_countries = []
        skipped_countries = []

        for country in countries:

            codes = country.get("codes") or {}

            alpha_3 = codes.get("alpha_3")

            if (
                isinstance(alpha_3, str)
                and alpha_3.strip()
            ):

                weather_countries.append(country)

            else:

                country_name = (
                    country.get(
                        "names",
                        {},
                    ).get(
                        "common",
                        "UNKNOWN",
                    )
                )

                skipped_countries.append(
                    country_name
                )

        print(
            "Countries eligible for weather ingestion: "
            f"{len(weather_countries)}"
        )

        print(
            "Countries skipped because they have "
            "no ISO alpha-3 code: "
            f"{len(skipped_countries)}"
        )

        if skipped_countries:

            print(
                "Skipped countries: "
                f"{skipped_countries}"
            )

        client = get_minio_client()

        forecast_date = date.today().isoformat()

        result = ingest_weather_for_countries(
            country_records=weather_countries,
            forecast_date=forecast_date,
            client=client,
        )

        print(
            "Weather ingestion completed."
        )

        print(
            f"Requested: "
            f"{result['total_requested']}"
        )

        print(
            f"Unique: "
            f"{result['total_unique']}"
        )

        print(
            f"Succeeded: "
            f"{result['total_succeeded']}"
        )

        print(
            f"Failed: "
            f"{result['total_failed']}"
        )

        print(
            f"Cache hits: "
            f"{result['cache_hits']}"
        )

        print(
            f"Cache misses: "
            f"{result['cache_misses']}"
        )

        print(
            f"Duration: "
            f"{result['duration_seconds']} seconds"
        )

        print(
            f"Status: "
            f"{result['status']}"
        )

        if result["total_failed"] > 0:

            raise RuntimeError(
                "Weather ingestion completed "
                "with failures."
            )

        return {
            "countries_loaded": (
                country_load_summary[
                    "countries_loaded"
                ]
            ),
            "weather_countries_processed": (
                result["total_succeeded"]
            ),
            "weather_cache_hits": (
                result["cache_hits"]
            ),
            "weather_cache_misses": (
                result["cache_misses"]
            ),
            "forecast_date": forecast_date,
            "status": result["status"],
        }

    # =====================================================
    # WEATHER → SNOWFLAKE RAW
    # =====================================================

    @task(
        execution_timeout=timedelta(minutes=20)
    )
    def load_weather_to_snowflake(
        weather_summary: dict,
    ) -> dict:

        from triplens.extract.countries_api import (
            get_all_countries,
        )
        from triplens.load.weather_snowflake_loader import (
            load_weather_records,
        )

        print(
            "Starting Snowflake RAW weather load..."
        )

        forecast_date = weather_summary[
            "forecast_date"
        ]

        countries = get_all_countries()

        print(
            "Countries retrieved for weather "
            "Snowflake load: "
            f"{len(countries)}"
        )

        weather_country_codes = []

        skipped_countries = []

        for country in countries:

            codes = country.get("codes") or {}

            alpha_3 = codes.get("alpha_3")

            if (
                isinstance(alpha_3, str)
                and alpha_3.strip()
            ):

                weather_country_codes.append(
                    alpha_3.strip().upper()
                )

            else:

                country_name = (
                    country.get(
                        "names",
                        {},
                    ).get(
                        "common",
                        "UNKNOWN",
                    )
                )

                skipped_countries.append(
                    country_name
                )

        print(
            "Weather country codes eligible for "
            "Snowflake load: "
            f"{len(weather_country_codes)}"
        )

        print(
            "Countries skipped because they have "
            "no ISO alpha-3 code: "
            f"{len(skipped_countries)}"
        )

        result = load_weather_records(
            country_codes=weather_country_codes,
            forecast_date=forecast_date,
        )

        print(
            "Snowflake RAW weather load completed."
        )

        print(
            f"Requested: "
            f"{result['total_requested']}"
        )

        print(
            f"Unique: "
            f"{result['total_unique']}"
        )

        print(
            f"Succeeded: "
            f"{result['total_succeeded']}"
        )

        print(
            f"Failed: "
            f"{result['total_failed']}"
        )

        print(
            f"Status: "
            f"{result['status']}"
        )

        if result["total_failed"] > 0:

            raise RuntimeError(
                "Snowflake RAW weather load "
                "completed with failures."
            )

        return {
            "weather_records_requested": (
                result["total_requested"]
            ),
            "weather_records_loaded": (
                result["total_succeeded"]
            ),
            "weather_records_failed": (
                result["total_failed"]
            ),
            "forecast_date": forecast_date,
            "status": result["status"],
        }

    # =====================================================
    # DBT TRANSFORMATION
    # =====================================================

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt build --profiles-dir .",
        cwd="/usr/local/airflow/triplens_dbt",
        execution_timeout=timedelta(minutes=20),
    )

    # =====================================================
    # TASK DEPENDENCIES
    # =====================================================

    cache_result = extract_and_cache_countries()

    country_load_result = (
        load_countries_to_snowflake(
            cache_result
        )
    )

    weather_result = ingest_weather(
        country_load_result
    )

    weather_snowflake_result = (
        load_weather_to_snowflake(
            weather_result
        )
    )

    (
        cache_result
        >> country_load_result
        >> weather_result
        >> weather_snowflake_result
        >> run_dbt
    )


triplens_country_pipeline()