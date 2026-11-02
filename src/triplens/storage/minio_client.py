
import json
import logging
import os
from io import BytesIO
from typing import Any

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from minio.lifecycleconfig import (
    Expiration,
    LifecycleConfig,
    Rule,
)

load_dotenv()

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "triplens-raw")

MINIO_SECURE = (
    os.getenv("MINIO_SECURE", "false").lower() == "true"
)

CACHE_EXPIRY_DAYS = 3


class MinIOStorageError(Exception):
    """Raised when a MinIO storage operation fails."""


def get_minio_client() -> Minio:
    """Create and return an authenticated MinIO client."""

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise MinIOStorageError(
            "MinIO credentials are not configured."
        )

    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )

        logger.info(
            "MinIO client initialized: %s",
            MINIO_ENDPOINT,
        )

        return client

    except Exception as exc:
        raise MinIOStorageError(
            "Failed to initialise MinIO client."
        ) from exc


def ensure_bucket_exists(client: Minio) -> None:
    """Create the configured bucket if it does not exist."""

    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)

            logger.info(
                "Created MinIO bucket: %s",
                MINIO_BUCKET,
            )
        else:
            logger.info(
                "MinIO bucket already exists: %s",
                MINIO_BUCKET,
            )

    except S3Error as exc:
        raise MinIOStorageError(
            f"Failed to verify or create bucket {MINIO_BUCKET}."
        ) from exc


def configure_cache_lifecycle(client: Minio) -> None:
    """
    Configure automatic expiration of cached API objects
    after three days.
    """
    try:
        lifecycle = LifecycleConfig(
            rules=[
                Rule(
                    status="Enabled",
                    rule_id="expire-api-cache-after-three-days",
                    expiration=Expiration(days=CACHE_EXPIRY_DAYS),
                )
            ]
        )

        client.set_bucket_lifecycle(
            bucket_name=MINIO_BUCKET,
            config=lifecycle,
        )

        logger.info(
            "Configured MinIO cache lifecycle: %s days",
            CACHE_EXPIRY_DAYS,
        )

    except S3Error as exc:
        raise MinIOStorageError(
            "Failed to configure MinIO bucket lifecycle."
        ) from exc


def upload_json(
    client: Minio,
    object_name: str,
    data: dict[str, Any] | list[Any],
) -> str:
    """Upload a JSON object to the configured MinIO bucket."""

    if not isinstance(object_name, str) or not object_name.strip():
        raise ValueError(
            "Object name must not be empty."
        )

    if not isinstance(data, (dict, list)):
        raise ValueError(
            "JSON data must be a dictionary or list."
        )

    try:
        json_data = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        stream = BytesIO(json_data)

        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=stream,
            length=len(json_data),
            content_type="application/json",
        )

        logger.info(
            "Uploaded JSON object: %s/%s",
            MINIO_BUCKET,
            object_name,
        )

        return object_name

    except (S3Error, TypeError, ValueError) as exc:
        raise MinIOStorageError(
            f"Failed to upload JSON object: {object_name}."
        ) from exc


def object_exists(
    client: Minio,
    object_name: str,
) -> bool:
    """Check whether an object exists in the configured bucket."""

    try:
        client.stat_object(
            MINIO_BUCKET,
            object_name,
        )

        return True

    except S3Error as exc:
        if exc.code in {
            "NoSuchKey",
            "NoSuchObject",
            "NoSuchBucket",
        }:
            return False

        raise MinIOStorageError(
            f"Failed to check object: {object_name}."
        ) from exc


def download_json(
    client: Minio,
    object_name: str,
) -> dict | list:
    """Download and decode a JSON object."""

    response = None

    try:
        response = client.get_object(
            MINIO_BUCKET,
            object_name,
        )

        content = response.read()

        return json.loads(
            content.decode("utf-8")
        )

    except S3Error as exc:
        raise MinIOStorageError(
            f"Failed to download JSON object: {object_name}."
        ) from exc

    except (ValueError, UnicodeDecodeError) as exc:
        raise MinIOStorageError(
            f"Invalid JSON object: {object_name}."
        ) from exc

    finally:
        if response is not None:
            response.close()
            response.release_conn()
