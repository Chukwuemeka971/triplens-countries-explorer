
import pytest

from triplens.storage.minio_client import (
    get_minio_client,
    ensure_bucket_exists,
    MINIO_BUCKET,
    MinIOStorageError,
)


def test_minio_connection():
    client = get_minio_client()

    assert client is not None


def test_minio_bucket_exists_or_can_be_created():
    client = get_minio_client()

    ensure_bucket_exists(client)

    assert client.bucket_exists(MINIO_BUCKET)


def test_missing_minio_credentials(monkeypatch):
    import triplens.storage.minio_client as storage

    monkeypatch.setattr(storage, "MINIO_ACCESS_KEY", None)

    with pytest.raises(
        MinIOStorageError,
        match="credentials are not configured",
    ):
        storage.get_minio_client()

def test_minio_lifecycle_policy():
    client = get_minio_client()

    from triplens.storage.minio_client import configure_cache_lifecycle

    configure_cache_lifecycle(client)

    lifecycle = client.get_bucket_lifecycle(
        bucket_name=MINIO_BUCKET
    )

    assert lifecycle is not None
    assert len(lifecycle.rules) > 0
    assert lifecycle.rules[0].expiration.days == 3
