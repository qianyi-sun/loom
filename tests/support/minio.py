"""Shared digest-pinned MinIO image for storage integration tests."""

import urllib3
from testcontainers.minio import MinioContainer
from tests.integration.minio_test_images import MINIO_TEST_IMAGE

__all__ = ["MINIO_TEST_IMAGE", "ensure_test_bucket"]


def ensure_test_bucket(container: MinioContainer, bucket: str) -> None:
    """Expose fixture S3 errors immediately; do not sleep through its deadline.

    Pinned MinIO sends Retry-After:120 for several different 503 error codes.
    The SDK's default retry would hide those codes behind pytest's 60s timeout.
    Disable retries for these setup calls only, including the one create.
    """
    with urllib3.PoolManager(retries=False, timeout=urllib3.Timeout(connect=5, read=5)) as transport:
        client = container.get_client(http_client=transport)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
