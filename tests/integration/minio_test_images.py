"""Pinned upstream image for fixtures using testcontainers' MinIO release.

Use MinIO's own Quay publication of RELEASE.2022-12-02T19-19-22Z, the version
intended by testcontainers' default. Pin its multi-platform manifest explicitly
instead of depending on the SDK's implicit Docker Hub image source.
"""

MINIO_TEST_IMAGE = (
    "quay.io/minio/minio@sha256:"
    "031fd97adca056cdbfd416a26670898d93adb9a79a3cf126aac5a41ddc22c5a3"
)
