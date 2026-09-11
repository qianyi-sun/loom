"""One explicit MinIO release for real storage integration tests."""

# Official upstream registry: https://github.com/minio/minio/blob/master/docs/docker/README.md
# Preserve testcontainers' release; its Docker Hub default is no longer pullable.
MINIO_TEST_IMAGE = "quay.io/minio/minio:RELEASE.2022-12-02T19-19-22Z"
