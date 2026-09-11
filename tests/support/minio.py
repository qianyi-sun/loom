"""Public immutable MinIO release for disposable storage integration fixtures.

This is RELEASE.2025-09-07T16-13-09Z, already used by the TLS bundle fixture.
Quay serves the same manifest bytes as the former Docker Hub digest, including
native amd64 and arm64 members. Do not inherit Testcontainers' Docker Hub default.
"""

MINIO_TEST_IMAGE = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
