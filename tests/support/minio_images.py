"""Disposable MinIO fixture images from the upstream Quay registry.

Keep the Testcontainers legacy release and the existing TLS fixture digest:
changing registry availability must not silently upgrade storage semantics.
"""

MINIO_TESTCONTAINERS_IMAGE = (
    "quay.io/minio/minio:RELEASE.2022-12-02T19-19-22Z"
    "@sha256:031fd97adca056cdbfd416a26670898d93adb9a79a3cf126aac5a41ddc22c5a3"
)
MINIO_TLS_IMAGE = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
