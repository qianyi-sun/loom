"""Disposable MinIO fixture images from the upstream Quay registry.

Keep the Testcontainers legacy release and the existing TLS fixture digest:
changing registry availability must not silently upgrade storage semantics.
"""

import subprocess

MINIO_TESTCONTAINERS_IMAGE = (
    "quay.io/minio/minio:RELEASE.2022-12-02T19-19-22Z"
    "@sha256:031fd97adca056cdbfd416a26670898d93adb9a79a3cf126aac5a41ddc22c5a3"
)
MINIO_TLS_IMAGE = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"


def prepare_test_images() -> None:
    """CI setup only: cold downloads have a bounded budget before pytest starts."""
    for image in (MINIO_TESTCONTAINERS_IMAGE, MINIO_TLS_IMAGE):
        cached = subprocess.run(["docker", "image", "inspect", image], check=False,
            timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if cached.returncode != 0:
            subprocess.run(["docker", "pull", image], check=True, timeout=180)


if __name__ == "__main__":
    prepare_test_images()
