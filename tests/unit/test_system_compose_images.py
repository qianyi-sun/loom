"""System and integration fixtures use the same available upstream MinIO image."""

from pathlib import Path

import yaml

from tests.integration.minio_test_images import MINIO_TEST_IMAGE


def test_system_minio_uses_the_pinned_upstream_fixture() -> None:
    root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((root / "deploy/docker-compose.test.yml").read_text())
    assert compose["services"]["minio"]["image"] == "${LOOM_SYSTEM_MINIO_IMAGE:-" + MINIO_TEST_IMAGE + "}"
