from __future__ import annotations

import re
from pathlib import Path

from tests.support.minio_images import MINIO_TLS_IMAGE

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_dev_compose_minio_uses_quay_pin() -> None:
    """Local Compose must not depend on Docker Hub minio/minio (#1969 / #1462)."""
    compose = (REPO_ROOT / "deploy" / "docker-compose.dev.yml").read_text()
    minio_block = compose.split("\n  minio:\n", 1)[1].split("\n\n  ", 1)[0]
    image_line = next(
        line for line in minio_block.splitlines() if line.strip().startswith("image:")
    )
    # Prefer the current Quay release digest (not the 2022 testcontainers pin):
    # laptop volumes already store newer xl headers.
    assert image_line.strip() == f"image: {MINIO_TLS_IMAGE}"
    assert "image: minio/minio" not in compose


def test_dev_compose_gateway_has_minio_credentials() -> None:
    """GatewaySettings requires MinIO keys; keep local Compose aligned (#1462)."""
    compose = (REPO_ROOT / "deploy" / "docker-compose.dev.yml").read_text()
    gateway_block = compose.split("\n  llm-gateway:\n", 1)[1].split(
        "\n\n  control-plane:\n", 1,
    )[0]
    assert "LOOM_GW_MINIO_ENDPOINT: http://minio:9000" in gateway_block
    assert "LOOM_GW_MINIO_ACCESS_KEY:" in gateway_block
    assert "LOOM_GW_MINIO_SECRET_KEY:" in gateway_block


def test_web_dev_container_uses_lockfile_stable_bootstrap() -> None:
    """The dev web container must not rewrite bind-mounted package-lock.json."""
    compose = REPO_ROOT / "deploy" / "docker-compose.dev.yml"
    text = compose.read_text()
    web_block = text.split("\n  web:\n", 1)[1].split("\n\nvolumes:", 1)[0]
    command_line = next(line for line in web_block.splitlines() if line.strip().startswith("command:"))

    assert "image: node:20-slim" not in web_block
    assert re.search(r"(?m)^\s+image: node:20\.\d+\.\d+-slim$", web_block)
    assert "npm ci --no-audit --no-fund" in command_line
    assert "npm install" not in command_line


def test_web_image_runtime_files_are_readable_by_unprivileged_nginx() -> None:
    """The nginx entrypoint invokes scripts with `sh`, which requires read permission."""
    dockerfile = (REPO_ROOT / "deploy" / "Dockerfile.web").read_text(encoding="utf-8")

    assert "chmod 755 /docker-entrypoint.d/40-loom-frontend-config.sh" in dockerfile
    assert "chmod 644 /etc/nginx/conf.d/default.conf" in dockerfile
    assert "chmod +x /docker-entrypoint.d/40-loom-frontend-config.sh" not in dockerfile


def test_loom_service_dev_container_uses_internal_minio_endpoint() -> None:
    """Downloads are service-proxied, so dev does not need public MinIO URLs."""
    compose = REPO_ROOT / "deploy" / "docker-compose.dev.yml"
    text = compose.read_text()
    service_block = text.split("\n  loom-service:\n", 1)[1].split(
        "\n\n  worker:", 1,
    )[0]

    assert "LOOM_SVC_MINIO_ENDPOINT: http://minio:9000" in service_block
    assert "LOOM_SVC_MINIO_PUBLIC_ENDPOINT" not in service_block
