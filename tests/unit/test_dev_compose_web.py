from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


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


def test_loom_service_dev_container_uses_internal_minio_endpoint() -> None:
    """Downloads are service-proxied, so dev does not need public MinIO URLs."""
    compose = REPO_ROOT / "deploy" / "docker-compose.dev.yml"
    text = compose.read_text()
    service_block = text.split("\n  loom-service:\n", 1)[1].split(
        "\n\n  worker:", 1,
    )[0]

    assert "LOOM_SVC_MINIO_ENDPOINT: http://minio:9000" in service_block
    assert "LOOM_SVC_MINIO_PUBLIC_ENDPOINT" not in service_block
