"""Worker image provenance tests (#744 Gate 1)."""

from __future__ import annotations

import json
from pathlib import Path

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.agent.terminus2.worker_provenance import (
    WORKER_PYTHON_VERSION,
    WORKER_WHEELS_REL,
    load_worker_wheel_provenance,
    worker_image_lock_pins,
    worker_image_provenance_summary,
)


def test_dockerfile_harbor_sha_matches_provenance() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.worker"
    text = dockerfile.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("ARG HARBOR_COMPAT_SHA="):
            assert line.split("=", 1)[1] == HARBOR_COMPAT_SHA
            return
    raise AssertionError("HARBOR_COMPAT_SHA ARG not found in Dockerfile.worker")


def test_worker_image_lock_pins_harbor_openai_litellm() -> None:
    pins = worker_image_lock_pins()
    assert pins["harbor"].endswith(HARBOR_COMPAT_SHA)
    assert pins["openai"].startswith("2.")
    assert pins["litellm"].startswith("1.")


def test_worker_wheel_provenance_matches_lock() -> None:
    wheels = load_worker_wheel_provenance()
    pins = worker_image_lock_pins()
    assert wheels["harbor_compat_sha"] == HARBOR_COMPAT_SHA
    assert wheels["python_version"] == WORKER_PYTHON_VERSION
    assert wheels["packages"]["openai"]["version"] == pins["openai"]
    assert wheels["packages"]["litellm"]["version"] == pins["litellm"]
    for pkg in ("openai", "litellm"):
        assert len(wheels["packages"][pkg]["sha256"]) == 64


def test_worker_image_provenance_summary() -> None:
    summary = worker_image_provenance_summary()
    assert summary["python_version"] == WORKER_PYTHON_VERSION
    assert summary["harbor_compat_sha"] == HARBOR_COMPAT_SHA
    assert summary["openai_version"] is not None
    assert summary["wheel_hashes"]["openai"]


def test_worker_wheels_json_is_valid() -> None:
    path = Path(__file__).resolve().parents[3] / WORKER_WHEELS_REL
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == "1"
