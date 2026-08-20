from __future__ import annotations

import base64
import io
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException

from loom.pipeline.keys import digest_bytes
from loom_service.routes.terminalgen_corpora import (
    _response,
    _verify_object,
    canonical_manifest_bytes,
)


class _Body(io.BytesIO):
    pass


class _Client:
    def __init__(self, body: bytes, *, checksum: bool = True) -> None:
        self.body = body
        self.checksum = checksum

    def head_object(self, **_kwargs: object) -> dict[str, object]:
        result: dict[str, object] = {"ContentLength": len(self.body)}
        if self.checksum:
            result["ChecksumSHA256"] = base64.b64encode(
                bytes.fromhex(digest_bytes(self.body).removeprefix("sha256:"))
            ).decode()
        return result

    def get_object(self, **_kwargs: object) -> dict[str, object]:
        return {"Body": _Body(self.body)}


def test_public_projection_exposes_download_routes_without_object_keys() -> None:
    alias = SimpleNamespace(alias="terminalgen-current", generation=2)
    version = SimpleNamespace(
        id=UUID(int=1),
        corpus_id="terminalgen-authorized",
        corpus_version=3,
        version_sha256="sha256:" + "1" * 64,
        recipe_digest="sha256:" + "2" * 64,
        plan_identity_sha256="sha256:" + "3" * 64,
        task_count=9000,
        runtime_corpus_artifact_id=UUID(int=2),
        final_audit_artifact_id=UUID(int=3),
        taskset_smoke_task_count=500,
        taskset_smoke_sha256="sha256:" + "4" * 64,
        taskset_smoke_size_bytes=123,
        taskset_smoke_object_key="private/secret/key.tar",
        published_at=datetime.now(UTC),
    )
    payload = _response(alias, version).model_dump(mode="json")
    assert "taskset_smoke_object_key" not in payload
    assert payload["taskset_archive_download_path"].endswith("/taskset-smoke/archive")
    assert payload["taskset_manifest_download_path"].endswith("/taskset-smoke/manifest")


def test_download_readback_uses_checksum_then_bounded_hash_fallback() -> None:
    body = b"deterministic corpus smoke\n"
    expected = digest_bytes(body)
    _verify_object(
        _Client(body),
        bucket="artifacts",
        key="smoke.tar",
        expected_size=len(body),
        expected_sha256=expected,
    )
    _verify_object(
        _Client(body, checksum=False),
        bucket="artifacts",
        key="smoke.tar",
        expected_size=len(body),
        expected_sha256=expected,
    )
    with pytest.raises(HTTPException, match="integrity drift"):
        _verify_object(
            _Client(body + b"drift"),
            bucket="artifacts",
            key="smoke.tar",
            expected_size=len(body),
            expected_sha256=expected,
        )


def test_manifest_serialization_is_stable() -> None:
    value = {
        "kind": "UserTaskSet",
        "apiVersion": "loom.taskset/v1",
        "metadata": {"name": "terminalgen-smoke", "display_name": "TerminalGen Smoke"},
    }
    assert canonical_manifest_bytes(value) == canonical_manifest_bytes(dict(reversed(value.items())))
