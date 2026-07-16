"""Unit tests for #865 verifier audit artifact collection + delivery packing."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom.trial.step_runner import _artifact_patterns
from loom_service.delivery_export_tb2_v2 import (
    Tb2V2ExportError,
    resolve_verifier_artifacts,
)


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self._objects = objects

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        from botocore.exceptions import ClientError

        data = self._objects.get((Bucket, Key))
        if data is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        return {"Body": _Body(data)}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        return None


def _trial_with_verifier_artifacts(
    *,
    log_body: bytes,
    meta: dict[str, object] | None = None,
    share_status: str = "shared",
    content_hash: str | None = None,
    include_meta: bool = True,
) -> tuple[Trial, _FakeS3, str]:
    trial_id = uuid4()
    team_id = uuid4()
    log_key = f"{team_id}/{trial_id}/main/.loom/verifier/script.log"
    meta_key = f"{team_id}/{trial_id}/main/.loom/verifier/script.log.meta.json"
    digest = hashlib.sha256(log_body).hexdigest()
    artifacts = [
        {
            "step_name": "main",
            "bucket": "artifacts",
            "key": log_key,
            "size": len(log_body),
            "content_hash": content_hash or f"sha256:{digest}",
            "share_status": share_status,
            "blocked_reason": "secret" if share_status == "blocked" else None,
        }
    ]
    objects: dict[tuple[str, str], bytes] = {("artifacts", log_key): log_body}
    if include_meta:
        meta_payload = meta or {
            "schema_version": "1",
            "truncated": False,
            "original_bytes": len(log_body),
            "kept_bytes": len(log_body),
        }
        meta_bytes = (json.dumps(meta_payload) + "\n").encode()
        artifacts.append(
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": meta_key,
                "size": len(meta_bytes),
                "content_hash": f"sha256:{hashlib.sha256(meta_bytes).hexdigest()}",
                "share_status": "shared",
                "blocked_reason": None,
            }
        )
        objects[("artifacts", meta_key)] = meta_bytes

    trial = Trial(
        id=trial_id,
        team_id=team_id,
        task_id="source-useful/task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2"},
        trajectory_index={"artifacts": artifacts},
    )
    return trial, _FakeS3(objects), log_key


def test_artifact_patterns_adds_verifier_glob_for_terminus2() -> None:
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name="terminus-2"),
        verifier=SimpleNamespace(name="script"),
    )
    step = SimpleNamespace(artifacts=["output.txt"])
    patterns = _artifact_patterns(ctx, step)  # type: ignore[arg-type]
    assert "output.txt" in patterns
    assert ".loom/agent/**" in patterns
    assert ".loom/verifier/**" in patterns


def test_artifact_patterns_adds_verifier_glob_for_script_verifier() -> None:
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name="oracle"),
        verifier=SimpleNamespace(name="script"),
    )
    step = SimpleNamespace(artifacts=[])
    patterns = _artifact_patterns(ctx, step)  # type: ignore[arg-type]
    assert ".loom/verifier/**" in patterns
    assert ".loom/agent/**" not in patterns


def test_resolve_verifier_artifacts_happy_path() -> None:
    body = b"--- stdout ---\n3 passed\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        meta={"schema_version": "1", "truncated": True, "original_bytes": 9999},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="artifacts",
    )
    by_path = {item.archive_path: item for item in resolved}
    assert "verifier/script.log" in by_path
    assert "verifier/script.log.meta.json" in by_path
    log_entry = by_path["verifier/script.log"]
    assert log_entry.data == body
    assert log_entry.truncated is True
    assert log_entry.share_status == "shared"
    assert log_entry.content_hash.startswith("sha256:")
    assert log_entry.step_name == "main"


def test_resolve_verifier_artifacts_hash_mismatch() -> None:
    body = b"ok\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        content_hash="sha256:" + ("0" * 64),
        include_meta=False,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_hash_mismatch"


def test_resolve_verifier_artifacts_missing_object() -> None:
    body = b"ok\n"
    trial, client, key = _trial_with_verifier_artifacts(
        log_body=body,
        include_meta=False,
    )
    client._objects.clear()
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_verifier_artifact"
    assert key in str(exc.value.detail)


def test_resolve_verifier_artifacts_blocked_share_status() -> None:
    body = b"secret token sk-ABCDEFGHIJKLMNOP\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        share_status="blocked",
        include_meta=False,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_blocked"


def test_resolve_verifier_artifacts_secret_scan_fails() -> None:
    body = b"authorization: Bearer sk-ABCDEFGHIJKLMNOPQRST\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        include_meta=False,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "secret_scan_failed"


def test_resolve_verifier_artifacts_empty_when_absent() -> None:
    trial = Trial(
        id=uuid4(),
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2"},
        trajectory_index={"artifacts": []},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=_FakeS3({}),
        artifacts_bucket="artifacts",
    )
    assert resolved == []
