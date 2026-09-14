from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from loom.db.schema import Trial
from loom.models.trajectory import (
    HermesArtifactRefEvent,
    HermesRuntimeProvenanceEvent,
)
from loom_service.delivery_export_hermes import (
    HermesExportError,
    resolve_native_artifacts,
    validate_hermes_eligibility,
)


def _trial(*, agent_name: str = "hermes") -> Trial:
    trial_id = uuid4()
    return Trial(
        id=trial_id,
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": agent_name},
        trajectory_index={"artifacts": []},
    )


def _common_fields(*, trial_id: UUID) -> dict[str, object]:
    return {
        "emitted_at": datetime.now(UTC),
        "trial_id": trial_id,
        "step_id": "main",
        "seq": 1,
    }


def test_eligibility_rejects_non_hermes_agent() -> None:
    trial = _trial(agent_name="terminus-2")
    with pytest.raises(HermesExportError) as exc:
        validate_hermes_eligibility([], trial)
    assert exc.value.code == "incompatible_agent"


def test_eligibility_requires_provenance_and_artifact_ref() -> None:
    trial = _trial()
    with pytest.raises(HermesExportError) as exc:
        validate_hermes_eligibility([], trial)
    assert exc.value.code == "missing_provenance"

    provenance = HermesRuntimeProvenanceEvent(
        **_common_fields(trial_id=trial.id),
        hermes_version="0.1.0",
        hermes_agent_ref="hermes@test",
        loom_bridge_revision="1.0",
    )
    with pytest.raises(HermesExportError) as exc:
        validate_hermes_eligibility([provenance], trial)
    assert exc.value.code == "missing_native_artifact"


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def test_resolve_native_artifacts_hash_mismatch_fail_closed() -> None:
    trial = _trial()
    native = json.dumps({"session_id": "s1", "messages": []}).encode()
    actual_hash = hashlib.sha256(native).hexdigest()
    artifact_key = f"{trial.team_id}/{trial.id}/main/.loom/agent/hermes_session.json"
    trial.trajectory_index = {
        "artifacts": [
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": artifact_key,
                "size": len(native),
                "content_hash": f"sha256:{actual_hash}",
            }
        ]
    }
    ref = HermesArtifactRefEvent(
        **_common_fields(trial_id=trial.id),
        artifact_kind="hermes.session",
        sandbox_path=".loom/agent/hermes_session.json",
        content_hash="deadbeef",
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", artifact_key): native})
    with pytest.raises(HermesExportError) as exc:
        resolve_native_artifacts(
            trial,
            [ref],
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_native_artifact"
    assert exc.value.detail["expected_hash"] == "deadbeef"
    assert exc.value.detail["actual_hash"] == actual_hash


def test_resolve_native_artifacts_success_with_restricted_ref() -> None:
    trial = _trial()
    native = json.dumps({"session_id": "s1", "messages": []}).encode()
    actual_hash = hashlib.sha256(native).hexdigest()
    artifact_key = f"{trial.team_id}/{trial.id}/main/.loom/agent/hermes_session.json"
    trial.trajectory_index = {
        "artifacts": [
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": artifact_key,
                "size": len(native),
                "content_hash": f"sha256:{actual_hash}",
            }
        ]
    }
    ref = HermesArtifactRefEvent(
        **_common_fields(trial_id=trial.id),
        artifact_kind="hermes.session",
        sandbox_path=".loom/agent/hermes_session.json",
        content_hash=actual_hash,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", artifact_key): native})
    resolved = resolve_native_artifacts(
        trial,
        [ref],
        client=client,
        artifacts_bucket="artifacts",
    )
    assert resolved["native/hermes_session.json"] == native
