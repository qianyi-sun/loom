from __future__ import annotations

from uuid import uuid4

from loom.pipeline.work_protocol import TrialClaimV1, WorkClaimV1


def test_unified_trial_payload_preserves_legacy_claim_fields_exactly() -> None:
    legacy = {
        "trial_id": uuid4(),
        "team_id": uuid4(),
        "task_id": "benchmark/task",
        "config": {"agent": {"name": "codex"}},
        "requires_caps": {"os": "linux", "gpu_vendor": "none"},
        "attempt_count": 1,
        "provider_connection_id": None,
        "family_key": None,
        "family_state_uri": None,
        "family_run_spec": None,
        "state": "claimed",
    }
    claim = TrialClaimV1.model_validate(legacy)
    assert set(claim.model_dump()) == set(legacy)
    envelope = WorkClaimV1(
        schema_version="loom.work-claim.v1", work_kind="trial", payload=claim
    )
    assert envelope.payload == claim
