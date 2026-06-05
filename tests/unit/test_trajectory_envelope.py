from datetime import UTC, datetime
from uuid import uuid4

from loom.models.trajectory import EventKind, TrialStartEvent


def test_event_kind_values():
    for v in (
        "trial_start", "trial_end", "trial_error", "trial_cancelled",
        "step_start", "step_end",
        "env_start", "env_ready", "env_stop", "env_exec",
        "file_upload", "file_download",
        "llm_call", "tool_use", "agent_thought",
        "verifier_start", "verifier_end", "verifier_check",
        "network_policy_change",
        "worker_lost_claim", "worker_drain_interrupted",
    ):
        assert EventKind(v).value == v


def test_envelope_required_fields():
    e = TrialStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="main",
        seq=0,
        task_id="t",
        agent_name="oracle",
        agent_mode="out-of-box",
    )
    assert e.kind == EventKind.TRIAL_START
    assert e.seq == 0
