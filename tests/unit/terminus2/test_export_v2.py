"""raw-harbor-tb2-v2 export unit tests (#745)."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.db.schema import Trial
from loom.models.trajectory import (
    AgentThoughtEvent,
    ChatMessage,
    EventKind,
    LLMCallEvent,
    Terminus2ArtifactRefEvent,
    Terminus2CommandEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
)
from loom.models.types import ModelSpec
from loom_service.delivery_export_tb2_v2 import (
    SECRET_PATTERNS,
    Tb2V2ExportError,
    build_per_trial_v2_bundle,
    build_terminal_transcript,
    parse_trajectory_events,
    resolve_native_artifacts,
    validate_v2_eligibility,
    validate_v2_joins,
)


def _base(**kwargs: object) -> dict[str, object]:
    base = {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": "main",
        "seq": 0,
    }
    base.update(kwargs)
    return base


def _trial(*, agent_name: str = "terminus-2") -> Trial:
    trial_id = uuid4()
    return Trial(
        id=trial_id,
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": agent_name},
        trajectory_index={
            "artifacts": [
                {
                    "step_name": "main",
                    "bucket": "artifacts",
                    "key": f"team/{trial_id}/main/.loom/agent/trajectory.json",
                    "size": 2,
                    "content_hash": "sha256:abc",
                }
            ]
        },
    )


def _provenance_event(**kwargs: object) -> Terminus2RuntimeProvenanceEvent:
    base = _base(seq=1)
    base.update(kwargs)
    return Terminus2RuntimeProvenanceEvent(
        **base,
        kind=EventKind.TERMINUS2_RUNTIME_PROVENANCE,
        loom_runtime_revision="1.0",
        harbor_compat_sha=HARBOR_COMPAT_SHA,
        parser_name="json",
        prompt_hash="abc",
        template_hashes={},
        benchmark_provenance=None,
    )


def _typed_turn_chain(*, trial_id: UUID, gw_id: str = "gw-1") -> list[object]:
    turn_id = "turn-1"
    return [
        _provenance_event(trial_id=trial_id, seq=1),
        LLMCallEvent(
            **_base(trial_id=trial_id, seq=2),
            kind=EventKind.LLM_CALL,
            model=ModelSpec(provider="openai", name="gpt-4"),
            rate_card_hash="h",
            system_prompt=None,
            messages=[ChatMessage(role="user", content="hi")],
            tools=None,
            tool_choice=None,
            response=ChatMessage(role="assistant", content="{}"),
            finish_reason="stop",
            input_tokens=1,
            cached_input_tokens=0,
            cache_write_tokens=0,
            output_tokens=1,
            thinking_tokens=0,
            provider_extras={},
            cost_usd_snapshot=0.0,
            duration_sec=0.1,
            streamed=False,
            time_to_first_token_sec=None,
            gateway_request_id=gw_id,
        ),
        Terminus2TurnEvent(
            **_base(trial_id=trial_id, seq=3),
            kind=EventKind.TERMINUS2_TURN,
            turn_id=turn_id,
            turn_index=0,
            gateway_request_id=gw_id,
            parse_state="ok",
            completion_state="continue",
        ),
        Terminus2CommandEvent(
            **_base(trial_id=trial_id, seq=4),
            kind=EventKind.TERMINUS2_COMMAND,
            turn_id=turn_id,
            command_batch_id="batch-1",
            command_id="cmd-1",
            index=0,
            keystrokes="ls\n",
            duration_sec=0.1,
        ),
        Terminus2TerminalObservationEvent(
            **_base(trial_id=trial_id, seq=5),
            kind=EventKind.TERMINUS2_TERMINAL_OBSERVATION,
            turn_id=turn_id,
            command_batch_id="batch-1",
            observation_id="obs-1",
            text="output\n",
            capture_source="incremental",
            byte_len=7,
            truncated=False,
            completeness="full",
            content_hash="abc",
            redaction_applied=False,
            is_aggregate=False,
        ),
        Terminus2ArtifactRefEvent(
            **_base(trial_id=trial_id, seq=6),
            kind=EventKind.TERMINUS2_ARTIFACT_REF,
            artifact_kind="terminus_2.pane",
            sandbox_path="/app/.loom/agent/trajectory.json",
            content_hash="deadbeef",
            size_bytes=2,
            share_policy="restricted",
        ),
    ]


def _messages_from_raw_log(raw_log: dict[str, object], *, normalize_tb2: bool = False) -> list[dict[str, object]]:
    del normalize_tb2
    request = raw_log.get("request")
    if isinstance(request, dict):
        body = request.get("body")
        if isinstance(body, dict) and isinstance(body.get("messages"), list):
            return list(body["messages"])
    return []


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        data = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(data)}


def test_parse_trajectory_events_reads_typed_jsonl() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)
    payload = b"".join(
        (json.dumps(event.model_dump(mode="json")) + "\n").encode() for event in events
    )
    parsed = parse_trajectory_events(io.BytesIO(payload))
    assert len(parsed) == len(events)


def test_validate_v2_eligibility_rejects_non_terminus2_agent() -> None:
    trial = _trial(agent_name="opencode")
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility([], trial)
    assert exc.value.code == "incompatible_agent"


def test_validate_v2_eligibility_rejects_missing_provenance() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                Terminus2TurnEvent(
                    **_base(),
                    kind=EventKind.TERMINUS2_TURN,
                    turn_id="t",
                    turn_index=0,
                    gateway_request_id="gw",
                    parse_state="ok",
                    completion_state="continue",
                )
            ],
            trial,
        )
    assert exc.value.code == "missing_provenance"


def test_validate_v2_eligibility_rejects_mixed_provenance() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                Terminus2RuntimeProvenanceEvent(
                    **_base(),
                    kind=EventKind.TERMINUS2_RUNTIME_PROVENANCE,
                    loom_runtime_revision="1.0",
                    harbor_compat_sha="wrong",
                    parser_name="json",
                    prompt_hash="abc",
                    template_hashes={},
                )
            ],
            trial,
        )
    assert exc.value.code == "mixed_provenance"


def test_validate_v2_eligibility_rejects_legacy_runtime_stream() -> None:
    trial = _trial()
    with pytest.raises(Tb2V2ExportError) as exc:
        validate_v2_eligibility(
            [
                _provenance_event(trial_id=trial.id),
                AgentThoughtEvent(
                    **_base(trial_id=trial.id, seq=2),
                    kind=EventKind.AGENT_THOUGHT,
                    content="legacy",
                ),
            ],
            trial,
        )
    assert exc.value.code == "legacy_runtime_stream"


def test_validate_v2_joins_requires_artifact_ref() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)[:-1]
    errors = validate_v2_joins(events)
    assert any("artifact_ref" in error for error in errors)


def test_build_terminal_transcript_emits_observation_rows() -> None:
    trial_id = uuid4()
    events = _typed_turn_chain(trial_id=trial_id)
    payload = build_terminal_transcript(events)
    rows = [json.loads(line) for line in payload.decode().splitlines()]
    assert rows[0]["text"] == "output\n"
    assert rows[0]["turn_id"] == "turn-1"


def test_resolve_native_artifacts_fails_on_hash_mismatch() -> None:
    trial = _trial()
    native = b"{}"
    key = trial.trajectory_index["artifacts"][0]["key"]
    client = _FakeS3({("artifacts", key): native})
    events = _typed_turn_chain(trial_id=trial.id)
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_native_artifacts(
            trial,
            events,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_native_artifact"


def test_build_per_trial_v2_bundle_happy_path() -> None:
    trial = _trial()
    native = b"{}"
    digest = hashlib.sha256(native).hexdigest()
    key = trial.trajectory_index["artifacts"][0]["key"]
    trial.trajectory_index["artifacts"][0]["content_hash"] = f"sha256:{digest}"
    events = _typed_turn_chain(trial_id=trial.id)
    events[-1] = Terminus2ArtifactRefEvent(
        **_base(trial_id=trial.id, seq=6),
        kind=EventKind.TERMINUS2_ARTIFACT_REF,
        artifact_kind="terminus_2.pane",
        sandbox_path="/app/.loom/agent/trajectory.json",
        content_hash=digest,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", key): native})
    bundle = build_per_trial_v2_bundle(
        trial=trial,
        events=events,
        calls=[],
        client=client,
        artifacts_bucket="artifacts",
        messages_from_raw_log=_messages_from_raw_log,
    )
    assert bundle.execution_trajectory["schema_version"] == "harbor-tb2-v2-projection"
    assert bundle.native_artifacts["native/harbor_trajectory.json"] == native


def test_secret_patterns_ignore_task_id_substrings() -> None:
    task_json = '{"task_id": "source-useful-5003/task-0001"}'
    assert "sk-" in task_json
    assert not any(pattern.search(task_json) for pattern in SECRET_PATTERNS)


def test_secret_patterns_detect_openai_key_shape() -> None:
    leaked = '{"token": "sk-abcdefghijklmnopqrstuvwxyz"}'
    assert any(pattern.search(leaked) for pattern in SECRET_PATTERNS)
