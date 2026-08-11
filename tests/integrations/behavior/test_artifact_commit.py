from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import pytest

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.cli import dispatch_stage
from loom.integrations.behavior.contracts import StageRequestV1, validate_stage_request
from loom.pipeline.state import (
    RetryClass,
    StageResultInputV1,
    StageResultOutputV1,
    StageResultProvenanceV1,
    StageResultV1,
)
from loom_worker.pipeline_attempt_workspace import AttemptWorkspace, AttemptWorkspaceError

_DIGEST = "sha256:" + "1" * 64
_IMAGE = "sha256:" + "2" * 64


def _result() -> StageResultV1:
    return StageResultV1(
        schema_version="loom.stage-result.v1",
        domain_outcome="compatible",
        reason_code="compatible",
        retry_class=RetryClass.NONE,
        inputs=[],
        outputs=[
            StageResultOutputV1(name="case_index", artifact_type="loom.platform-fanout-index.v1")
        ],
        metrics={},
        provenance=StageResultProvenanceV1(
            pipeline_run_id=UUID("00000000-0000-0000-0000-000000000001"),
            stage_run_id=UUID("00000000-0000-0000-0000-000000000002"),
            execution_attempt_id=UUID("00000000-0000-0000-0000-000000000003"),
            recipe_digest=_DIGEST,
            execution_spec_digest=_DIGEST,
            image_digest=_IMAGE,
        ),
        error=None,
    )


def _workspace(tmp_path: Path, **kwargs: object) -> AttemptWorkspace:
    return AttemptWorkspace(
        tmp_path,
        UUID("00000000-0000-0000-0000-000000000003"),
        "attempt-key",
        output_declarations={"case_index": "loom.platform-fanout-index.v1"},
        final_output_bytes_limit=1_000_000,
        **kwargs,
    )


def _write_index(workspace: AttemptWorkspace) -> None:
    workspace.write_artifact_json(
        "case_index", {"items": [], "schema_version": "loom.platform-fanout-index.v1"}
    )


def test_terminal_commit_is_canonical_and_replay_revalidates_every_byte(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    _write_index(workspace)

    committed = workspace.commit(_result())
    assert committed.complete.idempotency_key == "attempt-key"
    assert [item.name for item in committed.complete.outputs] == ["case_index"]
    assert committed.complete.outputs[0].files == []
    assert workspace.commit(_result()) == committed

    changed = _result().model_copy(update={"reason_code": "changed"})
    with pytest.raises(AttemptWorkspaceError, match="conflicting StageResult"):
        workspace.commit(changed)

    artifact = tmp_path / "artifacts" / "case_index" / "artifact.json"
    artifact.write_bytes(
        b'{"items":[{"output_name":"x","shard_key":"x"}],"schema_version":"loom.platform-fanout-index.v1"}\n'
    )
    with pytest.raises(AttemptWorkspaceError, match="inventory mismatch"):
        workspace.commit(_result())


@pytest.mark.parametrize(
    "boundary",
    [
        "terminal_before_artifacts_rename",
        "terminal_after_artifacts_rename",
        "terminal_after_stage_result",
        "terminal_during_complete_inventory",
        "terminal_after_complete",
    ],
)
def test_terminal_crash_boundaries_fail_closed_or_replay(tmp_path: Path, boundary: str) -> None:
    def crash(point: str) -> None:
        if point == boundary:
            raise RuntimeError("injected crash")

    workspace = _workspace(tmp_path, crash_injector=crash)
    _write_index(workspace)
    with pytest.raises(RuntimeError, match="injected crash"):
        workspace.commit(_result())

    retry = _workspace(tmp_path)
    if boundary == "terminal_after_complete":
        assert retry.commit(_result()).complete.idempotency_key == "attempt-key"
    else:
        with pytest.raises(AttemptWorkspaceError):
            retry.commit(_result())


def test_output_safety_budget_and_conflicting_key(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    with pytest.raises(ValueError, match="invalid component"):
        workspace.write_payload_bytes("case_index", "../escape", b"x")

    root = workspace.artifact_root("case_index")
    _write_index(workspace)
    os.symlink(tmp_path / "outside", root / "payload" / "link")
    with pytest.raises(AttemptWorkspaceError, match="forbidden filesystem object"):
        workspace.commit(_result())

    other = tmp_path / "other"
    other.mkdir()
    tiny = AttemptWorkspace(
        other,
        "attempt",
        "key",
        output_declarations={"case_index": "loom.platform-fanout-index.v1"},
        final_output_bytes_limit=1,
    )
    with pytest.raises(AttemptWorkspaceError, match="budget"):
        _write_index(tiny)


def test_extra_missing_and_hardlinked_files_are_rejected(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    root = workspace.artifact_root("case_index")
    workspace.write_payload_bytes("case_index", "payload/a", b"a")
    _write_index(workspace)
    os.link(root / "payload" / "a", root / "payload" / "b")
    with pytest.raises(AttemptWorkspaceError, match="hard-linked"):
        workspace.commit(_result())


def test_empty_output_commit_and_outside_write_fence(tmp_path: Path) -> None:
    empty = AttemptWorkspace(
        tmp_path,
        "empty-attempt",
        "empty-key",
        output_declarations={},
        final_output_bytes_limit=0,
    )
    empty_result = _result().model_copy(update={"outputs": []})
    assert empty.commit(empty_result).complete.outputs == []

    other = tmp_path / "other"
    other.mkdir()
    fenced = AttemptWorkspace(
        other,
        "attempt",
        "key",
        output_declarations={},
        final_output_bytes_limit=0,
    )
    (other / "payload").mkdir()
    with pytest.raises(AttemptWorkspaceError, match="outside the workspace"):
        fenced.commit(empty_result)


def test_cli_dispatcher_gives_adapter_only_the_attempt_workspace(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "contracts" / "rollout_request.json"
    request = validate_stage_request(load_canonical_document(fixture))

    def adapter(value: StageRequestV1, workspace: AttemptWorkspace) -> StageResultV1:
        assert value is request
        assert not hasattr(workspace, "output_dir")
        return StageResultV1(
            schema_version="loom.stage-result.v1",
            domain_outcome="rollout_failure",
            reason_code="task_failed",
            retry_class=RetryClass.NONE,
            inputs=[
                StageResultInputV1(
                    binding_name=binding.binding_name,
                    item_key=item.item_key,
                    artifact_id=item.artifact_id,
                    artifact_type=binding.artifact_type,
                    manifest_sha256=item.manifest_sha256,
                )
                for binding in value.inputs
                for item in binding.items
            ],
            outputs=[],
            metrics={},
            provenance=StageResultProvenanceV1(
                pipeline_run_id=value.run_id,
                stage_run_id=value.stage_run_id,
                execution_attempt_id=value.attempt_id,
                recipe_digest=value.provenance.recipe_digest,
                execution_spec_digest=value.provenance.execution_spec_digest,
                image_digest=value.provenance.image_digest.rsplit("@", maxsplit=1)[-1],
            ),
            error=None,
        )

    committed = dispatch_stage(request, tmp_path, adapter=adapter, output_declarations={})
    assert committed.complete.outputs == []
