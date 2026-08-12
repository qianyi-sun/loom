from __future__ import annotations

from uuid import UUID

import pytest

from loom.pipeline.checkpoint import (
    CheckpointPayloadFileV1,
    ExecutionCheckpointV1,
    execution_checkpoint_bytes,
    resume_compatibility_key,
)
from loom.pipeline.keys import digest_bytes

_D1 = "sha256:" + "1" * 64
_D2 = "sha256:" + "2" * 64
_D3 = "sha256:" + "3" * 64
_D4 = "sha256:" + "4" * 64
_LEDGER = b'{"ledger":true}\n'
_COMPLETE = b'{"complete":true}\n'


def _checkpoint() -> ExecutionCheckpointV1:
    return ExecutionCheckpointV1(
        schema_version="loom.execution-checkpoint.v1",
        pipeline_run_id=UUID(int=1),
        stage_run_id=UUID(int=2),
        attempt_id=UUID(int=3),
        sequence=0,
        recipe_digest=_D1,
        resolved_input_bindings_digest=_D2,
        execution_spec_digest=_D3,
        image_digest=_D4,
        resume_compatibility_key=resume_compatibility_key(
            recipe_digest=_D1,
            resolved_input_bindings_digest=_D2,
            execution_spec_digest=_D3,
            image_digest=_D4,
        ),
        inner_ledger_sha256=digest_bytes(_LEDGER),
        inner_complete_sha256=digest_bytes(_COMPLETE),
        files=[
            CheckpointPayloadFileV1(
                relative_path="COMPLETE.json",
                size_bytes=len(_COMPLETE),
                sha256=digest_bytes(_COMPLETE),
            ),
            CheckpointPayloadFileV1(
                relative_path="ledger.json",
                size_bytes=len(_LEDGER),
                sha256=digest_bytes(_LEDGER),
            ),
        ],
    )


def test_outer_checkpoint_is_closed_canonical_jcs_lf() -> None:
    checkpoint = _checkpoint()
    encoded = execution_checkpoint_bytes(checkpoint)
    assert encoded.endswith(b"\n") and not encoded.endswith(b"\n\n")
    assert ExecutionCheckpointV1.model_validate_json(encoded) == checkpoint
    assert checkpoint.exact_artifact_data_bytes == len(encoded) + len(_LEDGER) + len(_COMPLETE)


def test_outer_checkpoint_rejects_inventory_or_compatibility_drift() -> None:
    checkpoint = _checkpoint()
    with pytest.raises(ValueError, match="bytewise sorted"):
        ExecutionCheckpointV1.model_validate(
            checkpoint.model_copy(update={"files": list(reversed(checkpoint.files))}).model_dump()
        )
    with pytest.raises(ValueError, match="compatibility key"):
        ExecutionCheckpointV1.model_validate(
            checkpoint.model_copy(
                update={"resume_compatibility_key": "sha256:" + "f" * 64}
            ).model_dump()
        )


def test_checkpoint_policy_counts_only_exact_artifact_data_bytes() -> None:
    checkpoint = _checkpoint()
    checkpoint.require_within(checkpoint.exact_artifact_data_bytes)
    with pytest.raises(ValueError, match="checkpoint_too_large"):
        checkpoint.require_within(checkpoint.exact_artifact_data_bytes - 1)
