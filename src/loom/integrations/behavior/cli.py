"""The sole process entrypoint for Loom-native BEHAVIOR Pipeline stages."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.contracts import (
    MAX_ARTIFACT_DOCUMENT_BYTES,
    MAX_STAGE_REQUEST_BYTES,
    StageRequestV1,
    validate_artifact_document,
    validate_behavior_stage_result,
    validate_stage_request,
    validate_stage_result_document,
)
from loom.integrations.behavior.errors import (
    BehaviorContractError,
    BehaviorExitCode,
    BehaviorInterruptedError,
)
from loom.pipeline.spec import ContainerNodeV1
from loom.pipeline.state import StageResultInputV1, StageResultV1
from loom_worker.pipeline_attempt_workspace import (
    AttemptWorkspace,
    CommittedAttempt,
)

StageAdapter = Callable[[StageRequestV1, AttemptWorkspace], StageResultV1]


@dataclass(frozen=True)
class StageAdapterBinding:
    """Immutable adapter registration supplied by the stage implementation."""

    adapter: StageAdapter
    node: ContainerNodeV1 | None = None
    output_declarations: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.node is None and self.output_declarations is None:
            raise ValueError("stage adapter binding requires frozen output declarations")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m loom.integrations.behavior.cli")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="validate and invoke one BEHAVIOR stage adapter")
    run.add_argument("--request", type=Path, required=True)
    run.add_argument("--output-dir", type=Path, required=True)

    validate = commands.add_parser("validate", help="read-only validation of one canonical file")
    validate.add_argument("--kind", choices=("request", "result", "artifact"), required=True)
    validate.add_argument("--file", type=Path, required=True)
    return parser


def _validate(kind: str, path: Path) -> str:
    max_bytes = MAX_ARTIFACT_DOCUMENT_BYTES if kind == "artifact" else MAX_STAGE_REQUEST_BYTES
    value = load_canonical_document(path, max_bytes=max_bytes)
    if kind == "request":
        return str(validate_stage_request(value).schema_version)
    elif kind == "result":
        return str(validate_stage_result_document(value).schema_version)
    schema_version = getattr(validate_artifact_document(value), "schema_version", None)
    if not isinstance(schema_version, str):
        raise BehaviorContractError("Artifact model has no schema_version")
    return schema_version


def _run(request_path: Path, output_dir: Path) -> int:
    request = validate_stage_request(
        load_canonical_document(request_path, max_bytes=MAX_STAGE_REQUEST_BYTES)
    )
    if not output_dir.is_dir():
        raise BehaviorContractError("output-dir must be an existing directory")
    binding = _resolve_stage_adapter(request)
    if binding is None:
        raise BehaviorContractError(
            f"stage adapter is not installed for contract-only stage {request.stage.value}"
        )
    dispatch_stage(
        request,
        output_dir,
        adapter=binding.adapter,
        node=binding.node,
        output_declarations=binding.output_declarations,
    )
    return int(BehaviorExitCode.SUCCESS)


def _resolve_stage_adapter(request: StageRequestV1) -> StageAdapterBinding | None:
    """Resolve only adapters that are implemented and owned in this package."""

    if request.stage.value == "rollout":
        from loom.integrations.behavior.stages.rollout import rollout_stage_binding

        return rollout_stage_binding()
    return None


def dispatch_stage(
    request: StageRequestV1,
    output_dir: Path,
    *,
    adapter: StageAdapter,
    node: ContainerNodeV1 | None = None,
    output_declarations: Mapping[str, str] | None = None,
) -> CommittedAttempt:
    """Run one adapter through the sole worker-owned local output authority.

    Stage child issues register their implementation at this seam.  The adapter
    receives no output path, so all production writes must use the provided
    :class:`AttemptWorkspace`.
    """

    if node is None and output_declarations is None:
        raise BehaviorContractError("dispatcher requires frozen output declarations")
    workspace = AttemptWorkspace(
        output_dir,
        request.attempt_id,
        request.idempotency_key,
        node=node,
        output_declarations=output_declarations,
        final_output_bytes_limit=request.budget.final_output_bytes_limit,
        checkpoint_bytes_limit=request.budget.checkpoint_bytes_limit,
        checkpoint_min_interval_seconds=5,
        checkpoint_max_committed=20,
        resolved_input_bindings_digest=request.provenance.resolved_input_bindings_digest,
        execution_spec_digest=request.provenance.execution_spec_digest,
        recipe_digest=request.provenance.recipe_digest,
        image_digest=request.provenance.image_digest,
    )
    if output_dir.joinpath("COMPLETE.json").is_file():
        return workspace.validate_committed()
    result = validate_behavior_stage_result(
        adapter(request, workspace).model_dump(mode="json", exclude_none=False),
        stage=request.stage,
        exit_code=0,
    )
    expected_provenance = (
        request.run_id,
        request.stage_run_id,
        request.attempt_id,
        request.provenance.recipe_digest,
        request.provenance.execution_spec_digest,
        request.provenance.image_digest.rsplit("@", maxsplit=1)[-1],
    )
    actual_provenance = (
        result.provenance.pipeline_run_id,
        result.provenance.stage_run_id,
        result.provenance.execution_attempt_id,
        result.provenance.recipe_digest,
        result.provenance.execution_spec_digest,
        result.provenance.image_digest,
    )
    if actual_provenance != expected_provenance:
        raise BehaviorContractError("StageResult provenance does not match StageRequest")
    expected_inputs = [
        StageResultInputV1(
            binding_name=binding.binding_name,
            item_key=item.item_key,
            artifact_id=item.artifact_id,
            artifact_type=binding.artifact_type,
            manifest_sha256=item.manifest_sha256,
        )
        for binding in request.inputs
        for item in binding.items
    ]
    if result.inputs != expected_inputs:
        raise BehaviorContractError("StageResult inputs do not match StageRequest")
    return workspace.commit(result)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate":
            schema_version = _validate(args.kind, args.file)
            print(f"valid {args.kind}: {schema_version}")
            return int(BehaviorExitCode.SUCCESS)
        return _run(args.request, args.output_dir)
    except BehaviorInterruptedError as exc:
        return 128 + exc.signum
    except (BehaviorContractError, OSError, ValidationError, ValueError) as exc:
        print(f"contract error: {exc}", file=sys.stderr)
        return int(BehaviorExitCode.CONTRACT_ERROR)


if __name__ == "__main__":
    raise SystemExit(main())
