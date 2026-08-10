"""The sole process entrypoint for Loom-native BEHAVIOR Pipeline stages."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.contracts import (
    MAX_ARTIFACT_DOCUMENT_BYTES,
    MAX_STAGE_REQUEST_BYTES,
    validate_artifact_document,
    validate_stage_request,
    validate_stage_result_document,
)
from loom.integrations.behavior.errors import BehaviorContractError, BehaviorExitCode


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
    # Stage execution belongs to the stage child issues.  Keeping the one
    # entrypoint here makes an absent adapter fail closed instead of creating a
    # second script/CLI path or silently treating validation as execution.
    request = validate_stage_request(
        load_canonical_document(request_path, max_bytes=MAX_STAGE_REQUEST_BYTES)
    )
    if not output_dir.is_dir():
        raise BehaviorContractError("output-dir must be an existing directory")
    raise BehaviorContractError(
        f"stage adapter is not installed for contract-only stage {request.stage.value}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate":
            schema_version = _validate(args.kind, args.file)
            print(f"valid {args.kind}: {schema_version}")
            return int(BehaviorExitCode.SUCCESS)
        return _run(args.request, args.output_dir)
    except (BehaviorContractError, OSError, ValidationError, ValueError) as exc:
        print(f"contract error: {exc}", file=sys.stderr)
        return int(BehaviorExitCode.CONTRACT_ERROR)


if __name__ == "__main__":
    raise SystemExit(main())
