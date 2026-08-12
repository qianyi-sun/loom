"""Closed, dependency-free runtime for the generic Pipeline CPU fixture.

This module is copied verbatim into ``Dockerfile.pipeline-core-fixture``.  It
has no network, credential, provider, publisher, or arbitrary command surface.
The production container contract fixes ``/inputs`` and ``/outputs``; tests
call :func:`run_stage` with temporary roots without changing the CLI surface.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Final, Literal

Stage = Literal[
    "seed_set",
    "produce_index",
    "transform",
    "aggregate",
    "local_artifact_readback",
]

STAGES: Final[tuple[Stage, ...]] = (
    "aggregate",
    "local_artifact_readback",
    "produce_index",
    "seed_set",
    "transform",
)
INPUTS: Final = Path("/inputs")
OUTPUTS: Final = Path("/outputs")
MAX_INPUT_BYTES: Final = 1_048_576


class FixtureContractError(ValueError):
    """The immutable fixture invocation or materialized input is invalid."""


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _read_json(path: Path) -> object:
    try:
        details = path.lstat()
    except FileNotFoundError as exc:
        raise FixtureContractError(f"required input is missing: {path.name}") from exc
    if not path.is_file() or path.is_symlink() or details.st_nlink != 1:
        raise FixtureContractError("fixture inputs must be single-link regular files")
    if details.st_size > MAX_INPUT_BYTES:
        raise FixtureContractError("fixture input exceeds one MiB")
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FixtureContractError("fixture input is not JSON") from exc
    if _canonical(value) != raw:
        raise FixtureContractError("fixture input is not canonical JSON plus LF")
    return value


def _input_documents(root: Path) -> list[tuple[str, object]]:
    if not root.is_dir() or root.is_symlink():
        raise FixtureContractError("/inputs must be a real directory")
    result: list[tuple[str, object]] = []
    for path in sorted(root.rglob("artifact.json"), key=lambda item: item.as_posix().encode()):
        if not path.is_relative_to(root):
            raise FixtureContractError("input path escaped /inputs")
        result.append((path.relative_to(root).as_posix(), _read_json(path)))
    return result


def _require_schema(document: object, expected: str) -> None:
    if not isinstance(document, dict) or document.get("schema_version") != expected:
        raise FixtureContractError(f"fixture input must use {expected}")


def _artifact(schema_version: str, *, value: object) -> dict[str, object]:
    payload = _canonical(value)
    return {
        "files": [],
        "payload": {
            "value": value,
            "value_sha256": _digest(payload),
        },
        "schema_version": schema_version,
    }


def _write_artifact(root: Path, name: str, value: dict[str, object]) -> None:
    if name not in {
        "aggregate",
        "index",
        "item-000",
        "item-001",
        "receipt",
        "seed",
        "transformed",
    }:
        raise FixtureContractError("fixture output name is not allowlisted")
    target_root = root / name
    target_root.mkdir(mode=0o755)
    (target_root / "payload").mkdir(mode=0o755)
    target = target_root / "artifact.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    # The non-root container is the only writer, while the host worker must be
    # able to read and verify the completed output before committing it.
    fd = os.open(target, flags, 0o644)
    with os.fdopen(fd, "wb") as output:
        output.write(_canonical(value))
        output.flush()
        os.fsync(output.fileno())


def run_stage(stage: Stage, *, inputs_root: Path = INPUTS, outputs_root: Path = OUTPUTS) -> str:
    """Execute one allowlisted deterministic stage and return its domain outcome."""

    if stage not in STAGES:
        raise FixtureContractError("unknown pipeline fixture stage")
    if not outputs_root.is_dir() or outputs_root.is_symlink() or any(outputs_root.iterdir()):
        raise FixtureContractError("/outputs must be an empty real directory")
    documents = _input_documents(inputs_root)
    if stage == "seed_set":
        if documents:
            raise FixtureContractError("seed_set accepts no inputs")
        _write_artifact(
            outputs_root,
            "seed",
            _artifact("loom.pipeline-core-seed.v1", value={"items": ["item-000", "item-001"]}),
        )
        return "seeded"
    if stage == "produce_index":
        if len(documents) != 1:
            raise FixtureContractError("produce_index requires one seed")
        _require_schema(documents[0][1], "loom.pipeline-core-seed.v1")
        index: dict[str, object] = {
            "schema_version": "loom.platform-fanout-index.v1",
            "items": [
                {"output_name": "item-000", "shard_key": "item-000"},
                {"output_name": "item-001", "shard_key": "item-001"},
            ],
        }
        _write_artifact(outputs_root, "index", index)
        for ordinal in range(2):
            name = f"item-{ordinal:03d}"
            _write_artifact(
                outputs_root,
                name,
                _artifact("loom.pipeline-core-item.v1", value={"ordinal": ordinal}),
            )
        return "indexed"
    if stage == "transform":
        if len(documents) != 1:
            raise FixtureContractError("transform requires one fanout item")
        source_path, source = documents[0]
        _require_schema(source, "loom.pipeline-core-item.v1")
        _write_artifact(
            outputs_root,
            "transformed",
            _artifact(
                "loom.pipeline-core-transformed.v1",
                value={"source_path": source_path, "source_sha256": _digest(_canonical(source))},
            ),
        )
        return "transformed"
    if stage == "aggregate":
        request = _read_json(inputs_root / "stage-request.json")
        if not isinstance(request, dict) or request.get("schema_version") != (
            "loom.pipeline-core-aggregate-request.v1"
        ):
            raise FixtureContractError("aggregate requires its exact rendered request")
        transforms = request.get("transforms")
        if not isinstance(transforms, list) or len(transforms) != 2:
            raise FixtureContractError("aggregate requires exactly two transforms")
        _write_artifact(
            outputs_root,
            "aggregate",
            _artifact("loom.pipeline-core-aggregate.v1", value={"transform_count": 2}),
        )
        return "pass"
    if len(documents) != 1:
        raise FixtureContractError("local_artifact_readback requires one aggregate")
    source_path, source = documents[0]
    _require_schema(source, "loom.pipeline-core-aggregate.v1")
    _write_artifact(
        outputs_root,
        "receipt",
        _artifact(
            "loom.pipeline-core-receipt.v1",
            value={"source_path": source_path, "source_sha256": _digest(_canonical(source))},
        ),
    )
    return "verified"


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if values == ["--self-check"]:
        print(_canonical({"fixture": "pipeline-core-fixture@1", "status": "ok"}).decode(), end="")
        return 0
    if len(values) != 1 or values[0] not in STAGES:
        print("usage: pipeline-core-fixture STAGE", file=sys.stderr)
        return 64
    try:
        outcome = run_stage(values[0])
    except (FixtureContractError, OSError) as exc:
        print(f"fixture_contract_error: {exc}", file=sys.stderr)
        return 65
    print(_canonical({"domain_outcome": outcome}).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the image smoke
    raise SystemExit(main())
