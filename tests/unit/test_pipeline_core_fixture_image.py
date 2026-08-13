from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
)
from loom.pipeline_fixture import FixtureContractError, run_stage

ROOT = Path(__file__).resolve().parents[2]


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_image_build_inputs_are_minimal_pinned_and_closed() -> None:
    dockerfile = (ROOT / "deploy/Dockerfile.pipeline-core-fixture").read_text()
    assert "FROM python:3.12.13-slim-bookworm@sha256:" in dockerfile
    assert dockerfile.count("COPY ") == 2
    assert "pip install" not in dockerfile
    assert "curl " not in dockerfile
    assert "USER 65532:65532" in dockerfile
    assert "ENTRYPOINT [\"python\", \"-I\", \"-m\", \"loom.pipeline_fixture\"]" in dockerfile
    assert PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY == (
        "ghcr.io/qianyi-sun/loom-pipeline-core-fixture"
    )


def test_module_self_check_works_from_a_clean_python_process() -> None:
    result = subprocess.run(
        [sys.executable, "-I", str(ROOT / "src/loom/pipeline_fixture.py"), "--self-check"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "fixture": "pipeline-core-fixture@1",
        "status": "ok",
    }


def test_fixture_seed_and_transform_are_deterministic_and_fail_closed(tmp_path: Path) -> None:
    inputs = tmp_path / "inputs"
    outputs = tmp_path / "outputs"
    inputs.mkdir()
    outputs.mkdir()
    assert run_stage("seed_set", inputs_root=inputs, outputs_root=outputs) == "seeded"
    seed = (outputs / "seed/artifact.json").read_bytes()
    assert (outputs / "seed").stat().st_mode & 0o777 == 0o755
    assert (outputs / "seed/artifact.json").stat().st_mode & 0o777 == 0o644

    replay_outputs = tmp_path / "replay-outputs"
    replay_outputs.mkdir()
    assert run_stage("seed_set", inputs_root=inputs, outputs_root=replay_outputs) == "seeded"
    assert (replay_outputs / "seed/artifact.json").read_bytes() == seed

    with pytest.raises(FixtureContractError, match="empty real directory"):
        run_stage("seed_set", inputs_root=inputs, outputs_root=outputs)
    with pytest.raises(FixtureContractError, match="unknown"):
        run_stage("publisher", inputs_root=inputs, outputs_root=tmp_path)  # type: ignore[arg-type]


def test_fixture_runs_index_aggregate_and_readback_contracts(tmp_path: Path) -> None:
    seed_inputs = tmp_path / "seed-inputs"
    seed_outputs = tmp_path / "seed-outputs"
    seed_inputs.mkdir()
    seed_outputs.mkdir()
    _write_json(
        seed_inputs / "seed/artifact.json",
        {"files": [], "payload": {"value": {}}, "schema_version": "loom.pipeline-core-seed.v1"},
    )
    assert run_stage("produce_index", inputs_root=seed_inputs, outputs_root=seed_outputs) == "indexed"
    index = json.loads((seed_outputs / "index/artifact.json").read_bytes())
    assert [item["shard_key"] for item in index["items"]] == ["item-000", "item-001"]

    aggregate_inputs = tmp_path / "aggregate-inputs"
    aggregate_outputs = tmp_path / "aggregate-outputs"
    aggregate_inputs.mkdir()
    aggregate_outputs.mkdir()
    _write_json(
        aggregate_inputs / "stage-request.json",
        {
            "schema_version": "loom.pipeline-core-aggregate-request.v1",
            "transforms": [{"shard_key": "item-000"}, {"shard_key": "item-001"}],
        },
    )
    assert run_stage("aggregate", inputs_root=aggregate_inputs, outputs_root=aggregate_outputs) == (
        "pass"
    )

    receipt_inputs = tmp_path / "receipt-inputs"
    receipt_outputs = tmp_path / "receipt-outputs"
    receipt_inputs.mkdir()
    receipt_outputs.mkdir()
    _write_json(
        receipt_inputs / "aggregate/artifact.json",
        {
            "files": [],
            "payload": {"value": {"transform_count": 2}},
            "schema_version": "loom.pipeline-core-aggregate.v1",
        },
    )
    assert run_stage(
        "local_artifact_readback", inputs_root=receipt_inputs, outputs_root=receipt_outputs
    ) == "verified"
