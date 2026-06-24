from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "benchmark_score_alignment_gate.py"
MANIFEST = ROOT / "docs" / "benchmark-score-alignment.json"


def _load_module():
    spec = importlib.util.spec_from_file_location("benchmark_score_alignment_gate", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_layer1_manifest_covers_every_v1_supported_benchmark() -> None:
    gate = _load_module()

    manifest = gate.load_manifest(MANIFEST)
    results = gate.check_manifest(manifest)

    assert [r.status for r in results] == ["pass"]
    assert results[0].check_id == "benchmark_score_alignment.layer1_manifest"
    assert "12 benchmark score-alignment entries" in results[0].detail
    assert gate.manifest_benchmark_ids(manifest) == sorted(
        gate.V1_SUPPORTED_BENCHMARK_IDS
    )


def test_layer1_manifest_gate_reports_missing_benchmark(tmp_path: Path) -> None:
    gate = _load_module()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    manifest["benchmarks"] = [
        row for row in manifest["benchmarks"] if row["benchmark_id"] != "mbpp"
    ]
    path = tmp_path / "alignment.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    results = gate.check_manifest(gate.load_manifest(path))

    assert [r.status for r in results] == ["fail"]
    assert "missing v1 benchmarks: mbpp" in results[0].detail


def test_layer1_manifest_gate_requires_evidence_case() -> None:
    gate = _load_module()
    manifest = gate.load_manifest(MANIFEST)
    manifest["benchmarks"][0]["layer1_evidence"]["cases"] = []

    results = gate.check_manifest(manifest)

    assert [r.status for r in results] == ["fail"]
    assert "missing layer1 evidence case" in results[0].detail
