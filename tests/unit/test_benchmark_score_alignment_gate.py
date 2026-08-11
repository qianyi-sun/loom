from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "benchmark_score_alignment_gate.py"
MANIFEST = ROOT / "docs" / "score-alignment" / "manifest.json"


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
    assert "13 benchmark score-alignment entries" in results[0].detail
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


def test_layer1_manifest_gate_rejects_coder_harbor_cloud_in_decision(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    manifest = copy.deepcopy(json.loads(MANIFEST.read_text(encoding="utf-8")))
    manifest["benchmarks"][0]["harbor_support"]["decision"] = (
        "Mirror coder-harbor-cloud parity target"
    )
    path = tmp_path / "alignment.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    results = gate.check_manifest(gate.load_manifest(path))

    assert any(r.status == "fail" for r in results)
    assert any("coder-harbor-cloud" in r.detail for r in results if r.status == "fail")


def test_layer1_manifest_gate_rejects_coder_harbor_cloud_in_parity_target(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    manifest = copy.deepcopy(json.loads(MANIFEST.read_text(encoding="utf-8")))
    manifest["benchmarks"][0]["harbor_support"]["parity_target"] = (
        "coder-harbor-cloud v1.2"
    )
    path = tmp_path / "alignment.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    results = gate.check_manifest(gate.load_manifest(path))

    assert any(r.status == "fail" for r in results)
    assert any("coder-harbor-cloud" in r.detail for r in results if r.status == "fail")


def test_manifest_declares_harbor_framework_reference() -> None:
    gate = _load_module()
    manifest = gate.load_manifest(MANIFEST)
    ref = manifest.get("harbor_reference")
    assert ref is not None, "manifest must declare harbor_reference at top level"
    assert ref.get("repo") == "harbor-framework/harbor"
    assert ref.get("url") == "https://github.com/harbor-framework/harbor"
    pinned = ref.get("pinned_commit")
    assert (
        isinstance(pinned, str)
        and len(pinned) == 40
        and all(c in "0123456789abcdef" for c in pinned)
    ), "harbor_reference.pinned_commit must be a 40-char hex sha"


def test_manifest_gate_rejects_missing_harbor_reference(tmp_path: Path) -> None:
    gate = _load_module()
    manifest = gate.load_manifest(MANIFEST)
    poisoned = copy.deepcopy(manifest)
    poisoned.pop("harbor_reference", None)
    bad = tmp_path / "alignment.json"
    bad.write_text(json.dumps(poisoned), encoding="utf-8")
    loaded = gate.load_manifest(bad)
    results = gate.check_manifest(loaded)
    assert any(
        r.status == "fail" and "harbor_reference" in r.detail for r in results
    ), (
        "expected a fail mentioning harbor_reference, got: "
        f"{[(r.status, r.detail) for r in results]}"
    )


def test_manifest_gate_rejects_wrong_harbor_repo(tmp_path: Path) -> None:
    gate = _load_module()
    manifest = gate.load_manifest(MANIFEST)
    poisoned = copy.deepcopy(manifest)
    poisoned["harbor_reference"]["repo"] = "coder-harbor-cloud"
    bad = tmp_path / "alignment.json"
    bad.write_text(json.dumps(poisoned), encoding="utf-8")
    loaded = gate.load_manifest(bad)
    results = gate.check_manifest(loaded)
    assert any(
        r.status == "fail" and "harbor-framework/harbor" in r.detail for r in results
    ), (
        "expected a fail naming the correct repo, got: "
        f"{[(r.status, r.detail) for r in results]}"
    )
