from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "benchmark_score_alignment_gate.py"
MANIFEST = ROOT / "docs" / "score-alignment" / "manifest.json"
LAYER3_DOC = ROOT / "docs" / "score-alignment" / "layer-3.md"


def _load_module():
    spec = importlib.util.spec_from_file_location("benchmark_score_alignment_gate", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _benchmark(manifest: dict, benchmark_id: str) -> dict:
    return next(row for row in manifest["benchmarks"] if row["benchmark_id"] == benchmark_id)


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


def test_terminal_bench_2_layer3_manifest_records_preliminary_nonfinal_evidence() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    tb2 = _benchmark(manifest, "terminal-bench-2")
    physical = _benchmark(manifest, "terminal-bench-2@tb2.1-r6")

    assert "89" in physical["score_semantics"]["denominator"]
    assert physical["harbor_support"]["status"] == "supported"

    layer3 = tb2["layer3_evidence"]
    canonical = tb2["canonical_reference"]
    assert canonical["source_type"] == "harbor_hub_dataset"
    assert "terminal-bench-2-1/6" in canonical["url"]
    assert "89" in canonical["justification"]
    assert "terminal-bench-2@tb2.1-r6" in canonical["justification"]
    score = tb2["score_semantics"]
    assert "89" in score["denominator"]
    assert "including 0" in score["task_reward"]
    assert "platform/verifier failure" in score["task_reward"]
    assert layer3["status"] == "preliminary_pending_terminus_2_rerun"
    assert layer3["canonical_acceptance_status"] == "pending_terminus_2_rerun"
    assert "not canonical acceptance" in layer3["acceptance_caveat"].lower()
    assert "terminus-2" in layer3["acceptance_caveat"].lower()
    assert "trajectory_flush_failed" in layer3["acceptance_caveat"]
    assert "historical tb2.0" in layer3["acceptance_caveat"].lower()
    assert "not rev-6 profile evidence" in layer3["acceptance_caveat"].lower()

    runs = layer3["runs"]
    assert len(runs) == 1
    run = runs[0]
    assert run["agent"] == "claude-code"
    assert run["model"] == "claude-haiku-4-5"
    assert run["trial_count"] == 86
    assert run["reward_positive_count"] == 35
    assert run["reward_positive_rate"] == 35 / 86
    assert run["reward_positive_rate_display"] == "40.70%"
    assert run["clean_succeeded_reward_positive_count"] == 33
    assert run["clean_succeeded_reward_positive_rate"] == 33 / 86
    assert run["clean_succeeded_reward_positive_rate_display"] == "38.37%"
    assert run["trajectory_flush_failed_reward_positive_count"] == 2
    assert run["system_failures"] == 12
    assert run["upstream_reference_rate_display"] == "~40.2%"
    assert run["delta_pp"] == 0.50
    assert run["per_task_table"] == "docs/evidence/issue-222/per-task-results.json"
    assert "preliminary" in run["verdict"].lower()
    assert "not canonical acceptance" in run["verdict"].lower()
    assert "trajectory_flush_failed" in run["verdict"]


def test_terminal_bench_2_layer3_doc_matches_manifest_and_keeps_rerun_caveat() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    run = _benchmark(manifest, "terminal-bench-2")["layer3_evidence"]["runs"][0]
    doc = LAYER3_DOC.read_text(encoding="utf-8")

    assert "## terminal-bench-2" in doc
    assert "preliminary" in doc.lower()
    assert "not canonical acceptance" in doc.lower()
    assert "Terminus-2 rerun" in doc
    assert f"{run['reward_positive_count']}/{run['trial_count']}" in doc
    assert run["reward_positive_rate_display"] in doc
    assert f"{run['clean_succeeded_reward_positive_count']}/{run['trial_count']}" in doc
    assert run["clean_succeeded_reward_positive_rate_display"] in doc
    assert "trajectory_flush_failed" in doc
    assert f"{run['system_failures']} system failures" in doc
    assert run["upstream_reference_rate_display"] in doc
    assert f"{run['delta_pp']:.2f} pp" in doc
    assert run["per_task_table"] in doc
