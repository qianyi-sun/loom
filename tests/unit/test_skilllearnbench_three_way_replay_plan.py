from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "alignment" / "skilllearnbench_three_way_replay_plan.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "skilllearnbench_three_way_replay_plan",
        SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_three_way_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "task_id",
        "official_reward",
        "loom_arm_reward",
        "loom_x86_reward",
        "concordance",
        "loom_arm_arch",
        "loom_x86_arch",
        "loom_arm_failure",
        "loom_x86_failure",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_replay_plan_classifies_synthetic_dissent_rows(tmp_path: Path) -> None:
    mod = _load_module()
    csv_path = tmp_path / "three-way.csv"
    _write_three_way_csv(
        csv_path,
        [
            {
                "task_id": "poster/poster-1",
                "official_reward": "1.0",
                "loom_arm_reward": "0.0",
                "loom_x86_reward": "0.0",
                "concordance": "loom_agrees_official_dissents",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
            {
                "task_id": "search/search-2",
                "official_reward": "0.0",
                "loom_arm_reward": "1.0",
                "loom_x86_reward": "1.0",
                "concordance": "loom_agrees_official_dissents",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
            {
                "task_id": "dbscan/dbscan-3",
                "official_reward": "1.0",
                "loom_arm_reward": "0.0",
                "loom_x86_reward": "1.0",
                "concordance": "arm_dissents",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
            {
                "task_id": "video/video-4",
                "official_reward": "1.0",
                "loom_arm_reward": "1.0",
                "loom_x86_reward": "0.0",
                "concordance": "x86_dissents",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
            {
                "task_id": "missing/missing-5",
                "official_reward": "",
                "loom_arm_reward": "1.0",
                "loom_x86_reward": "1.0",
                "concordance": "incomplete",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "trajectory_flush_failed",
            },
            {
                "task_id": "match/match-6",
                "official_reward": "0.0",
                "loom_arm_reward": "0.0",
                "loom_x86_reward": "0.0",
                "concordance": "three_way_match",
                "loom_arm_arch": "arm64",
                "loom_x86_arch": "x86_64",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
        ],
    )

    plan = mod.build_replay_plan(csv_path)

    assert plan["summary"]["total_rows"] == 6
    assert plan["summary"]["planned_rows"] == 5
    assert plan["summary"]["category_counts"] == {
        "architecture_specific_rerun_needed": 2,
        "incomplete_or_missing_evidence": 1,
        "likely_verifier_artifact_replay_needed": 1,
        "official_semantics_drift_candidate": 1,
    }
    categories = {row["task_id"]: row["category"] for row in plan["rows"]}
    assert categories["poster/poster-1"] == "likely_verifier_artifact_replay_needed"
    assert categories["search/search-2"] == "official_semantics_drift_candidate"
    assert categories["dbscan/dbscan-3"] == "architecture_specific_rerun_needed"
    assert categories["video/video-4"] == "architecture_specific_rerun_needed"
    assert categories["missing/missing-5"] == "incomplete_or_missing_evidence"
    assert [row["task_id"] for row in plan["rows"]] == [
        "poster/poster-1",
        "search/search-2",
        "dbscan/dbscan-3",
        "video/video-4",
        "missing/missing-5",
    ]
    assert plan["rows"][0]["safe_next_commands"][0].startswith("rg -n ")
    assert "official output, Loom artifacts, verifier stdout/stderr" in (
        " ".join(plan["rows"][0]["evidence_requirements"])
    )


def test_cli_writes_stable_json_and_markdown(tmp_path: Path) -> None:
    mod = _load_module()
    csv_path = tmp_path / "three-way.csv"
    out_json = tmp_path / "plan.json"
    out_md = tmp_path / "plan.md"
    _write_three_way_csv(
        csv_path,
        [
            {
                "task_id": "poster/poster-1",
                "official_reward": "1.0",
                "loom_arm_reward": "0.0",
                "loom_x86_reward": "0.0",
                "concordance": "loom_agrees_official_dissents",
                "loom_arm_arch": "",
                "loom_x86_arch": "",
                "loom_arm_failure": "",
                "loom_x86_failure": "",
            },
        ],
    )

    assert (
        mod.main(
            [
                "--three-way-csv",
                str(csv_path),
                "--out-json",
                str(out_json),
                "--out-md",
                str(out_md),
            ],
        )
        == 0
    )

    rendered_json = json.loads(out_json.read_text(encoding="utf-8"))
    rendered_md = out_md.read_text(encoding="utf-8")
    assert rendered_json["summary"]["category_counts"] == {
        "likely_verifier_artifact_replay_needed": 1,
    }
    assert "| poster/poster-1 | loom_agrees_official_dissents | 1.0 | 0.0 | 0.0 | likely_verifier_artifact_replay_needed |" in rendered_md
    assert "Replay evidence requirements" in rendered_md
    assert "sk-secret" not in out_json.read_text(encoding="utf-8")
    assert "ghp_" not in rendered_md
