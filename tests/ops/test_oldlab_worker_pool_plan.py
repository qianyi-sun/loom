from __future__ import annotations

import csv
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PLAN = _ROOT / "deploy" / "worker-pools" / "oldlab" / "worker-plan.csv"
_CONTROLLER_ENV = (
    _ROOT / "deploy" / "worker-pools" / "oldlab" / "controller.env.example"
)
_README = _ROOT / "deploy" / "worker-pools" / "oldlab" / "README.md"
_DRY_RUN = _ROOT / "deploy" / "worker-pools" / "oldlab" / "dry-run-2026-06-24.txt"
_SMOKE_EVIDENCE = (
    _ROOT / "deploy" / "worker-pools" / "oldlab" / "smoke-evidence-2026-06-24.json"
)

_EXPECTED_NODES = (
    "TRT-EAI-OLDLAB-1",
    "trt-EAI-OLDLAB-2",
    "trt-eai-oldlab-3",
    "trt-eai-oldlab-4",
    "trt-eai-oldlab-5",
)


def _rows() -> list[dict[str, str]]:
    with _PLAN.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_oldlab_plan_includes_all_five_staged_nodes() -> None:
    rows = _rows()

    assert [row["host"] for row in rows] == list(_EXPECTED_NODES)
    assert {row["status"] for row in rows} == {"include"}
    assert all(row["reason"].strip() for row in rows)


def test_oldlab_plan_uses_initial_elastic_capacity_slice() -> None:
    for row in _rows():
        assert row["cpus"] == "12"
        assert row["mem_total_mib"] == "58000"
        assert row["recommended_concurrency"] == "6"


def test_oldlab_controller_env_matches_plan_nodes_and_caps() -> None:
    env = dict(
        line.split("=", maxsplit=1)
        for line in _CONTROLLER_ENV.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )

    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED"] == "true"
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES"] == ",".join(
        _EXPECTED_NODES,
    )
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_CPUS"] == "12"
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_MEMORY_MIB"] == "58000"
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_CONCURRENCY"] == "6"
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS"] == "5"
    assert env["LOOM_CP_SLURM_WORKER_CONTROLLER_REPO_DIR"].startswith(
        "/shared_work/",
    )


def test_oldlab_docs_require_node_visible_repo_and_idle_exit() -> None:
    readme = _README.read_text(encoding="utf-8")
    controller_env = _CONTROLLER_ENV.read_text(encoding="utf-8")

    assert "shared checkout path" in readme
    assert "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS" in readme
    assert "/shared_work/<operator>/loom-remote-worker" in controller_env


def test_oldlab_dry_run_covers_all_nodes_with_shared_repo_path() -> None:
    dry_run = _DRY_RUN.read_text(encoding="utf-8")

    for node in _EXPECTED_NODES:
        assert f"--nodelist={node}" in dry_run
    assert dry_run.count("sbatch ") == len(_EXPECTED_NODES)
    assert "/shared_work/qianyi/loom-remote-worker" in dry_run
    assert "/home/qianyi/dev/loom" not in dry_run


def test_oldlab_smoke_evidence_records_worker_capacity_fields() -> None:
    evidence = json.loads(_SMOKE_EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["batch_id"] == "a8e87938-6dc0-4646-97bd-c75b380e4719"
    assert evidence["runtime_seconds"] > 0
    assert evidence["failures"] == 0
    records = evidence["oldlab_worker_records"]
    assert len(records) == evidence["oldlab_workers"] == 2
    for record in records:
        assert record["node_name"] in {"trt-eai-oldlab-4", "trt-eai-oldlab-5"}
        assert record["slurm_job_id"]
        assert record["worker_id"]
        assert record["concurrency"] == 2
        assert record["trials_claimed"] > 0
        assert record["trials_failed"] == 0
        assert record["trials_with_result"] > 0
        assert record["trials_with_trajectory_index"] > 0
