from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/developer_sandbox_live_acceptance.py"
SCHEMA = REPO_ROOT / "docs/evidence/developer-sandbox-live-acceptance.schema.json"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("developer_sandbox_live_acceptance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ACCEPTANCE = _load_module()


def _run(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _iso(minutes: int, seconds: int = 0) -> str:
    value = datetime(2026, 7, 28, tzinfo=UTC) + timedelta(
        minutes=minutes,
        seconds=seconds,
    )
    return value.isoformat().replace("+00:00", "Z")


PHASE_BOUNDS = {
    "preflight": (0, 1),
    "baseline": (1, 2),
    "large_batch_burst": (2, 4),
    "fairness_contention": (4, 34),
    "mixed_non_loom": (34, 36),
    "cancel_cleanup": (36, 37),
    "ttl_cleanup": (37, 38),
    "submit_host_restart": (38, 39),
    "worker_crash": (39, 40),
    "final_drain": (40, 41),
}


def _phase_observed_at(phase: str, offset_seconds: int = 30) -> str:
    return _iso(PHASE_BOUNDS[phase][0], offset_seconds)


def _request_id(index: int) -> str:
    return str(uuid.UUID(int=index))


def _patch_live_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ACCEPTANCE, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_UID", os.getuid())
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_GID", os.getgid())
    monkeypatch.setattr(ACCEPTANCE.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        ACCEPTANCE.socket,
        "gethostname",
        lambda: f"{ACCEPTANCE.SUBMIT_HOST}.internal",
    )


def _runtime_envelope(
    sandbox: str,
    pool: str,
    index: int,
    candidate: str,
    tree: str,
) -> dict[str, Any]:
    job_id = str(1000 + index)
    job_path = f"/system.slice/slurmstepd.scope/job_{job_id}"
    if pool == "gb10":
        allocation = {
            "cpu_cores": 20,
            "memory_bytes": 115_000_000_000,
            "pids": 65_536,
            "gpu_count": 1,
            "tres": "cpu=20,mem=115000M,gres/gpu=1",
            "exclusive": False,
        }
        node = "trt-gb10-1"
    else:
        allocation = {
            "cpu_cores": 8,
            "memory_bytes": 32_000_000_000,
            "pids": 32_768,
            "gpu_count": 0,
            "tres": "cpu=8,mem=32000M",
            "exclusive": False,
        }
        node = "trt-eai-oldlab-1"
    containers = []
    for role_index, role in enumerate(ACCEPTANCE.CONTAINER_ROLES, start=1):
        limits = {
            "cpu_cores": 1,
            "memory_bytes": 1_000_000_000,
            "pids": 128,
        }
        containers.append(
            {
                "role": role,
                "container_id": f"{index * 16 + role_index:012x}",
                "cgroup_parent": job_path,
                "observed_cgroup_path": (f"{job_path}/docker/{index * 16 + role_index:012x}"),
                "limits": limits,
                "observed_limits": limits.copy(),
                "gpu_ids": ["GPU-0"] if pool == "gb10" and role == "trial" else [],
            },
        )
    return {
        "sandbox": sandbox,
        "pool": pool,
        "phase": "mixed_non_loom",
        "candidate_sha": candidate,
        "candidate_tree": tree,
        "observed_at": _phase_observed_at("mixed_non_loom"),
        "job_id": job_id,
        "node": node,
        "account": f"loom-dev-{sandbox}",
        "allocation": allocation,
        "cgroup": {
            "job_path": job_path,
            "controllers": ["cpu", "memory", "pids"],
            "delegated": True,
            "cpu_cores_max": allocation["cpu_cores"],
            "memory_bytes_max": allocation["memory_bytes"],
            "pids_max": allocation["pids"],
        },
        "containers": containers,
    }


def _evidence() -> dict[str, Any]:
    candidate = "a" * 40
    tree = "b" * 40
    session_id = "c" * 32
    phases = []
    for index, phase in enumerate(ACCEPTANCE.PHASES):
        phase_start, phase_finish = PHASE_BOUNDS[phase]
        duration_seconds = (phase_finish - phase_start) * 60
        phases.append(
            {
                "phase": phase,
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "started_at": _iso(phase_start),
                "finished_at": _iso(phase_finish),
                "deadline_seconds": duration_seconds,
                "status": "pass",
                "checkpoint_sha256": f"{index + 1:064x}",
            },
        )
    capacity_samples = []
    pair_index = 1
    for sandbox in ACCEPTANCE.SANDBOXES:
        for pool in ACCEPTANCE.POOLS:
            for phase_index, phase in enumerate(ACCEPTANCE.CAPACITY_PHASES, start=1):
                final = phase == "final_drain"
                capacity_samples.append(
                    {
                        "phase": phase,
                        "observed_at": _phase_observed_at(phase, pair_index),
                        "sandbox": sandbox,
                        "pool": pool,
                        "candidate_sha": candidate,
                        "candidate_tree": tree,
                        "request_id": _request_id(pair_index),
                        "lease_epoch": 1,
                        "observation_sequence": phase_index,
                        "requested_slots": 0 if final else 4,
                        "granted_slots": 0 if final else 2,
                        "pending_slots": 0 if final else 1,
                        "active_slots": 0 if final else 1,
                        "draining_slots": 0,
                        "terminal_slots": 2 if final else 0,
                    },
                )
            pair_index += 1
    runtime_envelopes = []
    index = 1
    for sandbox in ACCEPTANCE.SANDBOXES:
        for pool in ACCEPTANCE.POOLS:
            runtime_envelopes.append(
                _runtime_envelope(sandbox, pool, index, candidate, tree),
            )
            index += 1
    fairness = []
    for pool in ACCEPTANCE.POOLS:
        fairness.append(
            {
                "pool": pool,
                "phase": "fairness_contention",
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "started_at": _iso(PHASE_BOUNDS["fairness_contention"][0]),
                "finished_at": _iso(PHASE_BOUNDS["fairness_contention"][1]),
                "window_seconds": 1800,
                "max_grant_wait_seconds": 600,
                "max_grant_skew_ratio": 0.2,
                "participants": [
                    {
                        "sandbox": sandbox,
                        "requested_slots": 4,
                        "granted_slots_total": 8,
                        "grant_cycles": 2,
                        "first_grant_wait_seconds": 30,
                        "longest_starvation_seconds": 120,
                        "indefinite_starvation": False,
                    }
                    for sandbox in ACCEPTANCE.SANDBOXES
                ],
            },
        )
    peer_workloads = []
    for pool in ACCEPTANCE.POOLS:
        peer_workloads.append(
            {
                "pool": pool,
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "job_id": "9001" if pool == "oldlab" else "9002",
                "account": "research-peer",
                "baseline": {
                    "observed_at": _phase_observed_at("baseline"),
                    "running_jobs": 1,
                    "completed_jobs": 10,
                    "failed_jobs": 0,
                    "throughput_per_second": 10,
                    "p95_latency_seconds": 1,
                },
                "during": {
                    "observed_at": _phase_observed_at("mixed_non_loom"),
                    "running_jobs": 1,
                    "completed_jobs": 20,
                    "failed_jobs": 0,
                    "throughput_per_second": 9,
                    "p95_latency_seconds": 1.1,
                },
                "after": {
                    "observed_at": _phase_observed_at("final_drain"),
                    "running_jobs": 1,
                    "completed_jobs": 30,
                    "failed_jobs": 0,
                    "throughput_per_second": 10,
                    "p95_latency_seconds": 1,
                },
                "max_throughput_regression_ratio": 0.2,
                "disrupted": False,
            },
        )
    storage_io = [
        {
            "domain": pool,
            "candidate_sha": candidate,
            "candidate_tree": tree,
            "baseline_observed_at": _phase_observed_at("baseline"),
            "minimum_observed_at": _phase_observed_at("mixed_non_loom"),
            "after_observed_at": _phase_observed_at("final_drain"),
            "baseline_free_bytes": 1_000_000_000_000,
            "minimum_free_bytes": 900_000_000_000,
            "after_free_bytes": 950_000_000_000,
            "required_free_bytes": 100_000_000_000,
            "cache_peak_bytes": 10_000_000_000,
            "cache_limit_bytes": 20_000_000_000,
            "read_bytes": 100_000_000_000,
            "read_limit_bytes": 200_000_000_000,
            "write_bytes": 50_000_000_000,
            "write_limit_bytes": 100_000_000_000,
            "io_errors": 0,
            "enospc_events": 0,
        }
        for pool in ACCEPTANCE.POOLS
    ]
    fault_recovery = []
    for index, event in enumerate(ACCEPTANCE.FAULTS, start=1):
        phase = ACCEPTANCE.FAULT_PHASES[event]
        fault_recovery.append(
            {
                "event": event,
                "phase": phase,
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "sandbox": ACCEPTANCE.SANDBOXES[(index - 1) % 3],
                "pool": ACCEPTANCE.POOLS[(index - 1) % 2],
                "request_id": _request_id(100 + index),
                "injected_at": _phase_observed_at(phase, 10),
                "recovered_at": _phase_observed_at(phase, 50),
                "recovery_deadline_seconds": 600,
                "orphan_jobs": 0,
                "orphan_containers": 0,
                "orphan_leases": 0,
                "orphan_trials": 0,
                "retry_attribution": {
                    "interrupted_trials": 2,
                    "retryable_trials": 2,
                    "retried_trials": 2,
                    "duplicate_retries": 0,
                    "lost_trials": 0,
                    "unknown_attribution": 0,
                },
            },
        )
    return {
        "schema_version": 1,
        "candidate": {
            "sha": candidate,
            "tree": tree,
            "runtime_receipts": [
                {
                    "sandbox": sandbox,
                    "candidate_sha": candidate,
                    "candidate_tree": tree,
                    "collected_at": _iso(0),
                    "expires_at": _iso(60),
                    "payload_sha256": f"{index + 500:064x}",
                    "domain_generations": {
                        "oldlab": 10,
                        "gb10": 20,
                    },
                }
                for index, sandbox in enumerate(ACCEPTANCE.SANDBOXES)
            ],
        },
        "session": {
            "id": session_id,
            "submit_host": ACCEPTANCE.SUBMIT_HOST,
            "execute_acknowledged": True,
            "started_at": _iso(0),
            "completed_at": _iso(42),
            "collected_at": _iso(43),
            "max_collection_lag_seconds": 300,
        },
        "topology": {
            "sandboxes": list(ACCEPTANCE.SANDBOXES),
            "pools": list(ACCEPTANCE.POOLS),
            "eligible_nodes": list(ACCEPTANCE.EXPECTED_NODES),
            "excluded_nodes": ["trt-gb10-7"],
            "slot_budgets": ACCEPTANCE.POOL_SLOT_BUDGETS.copy(),
            "pending_slot_budgets": ACCEPTANCE.POOL_PENDING_BUDGETS.copy(),
        },
        "state_machine": phases,
        "cross_sandbox_negative": [
            {
                "phase": "baseline",
                "source": source,
                "target": target,
                "resource": resource,
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "observed_at": _phase_observed_at("baseline"),
                "denied": True,
            }
            for source in ACCEPTANCE.SANDBOXES
            for target in ACCEPTANCE.SANDBOXES
            if source != target
            for resource in ACCEPTANCE.CROSS_SANDBOX_RESOURCES
        ],
        "capacity_samples": capacity_samples,
        "large_batch_bursts": [
            {
                "pool": "oldlab",
                "phase": "large_batch_burst",
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "started_at": _iso(PHASE_BOUNDS["large_batch_burst"][0]),
                "finished_at": _iso(PHASE_BOUNDS["large_batch_burst"][1]),
                "batch_id": _request_id(201),
                "trial_count": 100,
                "completed_trials": 98,
                "failed_trials": 2,
                "cancelled_trials": 0,
                "duplicate_trial_ids": 0,
                "requested_slots": 20,
                "granted_slots": 20,
                "peak_active_slots": 20,
                "nodes": ["trt-eai-oldlab-1", "trt-eai-oldlab-3"],
                "node_trial_counts": {
                    "trt-eai-oldlab-1": 50,
                    "trt-eai-oldlab-3": 50,
                },
            },
            {
                "pool": "gb10",
                "phase": "large_batch_burst",
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "started_at": _iso(PHASE_BOUNDS["large_batch_burst"][0]),
                "finished_at": _iso(PHASE_BOUNDS["large_batch_burst"][1]),
                "batch_id": _request_id(202),
                "trial_count": 100,
                "completed_trials": 100,
                "failed_trials": 0,
                "cancelled_trials": 0,
                "duplicate_trial_ids": 0,
                "requested_slots": 140,
                "granted_slots": 140,
                "peak_active_slots": 120,
                "nodes": ["trt-gb10-1", "trt-gb10-2"],
                "node_trial_counts": {
                    "trt-gb10-1": 50,
                    "trt-gb10-2": 50,
                },
            },
        ],
        "fairness": fairness,
        "runtime_envelopes": runtime_envelopes,
        "peer_workloads": peer_workloads,
        "storage_io": storage_io,
        "fault_recovery": fault_recovery,
        "invariants": {
            "capacity_overshoot_events": 0,
            "duplicate_observations": 0,
            "duplicate_trials": 0,
            "indefinite_starvation_events": 0,
            "exclusive_slurm_jobs": 0,
            "cgroup_escape_events": 0,
            "peer_disruption_events": 0,
            "storage_error_events": 0,
            "orphan_jobs": 0,
            "orphan_containers": 0,
            "orphan_leases": 0,
            "orphan_trials": 0,
            "duplicate_retries": 0,
            "unattributed_retries": 0,
        },
    }


def _failures(evidence: dict[str, Any]) -> list[str]:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return list(ACCEPTANCE.verify_evidence(evidence, schema))


def _journaled_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, dict[str, Any], Path, dict[str, Any]]:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    session_id = state["session_id"]
    evidence = _evidence()
    evidence["session"]["id"] = session_id
    phase_dir = tmp_path / "phase-inputs"
    phase_dir.mkdir()
    for index, phase in enumerate(ACCEPTANCE.PHASES):
        phase_payload = evidence["state_machine"][index].copy()
        del phase_payload["checkpoint_sha256"]
        digest = hashlib.sha256(ACCEPTANCE._canonical_bytes(phase_payload)).hexdigest()
        evidence["state_machine"][index]["checkpoint_sha256"] = digest
        phase_path = phase_dir / f"{phase}.json"
        phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
        ACCEPTANCE.checkpoint_session(
            session_id,
            phase,
            phase_path,
            execute=True,
        )
    evidence_path = tmp_path / "final.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return session_id, evidence, evidence_path, schema


def test_schema_is_valid_and_complete_fixture_passes() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    assert _failures(_evidence()) == []


def test_default_plan_is_read_only_fixed_and_complete() -> None:
    completed = _run()

    assert completed.returncode == 0, completed.stderr
    plan = json.loads(completed.stdout)
    assert plan["mode"] == "plan_read_only"
    assert plan["live_mutations_supported"] is False
    assert plan["submit_host"] == "trt-eai-oldlab-2"
    assert plan["sandboxes"] == ["qianyi", "hongjian", "devansh"]
    assert plan["pools"] == ["oldlab", "gb10"]
    assert plan["excluded_nodes"] == ["trt-gb10-7"]
    assert plan["state_machine"] == list(ACCEPTANCE.PHASES)
    assert len(plan["stop_rules"]) >= 8


def test_session_mutation_requires_execute_before_host_checks() -> None:
    completed = _run(
        "session-start",
        "--candidate-sha",
        "a" * 40,
        "--candidate-tree",
        "b" * 40,
    )

    assert completed.returncode == 1
    assert "explicit --execute" in completed.stdout


def test_persistent_state_machine_is_ordered_and_candidate_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    session_id = state["session_id"]

    phase_evidence = {
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "phase": "preflight",
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="exact next phase"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "baseline",
            phase_path,
            execute=True,
        )

    advanced = ACCEPTANCE.checkpoint_session(
        session_id,
        "preflight",
        phase_path,
        execute=True,
    )
    assert advanced["completed_phases"] == ["preflight"]
    assert advanced["next_phase_index"] == 1
    persisted = json.loads(
        (tmp_path / "state/sessions" / session_id / "checkpoints/00-preflight.json").read_text(
            encoding="utf-8"
        ),
    )
    digest = hashlib.sha256(ACCEPTANCE._canonical_bytes(phase_evidence)).hexdigest()
    assert persisted["evidence_sha256"] == digest
    assert persisted["recorded_at"] == phase_evidence["finished_at"]


def test_complete_session_seals_verified_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )

    complete = ACCEPTANCE.finalize_session(
        session_id,
        evidence_path,
        schema,
        execute=True,
    )

    assert complete["status"] == "complete"
    assert len(complete["evidence_sha256"]) == 64
    sealed = tmp_path / "state/sessions" / session_id / "evidence.json"
    assert json.loads(sealed.read_text(encoding="utf-8")) == evidence


def test_state_tree_is_closed_root_only_and_rejects_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    target = tmp_path / "redirect"
    target.mkdir(mode=0o700)
    (tmp_path / "state").symlink_to(target, target_is_directory=True)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)


def test_state_tree_owner_modes_and_fqdn_host_are_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    session_dir = tmp_path / "state/sessions" / state["session_id"]

    for directory in (
        tmp_path / "state",
        tmp_path / "state/sessions",
        session_dir,
        session_dir / "checkpoints",
    ):
        metadata = directory.lstat()
        assert metadata.st_uid == os.getuid()
        assert metadata.st_gid == os.getgid()
        assert metadata.st_mode & 0o777 == 0o700
    for file_path in (session_dir / "state.json", session_dir / "session.lock"):
        metadata = file_path.lstat()
        assert metadata.st_mode & 0o777 == 0o600

    os.chmod(session_dir / "checkpoints", 0o755)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE._session_state(state["session_id"])


def test_state_file_owner_mismatch_fails_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_UID", os.getuid() + 1)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE._session_state(state["session_id"])


def test_late_checkpoint_create_failure_is_not_mistaken_for_durable_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    destination = (
        tmp_path
        / "state/sessions"
        / state["session_id"]
        / "checkpoints/00-preflight.json"
    )
    payload = {"schema_version": 1, "phase": "preflight"}
    original_fsync_directory = ACCEPTANCE._fsync_directory
    destination_fsync_attempts = 0

    def fail_first_destination_fsync(path: Path) -> None:
        nonlocal destination_fsync_attempts
        if path == destination.parent:
            destination_fsync_attempts += 1
            if destination_fsync_attempts == 1:
                raise OSError("simulated directory fsync failure")
        original_fsync_directory(path)

    monkeypatch.setattr(
        ACCEPTANCE,
        "_fsync_directory",
        fail_first_destination_fsync,
    )
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="cannot create"):
        ACCEPTANCE._write_or_verify_secure(destination, payload)
    assert destination.is_file()

    ACCEPTANCE._write_or_verify_secure(destination, payload)
    assert destination_fsync_attempts == 2


def test_checkpoint_is_crash_idempotent_and_rejects_changed_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    session_id = state["session_id"]
    phase_payload = {
        "phase": "preflight",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
    original_atomic = ACCEPTANCE._atomic_write
    failed = False

    def fail_state_once(path: Path, payload: dict[str, Any]) -> None:
        nonlocal failed
        if path.name == "state.json" and payload["next_phase_index"] == 1 and not failed:
            failed = True
            raise ACCEPTANCE.AcceptanceError("simulated state crash")
        original_atomic(path, payload)

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", fail_state_once)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="simulated state crash"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "preflight",
            phase_path,
            execute=True,
        )
    checkpoint = tmp_path / "state/sessions" / session_id / "checkpoints/00-preflight.json"
    assert checkpoint.is_file()
    assert ACCEPTANCE._session_state(session_id)["next_phase_index"] == 0

    changed = phase_payload.copy()
    changed["deadline_seconds"] = 601
    phase_path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="does not match"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "preflight",
            phase_path,
            execute=True,
        )

    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", original_atomic)
    recovered = ACCEPTANCE.checkpoint_session(
        session_id,
        "preflight",
        phase_path,
        execute=True,
    )
    assert recovered["next_phase_index"] == 1


def test_concurrent_same_phase_checkpoint_is_serialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = ACCEPTANCE.start_session("a" * 40, "b" * 40, execute=True)
    phase_payload = {
        "phase": "preflight",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")

    def checkpoint() -> dict[str, Any]:
        return dict(
            ACCEPTANCE.checkpoint_session(
                state["session_id"],
                "preflight",
                phase_path,
                execute=True,
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: checkpoint(), range(2)))

    assert [result["next_phase_index"] for result in results] == [1, 1]
    assert ACCEPTANCE._session_state(state["session_id"])["completed_phases"] == ["preflight"]


def test_finalize_is_crash_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence_payload, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    original_atomic = ACCEPTANCE._atomic_write
    failed = False

    def fail_complete_once(path: Path, payload: dict[str, Any]) -> None:
        nonlocal failed
        if path.name == "state.json" and payload["status"] == "complete" and not failed:
            failed = True
            raise ACCEPTANCE.AcceptanceError("simulated finalize crash")
        original_atomic(path, payload)

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", fail_complete_once)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="simulated finalize crash"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )
    session_dir = tmp_path / "state/sessions" / session_id
    assert (session_dir / "evidence.json").is_file()
    assert ACCEPTANCE._session_state(session_id)["status"] == "running"

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", original_atomic)
    recovered = ACCEPTANCE.finalize_session(
        session_id,
        evidence_path,
        schema,
        execute=True,
    )
    assert recovered["status"] == "complete"


def test_finalize_recomputes_phase_digest_and_rejects_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    evidence["state_machine"][0]["deadline_seconds"] = 601
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="checkpoint journal"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_checkpoint_metadata_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence_payload, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    checkpoint_path = tmp_path / "state/sessions" / session_id / "checkpoints/00-preflight.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["candidate_sha"] = "e" * 40
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="checkpoint journal"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_cross_sandbox_negative_matrix_is_exact_and_candidate_bound() -> None:
    evidence = _evidence()
    evidence["cross_sandbox_negative"].pop()
    assert any("schema violation" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["cross_sandbox_negative"][-1] = copy.deepcopy(
        evidence["cross_sandbox_negative"][0],
    )
    assert any("negative matrix is incomplete" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["cross_sandbox_negative"][0]["candidate_sha"] = "e" * 40
    assert any(
        "negative probe candidate does not match" in failure for failure in _failures(evidence)
    )


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["capacity_samples"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "capacity sample candidate does not match",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "candidate_sha",
                "e" * 40,
            ),
            "large-batch burst candidate does not match",
        ),
        (
            lambda item: item["fairness"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "fairness candidate does not match",
        ),
        (
            lambda item: item["runtime_envelopes"][0].__setitem__(
                "candidate_sha",
                "e" * 40,
            ),
            "runtime envelope candidate does not match",
        ),
        (
            lambda item: item["peer_workloads"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "peer candidate does not match",
        ),
        (
            lambda item: item["storage_io"][0].__setitem__(
                "candidate_sha",
                "e" * 40,
            ),
            "storage candidate does not match",
        ),
        (
            lambda item: item["fault_recovery"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "is not candidate-bound",
        ),
    ],
)
def test_all_evidence_domains_reject_an_old_candidate(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["cross_sandbox_negative"][0].__setitem__(
                "observed_at",
                _phase_observed_at("mixed_non_loom"),
            ),
            "negative probe is outside",
        ),
        (
            lambda item: item["capacity_samples"][0].__setitem__(
                "observed_at",
                _phase_observed_at("baseline"),
            ),
            "capacity sample is outside",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "started_at",
                _phase_observed_at("baseline"),
            ),
            "large batch is outside",
        ),
        (
            lambda item: item["fairness"][0].__setitem__(
                "started_at",
                _phase_observed_at("baseline"),
            ),
            "fairness is outside",
        ),
        (
            lambda item: item["runtime_envelopes"][0].__setitem__(
                "observed_at",
                _phase_observed_at("baseline"),
            ),
            "runtime envelope is outside",
        ),
        (
            lambda item: item["peer_workloads"][0]["baseline"].__setitem__(
                "observed_at",
                _phase_observed_at("preflight"),
            ),
            "peer checkpoints are outside",
        ),
        (
            lambda item: item["storage_io"][0].__setitem__(
                "baseline_observed_at",
                _phase_observed_at("preflight"),
            ),
            "storage observations are outside",
        ),
        (
            lambda item: item["fault_recovery"][0].__setitem__(
                "phase",
                "ttl_cleanup",
            ),
            "bound to the wrong phase",
        ),
    ],
)
def test_evidence_timestamps_must_land_in_the_exact_phase(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["session"].__setitem__("collected_at", _iso(50)),
            "freshness",
        ),
        (
            lambda item: item["capacity_samples"][0].__setitem__("active_slots", 30),
            "committed bounds",
        ),
        (
            lambda item: item["capacity_samples"].__setitem__(
                1,
                copy.deepcopy(item["capacity_samples"][0]),
            ),
            "capacity samples",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "nodes",
                ["trt-eai-oldlab-1"],
            ),
            "schema violation",
        ),
        (
            lambda item: item["runtime_envelopes"][0]["allocation"].__setitem__(
                "exclusive",
                True,
            ),
            "schema violation",
        ),
        (
            lambda item: item["runtime_envelopes"][0]["containers"][0].__setitem__(
                "observed_cgroup_path",
                "/system.slice/slurmstepd.scope/job_999/docker/escape",
            ),
            "escaped",
        ),
        (
            lambda item: item["peer_workloads"][0]["during"].__setitem__(
                "throughput_per_second",
                1,
            ),
            "throughput regression",
        ),
        (
            lambda item: item["storage_io"][0].__setitem__("io_errors", 1),
            "schema violation",
        ),
        (
            lambda item: item["fault_recovery"][0]["retry_attribution"].__setitem__(
                "retryable_trials",
                1,
            ),
            "fully retryable",
        ),
    ],
)
def test_acceptance_failures_are_closed(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


def test_capacity_overshoot_and_duplicate_observation_fail() -> None:
    evidence = _evidence()
    for sample in evidence["capacity_samples"]:
        if sample["pool"] == "oldlab":
            sample["requested_slots"] = 20
            sample["granted_slots"] = 20
            sample["pending_slots"] = 0
            sample["active_slots"] = 20
    failures = _failures(evidence)
    assert any("overshoots the slot budget" in failure for failure in failures)

    evidence = _evidence()
    evidence["capacity_samples"][1]["observation_sequence"] = evidence["capacity_samples"][0][
        "observation_sequence"
    ]
    failures = _failures(evidence)
    assert any("observation identity is duplicated" in failure for failure in failures)


def test_capacity_requires_every_phase_pair_and_zero_final_drain() -> None:
    evidence = _evidence()
    evidence["capacity_samples"].pop()
    assert any("schema violation" in failure for failure in _failures(evidence))

    evidence = _evidence()
    final = next(
        sample for sample in evidence["capacity_samples"] if sample["phase"] == "final_drain"
    )
    final["requested_slots"] = 1
    final["granted_slots"] = 1
    final["active_slots"] = 1
    assert any("final drain retains" in failure for failure in _failures(evidence))


def test_runtime_limits_and_candidate_binding_fail() -> None:
    evidence = _evidence()
    envelope = evidence["runtime_envelopes"][0]
    envelope["containers"][0]["observed_limits"]["pids"] = 999
    envelope["candidate_sha"] = "e" * 40

    failures = _failures(evidence)

    assert any("configured/observed limits differ" in failure for failure in failures)
    assert any("runtime envelope candidate does not match" in failure for failure in failures)


def test_runtime_account_node_and_gpu_envelopes_are_bound() -> None:
    evidence = _evidence()
    envelope = evidence["runtime_envelopes"][0]
    envelope["account"] = "loom-dev-devansh"
    envelope["node"] = "trt-gb10-1"
    envelope["containers"][0]["gpu_ids"] = ["GPU-0"]

    failures = _failures(evidence)

    assert any("account does not match" in failure for failure in failures)
    assert any("node does not match" in failure for failure in failures)
    assert any("OLDLAB runtime envelope" in failure for failure in failures)


def test_faults_require_exact_set_zero_orphans_and_bounded_recovery() -> None:
    evidence = _evidence()
    evidence["fault_recovery"][-1]["event"] = "cancel"

    assert any("fault recovery evidence" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["fault_recovery"][0]["recovered_at"] = _iso(50)
    assert any("recovery exceeded" in failure for failure in _failures(evidence))


def test_secret_like_input_is_rejected_without_echoing_value(tmp_path: Path) -> None:
    evidence = _evidence()
    evidence["api_token"] = "loom_api_DO_NOT_ECHO_123456"
    source = tmp_path / "unsafe.json"
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("verify", "--evidence", source)

    assert completed.returncode == 1
    assert "secret-like field" in completed.stdout
    assert "DO_NOT_ECHO" not in completed.stdout


def test_collect_canonicalizes_valid_evidence_without_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(_evidence()), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 0, completed.stdout
    assert json.loads(output.read_text(encoding="utf-8")) == _evidence()

    completed = _run("collect", "--input", source, "--output", output)
    assert completed.returncode == 1
    assert "cannot create acceptance artifact" in completed.stdout


def test_incomplete_evidence_is_not_collected(tmp_path: Path) -> None:
    evidence = _evidence()
    del evidence["fault_recovery"]
    source = tmp_path / "input.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 1
    assert not output.exists()
