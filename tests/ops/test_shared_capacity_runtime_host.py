from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import shared_capacity_runtime_host as host
from scripts.ops.developer_sandbox_capacity_contract import load_platform_health_contract

SHA = "a" * 40
TREE = "b" * 40
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
ADAPTER_CANDIDATES = {
    "qianyi": {"sha": SHA, "tree": TREE},
    "hongjian": {"sha": "c" * 40, "tree": "d" * 40},
    "devansh": {"sha": "e" * 40, "tree": "f" * 40},
}
CANDIDATE_SHAS = {
    sandbox: binding["sha"] for sandbox, binding in ADAPTER_CANDIDATES.items()
}


def _gate6_observations() -> dict[str, object]:
    started = NOW - timedelta(seconds=14_400)
    pairs = [(sandbox, pool) for sandbox in host.SANDBOXES for pool in host.POOLS]
    return {
        "soak": {
            "started_at": started.isoformat().replace("+00:00", "Z"),
            "completed_at": NOW.isoformat().replace("+00:00", "Z"),
            "duration_seconds": 14_400,
            "sample_count": 120,
            "required_duration_seconds": 14_400,
            "required_sample_count": 120,
            "workloads": [
                "loom",
                "non_loom_slurm",
                "kubernetes",
                "minio",
                "longhorn",
            ],
            "trial_success_numerator": 6,
            "trial_success_denominator": 6,
            "trial_success_ratio": 1.0,
            "minimum_trial_success_ratio": 0.95,
            "trial_outcomes": [
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "candidate_sha": ADAPTER_CANDIDATES[sandbox]["sha"],
                    "candidate_tree": ADAPTER_CANDIDATES[sandbox]["tree"],
                    "terminal_trial_count": 1,
                    "succeeded_trial_count": 1,
                    "failed_trial_count": 0,
                    "cancelled_trial_count": 0,
                    "retried_trial_count": 0,
                    "retry_attempt_count": 0,
                    "success_ratio": 1.0,
                }
                for sandbox, pool in pairs
            ],
            "resource_envelope_breaches": 0,
            "kube_api_healthy": True,
            "minio_quorum_healthy": True,
            "longhorn_healthy": True,
            "non_loom_slurm_healthy": True,
            "pair_headroom": [
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "min_free_cpu_cores": 4.0,
                    "min_free_memory_bytes": 16 * 1024**3,
                    "max_pid_usage_ratio": 0.1,
                    "observed_peak_concurrency": 1,
                    "within_reviewed_envelope": True,
                }
                for sandbox, pool in pairs
            ],
        },
        "device_isolation": [
            {
                "sandbox": sandbox,
                "pool": pool,
                "job_id": f"{sandbox}-{pool}-job",
                "node": f"{pool}-node",
                "host": f"{pool}-host",
                "allocated_ids": ["gpu0"] if pool == "gb10" else [],
                "all_allocated_usable": True,
                "unallocated_denied": True,
                "proof": {
                    "method": "native-cgroup-device-probe",
                    "allocated_probe_container_ids": [f"{sandbox}-{pool}-allocated"],
                    "denial_probe_container_ids": [f"{sandbox}-{pool}-denied"],
                    "observed_at": NOW.isoformat().replace("+00:00", "Z"),
                },
            }
            for sandbox, pool in pairs
        ],
        "cleanup": [
            {
                "event": event,
                "checkpoint": checkpoint,
                "job_ids": [f"{event}-job"],
                "terminal_states": ["CANCELLED"],
                "observed_within_seconds": 10,
                "maximum_cleanup_seconds": 300,
                "live_jobs": 0,
                "live_containers": 0,
                "durable_trial_state": True,
                "retryable_interrupted_trials": True,
                "observed_at": NOW.isoformat().replace("+00:00", "Z"),
            }
            for event, checkpoint in (
                ("cancellation", "cancel_cleanup"),
                ("ttl_expiry", "ttl_cleanup"),
                ("worker_crash", "worker_crash"),
                ("submit_host_restart", "submit_host_restart"),
            )
        ],
    }


def _platform_capacity() -> tuple[
    dict[str, dict[str, object]],
    dict[str, tuple[dict[str, object], str]],
]:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    contracts = {pool: host._candidate_platform_policy(candidate, pool) for pool in host.POOLS}
    capacity = {
        "oldlab": {
            **contracts["oldlab"][0],
            "minimum_node_cpu_cores": 24,
            "minimum_node_memory_bytes": 120 * 1024**3,
            "reserved_cpu_cores_per_node": 4,
            "reserved_memory_mib_per_node": 16384,
        },
        "gb10": {
            **contracts["gb10"][0],
            "minimum_node_cpu_cores": 20,
            "minimum_node_memory_bytes": 115000 * 1024**2,
            "reserved_cpu_cores_per_node": 4,
            "reserved_memory_mib_per_node": 23000,
        },
    }
    return capacity, contracts


def _platform_evidence(
    *,
    candidates: dict[str, dict[str, str]] | None = None,
    completed_at: datetime = NOW,
) -> tuple[dict[str, object], dict[str, object]]:
    capacity, contracts = _platform_capacity()
    evidence: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": "1" * 32,
        "candidates": candidates if candidates is not None else ADAPTER_CANDIDATES,
        "collector_host": "trt-eai-oldlab-2",
        "checkpoints": [],
        "mixed_jobs": [],
        "cancelled_jobs": [],
        "crashed_jobs": [],
        "node_intervals": {},
        "policy_capacity": capacity,
        "oldlab_capacity_recommendation": {
            "schema_version": 1,
            "pool": "oldlab",
            "source": host.PLATFORM_POLICY_SOURCES["oldlab"],
            "source_sha256": contracts["oldlab"][1],
            "values": capacity["oldlab"],
            "derivation": {
                "method": "installed-shared-capacity-policy-v1",
                "measured_node_count": 5,
                "minimum_observed_node_cpu_cores": 24,
                "minimum_observed_node_memory_bytes": 120 * 1024**3,
                "minimum_observed_free_cpu_cores": 8.0,
                "minimum_observed_free_memory_bytes": 32 * 1024**3,
                "minimum_required_free_cpu_cores": 4,
                "minimum_required_free_memory_bytes": 16 * 1024**3,
                "maximum_allowed_cpu_busy_ratio": 0.85,
                "all_nodes_passed": True,
            },
        },
        "gate6_observations": _gate6_observations(),
        "zero_orphans": True,
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
        "expires_at": (completed_at + host.PLATFORM_HEALTH_EVIDENCE_TTL)
        .isoformat()
        .replace("+00:00", "Z"),
    }
    evidence["payload_sha256"] = host._sha256(host._canonical_json(evidence))
    current: dict[str, object] = {
        "schema_version": 1,
        "session_id": "1" * 32,
        "evidence_path": (
            str(host.PLATFORM_HEALTH_ROOT / "sessions" / ("1" * 32) / "evidence.json")
        ),
        "payload_sha256": evidence["payload_sha256"],
    }
    return evidence, current


def _binding_readback(instance: str) -> subprocess.CompletedProcess[str]:
    sandbox, pool = instance.rsplit("-", 1)
    payload = {
        "pool": pool,
        "sandbox": sandbox,
        **ADAPTER_CANDIDATES[sandbox],
    }
    return subprocess.CompletedProcess(
        ("candidate-python",),
        0,
        stdout=host._canonical_json(payload).decode(),
        stderr="",
    )


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *args),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _source_repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "source"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "capacity-test@example.invalid")
    _git(repo, "config", "user.name", "Capacity Test")
    profile = repo / host.SOURCE_PROFILE.relative_to(host.REPO_ROOT)
    profile.parent.mkdir(parents=True)
    profile.write_bytes(host.SOURCE_PROFILE.read_bytes())
    (repo / "uv.lock").write_text("version = 1\nrevision = 1\n", encoding="utf-8")
    (repo / "payload").write_text("candidate\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "candidate")
    sha = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    return repo, sha, tree


def test_closed_profile_covers_exact_six_instances() -> None:
    profile = host._load_profile()

    assert profile["expected_hostname"] == "trt-eai-oldlab-2"
    assert profile["bootstrap_requires_zero_capacity"] is True
    assert profile["positive_admission_requires_platform_health"] is True
    assert profile["acceptance_contract_ttl_seconds"] == 86400
    assert profile["acceptance_default_phase_ttl_seconds"] == 7200
    assert profile["acceptance_mixed_non_loom_ttl_seconds"] == 21600
    assert profile["acceptance_ttl_cleanup_seconds"] == 120
    assert profile["acceptance_phases"] == list(host.ACCEPTANCE_PHASES)
    assert profile["adapter_instances"] == list(host.INSTANCES)
    assert len(profile["adapter_instances"]) == 6
    assert profile["slurm_domains"] == {
        "oldlab": {
            "submit_host": "trt-EAI-OLDLAB-2",
            "controller": "TRT-EAI-OLDLAB-1",
        },
        "gb10": {
            "submit_host": "trt-gb10-1",
            "controller": "trt-gb10-1",
        },
    }


def test_candidate_identity_requires_exact_clean_locked_source(tmp_path: Path) -> None:
    repo, sha, tree = _source_repo(tmp_path)

    assert host._candidate_identity(repo, sha) == host.Candidate(
        sha=sha,
        tree=tree,
        source=repo,
    )

    (repo / "untracked").write_text("drift\n", encoding="utf-8")
    with pytest.raises(host.RuntimeHostError, match="not clean"):
        host._candidate_identity(repo, sha)


def test_candidate_identity_rejects_unsafe_index_flags(tmp_path: Path) -> None:
    repo, sha, _tree = _source_repo(tmp_path)
    _git(repo, "update-index", "--skip-worktree", "payload")

    with pytest.raises(host.RuntimeHostError, match="flags"):
        host._candidate_identity(repo, sha)


def test_exact_candidate_blob_survives_source_after_verify_tamper(
    tmp_path: Path,
) -> None:
    repo, sha, _tree = _source_repo(tmp_path)
    candidate = host._candidate_identity(repo, sha)
    (repo / "payload").write_text("tampered after verification\n", encoding="utf-8")

    with pytest.raises(host.RuntimeHostError):
        host._validate_repository(repo, sha)
    assert host._read_candidate_file(candidate, Path("payload")) == b"candidate\n"


def test_git_verification_environment_ignores_host_configuration() -> None:
    environment = host._git_environment()

    assert environment["GIT_CONFIG_GLOBAL"] == "/dev/null"
    assert environment["GIT_CONFIG_SYSTEM"] == "/dev/null"
    assert environment["GIT_ATTR_NOSYSTEM"] == "1"
    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert environment["HOME"] == "/nonexistent"


def test_service_render_is_exact_sha_bound_and_never_uses_current() -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    rendered = host._render_service(
        host.ADAPTER_SERVICE_SOURCE.read_bytes(),
        candidate,
    ).decode()

    assert f"/opt/loom-shared-capacity/candidates/{SHA}/repo" in rendered
    assert f"/opt/loom-shared-capacity/candidates/{SHA}/venv" in rendered
    assert "${GIT_SHA}" not in rendered
    assert "/opt/loom-shared-capacity/current" not in rendered


def test_plan_is_secret_free_and_covers_exact_configs_units_and_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    desired = {
        Path(f"/etc/loom/file-{index}"): (f"value-{index}".encode(), 0o600) for index in range(13)
    }
    monkeypatch.setattr(host, "_desired_files", lambda value: desired)
    monkeypatch.setattr(
        host,
        "_load_candidate_profile",
        lambda value: host._load_profile(),
    )
    monkeypatch.setattr(
        host,
        "_read_candidate_file",
        lambda value, relative: b"locked",
    )

    result = host.plan(candidate, "install")
    encoded = json.dumps(result, sort_keys=True)

    assert result["mutation_authorized"] is False
    assert result["capacity_enabled_by_installer"] is False
    assert result["instances"] == list(host.INSTANCES)
    assert result["slurm_domains"]["oldlab"] != result["slurm_domains"]["gb10"]
    assert len(result["files"]) == 13
    assert all(row["sha256"] for row in result["files"])
    assert "admin.token" not in encoded
    assert "Bearer " not in encoded


def test_closed_world_unit_readback_rejects_duplicates_and_orphans() -> None:
    valid = [
        "loom-shared-capacity-supervisor.service loaded active exited",
        "loom-shared-capacity-supervisor.timer loaded active waiting",
        "loom-shared-capacity-adapter@qianyi-gb10.timer loaded active waiting",
    ]
    assert "loom-shared-capacity-adapter@qianyi-gb10.timer" in host._validate_managed_unit_rows(
        valid
    )

    with pytest.raises(host.RuntimeHostError, match="duplicate"):
        host._validate_managed_unit_rows([valid[0], valid[0]])
    with pytest.raises(host.RuntimeHostError, match="orphan"):
        host._validate_managed_unit_rows(
            ["loom-shared-capacity-adapter@unknown-gb10.timer loaded active waiting"],
        )
    with pytest.raises(host.RuntimeHostError, match="orphan"):
        host._validate_managed_unit_rows(
            ["loom-shared-capacity-unknown.service enabled"],
        )


def test_list_unit_files_rejects_orphan_installed_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        assert argv[1] == "list-unit-files"
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=(
                "loom-shared-capacity-adapter@.service static\n"
                "loom-shared-capacity-adapter@orphan-gb10.timer enabled\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(host, "_run", fake_run)

    with pytest.raises(host.RuntimeHostError, match="orphan"):
        host._installed_managed_unit_files()


def test_fragment_readback_requires_exact_installed_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        host,
        "_unit_fragment",
        lambda unit: ("loaded", "/tmp/untrusted.service"),
    )

    with pytest.raises(host.RuntimeHostError, match="fragment"):
        host._validate_unit_fragment(
            host.SUPERVISOR_SERVICE,
            host.SUPERVISOR_SERVICE_PATH,
        )


def test_closed_world_config_readback_rejects_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_root = tmp_path / "configs"
    config_root.mkdir()
    for instance in host.INSTANCES:
        (config_root / f"{instance}.toml").write_text("", encoding="utf-8")
    monkeypatch.setattr(host, "ADAPTER_CONFIG_ROOT", config_root)

    host._reject_orphan_configs()
    (config_root / "orphan-gb10.toml").write_text("", encoding="utf-8")
    with pytest.raises(host.RuntimeHostError, match="orphan"):
        host._reject_orphan_configs()


def test_install_failure_restores_snapshot_and_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    restored: list[tuple[Path, bool]] = []
    phases: list[str] = []
    events: list[str] = []
    journal = {
        "transaction_id": "1" * 32,
        "operation": "install",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": False,
        "staging_path": (f"/opt/loom-shared-capacity/candidates/.install-{SHA}-{'1' * 32}"),
        "files": {},
        "units": {},
    }
    journal_path = Path("/var/lib/loom-shared-capacity/test-journal.json")

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_load_candidate_profile", lambda value: {})
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_configs", lambda: None)
    monkeypatch.setattr(host, "_loaded_managed_units", lambda: set())
    monkeypatch.setattr(host, "_installed_managed_unit_files", lambda: set())
    monkeypatch.setattr(
        host,
        "_materialize_candidate",
        lambda value, staging: events.append("materialize") or True,
    )
    monkeypatch.setattr(
        host,
        "_write_journal",
        lambda value, **kwargs: events.append("journal") or (journal_path, journal),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda path, payload, phase: phases.append(phase),
    )
    monkeypatch.setattr(host, "_stop_units", lambda: None)
    monkeypatch.setattr(host, "_publish_files", lambda value: None)
    monkeypatch.setattr(host, "_atomic_write", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        host,
        "_publish_unit_state",
        lambda: (_ for _ in ()).throw(host.RuntimeHostError("activation failed")),
    )
    monkeypatch.setattr(
        host,
        "_restore_transaction",
        lambda path, payload: restored.append((path, True)),
    )

    with pytest.raises(host.RuntimeHostError, match="activation failed"):
        host.install(candidate)

    assert phases == ["stopped", "materialized", "published"]
    assert events == ["journal", "materialize"]
    assert restored == [(journal_path, True)]


def test_orphan_recovery_rolls_back_noncommitted_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = Path("/var/lib/loom-shared-capacity/orphan.json")
    payload = {"phase": "published"}
    restored: list[tuple[Path, bool]] = []
    monkeypatch.setattr(host, "_active_journal", lambda: (path, payload))
    monkeypatch.setattr(
        host,
        "_restore_transaction",
        lambda item, data: restored.append((item, True)),
    )

    host._recover_orphan()

    assert restored == [(path, True)]


def test_initial_activation_journal_persists_token_before_active_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_parent = tmp_path / "candidates"
    journal_root = tmp_path / "transactions"
    active_path = tmp_path / "active.json"
    admission_token = "1" * 32
    writes: list[tuple[Path, dict[str, object]]] = []
    monkeypatch.setattr(host, "CANDIDATE_PARENT", candidate_parent)
    monkeypatch.setattr(host, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "JOURNAL_ROOT", journal_root)
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "_ensure_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "_capture_files", lambda _paths: {})
    monkeypatch.setattr(
        host,
        "_systemctl_state",
        lambda _unit: {"enabled": False, "active": False},
    )
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: writes.append((path, json.loads(content))),
    )
    candidate = host.Candidate(SHA, TREE, tmp_path / "source")

    journal_path, payload = host._write_journal(
        candidate,
        operation="activate",
        admission_token=admission_token,
    )

    assert writes[0][0] == journal_path
    assert writes[0][1]["phase"] == "prepared"
    assert writes[0][1]["admission_token"] == admission_token
    assert writes[1][0] == active_path
    assert payload["admission_token"] == admission_token
    with pytest.raises(host.RuntimeHostError, match="admission binding"):
        host._write_journal(candidate, operation="admit")
    with pytest.raises(host.RuntimeHostError, match="cannot own"):
        host._write_journal(
            candidate,
            operation="install",
            admission_token=admission_token,
        )


def test_prepared_activation_orphan_restores_installed_and_reopens_exact_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "activate.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    journal_token = "2" * 32
    admission_token = "1" * 32
    payload = {
        "phase": "prepared",
        "operation": "activate",
        "transaction_id": journal_token,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": admission_token,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_active_journal", lambda: (path, payload))
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda _path, _payload, **_kwargs: (
            events.append("installed-restored")
            or (journal_token, "activate", SHA, TREE)
        ),
    )
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda _candidate, token: events.append(f"open:{token}"),
    )
    monkeypatch.setattr(host, "_update_journal", lambda *_args: None)
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)

    host._recover_orphan()

    assert events == ["installed-restored", f"open:{admission_token}"]
    assert not active_path.exists()


def test_prepared_admit_orphan_restores_bootstrap_and_recloses_exact_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admit.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    journal_token = "2" * 32
    admission_token = "1" * 32
    payload = {
        "phase": "prepared",
        "operation": "admit",
        "transaction_id": journal_token,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": admission_token,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_active_journal", lambda: (path, payload))
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda _candidate, token: events.append(f"close:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda _candidate, token: events.append(f"drain:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_verify_activated_capacity_drained",
        lambda _candidate, token: events.append(f"zero:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda _path, _payload, **_kwargs: (
            events.append("bootstrap-restored")
            or (journal_token, "admit", SHA, TREE)
        ),
    )
    monkeypatch.setattr(host, "_update_journal", lambda *_args: None)
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)

    host._recover_orphan()

    assert events == [
        f"close:{admission_token}",
        f"drain:{admission_token}",
        f"zero:{admission_token}",
        "bootstrap-restored",
    ]
    assert not active_path.exists()


def test_orphan_recovery_resumes_persisted_activated_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = Path("/var/lib/loom-shared-capacity/rollback.json")
    payload = {
        "operation": "install",
        "phase": "rollback-restoring",
    }
    resumed: list[tuple[Path, dict[str, object]]] = []
    monkeypatch.setattr(host, "_active_journal", lambda: (path, payload))
    monkeypatch.setattr(
        host,
        "_resume_activated_rollback",
        lambda item, data: resumed.append((item, data)),
    )

    host._recover_orphan()

    assert resumed == [(path, payload)]


def test_committed_bootstrap_recovery_preserves_closed_admission_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = Path("/var/lib/loom-shared-capacity/committed.json")
    transaction_id = "1" * 32
    payload = {
        "phase": "committed",
        "operation": "activate",
        "transaction_id": transaction_id,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
    }
    events: list[tuple[str, object]] = []
    active_journal = tmp_path / "active.json"
    active_journal.write_text("{}")
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_journal)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_active_journal", lambda: (path, payload))
    monkeypatch.setattr(host, "_fsync_directory", lambda value: events.append(("fsync", value)))

    host._recover_orphan()

    assert events == [("fsync", tmp_path)]
    assert not active_journal.exists()


def test_unjournaled_staging_path_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_parent = tmp_path / "candidates"
    candidate_parent.mkdir()
    (candidate_parent / f".install-{SHA}-orphan").mkdir()
    monkeypatch.setattr(host, "CANDIDATE_PARENT", candidate_parent)

    with pytest.raises(host.RuntimeHostError, match="unjournaled"):
        host._reject_orphan_stages()


def test_crash_restore_removes_journal_owned_stage_and_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_parent = tmp_path / "candidates"
    installer_root = tmp_path / "installer"
    journal_root = installer_root / "transactions"
    candidate_parent.mkdir()
    installer_root.mkdir()
    journal_root.mkdir()
    active_path = installer_root / "active.json"
    removed: list[Path] = []
    transaction_id = "1" * 32
    stage = candidate_parent / f".install-{SHA}-{transaction_id}"
    payload = {
        "transaction_id": transaction_id,
        "operation": "install",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": False,
        "staging_path": str(stage),
        "files": {str(path): {"present": False} for path in host._snapshot_paths()},
        "units": {unit: {"enabled": False, "active": False} for unit in host.ALL_UNITS},
    }
    monkeypatch.setattr(host, "CANDIDATE_PARENT", candidate_parent)
    monkeypatch.setattr(host, "INSTALLER_ROOT", installer_root)
    monkeypatch.setattr(host, "JOURNAL_ROOT", journal_root)
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "_stop_units", lambda: None)
    monkeypatch.setattr(host, "_restore_files", lambda snapshot: None)
    monkeypatch.setattr(host, "_restore_units", lambda states: None)
    monkeypatch.setattr(host, "_update_journal", lambda *args: None)
    monkeypatch.setattr(host, "_remove_path", removed.append)

    host._restore_transaction(journal_root / f"{transaction_id}.json", payload)

    assert removed == [stage, candidate_parent / SHA]


def test_active_handoff_is_not_consumed_during_install_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(host, "_run", fake_run)

    host._publish_unit_state()

    assert not any(command[1] in {"start", "enable"} for command in commands)
    assert any(command[1:3] == ("disable", "--now") for command in commands)


def test_bootstrap_preflight_checks_broker_and_all_six_receipts_without_health_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_candidate_python(
        _candidate: host.Candidate,
        code: str,
        *args: str,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((code, args))
        if code == host._ADAPTER_BINDING_READBACK:
            return _binding_readback(Path(args[0]).stem)
        return subprocess.CompletedProcess(("candidate-python",), 0, stdout="", stderr="")

    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        fake_candidate_python,
    )
    host._activation_preflight(candidate, transaction_id="1" * 32)

    assert len(calls) == 13
    assert all(code == host._ADAPTER_BINDING_READBACK for code, _args in calls[:6])
    assert calls[6][0] == host._BROKER_PREFLIGHT
    assert calls[6][1] == (
        str(host.SUPERVISOR_CONFIG_PATH),
        json.dumps(CANDIDATE_SHAS, sort_keys=True, separators=(",", ":")),
        "1" * 32,
    )
    adapter_calls = calls[7:]
    assert all(code == host._ADAPTER_PREFLIGHT for code, _args in adapter_calls)
    assert {Path(args[0]).stem for _code, args in adapter_calls} == set(host.INSTANCES)
    for _code, args in adapter_calls:
        sandbox, _pool = Path(args[0]).stem.rsplit("-", 1)
        assert args[1:] == (
            ADAPTER_CANDIDATES[sandbox]["sha"],
            ADAPTER_CANDIDATES[sandbox]["tree"],
        )
    assert "_load_admin_token" in host._ADAPTER_PREFLIGHT
    assert "_validate_runtime_attestation" in host._ADAPTER_PREFLIGHT


def test_platform_health_gate_binds_candidate_and_reviewed_positive_headroom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: (str(evidence["payload_sha256"]), "6" * 64),
    )

    host._validate_platform_health_activation_gate(
        candidate,
        ADAPTER_CANDIDATES,
        now=NOW,
    )

    capacity = evidence["policy_capacity"]
    assert isinstance(capacity, dict)
    gb10 = capacity["gb10"]
    assert isinstance(gb10, dict)
    gb10["requested_cpus"] = 20
    unsigned = {key: value for key, value in evidence.items() if key != "payload_sha256"}
    evidence["payload_sha256"] = host._sha256(host._canonical_json(unsigned))
    current["payload_sha256"] = evidence["payload_sha256"]
    with pytest.raises(host.RuntimeHostError, match="activation gate"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


def test_platform_health_gate_rejects_partial_evidence_that_cannot_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    assert evidence["checkpoints"] == []
    assert evidence["mixed_jobs"] == []
    assert evidence["node_intervals"] == {}
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: (_ for _ in ()).throw(
            host.RuntimeHostError("platform-health checkpoint sequence is incomplete"),
        ),
    )

    with pytest.raises(host.RuntimeHostError, match="incomplete"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


def test_platform_health_gate_rejects_partial_33_checkpoint_live_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: (_ for _ in ()).throw(
            host.RuntimeHostError("live acceptance checkpoint journal is incomplete"),
        ),
    )

    with pytest.raises(host.RuntimeHostError, match="checkpoint journal is incomplete"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


@pytest.mark.parametrize("attack", ("extra-soak-field", "missing-device-pair", "bad-cleanup"))
def test_platform_health_gate_rejects_gate6_shape_drift(
    attack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    gate6 = evidence["gate6_observations"]
    assert isinstance(gate6, dict)
    if attack == "extra-soak-field":
        soak = gate6["soak"]
        assert isinstance(soak, dict)
        soak["unexpected"] = True
    elif attack == "missing-device-pair":
        devices = gate6["device_isolation"]
        assert isinstance(devices, list)
        devices.pop()
    else:
        cleanup = gate6["cleanup"]
        assert isinstance(cleanup, list)
        row = cleanup[0]
        assert isinstance(row, dict)
        row["live_jobs"] = 1
    unsigned = {key: value for key, value in evidence.items() if key != "payload_sha256"}
    evidence["payload_sha256"] = host._sha256(host._canonical_json(unsigned))
    current["payload_sha256"] = evidence["payload_sha256"]
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: pytest.fail("shape drift must fail before rebuild"),
    )

    with pytest.raises(host.RuntimeHostError, match="activation gate"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


@pytest.mark.parametrize(
    "attack",
    (
        "numerator-mismatch",
        "zero-denominator",
        "duplicate-pair",
        "candidate-drift",
        "terminal-sum-mismatch",
        "retry-count-mismatch",
        "retry-attempt-mismatch",
    ),
)
def test_platform_health_gate_rejects_trial_outcome_accounting_drift(
    attack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    gate6 = evidence["gate6_observations"]
    assert isinstance(gate6, dict)
    soak = gate6["soak"]
    assert isinstance(soak, dict)
    outcomes = soak["trial_outcomes"]
    assert isinstance(outcomes, list)
    first = outcomes[0]
    assert isinstance(first, dict)
    if attack == "numerator-mismatch":
        soak["trial_success_numerator"] = 7
    elif attack == "zero-denominator":
        soak["trial_success_denominator"] = 0
    elif attack == "duplicate-pair":
        outcomes[-1] = dict(first)
    elif attack == "candidate-drift":
        first["candidate_sha"] = "9" * 40
    elif attack == "terminal-sum-mismatch":
        first["terminal_trial_count"] = 2
    elif attack == "retry-count-mismatch":
        first["retried_trial_count"] = 2
        first["retry_attempt_count"] = 2
    else:
        first["retried_trial_count"] = 1
        first["retry_attempt_count"] = 0
    unsigned = {key: value for key, value in evidence.items() if key != "payload_sha256"}
    evidence["payload_sha256"] = host._sha256(host._canonical_json(unsigned))
    current["payload_sha256"] = evidence["payload_sha256"]
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: pytest.fail(
            "trial outcome drift must fail before rebuild",
        ),
    )

    with pytest.raises(host.RuntimeHostError, match="activation gate"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


def test_platform_health_rebuild_returns_exact_platform_and_gate6_digests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    payload = {
        "gate6_sha256": "6" * 64,
        "platform_health_sha256": "7" * 64,
    }
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda *_args: subprocess.CompletedProcess(
            ("candidate-python",),
            0,
            stdout=host._canonical_json(payload).decode(),
            stderr="",
        ),
    )

    assert host._verify_platform_health_authority_evidence(
        candidate,
        Path("/var/lib/loom-platform/session/evidence.json"),
    ) == ("7" * 64, "6" * 64)


@pytest.mark.parametrize("attack", ["digest", "capacity-extra", "recommendation-extra"])
def test_platform_health_gate_rejects_policy_binding_drift(
    attack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    evidence, current = _platform_evidence()
    capacity = evidence["policy_capacity"]
    recommendation = evidence["oldlab_capacity_recommendation"]
    assert isinstance(capacity, dict)
    assert isinstance(recommendation, dict)
    if attack == "digest":
        recommendation["source_sha256"] = "f" * 64
    elif attack == "capacity-extra":
        oldlab = capacity["oldlab"]
        assert isinstance(oldlab, dict)
        oldlab["unexpected"] = 1
    else:
        recommendation["unexpected"] = True
    unsigned = {key: value for key, value in evidence.items() if key != "payload_sha256"}
    evidence["payload_sha256"] = host._sha256(host._canonical_json(unsigned))
    current["payload_sha256"] = evidence["payload_sha256"]
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_verify_platform_health_authority_evidence",
        lambda _candidate, _path: (str(evidence["payload_sha256"]), "6" * 64),
    )

    with pytest.raises(host.RuntimeHostError, match="activation gate"):
        host._validate_platform_health_activation_gate(
            candidate,
            ADAPTER_CANDIDATES,
            now=NOW,
        )


def test_candidate_platform_policy_rejects_extra_source_fields(tmp_path: Path) -> None:
    source = tmp_path / "candidate"
    for relative in (
        "deploy/developer-sandboxes/platform-health-authority.toml",
        *host.PLATFORM_POLICY_SOURCES.values(),
    ):
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(host.REPO_ROOT / relative, destination)
    oldlab = source / host.PLATFORM_POLICY_SOURCES["oldlab"]
    oldlab.write_text(
        oldlab.read_text(encoding="utf-8") + "\nunexpected_policy_drift = true\n",
        encoding="utf-8",
    )

    with pytest.raises(host.RuntimeHostError, match="policy"):
        host._candidate_platform_policy(host.Candidate(SHA, TREE, source), "oldlab")


def test_candidate_platform_policy_uses_full_gb10_capacity_inventory() -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    health = load_platform_health_contract(candidate.source)

    assert health.gb10_nodes == tuple(f"trt-gb10-{index}" for index in range(1, 16))
    assert health.host_aliases["trt-gb10-7"] == "gx10-0faf"
    assert len(health.capacity_gb10_nodes) == 15
    assert "trt-gb10-7" in health.capacity_gb10_nodes
    policy, _digest = host._candidate_platform_policy(candidate, "gb10")
    assert policy["max_jobs"] <= len(health.capacity_gb10_nodes)


@pytest.mark.parametrize("attack", ["expired", "future", "missing", "extra", "one-drift"])
def test_platform_health_gate_rejects_before_broker_mutation(
    attack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = json.loads(json.dumps(ADAPTER_CANDIDATES))
    adapters = json.loads(json.dumps(ADAPTER_CANDIDATES))
    completed_at = NOW
    if attack == "expired":
        completed_at = NOW - host.PLATFORM_HEALTH_EVIDENCE_TTL
    elif attack == "future":
        completed_at = NOW + timedelta(minutes=1)
    elif attack == "missing":
        del candidates["devansh"]
    elif attack == "extra":
        candidates["foreign"] = {"sha": "1" * 40, "tree": "2" * 40}
    else:
        adapters["devansh"] = {"sha": "1" * 40, "tree": "2" * 40}
    evidence, current = _platform_evidence(
        candidates=candidates,
        completed_at=completed_at,
    )
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    mutations: list[str] = []
    monkeypatch.setattr(host, "_adapter_candidate_bindings", lambda _value: adapters)
    monkeypatch.setattr(host, "_platform_health_now", lambda _value: NOW)
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
    )
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda *_args: mutations.append("candidate-program"),
    )

    with pytest.raises(host.RuntimeHostError):
        host._positive_capacity_admission_gate(candidate)

    assert mutations == []


def test_adapter_candidate_bindings_require_exact_two_pool_agreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda _candidate, _code, path: _binding_readback(Path(path).stem),
    )

    assert host._adapter_candidate_bindings(candidate) == ADAPTER_CANDIDATES

    def drifted_readback(
        _candidate: host.Candidate,
        _code: str,
        path: str,
    ) -> subprocess.CompletedProcess[str]:
        instance = Path(path).stem
        completed = _binding_readback(instance)
        if instance == "hongjian-oldlab":
            payload = json.loads(completed.stdout)
            payload["sha"] = "1" * 40
            return subprocess.CompletedProcess(
                completed.args,
                0,
                stdout=host._canonical_json(payload).decode(),
                stderr="",
            )
        return completed

    monkeypatch.setattr(host, "_run_candidate_python", drifted_readback)
    with pytest.raises(host.RuntimeHostError, match="drifted across pools"):
        host._adapter_candidate_bindings(candidate)


@pytest.mark.parametrize(
    ("name", "program", "argument_count"),
    (
        ("broker-preflight", host._BROKER_PREFLIGHT, 3),
        ("broker-open", host._BROKER_OPEN, 2),
        ("broker-retire", host._BROKER_RETIRE, 4),
        ("acceptance-retire", host._ACCEPTANCE_RETIRE, 5),
        ("platform-health-rebuild", host._PLATFORM_HEALTH_AUTHORITY_REBUILD, 1),
        ("acceptance-session-readback", host._ACCEPTANCE_SESSION_READBACK, 1),
        ("acceptance-open", host._ACCEPTANCE_OPEN, 3),
        ("acceptance-cohort", host._ACCEPTANCE_COHORT, 4),
        ("acceptance-cohort-status", host._ACCEPTANCE_COHORT_STATUS, 4),
        ("acceptance-cancel", host._ACCEPTANCE_CANCEL, 6),
        ("acceptance-close", host._ACCEPTANCE_CLOSE, 3),
        ("acceptance-contract-readback", host._ACCEPTANCE_CONTRACT_READBACK, 1),
        ("adapter-preflight", host._ADAPTER_PREFLIGHT, 3),
        ("adapter-binding-readback", host._ADAPTER_BINDING_READBACK, 1),
        ("generation-readback", host._GENERATION_READBACK, 1),
        ("activated-adapter-readback", host._ACTIVATED_ADAPTER_READBACK, 4),
    ),
)
def test_embedded_candidate_programs_compile_and_bind_exact_arguments(
    name: str,
    program: str,
    argument_count: int,
) -> None:
    compiled = compile(program, f"<{name}>", "exec")

    assert compiled.co_code
    assert host._EMBEDDED_PROGRAM_ARGUMENT_COUNTS[program] == argument_count


def test_embedded_candidate_program_rejects_argument_count_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    invoked = False

    def fake_run(*args: object, **kwargs: object) -> None:
        nonlocal invoked
        invoked = True

    monkeypatch.setattr(host, "_run", fake_run)

    with pytest.raises(host.RuntimeHostError, match="argument contract"):
        host._run_candidate_python(
            candidate,
            host._ACTIVATED_ADAPTER_READBACK,
            "config-only",
        )

    assert invoked is False


def test_embedded_candidate_program_rejects_syntax_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    broken = "from pathlib import Path\\n)\\n"
    monkeypatch.setitem(host._EMBEDDED_PROGRAM_ARGUMENT_COUNTS, broken, 0)

    with pytest.raises(host.RuntimeHostError, match="program is invalid"):
        host._run_candidate_python(candidate, broken)


def test_embedded_platform_rebuild_loads_soak_hash_chain_before_final_rebuild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.ops import developer_sandbox_platform_health_authority as authority

    class SamplesReachedError(RuntimeError):
        pass

    receipt = {
        "session_id": "2" * 32,
        "candidates": ADAPTER_CANDIDATES,
    }
    samples = [{"sequence": index} for index in range(1, 121)]
    evidence_path = tmp_path / "session" / "evidence.json"
    evidence_path.parent.mkdir()
    monkeypatch.setattr(authority, "load_config", lambda _path: object())
    monkeypatch.setattr(authority, "_load_receipts", lambda _root: [receipt])
    monkeypatch.setattr(
        authority,
        "_load_samples",
        lambda _config, root, *, session_id, candidates: (
            samples
            if (
                root == evidence_path.parent
                and session_id == receipt["session_id"]
                and candidates == ADAPTER_CANDIDATES
            )
            else pytest.fail("embedded sample authority binding drifted")
        ),
    )

    def verify(
        _config: object,
        receipts: object,
        *,
        require_complete: bool,
        samples: object,
    ) -> None:
        assert receipts == [receipt]
        assert require_complete is True
        assert samples == [{"sequence": index} for index in range(1, 121)]
        raise SamplesReachedError

    monkeypatch.setattr(authority, "_verify_checkpoints", verify)
    monkeypatch.setattr(
        sys,
        "argv",
        ("embedded", str(host.REPO_ROOT), str(evidence_path)),
    )

    with pytest.raises(SamplesReachedError):
        exec(
            compile(
                host._PLATFORM_HEALTH_AUTHORITY_REBUILD,
                "<platform-health-rebuild>",
                "exec",
            ),
            {"__name__": "__main__"},
        )


def test_embedded_acceptance_retire_uses_token_session_api_not_generic_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from scripts.ops import shared_capacity_supervisor as supervisor

    from loom_control_plane.shared_capacity_broker import (
        BrokerError,
        SharedCapacityBroker,
    )

    state_db = (tmp_path / "broker.sqlite3").resolve()
    token = "1" * 32
    session_id = "2" * 32
    candidate_shas = CANDIDATE_SHAS
    broker = SharedCapacityBroker(state_db)
    broker.close_admission(token)
    contract = {
        "schema_version": 1,
        "admission_token": token,
        "session_id": session_id,
        "candidate_shas": candidate_shas,
        "phases": ["multi_candidate_overlap"],
        "target_slots": {"gb10": 2, "oldlab": 2},
        "ttl_seconds": {"multi_candidate_overlap": 7200},
        "pool_slot_budgets": {"gb10": 6, "oldlab": 6},
        "pool_pending_slot_budgets": {"gb10": 6, "oldlab": 6},
        "expires_at": (datetime.now(UTC) + timedelta(hours=1))
        .isoformat()
        .replace("+00:00", "Z"),
    }
    broker.open_acceptance_admission(token=token, contract=contract)
    cohort = broker.request_acceptance_cohort(
        token=token,
        session_id=session_id,
        phase="multi_candidate_overlap",
    )
    target_id = str(cohort[0]["request"]["id"])  # type: ignore[index]
    with pytest.raises(BrokerError, match="exact acceptance cancel"):
        broker.cancel(target_id)

    monkeypatch.setattr(
        supervisor,
        "load_config",
        lambda _path: SimpleNamespace(state_db=state_db),
    )
    monkeypatch.setattr(
        supervisor,
        "_validate_report_budgets",
        lambda _report, _config: None,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        (
            "embedded",
            str(host.REPO_ROOT),
            "/etc/loom/shared-capacity-supervisor.toml",
            json.dumps(candidate_shas, sort_keys=True, separators=(",", ":")),
            token,
            session_id,
            str(state_db),
        ),
    )

    exec(
        compile(host._ACCEPTANCE_RETIRE, "<acceptance-retire>", "exec"),
        {"__name__": "__main__"},
    )

    assert capsys.readouterr().out == "drained\n"
    assert all(
        row["request"]["state"] == "terminal"  # type: ignore[index]
        for row in broker.status()["requests"]
    )


def _zero_broker_preflight() -> tuple[dict[str, object], dict[str, object]]:
    selected: dict[str, object] = {}
    requests: list[dict[str, object]] = []
    for index, instance in enumerate(host.INSTANCES):
        request_id = f"request-{index}"
        sandbox, pool = instance.rsplit("-", 1)
        selected[instance] = {
            "request_id": request_id,
            "candidate_sha": CANDIDATE_SHAS[sandbox],
            "enabled": False,
            "min_slots": 0,
            "max_slots": 0,
        }
        requests.append(
            {
                "request": {
                    "id": request_id,
                    "sandbox": sandbox,
                    "pool": pool,
                    "candidate_sha": CANDIDATE_SHAS[sandbox],
                    "state": "terminal",
                },
                "lease": {
                    "pending_slots": 0,
                    "active_slots": 0,
                    "draining_slots": 0,
                },
            },
        )
    return (
        {
            "requests": requests,
            "aggregate": {
                "committed_slots": 0,
                "pending_slots": 0,
                "active_slots": 0,
                "draining_slots": 0,
            },
        },
        selected,
    )


def test_activation_rejects_nonzero_broker_handoff() -> None:
    report, selected = _zero_broker_preflight()
    handoff = selected[host.INSTANCES[0]]
    assert isinstance(handoff, dict)
    handoff.update({"enabled": True, "max_slots": 1})

    with pytest.raises(host.RuntimeHostError, match="zero-capacity"):
        host._validate_zero_broker_handoffs(report, selected, CANDIDATE_SHAS)


def test_activation_accepts_distinct_candidate_set_and_rejects_one_wrong_binding() -> None:
    report, selected = _zero_broker_preflight()

    host._validate_zero_broker_handoffs(report, selected, CANDIDATE_SHAS)
    wrong = selected["hongjian-gb10"]
    assert isinstance(wrong, dict)
    wrong["candidate_sha"] = SHA
    with pytest.raises(host.RuntimeHostError, match="zero-capacity"):
        host._validate_zero_broker_handoffs(report, selected, CANDIDATE_SHAS)


def _retirement_report(
    *,
    state: str,
    candidate_shas: dict[str, str] | None = None,
) -> dict[str, object]:
    exact_shas = CANDIDATE_SHAS if candidate_shas is None else candidate_shas
    requests: list[dict[str, object]] = []
    for index, instance in enumerate(host.INSTANCES):
        sandbox, pool = instance.rsplit("-", 1)
        live = state != "terminal"
        requests.append(
            {
                "request": {
                    "id": f"request-{index}",
                    "sandbox": sandbox,
                    "pool": pool,
                    "candidate_sha": exact_shas[sandbox],
                    "state": state,
                },
                "lease": {
                    "granted_slots": 1 if live else 0,
                    "pending_slots": 0,
                    "active_slots": 1 if live else 0,
                    "draining_slots": 0,
                    "committed_slots": 1 if live else 0,
                },
            },
        )
    total = len(host.INSTANCES) if state != "terminal" else 0
    return {
        "requests": requests,
        "aggregate": {
            "granted_slots": total,
            "pending_slots": 0,
            "active_slots": total,
            "draining_slots": 0,
            "committed_slots": total,
        },
    }


def test_retirement_targets_only_exact_candidate_and_ignores_foreign_terminal() -> None:
    report = _retirement_report(state="active")
    requests = report["requests"]
    assert isinstance(requests, list)
    requests.append(
        {
            "request": {
                "id": "foreign-terminal",
                "sandbox": "qianyi",
                "pool": "gb10",
                "candidate_sha": "c" * 40,
                "state": "terminal",
            },
            "lease": {
                "granted_slots": 0,
                "pending_slots": 0,
                "active_slots": 0,
                "draining_slots": 0,
                "committed_slots": 0,
            },
        },
    )

    assert host._retirement_request_ids(report, CANDIDATE_SHAS) == tuple(
        f"request-{index}" for index in range(len(host.INSTANCES))
    )
    assert "foreign-terminal" not in host._retirement_request_ids(
        report,
        CANDIDATE_SHAS,
    )


def test_retirement_rejects_foreign_nonterminal_lane_without_cancelling_it() -> None:
    report = _retirement_report(state="terminal")
    requests = report["requests"]
    assert isinstance(requests, list)
    requests.append(
        {
            "request": {
                "id": "foreign-active",
                "sandbox": "qianyi",
                "pool": "gb10",
                "candidate_sha": "c" * 40,
                "state": "active",
            },
            "lease": {
                "granted_slots": 1,
                "pending_slots": 0,
                "active_slots": 1,
                "draining_slots": 0,
                "committed_slots": 1,
            },
        },
    )

    with pytest.raises(host.RuntimeHostError, match="another candidate"):
        host._retirement_request_ids(report, CANDIDATE_SHAS)


def test_retirement_requires_six_terminal_zero_lanes() -> None:
    report = _retirement_report(state="terminal")

    assert host._retirement_is_drained(report, CANDIDATE_SHAS) is True
    requests = report["requests"]
    assert isinstance(requests, list)
    requests.pop()
    with pytest.raises(host.RuntimeHostError, match="lane is missing"):
        host._retirement_is_drained(report, CANDIDATE_SHAS)


def test_retirement_uses_distinct_candidate_set_and_rejects_one_wrong_binding() -> None:
    report = _retirement_report(state="active")

    assert len(host._retirement_request_ids(report, CANDIDATE_SHAS)) == len(host.INSTANCES)
    requests = report["requests"]
    assert isinstance(requests, list)
    hongjian = next(
        item["request"]
        for item in requests
        if item["request"]["sandbox"] == "hongjian"
        and item["request"]["pool"] == "gb10"
    )
    hongjian["candidate_sha"] = SHA
    with pytest.raises(host.RuntimeHostError, match="lane is missing"):
        host._retirement_request_ids(report, CANDIDATE_SHAS)


def test_activation_rejects_zero_but_nonterminal_request() -> None:
    report, selected = _zero_broker_preflight()
    requests = report["requests"]
    assert isinstance(requests, list)
    request = requests[0]["request"]
    assert isinstance(request, dict)
    request["state"] = "pending"

    with pytest.raises(host.RuntimeHostError, match="terminal zero-capacity"):
        host._validate_zero_broker_handoffs(report, selected, CANDIDATE_SHAS)


def test_activation_rejects_residual_active_control_plane_policy() -> None:
    policy = {
        "enabled": False,
        "min_slots": 0,
        "max_slots": 0,
        "actuator_config": {"candidate_sha": SHA},
        "last_pending_slots": 0,
        "last_actual_slots": 1,
        "last_draining_slots": 0,
        "last_occupied_slots": 0,
        "last_queued_slots": 0,
    }

    with pytest.raises(host.RuntimeHostError, match="live capacity"):
        host._validate_zero_cp_policy(policy, candidate_sha=SHA)


def test_activation_rejects_residual_live_slurm_worker_job() -> None:
    status = {
        "summary": [],
        "jobs": [
            {
                "environment": "sandbox-qianyi",
                "pool_name": "gb10",
                "state": "running",
            },
        ],
    }

    with pytest.raises(host.RuntimeHostError, match="live Slurm"):
        host._validate_zero_worker_status(
            status,
            environment="sandbox-qianyi",
            pool_name="gb10",
        )


def test_bootstrap_and_admit_zero_readback_reject_enabled_handoff() -> None:
    enabled = SimpleNamespace(enabled=True, min_slots=0, max_slots=1)
    zero_state = {
        "pending_slots": 0,
        "active_slots": 0,
        "draining_slots": 0,
    }

    with pytest.raises(host.RuntimeHostError, match="not disabled at zero"):
        host._validate_zero_adapter_state(enabled, zero_state)

    disabled = SimpleNamespace(enabled=False, min_slots=0, max_slots=0)
    host._validate_zero_adapter_state(disabled, zero_state)
    zero_state["active_slots"] = 1
    with pytest.raises(host.RuntimeHostError, match="live capacity"):
        host._validate_zero_adapter_state(disabled, zero_state)


def test_activation_orders_supervisor_generation_before_adapters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    events: list[str] = []

    def fake_run(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        events.append(" ".join(argv[1:]))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(host, "_run", fake_run)
    monkeypatch.setattr(host, "_service_result", lambda unit: ("success", "0"))
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda value, code, *args: events.append("generation-readback"),
    )

    host._activate_units(candidate)

    supervisor_start = events.index(f"start {host.SUPERVISOR_SERVICE}")
    generation = events.index("generation-readback")
    supervisor_timer = events.index(f"enable --now {host.SUPERVISOR_TIMER}")
    first_adapter = events.index(f"start {host.ADAPTER_SERVICES[0]}")
    assert supervisor_start < generation < supervisor_timer < first_adapter
    assert sum(event.startswith("start loom-shared-capacity-adapter@") for event in events) == 6


def test_activated_readback_checks_generation_and_six_cp_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_candidate_python(
        _candidate: host.Candidate,
        code: str,
        *args: str,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((code, args))
        if code == host._ADAPTER_BINDING_READBACK:
            return _binding_readback(Path(args[0]).stem)
        return subprocess.CompletedProcess(("candidate-python",), 0, stdout="", stderr="")

    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        fake_candidate_python,
    )

    host._activated_adapter_readback(candidate, require_zero=False)

    assert all(code == host._ADAPTER_BINDING_READBACK for code, _args in calls[:6])
    assert calls[6][0] == host._GENERATION_READBACK
    assert len(calls) == 13
    adapter_calls = calls[7:]
    assert all(code == host._ACTIVATED_ADAPTER_READBACK for code, _args in adapter_calls)
    for _code, args in adapter_calls:
        sandbox, _pool = Path(args[0]).stem.rsplit("-", 1)
        assert args[1:] == (
            ADAPTER_CANDIDATES[sandbox]["sha"],
            ADAPTER_CANDIDATES[sandbox]["tree"],
            "allow-positive",
        )


def test_fourth_adapter_failure_stops_activation_after_zero_cp_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    zero_policy = {
        "enabled": False,
        "min_slots": 0,
        "max_slots": 0,
        "actuator_config": {"candidate_sha": SHA},
        "last_pending_slots": 0,
        "last_actual_slots": 0,
        "last_draining_slots": 0,
        "last_occupied_slots": 0,
        "last_queued_slots": 0,
    }
    host._validate_zero_cp_policy(zero_policy, candidate_sha=SHA)
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(host, "_run", fake_run)
    monkeypatch.setattr(host, "_run_candidate_python", lambda *args: None)
    monkeypatch.setattr(
        host,
        "_service_result",
        lambda unit: ("failed", "1") if unit == host.ADAPTER_SERVICES[3] else ("success", "0"),
    )

    with pytest.raises(host.RuntimeHostError, match="adapter activation"):
        host._activate_units(candidate)

    started = [command[2] for command in commands if command[1] == "start"]
    assert started[-1] == host.ADAPTER_SERVICES[3]
    assert host.ADAPTER_SERVICES[4] not in started


def test_activation_failure_restores_installed_inactive_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_repo = host.CANDIDATE_PARENT / SHA / "repo"
    transaction_id = "2" * 32
    journal_path = Path("/var/lib/loom-shared-capacity/activation.json")
    journal = {
        "transaction_id": transaction_id,
        "operation": "activate",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": True,
        "staging_path": (f"/opt/loom-shared-capacity/candidates/.install-{SHA}-{transaction_id}"),
        "files": {},
        "units": {},
    }
    restored: list[Path] = []
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda path, label: {
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "activation_status": "installed",
            "transaction_id": "1" * 32,
        },
    )
    monkeypatch.setattr(host, "check", lambda *args, **kwargs: {"status": "pass"})
    monkeypatch.setattr(host, "_activation_preflight", lambda value, **kwargs: None)
    monkeypatch.setattr(
        host,
        "_write_journal",
        lambda value, **kwargs: (journal_path, journal),
    )
    monkeypatch.setattr(host, "_update_journal", lambda *args: None)
    monkeypatch.setattr(
        host,
        "_activate_units",
        lambda value: (_ for _ in ()).throw(host.RuntimeHostError("adapter failed")),
    )
    monkeypatch.setattr(
        host,
        "_restore_transaction",
        lambda path, payload: restored.append(path),
    )

    with pytest.raises(host.RuntimeHostError, match="adapter failed"):
        host.activate(SHA)

    assert restored == [journal_path]
    assert candidate_repo == host.CANDIDATE_PARENT / SHA / "repo"


def test_successful_activation_commits_bootstrap_with_admission_fenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_id = "2" * 32
    admission_token = "1" * 32
    journal_path = tmp_path / "activation.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    journal: dict[str, object] = {
        "transaction_id": transaction_id,
        "operation": "activate",
    }
    phases: list[str] = []
    states: list[dict[str, object]] = []
    preflight_tokens: list[str] = []

    def write_activation_journal(
        _candidate: host.Candidate,
        **kwargs: object,
    ) -> tuple[Path, dict[str, object]]:
        journal["admission_token"] = kwargs["admission_token"]
        return journal_path, journal

    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda path, label: {
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "activation_status": "installed",
            "transaction_id": admission_token,
        },
    )
    monkeypatch.setattr(
        host,
        "check",
        lambda _candidate, *, activation_mode="installed": {
            "status": "pass",
            "activation_status": activation_mode,
        },
    )
    monkeypatch.setattr(
        host,
        "_write_journal",
        write_activation_journal,
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda _path, _payload, phase: phases.append(phase),
    )
    monkeypatch.setattr(
        host,
        "_activation_preflight",
        lambda _candidate, *, transaction_id: preflight_tokens.append(transaction_id),
    )
    monkeypatch.setattr(host, "_activate_units", lambda _candidate: None)
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda _path, content, **_kwargs: states.append(json.loads(content)),
    )
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda *_args: pytest.fail("bootstrap must keep admission fenced"),
    )

    report = host.activate(SHA)

    assert report["activation_status"] == "bootstrap-active"
    assert preflight_tokens == [admission_token]
    assert journal["admission_token"] == admission_token
    assert states[-1]["activation_status"] == "bootstrap-active"
    assert phases[-2:] == ["units-activated", "committed"]
    assert not active_path.exists()


def test_admission_authorization_persists_digest_before_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_id = "2" * 32
    admission_token = "1" * 32
    digest = "d" * 64
    gate6_digest = "6" * 64
    journal_path = Path("/var/lib/loom-shared-capacity/admit.json")
    journal: dict[str, object] = {
        "transaction_id": transaction_id,
        "operation": "admit",
    }
    phases: list[str] = []
    resumed: list[dict[str, object]] = []
    zero_reads: list[bool] = []

    def write_admission_journal(
        _candidate: host.Candidate,
        **kwargs: object,
    ) -> tuple[Path, dict[str, object]]:
        journal["admission_token"] = kwargs["admission_token"]
        return journal_path, journal

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda path, label: {
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "activation_status": "bootstrap-active",
            "transaction_id": admission_token,
        },
    )
    monkeypatch.setattr(host, "check", lambda *args, **kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_positive_capacity_admission_gate",
        lambda _candidate: (ADAPTER_CANDIDATES, digest, gate6_digest),
    )
    monkeypatch.setattr(
        host,
        "_write_journal",
        write_admission_journal,
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda _path, _payload, phase: phases.append(phase),
    )
    monkeypatch.setattr(
        host,
        "_activated_adapter_readback",
        lambda _candidate, *, require_zero: zero_reads.append(require_zero),
    )
    monkeypatch.setattr(host, "_admission_fence", lambda _candidate: admission_token)
    monkeypatch.setattr(
        host,
        "_resume_admission",
        lambda _path, payload: resumed.append(dict(payload)) or {"status": "pass"},
    )

    assert host.admit(SHA) == {"status": "pass"}
    assert phases == ["admission-validating", "admission-authorized"]
    assert journal["admission_token"] == admission_token
    assert journal["platform_health_payload_sha256"] == digest
    assert journal["gate6_payload_sha256"] == gate6_digest
    assert resumed[0]["platform_health_payload_sha256"] == digest
    assert resumed[0]["gate6_payload_sha256"] == gate6_digest
    assert zero_reads == [True]


def test_authorized_admission_resume_opens_exact_fence_then_commits_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_id = "2" * 32
    admission_token = "1" * 32
    digest = "d" * 64
    gate6_digest = "6" * 64
    journal_path = tmp_path / "admit.json"
    active_path = tmp_path / "active.json"
    state_path = tmp_path / "state.json"
    active_path.write_text("{}\n", encoding="utf-8")
    state_path.write_text(
        json.dumps(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "activation_status": "bootstrap-active",
                "transaction_id": admission_token,
            },
        ),
        encoding="utf-8",
    )
    payload: dict[str, object] = {
        "transaction_id": transaction_id,
        "operation": "admit",
        "phase": "admission-authorized",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": admission_token,
        "platform_health_payload_sha256": digest,
        "gate6_payload_sha256": gate6_digest,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda _path, _payload: (
            transaction_id,
            "admit",
            SHA,
            TREE,
            {},
            {},
        ),
    )
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda _candidate, token: events.append(f"close:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_positive_capacity_admission_gate",
        lambda _candidate: (ADAPTER_CANDIDATES, digest, gate6_digest),
    )
    monkeypatch.setattr(
        host,
        "_activated_adapter_readback",
        lambda _candidate, *, require_zero: events.append(f"zero:{require_zero}"),
    )
    monkeypatch.setattr(host, "_admission_fence", lambda _candidate: admission_token)
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda _candidate, token: events.append(f"open:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda _path, data, phase: (events.append(phase), data.update(phase=phase)),
    )
    monkeypatch.setattr(
        host,
        "check",
        lambda _candidate, *, activation_mode: (
            events.append(f"check:{activation_mode}") or {"status": "pass"}
        ),
    )
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: path.write_bytes(content),
    )
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)

    assert host._resume_admission(journal_path, payload) == {"status": "pass"}
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert events == [
        f"close:{admission_token}",
        "zero:True",
        f"open:{admission_token}",
        "admission-open",
        "state-activated",
        "check:activated",
        "committed",
    ]
    assert state["activation_status"] == "activated"
    assert state["platform_health_payload_sha256"] == digest
    assert state["gate6_payload_sha256"] == gate6_digest
    assert not active_path.exists()


def test_stale_admission_resume_recloses_and_drains_before_restoring_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_id = "2" * 32
    admission_token = "1" * 32
    authorized_digest = "d" * 64
    current_digest = "e" * 64
    gate6_digest = "6" * 64
    journal_path = tmp_path / "admit.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    payload: dict[str, object] = {
        "transaction_id": transaction_id,
        "operation": "admit",
        "phase": "admission-authorized",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": admission_token,
        "platform_health_payload_sha256": authorized_digest,
        "gate6_payload_sha256": gate6_digest,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_active_journal", lambda: (journal_path, payload))
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda _path, _payload: (
            transaction_id,
            "admit",
            SHA,
            TREE,
            {},
            {},
        ),
    )
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda _candidate, token: events.append(f"close:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_positive_capacity_admission_gate",
        lambda _candidate: (ADAPTER_CANDIDATES, current_digest, gate6_digest),
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda _candidate, token: events.append(f"drain:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_verify_activated_capacity_drained",
        lambda _candidate, token: events.append(f"zero:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda _path, _payload, **_kwargs: (
            events.append("bootstrap-restored")
            or (transaction_id, "admit", SHA, TREE)
        ),
    )
    monkeypatch.setattr(host, "_update_journal", lambda *_args: None)
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)

    with pytest.raises(host.RuntimeHostError, match="evidence digest drifted"):
        host._recover_orphan()

    assert events == [
        f"close:{admission_token}",
        f"close:{admission_token}",
        f"drain:{admission_token}",
        f"zero:{admission_token}",
        "bootstrap-restored",
    ]
    assert not active_path.exists()


def test_post_open_check_failure_drains_racing_request_before_bootstrap_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_id = "2" * 32
    admission_token = "1" * 32
    digest = "d" * 64
    gate6_digest = "6" * 64
    journal_path = tmp_path / "admit.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    journal: dict[str, object] = {
        "transaction_id": transaction_id,
        "operation": "admit",
        "phase": "prepared",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": True,
        "admission_token": admission_token,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda _path, _label: {
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "activation_status": "bootstrap-active",
            "transaction_id": admission_token,
        },
    )

    def check_runtime(
        _candidate: host.Candidate,
        *,
        activation_mode: str,
    ) -> dict[str, object]:
        if activation_mode == "activated":
            events.append("racing-positive-request-observed")
            raise host.RuntimeHostError("post-open check failed")
        return {"status": "pass"}

    monkeypatch.setattr(host, "check", check_runtime)
    monkeypatch.setattr(
        host,
        "_positive_capacity_admission_gate",
        lambda _candidate: (ADAPTER_CANDIDATES, digest, gate6_digest),
    )
    monkeypatch.setattr(
        host,
        "_write_journal",
        lambda _candidate, **_kwargs: (journal_path, journal),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda _path, data, phase: data.update(phase=phase),
    )
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda _path, _payload: (
            transaction_id,
            "admit",
            SHA,
            TREE,
            {},
            {},
        ),
    )
    monkeypatch.setattr(
        host,
        "_activated_adapter_readback",
        lambda _candidate, *, require_zero: events.append(f"zero-read:{require_zero}"),
    )
    monkeypatch.setattr(host, "_admission_fence", lambda _candidate: admission_token)
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda _candidate, token: events.append(f"close:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda _candidate, token: events.append(f"open:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda _candidate, token: events.append(f"drain:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_verify_activated_capacity_drained",
        lambda _candidate, token: events.append(f"drained-zero:{token}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda _path, _payload, **_kwargs: (
            events.append("bootstrap-restored")
            or (transaction_id, "admit", SHA, TREE)
        ),
    )
    monkeypatch.setattr(host, "_atomic_write", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "_fsync_directory", lambda _path: None)

    with pytest.raises(host.RuntimeHostError, match="post-open check failed"):
        host.admit(SHA)

    open_index = events.index(f"open:{admission_token}")
    race_index = events.index("racing-positive-request-observed")
    drain_index = events.index(f"drain:{admission_token}")
    restore_index = events.index("bootstrap-restored")
    assert open_index < race_index < drain_index < restore_index
    assert f"drained-zero:{admission_token}" in events


def test_admission_rejects_authority_gate_before_transaction_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutations: list[str] = []
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(host, "_reject_orphan_stages", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda path, label: {
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "activation_status": "bootstrap-active",
            "transaction_id": "1" * 32,
        },
    )
    monkeypatch.setattr(host, "check", lambda *args, **kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_positive_capacity_admission_gate",
        lambda _candidate: (_ for _ in ()).throw(
            host.RuntimeHostError("platform-health activation gate is not satisfied"),
        ),
    )
    monkeypatch.setattr(
        host,
        "_write_journal",
        lambda *_args, **_kwargs: mutations.append("journal"),
    )

    with pytest.raises(host.RuntimeHostError, match="activation gate"):
        host.admit(SHA)

    assert mutations == []


def test_activated_rollback_reopens_fence_only_after_external_and_local_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.json"
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    transaction_id = "1" * 32
    payload = {
        "phase": "rollback-closing-admission",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": False,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda item, data: (transaction_id, "install", SHA, TREE, {}, {}),
    )
    monkeypatch.setattr(host, "_validate_rollback_recovery", lambda data: None)
    monkeypatch.setattr(
        host,
        "_request_capacity_retirement",
        lambda candidate, token: events.append("close-and-cancel") or "drained",
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda candidate, token: events.append("external-drained"),
    )
    monkeypatch.setattr(
        host,
        "_verify_activated_capacity_drained",
        lambda candidate, token: events.append("external-readback"),
    )
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda item, data, **kwargs: (
            events.append("local-restored") or (transaction_id, "install", SHA, TREE)
        ),
    )
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda candidate, token: events.append("fence-open"),
    )
    monkeypatch.setattr(
        host,
        "_remove_path",
        lambda value: events.append("candidate-removed"),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda item, data, phase: data.update(phase=phase) or events.append(f"phase:{phase}"),
    )
    monkeypatch.setattr(
        host,
        "_fsync_directory",
        lambda value: events.append("journal-cleared"),
    )

    host._complete_activated_rollback(path, payload)

    assert events.index("external-readback") < events.index("local-restored")
    assert events.index("local-restored") < events.index("fence-open")
    assert events.index("fence-open") < events.index("candidate-removed")
    assert payload["phase"] == "rolled-back"
    assert not active.exists()


def test_activated_rollback_partial_drain_failure_keeps_fence_and_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = Path("/var/lib/loom-shared-capacity/rollback.json")
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    transaction_id = "1" * 32
    payload = {
        "phase": "rollback-closing-admission",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": False,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active)
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda item, data: (transaction_id, "install", SHA, TREE, {}, {}),
    )
    monkeypatch.setattr(host, "_validate_rollback_recovery", lambda data: None)
    monkeypatch.setattr(
        host,
        "_request_capacity_retirement",
        lambda candidate, token: events.append("fence-closed") or "pending",
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda candidate, token: (_ for _ in ()).throw(
            host.RuntimeHostError("partial adapter failure"),
        ),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda item, data, phase: data.update(phase=phase) or events.append(f"phase:{phase}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda *args, **kwargs: events.append("local-restored"),
    )
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda *args: events.append("fence-open"),
    )

    with pytest.raises(host.RuntimeHostError, match="partial adapter failure"):
        host._complete_activated_rollback(path, payload)

    assert payload["phase"] == "rollback-draining"
    assert events == ["fence-closed", "phase:rollback-draining"]
    assert active.exists()


def test_activated_rollback_resumes_after_local_restore_before_opening_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.json"
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    transaction_id = "1" * 32
    payload = {
        "phase": "rollback-restored-fenced",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": False,
    }
    events: list[str] = []
    monkeypatch.setattr(host, "ACTIVE_JOURNAL_PATH", active)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(
        host,
        "_validate_transaction",
        lambda item, data: (transaction_id, "install", SHA, TREE, {}, {}),
    )
    monkeypatch.setattr(host, "_validate_rollback_recovery", lambda data: None)
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda candidate, token: events.append("fence-open"),
    )
    monkeypatch.setattr(
        host,
        "_remove_path",
        lambda value: events.append("candidate-removed"),
    )
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda item, data, phase: data.update(phase=phase),
    )
    monkeypatch.setattr(host, "_fsync_directory", lambda value: None)
    monkeypatch.setattr(
        host,
        "_restore_local_transaction",
        lambda *args, **kwargs: events.append("unexpected-restore"),
    )
    monkeypatch.setattr(
        host,
        "_drain_activated_capacity",
        lambda *args: events.append("unexpected-drain"),
    )

    host._resume_activated_rollback(path, payload)

    assert events == ["fence-open", "candidate-removed"]
    assert payload["phase"] == "rolled-back"
    assert not active.exists()


@pytest.mark.parametrize(
    "previous_program",
    (b"#!/bin/sh\nexit 99\n", None),
    ids=("old-public-program", "removed-public-program"),
)
def test_persisted_disk_recovery_survives_public_program_restore_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    previous_program: bytes | None,
) -> None:
    recovery_path = tmp_path / "runtime-host-recovery"
    public_program = tmp_path / "runtime-host"
    journal_path = tmp_path / "journal.json"
    active_path = tmp_path / "active.json"
    active_path.write_text("{}\n", encoding="utf-8")
    source = Path(host.__file__).read_bytes()
    payload: dict[str, object] = {
        "transaction_id": "1" * 32,
        "operation": "install",
        "phase": "committed",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_previously_existed": True,
    }
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)

    def atomic_write(path: Path, content: bytes, *, mode: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(mode)

    monkeypatch.setattr(host, "RECOVERY_PROGRAM_PATH", recovery_path)
    monkeypatch.setattr(host, "_read_candidate_file", lambda *args: source)
    monkeypatch.setattr(host, "_atomic_write", atomic_write)
    monkeypatch.setattr(
        host,
        "_update_journal",
        lambda path, data, phase: data.update(phase=phase),
    )

    host._prepare_rollback_recovery(journal_path, payload, candidate)
    host._validate_rollback_recovery(payload)
    if previous_program is not None:
        public_program.write_bytes(previous_program)
    payload["phase"] = "rollback-drained"

    module_name = "persisted_shared_capacity_recovery"
    spec = importlib.util.spec_from_loader(
        module_name,
        SourceFileLoader(module_name, str(recovery_path)),
    )
    assert spec is not None and spec.loader is not None
    recovery = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = recovery
    try:
        spec.loader.exec_module(recovery)
        events: list[str] = []
        report = _retirement_report(state="terminal")
        recovery.RECOVERY_PROGRAM_PATH = recovery_path
        recovery.ACTIVE_JOURNAL_PATH = active_path
        recovery.INSTALLER_ROOT = tmp_path
        recovery._require_live_host = lambda: None
        recovery._lock = nullcontext
        recovery._active_journal = lambda: (journal_path, payload)
        recovery._validate_transaction = lambda path, data: (
            "1" * 32,
            "install",
            SHA,
            TREE,
            {},
            {},
        )

        def verify_six_lanes(_candidate: object, _token: str) -> None:
            assert recovery._retirement_is_drained(report, CANDIDATE_SHAS)
            events.append("six-lanes-terminal")

        recovery._verify_activated_capacity_drained = verify_six_lanes
        recovery._restore_local_transaction = lambda path, data, **kwargs: (
            events.append("local-restored") or ("1" * 32, "install", SHA, TREE)
        )
        recovery._open_activation_admission = lambda candidate, token: events.append("fence-open")
        recovery._update_journal = lambda path, data, phase: data.update(phase=phase)
        recovery._fsync_directory = lambda path: events.append("journal-cleared")
        recovery._recover_orphan = lambda: recovery._resume_activated_rollback(
            journal_path,
            payload,
        )

        assert recovery.main(("recover", "--execute")) == 0
    finally:
        sys.modules.pop(spec.name, None)

    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "recovered"
    assert events == [
        "six-lanes-terminal",
        "local-restored",
        "fence-open",
        "journal-cleared",
    ]
    assert payload["phase"] == "rolled-back"
    assert not active_path.exists()
    if previous_program is None:
        assert not public_program.exists()
    else:
        assert public_program.read_bytes() == previous_program
    assert recovery_path.exists()


def test_install_refuses_to_replace_an_activated_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "activation_status": "activated",
            },
        ),
        encoding="utf-8",
    )
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    journal_written = False
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_load_candidate_profile", lambda value: {})
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)

    def write_journal(*args: object, **kwargs: object) -> None:
        nonlocal journal_written
        journal_written = True

    monkeypatch.setattr(host, "_write_journal", write_journal)

    with pytest.raises(host.RuntimeHostError, match="retired through rollback"):
        host.install(candidate)

    assert journal_written is False


def test_rollback_requires_exact_active_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(
        host,
        "_load_json",
        lambda path, label: {
            "candidate_sha": "c" * 40,
            "transaction_id": "1" * 32,
        },
    )

    with pytest.raises(host.RuntimeHostError, match="does not match"):
        host.rollback(SHA)


def test_public_failure_output_is_secret_safe(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        host,
        "_candidate_identity",
        lambda source, sha: (_ for _ in ()).throw(
            host.RuntimeHostError("Bearer loom_admin_should_not_escape"),
        ),
    )

    assert (
        host.main(
            (
                "install",
                "--source-repo",
                str(host.REPO_ROOT),
                "--candidate-sha",
                SHA,
                "--execute",
            ),
        )
        == 1
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: shared-capacity runtime host failed safely\n"
    assert "loom_admin" not in captured.err


def _acceptance_session(
    *,
    phase: str = "multi_candidate_overlap",
    offset: int = 0,
) -> dict[str, object]:
    return {
        "session_id": "2" * 32,
        "status": "running",
        "candidates": ADAPTER_CANDIDATES,
        "next_phase_index": host.LIVE_PHASES.index(phase) * len(host.SANDBOXES) + offset,
        "phase_checkpoints": len(host.LIVE_PHASES) * len(host.SANDBOXES),
        "phases": list(host.LIVE_PHASES),
    }


@pytest.mark.parametrize("offset", (0, 1, 2))
def test_acceptance_phase_authorization_allows_exact_checkpoint_prefix(
    offset: int,
) -> None:
    host._require_acceptance_phase_prefix(
        _acceptance_session(phase="fairness_contention", offset=offset),
        "fairness_contention",
    )


@pytest.mark.parametrize(
    ("phase", "offset"),
    (("large_batch_burst", 2), ("mixed_non_loom", 0)),
)
def test_acceptance_phase_authorization_rejects_adjacent_phase_prefix(
    phase: str,
    offset: int,
) -> None:
    with pytest.raises(host.RuntimeHostError, match="prefix"):
        host._require_acceptance_phase_prefix(
            _acceptance_session(phase=phase, offset=offset),
            "fairness_contention",
        )


def test_acceptance_contract_is_exact_candidate_policy_and_fixed_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    monkeypatch.setattr(
        host,
        "_adapter_candidate_bindings",
        lambda _candidate: ADAPTER_CANDIDATES,
    )
    monkeypatch.setattr(
        host,
        "_candidate_platform_policy",
        lambda _candidate, pool: (
            {
                "requested_concurrency": 2 if pool == "gb10" else 4,
                "slot_budget": 6 if pool == "gb10" else 12,
                "pending_slot_budget": 4 if pool == "gb10" else 10,
            },
            "d" * 64,
        ),
    )

    contract = host._acceptance_contract(
        candidate,
        admission_token="1" * 32,
        session=_acceptance_session(),
        now=NOW,
    )

    assert contract["candidate_shas"] == CANDIDATE_SHAS
    assert contract["target_slots"] == {"gb10": 2, "oldlab": 4}
    assert contract["ttl_seconds"] == {
            phase: (
                host.ACCEPTANCE_TTL_CLEANUP_SECONDS
                if phase == "ttl_cleanup"
                else (
                    host.ACCEPTANCE_MIXED_NON_LOOM_TTL_SECONDS
                    if phase == "mixed_non_loom"
                    else host.ACCEPTANCE_DEFAULT_PHASE_TTL_SECONDS
                )
            )
        for phase in host.ACCEPTANCE_PHASES
    }
    assert contract["expires_at"] == (
        NOW + timedelta(seconds=host.ACCEPTANCE_CONTRACT_TTL_SECONDS)
    ).isoformat()


def test_acceptance_broker_readback_accepts_canonical_list_and_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    rows = [{"request": {"id": str(index)}} for index in range(6)]
    outputs = iter(
        (
            subprocess.CompletedProcess(
                ("candidate-python",),
                0,
                stdout=host._canonical_json_value(rows).decode(),
                stderr="",
            ),
            subprocess.CompletedProcess(
                ("candidate-python",),
                0,
                stdout=host._canonical_json_value(None).decode(),
                stderr="",
            ),
        ),
    )
    monkeypatch.setattr(host, "_run_candidate_python", lambda *_args: next(outputs))
    monkeypatch.setattr(
        host,
        "_candidate_broker_state_db",
        lambda _candidate: Path("/var/lib/loom-shared-capacity/broker.sqlite3"),
    )

    assert (
        host._run_acceptance_program(
            candidate,
            host._ACCEPTANCE_COHORT,
            "1" * 32,
            "2" * 32,
            "fairness_contention",
        )
        == rows
    )
    assert host._acceptance_contract_readback(candidate) is None


def test_acceptance_cohort_journals_before_exact_broker_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    state = {
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "transaction_id": "1" * 32,
        "activation_status": "acceptance-active",
        "acceptance_session_id": "2" * 32,
    }
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(
        host,
        "_acceptance_state_candidate",
        lambda **_kwargs: (candidate, state, "1" * 32),
    )
    monkeypatch.setattr(host, "check", lambda *_args, **_kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_acceptance_session_readback",
        lambda *_args: _acceptance_session(phase="fairness_contention"),
    )
    monkeypatch.setattr(
        host,
        "_write_acceptance_operation",
        lambda payload: events.append(("journal", dict(payload))),
    )
    monkeypatch.setattr(
        host,
        "_run_acceptance_program",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        host,
        "_resume_acceptance_operation",
        lambda: events.append(("replay", None)) or {"status": "pass"},
    )

    assert host.acceptance_cohort("2" * 32, "fairness_contention") == {
        "status": "pass",
    }
    assert [event[0] for event in events] == ["journal", "replay"]
    payload = events[0][1]
    assert isinstance(payload, dict)
    assert payload == {
        "schema_version": 1,
        "operation": "cohort",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
        "phase": "fairness_contention",
        "mode": "rotate",
        "step": "prepared",
        "checkpoint_offset": 0,
    }


def test_acceptance_late_checkpoint_prefix_requires_existing_exact_cohort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    journaled: list[dict[str, object]] = []
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(
        host,
        "_acceptance_state_candidate",
        lambda **_kwargs: (candidate, {}, "1" * 32),
    )
    monkeypatch.setattr(host, "check", lambda *_args, **_kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_acceptance_session_readback",
        lambda *_args: _acceptance_session(
            phase="fairness_contention",
            offset=1,
        ),
    )
    monkeypatch.setattr(host, "_run_acceptance_program", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_write_acceptance_operation",
        lambda payload: journaled.append(dict(payload)),
    )

    with pytest.raises(host.RuntimeHostError, match="before the first"):
        host.acceptance_cohort("2" * 32, "fairness_contention")

    assert journaled == []


@pytest.mark.parametrize("offset", (1, 2))
def test_acceptance_open_rejects_late_multi_prefix_without_state_change(
    offset: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = host.Candidate(SHA, TREE, host.REPO_ROOT)
    state = {
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "transaction_id": "1" * 32,
        "activation_status": "bootstrap-active",
    }
    journaled: list[dict[str, object]] = []
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_lock", nullcontext)
    monkeypatch.setattr(host, "_recover_orphan", lambda: None)
    monkeypatch.setattr(
        host,
        "_acceptance_state_candidate",
        lambda **_kwargs: (candidate, state, "1" * 32),
    )
    monkeypatch.setattr(host, "check", lambda *_args, **_kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_acceptance_session_readback",
        lambda *_args: _acceptance_session(
            phase="multi_candidate_overlap",
            offset=offset,
        ),
    )
    monkeypatch.setattr(
        host,
        "_write_acceptance_operation",
        lambda payload: journaled.append(dict(payload)),
    )

    with pytest.raises(host.RuntimeHostError, match="opening prefix"):
        host.acceptance_open("2" * 32)

    assert state["activation_status"] == "bootstrap-active"
    assert journaled == []


def test_acceptance_cohort_rotation_crash_replays_drain_then_same_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_path = tmp_path / "acceptance-operation.json"
    state_path = tmp_path / "state.json"
    payload = {
        "schema_version": 1,
        "operation": "cohort",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
        "phase": "large_batch_burst",
        "mode": "rotate",
        "step": "prepared",
        "checkpoint_offset": 0,
    }
    state_path.write_bytes(
        host._canonical_json(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "transaction_id": "1" * 32,
                "activation_status": "acceptance-active",
                "acceptance_session_id": "2" * 32,
            },
        ),
    )
    operation_path.write_bytes(host._canonical_json(payload))
    operation_path.chmod(0o600)
    events: list[str] = []
    rows = [{"request": {"id": str(index)}} for index in range(6)]

    def drain(*_args: object) -> None:
        events.append("drain")
        if events.count("drain") == 1:
            raise host.RuntimeHostError("simulated drain interruption")

    def broker(
        _candidate: host.Candidate,
        code: str,
        *_args: str,
    ) -> object:
        if code == host._ACCEPTANCE_COHORT:
            events.append("create")
            return rows
        if code == host._ACCEPTANCE_COHORT_STATUS:
            events.append("readback")
            return rows
        raise AssertionError("unexpected acceptance program")

    monkeypatch.setattr(host, "ACCEPTANCE_OPERATION_PATH", operation_path)
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "CANDIDATE_PARENT", tmp_path / "candidates")
    monkeypatch.setattr(host, "_drain_acceptance_capacity", drain)
    monkeypatch.setattr(host, "_verify_acceptance_capacity_drained", lambda *_args: None)
    monkeypatch.setattr(host, "_run_acceptance_program", broker)
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: path.write_bytes(content),
    )

    with pytest.raises(host.RuntimeHostError, match="interruption"):
        host._recover_acceptance_operation()
    assert operation_path.exists()
    host._recover_acceptance_operation()

    assert events == ["drain", "drain", "create", "readback"]
    assert not operation_path.exists()


def test_acceptance_open_check_failure_compensates_and_recovery_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_path = tmp_path / "acceptance-operation.json"
    state_path = tmp_path / "state.json"
    contract = {
        "schema_version": 1,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
    }
    payload = {
        "schema_version": 1,
        "operation": "open",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
        "contract": contract,
        "step": "state-active",
    }
    state_path.write_bytes(
        host._canonical_json(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "transaction_id": "1" * 32,
                "activation_status": "acceptance-active",
                "acceptance_session_id": "2" * 32,
                "acceptance_contract_sha256": host._sha256(
                    host._canonical_json(contract),
                ),
            },
        ),
    )
    operation_path.write_bytes(host._canonical_json(payload))
    operation_path.chmod(0o600)
    events: list[str] = []
    monkeypatch.setattr(host, "ACCEPTANCE_OPERATION_PATH", operation_path)
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "CANDIDATE_PARENT", tmp_path / "candidates")
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda *_args: events.append("general-fence-closed"),
    )
    monkeypatch.setattr(
        host,
        "_drain_acceptance_capacity",
        lambda *_args: events.append("drain"),
    )
    monkeypatch.setattr(
        host,
        "_verify_acceptance_capacity_drained",
        lambda *_args: events.append("six-zero"),
    )
    monkeypatch.setattr(
        host,
        "_run_acceptance_program",
        lambda *_args: events.append("contract-closed"),
    )
    monkeypatch.setattr(
        host,
        "check",
        lambda _candidate, *, activation_mode: (
            (_ for _ in ()).throw(host.RuntimeHostError("persistent check failure"))
            if activation_mode == "acceptance-active"
            else {"status": "pass", "activation_status": activation_mode}
        ),
    )
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: path.write_bytes(content),
    )

    host._recover_acceptance_operation()
    host._recover_acceptance_operation()

    assert events == [
        "general-fence-closed",
        "drain",
        "six-zero",
        "contract-closed",
    ]
    assert json.loads(state_path.read_text())["activation_status"] == "bootstrap-active"
    assert not operation_path.exists()


def test_acceptance_open_crash_replays_identical_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_path = tmp_path / "acceptance-operation.json"
    state_path = tmp_path / "state.json"
    contract = {
        "schema_version": 1,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
    }
    payload = {
        "schema_version": 1,
        "operation": "open",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
        "contract": contract,
        "step": "contract-open",
    }
    state_path.write_bytes(
        host._canonical_json(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "transaction_id": "1" * 32,
                "activation_status": "bootstrap-active",
            },
        ),
    )
    operation_path.write_bytes(host._canonical_json(payload))
    operation_path.chmod(0o600)
    calls: list[tuple[str, ...]] = []

    def successful_check(
        _candidate: host.Candidate,
        *,
        activation_mode: str,
    ) -> dict[str, object]:
        return {"status": "pass", "activation_status": activation_mode}

    monkeypatch.setattr(host, "ACCEPTANCE_OPERATION_PATH", operation_path)
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "CANDIDATE_PARENT", tmp_path / "candidates")
    monkeypatch.setattr(
        host,
        "_run_acceptance_program",
        lambda _candidate, _code, *args: calls.append(args) or contract,
    )
    monkeypatch.setattr(host, "check", successful_check)
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: path.write_bytes(content),
    )

    host._recover_acceptance_operation()

    assert calls == []
    assert not operation_path.exists()
    assert json.loads(state_path.read_text())["activation_status"] == "acceptance-active"


def test_acceptance_close_replay_drains_before_contract_close_and_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_path = tmp_path / "acceptance-operation.json"
    state_path = tmp_path / "state.json"
    payload = {
        "schema_version": 1,
        "operation": "close",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "admission_token": "1" * 32,
        "session_id": "2" * 32,
    }
    state_path.write_bytes(
        host._canonical_json(
            {
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "transaction_id": "1" * 32,
                "activation_status": "acceptance-active",
                "acceptance_session_id": "2" * 32,
                "acceptance_contract_sha256": "d" * 64,
            },
        ),
    )
    operation_path.write_bytes(host._canonical_json(payload))
    operation_path.chmod(0o600)
    events: list[str] = []
    monkeypatch.setattr(host, "ACCEPTANCE_OPERATION_PATH", operation_path)
    monkeypatch.setattr(host, "STATE_PATH", state_path)
    monkeypatch.setattr(host, "INSTALLER_ROOT", tmp_path)
    monkeypatch.setattr(host, "CANDIDATE_PARENT", tmp_path / "candidates")
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, content, **_kwargs: path.write_bytes(content),
    )
    monkeypatch.setattr(
        host,
        "_close_activation_admission",
        lambda *_args: events.append("general-fence-closed"),
    )
    monkeypatch.setattr(
        host,
        "_drain_acceptance_capacity",
        lambda *_args: events.append("drain"),
    )
    monkeypatch.setattr(
        host,
        "_verify_acceptance_capacity_drained",
        lambda *_args: events.append("six-zero"),
    )
    monkeypatch.setattr(
        host,
        "_run_acceptance_program",
        lambda *_args: events.append("contract-closed"),
    )
    monkeypatch.setattr(
        host,
        "check",
        lambda *_args, **_kwargs: events.append("bootstrap-check") or {"status": "pass"},
    )

    host._recover_acceptance_operation()

    assert events == [
        "general-fence-closed",
        "drain",
        "six-zero",
        "contract-closed",
        "bootstrap-check",
    ]
    state = json.loads(state_path.read_text())
    assert state["activation_status"] == "bootstrap-active"
    assert "acceptance_session_id" not in state
    assert "acceptance_contract_sha256" not in state
    assert not operation_path.exists()


def test_recover_orphan_always_replays_acceptance_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovered: list[str] = []
    monkeypatch.setattr(host, "_active_journal", lambda: None)
    monkeypatch.setattr(
        host,
        "_recover_acceptance_operation",
        lambda: recovered.append("acceptance"),
    )

    host._recover_orphan()

    assert recovered == ["acceptance"]


def test_acceptance_cli_has_no_capacity_or_ttl_override() -> None:
    args = host._parser().parse_args(
        (
            "acceptance-cohort",
            "--session-id",
            "2" * 32,
            "--phase",
            "ttl_cleanup",
        ),
    )
    assert vars(args) == {
        "command": "acceptance-cohort",
        "session_id": "2" * 32,
        "phase": "ttl_cleanup",
        "execute": False,
    }
    with pytest.raises(SystemExit):
        host._parser().parse_args(
            (
                "acceptance-cohort",
                "--session-id",
                "2" * 32,
                "--phase",
                "ttl_cleanup",
                "--ttl-seconds",
                "3600",
            ),
        )
