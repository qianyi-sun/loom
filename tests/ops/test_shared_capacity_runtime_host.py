from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest
from scripts.ops import shared_capacity_runtime_host as host

SHA = "a" * 40
TREE = "b" * 40
NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
ADAPTER_CANDIDATES = {
    "qianyi": {"sha": SHA, "tree": TREE},
    "hongjian": {"sha": "c" * 40, "tree": "d" * 40},
    "devansh": {"sha": "e" * 40, "tree": "f" * 40},
}


def _platform_evidence(
    *,
    candidates: dict[str, dict[str, str]] | None = None,
    completed_at: datetime = NOW,
) -> tuple[dict[str, object], dict[str, object]]:
    evidence: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": "1" * 32,
        "candidates": candidates if candidates is not None else ADAPTER_CANDIDATES,
        "policy_capacity": {
            "oldlab": {"max_slots": 20},
            "gb10": {
                "max_slots": 112,
                "requested_cpus": 16,
                "requested_memory_mib": 92000,
                "requested_concurrency": 8,
                "reserved_cpu_cores_per_node": 4,
                "reserved_memory_mib_per_node": 23000,
            },
        },
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


def test_committed_activation_recovery_reopens_exact_admission_before_cleanup(
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
    monkeypatch.setattr(
        host,
        "_open_activation_admission",
        lambda candidate, token: events.append(("open", (candidate.sha, token))),
    )
    monkeypatch.setattr(host, "_fsync_directory", lambda value: events.append(("fsync", value)))

    host._recover_orphan()

    assert events[0] == ("open", (SHA, transaction_id))
    assert not active_journal.exists()
    assert events[1] == ("fsync", tmp_path)


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


def test_activation_preflight_checks_broker_and_all_six_receipts(
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
    monkeypatch.setattr(
        host,
        "_validate_platform_health_activation_gate",
        lambda value, bindings: calls.append(
            (
                "platform-health",
                (value.sha, value.tree, json.dumps(bindings, sort_keys=True)),
            ),
        ),
    )

    host._activation_preflight(candidate, transaction_id="1" * 32)

    assert len(calls) == 14
    assert all(code == host._ADAPTER_BINDING_READBACK for code, _args in calls[:6])
    assert calls[6] == (
        "platform-health",
        (SHA, TREE, json.dumps(ADAPTER_CANDIDATES, sort_keys=True)),
    )
    assert calls[7][0] == host._BROKER_PREFLIGHT
    assert calls[7][1] == (str(host.SUPERVISOR_CONFIG_PATH), SHA, "1" * 32)
    adapter_calls = calls[8:]
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
    candidate = host.Candidate(SHA, TREE, Path(f"/opt/candidates/{SHA}/repo"))
    evidence, current = _platform_evidence()
    monkeypatch.setattr(
        host,
        "_platform_health_json",
        lambda _path, *, label: current if "pointer" in label else evidence,
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
        host._activation_preflight(candidate, transaction_id="1" * 32)

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
        ("adapter-preflight", host._ADAPTER_PREFLIGHT, 3),
        ("adapter-binding-readback", host._ADAPTER_BINDING_READBACK, 1),
        ("generation-readback", host._GENERATION_READBACK, 1),
        ("activated-adapter-readback", host._ACTIVATED_ADAPTER_READBACK, 3),
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


def _zero_broker_preflight() -> tuple[dict[str, object], dict[str, object]]:
    selected: dict[str, object] = {}
    requests: list[dict[str, object]] = []
    for index, instance in enumerate(host.INSTANCES):
        request_id = f"request-{index}"
        sandbox, pool = instance.rsplit("-", 1)
        selected[instance] = {
            "request_id": request_id,
            "candidate_sha": SHA,
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
        host._validate_zero_broker_handoffs(report, selected, SHA)


def _retirement_report(
    *,
    state: str,
    candidate_sha: str = SHA,
) -> dict[str, object]:
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
                    "candidate_sha": candidate_sha,
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

    assert host._retirement_request_ids(report, SHA) == tuple(
        f"request-{index}" for index in range(len(host.INSTANCES))
    )
    assert "foreign-terminal" not in host._retirement_request_ids(report, SHA)


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
        host._retirement_request_ids(report, SHA)


def test_retirement_requires_six_terminal_zero_lanes() -> None:
    report = _retirement_report(state="terminal")

    assert host._retirement_is_drained(report, SHA) is True
    requests = report["requests"]
    assert isinstance(requests, list)
    requests.pop()
    with pytest.raises(host.RuntimeHostError, match="lane is missing"):
        host._retirement_is_drained(report, SHA)


def test_activation_rejects_zero_but_nonterminal_request() -> None:
    report, selected = _zero_broker_preflight()
    requests = report["requests"]
    assert isinstance(requests, list)
    request = requests[0]["request"]
    assert isinstance(request, dict)
    request["state"] = "pending"

    with pytest.raises(host.RuntimeHostError, match="terminal zero-capacity"):
        host._validate_zero_broker_handoffs(report, selected, SHA)


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
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda value, code, *args: calls.append((code, args)),
    )

    host._activated_adapter_readback(candidate)

    assert calls[0][0] == host._GENERATION_READBACK
    assert len(calls) == 7
    assert all(code == host._ACTIVATED_ADAPTER_READBACK for code, _args in calls[1:])


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
    monkeypatch.setattr(host, "_activation_gate_preflight", lambda value: ADAPTER_CANDIDATES)
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


def test_activation_rejects_authority_gate_before_transaction_mutation(
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
            "activation_status": "installed",
        },
    )
    monkeypatch.setattr(host, "check", lambda *args, **kwargs: {"status": "pass"})
    monkeypatch.setattr(
        host,
        "_activation_gate_preflight",
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
        host.activate(SHA)

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
            assert recovery._retirement_is_drained(report, SHA)
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
