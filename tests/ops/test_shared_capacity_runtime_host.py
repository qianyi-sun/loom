from __future__ import annotations

import json
import subprocess
from contextlib import nullcontext
from pathlib import Path

import pytest
from scripts.ops import shared_capacity_runtime_host as host

SHA = "a" * 40
TREE = "b" * 40


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
        lambda value: events.append("journal") or (journal_path, journal),
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
        "candidate_sha": SHA,
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
    monkeypatch.setattr(
        host,
        "_run_candidate_python",
        lambda value, code, *args: calls.append((code, args)),
    )

    host._activation_preflight(candidate)

    assert len(calls) == 7
    assert calls[0][0] == host._BROKER_PREFLIGHT
    adapter_calls = calls[1:]
    assert all(code == host._ADAPTER_PREFLIGHT for code, _args in adapter_calls)
    assert {Path(args[0]).stem for _code, args in adapter_calls} == set(host.INSTANCES)
    assert all(args[1:] == (SHA, TREE) for _code, args in adapter_calls)
    assert "_load_admin_token" in host._ADAPTER_PREFLIGHT
    assert "_validate_runtime_attestation" in host._ADAPTER_PREFLIGHT


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
    monkeypatch.setattr(host, "_activation_preflight", lambda value: None)
    monkeypatch.setattr(host, "_write_journal", lambda value: (journal_path, journal))
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
