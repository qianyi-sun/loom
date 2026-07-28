from __future__ import annotations

import json
import os
import stat
import subprocess
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_slurm_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/oldlab.toml"
GB10_PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/gb10.toml"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "root"
    (root / "etc/slurm").mkdir(parents=True)
    (root / "etc/docker").mkdir(parents=True)
    (root / "etc/slurm/slurm.conf").write_text(
        "\n".join(
            (
                "ClusterName=trt-oldlab",
                "ProctrackType=proctrack/linuxproc",
                "TaskPlugin=task/none",
                "PriorityType=priority/basic",
                "PriorityWeightFairshare=0",
                "",
            ),
        ),
        encoding="utf-8",
    )
    (root / "etc/slurm/cgroup.conf").write_text(
        "CgroupPlugin=autodetect\nConstrainCores=yes\nConstrainRAMSpace=no\n",
        encoding="utf-8",
    )
    (root / "etc/docker/daemon.json").write_text(
        json.dumps(
            {
                "features": {"containerd-snapshotter": False},
                "exec-opts": ["native.cgroupdriver=systemd"],
            },
        ),
        encoding="utf-8",
    )
    return root


def test_profile_is_exact_three_sandbox_fairshare_contract() -> None:
    loaded = policy.load_profile(PROFILE)

    assert loaded.cluster == "trt-oldlab"
    assert loaded.child_accounts == (
        "loom-dev-qianyi",
        "loom-dev-hongjian",
        "loom-dev-devansh",
    )
    assert loaded.users == ("qianyi", "hongjian", "devansh")
    assert loaded.docker_cgroup_driver == "cgroupfs"
    assert loaded.slurm["accounting_storage_enforce"] == ("associations,limits,qos,safe")


def test_gb10_profile_maps_connection_aliases_to_canonical_hosts() -> None:
    loaded = policy.load_profile(GB10_PROFILE)

    assert policy._slurm_node_for_host(loaded, "gx10-01c7") == "trt-gb10-1"
    assert policy._slurm_node_for_host(loaded, "trt-gb10-1") is None
    assert "trt-gb10-7" not in loaded.host_aliases


def test_render_preserves_unrelated_settings_and_removes_duplicate_keys(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    slurm = root / "etc/slurm/slurm.conf"
    slurm.write_text(
        slurm.read_text() + "TaskPlugin=task/affinity\n" + "SlurmctldHost=TRT-EAI-OLDLAB-1\n",
        encoding="utf-8",
    )

    rendered = policy.desired_files(root, loaded)[slurm]

    assert rendered.count("TaskPlugin=") == 1
    assert "TaskPlugin=task/cgroup,task/affinity" in rendered
    assert "SlurmctldHost=TRT-EAI-OLDLAB-1" in rendered
    assert "PriorityWeightFairshare=10000" in rendered


def test_daemon_merge_preserves_existing_keys_and_replaces_driver(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    rendered = policy.desired_files(root, loaded)[root / "etc/docker/daemon.json"]
    payload = json.loads(rendered)

    assert payload["features"] == {"containerd-snapshotter": False}
    assert payload["exec-opts"] == ["native.cgroupdriver=cgroupfs"]


def test_apply_to_offline_root_is_idempotent_and_snapshots(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    first = policy.apply(root, loaded, restart=False, apply_accounting=False)
    second = policy.apply(root, loaded, restart=False, apply_accounting=False)

    assert first["mutation_authorized"] is True
    assert second["mutation_authorized"] is True
    assert all(row["converged"] for row in second["files"])
    assert Path(first["snapshot"]).is_dir()
    assert Path(second["snapshot"]).is_dir()
    assert (root / "etc/slurm/cgroup.conf").read_text() == policy.render_cgroup_conf(loaded)


def test_rollback_restores_preinstall_absence_without_mixed_state(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original_slurm = (root / "etc/slurm/slurm.conf").read_text()
    candidate = "9" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )

    result = policy.rollback(root, loaded, candidate_sha=candidate)

    assert result["phase"] == "committed"
    assert (root / "etc/slurm/slurm.conf").read_text() == original_slurm
    assert not (root / "etc/loom/slurm-job-cgroup-guard.json").exists()
    assert not (root / "usr/libexec/loom-slurm-job-cgroup-guard").exists()


def test_file_plan_rejects_guard_permission_drift(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    candidate = "8" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )
    config = root / "etc/loom/slurm-job-cgroup-guard.json"
    config.chmod(0o644)

    result = policy.plan(root, loaded, candidate_sha=candidate)
    row = next(item for item in result["files"] if item["path"] == str(config))

    assert row["live_sha256"] == row["desired_sha256"]
    assert row["live_mode"] == 0o644
    assert row["desired_mode"] == 0o600
    assert row["converged"] is False
    assert result["file_plan"]["converged"] is False


def test_invalid_or_weakened_profile_fails_closed(tmp_path: Path) -> None:
    raw = PROFILE.read_text(encoding="utf-8").replace(
        "constrain_devices = true",
        "constrain_devices = false",
    )
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(raw, encoding="utf-8")

    with pytest.raises(policy.PolicyError, match="constrain_devices"):
        policy.load_profile(invalid)


def test_accounting_plan_has_parent_budget_and_one_child_per_user() -> None:
    loaded = policy.load_profile(PROFILE)

    commands = policy.accounting_commands(loaded)
    flattened = [" ".join(command) for command in commands]

    assert any(
        "account=loom-dev set Fairshare=1 GrpTRES=cpu=40,mem=160G" in row for row in flattened
    )
    for user in loaded.users:
        assert any(f"add user {user}" in row for row in flattened)
    for account in loaded.child_accounts:
        assert any(f"add account {account} Parent=loom-dev" in row for row in flattened)


@pytest.mark.parametrize(
    "crash_phase",
    ("files_written", "accounting_applied", "services_reconfigured", "verified"),
)
def test_next_apply_recovers_every_orphan_transaction_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_phase: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original_slurm = (root / "etc/slurm/slurm.conf").read_text()
    original_advance = policy._advance_journal

    def crash_after_durable_phase(
        path: Path,
        payload: dict[str, object],
        phase: str,
    ) -> None:
        original_advance(path, payload, phase)
        if phase == crash_phase:
            raise SystemExit("simulated process death")

    monkeypatch.setattr(policy, "_advance_journal", crash_after_durable_phase)
    with pytest.raises(SystemExit, match="simulated process death"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )

    journal = json.loads(policy._journal_path(root, loaded).read_text())
    assert journal["phase"] == crash_phase
    restored: list[Path] = []
    original_restore = policy._restore_snapshot

    def observe_restore(observed_root: Path, snapshot: Path) -> None:
        restored.append(snapshot)
        original_restore(observed_root, snapshot)

    monkeypatch.setattr(policy, "_advance_journal", original_advance)
    monkeypatch.setattr(policy, "_restore_snapshot", observe_restore)
    result = policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )

    assert restored
    assert result["phase"] == "committed"
    assert original_slurm != (root / "etc/slurm/slurm.conf").read_text()
    assert stat.S_IMODE(policy._journal_path(root, loaded).stat().st_mode) == 0o600


@pytest.mark.parametrize("field", ("snapshot", "accounting_snapshot"))
def test_apply_rejects_noncanonical_paths_from_committed_journal(
    tmp_path: Path,
    field: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal_path = policy._journal_path(root, loaded)
    journal = json.loads(journal_path.read_text())
    journal[field] = str(tmp_path / "outside-snapshot")
    policy._write_journal(journal_path, journal)

    with pytest.raises(policy.PolicyError, match=r"canonical|not canonical"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_apply_rejects_symlink_or_hardlinked_journal(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal = policy._journal_path(root, loaded)
    hardlink = tmp_path / "journal-hardlink"
    os.link(journal, hardlink)
    with pytest.raises(policy.PolicyError, match="journal is unsafe"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )
    hardlink.unlink()

    external = tmp_path / "external-journal"
    external.write_bytes(journal.read_bytes())
    external.chmod(0o600)
    journal.unlink()
    journal.symlink_to(external)
    with pytest.raises(policy.PolicyError, match="journal is unreadable"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_apply_rejects_canonical_snapshot_path_that_is_a_symlink(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal_path = policy._journal_path(root, loaded)
    journal = json.loads(journal_path.read_text())
    external = tmp_path / "external-snapshot"
    external.mkdir(mode=0o700)
    alias = policy._state_root(root) / "snapshots" / "20000101T000000.000000Z"
    alias.symlink_to(external, target_is_directory=True)
    journal["snapshot"] = str(alias)
    policy._write_journal(journal_path, journal)

    with pytest.raises(policy.PolicyError, match="ownership is unsafe"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_accounting_failure_uses_targeted_cas_without_cluster_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original = (root / "etc/slurm/slurm.conf").read_text()
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *args, **kwargs: ("trt-eai-oldlab-1", loaded.controller),
    )
    calls: list[tuple[str, ...]] = []

    def fake_run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        calls.append(command)
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)

    def fail_apply(
        _profile: policy.Profile,
        _snapshot: dict[str, object],
    ) -> None:
        raise policy.PolicyError("accounting mutation failed")

    monkeypatch.setattr(policy, "_apply_accounting", fail_apply)
    with pytest.raises(policy.PolicyError, match="accounting mutation failed"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=True,
            candidate_sha="b" * 40,
        )

    assert (root / "etc/slurm/slurm.conf").read_text() == original
    assert not any("dump" in command or "load" in command for command in calls)
    journal = json.loads(policy._journal_path(root, loaded).read_text())
    assert journal["phase"] == "rolled_back"


def test_accounting_cas_ignores_unrelated_accounts_but_rejects_owned_field_drift() -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    before = json.loads(json.dumps(desired))
    before["accounts"][loaded.parent_account]["Fairshare"] = "2"
    current = json.loads(json.dumps(desired))
    current["accounts"]["unrelated-research"] = {
        "ParentName": "",
        "Fairshare": "99",
        "GrpTRES": "cpu=999",
    }

    policy._validate_accounting_cas(current, before, desired)

    current["accounts"][loaded.parent_account]["Fairshare"] = "777"
    with pytest.raises(policy.PolicyError, match="changed concurrently"):
        policy._validate_accounting_cas(current, before, desired)


def test_accounting_cas_refuses_to_delete_new_identity_with_external_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": {"qos": {}, "accounts": {}, "associations": {}},
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: desired)
    monkeypatch.setattr(
        policy,
        "_accounting_external_references",
        lambda _profile: {loaded.parent_account},
    )

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("accounting mutation ran before the external-reference gate")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    with pytest.raises(policy.PolicyError, match="external references"):
        policy._restore_accounting(loaded, snapshot)


def test_accounting_restore_rejects_owned_drift_before_first_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    before = json.loads(json.dumps(desired))
    before["accounts"][loaded.parent_account]["Fairshare"] = "2"
    current = json.loads(json.dumps(desired))
    current["accounts"][loaded.parent_account]["Fairshare"] = "777"
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": before,
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: current)

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("accounting mutation ran before the CAS drift gate")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    with pytest.raises(policy.PolicyError, match="changed concurrently"):
        policy._restore_accounting(loaded, snapshot)


def test_accounting_restore_deletes_only_exact_new_loom_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    empty = {"qos": {}, "accounts": {}, "associations": {}}
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": empty,
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: desired)
    monkeypatch.setattr(policy, "_accounting_external_references", lambda _profile: set())
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(
        policy,
        "_require_accounting_state",
        lambda _profile, _expected, *, phase: None,
    )
    monkeypatch.setattr(
        policy,
        "_checked_accounting_transition",
        lambda _profile, command, _expected, next_expected: (
            commands.append(tuple(command)) or next_expected
        ),
    )
    policy._restore_accounting(loaded, snapshot)

    delete_commands = [command for command in commands if "delete" in command]
    for user, account in zip(loaded.users, loaded.child_accounts, strict=True):
        assert (
            "sacctmgr",
            "-i",
            "delete",
            "user",
            "where",
            f"name={user}",
            f"account={account}",
            f"cluster={loaded.cluster}",
        ) in delete_commands
    for account in (*loaded.child_accounts, loaded.parent_account):
        assert (
            "sacctmgr",
            "-i",
            "delete",
            "account",
            "where",
            f"account={account}",
            f"cluster={loaded.cluster}",
        ) in delete_commands
    assert (
        "sacctmgr",
        "-i",
        "delete",
        "qos",
        "where",
        f"name={loaded.qos}",
    ) in delete_commands
    assert len(delete_commands) == len(loaded.users) + len(loaded.child_accounts) + 2


def test_accounting_domain_lock_blocks_second_tool_transaction_between_gate_and_mutation(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    first_preflight = threading.Event()
    release_first = threading.Event()
    first_mutation = threading.Event()
    second_preflight = threading.Event()
    errors: list[BaseException] = []

    def first_transaction() -> None:
        try:
            with policy._domain_lock(root, loaded):
                first_preflight.set()
                if not release_first.wait(timeout=2):
                    raise RuntimeError("first accounting transaction was not released")
                first_mutation.set()
        except BaseException as exc:
            errors.append(exc)

    def second_transaction() -> None:
        try:
            if not first_preflight.wait(timeout=2):
                raise RuntimeError("first accounting transaction did not acquire the lock")
            with policy._domain_lock(root, loaded):
                second_preflight.set()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=first_transaction)
    second = threading.Thread(target=second_transaction)
    first.start()
    assert first_preflight.wait(timeout=2)
    second.start()
    assert not second_preflight.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert errors == []
    assert first_mutation.is_set()
    assert second_preflight.is_set()
    lock = policy._state_root(root) / "locks" / f"{loaded.cluster}.lock"
    metadata = lock.lstat()
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert metadata.st_nlink == 1


def test_accounting_domain_lock_rejects_symlink_and_hardlink(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    lock = policy._state_root(root) / "locks" / f"{loaded.cluster}.lock"
    lock.parent.mkdir(parents=True, mode=0o700)
    lock.parent.chmod(0o700)
    external = tmp_path / "external-lock"
    external.write_text("")
    external.chmod(0o600)
    lock.symlink_to(external)
    with pytest.raises(policy.PolicyError, match="lock could not be opened safely"):
        with policy._domain_lock(root, loaded):
            pytest.fail("symlink lock was acquired")

    lock.unlink()
    lock.write_text("")
    lock.chmod(0o600)
    hardlink = tmp_path / "lock-hardlink"
    os.link(lock, hardlink)
    with pytest.raises(policy.PolicyError, match="lock inode is unsafe"):
        with policy._domain_lock(root, loaded):
            pytest.fail("hardlinked lock was acquired")


def test_accounting_transition_detects_bypass_drift_after_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    expected = policy._accounting_desired_state(loaded)
    next_expected = json.loads(json.dumps(expected))
    next_expected["accounts"][loaded.parent_account]["Fairshare"] = "2"
    bypassed = json.loads(json.dumps(next_expected))
    bypassed["accounts"][loaded.parent_account]["Fairshare"] = "777"
    states = iter((expected, bypassed))
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: next(states))
    monkeypatch.setattr(policy, "_run", lambda _command: "")

    with pytest.raises(policy.PolicyError, match="after mutation"):
        policy._checked_accounting_transition(
            loaded,
            ("sacctmgr", "-i", "modify", "account"),
            expected,
            next_expected,
        )


def test_accounting_restore_rechecks_external_refs_immediately_before_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    current = {
        "qos": {},
        "accounts": {
            loaded.parent_account: desired["accounts"][loaded.parent_account],
        },
        "associations": {},
    }
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": {"qos": {}, "accounts": {}, "associations": {}},
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: current)
    references = iter((set(), {loaded.parent_account}))
    monkeypatch.setattr(
        policy,
        "_accounting_external_references",
        lambda _profile: next(references),
    )
    monkeypatch.setattr(
        policy,
        "_run",
        lambda _command: pytest.fail("delete ran after an external reference appeared"),
    )

    with pytest.raises(policy.PolicyError, match="gained an external reference"):
        policy._restore_accounting(loaded, snapshot)


def test_service_reload_failure_restores_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original = (root / "etc/docker/daemon.json").read_text()
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *args, **kwargs: ("trt-eai-oldlab-1", loaded.controller),
    )
    calls = 0

    def restart(_profile: policy.Profile, _node: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise policy.PolicyError("daemon reload failed")

    monkeypatch.setattr(policy, "_restart_services", restart)
    monkeypatch.setattr(
        policy,
        "_restore_services",
        lambda _root, _profile, _node: restart(_profile, _node),
    )
    with pytest.raises(policy.PolicyError, match="daemon reload failed"):
        policy.apply(
            root,
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="c" * 40,
        )

    assert calls == 2
    assert (root / "etc/docker/daemon.json").read_text() == original


def _live_outputs(profile: policy.Profile) -> dict[str, str]:
    slurm = "\n".join(
        f"{key} = {profile.slurm[field]}" for key, field in policy._SLURM_KEYS.items()
    )
    accounts = "\n".join(
        (
            "loom-dev||1|cpu=40,mem=160G",
            "loom-dev-qianyi|loom-dev|1|",
            "loom-dev-hongjian|loom-dev|1|",
            "loom-dev-devansh|loom-dev|1|",
        ),
    )
    associations = "\n".join(
        f"{user}|{account}|1|loom-dev|loom-dev"
        for user, account in zip(profile.users, profile.child_accounts, strict=True)
    )
    return {
        "scontrol": slurm,
        "docker": "cgroupfs\n",
        "enabled": "enabled\n",
        "active": "active\n",
        "qos": "loom-dev|0|02:00:00|10|20\n",
        "accounts": accounts,
        "associations": associations,
    }


def test_live_readback_rejects_effective_slurm_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    candidate = "d" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )
    desired = policy.desired_files(root, loaded, candidate_sha=candidate)
    guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
    status = {
        "schema_version": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate,
        "config_sha256": policy._sha256(desired[guard_config].encode()),
        "scanned": 1,
        "verified": 1,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probe": {
            "job_id": "123",
            "candidate_sha": candidate,
            "observed_at": datetime.now(UTC).isoformat(),
            "cpu_max": "200000 100000",
            "memory_max": "8388608000",
            "pids_max": "32768",
            "gpu_tres": "not-required",
            "gpu_verified": True,
        },
    }
    status_path = root / policy._GUARD_STATUS_RELATIVE
    policy._atomic_write(status_path, json.dumps(status) + "\n", mode=0o600)
    outputs = _live_outputs(loaded)

    def fake_run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        if command[:3] == ("scontrol", "show", "config"):
            return outputs["scontrol"]
        if command[:3] == ("docker", "info", "--format"):
            return outputs["docker"]
        if command[:2] == ("systemctl", "is-enabled"):
            return outputs["enabled"]
        if command[:2] == ("systemctl", "is-active"):
            return outputs["active"]
        joined = " ".join(command)
        if "show qos" in joined:
            return outputs["qos"]
        if "show account " in joined:
            return outputs["accounts"]
        if "show association " in joined:
            return outputs["associations"]
        raise AssertionError(command)

    monkeypatch.setattr(policy, "_run", fake_run)
    assert policy.live_readback(
        root,
        loaded,
        candidate_sha=candidate,
        require_probe=True,
    )["converged"]

    outputs["scontrol"] = outputs["scontrol"].replace(
        "TaskPlugin = task/cgroup,task/affinity",
        "TaskPlugin = task/none",
    )
    with pytest.raises(policy.PolicyError, match="TaskPlugin"):
        policy.live_readback(
            root,
            loaded,
            candidate_sha=candidate,
            require_probe=True,
        )


def test_guard_all_failed_status_is_not_accepted_as_live_health(tmp_path: Path) -> None:
    path = tmp_path / policy._GUARD_STATUS_RELATIVE
    payload = {
        "schema_version": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_sha": "e" * 40,
        "config_sha256": "f" * 64,
        "scanned": 3,
        "verified": 0,
        "unrelated": 0,
        "failed": 3,
        "failures": [{"job_id": "1", "reason": "readback failed"}],
        "resource_probe": None,
    }
    policy._atomic_write(path, json.dumps(payload) + "\n", mode=0o600)

    with pytest.raises(policy.PolicyError, match="failed or drifted"):
        policy._guard_status_readback(
            tmp_path,
            candidate_sha="e" * 40,
            expected_config_sha256="f" * 64,
            require_probe=True,
        )


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repo), *args),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _strict_candidate_fixture(
    tmp_path: Path,
    *,
    attributes: str | None = None,
) -> tuple[Path, Path, str]:
    repository = tmp_path / "candidate"
    repository.mkdir(mode=0o755)
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Policy Test")
    _git(repository, "config", "user.email", "policy@example.com")
    (repository / "tracked.txt").write_text("candidate bytes\n")
    if attributes is not None:
        (repository / ".gitattributes").write_text(attributes)
    _git(repository, "add", ".")
    _git(repository, "commit", "-qm", "candidate")
    candidate = _git(repository, "rev-parse", "HEAD")
    env_dir = tmp_path / "private"
    env_dir.mkdir(mode=0o700)
    worker_env = env_dir / "worker.env"
    worker_env.write_text("LOOM_WORKER_TOKEN_FILE=/run/loom/token\n")
    worker_env.chmod(0o600)
    return repository, worker_env, candidate


def test_strict_candidate_binding_reads_raw_tree_and_private_env(tmp_path: Path) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path)

    binding = policy.strict_candidate_binding(
        repository,
        worker_env,
        candidate_sha=candidate,
    )

    assert binding["repository"]["candidate_sha"] == candidate
    assert binding["repository"]["candidate_tree"] == _git(
        repository,
        "rev-parse",
        "HEAD^{tree}",
    )
    assert binding["worker_env"]["keys"] == ["LOOM_WORKER_TOKEN_FILE"]
    assert binding["worker_env"]["inode"] == worker_env.stat().st_ino
    assert binding["worker_env"]["uid"] == worker_env.stat().st_uid
    assert binding["worker_env"]["gid"] == worker_env.stat().st_gid


@pytest.mark.parametrize("flag", ("--skip-worktree", "--assume-unchanged"))
def test_strict_candidate_rejects_hidden_index_flags(
    tmp_path: Path,
    flag: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path)
    _git(repository, "update-index", flag, "tracked.txt")

    with pytest.raises(policy.PolicyError, match="skip-worktree or assume-unchanged"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


@pytest.mark.parametrize("mutation", ("extra", "raw-drift"))
def test_strict_candidate_rejects_extra_files_and_raw_byte_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path)
    if mutation == "extra":
        (repository / "ignored-or-untracked.txt").write_text("extra\n")
        expected = "extra or missing"
    else:
        (repository / "tracked.txt").write_text("different raw bytes\n")
        expected = "raw tracked bytes"

    with pytest.raises(policy.PolicyError, match=expected):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_strict_candidate_rejects_clean_filter_interference(tmp_path: Path) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(
        tmp_path,
        attributes="*.txt filter=unsafe-clean\n",
    )

    with pytest.raises(policy.PolicyError, match="interfering Git filter"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_strict_candidate_rejects_symlink_parent_and_nonprivate_env(tmp_path: Path) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path)
    alias = tmp_path / "candidate-alias"
    alias.symlink_to(repository, target_is_directory=True)

    with pytest.raises(policy.PolicyError, match="symlinks"):
        policy.strict_candidate_binding(
            alias,
            worker_env,
            candidate_sha=candidate,
        )

    worker_env.chmod(0o640)
    with pytest.raises(policy.PolicyError, match="0600"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )

    worker_env.chmod(0o600)
    with pytest.raises(policy.PolicyError, match="batch UID/GID"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
            expected_batch_uid=worker_env.stat().st_uid + 1,
            expected_batch_gid=worker_env.stat().st_gid,
        )


@pytest.mark.parametrize(
    "contents",
    (
        "LOOM_TOKEN=value\nLOOM_TOKEN=other\n",
        "lowercase=value\n",
        "LOOM_TOKEN\n",
    ),
)
def test_strict_candidate_rejects_duplicate_or_invalid_env_keys(
    tmp_path: Path,
    contents: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path)
    worker_env.write_text(contents)
    worker_env.chmod(0o600)

    with pytest.raises(policy.PolicyError, match=r"duplicate|invalid"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_slurm_profiles_keep_independent_controller_routes() -> None:
    oldlab = policy.load_profile(PROFILE)
    gb10 = policy.load_profile(GB10_PROFILE)

    assert (oldlab.cluster, oldlab.controller, oldlab.submit_host) == (
        "trt-oldlab",
        "TRT-EAI-OLDLAB-1",
        "trt-EAI-OLDLAB-2",
    )
    assert (gb10.cluster, gb10.controller, gb10.submit_host) == (
        "trt-gb10",
        "trt-gb10-1",
        "trt-gb10-1",
    )
    assert oldlab.cluster != gb10.cluster


def _allocation_inflight_payload(
    loaded: policy.Profile,
    candidate: str,
    *,
    job_id: str = "123",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate,
        "cluster": loaded.cluster,
        "controller": loaded.controller,
        "submit_host": loaded.submit_host,
        "job_id": job_id,
        "job_name": f"loom-policy-{candidate}-probe",
        "batch_uid": 501,
        "batch_gid": 20,
        "phase": "submitted",
    }


def test_allocation_probe_lock_serializes_candidate_transactions(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "6" * 40
    first_entered = threading.Event()
    release_first = threading.Event()
    second_attempting = threading.Event()
    second_submitted = threading.Event()
    errors: list[BaseException] = []

    def first_transaction() -> None:
        try:
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                candidate,
                enforce_root_ownership=False,
            ):
                first_entered.set()
                if not release_first.wait(timeout=2):
                    raise RuntimeError("first allocation transaction was not released")
        except BaseException as exc:
            errors.append(exc)

    def second_transaction() -> None:
        try:
            if not first_entered.wait(timeout=2):
                raise RuntimeError("first allocation transaction did not acquire the lock")
            second_attempting.set()
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                candidate,
                enforce_root_ownership=False,
            ):
                second_submitted.set()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=first_transaction)
    second = threading.Thread(target=second_transaction)
    first.start()
    assert first_entered.wait(timeout=2)
    second.start()
    assert second_attempting.wait(timeout=2)
    assert not second_submitted.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert second_submitted.is_set()
    lock = policy._allocation_lock_path(tmp_path, loaded, candidate)
    metadata = lock.lstat()
    assert stat.S_ISREG(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert (metadata.st_uid, metadata.st_gid) == (os.geteuid(), os.getegid())


def test_allocation_cleanup_archives_completed_job_without_scancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "3" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(
        policy,
        "_probe_accounting_rows",
        lambda job_id: [
            [job_id, payload["job_name"], "COMPLETED", "node", "cpu=1", "account"],
        ],
    )

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("terminal allocation cleanup must not call scancel")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    policy._cancel_allocation_job(
        path,
        payload,
        loaded,
        enforce_root_ownership=False,
    )

    assert not path.exists()
    history = path.parent / f"{candidate}.123.terminal.json"
    assert json.loads(history.read_text())["terminal_state"] == "COMPLETED"


@pytest.mark.parametrize(
    ("message", "expected"),
    (
        ("allocation probe did not reach a terminal state before timeout", "timeout"),
        ("sacct failed safely with exit code 1", "sacct failed"),
    ),
)
def test_allocation_probe_poll_failure_cancels_and_waits_for_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    message: str,
    expected: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "e" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    poll_calls = 0
    commands: list[tuple[str, ...]] = []

    def fake_poll(
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float = policy._ALLOCATION_POLL_SECONDS,
    ) -> list[list[str]]:
        nonlocal poll_calls
        del timeout_seconds, poll_seconds
        poll_calls += 1
        if poll_calls == 1:
            raise policy.PolicyError(message)
        return [[job_id, payload["job_name"], "CANCELLED", "", "", ""]]

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_poll_probe_terminal", fake_poll)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id: [])
    monkeypatch.setattr(policy, "_run", fake_run)
    with pytest.raises(policy.PolicyError, match=expected):
        policy._poll_allocation_or_cancel(
            path,
            payload,
            loaded,
            timeout_seconds=5,
            enforce_root_ownership=False,
        )

    assert poll_calls == 2
    assert commands == [("scancel", f"--clusters={loaded.cluster}", "123")]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_probe_recovers_crash_journal_before_new_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "f" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        command = tuple(argv)
        commands.append(command)
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id: [])
    monkeypatch.setattr(
        policy,
        "_poll_probe_terminal",
        lambda job_id, **_kwargs: [
            [job_id, payload["job_name"], "CANCELLED", "", "", ""],
        ],
    )
    with policy._allocation_probe_lock(
        tmp_path,
        loaded,
        candidate,
        enforce_root_ownership=False,
    ):
        policy._recover_allocation_probe(
            path,
            loaded,
            candidate_sha=candidate,
            job_name=str(payload["job_name"]),
            enforce_root_ownership=False,
        )

    assert commands == [
        ("scancel", f"--clusters={loaded.cluster}", "123"),
        ("squeue", "-h", "-n", payload["job_name"], "-o", "%A|%j|%T"),
    ]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_recovery_archives_terminal_job_without_scancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "4" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(
        policy,
        "_probe_accounting_rows",
        lambda job_id: [
            [job_id, payload["job_name"], "COMPLETED", "node", "cpu=1", "account"],
        ],
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    policy._recover_allocation_probe(
        path,
        loaded,
        candidate_sha=candidate,
        job_name=str(payload["job_name"]),
        enforce_root_ownership=False,
    )

    assert commands == [
        ("squeue", "-h", "-n", payload["job_name"], "-o", "%A|%j|%T"),
    ]
    assert not path.exists()
    history = path.parent / f"{candidate}.123.terminal.json"
    assert json.loads(history.read_text())["terminal_state"] == "COMPLETED"


def test_allocation_cleanup_scancels_when_initial_sacct_is_temporarily_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "5" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id: [])
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(
        policy,
        "_poll_probe_terminal",
        lambda job_id, **_kwargs: [
            [job_id, payload["job_name"], "CANCELLED", "", "", ""],
        ],
    )
    policy._cancel_allocation_job(
        path,
        payload,
        loaded,
        enforce_root_ownership=False,
    )

    assert commands == [("scancel", f"--clusters={loaded.cluster}", "123")]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_probe_scancel_failure_remains_durably_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "1" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)

    def fail_scancel(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del argv, timeout
        raise policy.PolicyError("scancel failed safely with exit code 1")

    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id: [])
    monkeypatch.setattr(policy, "_run", fail_scancel)
    with pytest.raises(policy.PolicyError, match="scancel failed"):
        policy._cancel_allocation_job(
            path,
            payload,
            loaded,
            enforce_root_ownership=False,
        )

    assert path.exists()
    assert json.loads(path.read_text())["phase"] == "cancel_failed"


def test_allocation_probe_cancel_readback_failure_remains_durably_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "2" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id: [])
    monkeypatch.setattr(policy, "_run", lambda _argv, **_kwargs: "")

    def fail_readback(_job_id: str, **_kwargs: float) -> list[list[str]]:
        raise policy.PolicyError("cancel readback timed out")

    monkeypatch.setattr(policy, "_poll_probe_terminal", fail_readback)

    with pytest.raises(policy.PolicyError, match="cancel readback timed out"):
        policy._cancel_allocation_job(
            path,
            payload,
            loaded,
            enforce_root_ownership=False,
        )

    assert path.exists()
    assert json.loads(path.read_text())["phase"] == "cancel_readback_failed"


def test_allocation_probe_readback_is_candidate_and_route_bound(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "a" * 40
    binding = {
        "repository": {
            "path": "/candidate",
            "candidate_sha": candidate,
            "candidate_tree": "b" * 40,
            "tracked_files": 10,
        },
        "worker_env": {
            "path": "/private/worker.env",
            "device": 1,
            "inode": 2,
            "uid": 501,
            "gid": 20,
            "sha256": "c" * 64,
            "keys": ["LOOM_WORKER_TOKEN_FILE"],
        },
    }
    payload = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate,
        "candidate_tree": "b" * 40,
        "cluster": loaded.cluster,
        "controller": loaded.controller,
        "submit_host": loaded.submit_host,
        "submitting_host": "trt-eai-oldlab-2",
        "job_id": "123",
        "job_name": f"loom-policy-{candidate}-probe",
        "node": "trt-eai-oldlab-3",
        "state": "COMPLETED",
        "account": loaded.child_accounts[0],
        "qos": loaded.qos,
        "alloc_tres": "cpu=1,mem=256M",
        "gpu_verified": True,
        "sbatch_verified": True,
        "srun_verified": True,
        "batch_uid": 501,
        "batch_gid": 20,
        "candidate_binding": binding,
    }
    target = policy._allocation_probe_path(tmp_path, loaded, candidate)
    policy._atomic_write(target, json.dumps(payload) + "\n", mode=0o600)

    observed = policy.allocation_probe_readback(
        tmp_path,
        loaded,
        candidate_sha=candidate,
        candidate_binding=binding,
    )
    assert observed["cluster"] == "trt-oldlab"
    payload["controller"] = "trt-gb10-1"
    policy._atomic_write(target, json.dumps(payload) + "\n", mode=0o600)
    with pytest.raises(policy.PolicyError, match="binding drifted"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            candidate_sha=candidate,
            candidate_binding=binding,
        )
