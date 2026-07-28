from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_host as host
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

SHA = "a" * 40


def _temporary_profile(tmp_path: Path, sandbox: str = "qianyi") -> host.Profile:
    checked_in = next(profile for profile in host.load_profiles() if profile.sandbox == sandbox)
    state = tmp_path / "state" / sandbox
    return replace(
        checked_in,
        candidate_root=tmp_path / "candidates" / sandbox,
        state_root=state,
        cache_root=state / "cache",
        evidence_root=state / "evidence",
        runtime_root=state / "runtime",
    )


def _current_identity(user: str) -> host.Identity:
    return host.Identity(
        user=user,
        group="test-group",
        uid=os.getuid(),
        gid=os.getgid(),
    )


def _git(candidate: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(candidate), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_plan_covers_all_fixed_paths_ports_and_nfs_readbacks_without_secrets() -> None:
    profiles = host.load_profiles()
    plan = host.plan_document(profiles, SHA, "install")
    encoded = json.dumps(plan, sort_keys=True)

    assert plan["mutation_authorized"] is False
    assert plan["remote_url"] == "https://github.com/qianyi-sun/loom.git"
    assert {row["sandbox"] for row in plan["sandboxes"]} == set(host.SANDBOXES)
    assert sum(len(row["ports"]) for row in plan["sandboxes"]) == 30
    assert (
        len(
            {port for row in plan["sandboxes"] for port in row["ports"].values()},
        )
        == 30
    )
    assert all(len(row["nfs_readback_commands"]) == 5 for row in plan["sandboxes"])
    assert all(row["candidate"].endswith(SHA) for row in plan["sandboxes"])
    assert "LOOM_DEV_POSTGRES_PASSWORD" not in encoded
    assert "loom_admin_" not in encoded


def test_secret_initialization_is_private_idempotent_and_unique(tmp_path: Path) -> None:
    qianyi = _temporary_profile(tmp_path, "qianyi")
    hongjian = _temporary_profile(tmp_path, "hongjian")
    qianyi_identity = _current_identity("qianyi")
    hongjian_identity = _current_identity("hongjian")

    host.ensure_secret_files(qianyi, qianyi_identity)
    first_env = qianyi.secrets_env.read_bytes()
    first_admin = qianyi.admin_secret.read_bytes()
    host.ensure_secret_files(qianyi, qianyi_identity)
    host.ensure_secret_files(hongjian, hongjian_identity)

    assert qianyi.secrets_env.read_bytes() == first_env
    assert qianyi.admin_secret.read_bytes() == first_admin
    for path in (qianyi.secrets_env, qianyi.admin_secret, hongjian.secrets_env):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert (path.stat().st_uid, path.stat().st_gid) == (os.getuid(), os.getgid())
    qianyi_values = host._parse_env_file(qianyi.secrets_env)
    hongjian_values = host._parse_env_file(hongjian.secrets_env)
    for key in (
        "LOOM_DEV_POSTGRES_PASSWORD",
        "LOOM_DEV_MINIO_ROOT_PASSWORD",
        "LOOM_CP_STEP_JWT_SIGNING_KEY",
        "LOOM_SECRET_STORE_MASTER_KEY",
        "LOOM_WORKER_TOKEN",
    ):
        assert qianyi_values[key] != hongjian_values[key]


def test_secret_initialization_rejects_permissive_existing_file(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_secret_files(profile, identity)
    profile.secrets_env.chmod(0o640)

    with pytest.raises(host.HostConvergeError, match="mode 0600"):
        host.ensure_secret_files(profile, identity)


def test_private_root_convergence_rejects_symlink(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    target = tmp_path / "unrelated"
    target.mkdir()
    profile.state_root.parent.mkdir(parents=True)
    profile.state_root.symlink_to(target, target_is_directory=True)

    with pytest.raises(host.HostConvergeError, match="root is unsafe"):
        host.ensure_private_roots(profile, _current_identity("qianyi"))

    assert stat.S_IMODE(target.stat().st_mode) != 0o700


def test_exact_candidate_verifier_requires_clean_immutable_tree(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    candidate = profile.candidate_root / SHA
    candidate.mkdir(parents=True)
    _git(candidate, "init")
    _git(candidate, "config", "user.email", "sandbox-test@example.invalid")
    _git(candidate, "config", "user.name", "Sandbox Test")
    (candidate / "README.md").write_text("exact candidate\n", encoding="utf-8")
    _git(candidate, "add", "README.md")
    _git(candidate, "commit", "-m", "candidate")
    actual_sha = _git(candidate, "rev-parse", "HEAD")
    immutable = profile.candidate_root / actual_sha
    candidate.rename(immutable)
    for root, directories, files in os.walk(immutable):
        for entry in [Path(root), *(Path(root) / name for name in directories + files)]:
            if not entry.is_symlink():
                entry.chmod(stat.S_IMODE(entry.stat().st_mode) & ~0o222)

    tree = host.verify_candidate(
        profile,
        immutable,
        actual_sha,
        _current_identity("publisher"),
    )

    assert len(tree) == 40
    readme = immutable / "README.md"
    readme.chmod(0o640)
    with pytest.raises(host.HostConvergeError, match="writable entry"):
        host.verify_candidate(
            profile,
            immutable,
            actual_sha,
            _current_identity("publisher"),
        )


def test_desired_state_records_only_one_safe_rollback_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile = _temporary_profile(tmp_path)
    first = "1" * 40
    second = "2" * 40

    assert host.write_desired(profile, first, "3" * 40) is None
    previous = host.write_desired(profile, second, "4" * 40)
    desired = json.loads(profile.desired_file.read_text(encoding="utf-8"))

    assert previous is not None
    assert desired["candidate_sha"] == second
    assert desired["previous_sha"] == first
    assert stat.S_IMODE(profile.desired_file.stat().st_mode) == 0o600


def test_desired_state_binding_rejects_tree_or_secret_path_drift(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    payload = host._desired_payload(
        profile,
        SHA,
        "b" * 40,
        previous_sha="c" * 40,
    )
    host._validate_desired_binding(profile, payload, sha=SHA, tree="b" * 40)

    payload["candidate_tree"] = "d" * 40
    with pytest.raises(host.HostConvergeError, match="desired state binding"):
        host._validate_desired_binding(profile, payload, sha=SHA, tree="b" * 40)


def test_update_refuses_migration_tree_change_before_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile = _temporary_profile(tmp_path)
    current = "1" * 40
    target = "2" * 40
    host.write_desired(profile, current, "3" * 40)
    monkeypatch.setattr(host, "verify_candidate", lambda *args, **kwargs: "3" * 40)
    monkeypatch.setattr(
        host,
        "_migration_tree",
        lambda candidate, publisher: "4" * 40 if candidate.name == current else "5" * 40,
    )

    with pytest.raises(host.HostConvergeError, match="migration-tree change"):
        host.require_migration_compatible_update(
            profile,
            target,
            _current_identity("publisher"),
        )


def test_fresh_database_tokens_are_minted_into_private_file_without_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_secret_files(profile, identity)
    monkeypatch.setattr(host, "_wait_for_control_plane", lambda selected: None)
    monkeypatch.setattr(host, "_read_admin_token", lambda path: "private-admin")

    def request(
        url: str,
        *,
        token: str | None,
        expected: set[int],
    ) -> tuple[int, dict[str, str]]:
        if url.endswith("/workers/register"):
            return 401, {}
        if url.endswith("/worker-tokens"):
            assert token == "private-admin"
            return 201, {"token": "loom_w_private-worker"}
        assert url.endswith("/batch-runner-tokens")
        assert token == "private-admin"
        return 201, {"token": "loom_br_private-batch"}

    monkeypatch.setattr(host, "_request_json", request)

    assert host.bootstrap_runtime_tokens(profile, identity) is True
    values = host._parse_env_file(profile.secrets_env)
    captured = capsys.readouterr()
    assert values["LOOM_WORKER_TOKEN"] == "loom_w_private-worker"
    assert values["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] == "loom_br_private-batch"
    assert stat.S_IMODE(profile.secrets_env.stat().st_mode) == 0o600
    assert "private-worker" not in captured.out + captured.err
    assert "private-batch" not in captured.out + captured.err


def test_candidate_child_environment_drops_ambient_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setenv("DO_NOT_FORWARD_PRIVATE_VALUE", "private")

    environment = host._candidate_environment(profile, profile.candidate_root / SHA)

    assert "DO_NOT_FORWARD_PRIVATE_VALUE" not in environment
    assert environment["GIT_CONFIG_NOSYSTEM"] == "1"
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_systemd_entry_is_fixed_and_replayable() -> None:
    unit = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/loom-developer-sandbox@.service"
    ).read_text(encoding="utf-8")

    assert "ConditionPathExists=/etc/loom/developer-sandboxes/desired/%i.json" in unit
    assert (
        "ExecStart=/usr/local/libexec/loom-developer-sandbox-host service-converge --sandbox %i"
    ) in unit
    assert "EnvironmentFile=" not in unit
    assert "RemainAfterExit=" not in unit
    assert "WantedBy=multi-user.target" in unit


def test_cli_plan_is_non_mutating_and_secret_safe(capsys: pytest.CaptureFixture[str]) -> None:
    rc = host.main(["plan", "--candidate-sha", SHA])
    captured = capsys.readouterr()

    assert rc == 0
    payload = json.loads(captured.out)
    assert payload["mutation_authorized"] is False
    assert not captured.err
    assert "LOOM_DEV_POSTGRES_PASSWORD" not in captured.out


def test_cli_executed_check_remains_read_only_and_reports_verified(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checked: list[str] = []
    monkeypatch.setattr(host, "service_check", checked.append)

    rc = host.main(
        [
            "check",
            "--candidate-sha",
            SHA,
            "--sandbox",
            "qianyi",
            "--execute",
        ],
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert checked == ["qianyi"]
    assert payload["mutation_authorized"] is False
    assert payload["verified"] is True
    assert payload["status"] == "succeeded"


def test_failed_child_output_is_not_relayed_in_error() -> None:
    private_value = "private-child-output"

    with pytest.raises(host.HostConvergeError) as caught:
        host._run(
            (
                sys.executable,
                "-c",
                (
                    "import sys; "
                    f"print({private_value!r}); "
                    f"sys.stderr.write({private_value!r}); "
                    "raise SystemExit(7)"
                ),
            ),
        )

    assert private_value not in str(caught.value)
    assert "exit code 7" in str(caught.value)


def test_host_installer_and_profiles_require_the_full_ci_matrix() -> None:
    for changed_path in (
        "scripts/ops/developer_sandbox_host.py",
        "deploy/developer-sandboxes/qianyi.toml",
        "deploy/developer-sandboxes/loom-developer-sandbox@.service",
    ):
        plan = plan_validations(
            changed_paths=[changed_path],
            labels=set(),
            event_name="pull_request",
        )
        assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
        assert plan.unowned_runtime is False
        assert all("protected-developer-sandbox" in plan.reasons[name] for name in HEAVY_CHECKS)
