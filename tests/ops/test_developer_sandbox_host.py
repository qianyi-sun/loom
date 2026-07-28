from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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


def _receipt(profile: host.Profile, sha: str) -> host.ActivationReceipt:
    return host.ActivationReceipt(
        path=host.combined_receipt_path(profile, sha),
        payload_sha256="d" * 64,
        fleet_payload_sha256="sha256:" + "e" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
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
    assert "remote_url" not in plan
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


def test_private_root_convergence_does_not_follow_raced_child_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    target = tmp_path / "unrelated"
    target.mkdir(mode=0o755)
    target_before = target.stat()
    mkdir_private_dir_at = host._mkdir_private_dir_at

    def replace_created_cache_with_symlink(parent_fd: int, name: str) -> None:
        mkdir_private_dir_at(parent_fd, name)
        if name == profile.cache_root.name:
            profile.cache_root.rmdir()
            profile.cache_root.symlink_to(target, target_is_directory=True)

    monkeypatch.setattr(
        host,
        "_mkdir_private_dir_at",
        replace_created_cache_with_symlink,
    )

    with pytest.raises(host.HostConvergeError, match="root is unsafe"):
        host.ensure_private_roots(profile, identity)

    target_after = target.stat()
    assert (target_after.st_dev, target_after.st_ino) == (
        target_before.st_dev,
        target_before.st_ino,
    )
    assert stat.S_IMODE(target_after.st_mode) == stat.S_IMODE(target_before.st_mode)
    state_metadata = profile.state_root.stat()
    assert stat.S_IMODE(state_metadata.st_mode) == 0o700
    assert (state_metadata.st_uid, state_metadata.st_gid) == (
        identity.uid,
        identity.gid,
    )


def test_atomic_write_does_not_follow_target_replaced_after_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_private_roots(profile, identity)
    parent_before = profile.secrets_root.stat()
    target = tmp_path / "unrelated"
    target.mkdir(mode=0o755)
    target_before = target.stat()
    replace_file_at = host._replace_file_at

    def replace_then_swap_target(
        parent_fd: int,
        source: str,
        destination: str,
    ) -> None:
        replace_file_at(parent_fd, source, destination)
        os.unlink(destination, dir_fd=parent_fd)
        os.symlink(target, destination, dir_fd=parent_fd)

    monkeypatch.setattr(host, "_replace_file_at", replace_then_swap_target)

    with pytest.raises(host.HostConvergeError, match="target binding changed"):
        host._atomic_write(
            profile.secrets_env,
            b"private\n",
            mode=0o600,
            identity=identity,
        )

    target_after = target.stat()
    assert (target_after.st_dev, target_after.st_ino) == (
        target_before.st_dev,
        target_before.st_ino,
    )
    assert stat.S_IMODE(target_after.st_mode) == stat.S_IMODE(target_before.st_mode)
    parent_after = profile.secrets_root.stat()
    assert stat.S_IMODE(parent_after.st_mode) == stat.S_IMODE(parent_before.st_mode)
    assert (parent_after.st_uid, parent_after.st_gid) == (
        parent_before.st_uid,
        parent_before.st_gid,
    )


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
                entry.chmod(0o2750 if entry.is_dir() else 0o640)

    tree = host.verify_candidate(
        profile,
        immutable,
        actual_sha,
        _current_identity("publisher"),
    )

    assert len(tree) == 40
    readme = immutable / "README.md"
    readme.chmod(0o660)
    with pytest.raises(host.HostConvergeError, match="group/world-writable"):
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

    assert host.write_desired(profile, first, "3" * 40, _receipt(profile, first)) is None
    previous = host.write_desired(profile, second, "4" * 40, _receipt(profile, second))
    desired = json.loads(profile.desired_file.read_text(encoding="utf-8"))

    assert previous is not None
    assert desired["candidate_sha"] == second
    assert desired["previous_sha"] == first
    assert stat.S_IMODE(profile.desired_file.stat().st_mode) == 0o600


def test_desired_state_binding_rejects_tree_or_secret_path_drift(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    receipt = _receipt(profile, SHA)
    payload = host._desired_payload(
        profile,
        SHA,
        "b" * 40,
        previous_sha="c" * 40,
        receipt=receipt,
    )
    host._validate_desired_binding(
        profile,
        payload,
        sha=SHA,
        tree="b" * 40,
        receipt=receipt,
    )

    payload["candidate_tree"] = "d" * 40
    with pytest.raises(host.HostConvergeError, match="desired state binding"):
        host._validate_desired_binding(
            profile,
            payload,
            sha=SHA,
            tree="b" * 40,
            receipt=receipt,
        )


def test_update_refuses_migration_tree_change_before_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile = _temporary_profile(tmp_path)
    current = "1" * 40
    target = "2" * 40
    host.write_desired(profile, current, "3" * 40, _receipt(profile, current))
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
    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
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


def test_failed_first_install_removes_desired_and_stops_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile.desired_file.parent.mkdir(parents=True)
    profile.desired_file.write_text("{}\n", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(host, "_sandbox_state_sha", lambda _profile: None)
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, previous, sha: events.append(f"relay:{previous}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_invalidate_receipt",
        lambda _profile, sha: events.append(f"invalidate:{sha}"),
    )
    monkeypatch.setattr(host, "_remove_transaction", lambda _profile: events.append("remove"))

    host._recover_transaction(
        profile,
        {
            "candidate_sha": SHA,
            "previous_desired": None,
            "previous_relay_sha": None,
        },
    )

    assert not profile.desired_file.exists()
    assert events == [
        f"prepare-stop:{SHA}",
        f"relay:None:{SHA}",
        f"invalidate:{SHA}",
        "remove",
    ]


def test_failed_upgrade_restores_previous_desired_and_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile.desired_file.parent.mkdir(parents=True)
    previous_sha = "b" * 40
    previous = {"schema_version": 1, "sandbox": profile.sandbox, "candidate_sha": previous_sha}
    events: list[str] = []
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(host, "_restore_relay", lambda *_args: events.append("relay"))
    monkeypatch.setattr(host, "_invalidate_receipt", lambda *_args: events.append("invalidate"))
    monkeypatch.setattr(host, "_remove_transaction", lambda _profile: events.append("remove"))

    host._recover_transaction(
        profile,
        {
            "candidate_sha": SHA,
            "previous_desired": previous,
            "previous_relay_sha": previous_sha,
        },
    )

    assert json.loads(profile.desired_file.read_text(encoding="utf-8")) == previous
    assert events == [f"update:{previous_sha}", "relay", "invalidate", "remove"]


def test_empty_namespace_first_install_materializes_before_prepare_and_attest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    source_tree = "b" * 40
    events: list[str] = []
    secrets = {
        "LOOM_DEV_POSTGRES_PASSWORD": "postgres",
        "LOOM_DEV_MINIO_ROOT_PASSWORD": "minio",
        "LOOM_CP_STEP_JWT_SIGNING_KEY": "jwt",
        "LOOM_SECRET_STORE_MASTER_KEY": "master",
        "LOOM_WORKER_TOKEN": "worker",
    }

    @contextmanager
    def lock(*_args: object) -> object:
        yield

    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "_bootstrap_domain_runtime_hosts",
        lambda *_args: events.append("bootstrap-empty-domain-roots"),
    )
    monkeypatch.setattr(host, "verify_developer_docker_access", lambda *_args: None)
    monkeypatch.setattr(host, "ensure_secret_files", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_materialize_domain_candidates",
        lambda *_args: events.append("materialize-no-fleet"),
    )
    monkeypatch.setattr(host, "verify_candidate_root", lambda *_args: None)

    def verify(*_args: object) -> str:
        assert events[:2] == ["bootstrap-empty-domain-roots", "materialize-no-fleet"]
        events.append("verify-19-node-candidate")
        return source_tree

    monkeypatch.setattr(host, "verify_candidate", verify)
    monkeypatch.setattr(
        host,
        "_install_assets",
        lambda *_args: events.append("install-exact-assets"),
    )
    monkeypatch.setattr(host, "verify_candidate_profile_bytes", lambda *_args: None)
    monkeypatch.setattr(host, "verify_candidate_consumer", lambda *_args: None)
    monkeypatch.setattr(host, "require_migration_compatible_update", lambda *_args: None)
    monkeypatch.setattr(host, "_converge_domain_runtime_hosts", lambda *_args: None)
    monkeypatch.setattr(host, "_parse_env_file", lambda *_args: secrets)
    monkeypatch.setattr(host, "_read_admin_token", lambda *_args: "admin")
    monkeypatch.setattr(host, "_activation_lock", lock)
    monkeypatch.setattr(host, "_transaction_payload", lambda *_args: None)
    monkeypatch.setattr(host, "_load_json", lambda *_args: None)
    monkeypatch.setattr(host, "_current_relay_sha", lambda *_args: None)
    monkeypatch.setattr(host, "_write_transaction", lambda *args, **kwargs: None)
    monkeypatch.setattr(host, "_assert_capacity_units_stopped", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, _sha, operation: events.append(f"lifecycle:{operation}"),
    )
    monkeypatch.setattr(host, "verify_listening_ports", lambda *_args: None)
    monkeypatch.setattr(host, "assert_capacity_quiescent", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_install_remote_link_fleet",
        lambda *_args: events.append("fleet-after-prepare"),
    )

    def attest(*_args: object) -> None:
        assert "fleet-after-prepare" in events
        events.append("attest-after-fleet")

    monkeypatch.setattr(host, "_publish_domain_attestations", attest)
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "verify_combined_receipt",
        lambda *_args: _receipt(profile, SHA),
    )
    monkeypatch.setattr(host, "write_desired", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_run",
        lambda *_args, **_kwargs: type("Completed", (), {"stdout": "", "returncode": 0})(),
    )
    monkeypatch.setattr(host, "service_check", lambda *_args: None)
    monkeypatch.setattr(host, "_remove_transaction", lambda *_args: None)

    host._install_materialized(
        (profile,),
        SHA,
        tmp_path / "candidate.bundle",
        source_tree,
    )

    assert events.index("bootstrap-empty-domain-roots") < events.index(
        "materialize-no-fleet",
    )
    assert events.index("materialize-no-fleet") < events.index("lifecycle:prepare")
    assert events.index("lifecycle:prepare") < events.index("fleet-after-prepare")
    assert events.index("fleet-after-prepare") < events.index("attest-after-fleet")


def test_materialization_archive_reads_helpers_from_commit_not_mutable_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    script = repo / "scripts/ops/developer_sandbox_domain_runtime.py"
    config = repo / "deploy/developer-sandboxes/runtime-domains.toml"
    script.parent.mkdir(parents=True)
    config.parent.mkdir(parents=True)
    script.write_text("committed-helper\n", encoding="utf-8")
    config.write_text("committed-config\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Host Test")
    _git(repo, "config", "user.email", "host@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "candidate")
    sha = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    script.write_text("tampered-after-stage\n", encoding="utf-8")
    config.write_text("tampered-config\n", encoding="utf-8")
    bundle = tmp_path / "candidate.bundle"
    bundle.write_bytes(b"bounded-bundle")
    monkeypatch.setattr(host, "REPO_ROOT", repo)

    archive = host._materialization_archive(bundle, sha, tree)
    with host.tarfile.open(fileobj=host.io.BytesIO(archive), mode="r") as tar:
        helper = tar.extractfile("developer_sandbox_domain_runtime.py")
        runtime_config = tar.extractfile("runtime-domains.toml")
        assert helper is not None and helper.read() == b"committed-helper\n"
        assert runtime_config is not None and runtime_config.read() == b"committed-config\n"


def test_remote_link_fleet_reads_each_client_from_issuance_clients_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    issuance_root = tmp_path / "issuance"
    sources: list[Path] = []
    monkeypatch.setattr(host, "REMOTE_LINK_ISSUANCE_ROOT", issuance_root)
    monkeypatch.setattr(
        host,
        "_parse_env_file",
        lambda *_args: {
            "LOOM_WORKER_TOKEN": "worker",
            "LOOM_DEV_MINIO_ROOT_USER": "access",
            "LOOM_DEV_MINIO_ROOT_PASSWORD": "secret",
        },
    )
    monkeypatch.setattr(host, "_run_candidate_program", lambda *_args: {})
    monkeypatch.setattr(host, "_verify_remote_candidate", lambda *_args: None)

    def archive(source: Path, **_kwargs: object) -> bytes:
        sources.append(source)
        return b"credentials"

    monkeypatch.setattr(host, "_archive_credentials", archive)
    monkeypatch.setattr(
        host,
        "_ssh",
        lambda *_args, **_kwargs: type("Completed", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(host, "_cleanup_remote_stage", lambda *_args: None)

    host._install_remote_link_fleet(
        profile,
        SHA,
        "b" * 40,
        _current_identity("root"),
    )

    assert sources == [
        issuance_root / profile.sandbox / SHA / "clients" / node
        for node in host.ELIGIBLE_LINK_NODES
    ]


def test_remote_cleanup_failure_is_persisted_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setattr(host, "SOURCE_STAGING_ROOT", tmp_path / "source")
    monkeypatch.setattr(os, "chown", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_ensure_root_private_directory",
        lambda path: (path.mkdir(parents=True, exist_ok=True), path.chmod(0o700)),
    )
    monkeypatch.setattr(
        host,
        "_ssh",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            host.HostConvergeError("remote cleanup failed"),
        ),
    )

    with pytest.raises(host.HostConvergeError, match="remote cleanup failed"):
        host._cleanup_remote_stage(
            profile,
            SHA,
            "oldlab",
            "oldlab-1",
            Path("/run/loom-developer-sandbox-installer/source/qianyi"),
        )

    failure = host._remote_stage_failure_path(profile, SHA, "oldlab", "oldlab-1")
    payload = json.loads(failure.read_text(encoding="utf-8"))
    assert payload["status"] == "remote-cleanup-failed"
    assert stat.S_IMODE(failure.stat().st_mode) == 0o600


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
