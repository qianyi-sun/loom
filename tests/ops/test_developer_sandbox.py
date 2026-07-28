from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox as sandbox

SHA = "a" * 40
TREE = "b" * 40


class FakeRunner:
    def __init__(self, *, dirty: bool = False) -> None:
        self.dirty = dirty
        self.calls: list[tuple[tuple[str, ...], Path, Mapping[str, str] | None]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> sandbox.CommandResult:
        command = tuple(argv)
        self.calls.append((command, cwd, env))
        if command[:3] == ("git", "rev-parse", "--verify"):
            return sandbox.CommandResult(0, TREE + "\n" if command[-1] == "HEAD^{tree}" else SHA + "\n")
        if command[:2] == ("git", "status"):
            return sandbox.CommandResult(0, " M unsafe\n" if self.dirty else "")
        if command[:2] == ("docker", "compose") and "ps" in command:
            rows = [
                {"Service": service, "State": "running", "Health": "healthy"}
                for service in sandbox._EXPECTED_SERVICES
            ]
            return sandbox.CommandResult(0, json.dumps(rows))
        return sandbox.CommandResult(0)


def _write_profiles(tmp_path: Path) -> tuple[Path, Path]:
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    candidates = tmp_path / "candidates" / "sandboxes"
    state_parent = tmp_path / "developer-sandboxes"
    for index, owner in enumerate(sandbox.ALLOWED_SANDBOXES):
        port = 20_000 + index * 1_000
        state = state_parent / owner
        (profiles_dir / f"{owner}.toml").write_text(
            f"""schema_version = 1
sandbox = "{owner}"
ssh_target = "oldlab-2"
canonical_hostname = "trt-eai-oldlab-2"
compose_project = "loom-sandbox-{owner}"
bind_address = "127.0.0.1"
provider_connection_namespace = "sandbox-{owner}"
candidate_root = "{candidates / owner}"
state_root = "{state}"
cache_root = "{state / "cache"}"
evidence_root = "{state / "evidence"}"
runtime_root = "{state / "runtime"}"

[ports]
postgres = {port + 1}
minio = {port + 2}
minio_console = {port + 3}
control_plane = {port + 4}
loom_service = {port + 5}
llm_gateway = {port + 6}
egress_xds = {port + 7}
egress_proxy = {port + 8}
egress_admin = {port + 9}
web = {port + 10}

[database]
name = "loom_sandbox_{owner}"

[object_store]
task_bucket = "loom-sandbox-{owner}-tasks"
trajectories_bucket = "loom-sandbox-{owner}-trajectories"
artifacts_bucket = "loom-sandbox-{owner}-artifacts"
""",
            encoding="utf-8",
        )
    return profiles_dir, candidates


def _write_secret_files(tmp_path: Path) -> tuple[Path, Path]:
    secrets = tmp_path / "sandbox.env"
    secrets.write_text(
        "\n".join(f"{key}=test-value" for key in sorted(sandbox._REQUIRED_SECRET_ENV_KEYS))
        + "\n",
        encoding="utf-8",
    )
    admin = tmp_path / "admin.toml"
    admin.write_text(f'[admin]\ntoken = "loom_admin_{"A" * 43}"\n', encoding="utf-8")
    secrets.chmod(0o600)
    admin.chmod(0o600)
    return secrets, admin


def _candidate(candidates: Path, owner: str = "qianyi") -> Path:
    source = candidates / owner / SHA
    (source / "deploy").mkdir(parents=True)
    (source / "deploy" / "docker-compose.dev.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    return source


def test_checked_in_profiles_are_typed_and_cross_profile_distinct() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    profiles = sandbox.load_profiles(repo_root / "deploy" / "developer-sandboxes")

    assert {profile.sandbox for profile in profiles} == set(sandbox.ALLOWED_SANDBOXES)
    assert len({port for profile in profiles for port in profile.ports.values()}) == 30
    assert all(profile.bind_address == "127.0.0.1" for profile in profiles)


def test_profile_loader_rejects_cross_profile_port_collision(tmp_path: Path) -> None:
    profiles_dir, _ = _write_profiles(tmp_path)
    devansh = profiles_dir / "devansh.toml"
    devansh.write_text(
        devansh.read_text(encoding="utf-8").replace(
            "postgres = 22001",
            "postgres = 20001",
        ),
        encoding="utf-8",
    )

    with pytest.raises(sandbox.SandboxProfileError, match="ports must be distinct"):
        sandbox.load_profiles(profiles_dir)


def test_create_plan_is_exact_candidate_bound_and_never_executes_compose(
    tmp_path: Path,
) -> None:
    profiles_dir, candidates = _write_profiles(tmp_path)
    source = _candidate(candidates)
    secrets, admin = _write_secret_files(tmp_path)
    runner = FakeRunner()
    host_called = False

    def hostname() -> str:
        nonlocal host_called
        host_called = True
        return "wrong-host"

    plan = sandbox.operate(
        "create",
        profile_path=profiles_dir / "qianyi.toml",
        source_repo=source,
        candidate_sha=SHA,
        secrets_env=secrets,
        admin_secret_file=admin,
        execute=False,
        delete_volumes=False,
        runner=runner,
        canonical_hostname=hostname,
    )

    assert plan["candidate_path"] == str(source)
    assert plan["mutation_authorized"] is False
    assert not host_called
    assert all(call[0][0] == "git" for call in runner.calls)


def test_create_rejects_candidate_outside_profile_sha_path(tmp_path: Path) -> None:
    profiles_dir, _ = _write_profiles(tmp_path)
    source = tmp_path / "arbitrary-checkout"
    (source / "deploy").mkdir(parents=True)
    secrets, admin = _write_secret_files(tmp_path)

    with pytest.raises(sandbox.SandboxOperationError, match="candidate_root/<sha>"):
        sandbox.operate(
            "create",
            profile_path=profiles_dir / "qianyi.toml",
            source_repo=source,
            candidate_sha=SHA,
            secrets_env=secrets,
            admin_secret_file=admin,
            execute=False,
            delete_volumes=False,
            runner=FakeRunner(),
        )


def test_execute_fails_closed_on_wrong_host_before_compose(tmp_path: Path) -> None:
    profiles_dir, candidates = _write_profiles(tmp_path)
    source = _candidate(candidates)
    secrets, admin = _write_secret_files(tmp_path)
    runner = FakeRunner()

    with pytest.raises(sandbox.SandboxOperationError, match="execution host"):
        sandbox.operate(
            "create",
            profile_path=profiles_dir / "qianyi.toml",
            source_repo=source,
            candidate_sha=SHA,
            secrets_env=secrets,
            admin_secret_file=admin,
            execute=True,
            delete_volumes=False,
            runner=runner,
            canonical_hostname=lambda: "not-oldlab-2",
        )

    assert all(call[0][0] == "git" for call in runner.calls)


def test_execute_create_records_exact_state_without_exposing_secret_values(
    tmp_path: Path,
) -> None:
    profiles_dir, candidates = _write_profiles(tmp_path)
    source = _candidate(candidates)
    secrets, admin = _write_secret_files(tmp_path)
    runner = FakeRunner()

    result = sandbox.operate(
        "create",
        profile_path=profiles_dir / "qianyi.toml",
        source_repo=source,
        candidate_sha=SHA,
        secrets_env=secrets,
        admin_secret_file=admin,
        execute=True,
        delete_volumes=False,
        runner=runner,
        canonical_hostname=lambda: "trt-EAI-OLDLAB-2",
    )

    state = json.loads(
        (tmp_path / "developer-sandboxes/qianyi/sandbox-state.json").read_text(),
    )
    docker_calls = [call for call in runner.calls if call[0][0] == "docker"]
    assert result["status"] == "succeeded"
    assert state["candidate_sha"] == SHA
    assert state["candidate_tree"] == TREE
    assert docker_calls
    assert all("test-value" not in " ".join(call[0]) for call in docker_calls)
    assert all("LOOM_DEV_POSTGRES_PASSWORD" not in (call[2] or {}) for call in docker_calls)


def test_destroy_preserves_volumes_unless_explicit(tmp_path: Path) -> None:
    profile = sandbox.load_profile(
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/qianyi.toml",
    )
    source = tmp_path / "candidate"
    (source / "deploy").mkdir(parents=True)
    (source / "deploy/docker-compose.dev.yml").write_text("services: {}\n")
    binding = sandbox.CandidateBinding(SHA, TREE, source)
    secrets = Path("/secrets.env")

    safe = sandbox.build_commands(
        "destroy",
        profile=profile,
        binding=binding,
        secrets_env=secrets,
    )
    deleting = sandbox.build_commands(
        "destroy",
        profile=profile,
        binding=binding,
        secrets_env=secrets,
        delete_volumes=True,
    )

    assert "--volumes" not in safe[-1].argv
    assert "--volumes" in deleting[-1].argv


def test_prepare_bootstrap_starts_loopback_stack_without_committing_state(
    tmp_path: Path,
) -> None:
    profiles_dir, candidates = _write_profiles(tmp_path)
    source = _candidate(candidates)
    secrets, admin = _write_secret_files(tmp_path)
    runner = FakeRunner()

    result = sandbox.operate(
        "prepare",
        profile_path=profiles_dir / "qianyi.toml",
        source_repo=source,
        candidate_sha=SHA,
        secrets_env=secrets,
        admin_secret_file=admin,
        execute=True,
        delete_volumes=False,
        runner=runner,
        canonical_hostname=lambda: "trt-eai-oldlab-2",
    )

    assert result["status"] == "succeeded"
    assert not (tmp_path / "developer-sandboxes/qianyi/sandbox-state.json").exists()
    assert any("--force-recreate" in call[0] for call in runner.calls)


def test_prepare_stop_is_valid_without_committed_state(tmp_path: Path) -> None:
    profiles_dir, candidates = _write_profiles(tmp_path)
    source = _candidate(candidates)
    secrets, admin = _write_secret_files(tmp_path)
    runner = FakeRunner()

    result = sandbox.operate(
        "prepare-stop",
        profile_path=profiles_dir / "qianyi.toml",
        source_repo=source,
        candidate_sha=SHA,
        secrets_env=secrets,
        admin_secret_file=admin,
        execute=True,
        delete_volumes=False,
        runner=runner,
        canonical_hostname=lambda: "trt-eai-oldlab-2",
    )

    assert result["status"] == "succeeded"
    assert any("down" in call[0] and "--volumes" not in call[0] for call in runner.calls)


def test_dirty_candidate_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "repo"
    source.mkdir()

    with pytest.raises(sandbox.SandboxOperationError, match="not clean"):
        sandbox.bind_candidate(source, SHA, runner=FakeRunner(dirty=True))


def test_dev_compose_exposes_all_sandbox_isolation_inputs() -> None:
    compose = (
        Path(__file__).resolve().parents[2] / "deploy/docker-compose.dev.yml"
    ).read_text(encoding="utf-8")

    for key in (
        "LOOM_DEV_IMAGE_TAG",
        "LOOM_DEV_POSTGRES_PORT",
        "LOOM_DEV_POSTGRES_DB",
        "LOOM_DEV_MINIO_PORT",
        "LOOM_DEV_MINIO_CONSOLE_PORT",
        "LOOM_DEV_ADMIN_SECRET_FILE",
        "LOOM_DEV_TRAJECTORIES_BUCKET",
        "LOOM_DEV_ARTIFACTS_BUCKET",
        "LOOM_DEV_WEB_PORT",
    ):
        assert key in compose
