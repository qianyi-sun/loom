from __future__ import annotations

import importlib
import json
import os
import stat
import subprocess
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import ModuleType

import pytest
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_FILES = (
    "scripts/ops/converge_personal_dev_native_builder_release.py",
    "scripts/ops/install_personal_dev_native_builder_runtime.py",
    "scripts/ops/install_personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_conformance.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
    "scripts/ops/personal_dev_native_builder_runtime_profile.py",
    "scripts/ops/staging_rollout_sealed_source.py",
    "deploy/personal-dev-native-builder/dockerd.json",
    "deploy/personal-dev-native-builder/loom-personal-dev-builder-dockerd.service",
    "deploy/personal-dev-native-builder/loom-personal-dev-builder.slice",
    "deploy/personal-dev-native-builder/loom-personal-dev-native-builder-agent.service.in",
    "deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.sudoers",
    "deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.tmpfiles",
    "deploy/personal-dev-native-builder/loom-personal-dev-native-builder.sysusers",
    "deploy/personal-dev-native-builder/provider-network.nft",
    "deploy/personal-dev-native-builder/runsc.toml",
    "deploy/personal-dev-native-builder/runtime-profile-v1.json",
)
SUDOERS = (
    b'qianyi ALL=(root) NOPASSWD:NOSETENV: '
    b'/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority ""\n'
)
TMPFILES = (
    b"d /var/lib/loom/personal-dev-native-builder-runtime-authority 0700 root root -\n"
    b"f /run/lock/loom-personal-dev-native-builder-runtime-authority.lock "
    b"0600 root root -\n"
    b"d /run/loom-personal-dev-native-builder-runtime-authority 0700 root root -\n"
)


def _module() -> ModuleType:
    return importlib.import_module(
        "scripts.ops.install_personal_dev_native_builder_runtime_authority"
    )


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-c", "maintenance.auto=false", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
        umask=0o022,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "Native Authority Test",
            "GIT_AUTHOR_EMAIL": "native-authority@example.invalid",
            "GIT_COMMITTER_NAME": "Native Authority Test",
            "GIT_COMMITTER_EMAIL": "native-authority@example.invalid",
        },
    ).stdout.strip()


def _copy_source_file(source_root: Path, relative: str) -> None:
    destination = source_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    destination.write_bytes((REPO_ROOT / relative).read_bytes())
    destination.chmod(0o644)


def _sealed_source(tmp_path: Path) -> tuple[Path, str, str, str]:
    source_root = tmp_path / "sealed-source"
    source_root.mkdir(mode=0o700)
    _git(source_root, "init")
    _git(
        source_root,
        "remote",
        "add",
        "origin",
        "https://github.com/qianyi-sun/loom.git",
    )
    marker = source_root / "base.txt"
    marker.write_text("approved base\n", encoding="ascii")
    marker.chmod(0o644)
    _git(source_root, "add", "base.txt")
    _git(source_root, "commit", "-m", "approved base")
    base = _git(source_root, "rev-parse", "HEAD")
    for relative in SOURCE_FILES:
        _copy_source_file(source_root, relative)
    for directory in source_root.rglob("*"):
        if directory.is_dir() and ".git" not in directory.parts:
            directory.chmod(0o755)
    _git(source_root, "add", "--all")
    _git(source_root, "commit", "-m", "sealed native authority")
    commit = _git(source_root, "rev-parse", "HEAD")
    tree = _git(source_root, "rev-parse", "HEAD^{tree}")
    _git(source_root, "checkout", "--detach", commit)
    source_root.chmod(0o700)
    return source_root, base, commit, tree


def _host_root(tmp_path: Path) -> Path:
    host_root = tmp_path / "host"
    for relative in (
        "etc",
        "etc/sudoers.d",
        "run",
        "run/lock",
        "usr",
        "usr/lib",
        "usr/local",
        "usr/local/lib",
        "usr/local/libexec",
        "var",
        "var/lib",
    ):
        path = host_root / relative
        path.mkdir(parents=True, exist_ok=True, mode=0o755)
        path.chmod(0o755)
    host_root.chmod(0o755)
    return host_root


def _remove_unsafe_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    names = {"BASH_ENV", "CDPATH", "ENV", "IFS"}
    prefixes = ("DOCKER_", "GIT_", "LD_", "NFTABLES_", "PYTHON", "SUDO_", "SYSTEMD_")
    for name in tuple(os.environ):
        if name in names or name.startswith(prefixes):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture
def authority_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[ModuleType, Path, str, str]]:
    module = _module()
    source_root, base, commit, tree = _sealed_source(tmp_path)
    host_root = _host_root(tmp_path)
    monkeypatch.setattr(module, "SOURCE_ROOT", source_root)
    monkeypatch.setattr(module, "APPROVED_BASE_SHA", base)
    monkeypatch.setattr(module, "HOST_ROOT", host_root)
    monkeypatch.setattr(module, "ROOT_UID", os.getuid())
    monkeypatch.setattr(module, "ROOT_GID", os.getgid())
    monkeypatch.setattr(
        module,
        "__file__",
        str(source_root / "scripts/ops/install_personal_dev_native_builder_runtime_authority.py"),
    )
    monkeypatch.setattr(module.os, "getresuid", lambda: (0, 0, 0))
    monkeypatch.setattr(module.os, "getresgid", lambda: (0, 0, 0))
    load_validator = module._load_validator

    def trust_temporary_parents() -> ModuleType:
        validator = load_validator()
        validator._validate_parent_authority = lambda *_args, **_kwargs: None
        return validator

    monkeypatch.setattr(module, "_load_validator", trust_temporary_parents)
    _remove_unsafe_environment(monkeypatch)
    yield module, host_root, commit, tree


def _installed(host_root: Path, absolute: str) -> Path:
    return host_root.joinpath(*Path(absolute).parts[1:])


def _tree_snapshot(root: Path) -> tuple[tuple[str, str, int, bytes], ...]:
    values: list[tuple[str, str, int, bytes]] = []
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        kind = "directory" if stat.S_ISDIR(metadata.st_mode) else "file"
        payload = path.read_bytes() if kind == "file" else b""
        values.append((str(path.relative_to(root)), kind, stat.S_IMODE(metadata.st_mode), payload))
    return tuple(values)


def test_sudoers_and_tmpfiles_assets_have_the_only_authorized_rules() -> None:
    sudoers = (
        REPO_ROOT
        / "deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.sudoers"
    ).read_bytes()
    tmpfiles = (
        REPO_ROOT
        / "deploy/personal-dev-native-builder/loom-personal-dev-native-builder-runtime-authority.tmpfiles"
    ).read_bytes()

    assert sudoers == SUDOERS
    assert sudoers.count(b"qianyi") == 1
    assert b"(root)" in sudoers
    assert b"NOPASSWD:NOSETENV:" in sudoers
    assert b"*" not in sudoers
    assert sudoers.endswith(b' ""\n')
    assert tmpfiles == TMPFILES


@pytest.mark.parametrize(
    "path",
    (
        "scripts/ops/converge_personal_dev_native_builder_release.py",
        "scripts/ops/install_personal_dev_native_builder_runtime.py",
        "scripts/ops/install_personal_dev_native_builder_runtime_authority.py",
        "scripts/ops/personal_dev_native_builder_conformance.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
        "scripts/ops/personal_dev_native_builder_runtime_profile.py",
        "tests/ops/test_install_personal_dev_native_builder_runtime_authority.py",
        "tests/ops/test_personal_dev_native_builder_runtime_authority.py",
        "deploy/personal-dev-native-builder/runtime-profile-v1.json",
    ),
)
def test_native_authority_paths_select_every_heavy_gate_as_protected(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all("protected-native-authority" in plan.reasons[name] for name in HEAVY_CHECKS)


def test_bootstrap_installs_the_authoritative_inventory_and_is_idempotent(
    authority_install: tuple[ModuleType, Path, str, str],
) -> None:
    module, host_root, source_sha, source_tree = authority_install

    first = module.bootstrap(source_sha, source_tree)
    after_first = _tree_snapshot(host_root)
    second = module.bootstrap(source_sha, source_tree)

    assert first["status"] == "ok"
    assert first["changed"] is True
    assert second == {**first, "changed": False}
    assert _tree_snapshot(host_root) == after_first
    assert set(first["asset_sha256"]) == {
        "broker",
        "conformance",
        "converger",
        "installer",
        "launcher",
        "protocol",
        "runtime_asset_agent_service_template",
        "runtime_asset_dockerd_config",
        "runtime_asset_dockerd_service",
        "runtime_asset_nftables",
        "runtime_asset_profile",
        "runtime_asset_runsc_config",
        "runtime_asset_slice_unit",
        "runtime_asset_sysusers",
        "runtime_profile_helper",
        "sudoers",
        "tmpfiles",
    }
    assert first["runtime_profile_sha256"] == first["asset_sha256"]["runtime_asset_profile"]
    assert len(json.dumps(first, sort_keys=True, separators=(",", ":")).encode("ascii")) < 64 * 1024

    launcher = _installed(
        host_root,
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
    )
    broker = _installed(
        host_root,
        "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/scripts/ops/"
        "personal_dev_native_builder_runtime_authority.py",
    )
    assert stat.S_IMODE(launcher.stat().st_mode) == 0o555
    assert stat.S_IMODE(broker.stat().st_mode) == 0o444
    for path in host_root.rglob("*"):
        if path.is_file() and path != launcher:
            assert stat.S_IMODE(path.stat().st_mode) & 0o111 == 0
        if path.is_file():
            assert path.stat().st_nlink == 1
            assert (path.stat().st_uid, path.stat().st_gid) == (os.getuid(), os.getgid())

    policy = json.loads(
        _installed(
            host_root,
            "/etc/loom/personal-dev-native-builder-runtime-authority.json",
        ).read_text(encoding="ascii")
    )
    assert policy["asset_sha256"] == first["asset_sha256"]
    assert policy["authority_source_sha"] == source_sha
    assert policy["authority_source_tree"] == source_tree

    assert stat.S_IMODE(
        _installed(
            host_root,
            "/var/lib/loom/personal-dev-native-builder-runtime-authority",
        ).stat().st_mode
    ) == 0o700
    lock = _installed(
        host_root,
        "/run/lock/loom-personal-dev-native-builder-runtime-authority.lock",
    )
    assert lock.read_bytes() == b""
    assert stat.S_IMODE(lock.stat().st_mode) == 0o600
    assert stat.S_IMODE(
        _installed(
            host_root,
            "/run/loom-personal-dev-native-builder-runtime-authority",
        ).stat().st_mode
    ) == 0o700
    assert not _installed(
        host_root,
        "/var/lib/loom/personal-dev-native-builder-runtime-authority/state-v1.json",
    ).exists()


@pytest.mark.parametrize("unsafe_name", ("SUDO_UID", "PYTHONPATH", "LD_PRELOAD"))
def test_bootstrap_rejects_sudo_and_ambient_unsafe_environment(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_name: str,
) -> None:
    module = _module()
    monkeypatch.setattr(module.os, "getresuid", lambda: (0, 0, 0))
    monkeypatch.setattr(module.os, "getresgid", lambda: (0, 0, 0))
    monkeypatch.setenv(unsafe_name, "untrusted")

    with pytest.raises(module.BootstrapError, match="direct_root_required"):
        module.bootstrap("1" * 40, "2" * 40)


def test_bootstrap_rejects_non_root_before_opening_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module.os, "getresuid", lambda: (1000, 1000, 1000))
    monkeypatch.setattr(module.os, "getresgid", lambda: (1000, 1000, 1000))
    _remove_unsafe_environment(monkeypatch)

    with pytest.raises(module.BootstrapError, match="direct_root_required"):
        module.bootstrap("1" * 40, "2" * 40)


@pytest.mark.parametrize("mutation", ("symlink", "hardlink", "writable"))
def test_bootstrap_rejects_unsafe_source_members_before_installing(
    authority_install: tuple[ModuleType, Path, str, str],
    mutation: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    source = module.SOURCE_ROOT / "deploy/personal-dev-native-builder/runtime-profile-v1.json"
    if mutation == "symlink":
        payload = source.read_bytes()
        source.unlink()
        target = source.with_name("profile-target")
        target.write_bytes(payload)
        target.chmod(0o644)
        source.symlink_to(target.name)
    elif mutation == "hardlink":
        os.link(source, source.with_name("profile-hardlink"))
    else:
        source.chmod(0o664)

    before = _tree_snapshot(host_root)
    with pytest.raises(module.BootstrapError, match="sealed_source_invalid"):
        module.bootstrap(source_sha, source_tree)
    assert _tree_snapshot(host_root) == before


def test_bootstrap_never_overwrites_installed_drift(
    authority_install: tuple[ModuleType, Path, str, str],
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    destination = _installed(
        host_root,
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
    )
    destination.write_bytes(b"operator drift\n")
    destination.chmod(0o555)

    with pytest.raises(module.BootstrapError, match="installed_drift"):
        module.bootstrap(source_sha, source_tree)

    assert destination.read_bytes() == b"operator drift\n"
    assert not _installed(
        host_root,
        "/etc/sudoers.d/loom-personal-dev-native-builder-runtime-authority",
    ).exists()


@pytest.mark.parametrize("collision", ("staged-sudoers", "launcher-temporary"))
def test_bootstrap_never_removes_a_preexisting_temporary_name_collision(
    authority_install: tuple[ModuleType, Path, str, str],
    collision: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    if collision == "staged-sudoers":
        existing = host_root / "run" / (
            f".loom-native-authority-sudoers.validate-{os.getpid()}"
        )
        expected_error = "sudoers_invalid"
    else:
        existing = host_root / "usr/local/libexec" / (
            ".loom-personal-dev-native-builder-runtime-authority"
            f".new-{os.getpid()}"
        )
        expected_error = "installed_drift"
    existing.write_bytes(b"preexisting root data\n")
    existing.chmod(0o600)

    with pytest.raises(module.BootstrapError, match=expected_error):
        module.bootstrap(source_sha, source_tree)

    assert existing.read_bytes() == b"preexisting root data\n"


def test_failed_installed_sudoers_validation_rolls_back_every_created_object(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    before = _tree_snapshot(host_root)
    calls: list[tuple[str, ...]] = []
    environments: list[dict[str, str]] = []

    def fail_installed(
        argv: Sequence[str],
        environment: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(argv))
        environments.append(dict(environment))
        return subprocess.CompletedProcess(
            list(argv),
            0 if len(calls) == 1 else 1,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(module, "_COMMAND_RUNNER", fail_installed)
    with pytest.raises(module.BootstrapError, match="sudoers_invalid"):
        module.bootstrap(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before
    assert len(calls) == 2
    assert all(call[:2] == ("/usr/sbin/visudo", "-cf") for call in calls)
    assert all(environment == module.ROOT_ENVIRONMENT for environment in environments)


def test_cli_has_only_the_direct_bootstrap_shape() -> None:
    module = _module()

    with pytest.raises(SystemExit, match="2"):
        module.main(["status"])
