from __future__ import annotations

import importlib
import json
import os
import stat
import subprocess
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace

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
    b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
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
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="gx10-01c7", machine="aarch64"),
    )
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

    assert (
        stat.S_IMODE(
            _installed(
                host_root,
                "/var/lib/loom/personal-dev-native-builder-runtime-authority",
            )
            .stat()
            .st_mode
        )
        == 0o700
    )
    lock = _installed(
        host_root,
        "/run/lock/loom-personal-dev-native-builder-runtime-authority.lock",
    )
    assert lock.read_bytes() == b""
    assert stat.S_IMODE(lock.stat().st_mode) == 0o600
    assert (
        stat.S_IMODE(
            _installed(
                host_root,
                "/run/loom-personal-dev-native-builder-runtime-authority",
            )
            .stat()
            .st_mode
        )
        == 0o700
    )
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


@pytest.mark.parametrize(
    ("nodename", "machine"),
    (("wrong-host", "aarch64"), ("gx10-01c7", "x86_64")),
    ids=("wrong-host", "wrong-architecture"),
)
def test_bootstrap_rejects_wrong_target_before_any_filesystem_change(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    nodename: str,
    machine: str,
) -> None:
    """Catches publishing the sealed authority on any host except the fixed GB10."""
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename=nodename, machine=machine),
    )
    before = _tree_snapshot(host_root)

    with pytest.raises(module.BootstrapError, match="target_host_required"):
        module.bootstrap(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before


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


def test_source_capture_rejects_identity_change_between_lstat_and_open(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    source = module.SOURCE_ROOT / (
        "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py"
    )
    replacement = source.with_name("launcher-replacement")
    original_open = module.os.open
    original_validate = module._validate_sealed_source
    armed = False
    attacked = False

    def validate_then_arm(*args: object, **kwargs: object) -> None:
        nonlocal armed
        original_validate(*args, **kwargs)
        armed = True

    def replace_before_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        if armed and not attacked and dir_fd is None and Path(path) == source:
            attacked = True
            replacement.write_bytes(source.read_bytes())
            replacement.chmod(0o644)
            os.replace(replacement, source)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(module, "_validate_sealed_source", validate_then_arm)
    monkeypatch.setattr(module.os, "open", replace_before_open)

    before = _tree_snapshot(host_root)
    with pytest.raises(module.BootstrapError, match="source_asset_invalid"):
        module.bootstrap(source_sha, source_tree)

    assert attacked is True
    assert _tree_snapshot(host_root) == before


def test_destination_publication_race_preserves_the_concurrent_file(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    destination = _installed(
        host_root,
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
    )
    original_rename = module._rename_noreplace
    racer = b"concurrent root publication\n"
    attacked = False

    def publish_racer(source: Path, target: Path) -> None:
        nonlocal attacked
        if not attacked and target == destination:
            attacked = True
            target.write_bytes(racer)
            target.chmod(0o555)
        original_rename(source, target)

    monkeypatch.setattr(module, "_rename_noreplace", publish_racer)

    with pytest.raises(module.BootstrapError, match="installed_drift"):
        module.bootstrap(source_sha, source_tree)

    assert attacked is True
    assert destination.read_bytes() == racer
    assert not _installed(
        host_root,
        "/etc/sudoers.d/loom-personal-dev-native-builder-runtime-authority",
    ).exists()


@pytest.mark.parametrize("boundary", ("file", "parent"))
def test_fsync_failure_rolls_back_the_real_transaction(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    temporary = _installed(
        host_root,
        f"/usr/local/libexec/.loom-personal-dev-native-builder-runtime-authority.new-{os.getpid()}",
    )
    parent = temporary.parent
    original_fsync = module.os.fsync
    attacked = False

    def fail_boundary(descriptor: int) -> None:
        nonlocal attacked
        descriptor_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        target = temporary if boundary == "file" else parent
        if not attacked and descriptor_path == target:
            attacked = True
            raise OSError("injected durability failure")
        original_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", fail_boundary)

    before = _tree_snapshot(host_root)
    with pytest.raises(module.BootstrapError, match="publication_failed"):
        module.bootstrap(source_sha, source_tree)

    assert attacked is True
    assert _tree_snapshot(host_root) == before


@pytest.mark.parametrize("collision", ("staged-sudoers", "launcher-temporary"))
def test_bootstrap_never_removes_a_preexisting_temporary_name_collision(
    authority_install: tuple[ModuleType, Path, str, str],
    collision: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    if collision == "staged-sudoers":
        existing = host_root / "run" / (f".loom-native-authority-sudoers.validate-{os.getpid()}")
        expected_error = "sudoers_invalid"
    else:
        existing = (
            host_root
            / "usr/local/libexec"
            / (f".loom-personal-dev-native-builder-runtime-authority.new-{os.getpid()}")
        )
        expected_error = "installed_drift"
    existing.write_bytes(b"preexisting root data\n")
    existing.chmod(0o600)

    with pytest.raises(module.BootstrapError, match=expected_error):
        module.bootstrap(source_sha, source_tree)

    assert existing.read_bytes() == b"preexisting root data\n"


def test_temporary_cleanup_preserves_a_replacement_raced_in_after_creation(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    temporary = _installed(
        host_root,
        f"/usr/local/libexec/.loom-personal-dev-native-builder-runtime-authority.new-{os.getpid()}",
    )
    displaced = host_root / "displaced-attempt-temporary"
    racer = b"replacement temporary\n"
    original_fsync = module.os.fsync
    attacked = False

    def replace_temporary(descriptor: int) -> None:
        nonlocal attacked
        descriptor_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if not attacked and descriptor_path == temporary:
            attacked = True
            os.rename(temporary, displaced)
            temporary.write_bytes(racer)
            temporary.chmod(0o600)
            raise OSError("injected write durability failure")
        original_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", replace_temporary)

    with pytest.raises(module.BootstrapError, match="publication_failed"):
        module.bootstrap(source_sha, source_tree)

    assert attacked is True
    assert temporary.read_bytes() == racer


def test_rollback_preserves_an_installed_file_replacement(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    launcher = _installed(
        host_root,
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
    )
    displaced = host_root / "displaced-attempt-launcher"
    racer = b"replacement installed file\n"

    def replace_then_fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        os.rename(launcher, displaced)
        launcher.write_bytes(racer)
        launcher.chmod(0o555)
        raise module.BootstrapError("installed_validation_failed")

    monkeypatch.setattr(module, "_validate_installed_policy", replace_then_fail)

    with pytest.raises(module.BootstrapError, match="installed_validation_failed"):
        module.bootstrap(source_sha, source_tree)

    assert launcher.read_bytes() == racer


def test_rollback_preserves_a_created_directory_replacement(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    state_root = _installed(
        host_root,
        "/var/lib/loom/personal-dev-native-builder-runtime-authority",
    )
    displaced = host_root / "displaced-attempt-state-root"

    def replace_then_fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        os.rename(state_root, displaced)
        state_root.mkdir(mode=0o700)
        raise module.BootstrapError("installed_validation_failed")

    monkeypatch.setattr(module, "_validate_installed_policy", replace_then_fail)

    with pytest.raises(module.BootstrapError, match="installed_validation_failed"):
        module.bootstrap(source_sha, source_tree)

    assert state_root.is_dir()
    assert state_root.stat().st_ino != displaced.stat().st_ino


def test_rollback_preserves_directory_replaced_before_identity_capture(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    state_root = _installed(
        host_root,
        "/var/lib/loom/personal-dev-native-builder-runtime-authority",
    )
    displaced = host_root / "displaced-before-directory-identity"
    original_mkdir = module.os.mkdir
    attacked = False

    def replace_before_identity(
        path: os.PathLike[str] | str,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal attacked
        if dir_fd is None:
            original_mkdir(path, mode)
            created_path = Path(os.fspath(path))
        else:
            original_mkdir(path, mode, dir_fd=dir_fd)
            created_path = Path(os.readlink(f"/proc/self/fd/{dir_fd}")) / Path(path)
        if not attacked and created_path == state_root:
            attacked = True
            os.rename(state_root, displaced)
            original_mkdir(state_root, 0o700)

    def fail_after_creation(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise module.BootstrapError("installed_validation_failed")

    monkeypatch.setattr(module.os, "mkdir", replace_before_identity)
    monkeypatch.setattr(module, "_validate_installed_policy", fail_after_creation)

    with pytest.raises(module.BootstrapError, match="publication_failed"):
        module.bootstrap(source_sha, source_tree)

    assert attacked is True
    assert state_root.is_dir()
    assert state_root.stat().st_ino != displaced.stat().st_ino


def test_installed_policy_is_validated_against_staged_sudoers_before_publication(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    sudoers = _installed(
        host_root,
        "/etc/sudoers.d/loom-personal-dev-native-builder-runtime-authority",
    )
    original_validate = module._validate_installed_policy
    observed = False

    def validate_before_publication(*args: object, **kwargs: object) -> None:
        nonlocal observed
        staged = kwargs["sudoers_path"]
        assert isinstance(staged, Path)
        assert not sudoers.exists()
        assert staged != sudoers
        assert staged.read_bytes() == SUDOERS
        assert stat.S_IMODE(staged.stat().st_mode) == 0o440
        observed = True
        original_validate(*args, **kwargs)

    monkeypatch.setattr(module, "_validate_installed_policy", validate_before_publication)

    receipt = module.bootstrap(source_sha, source_tree)

    assert observed is True
    assert receipt["status"] == "ok"
    assert sudoers.read_bytes() == SUDOERS


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
