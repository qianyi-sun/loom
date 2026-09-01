from __future__ import annotations

import ast
import hashlib
import importlib
import json
import os
import stat
import subprocess
import sys
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
    "scripts/ops/personal_dev_native_builder_runtime_crypto.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py",
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

ROOT_EXECUTION_PYTHON = (
    "scripts/ops/install_personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
    "scripts/ops/install_personal_dev_native_builder_runtime.py",
    "scripts/ops/personal_dev_native_builder_runtime_profile.py",
    "scripts/ops/converge_personal_dev_native_builder_release.py",
    "scripts/ops/personal_dev_native_builder_conformance.py",
    "scripts/ops/personal_dev_native_builder_runtime_crypto.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
    "scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py",
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


def test_root_execution_import_closure_is_stdlib_or_policy_bound() -> None:
    """Catches an ambient Python package entering any root execution path."""
    policy_modules = {
        f"scripts.ops.{Path(relative).stem}" for relative in ROOT_EXECUTION_PYTHON
    } | {"scripts", "scripts.ops"}
    ambient: set[str] = set()
    for relative in ROOT_EXECUTION_PYTHON:
        tree = ast.parse((REPO_ROOT / relative).read_bytes(), filename=relative)
        for node in ast.walk(tree):
            names: tuple[str, ...]
            if isinstance(node, ast.Import):
                names = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                names = (node.module,)
            else:
                continue
            for name in names:
                root = name.partition(".")[0]
                if root not in sys.stdlib_module_names and name not in policy_modules:
                    ambient.add(name)

    assert ambient == set()


@pytest.mark.parametrize(
    "entrypoint",
    ("bootstrap", "broker", "client", "material-client"),
)
def test_root_execution_rejects_ambient_import_resolution(entrypoint: str) -> None:
    """Catches bootstrap, broker, or client loading code outside the sealed closure."""
    child = f"""
import hashlib
import importlib.abc
import os
import pathlib
import sys

root = pathlib.Path({str(REPO_ROOT)!r})
allowed_repository = {{"scripts", "scripts.ops"}} | {{
    "scripts.ops." + pathlib.Path(value).stem
    for value in {ROOT_EXECUTION_PYTHON!r}
}} | {{
    pathlib.Path(value).stem for value in {ROOT_EXECUTION_PYTHON!r}
}}

class RejectAmbient(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        root_name = fullname.partition(".")[0]
        if (
            root_name not in sys.stdlib_module_names
            and root_name != "org"
            and fullname not in allowed_repository
        ):
            raise RuntimeError(f"ambient import blocked: {{fullname}}")
        return None

sys.meta_path.insert(0, RejectAmbient())
sys.path[:0] = [str(root), str(root / "scripts" / "ops")]

if {entrypoint!r} == "bootstrap":
    from scripts.ops import install_personal_dev_native_builder_runtime_authority as module
    module.ROOT_UID = os.getuid()
    module.ROOT_GID = os.getgid()
    module.SOURCE_ROOT = root
    module._read_source_file = lambda path, **_kwargs: path.read_bytes()
    with module._pinned_source_contract():
        pass
elif {entrypoint!r} == "broker":
    from scripts.ops import personal_dev_native_builder_runtime_authority
elif {entrypoint!r} == "client":
    import personal_dev_native_builder_runtime_authority_client as module
    seed = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc4"
                         "4449c5697b326919703bac031cae7f60")
    public = bytes.fromhex("d75a980182b10ab7d54bfed3c964073a"
                           "0ee172f3daa62325af021a68f707511a")
    module._read_descriptor = lambda *_args: seed
    emitted = module._emit_public_key([
        "--private-key-fd", "3",
        "--expected-public-key-sha256", hashlib.sha256(public).hexdigest(),
    ])
    if emitted != public:
        raise SystemExit(3)
else:
    from scripts.ops import personal_dev_native_builder_runtime_authority_material_client
"""

    completed = subprocess.run(
        [sys.executable, "-I", "-c", child],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


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
        "docs/architecture/2026-08-31-personal-dev-native-runtime-authority-design.md",
        "docs/architecture/2026-09-01-personal-dev-native-operator-material-authority-design.md",
        "docs/implementation-plans/2026-09-01-personal-dev-native-operator-material-authority.md",
        "docs/runbooks/personal-dev-native-builder-acceptance.md",
        "docs/runbooks/personal-dev-native-builder-runtime.md",
        "scripts/ops/converge_personal_dev_native_builder_release.py",
        "scripts/ops/install_personal_dev_native_builder_runtime.py",
        "scripts/ops/install_personal_dev_native_builder_runtime_authority.py",
        "scripts/ops/personal_dev_native_builder_conformance.py",
        "scripts/ops/personal_dev_native_builder_runtime_crypto.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_material_client.py",
        "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
        "scripts/ops/personal_dev_native_builder_runtime_profile.py",
        "tests/ops/test_install_personal_dev_native_builder_runtime_authority.py",
        "tests/ops/test_personal_dev_native_builder_runbooks.py",
        "tests/ops/test_personal_dev_native_builder_runtime_authority.py",
        "deploy/personal-dev-native-builder/runtime-profile-v1.json",
        "deploy/worker-pools/gb10/README.md",
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
        "authority_client",
        "broker",
        "conformance",
        "converger",
        "crypto_helper",
        "installer",
        "launcher",
        "material_client",
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
    material_client = _installed(
        host_root,
        "/usr/local/libexec/"
        "loom-personal-dev-native-builder-runtime-authority-material-client",
    )
    broker = _installed(
        host_root,
        "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/scripts/ops/"
        "personal_dev_native_builder_runtime_authority.py",
    )
    assert stat.S_IMODE(launcher.stat().st_mode) == 0o555
    assert stat.S_IMODE(material_client.stat().st_mode) == 0o555
    assert stat.S_IMODE(broker.stat().st_mode) == 0o444
    for path in host_root.rglob("*"):
        if path.is_file() and path not in {launcher, material_client}:
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


def test_operator_bootstrap_installs_only_sealed_material_client_inventory(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches installing any GB10 runtime authority or sudo rule on OLDLAB."""
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )

    first = module.bootstrap_operator_material(source_sha, source_tree)
    after_first = _tree_snapshot(host_root)
    second = module.bootstrap_operator_material(source_sha, source_tree)

    assert first == {
        "asset_sha256": first["asset_sha256"],
        "changed": True,
        "policy_sha256": first["policy_sha256"],
        "source_base_sha": module.APPROVED_BASE_SHA,
        "source_sha": source_sha,
        "source_tree_sha": source_tree,
        "status": "ok",
        "target": "operator-material",
    }
    assert set(first["asset_sha256"]) == {
        "authority_client",
        "crypto_helper",
        "launcher",
        "material_client",
        "protocol",
    }
    assert second == {**first, "changed": False}
    assert _tree_snapshot(host_root) == after_first

    installed_ops = _installed(
        host_root,
        "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/scripts/ops",
    )
    assert {path.name for path in installed_ops.iterdir()} == {
        "personal_dev_native_builder_runtime_authority_client.py",
        "personal_dev_native_builder_runtime_authority_protocol.py",
        "personal_dev_native_builder_runtime_crypto.py",
    }
    assert stat.S_IMODE(
        _installed(
            host_root,
            "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
        ).stat().st_mode
    ) == 0o555
    assert stat.S_IMODE(
        _installed(
            host_root,
            "/usr/local/libexec/"
            "loom-personal-dev-native-builder-runtime-authority-material-client",
        ).stat().st_mode
    ) == 0o555
    installed_assets = {
        "authority_client": installed_ops
        / "personal_dev_native_builder_runtime_authority_client.py",
        "crypto_helper": installed_ops
        / "personal_dev_native_builder_runtime_crypto.py",
        "launcher": _installed(
            host_root,
            "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
        ),
        "material_client": _installed(
            host_root,
            "/usr/local/libexec/"
            "loom-personal-dev-native-builder-runtime-authority-material-client",
        ),
        "protocol": installed_ops
        / "personal_dev_native_builder_runtime_authority_protocol.py",
    }
    asset_digests = first["asset_sha256"]
    assert isinstance(asset_digests, dict)
    assert asset_digests == {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in installed_assets.items()
    }

    policy_path = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-operator-material-authority.json",
    )
    policy = json.loads(policy_path.read_text(encoding="ascii"))
    assert policy == {
        "asset_sha256": first["asset_sha256"],
        "authority_source_sha": source_sha,
        "authority_source_tree": source_tree,
        "schema": "loom.personal-dev-native-builder-operator-material-authority-policy.v1",
    }
    assert stat.S_IMODE(policy_path.stat().st_mode) == 0o444
    assert first["policy_sha256"] == hashlib.sha256(policy_path.read_bytes()).hexdigest()

    material_root = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-authority-material",
    )
    assert stat.S_IMODE(material_root.stat().st_mode) == 0o700
    assert tuple(material_root.iterdir()) == ()

    for forbidden in (
        "/etc/sudoers.d/loom-personal-dev-native-builder-runtime-authority",
        "/usr/lib/tmpfiles.d/"
        "loom-personal-dev-native-builder-runtime-authority.conf",
        "/var/lib/loom/personal-dev-native-builder-runtime-authority",
        "/run/lock/loom-personal-dev-native-builder-runtime-authority.lock",
        "/run/loom-personal-dev-native-builder-runtime-authority",
        "/usr/local/lib/loom/personal-dev-native-builder-runtime-authority/"
        "scripts/ops/personal_dev_native_builder_runtime_authority.py",
        "/usr/local/lib/loom/personal-dev-native-builder-runtime-authority/"
        "deploy/personal-dev-native-builder/runtime-profile-v1.json",
    ):
        assert not _installed(host_root, forbidden).exists()


@pytest.mark.parametrize(
    ("nodename", "machine"),
    (("gx10-01c7", "aarch64"), ("TRT-EAI-OLDLAB-1", "aarch64")),
    ids=("runtime-host", "wrong-architecture"),
)
def test_operator_bootstrap_rejects_non_oldlab_target_before_mutation(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    nodename: str,
    machine: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename=nodename, machine=machine),
    )
    before = _tree_snapshot(host_root)

    with pytest.raises(module.BootstrapError, match="target_host_required"):
        module.bootstrap_operator_material(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before


@pytest.mark.parametrize(
    "boundary",
    (
        "authority-client",
        "crypto-helper",
        "launcher",
        "material-client",
        "protocol",
        "policy-stage",
        "policy",
        "material-root",
        "installed-validation",
    ),
)
def test_operator_bootstrap_rolls_back_every_publication_boundary(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    target_paths = {
        "authority-client": _installed(
            host_root,
            "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/"
            "scripts/ops/personal_dev_native_builder_runtime_authority_client.py",
        ),
        "crypto-helper": _installed(
            host_root,
            "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/"
            "scripts/ops/personal_dev_native_builder_runtime_crypto.py",
        ),
        "launcher": _installed(
            host_root,
            "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
        ),
        "material-client": _installed(
            host_root,
            "/usr/local/libexec/"
            "loom-personal-dev-native-builder-runtime-authority-material-client",
        ),
        "protocol": _installed(
            host_root,
            "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/"
            "scripts/ops/personal_dev_native_builder_runtime_authority_protocol.py",
        ),
        "policy": _installed(
            host_root,
            "/etc/loom/personal-dev-native-builder-operator-material-authority.json",
        ),
        "policy-stage": _installed(
            host_root,
            "/etc/loom/"
            f".personal-dev-native-builder-operator-material-authority.json.validate-{os.getpid()}",
        ),
    }
    original_install = module._install_file
    original_ensure = module._ensure_directory

    def fail_after_file(
        path: Path,
        payload: bytes,
        mode: int,
        created: list[object],
    ) -> bool:
        changed = original_install(path, payload, mode, created)
        if target_paths.get(boundary) == path:
            raise module.BootstrapError("publication_failed")
        return changed

    def fail_after_directory(
        path: Path,
        mode: int,
        created: list[object],
    ) -> bool:
        changed = original_ensure(path, mode, created)
        if boundary == "material-root" and path == _installed(
            host_root,
            "/etc/loom/personal-dev-native-builder-authority-material",
        ):
            raise module.BootstrapError("publication_failed")
        return changed

    monkeypatch.setattr(module, "_install_file", fail_after_file)
    monkeypatch.setattr(module, "_ensure_directory", fail_after_directory)
    if boundary == "installed-validation":
        monkeypatch.setattr(
            module,
            "_validate_installed_policy",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                module.BootstrapError("installed_validation_failed")
            ),
        )

    before = _tree_snapshot(host_root)
    with pytest.raises(module.BootstrapError):
        module.bootstrap_operator_material(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before


def test_operator_policy_is_published_only_after_installed_validation(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches the callable material policy becoming visible before validation."""
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    policy_path = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-operator-material-authority.json",
    )
    material_root = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-authority-material",
    )
    original_install = module._install_file
    original_validate = module._validate_installed_policy
    events: list[str] = []

    def observe_policy_commit(
        path: Path,
        payload: bytes,
        mode: int,
        created: list[object],
    ) -> bool:
        if path == policy_path:
            assert events == ["validated"]
            assert material_root.is_dir()
            events.append("policy-committed")
        return original_install(path, payload, mode, created)

    def observe_validation(*args: object, **kwargs: object) -> None:
        assert not policy_path.exists()
        assert material_root.is_dir()
        original_validate(*args, **kwargs)
        events.append("validated")

    monkeypatch.setattr(module, "_install_file", observe_policy_commit)
    monkeypatch.setattr(module, "_validate_installed_policy", observe_validation)

    receipt = module.bootstrap_operator_material(source_sha, source_tree)

    assert receipt["status"] == "ok"
    assert events == ["validated", "policy-committed"]


def test_operator_bootstrap_never_opens_provisioned_material(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    module.bootstrap_operator_material(source_sha, source_tree)
    material_root = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-authority-material",
    )
    key = material_root / "agent-ed25519"
    ca = material_root / "service-ca.pem"
    key.write_bytes(b"k" * 32)
    ca.write_bytes(b"test certificate fixture")
    key.chmod(0o400)
    ca.chmod(0o444)
    original_open = module.os.open

    def reject_material_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if Path(os.fsdecode(path)).name in {"agent-ed25519", "service-ca.pem"}:
            raise AssertionError("bootstrap opened protected material")
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(module.os, "open", reject_material_open)

    receipt = module.bootstrap_operator_material(source_sha, source_tree)

    assert receipt["changed"] is False


@pytest.mark.parametrize(
    "drift",
    ("authority-client", "launcher", "material-root", "policy"),
)
def test_operator_bootstrap_preserves_preexisting_installed_drift(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    module.bootstrap_operator_material(source_sha, source_tree)
    targets = {
        "authority-client": (
            "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority/"
            "scripts/ops/personal_dev_native_builder_runtime_authority_client.py"
        ),
        "launcher": (
            "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
        ),
        "material-root": (
            "/etc/loom/personal-dev-native-builder-authority-material"
        ),
        "policy": (
            "/etc/loom/"
            "personal-dev-native-builder-operator-material-authority.json"
        ),
    }
    target = _installed(host_root, targets[drift])
    if drift == "material-root":
        target.chmod(0o755)
    else:
        target.chmod(0o644)
        target.write_bytes(b"preexisting root drift\n")
        target.chmod(0o555 if drift == "launcher" else 0o444)

    with pytest.raises(module.BootstrapError, match="installed_drift"):
        module.bootstrap_operator_material(source_sha, source_tree)

    if drift == "material-root":
        assert stat.S_IMODE(target.stat().st_mode) == 0o755
    else:
        assert target.read_bytes() == b"preexisting root drift\n"


def test_operator_rollback_preserves_replacement_material_directory(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    material_root = _installed(
        host_root,
        "/etc/loom/personal-dev-native-builder-authority-material",
    )
    displaced = host_root / "displaced-operator-material-root"

    def replace_then_fail(*_args: object, **_kwargs: object) -> None:
        os.rename(material_root, displaced)
        material_root.mkdir(mode=0o700)
        raise module.BootstrapError("installed_validation_failed")

    monkeypatch.setattr(module, "_validate_installed_policy", replace_then_fail)

    with pytest.raises(module.BootstrapError, match="installed_validation_failed"):
        module.bootstrap_operator_material(source_sha, source_tree)

    assert material_root.is_dir()
    assert material_root.stat().st_ino != displaced.stat().st_ino


def test_operator_bootstrap_rejects_unsafe_source_before_installing(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    source = module.SOURCE_ROOT / (
        "scripts/ops/"
        "personal_dev_native_builder_runtime_authority_material_client.py"
    )
    source.chmod(0o664)
    before = _tree_snapshot(host_root)

    with pytest.raises(module.BootstrapError, match="sealed_source_invalid"):
        module.bootstrap_operator_material(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before


@pytest.mark.parametrize(
    "inventory_override",
    (
        "ASSET_SPECS",
        (
            "MappingProxyType({**OPERATOR_MATERIAL_ASSET_SPECS, "
            "'authority_client': ASSET_SPECS['protocol'], "
            "'protocol': ASSET_SPECS['authority_client']})"
        ),
    ),
    ids=("full-runtime", "swapped-paths"),
)
def test_operator_bootstrap_rejects_nonexact_inventory_before_publication(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    inventory_override: str,
) -> None:
    """Catches a nonexact inventory reaching any OLDLAB publication boundary."""
    module, host_root, _source_sha, _source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename="TRT-EAI-OLDLAB-1", machine="x86_64"),
    )
    launcher = module.SOURCE_ROOT / (
        "scripts/ops/"
        "personal_dev_native_builder_runtime_authority_launcher.py"
    )
    launcher.write_text(
        launcher.read_text(encoding="utf-8")
        + "\nOPERATOR_MATERIAL_ASSET_SPECS = "
        + inventory_override
        + "\n",
        encoding="utf-8",
    )
    launcher.chmod(0o644)
    _git(module.SOURCE_ROOT, "add", str(launcher.relative_to(module.SOURCE_ROOT)))
    _git(module.SOURCE_ROOT, "commit", "-m", "malformed operator inventory")
    source_sha = _git(module.SOURCE_ROOT, "rev-parse", "HEAD")
    source_tree = _git(module.SOURCE_ROOT, "rev-parse", "HEAD^{tree}")

    def reject_publication(*_args: object, **_kwargs: object) -> bool:
        raise AssertionError("operator inventory reached publication")

    monkeypatch.setattr(module, "_install_file", reject_publication)
    before = _tree_snapshot(host_root)

    with pytest.raises(module.BootstrapError, match="source_inventory_invalid"):
        module.bootstrap_operator_material(source_sha, source_tree)

    assert _tree_snapshot(host_root) == before


def test_operator_bootstrap_rejects_unsafe_root_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    monkeypatch.setattr(module.os, "getresuid", lambda: (0, 0, 0))
    monkeypatch.setattr(module.os, "getresgid", lambda: (0, 0, 0))
    monkeypatch.setenv("SUDO_UID", "1000")

    with pytest.raises(module.BootstrapError, match="direct_root_required"):
        module.bootstrap_operator_material("1" * 40, "2" * 40)


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


@pytest.mark.parametrize(
    ("bootstrap_name", "nodename", "machine"),
    (
        ("bootstrap", "gx10-01c7", "aarch64"),
        (
            "bootstrap_operator_material",
            "TRT-EAI-OLDLAB-1",
            "x86_64",
        ),
    ),
    ids=("runtime", "operator-material"),
)
def test_source_capture_rejects_identity_change_between_lstat_and_open(
    authority_install: tuple[ModuleType, Path, str, str],
    monkeypatch: pytest.MonkeyPatch,
    bootstrap_name: str,
    nodename: str,
    machine: str,
) -> None:
    module, host_root, source_sha, source_tree = authority_install
    monkeypatch.setattr(
        module.os,
        "uname",
        lambda: SimpleNamespace(nodename=nodename, machine=machine),
    )
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
        getattr(module, bootstrap_name)(source_sha, source_tree)

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


def test_cli_dispatches_only_the_explicit_operator_material_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    calls: list[tuple[str, str]] = []

    def operator(source_sha: str, source_tree_sha: str) -> dict[str, object]:
        calls.append((source_sha, source_tree_sha))
        return {"status": "ok", "target": "operator-material"}

    monkeypatch.setattr(module, "bootstrap_operator_material", operator, raising=False)

    assert (
        module.main(
            [
                "--source-sha",
                "1" * 40,
                "--source-tree-sha",
                "2" * 40,
                "--target",
                "operator-material",
            ]
        )
        == 0
    )
    assert calls == [("1" * 40, "2" * 40)]
    assert json.loads(capsys.readouterr().out) == {
        "status": "ok",
        "target": "operator-material",
    }
