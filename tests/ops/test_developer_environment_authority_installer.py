from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import pytest
from scripts.ops import developer_environment_authority_installer as installer

SHA_ONE = "a" * 40
TREE_ONE = "b" * 40
SHA_TWO = "c" * 40
TREE_TWO = "d" * 40


@pytest.fixture
def filesystem_root() -> Path:
    path = Path(tempfile.mkdtemp(prefix="loom-ea-", dir="/tmp"))
    os.chown(path, os.getuid(), os.getgid())
    try:
        yield path
    finally:
        shutil.rmtree(path)


def _candidate_source(tmp_path: Path, name: str) -> Path:
    source = tmp_path / name
    repository = Path(__file__).resolve().parents[2]
    paths = {relative for relative, _destination, _mode in installer.EXPECTED_ASSETS} | {
        installer.MANIFEST_RELATIVE.as_posix(),
        installer.NODE_CAPACITY_CONTRACT_RELATIVE.as_posix(),
    }
    for relative in paths:
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repository / relative, destination)
    return source


class FakeHostCommands:
    def __init__(self, filesystem_root: Path, group_gid: int) -> None:
        self.filesystem_root = filesystem_root
        self.group_gid = group_gid
        self.calls: list[tuple[str, ...]] = []
        self.disabled_units: set[str] = set()

    def _host(self, path: Path) -> Path:
        return self.filesystem_root / path.relative_to("/")

    def _runtime_state(self) -> None:
        database = self._host(installer.REGISTRY_DATABASE)
        database.parent.mkdir(parents=True, exist_ok=True)
        if not database.exists():
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    """
                    CREATE TABLE deployments (
                        deployment_id TEXT PRIMARY KEY,
                        applied_resource_generation INTEGER,
                        applied_registry_generation INTEGER,
                        applied_registry_payload_sha256 TEXT,
                        finalization_payload_sha256 TEXT,
                        phase TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE deployment_finalizations (
                        deployment_id TEXT PRIMARY KEY,
                        payload_json TEXT NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                connection.commit()
            finally:
                connection.close()
        database.chmod(0o600)

        unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-environment.registry-snapshot",
            "generation": 1,
            "environments": [],
            "candidates": [],
            "deployments": [],
            "deployment_finalizations": [],
        }
        snapshot = {
            **unsigned,
            "payload_sha256": installer._digest(installer._canonical(unsigned)),
        }
        snapshot_path = self._host(installer.REGISTRY_SNAPSHOT)
        snapshot_path.write_bytes(installer._canonical(snapshot))
        snapshot_path.chmod(0o600)

        runtime = self._host(installer.AUTHORITY_RUNTIME_ROOT)
        runtime.mkdir(parents=True, exist_ok=True)
        os.chown(runtime, os.getuid(), self.group_gid)
        runtime.chmod(0o755)
        endpoint = self._host(installer.AUTHORITY_SOCKET)
        endpoint.unlink(missing_ok=True)
        bound = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            bound.bind(str(endpoint))
        finally:
            bound.close()
        os.chown(endpoint, os.getuid(), self.group_gid)
        endpoint.chmod(0o660)

    def __call__(
        self,
        argv: tuple[str, ...] | list[str],
    ) -> subprocess.CompletedProcess[bytes]:
        command = tuple(argv)
        self.calls.append(command)
        if (
            command[:3] == ("/usr/bin/systemctl", "is-enabled", "--quiet")
            and command[-1] in self.disabled_units
        ):
            return subprocess.CompletedProcess(command, 1, b"", b"disabled\n")
        if command[:2] == ("/usr/bin/id", "-nG"):
            return subprocess.CompletedProcess(
                command,
                0,
                b"sharedwork loom-developers\n",
                b"",
            )
        if command[-1:] == ("status",):
            payload = {
                "schema_version": 1,
                "action": "status",
                "status": "succeeded",
            }
            return subprocess.CompletedProcess(
                command,
                0,
                installer._canonical(payload),
                b"",
            )
        if command == (str(installer.NODE_TRANSPORT), "check-client"):
            return subprocess.CompletedProcess(
                command,
                0,
                installer._canonical(
                    {
                        "schema_version": 1,
                        "action": "check-client",
                        "initiator": installer.NODE,
                        "roles": ["oldlab-check", "gb10-check"],
                        "public_key_fingerprints": {
                            "oldlab-check": "SHA256:test-oldlab",
                            "gb10-check": "SHA256:test-gb10",
                        },
                        "status": "succeeded",
                    }
                ),
                b"",
            )
        if (
            command[:3] == ("/usr/bin/systemctl", "enable", "--now")
            or command[:2] == ("/usr/bin/systemctl", "restart")
            or command[-1:] in {("init",), ("import-seed",)}
        ):
            self._runtime_state()
        return subprocess.CompletedProcess(command, 0, b"", b"")


def _authority_installer(
    *,
    filesystem_root: Path,
    source_root: Path,
    runner: FakeHostCommands,
) -> installer.AuthorityInstaller:
    uid = os.getuid()
    gid = os.getgid()
    transport = filesystem_root / installer.NODE_TRANSPORT.relative_to("/")
    transport.parent.mkdir(parents=True, exist_ok=True)
    transport.write_bytes(b"#!/bin/sh\nexit 0\n")
    transport.chmod(0o755)
    os.chown(transport, uid, gid)
    capacity_contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    capacity_contract.parent.mkdir(parents=True, exist_ok=True)
    capacity_contract.write_bytes(
        (source_root / installer.NODE_CAPACITY_CONTRACT_RELATIVE).read_bytes()
    )
    capacity_contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)
    os.chown(capacity_contract, uid, gid)
    node_policy = filesystem_root / installer.NODE_AUTHORITY_POLICY.relative_to("/")
    node_policy.parent.mkdir(parents=True, exist_ok=True)
    node_policy.write_bytes(
        installer._canonical(
            {
                "schema_version": installer.SCHEMA_VERSION,
                "source_sha": SHA_TWO if "candidate-two" in source_root.name else SHA_ONE,
                "source_tree": TREE_TWO if "candidate-two" in source_root.name else TREE_ONE,
                "node": installer.NODE,
                "asset_sha256": {
                    installer.NODE_CAPACITY_CONTRACT_RELATIVE.as_posix(): installer._digest(
                        capacity_contract.read_bytes()
                    )
                },
            }
        )
    )
    node_policy.chmod(0o600)
    os.chown(node_policy, uid, gid)
    value = installer.AuthorityInstaller(
        filesystem_root=filesystem_root,
        source_root=source_root,
        runner=runner,
        expected_uid=uid,
        expected_gid=gid,
        group_resolver=lambda name: types.SimpleNamespace(
            gr_name=name,
            gr_gid=gid,
        ),
        account_resolver=lambda name: types.SimpleNamespace(
            pw_name=name,
            pw_uid={"qianyi": 2005, "hongjian": 2006, "devansh": 2012}[name],
        ),
    )
    value._verify_candidate = lambda _sha, _tree: None  # type: ignore[method-assign]
    return value


def test_installer_preflight_refuses_legacy_committed_rows_without_ddl(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-preflight")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    database = filesystem_root / installer.REGISTRY_DATABASE.relative_to("/")
    database.parent.mkdir(parents=True)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            CREATE TABLE deployments (
                deployment_id TEXT PRIMARY KEY,
                phase TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO deployments VALUES (?, 'committed')",
            ("dep-" + "1" * 32,),
        )
        connection.commit()
    finally:
        connection.close()
    database.chmod(0o600)
    before = database.read_bytes()

    with pytest.raises(
        installer.AuthorityInstallerError,
        match="legacy committed finalization migration required",
    ):
        authority._preflight_registry_finalization_schema()

    assert database.read_bytes() == before
    connection = sqlite3.connect(database)
    try:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    finally:
        connection.close()
    assert tables == {"deployments"}


def test_bootstrap_replay_and_readback_install_closed_fixed_assets(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    node_contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    node_contract_before = node_contract.stat()
    node_contract_payload = node_contract.read_bytes()

    first = authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    replay = authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    readback = authority.install(
        action="environment-authority-readback",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )

    assert first["status"] == "succeeded"
    assert first["node"] == "oldlab-2"
    assert first["idempotent"] is False
    assert replay["idempotent"] is True
    assert readback["action"] == "environment-authority-readback"
    assert first["installed_asset_digests"] == readback["installed_asset_digests"]
    assert first["node_capacity_contract_sha256"] == installer._digest(
        (source / installer.NODE_CAPACITY_CONTRACT_RELATIVE).read_bytes()
    )
    assert first["node_capacity_contract_sha256"] == readback["node_capacity_contract_sha256"]
    assert first["registry_snapshot_sha256"] == readback["registry_snapshot_sha256"]
    assert str(installer.NODE_CAPACITY_CONTRACT) not in installer.FIXED_DESTINATIONS
    assert str(installer.NODE_CAPACITY_CONTRACT) not in installer.MANAGED_DESTINATIONS
    assert node_contract.read_bytes() == node_contract_payload
    assert node_contract.stat().st_ino == node_contract_before.st_ino
    assert stat.S_IMODE(node_contract.stat().st_mode) == installer.NODE_CAPACITY_CONTRACT_MODE
    for relative, destination, rendered_mode in installer.EXPECTED_ASSETS:
        installed = filesystem_root / destination.removeprefix("/")
        assert installed.read_bytes() == (source / relative).read_bytes()
        assert stat.S_IMODE(installed.stat().st_mode) == int(
            rendered_mode,
            8,
        )
    capacity_state = filesystem_root / installer.CAPACITY_RUNTIME_STATE.relative_to("/")
    assert capacity_state.read_bytes() == installer.INITIAL_CAPACITY_RUNTIME_STATE
    assert (
        filesystem_root / "usr/local/libexec/loom-developer-environment/"
        "developer_environment_authority_installer.py"
    ).is_file()
    journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"prepared"' in journal
    assert '"phase":"committed"' in journal
    assert "HOME" not in installer._clean_env()
    for unit in (installer.SOCKET_UNIT, installer.RENEWAL_TIMER_UNIT):
        assert ("/usr/bin/systemctl", "is-enabled", "--quiet", unit) in commands.calls
    authority_unit = (
        filesystem_root / "etc/systemd/system/loom-developer-environment-authority.service"
    ).read_text(encoding="utf-8")
    assert "ReadWritePaths=-/var/lib/loom-developer-environment-runtime" in authority_unit
    assert "ReadWritePaths=-/var/lib\n" not in authority_unit
    assert "ProtectProc=invisible" in authority_unit
    assert "ProcSubset=all" in authority_unit
    assert "ProcSubset=pid" not in authority_unit
    assert "ReadOnlyPaths=/var/run/docker.sock" in authority_unit
    assert "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6" in authority_unit


@pytest.mark.parametrize(
    "drift",
    (
        "missing",
        "content",
        "mode",
        "symlink",
        "hardlink",
        "unsafe-ancestor",
        "policy-candidate",
    ),
)
def test_node_owned_capacity_contract_prerequisite_fails_before_transaction(
    tmp_path: Path,
    filesystem_root: Path,
    drift: str,
) -> None:
    source = _candidate_source(tmp_path, f"candidate-{drift}")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    if drift == "missing":
        contract.unlink()
    elif drift == "content":
        contract.write_bytes(b"drifted\n")
        contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)
    elif drift == "mode":
        contract.chmod(0o600)
    elif drift == "symlink":
        contract.unlink()
        contract.symlink_to(source / installer.NODE_CAPACITY_CONTRACT_RELATIVE)
    elif drift == "hardlink":
        peer = contract.with_name("developer_sandbox_capacity_contract.peer")
        contract.rename(peer)
        os.link(peer, contract)
    elif drift == "unsafe-ancestor":
        contract.parent.chmod(0o775)
    else:
        policy = filesystem_root / installer.NODE_AUTHORITY_POLICY.relative_to("/")
        payload = installer.json.loads(policy.read_bytes())
        payload["source_sha"] = SHA_TWO
        policy.write_bytes(installer._canonical(payload))
        policy.chmod(0o600)

    with pytest.raises(installer.AuthorityInstallerError):
        authority.install(
            action="environment-authority-bootstrap",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )

    assert not (filesystem_root / installer.ACTIVE_PATH.relative_to("/")).exists()
    assert not (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).exists()
    assert not (filesystem_root / installer.INSTALLED_PATH.relative_to("/")).exists()
    assert all(
        not (filesystem_root / Path(destination).relative_to("/")).exists()
        for destination in installer.FIXED_DESTINATIONS
    )


def test_readback_rejects_node_capacity_contract_drift(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    contract.write_bytes(b"drifted after install\n")
    contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)

    with pytest.raises(installer.AuthorityInstallerError, match="prerequisite drifted"):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_upgrade_prerequisite_drift_preserves_old_environment_then_resumes(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    installed_path = filesystem_root / installer.INSTALLED_PATH.relative_to("/")
    journal_path = filesystem_root / installer.JOURNAL_PATH.relative_to("/")
    old_installed = installed_path.read_bytes()
    old_journal = journal_path.read_bytes()

    second_source = _candidate_source(tmp_path, "candidate-two")
    second = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    contract.write_bytes(b"drifted before upgrade\n")
    contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)

    with pytest.raises(installer.AuthorityInstallerError, match="prerequisite drifted"):
        second.install(
            action="environment-authority-upgrade",
            candidate_sha=SHA_TWO,
            candidate_tree=TREE_TWO,
        )

    assert installed_path.read_bytes() == old_installed
    assert journal_path.read_bytes() == old_journal
    assert not (filesystem_root / installer.ACTIVE_PATH.relative_to("/")).exists()

    contract.write_bytes((second_source / installer.NODE_CAPACITY_CONTRACT_RELATIVE).read_bytes())
    contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)
    resumed = second.install(
        action="environment-authority-upgrade",
        candidate_sha=SHA_TWO,
        candidate_tree=TREE_TWO,
    )
    assert resumed["source_sha"] == SHA_TWO
    assert resumed["status"] == "succeeded"


def test_prerequisite_race_rolls_back_before_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    contract = filesystem_root / installer.NODE_CAPACITY_CONTRACT.relative_to("/")
    validate = authority._validate_node_capacity_contract_prerequisite
    calls = 0

    def race(*, candidate_sha: str, candidate_tree: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            contract.write_bytes(b"raced during environment transaction\n")
            contract.chmod(installer.NODE_CAPACITY_CONTRACT_MODE)
        return validate(candidate_sha=candidate_sha, candidate_tree=candidate_tree)

    monkeypatch.setattr(
        authority,
        "_validate_node_capacity_contract_prerequisite",
        race,
    )
    with pytest.raises(installer.AuthorityInstallerError, match="prerequisite drifted"):
        authority.install(
            action="environment-authority-bootstrap",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )

    assert not (filesystem_root / installer.ACTIVE_PATH.relative_to("/")).exists()
    assert not (filesystem_root / installer.INSTALLED_PATH.relative_to("/")).exists()
    journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"rolled-back"' in journal
    assert '"phase":"committed"' not in journal
    assert all(
        not (filesystem_root / Path(destination).relative_to("/")).exists()
        for destination in installer.FIXED_DESTINATIONS
    )


def test_readback_rejects_group_writable_installed_import_ancestor(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-writable-import-ancestor")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    import_root = filesystem_root / "usr/local/libexec/scripts"
    import_root.chmod(0o775)

    with pytest.raises(
        installer.AuthorityInstallerError,
        match="import ancestry is unsafe",
    ):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_readback_rejects_untracked_import_bytecode_inventory(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-bytecode-inventory")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    cache = filesystem_root / "usr/local/libexec/scripts/ops/__pycache__"
    cache.mkdir(mode=0o700)
    (cache / "developer_environment_registry.cpython-311.pyc").write_bytes(b"untracked")

    with pytest.raises(
        installer.AuthorityInstallerError,
        match="import inventory is not closed",
    ):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_upgrade_retires_closed_legacy_bytecode_caches(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    for relative, name in (
        (
            "usr/local/libexec/loom-developer-environment/__pycache__",
            "developer_environment_authority.cpython-312.pyc",
        ),
        (
            "usr/local/libexec/scripts/ops/__pycache__",
            "developer_environment_registry.cpython-312.pyc",
        ),
    ):
        cache = filesystem_root / relative
        cache.mkdir(mode=0o755)
        bytecode = cache / name
        bytecode.write_bytes(b"derived bytecode")
        bytecode.chmod(0o644)

    second_source = _candidate_source(tmp_path, "candidate-two")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    report = upgraded.install(
        action="environment-authority-upgrade",
        candidate_sha=SHA_TWO,
        candidate_tree=TREE_TWO,
    )

    assert report["status"] == "succeeded"
    assert all(
        not (filesystem_root / path.relative_to("/")).exists()
        for path in installer.LEGACY_BYTECODE_CACHE_DIRECTORIES
    )
    journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"legacy-bytecode-cache-retired"' in journal
    assert "developer_environment_authority.cpython-312.pyc" in journal
    assert "developer_environment_registry.cpython-312.pyc" in journal


def test_upgrade_rejects_unknown_legacy_cache_entry_without_removing_it(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    safe_cache = filesystem_root / "usr/local/libexec/loom-developer-environment/__pycache__"
    safe_cache.mkdir(mode=0o755)
    safe_bytecode = safe_cache / "developer_environment_authority.cpython-312.pyc"
    safe_bytecode.write_bytes(b"derived bytecode")
    safe_bytecode.chmod(0o644)
    unsafe_cache = filesystem_root / "usr/local/libexec/scripts/ops/__pycache__"
    unsafe_cache.mkdir(mode=0o755)
    unknown = unsafe_cache / "operator-note"
    unknown.write_bytes(b"preserve me")
    unknown.chmod(0o644)

    second_source = _candidate_source(tmp_path, "candidate-two")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    with pytest.raises(
        installer.AuthorityInstallerError,
        match="legacy bytecode cache is not closed",
    ):
        upgraded.install(
            action="environment-authority-upgrade",
            candidate_sha=SHA_TWO,
            candidate_tree=TREE_TWO,
        )

    assert unknown.read_bytes() == b"preserve me"
    assert safe_bytecode.read_bytes() == b"derived bytecode"
    journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"legacy-bytecode-cache-retirement-prepared"' not in journal


def test_upgrade_rejects_cache_behind_symlink_ancestor_without_external_deletion(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    runtime_root = filesystem_root / "usr/local/libexec/loom-runtime-python"
    preserved = tmp_path / "preserved-runtime"
    runtime_root.rename(preserved)
    external = tmp_path / "external-runtime"
    cache = external / "loom_control_plane/__pycache__"
    cache.mkdir(parents=True, mode=0o755)
    sentinel = cache / "__init__.cpython-312.pyc"
    sentinel.write_bytes(b"outside authority")
    sentinel.chmod(0o644)
    runtime_root.symlink_to(external, target_is_directory=True)

    second_source = _candidate_source(tmp_path, "candidate-two")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    with pytest.raises(
        installer.AuthorityInstallerError,
        match="import ancestry is unsafe",
    ):
        upgraded.install(
            action="environment-authority-upgrade",
            candidate_sha=SHA_TWO,
            candidate_tree=TREE_TWO,
        )

    assert sentinel.read_bytes() == b"outside authority"


def test_interrupted_bytecode_retirement_is_journaled_and_retry_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    cache = filesystem_root / "usr/local/libexec/loom-developer-environment/__pycache__"
    cache.mkdir(mode=0o755)
    bytecode = cache / "developer_environment_authority.cpython-312.pyc"
    bytecode.write_bytes(b"derived bytecode")
    bytecode.chmod(0o644)
    second_source = _candidate_source(tmp_path, "candidate-two")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    real_unlink = Path.unlink

    class SimulatedCrash(BaseException):
        pass

    def crash_after_unlink(path: Path, *args: object, **kwargs: object) -> None:
        real_unlink(path, *args, **kwargs)
        if path == bytecode:
            raise SimulatedCrash

    with monkeypatch.context() as scoped:
        scoped.setattr(Path, "unlink", crash_after_unlink)
        with pytest.raises(SimulatedCrash):
            upgraded.install(
                action="environment-authority-upgrade",
                candidate_sha=SHA_TWO,
                candidate_tree=TREE_TWO,
            )

    interrupted_journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"legacy-bytecode-cache-retirement-prepared"' in interrupted_journal
    assert '"phase":"legacy-bytecode-cache-retired"' not in interrupted_journal

    report = upgraded.install(
        action="environment-authority-upgrade",
        candidate_sha=SHA_TWO,
        candidate_tree=TREE_TWO,
    )

    assert report["status"] == "succeeded"
    assert not cache.exists()
    converged_journal = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_text(
        encoding="ascii"
    )
    assert '"phase":"legacy-bytecode-cache-retired"' in converged_journal


def test_bytecode_retirement_requires_complete_prepared_journal_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    cache = filesystem_root / "usr/local/libexec/loom-developer-environment/__pycache__"
    cache.mkdir(mode=0o755)
    bytecode = cache / "developer_environment_authority.cpython-312.pyc"
    bytecode.write_bytes(b"derived bytecode")
    bytecode.chmod(0o644)
    second_source = _candidate_source(tmp_path, "candidate-two")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    monkeypatch.setattr(installer.os, "write", lambda _descriptor, _payload: 0)

    with pytest.raises(
        installer.AuthorityInstallerError,
        match="journal write failed",
    ):
        upgraded.install(
            action="environment-authority-upgrade",
            candidate_sha=SHA_TWO,
            candidate_tree=TREE_TWO,
        )

    assert bytecode.read_bytes() == b"derived bytecode"


def test_append_journal_retries_short_regular_file_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-short-journal-write")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority._ensure_directory(installer.STATE_ROOT, 0o700)
    real_write = os.write
    writes: list[int] = []

    def short_write(descriptor: int, payload: object) -> int:
        view = memoryview(payload)
        limit = max(1, len(view) // 3)
        writes.append(limit)
        return real_write(descriptor, view[:limit])

    monkeypatch.setattr(installer.os, "write", short_write)
    authority._append_journal(
        {
            "action": "test-short-write",
            "phase": "complete",
            "payload": "x" * 8192,
        }
    )

    raw = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_bytes()
    event = json.loads(raw)
    assert raw == installer._canonical(event)
    assert len(writes) > 1


def test_append_journal_recovers_partial_event_after_process_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-crashed-journal-write")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority._ensure_directory(installer.STATE_ROOT, 0o700)
    authority._append_journal(
        {
            "action": "baseline",
            "phase": "complete",
        }
    )
    real_write = os.write
    crashed = False

    class SimulatedCrash(BaseException):
        pass

    def partial_then_crash(descriptor: int, payload: object) -> int:
        nonlocal crashed
        if not crashed:
            crashed = True
            view = memoryview(payload)
            real_write(descriptor, view[: max(1, len(view) // 2)])
            raise SimulatedCrash
        return real_write(descriptor, payload)

    with monkeypatch.context() as scoped:
        scoped.setattr(installer.os, "write", partial_then_crash)
        with pytest.raises(SimulatedCrash):
            authority._append_journal(
                {
                    "action": "interrupted",
                    "phase": "prepared",
                    "payload": "x" * 8192,
                }
            )

    authority._append_journal(
        {
            "action": "retry",
            "phase": "complete",
        }
    )

    raw = (filesystem_root / installer.JOURNAL_PATH.relative_to("/")).read_bytes()
    lines = raw.splitlines(keepends=True)
    assert len(lines) == 2
    for line in lines:
        event = json.loads(line)
        assert line == installer._canonical(event)
    assert [json.loads(line)["action"] for line in lines] == ["baseline", "retry"]


@pytest.mark.parametrize(
    "relative",
    (
        "scripts/ops/developer_environment_runtime_authority.py",
        "scripts/ops/shared_capacity_supervisor.py",
    ),
)
def test_root_python_entrypoints_ignore_hostile_import_environment_from_clean_cwd(
    relative: str,
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[2]
    hostile = tmp_path / "hostile"
    hostile.mkdir()
    marker = tmp_path / "sitecustomize-ran"
    user_site = (
        hostile
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    for import_root in (hostile, user_site):
        import_root.mkdir(parents=True, exist_ok=True)
        (import_root / "sitecustomize.py").write_text(
            f"from pathlib import Path\nPath({str(marker)!r}).write_text('unsafe')\n",
            encoding="utf-8",
        )
        fake_package = import_root / "scripts/ops"
        fake_package.mkdir(parents=True)
        (import_root / "scripts/__init__.py").write_text("", encoding="utf-8")
        (fake_package / "__init__.py").write_text("", encoding="utf-8")
        (fake_package / "developer_environment_registry.py").write_text(
            "raise RuntimeError('hostile import root')\n",
            encoding="utf-8",
        )
    for cwd in (Path("/"), hostile):
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-B",
                str(repository / relative),
                "--help",
            ],
            cwd=cwd,
            env={
                "PATH": "/usr/bin:/bin",
                "HOME": str(hostile),
                "PYTHONPATH": str(hostile),
                "PYTHONUSERBASE": str(hostile),
            },
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert completed.returncode == 0, completed.stderr
        assert "hostile import root" not in completed.stderr
    assert not marker.exists()


def test_runtime_wrapper_and_supervisor_service_use_exact_isolated_python() -> None:
    repository = Path(__file__).resolve().parents[2]
    wrapper = (
        repository / "deploy/developer-sandboxes/loom-developer-environment-runtime-authority"
    ).read_text(encoding="ascii")
    unit = (
        repository / "deploy/developer-sandboxes/loom-developer-shared-capacity-supervisor.service"
    ).read_text(encoding="ascii")

    assert (
        "exec /usr/bin/python3 -I -B \\\n"
        "  /usr/local/libexec/scripts/ops/developer_environment_runtime_authority.py"
    ) in wrapper
    assert "PYTHONPATH" not in unit
    assert (
        "ExecStart=/usr/bin/python3 -I -B "
        "/usr/local/libexec/scripts/ops/shared_capacity_supervisor.py "
        "--config /etc/loom/shared-capacity-supervisor.toml run"
    ) in unit
    assert (
        "scripts/ops/developer_environment_runtime_authority.py",
        "/usr/local/libexec/scripts/ops/developer_environment_runtime_authority.py",
        "0444",
    ) in installer.EXPECTED_ASSETS
    assert (
        "deploy/developer-sandboxes/loom-developer-environment-runtime-authority",
        "/usr/local/libexec/loom-developer-environment-runtime-authority",
        "0555",
    ) in installer.EXPECTED_ASSETS


def test_bootstrap_fails_before_enable_when_fixed_transport_is_missing(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "missing-transport")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    (filesystem_root / installer.NODE_TRANSPORT.relative_to("/")).unlink()

    with pytest.raises(
        installer.AuthorityInstallerError,
        match="installer file is unavailable",
    ):
        authority.install(
            action="environment-authority-bootstrap",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )

    assert not any(call[:3] == ("/usr/bin/systemctl", "enable", "--now") for call in commands.calls)


@pytest.mark.parametrize(
    "unit",
    (installer.SOCKET_UNIT, installer.RENEWAL_TIMER_UNIT),
)
def test_readback_rejects_active_but_disabled_persistent_unit(
    unit: str,
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    commands.disabled_units.add(unit)

    with pytest.raises(installer.AuthorityInstallerError):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_capacity_wrapper_and_fixed_policy_share_one_verified_install_contract() -> None:
    repository = Path(__file__).resolve().parents[2]
    wrapper = (
        repository / "deploy/developer-sandboxes/loom-developer-environment-capacity-authority"
    ).read_text(encoding="ascii")
    policy_destination = "/usr/local/libexec/scripts/ops/developer_sandbox_slurm_policy.py"

    assert (
        "scripts/ops/developer_sandbox_slurm_policy.py",
        policy_destination,
        "0555",
    ) in installer.EXPECTED_ASSETS
    assert (
        "deploy/developer-sandboxes/loom-developer-environment-capacity-authority",
        "/usr/local/libexec/loom-developer-environment-capacity-authority",
        "0555",
    ) in installer.EXPECTED_ASSETS
    assert policy_destination in wrapper
    assert (
        "/usr/local/libexec/loom-developer-environment/developer_sandbox_slurm_policy.py"
        not in wrapper
    )
    assert "capacity-reconcile --execute" in wrapper
    assert 'capacity-check "$@"' in wrapper
    for source, destination in (
        (
            "scripts/ops/developer_sandbox_platform_health_authority.py",
            "/usr/local/libexec/scripts/ops/developer_sandbox_platform_health_authority.py",
        ),
        (
            "scripts/ops/developer_sandbox_live_acceptance.py",
            "/usr/local/libexec/scripts/ops/developer_sandbox_live_acceptance.py",
        ),
        (
            "scripts/ops/developer_environment_acceptance_probe.py",
            "/usr/local/libexec/scripts/ops/developer_environment_acceptance_probe.py",
        ),
    ):
        assert (source, destination, "0444") in installer.EXPECTED_ASSETS


def test_upgrade_replaces_assets_and_preserves_previous_generation_backups(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    capacity_state = filesystem_root / installer.CAPACITY_RUNTIME_STATE.relative_to("/")
    activated_state = installer._canonical(
        {
            "schema_version": 1,
            "candidate_sha": SHA_ONE,
            "candidate_tree": TREE_ONE,
            "transaction_id": "1" * 32,
            "activation_status": "activated",
            "registry_generation": 9,
            "registry_payload_sha256": "9" * 64,
            "runtime_manifest": {"manifest_sha256": "8" * 64},
        }
    )
    capacity_state.write_bytes(activated_state)
    capacity_state.chmod(0o600)

    second_source = _candidate_source(tmp_path, "candidate-two")
    changed = second_source / "scripts/ops/developer_environment_cli.py"
    changed.write_bytes(changed.read_bytes() + b"\n# upgraded candidate\n")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    report = upgraded.install(
        action="environment-authority-upgrade",
        candidate_sha=SHA_TWO,
        candidate_tree=TREE_TWO,
    )

    assert report["status"] == "succeeded"
    assert report["source_sha"] == SHA_TWO
    assert capacity_state.read_bytes() == activated_state
    installed_cli = filesystem_root / "usr/local/bin/loom-developer-environment"
    assert installed_cli.read_bytes() == changed.read_bytes()
    transactions = filesystem_root / installer.TRANSACTION_ROOT.relative_to("/")
    assert any(path.is_dir() for path in transactions.iterdir())


def test_upgrade_does_not_depend_on_removed_legacy_owner(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )

    second_source = _candidate_source(tmp_path, "candidate-two")
    changed = second_source / "scripts/ops/developer_environment_cli.py"
    changed.write_bytes(changed.read_bytes() + b"\n# upgraded candidate\n")
    upgraded = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )

    def removed_legacy_owner(name: str) -> object:
        if name == "devansh":
            raise KeyError(name)
        return types.SimpleNamespace(
            pw_name=name,
            pw_uid={"qianyi": 2005, "hongjian": 2006}[name],
        )

    upgraded.account_resolver = removed_legacy_owner
    commands.calls.clear()

    report = upgraded.install(
        action="environment-authority-upgrade",
        candidate_sha=SHA_TWO,
        candidate_tree=TREE_TWO,
    )

    assert report["status"] == "succeeded"
    assert not any(command[:1] == ("/usr/sbin/usermod",) for command in commands.calls)


def test_mid_asset_crash_is_recovered_to_previous_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filesystem_root: Path,
) -> None:
    first_source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    first = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    first.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    old_cli = (filesystem_root / "usr/local/bin/loom-developer-environment").read_bytes()

    second_source = _candidate_source(tmp_path, "candidate-two")
    changed = second_source / "scripts/ops/developer_environment_cli.py"
    changed.write_bytes(changed.read_bytes() + b"\n# interrupted upgrade\n")
    interrupted = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=second_source,
        runner=commands,
    )
    real_write = interrupted._atomic_write
    installed_count = 0

    class SimulatedCrash(BaseException):
        pass

    def crash_after_third_asset(
        destination: Path,
        payload: bytes,
        mode: int,
    ) -> None:
        nonlocal installed_count
        real_write(destination, payload, mode)
        if str(destination) in installer.FIXED_DESTINATIONS:
            installed_count += 1
            if installed_count == 3:
                raise SimulatedCrash

    monkeypatch.setattr(interrupted, "_atomic_write", crash_after_third_asset)
    with pytest.raises(SimulatedCrash):
        interrupted.install(
            action="environment-authority-upgrade",
            candidate_sha=SHA_TWO,
            candidate_tree=TREE_TWO,
        )

    active = filesystem_root / installer.ACTIVE_PATH.relative_to("/")
    assert active.is_file()
    recovered = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=first_source,
        runner=commands,
    )
    recovered.recover()

    assert not active.exists()
    assert (filesystem_root / "usr/local/bin/loom-developer-environment").read_bytes() == old_cli
    report = recovered.install(
        action="environment-authority-readback",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )
    assert report["status"] == "succeeded"


def test_readback_rejects_asset_unknown_file_symlink_and_socket_parent_drift(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    authority.install(
        action="environment-authority-bootstrap",
        candidate_sha=SHA_ONE,
        candidate_tree=TREE_ONE,
    )

    cli = filesystem_root / "usr/local/bin/loom-developer-environment"
    original = cli.read_bytes()
    cli.chmod(0o755)
    cli.write_bytes(b"tampered")
    cli.chmod(0o555)
    with pytest.raises(installer.AuthorityInstallerError, match="asset drifted"):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )
    cli.chmod(0o755)
    cli.write_bytes(original)
    cli.chmod(0o555)

    unknown = filesystem_root / "usr/local/libexec/loom-developer-environment/unknown"
    unknown.write_bytes(b"unknown")
    with pytest.raises(installer.AuthorityInstallerError, match="not closed"):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )
    unknown.unlink()

    cli.unlink()
    cli.symlink_to("/etc/passwd")
    with pytest.raises(installer.AuthorityInstallerError, match="unavailable"):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )
    cli.unlink()
    cli.write_bytes(original)
    cli.chmod(0o555)

    runtime = filesystem_root / installer.AUTHORITY_RUNTIME_ROOT.relative_to("/")
    runtime.chmod(0o750)
    with pytest.raises(
        installer.AuthorityInstallerError,
        match="permission model",
    ):
        authority.install(
            action="environment-authority-readback",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_manifest_drift_fails_closed(
    tmp_path: Path,
    filesystem_root: Path,
) -> None:
    source = _candidate_source(tmp_path, "candidate-one")
    manifest = source / installer.MANIFEST_RELATIVE
    manifest.write_bytes(manifest.read_bytes() + b"\nunknown = true\n")
    commands = FakeHostCommands(filesystem_root, os.getgid())
    authority = _authority_installer(
        filesystem_root=filesystem_root,
        source_root=source,
        runner=commands,
    )
    with pytest.raises(installer.AuthorityInstallerError, match="manifest"):
        authority.install(
            action="environment-authority-bootstrap",
            candidate_sha=SHA_ONE,
            candidate_tree=TREE_ONE,
        )


def test_canonical_physical_hostname_is_case_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(installer.os, "getuid", lambda: 0)
    monkeypatch.setattr(installer.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        installer.socket,
        "gethostname",
        lambda: "TRT-EAI-OLDLAB-2.internal.example",
    )

    installer._require_canonical_root()


@pytest.mark.parametrize("hostname", ["oldlab-2", "trt-eai-oldlab-3"])
def test_logical_or_wrong_physical_hostname_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    hostname: str,
) -> None:
    monkeypatch.setattr(installer.os, "getuid", lambda: 0)
    monkeypatch.setattr(installer.os, "geteuid", lambda: 0)
    monkeypatch.setattr(installer.socket, "gethostname", lambda: hostname)

    with pytest.raises(installer.AuthorityInstallerError, match="trt-eai-oldlab-2"):
        installer._require_canonical_root()
