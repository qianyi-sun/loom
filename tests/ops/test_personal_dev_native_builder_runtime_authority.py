from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import signal
import stat
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest
import scripts.ops.personal_dev_native_builder_runtime_authority as authority_module
import scripts.ops.personal_dev_native_builder_runtime_authority_launcher as launcher_module
from scripts.ops.converge_personal_dev_native_builder_release import (
    NativeBuilderReleaseConfig,
)
from scripts.ops.personal_dev_native_builder_conformance import (
    CommandResult,
    ConformanceInputs,
)
from scripts.ops.personal_dev_native_builder_runtime_authority import (
    EPHEMERAL_SECRET_ROOT,
    LOCK_PATH,
    STATE_PATH,
    STATE_ROOT,
    AuthorityError,
    AuthorityPolicy,
    EphemeralSecretFiles,
    FileStateStore,
    HostStatus,
    RootArchiveCopies,
    RuntimeAuthority,
    SecretPaths,
    StateSnapshot,
    authority_lock,
    encode_policy,
    encode_receipt,
    encode_state,
)
from scripts.ops.personal_dev_native_builder_runtime_authority_launcher import (
    ASSET_SPECS,
    LIBEXEC_PATH,
    LIBRARY_ROOT,
    POLICY_PATH,
    AssetSpec,
    LauncherError,
    load_policy,
    sanitize_environment,
    verify_invocation,
)
from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
    AuthorityRequest,
    AuthorityRequestHeader,
)

SOURCE_SHA = "1" * 40
SOURCE_TREE = "2" * 40
PROFILE_SHA256 = "3" * 64
REQUEST_ID = "00000000-0000-0000-0000-000000000001"


class Unused:
    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"unexpected boundary call: {name}")


class MemoryStates:
    def read(self) -> None:
        return None


class StatusHost:
    def __init__(
        self,
        *,
        host_name: str = "gx10-01c7",
        architecture: str = "aarch64",
    ) -> None:
        self.host_name = host_name
        self.architecture = architecture

    def status(self) -> HostStatus:
        return HostStatus(
            host_name=self.host_name,
            architecture=self.architecture,
            dockerd_active=False,
            agent_active=False,
            nft_present=False,
            managed_containers=0,
            managed_networks=None,
        )


def _policy(
    *,
    assets: dict[str, str] | None = None,
    runtime_profile_sha256: str = PROFILE_SHA256,
) -> AuthorityPolicy:
    return AuthorityPolicy(
        authority_source_sha=SOURCE_SHA,
        authority_source_tree=SOURCE_TREE,
        runtime_profile_sha256=runtime_profile_sha256,
        asset_sha256=MappingProxyType(assets or {}),
    )


def _request(operation: str = "status", **changes: object) -> AuthorityRequest:
    values: dict[str, object] = {
        "authority_source_sha": SOURCE_SHA,
        "authority_source_tree": SOURCE_TREE,
        "operation": operation,
        "request_id": REQUEST_ID,
        "runtime_profile_sha256": PROFILE_SHA256,
        "schema_version": 1,
    }
    values.update(changes)
    return AuthorityRequest(AuthorityRequestHeader.from_mapping(values), b"")


def _runtime(host: object | None = None) -> RuntimeAuthority:
    return RuntimeAuthority(
        policy=_policy(),
        installer=Unused(),
        converger_factory=Unused(),
        conformance=Unused(),
        host=host or StatusHost(),
        states=MemoryStates(),
        archives=Unused(),
        secrets=Unused(),
    )


def _sudo_environment() -> dict[str, str]:
    return {
        "SUDO_COMMAND": str(LIBEXEC_PATH),
        "SUDO_GID": "1002",
        "SUDO_UID": "1001",
        "SUDO_USER": "qianyi",
    }


@pytest.mark.parametrize(
    ("change", "value"),
    [
        ("argv", [str(LIBEXEC_PATH), "status"]),
        ("uid_triplet", (0, 0, 1)),
        ("gid_triplet", (0, 1, 0)),
        ("SUDO_USER", "root"),
        ("SUDO_UID", "01001"),
        ("SUDO_GID", "1001"),
        ("SUDO_COMMAND", f"{LIBEXEC_PATH} status"),
    ],
)
def test_invocation_rejects_every_root_sudo_identity_drift(
    change: str,
    value: object,
) -> None:
    arguments: dict[str, object] = {
        "argv": [str(LIBEXEC_PATH)],
        "environ": _sudo_environment(),
        "uid_triplet": (0, 0, 0),
        "gid_triplet": (0, 0, 0),
        "operator_uid": 1001,
        "operator_gid": 1002,
    }
    if change in arguments:
        arguments[change] = value
    else:
        environment = dict(_sudo_environment())
        environment[change] = value
        arguments["environ"] = environment

    with pytest.raises(LauncherError, match="invocation_invalid"):
        verify_invocation(**arguments)


def test_environment_is_rejected_then_replaced_with_fixed_literals() -> None:
    unsafe = _sudo_environment() | {"PYTHONPATH": "/operator/stage"}
    with pytest.raises(LauncherError, match="invocation_invalid"):
        verify_invocation(
            argv=[str(LIBEXEC_PATH)],
            environ=unsafe,
            uid_triplet=(0, 0, 0),
            gid_triplet=(0, 0, 0),
            operator_uid=1001,
            operator_gid=1002,
        )

    inherited = _sudo_environment() | {"LANG": "en_CA.UTF-8", "TERM": "xterm"}
    sanitize_environment(inherited)
    assert inherited == {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def test_executable_entrypoint_does_not_resolve_python_from_inherited_path(
    tmp_path: Path,
) -> None:
    entrypoint = tmp_path / "authority"
    entrypoint.write_bytes(Path(launcher_module.__file__).read_bytes())
    entrypoint.chmod(0o555)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "poisoned-interpreter-ran"
    fake_python = fake_bin / "python3"
    fake_python.write_text(
        f"#!/bin/sh\n/usr/bin/touch '{marker}'\n",
        encoding="ascii",
    )
    fake_python.chmod(0o755)

    subprocess.run(
        [entrypoint],
        check=False,
        capture_output=True,
        env={"PATH": str(fake_bin)},
    )

    assert not marker.exists()


def test_fixed_authority_paths_and_complete_asset_inventory() -> None:
    from scripts.ops.personal_dev_native_builder_runtime_authority_launcher import (
        BROKER_PATH,
    )

    assert LIBEXEC_PATH == Path(
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
    )
    assert LIBRARY_ROOT == Path(
        "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority"
    )
    assert POLICY_PATH == Path(
        "/etc/loom/personal-dev-native-builder-runtime-authority.json"
    )
    assert STATE_ROOT == Path(
        "/var/lib/loom/personal-dev-native-builder-runtime-authority"
    )
    assert STATE_PATH == STATE_ROOT / "state-v1.json"
    assert LOCK_PATH == Path(
        "/run/lock/loom-personal-dev-native-builder-runtime-authority.lock"
    )
    assert EPHEMERAL_SECRET_ROOT == Path(
        "/run/loom-personal-dev-native-builder-runtime-authority"
    )
    assert BROKER_PATH == (
        LIBRARY_ROOT
        / "scripts"
        / "ops"
        / "personal_dev_native_builder_runtime_authority.py"
    )
    assert ASSET_SPECS["launcher"] == AssetSpec(LIBEXEC_PATH, 0o555)
    assert ASSET_SPECS["broker"] == AssetSpec(BROKER_PATH, 0o444)
    assert set(ASSET_SPECS) == {
        "launcher",
        "broker",
        "conformance",
        "converger",
        "installer",
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


@pytest.mark.parametrize("poisoned_name", ["broker", "dependency"])
def test_launcher_rejects_poisoned_application_before_top_level_code_runs(
    tmp_path: Path,
    poisoned_name: str,
) -> None:
    launcher_source = (
        Path(__file__).parents[2]
        / "scripts"
        / "ops"
        / "personal_dev_native_builder_runtime_authority_launcher.py"
    )
    installed_launcher = tmp_path / "authority"
    broker = tmp_path / "broker.py"
    dependency = tmp_path / "dependency.py"
    policy_path = tmp_path / "policy.json"
    marker = tmp_path / "poisoned-top-level-ran"
    installed_launcher.write_bytes(launcher_source.read_bytes())
    installed_launcher.chmod(0o555)
    broker.write_text(
        "import dependency\n"
        "def serve_validated(_policy):\n"
        "    raise RuntimeError('must not serve')\n",
        encoding="ascii",
    )
    dependency.write_text("VALUE = 1\n", encoding="ascii")
    broker.chmod(0o444)
    dependency.chmod(0o444)
    assets = {
        "broker": hashlib.sha256(broker.read_bytes()).hexdigest(),
        "dependency": hashlib.sha256(dependency.read_bytes()).hexdigest(),
        "launcher": hashlib.sha256(installed_launcher.read_bytes()).hexdigest(),
    }
    policy_path.write_bytes(
        (
            json.dumps(
                {
                    "asset_sha256": assets,
                    "authority_source_sha": SOURCE_SHA,
                    "authority_source_tree": SOURCE_TREE,
                    "runtime_profile_sha256": PROFILE_SHA256,
                    "schema": (
                        "loom.personal-dev-native-builder-runtime-authority-policy.v1"
                    ),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("ascii")
    )
    policy_path.chmod(0o444)
    poisoned = broker if poisoned_name == "broker" else dependency
    poisoned.chmod(0o644)
    poisoned.write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\n",
        encoding="ascii",
    )
    poisoned.chmod(0o444)
    child = (
        "import importlib.machinery,importlib.util,os,pathlib,sys\n"
        f"path=pathlib.Path({str(installed_launcher)!r})\n"
        "loader=importlib.machinery.SourceFileLoader('installed_launcher',str(path))\n"
        "spec=importlib.util.spec_from_loader('installed_launcher',loader)\n"
        "module=importlib.util.module_from_spec(spec)\n"
        "sys.modules[spec.name]=module\n"
        "spec.loader.exec_module(module)\n"
        "assets={\n"
        f"'launcher':module.AssetSpec(path,0o555),\n"
        f"'broker':module.AssetSpec(pathlib.Path({str(broker)!r}),0o444),\n"
        f"'dependency':module.AssetSpec(pathlib.Path({str(dependency)!r}),0o444),\n"
        "}\n"
        "try:\n"
        f" module.launch(policy_path=pathlib.Path({str(policy_path)!r}),"
        f"asset_specs=assets,broker_path=pathlib.Path({str(broker)!r}),"
        f"library_root=pathlib.Path({str(tmp_path)!r}),"
        "expected_uid=os.getuid(),expected_gid=os.getgid())\n"
        "except module.LauncherError:\n"
        " print('rejected')\n"
    )

    completed = subprocess.run(
        [sys.executable, "-I", "-c", child],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert completed.stdout == "rejected\n"
    assert completed.stderr == ""
    assert not marker.exists()


def test_launcher_registers_itself_before_loading_the_validated_broker(
    tmp_path: Path,
) -> None:
    installed_launcher = tmp_path / "authority"
    broker = tmp_path / "broker.py"
    policy_path = tmp_path / "policy.json"
    marker = tmp_path / "broker-served"
    installed_launcher.write_bytes(Path(launcher_module.__file__).read_bytes())
    installed_launcher.chmod(0o555)
    broker.write_text(
        "from scripts.ops.personal_dev_native_builder_runtime_authority_launcher "
        "import LIBEXEC_PATH\n"
        "def serve_validated(policy):\n"
        "    from pathlib import Path\n"
        f"    Path({str(marker)!r}).write_text("
        "policy['schema'] + '\\n' + str(LIBEXEC_PATH), encoding='ascii')\n",
        encoding="ascii",
    )
    broker.chmod(0o444)
    assets = {
        "broker": hashlib.sha256(broker.read_bytes()).hexdigest(),
        "launcher": hashlib.sha256(installed_launcher.read_bytes()).hexdigest(),
    }
    policy_path.write_bytes(
        (
            json.dumps(
                {
                    "asset_sha256": assets,
                    "authority_source_sha": SOURCE_SHA,
                    "authority_source_tree": SOURCE_TREE,
                    "runtime_profile_sha256": PROFILE_SHA256,
                    "schema": (
                        "loom.personal-dev-native-builder-runtime-authority-policy.v1"
                    ),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("ascii")
    )
    policy_path.chmod(0o444)
    child = (
        "import importlib.machinery,importlib.util,os,pathlib,sys\n"
        f"path=pathlib.Path({str(installed_launcher)!r})\n"
        "loader=importlib.machinery.SourceFileLoader('installed_launcher',str(path))\n"
        "spec=importlib.util.spec_from_loader('installed_launcher',loader)\n"
        "module=importlib.util.module_from_spec(spec)\n"
        "sys.modules[spec.name]=module\n"
        "spec.loader.exec_module(module)\n"
        "assets={\n"
        f"'launcher':module.AssetSpec(path,0o555),\n"
        f"'broker':module.AssetSpec(pathlib.Path({str(broker)!r}),0o444),\n"
        "}\n"
        f"module.launch(policy_path=pathlib.Path({str(policy_path)!r}),"
        f"asset_specs=assets,broker_path=pathlib.Path({str(broker)!r}),"
        f"library_root=pathlib.Path({str(tmp_path)!r}),"
        "expected_uid=os.getuid(),expected_gid=os.getgid())\n"
    )

    completed = subprocess.run(
        [sys.executable, "-I", "-c", child],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert completed.stdout == ""
    assert completed.stderr == ""
    assert marker.read_text(encoding="ascii") == (
        "loom.personal-dev-native-builder-runtime-authority-policy.v1\n"
        "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
    )


def test_policy_loader_requires_canonical_root_owned_single_link_assets(
    tmp_path: Path,
) -> None:
    first = tmp_path / "broker"
    second = tmp_path / "profile"
    first.write_bytes(b"broker\n")
    second.write_bytes(b"profile\n")
    first.chmod(0o555)
    second.chmod(0o444)
    specs = MappingProxyType(
        {
            "broker": AssetSpec(first, 0o555),
            "runtime_asset_profile": AssetSpec(second, 0o444),
        }
    )
    profile_digest = hashlib.sha256(b"profile\n").hexdigest()
    policy = _policy(
        assets={
            "broker": hashlib.sha256(b"broker\n").hexdigest(),
            "runtime_asset_profile": profile_digest,
        },
        runtime_profile_sha256=profile_digest,
    )
    policy_path = tmp_path / "policy.json"
    policy_path.write_bytes(encode_policy(policy))
    policy_path.chmod(0o444)

    assert load_policy(
        policy_path=policy_path,
        asset_specs=specs,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ) == policy.public()

    hardlink = tmp_path / "profile-hardlink"
    os.link(second, hardlink)
    with pytest.raises(LauncherError, match="asset_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )
    hardlink.unlink()

    second.chmod(0o644)
    with pytest.raises(LauncherError, match="asset_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )


def test_policy_loader_rejects_symlink_digest_and_noncanonical_policy(
    tmp_path: Path,
) -> None:
    asset = tmp_path / "asset"
    target = tmp_path / "target"
    target.write_bytes(b"reviewed\n")
    target.chmod(0o444)
    asset.symlink_to(target)
    specs = MappingProxyType({"broker": AssetSpec(asset, 0o444)})
    policy = _policy(assets={"broker": hashlib.sha256(b"reviewed\n").hexdigest()})
    policy_path = tmp_path / "policy.json"
    policy_path.write_bytes(encode_policy(policy))
    policy_path.chmod(0o444)

    with pytest.raises(LauncherError, match="asset_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )

    asset.unlink()
    asset.write_bytes(b"drifted\n")
    asset.chmod(0o444)
    with pytest.raises(LauncherError, match="asset_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )

    asset.chmod(0o644)
    asset.write_bytes(b"reviewed\n")
    asset.chmod(0o444)
    policy_path.chmod(0o644)
    policy_path.write_bytes(encode_policy(policy).replace(b'"schema":', b'"schema": '))
    policy_path.chmod(0o444)
    with pytest.raises(LauncherError, match="policy_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )


def test_policy_loader_rejects_an_asset_below_a_symlinked_directory(
    tmp_path: Path,
) -> None:
    real_directory = tmp_path / "real-library"
    real_directory.mkdir()
    asset = real_directory / "asset"
    asset.write_bytes(b"reviewed\n")
    asset.chmod(0o444)
    linked_directory = tmp_path / "linked-library"
    linked_directory.symlink_to(real_directory, target_is_directory=True)
    specs = MappingProxyType(
        {"broker": AssetSpec(linked_directory / "asset", 0o444)}
    )
    policy = _policy(
        assets={"broker": hashlib.sha256(b"reviewed\n").hexdigest()}
    )
    policy_path = tmp_path / "policy.json"
    policy_path.write_bytes(encode_policy(policy))
    policy_path.chmod(0o444)

    with pytest.raises(LauncherError, match="asset_invalid"):
        load_policy(
            policy_path=policy_path,
            asset_specs=specs,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        )


def test_lock_is_exclusive_nonblocking_and_requires_safe_metadata(tmp_path: Path) -> None:
    lock = tmp_path / "authority.lock"
    lock.touch(mode=0o600)
    lock.chmod(0o600)
    with authority_lock(
        lock,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ):
        with pytest.raises(AuthorityError, match="authority_busy"):
            with authority_lock(
                lock,
                expected_uid=os.getuid(),
                expected_gid=os.getgid(),
            ):
                raise AssertionError("unreachable")

    lock.chmod(0o666)
    with pytest.raises(AuthorityError, match="lock_invalid"):
        with authority_lock(
            lock,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
        ):
            raise AssertionError("unreachable")


def test_status_is_canonical_public_and_capacity_zero() -> None:
    receipt = _runtime().dispatch(_request())

    assert receipt == {
        "agent_service": "inactive",
        "architecture": "aarch64",
        "authority_source_sha": SOURCE_SHA,
        "authority_source_tree": SOURCE_TREE,
        "dockerd_service": "inactive",
        "executable_new_capacity": 0,
        "host_name": "gx10-01c7",
        "managed_containers": 0,
        "managed_networks": None,
        "nft_table": "absent",
        "operation": "status",
        "phase": "inert",
        "request_id": REQUEST_ID,
        "runtime_profile_sha256": PROFILE_SHA256,
        "schema": "loom.personal-dev-native-builder-runtime-authority-receipt.v1",
        "state": None,
        "state_sha256": "",
    }
    assert encode_receipt(receipt) == (
        b'{"agent_service":"inactive","architecture":"aarch64",'
        b'"authority_source_sha":"1111111111111111111111111111111111111111",'
        b'"authority_source_tree":"2222222222222222222222222222222222222222",'
        b'"dockerd_service":"inactive","executable_new_capacity":0,'
        b'"host_name":"gx10-01c7","managed_containers":0,'
        b'"managed_networks":null,"nft_table":"absent","operation":"status",'
        b'"phase":"inert","request_id":"00000000-0000-0000-0000-000000000001",'
        b'"runtime_profile_sha256":"3333333333333333333333333333333333333333333333333333333333333333",'
        b'"schema":"loom.personal-dev-native-builder-runtime-authority-receipt.v1",'
        b'"state":null,"state_sha256":""}\n'
    )


def test_dispatch_rejects_authority_or_host_identity_drift() -> None:
    with pytest.raises(AuthorityError, match="request_identity_invalid"):
        _runtime().dispatch(
            _request(authority_source_sha="4" * 40, authority_source_tree="5" * 40)
        )
    with pytest.raises(AuthorityError, match="host_identity_invalid"):
        _runtime(StatusHost(host_name="other-host")).dispatch(_request())
    with pytest.raises(AuthorityError, match="host_identity_invalid"):
        _runtime(StatusHost(architecture="x86_64")).dispatch(_request())


def test_dispatch_revalidates_forged_request_before_any_boundary() -> None:
    forged = AuthorityRequest(
        AuthorityRequestHeader(
            MappingProxyType(
                {
                    "authority_source_sha": SOURCE_SHA,
                    "authority_source_tree": SOURCE_TREE,
                    "operation": "status",
                    "request_id": REQUEST_ID,
                    "runtime_profile_sha256": PROFILE_SHA256,
                    "schema_version": 1,
                }
            )
        ),
        b"unexpected payload",
    )

    with pytest.raises(AuthorityError, match="request_invalid"):
        _runtime().dispatch(forged)


def test_launcher_main_failure_is_stable_and_never_echoes_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail() -> None:
        raise RuntimeError("private-key-bytes ca-bytes /secret/path")

    monkeypatch.setattr(
        launcher_module.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=1001, pw_gid=1002),
    )
    monkeypatch.setattr(launcher_module, "verify_invocation", lambda **_values: None)
    monkeypatch.setattr(launcher_module, "sanitize_environment", lambda _env: None)
    monkeypatch.setattr(launcher_module, "launch", fail)
    with pytest.raises(SystemExit, match="1"):
        launcher_module.main()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error:authority_failed\n"


def test_validated_broker_dispatches_only_after_launcher_policy_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pwd

    events: list[str] = []
    policy = _policy()
    request = _request()
    receipt = {"operation": "status", "status": "ok"}

    class Input:
        buffer = io.BytesIO(b"request frame")

    class Output:
        buffer = io.BytesIO()

    class Runtime:
        def dispatch(self, observed: AuthorityRequest) -> dict[str, object]:
            events.append("dispatch")
            assert observed is request
            return receipt

    @contextmanager
    def locked() -> Iterator[None]:
        events.append("lock.enter")
        try:
            yield
        finally:
            events.append("lock.exit")

    def build(
        observed_policy: AuthorityPolicy,
        *,
        operator_uid: int,
        operator_gid: int,
    ) -> Runtime:
        events.append("build")
        assert observed_policy == policy
        assert (operator_uid, operator_gid) == (1001, 1002)
        return Runtime()

    output = Output()
    monkeypatch.setattr(pwd, "getpwnam", lambda _name: SimpleNamespace(pw_uid=1001, pw_gid=1002))
    monkeypatch.setattr(authority_module, "authority_lock", locked)
    monkeypatch.setattr(authority_module, "parse_request", lambda _stream: events.append("parse") or request)
    monkeypatch.setattr(authority_module, "_build_runtime", build, raising=False)
    monkeypatch.setattr(authority_module.sys, "stdin", Input())
    monkeypatch.setattr(authority_module.sys, "stdout", output)

    authority_module.serve_validated(policy.public())

    assert events == [
        "lock.enter",
        "parse",
        "build",
        "dispatch",
        "lock.exit",
    ]
    assert output.buffer.getvalue() == encode_receipt(receipt)


def test_bounded_runner_rejects_output_past_its_limit() -> None:
    runner_type = authority_module.BoundedSubprocessRunner
    runner = runner_type(timeout_seconds=5.0, maximum_output=16)

    with pytest.raises(AuthorityError, match="command_output_invalid"):
        runner.run(
            (
                authority_module.sys.executable,
                "-c",
                "import sys;sys.stdout.write('x'*17)",
            )
        )


def test_bounded_runner_cleans_a_descendant_left_by_a_successful_parent(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "process-group"
    program = (
        "import os,pathlib,signal,subprocess,sys,time\n"
        "child=subprocess.Popen([sys.executable,'-c',"
        "'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(10)'"
        "],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpgrp()),encoding='ascii')\n"
    )
    runner = authority_module.BoundedSubprocessRunner(
        timeout_seconds=5.0,
        maximum_output=1024,
    )

    with pytest.raises(AuthorityError, match="command_cleanup_failed"):
        runner.run((sys.executable, "-c", program))

    process_group = int(marker.read_text(encoding="ascii"))
    assert not runner._group_exists(process_group)


def test_bounded_runner_replays_termination_received_during_group_cleanup() -> None:
    replayed: list[int] = []
    previous = signal.signal(
        signal.SIGTERM,
        lambda signum, _frame: replayed.append(signum),
    )
    program = (
        "import os,signal,time\n"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
        "time.sleep(.1)\n"
        "os.kill(os.getppid(),signal.SIGTERM)\n"
        "time.sleep(.1)\n"
        "os.kill(os.getppid(),signal.SIGTERM)\n"
        "time.sleep(10)\n"
    )
    try:
        with pytest.raises(AuthorityError, match="command_interrupted"):
            authority_module.BoundedSubprocessRunner(
                timeout_seconds=5.0,
                maximum_output=1024,
            ).run((sys.executable, "-c", program))
    finally:
        signal.signal(signal.SIGTERM, previous)

    assert replayed == [signal.SIGTERM]


def test_cleanup_defers_then_replays_the_first_termination_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    installed: dict[int, object] = {}

    def previous(_signum: int, _frame: object) -> None:
        events.append("signal.replayed")

    def install(signum: int, handler: object) -> object:
        old = installed.get(signum, previous)
        installed[signum] = handler
        return old

    monkeypatch.setattr(authority_module.signal, "signal", install)

    with authority_module._defer_cleanup_signals():
        handler = installed[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)
        events.append("cleanup.finished")

    assert events == ["cleanup.finished", "signal.replayed"]


def test_system_host_adapter_uses_only_fixed_commands_and_validates_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    container_id = "a" * 64
    network_id = "b" * 64

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            del check
            assert env == {
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/bin:/bin",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            command = tuple(argv)
            calls.append(command)
            if command[1:3] == (
                "is-active",
                "loom-personal-dev-builder-dockerd.service",
            ):
                return CommandResult(0, "active\n", "")
            if command[1:3] == (
                "is-active",
                "loom-personal-dev-native-builder-agent.service",
            ):
                return CommandResult(0, "active\n", "")
            if command == ("/usr/sbin/nft", "list", "tables"):
                return CommandResult(
                    0,
                    "table inet loom_personal_dev_builder\n",
                    "",
                )
            if "network" in command:
                return CommandResult(0, network_id + "\n", "")
            return CommandResult(0, container_id + "\n", "")

    monkeypatch.setattr(
        authority_module.os,
        "uname",
        lambda: SimpleNamespace(nodename="gx10-01c7", machine="aarch64"),
    )
    host = authority_module.SystemHostAdapter(runner=Runner())

    assert host.status() == HostStatus(
        host_name="gx10-01c7",
        architecture="aarch64",
        dockerd_active=True,
        agent_active=True,
        nft_present=True,
        managed_containers=1,
        managed_networks=1,
    )
    assert calls == [
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        ),
        ("/usr/sbin/nft", "list", "tables"),
        (
            "/usr/bin/docker",
            "-H",
            "unix:///run/loom-personal-dev-builder/docker.sock",
            "container",
            "ls",
            "--all",
            "--quiet",
            "--no-trunc",
        ),
        (
            "/usr/bin/docker",
            "-H",
            "unix:///run/loom-personal-dev-builder/docker.sock",
            "network",
            "ls",
            "--quiet",
            "--no-trunc",
            "--filter",
            "type=custom",
        ),
    ]


def test_inactive_host_inventory_proves_containers_empty_but_networks_unobserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    docker_root = tmp_path / "docker"
    (docker_root / "containers").mkdir(parents=True)
    (docker_root / "network" / "files").mkdir(parents=True)
    (docker_root / "network" / "files" / "local-kv.db").write_bytes(
        b"docker/network/v1.0/network/" + (b"a" * 64) + b"/host-none-bytes"
    )
    (docker_root / "image").mkdir()
    (docker_root / "image" / "retained-cache").write_bytes(b"image metadata")
    (docker_root / "overlay2").mkdir()
    (docker_root / "overlay2" / "retained-layer").write_bytes(b"layer metadata")
    for directory in (
        docker_root,
        docker_root / "containers",
        docker_root / "network",
        docker_root / "network" / "files",
        docker_root / "image",
        docker_root / "overlay2",
    ):
        directory.chmod(0o755)

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            del check, env
            command = tuple(argv)
            if command[0] == "/usr/bin/docker":
                raise AssertionError("inactive inventory must not use a dead socket")
            if command[0] == "/usr/bin/systemctl":
                return CommandResult(3, "inactive\n", "")
            return CommandResult(0, "", "")

    monkeypatch.setattr(authority_module, "_DOCKER_DATA_ROOT", docker_root)
    monkeypatch.setattr(
        authority_module.os,
        "uname",
        lambda: SimpleNamespace(nodename="gx10-01c7", machine="aarch64"),
    )

    assert authority_module.SystemHostAdapter(
        runner=Runner(),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    ).status() == HostStatus(
        host_name="gx10-01c7",
        architecture="aarch64",
        dockerd_active=False,
        agent_active=False,
        nft_present=False,
        managed_containers=0,
        managed_networks=None,
    )


def test_dockerd_start_rejects_a_stopped_container_store_before_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    docker_root = tmp_path / "docker"
    container_id = "a" * 64
    (docker_root / "containers" / container_id).mkdir(parents=True)
    for directory in (
        docker_root,
        docker_root / "containers",
        docker_root / "containers" / container_id,
    ):
        directory.chmod(0o755)
    calls: list[tuple[str, ...]] = []

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            del check, env
            calls.append(tuple(argv))
            if argv[0] == "/usr/bin/systemctl":
                return CommandResult(3, "inactive\n", "")
            return CommandResult(0, "", "")

    monkeypatch.setattr(authority_module, "_DOCKER_DATA_ROOT", docker_root)
    host = authority_module.SystemHostAdapter(
        runner=Runner(),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )

    with pytest.raises(AuthorityError, match="managed_objects_invalid"):
        host.start_dockerd()

    assert not any(call[1:2] == ("start",) for call in calls)


@pytest.mark.parametrize("live_kind", ["container", "network"])
def test_dockerd_start_rejects_post_start_inventory_before_returning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    live_kind: str,
) -> None:
    calls: list[tuple[str, ...]] = []
    active = False

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            nonlocal active
            del check, env
            command = tuple(argv)
            calls.append(command)
            if command[0] == "/usr/bin/systemctl":
                action = command[1]
                if action == "is-active":
                    return (
                        CommandResult(0, "active\n", "")
                        if active
                        else CommandResult(3, "inactive\n", "")
                    )
                active = action == "start"
                return CommandResult(0, "", "")
            if live_kind in command:
                return CommandResult(0, "c" * 64 + "\n", "")
            return CommandResult(0, "", "")

    monkeypatch.setattr(authority_module, "_DOCKER_DATA_ROOT", tmp_path / "docker")
    host = authority_module.SystemHostAdapter(
        runner=Runner(),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )

    with pytest.raises(AuthorityError, match="managed_objects_invalid"):
        host.start_dockerd()

    assert active is False
    assert calls.index(
        ("/usr/bin/systemctl", "start", "loom-personal-dev-builder-dockerd.service")
    ) < calls.index(
        (
            "/usr/bin/docker",
            "-H",
            "unix:///run/loom-personal-dev-builder/docker.sock",
            "container",
            "ls",
            "--all",
            "--quiet",
            "--no-trunc",
        )
    ) < calls.index(
        ("/usr/bin/systemctl", "stop", "loom-personal-dev-builder-dockerd.service")
    )


def test_system_host_adapter_mutates_only_exact_services_and_nft_table() -> None:
    calls: list[tuple[str, ...]] = []
    services = {
        "loom-personal-dev-builder-dockerd.service": False,
        "loom-personal-dev-native-builder-agent.service": False,
    }
    nft_present = False

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            nonlocal nft_present
            del check, env
            command = tuple(argv)
            calls.append(command)
            if command == ("/usr/sbin/nft", "list", "tables"):
                output = (
                    "table inet loom_personal_dev_builder\n"
                    if nft_present
                    else ""
                )
                return CommandResult(0, output, "")
            if command[:2] == ("/usr/sbin/nft", "--check"):
                return CommandResult(0, "", "")
            if command[:2] == ("/usr/sbin/nft", "--file"):
                nft_present = True
                return CommandResult(0, "", "")
            if command[:3] == ("/usr/sbin/nft", "delete", "table"):
                nft_present = False
                return CommandResult(0, "", "")
            if command[0] == "/usr/bin/docker":
                return CommandResult(0, "", "")
            action = command[1]
            unit = command[2]
            if action == "is-active":
                return (
                    CommandResult(0, "active\n", "")
                    if services[unit]
                    else CommandResult(3, "inactive\n", "")
                )
            services[unit] = action == "start"
            return CommandResult(0, "", "")

    host = authority_module.SystemHostAdapter(runner=Runner())

    host.load_nft()
    host.start_dockerd()
    host.start_agent()
    host.stop_agent()
    host.stop_dockerd()
    host.delete_nft()

    assert services == {
        "loom-personal-dev-builder-dockerd.service": False,
        "loom-personal-dev-native-builder-agent.service": False,
    }
    assert nft_present is False
    assert calls == [
        ("/usr/sbin/nft", "list", "tables"),
        (
            "/usr/sbin/nft",
            "--check",
            "--file",
            "/etc/loom/personal-dev-native-builder/provider-network.nft",
        ),
        (
            "/usr/sbin/nft",
            "--file",
            "/etc/loom/personal-dev-native-builder/provider-network.nft",
        ),
        ("/usr/sbin/nft", "list", "tables"),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/systemctl",
            "start",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/docker",
            "-H",
            "unix:///run/loom-personal-dev-builder/docker.sock",
            "container",
            "ls",
            "--all",
            "--quiet",
            "--no-trunc",
        ),
        (
            "/usr/bin/docker",
            "-H",
            "unix:///run/loom-personal-dev-builder/docker.sock",
            "network",
            "ls",
            "--quiet",
            "--no-trunc",
            "--filter",
            "type=custom",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "start",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "stop",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/systemctl",
            "stop",
            "loom-personal-dev-builder-dockerd.service",
        ),
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-builder-dockerd.service",
        ),
        ("/usr/sbin/nft", "list", "tables"),
        (
            "/usr/sbin/nft",
            "delete",
            "table",
            "inet",
            "loom_personal_dev_builder",
        ),
        ("/usr/sbin/nft", "list", "tables"),
    ]


def test_system_host_stop_is_idempotent_when_the_exact_unit_is_absent() -> None:
    calls: list[tuple[str, ...]] = []

    class Runner:
        def run(
            self,
            argv: tuple[str, ...] | list[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            del check, env
            command = tuple(argv)
            calls.append(command)
            if command[1] != "is-active":
                raise AssertionError("an absent unit must not be mutated")
            return CommandResult(4, "unknown\n", "")

    authority_module.SystemHostAdapter(runner=Runner()).stop_agent()

    assert calls == [
        (
            "/usr/bin/systemctl",
            "is-active",
            "loom-personal-dev-native-builder-agent.service",
        )
    ]


def test_runtime_builder_rejects_profile_outside_installed_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[Path] = []

    def load(path: Path) -> SimpleNamespace:
        observed.append(path)
        return SimpleNamespace(sha256="f" * 64)

    monkeypatch.setattr(
        authority_module,
        "load_native_builder_runtime_profile",
        load,
        raising=False,
    )

    with pytest.raises(AuthorityError, match="runtime_profile_invalid"):
        authority_module._build_runtime(
            _policy(),
            operator_uid=1001,
            operator_gid=1002,
        )

    assert observed == [
        LIBRARY_ROOT
        / "deploy"
        / "personal-dev-native-builder"
        / "runtime-profile-v1.json"
    ]


def test_runtime_builder_wires_only_fixed_typed_adapters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_profile = (
        Path(authority_module.__file__).parents[2]
        / "deploy"
        / "personal-dev-native-builder"
        / "runtime-profile-v1.json"
    )
    profile = authority_module.load_native_builder_runtime_profile(repository_profile)
    observed: list[Path] = []

    def load(path: Path) -> object:
        observed.append(path)
        return profile

    monkeypatch.setattr(
        authority_module,
        "load_native_builder_runtime_profile",
        load,
    )

    runtime = authority_module._build_runtime(
        _policy(runtime_profile_sha256=profile.sha256),
        operator_uid=1001,
        operator_gid=1002,
    )

    assert isinstance(runtime, RuntimeAuthority)
    assert isinstance(
        runtime.installer,
        authority_module.PersonalDevNativeBuilderRuntimeInstaller,
    )
    assert runtime.installer.profile is profile
    assert isinstance(runtime.host, authority_module.SystemHostAdapter)
    assert isinstance(runtime.states, FileStateStore)
    assert isinstance(runtime.archives, RootArchiveCopies)
    assert runtime.archives.operator_uid == 1001
    assert runtime.archives.operator_gid == 1002
    assert isinstance(runtime.secrets, EphemeralSecretFiles)
    assert runtime.secrets.private_key_mode == profile.private_key_mode
    assert isinstance(runtime.conformance, authority_module.ConformanceOperations)
    assert observed == [
        LIBRARY_ROOT
        / "deploy"
        / "personal-dev-native-builder"
        / "runtime-profile-v1.json"
    ]


def test_broker_ast_has_no_unrelated_control_plane_executables() -> None:
    source = Path(authority_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    literal_words = {
        word.lower()
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        for word in node.value.replace("/", " ").split()
    }
    assert literal_words.isdisjoint(
        {
            "database",
            "kubectl",
            "kubernetes",
            "psql",
            "sacct",
            "scontrol",
            "sinfo",
            "slurm",
            "task",
            "worker",
        }
    )


CURRENT_AGENT = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64
)
CURRENT_BUILDER = (
    "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
)
PREVIOUS_AGENT = (
    "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "c" * 64
)
PREVIOUS_BUILDER = (
    "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "d" * 64
)
CURRENT_REVISION = "4" * 40
PREVIOUS_REVISION = "5" * 40
ARCHIVE_SHA512 = "6" * 128
PUBLIC_ORIGIN = "https://native-builds.example.com"
CONFORMANCE_RECEIPT: dict[str, object] = {
    "architecture": "arm64",
    "buildkit_sandbox_id": "7" * 64,
    "client_sandbox_id": "8" * 64,
    "cross_provider_network": "denied",
    "foreign_to_provider": "denied",
    "host_to_provider": "denied",
    "managed_containers_after": 0,
    "managed_networks_after": 0,
    "platform": "linux/arm64",
    "private_control_plane": "denied",
    "public_https": "allowed",
    "runtime": "runsc-personal-dev-native",
    "schema": "loom-personal-dev-native-builder-conformance-v1",
    "status": "passed",
}


def _prepare_request() -> AuthorityRequest:
    archive_path = (
        f"/var/tmp/loom-personal-dev-native-builder/{REQUEST_ID}/"
        "gvisor-release-20260810.0-aarch64.tar.bz2"
    )
    return _request(
        "prepare",
        archive_path=archive_path,
        archive_sha512=ARCHIVE_SHA512,
        current_agent=CURRENT_AGENT,
        current_builder=CURRENT_BUILDER,
        current_revision=CURRENT_REVISION,
        previous_agent=PREVIOUS_AGENT,
        previous_builder=PREVIOUS_BUILDER,
        previous_revision=PREVIOUS_REVISION,
        public_store_origin=PUBLIC_ORIGIN,
    )


def _prepared_state() -> dict[str, object]:
    return {
        "authority_source_sha": SOURCE_SHA,
        "authority_source_tree": SOURCE_TREE,
        "conformance": CONFORMANCE_RECEIPT,
        "current_agent": CURRENT_AGENT,
        "current_builder": CURRENT_BUILDER,
        "current_revision": CURRENT_REVISION,
        "phase": "prepared",
        "previous_agent": PREVIOUS_AGENT,
        "previous_builder": PREVIOUS_BUILDER,
        "previous_revision": PREVIOUS_REVISION,
        "public_store_origin": PUBLIC_ORIGIN,
        "runtime_profile_sha256": PROFILE_SHA256,
        "schema": "loom.personal-dev-native-builder-runtime-authority-state.v1",
    }


class RecordingStates:
    def __init__(
        self,
        events: list[str],
        *,
        fail_publish: bool = False,
        fail_remove: bool = False,
    ) -> None:
        self.events = events
        self.snapshot: StateSnapshot | None = None
        self.fail_publish = fail_publish
        self.fail_remove = fail_remove

    def read(self) -> StateSnapshot | None:
        self.events.append("state.read")
        return self.snapshot

    def publish(self, value: dict[str, object]) -> StateSnapshot:
        self.events.append("state.publish")
        if self.fail_publish:
            raise AuthorityError("injected_failure")
        payload = encode_state(value)
        self.snapshot = StateSnapshot(
            MappingProxyType(dict(value)),
            hashlib.sha256(payload).hexdigest(),
        )
        return self.snapshot

    def remove(self, *, expected_sha256: str) -> None:
        self.events.append("state.remove")
        if self.fail_remove:
            raise AuthorityError("injected_failure")
        if self.snapshot is None or self.snapshot.sha256 != expected_sha256:
            raise AuthorityError("state_changed")
        self.snapshot = None


class RecordingArchives:
    def __init__(self, events: list[str], *, fail_unlink: bool = False) -> None:
        self.events = events
        self.fail_unlink = fail_unlink
        self.present = False
        self.path = Path("/private/archive-copy")

    @contextmanager
    def copy(
        self,
        source: Path,
        *,
        expected_sha512: str,
        request_id: str,
    ) -> Iterator[Path]:
        self.events.append(
            f"archive.copy:{source}:{expected_sha512}:{request_id}"
        )
        self.present = True
        try:
            yield self.path
        finally:
            self.events.append("archive.unlink")
            self.present = False
            if self.fail_unlink:
                raise AuthorityError("injected_failure")


class RecordingInstaller:
    def __init__(self, events: list[str], *, fail: str | None = None) -> None:
        self.events = events
        self.fail = fail
        self.stage_arguments: dict[str, object] | None = None
        self.staged_present = False

    def _record(self, name: str) -> None:
        self.events.append(name)
        if self.fail == name:
            self.fail = None
            raise AuthorityError("injected_failure")

    def preflight(self, archive: Path) -> dict[str, object]:
        self._record(f"installer.preflight:{archive}")
        return {"operation": "preflight"}

    def install(self, archive: Path) -> dict[str, object]:
        self._record(f"installer.install:{archive}")
        return {"operation": "install"}

    def stage_agent_authorized(self, **arguments: object) -> dict[str, object]:
        self.events.append("installer.stage_agent_authorized")
        self.stage_arguments = dict(arguments)
        self.staged_present = True
        if self.fail == "installer.stage_agent_authorized":
            self.fail = None
            raise AuthorityError("injected_failure")
        return {"operation": "stage-agent", "state": "staged"}

    def discard_agent_stage(self) -> None:
        self.events.append("installer.discard_agent_stage")
        self.staged_present = False
        if self.fail == "installer.discard_agent_stage":
            self.fail = None
            raise AuthorityError("cleanup_injected")

    def verify_active(self) -> dict[str, object]:
        self._record("installer.verify_active")
        return {"operation": "verify-active", "state": "active"}

    def remove(self) -> dict[str, object]:
        self._record("installer.remove")
        return {
            "operation": "remove",
            "retained": "dedicated-image-cache-and-system-identities",
            "state": "managed-files-absent",
        }

    def verify_staged(self) -> dict[str, object]:
        self._record("installer.verify_staged")
        return {"operation": "verify-staged", "state": "staged"}


class RecordingHost:
    def __init__(
        self,
        events: list[str],
        *,
        fail: str | None = None,
        cleanup_fail: str | None = None,
    ) -> None:
        self.events = events
        self.fail = fail
        self.cleanup_fail = cleanup_fail
        self.counts: dict[str, int] = {}
        self.dockerd_active = False
        self.agent_active = False
        self.nft_present = False
        self.managed_containers = 0
        self.managed_networks = 0

    def _after(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1
        numbered = f"{name}:{self.counts[name]}"
        if self.fail in {name, numbered}:
            self.fail = None
            raise AuthorityError("injected_failure")
        if self.cleanup_fail == name:
            self.cleanup_fail = None
            raise AuthorityError("cleanup_injected")

    def status(self) -> HostStatus:
        return HostStatus(
            host_name="gx10-01c7",
            architecture="aarch64",
            dockerd_active=self.dockerd_active,
            agent_active=self.agent_active,
            nft_present=self.nft_present,
            managed_containers=self.managed_containers,
            managed_networks=(
                self.managed_networks if self.dockerd_active else None
            ),
        )

    def verify_inert(self, *, require_empty: bool) -> HostStatus:
        self.events.append(f"host.verify_inert:{str(require_empty).lower()}")
        if (
            self.dockerd_active
            or self.agent_active
            or self.nft_present
            or (
                require_empty
                and (
                    self.managed_containers
                    or (self.dockerd_active and self.managed_networks)
                )
            )
        ):
            raise AuthorityError("host_state_invalid")
        self._after("host.verify_inert")
        return self.status()

    def load_nft(self) -> None:
        self.events.append("host.load_nft")
        self.nft_present = True
        self._after("host.load_nft")

    def delete_nft(self) -> None:
        self.events.append("host.delete_nft")
        self.nft_present = False
        self._after("host.delete_nft")

    def start_dockerd(self) -> None:
        self.events.append("host.start_dockerd")
        self.dockerd_active = True
        self._after("host.start_dockerd")

    def stop_dockerd(self) -> None:
        self.events.append("host.stop_dockerd")
        self.dockerd_active = False
        self._after("host.stop_dockerd")

    def start_agent(self) -> None:
        self.events.append("host.start_agent")
        self.agent_active = True
        self._after("host.start_agent")

    def stop_agent(self) -> None:
        self.events.append("host.stop_agent")
        self.agent_active = False
        self._after("host.stop_agent")


class RecordingConverger:
    def __init__(
        self,
        events: list[str],
        *,
        fail: str | None = None,
        unstable_plan: bool = False,
    ) -> None:
        self.events = events
        self.fail = fail
        self.unstable_plan = unstable_plan
        self.plan_count = 0

    def _record(self, name: str) -> None:
        self.events.append(f"converger.{name}")
        if self.fail == name:
            self.fail = None
            raise AuthorityError("injected_failure")

    def plan(self) -> dict[str, object]:
        self._record("plan")
        self.plan_count += 1
        return {
            "operation": "plan",
            "pull": [CURRENT_AGENT, CURRENT_BUILDER],
            "sequence": self.plan_count if self.unstable_plan else 0,
        }

    def apply(self) -> dict[str, object]:
        self._record("apply")
        return {"operation": "apply", "state": "converged"}

    def verify(self) -> dict[str, object]:
        self._record("verify")
        return {"operation": "verify", "state": "converged"}


class RecordingConformance:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail
        self.inputs: ConformanceInputs | None = None

    def run(self, inputs: ConformanceInputs) -> dict[str, object]:
        self.events.append("conformance.run")
        self.inputs = inputs
        if self.fail:
            self.fail = False
            raise AuthorityError("injected_failure")
        return dict(CONFORMANCE_RECEIPT)


def _prepare_runtime(
    *,
    installer_fail: str | None = None,
    host_fail: str | None = None,
    cleanup_fail: str | None = None,
    converger_fail: str | None = None,
    conformance_fail: bool = False,
    unstable_plan: bool = False,
    archive_unlink_fail: bool = False,
    state_publish_fail: bool = False,
) -> tuple[
    RuntimeAuthority,
    list[str],
    RecordingHost,
    RecordingStates,
    RecordingArchives,
    RecordingConformance,
    list[NativeBuilderReleaseConfig],
]:
    events: list[str] = []
    host = RecordingHost(
        events,
        fail=host_fail,
        cleanup_fail=cleanup_fail,
    )
    states = RecordingStates(events, fail_publish=state_publish_fail)
    archives = RecordingArchives(events, fail_unlink=archive_unlink_fail)
    conformance = RecordingConformance(events, fail=conformance_fail)
    converger = RecordingConverger(
        events,
        fail=converger_fail,
        unstable_plan=unstable_plan,
    )
    configs: list[NativeBuilderReleaseConfig] = []

    def factory(config: NativeBuilderReleaseConfig) -> RecordingConverger:
        events.append("converger.create")
        configs.append(config)
        return converger

    runtime = RuntimeAuthority(
        policy=_policy(),
        installer=RecordingInstaller(events, fail=installer_fail),
        converger_factory=factory,
        conformance=conformance,
        host=host,
        states=states,
        archives=archives,
        secrets=Unused(),
    )
    return runtime, events, host, states, archives, conformance, configs


def test_prepare_runs_exact_transition_and_publishes_only_after_inert_exit() -> None:
    runtime, events, host, states, archives, conformance, configs = _prepare_runtime()

    receipt = runtime.dispatch(_prepare_request())

    archive_path = (
        f"/var/tmp/loom-personal-dev-native-builder/{REQUEST_ID}/"
        "gvisor-release-20260810.0-aarch64.tar.bz2"
    )
    assert events == [
        "state.read",
        "host.verify_inert:true",
        f"archive.copy:{archive_path}:{ARCHIVE_SHA512}:{REQUEST_ID}",
        "installer.preflight:/private/archive-copy",
        "installer.install:/private/archive-copy",
        "installer.verify_staged",
        "host.load_nft",
        "host.start_dockerd",
        "converger.create",
        "converger.plan",
        "converger.plan",
        "converger.apply",
        "converger.verify",
        "conformance.run",
        "host.stop_dockerd",
        "host.delete_nft",
        "host.verify_inert:true",
        "archive.unlink",
        "state.publish",
    ]
    assert len(configs) == 1
    config = configs[0]
    assert config.current_agent.reference == CURRENT_AGENT
    assert config.current_agent.revision == CURRENT_REVISION
    assert config.current_builder.reference == CURRENT_BUILDER
    assert config.current_builder.revision == CURRENT_REVISION
    assert config.previous_agent is not None
    assert config.previous_agent.reference == PREVIOUS_AGENT
    assert config.previous_agent.revision == PREVIOUS_REVISION
    assert config.previous_builder is not None
    assert config.previous_builder.reference == PREVIOUS_BUILDER
    assert config.previous_builder.revision == PREVIOUS_REVISION
    assert conformance.inputs == ConformanceInputs(
        builder_image=CURRENT_BUILDER,
        agent_image=CURRENT_AGENT,
        public_https=PUBLIC_ORIGIN,
    )
    assert archives.present is False
    assert host.dockerd_active is False
    assert host.agent_active is False
    assert host.nft_present is False
    assert states.snapshot is not None
    assert dict(states.snapshot.value) == _prepared_state()
    assert states.snapshot.sha256 == hashlib.sha256(
        encode_state(_prepared_state())
    ).hexdigest()
    assert receipt["operation"] == "prepare"
    assert receipt["phase"] == "prepared"
    assert receipt["state"] == _prepared_state()
    assert receipt["state_sha256"] == states.snapshot.sha256
    assert receipt["executable_new_capacity"] == 0


def test_prepare_rejects_noninert_state_before_any_mutation() -> None:
    runtime, events, host, states, archives, _, _ = _prepare_runtime()
    payload = encode_state(_prepared_state())
    states.snapshot = StateSnapshot(
        MappingProxyType(_prepared_state()),
        hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(AuthorityError, match="phase_invalid"):
        runtime.dispatch(_prepare_request())

    assert events == ["state.read"]
    assert not host.dockerd_active
    assert not archives.present


def test_prepare_rejects_nondeterministic_plans_and_compensates() -> None:
    runtime, events, host, states, archives, _, _ = _prepare_runtime(
        unstable_plan=True
    )

    with pytest.raises(AuthorityError, match="convergence_plan_invalid"):
        runtime.dispatch(_prepare_request())

    assert "converger.apply" not in events
    assert events[-4:] == [
        "host.stop_agent",
        "host.stop_dockerd",
        "host.delete_nft",
        "host.verify_inert:true",
    ]
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot is None
    assert archives.present is False


@pytest.mark.parametrize(
    ("failure_kind", "failure_value"),
    [
        ("installer", "installer.install:/private/archive-copy"),
        ("installer", "installer.verify_staged"),
        ("host", "host.load_nft"),
        ("host", "host.start_dockerd"),
        ("converger", "plan"),
        ("converger", "apply"),
        ("converger", "verify"),
        ("conformance", True),
        ("host", "host.stop_dockerd"),
        ("host", "host.delete_nft"),
        ("host", "host.verify_inert:2"),
        ("archive", True),
        ("state", True),
    ],
)
def test_prepare_failure_after_each_mutation_compensates_to_inert(
    failure_kind: str,
    failure_value: object,
) -> None:
    arguments: dict[str, object] = {}
    if failure_kind == "installer":
        arguments["installer_fail"] = failure_value
    elif failure_kind == "host":
        arguments["host_fail"] = failure_value
    elif failure_kind == "converger":
        arguments["converger_fail"] = failure_value
    elif failure_kind == "conformance":
        arguments["conformance_fail"] = failure_value
    elif failure_kind == "archive":
        arguments["archive_unlink_fail"] = failure_value
    else:
        arguments["state_publish_fail"] = failure_value
    runtime, events, host, states, archives, _, _ = _prepare_runtime(**arguments)

    with pytest.raises(AuthorityError):
        runtime.dispatch(_prepare_request())

    assert "host.stop_agent" in events
    assert "host.stop_dockerd" in events
    assert "host.delete_nft" in events
    assert events[-1] == "host.verify_inert:true"
    assert not host.dockerd_active
    assert not host.agent_active
    assert not host.nft_present
    assert states.snapshot is None
    assert archives.present is False


def test_prepare_cleanup_failure_is_stable_and_does_not_skip_later_cleanup() -> None:
    runtime, events, host, states, archives, _, _ = _prepare_runtime(
        conformance_fail=True,
        cleanup_fail="host.stop_agent",
    )

    with pytest.raises(AuthorityError, match="cleanup_failed"):
        runtime.dispatch(_prepare_request())

    assert events[-3:] == [
        "host.stop_dockerd",
        "host.delete_nft",
        "host.verify_inert:true",
    ]
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot is None
    assert archives.present is False


def test_state_bytes_are_canonical_public_and_reject_secret_fields() -> None:
    payload = encode_state(_prepared_state())
    assert payload.endswith(b"\n")
    assert payload == encode_state(dict(reversed(list(_prepared_state().items()))))
    assert b"archive" not in payload
    assert b"private_key" not in payload
    assert b"ca_sha256" not in payload

    for forbidden in (
        "private_key_sha256",
        "ca_sha256",
        "secret_path",
        "argv",
        "environment",
        "stdout",
        "stderr",
    ):
        drifted = dict(_prepared_state())
        drifted[forbidden] = "not-public"
        with pytest.raises(AuthorityError, match="state_invalid"):
            encode_state(drifted)


def test_file_state_store_atomically_fsyncs_mode_0600_and_reads_exact_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    path = root / "state-v1.json"
    store = FileStateStore(
        root=root,
        path=path,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    fsync_kinds: list[str] = []
    real_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        fsync_kinds.append(
            "directory" if os.path.isdir(f"/proc/self/fd/{descriptor}") else "file"
        )
        real_fsync(descriptor)

    monkeypatch.setattr(authority_module.os, "fsync", record_fsync)
    published = store.publish(_prepared_state())

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.read_bytes() == encode_state(_prepared_state())
    assert published.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert store.read() == published
    assert fsync_kinds.count("file") >= 1
    assert fsync_kinds.count("directory") >= 1


def test_file_state_store_publishes_through_retained_directory_after_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    displaced = tmp_path / "state-opened"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    store = FileStateStore(
        root=root,
        path=root / "state-v1.json",
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    real_replace = os.replace
    swapped = False

    def swap_then_replace(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        destination: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        **arguments: object,
    ) -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            root.rename(displaced)
            root.mkdir(mode=0o700)
            root.chmod(0o700)
        real_replace(source, destination, **arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(authority_module.os, "replace", swap_then_replace)

    published = store.publish(_prepared_state())

    assert published.sha256 == hashlib.sha256(
        encode_state(_prepared_state())
    ).hexdigest()
    assert (displaced / "state-v1.json").read_bytes() == encode_state(
        _prepared_state()
    )
    assert list(root.iterdir()) == []


def test_archive_copy_is_descriptor_bound_root_private_and_always_unlinked(
    tmp_path: Path,
) -> None:
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    source = tmp_path / "archive.tar.bz2"
    source.write_bytes(b"fixed archive bytes")
    source.chmod(0o600)
    digest = hashlib.sha512(b"fixed archive bytes").hexdigest()
    copies = RootArchiveCopies(
        root=root,
        operator_uid=os.getuid(),
        operator_gid=os.getgid(),
        root_uid=os.getuid(),
        root_gid=os.getgid(),
    )

    with copies.copy(source, expected_sha512=digest, request_id=REQUEST_ID) as copied:
        copied_path = copied
        assert copied.read_bytes() == b"fixed archive bytes"
        assert copied.stat().st_ino != source.stat().st_ino
        assert stat.S_IMODE(copied.stat().st_mode) == 0o600
        assert copied.stat().st_nlink == 1
    assert not copied_path.exists()

    hardlink = tmp_path / "archive-link"
    os.link(source, hardlink)
    with pytest.raises(AuthorityError, match="archive_invalid"):
        with copies.copy(source, expected_sha512=digest, request_id=REQUEST_ID):
            raise AssertionError("unreachable")
    hardlink.unlink()
    source.unlink()
    source.symlink_to(tmp_path / "missing")
    with pytest.raises(AuthorityError, match="archive_invalid"):
        with copies.copy(source, expected_sha512=digest, request_id=REQUEST_ID):
            raise AssertionError("unreachable")


def test_archive_copy_unlinks_partial_destination_when_digest_is_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    source = tmp_path / "archive.tar.bz2"
    source.write_bytes(b"fixed archive bytes")
    source.chmod(0o600)
    copies = RootArchiveCopies(
        root=root,
        operator_uid=os.getuid(),
        operator_gid=os.getgid(),
        root_uid=os.getuid(),
        root_gid=os.getgid(),
    )

    with pytest.raises(AuthorityError, match="archive_invalid"):
        with copies.copy(source, expected_sha512="f" * 128, request_id=REQUEST_ID):
            raise AssertionError("unreachable")

    assert list(root.iterdir()) == []


def test_archive_copy_keeps_root_directory_fd_across_path_replacement(
    tmp_path: Path,
) -> None:
    root = tmp_path / "private"
    displaced = tmp_path / "private-opened"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    source = tmp_path / "archive.tar.bz2"
    source.write_bytes(b"fixed archive bytes")
    source.chmod(0o600)
    copies = RootArchiveCopies(
        root=root,
        operator_uid=os.getuid(),
        operator_gid=os.getgid(),
        root_uid=os.getuid(),
        root_gid=os.getgid(),
    )

    with copies.copy(
        source,
        expected_sha512=hashlib.sha512(b"fixed archive bytes").hexdigest(),
        request_id=REQUEST_ID,
    ) as copied:
        root.rename(displaced)
        root.mkdir(mode=0o700)
        root.chmod(0o700)
        assert copied.read_bytes() == b"fixed archive bytes"

    assert list(displaced.iterdir()) == []
    assert list(root.iterdir()) == []


AGENT_INSTANCE_ID = "00000000-0000-0000-0000-000000000002"
AGENT_KEY_ID = "native-agent-v1"
PUBLIC_KEY_SHA256 = "9" * 64
SERVICE_ORIGIN = "https://native-control.example.com"
PRIVATE_SEED = b"k" * 32
SERVICE_CA = b"private service ca bytes\n"


def _snapshot(value: dict[str, object]) -> StateSnapshot:
    payload = encode_state(value)
    return StateSnapshot(
        MappingProxyType(dict(value)),
        hashlib.sha256(payload).hexdigest(),
    )


def _staged_state() -> dict[str, object]:
    value = _prepared_state()
    value.update(
        {
            "agent_instance_id": AGENT_INSTANCE_ID,
            "agent_key_id": AGENT_KEY_ID,
            "phase": "staged",
            "public_key_sha256": PUBLIC_KEY_SHA256,
            "service_origin": SERVICE_ORIGIN,
        }
    )
    return value


def _active_state() -> dict[str, object]:
    value = _staged_state()
    value["phase"] = "active"
    return value


def _stage_request(state_sha256: str) -> AuthorityRequest:
    header = AuthorityRequestHeader.from_mapping(
        {
            "agent_image": CURRENT_AGENT,
            "agent_instance_id": AGENT_INSTANCE_ID,
            "agent_key_id": AGENT_KEY_ID,
            "authority_source_sha": SOURCE_SHA,
            "authority_source_tree": SOURCE_TREE,
            "builder_image": CURRENT_BUILDER,
            "expected_public_key_sha256": PUBLIC_KEY_SHA256,
            "expected_state_sha256": state_sha256,
            "operation": "stage-agent",
            "private_key_length": len(PRIVATE_SEED),
            "request_id": REQUEST_ID,
            "runtime_profile_sha256": PROFILE_SHA256,
            "schema_version": 1,
            "service_ca_length": len(SERVICE_CA),
            "service_origin": SERVICE_ORIGIN,
        }
    )
    return AuthorityRequest(header, PRIVATE_SEED + SERVICE_CA)


def _state_request(operation: str, state_sha256: str) -> AuthorityRequest:
    return _request(operation, expected_state_sha256=state_sha256)


class RecordingSecrets:
    def __init__(self, events: list[str], *, fail_unlink: bool = False) -> None:
        self.events = events
        self.fail_unlink = fail_unlink
        self.present = False
        self.private_bytes = b""
        self.ca_bytes = b""
        self.paths = SecretPaths(
            private_key=Path("/ephemeral/private-key"),
            ca_file=Path("/ephemeral/service-ca"),
        )

    @contextmanager
    def files(
        self,
        payload: bytes,
        *,
        private_key_length: int,
        service_ca_length: int,
        request_id: str,
    ) -> Iterator[SecretPaths]:
        assert request_id == REQUEST_ID
        assert len(payload) == private_key_length + service_ca_length
        self.events.append("secret.create")
        self.private_bytes = payload[:private_key_length]
        self.ca_bytes = payload[private_key_length:]
        self.present = True
        try:
            yield self.paths
        finally:
            self.events.append("secret.unlink")
            self.present = False
            self.private_bytes = b""
            self.ca_bytes = b""
            if self.fail_unlink:
                raise AuthorityError("injected_failure")


def _transition_runtime(
    initial: dict[str, object],
    *,
    installer_fail: str | None = None,
    host_fail: str | None = None,
    cleanup_fail: str | None = None,
    state_publish_fail: bool = False,
    state_remove_fail: bool = False,
    secret_unlink_fail: bool = False,
    active_host: bool = False,
) -> tuple[
    RuntimeAuthority,
    list[str],
    RecordingHost,
    RecordingStates,
    RecordingInstaller,
    RecordingSecrets,
]:
    events: list[str] = []
    host = RecordingHost(
        events,
        fail=host_fail,
        cleanup_fail=cleanup_fail,
    )
    host.agent_active = active_host
    host.dockerd_active = active_host
    host.nft_present = active_host
    states = RecordingStates(
        events,
        fail_publish=state_publish_fail,
        fail_remove=state_remove_fail,
    )
    states.snapshot = _snapshot(initial)
    installer = RecordingInstaller(events, fail=installer_fail)
    secrets = RecordingSecrets(events, fail_unlink=secret_unlink_fail)
    runtime = RuntimeAuthority(
        policy=_policy(),
        installer=installer,
        converger_factory=Unused(),
        conformance=Unused(),
        host=host,
        states=states,
        archives=Unused(),
        secrets=secrets,
    )
    return runtime, events, host, states, installer, secrets


def test_stage_agent_splits_ephemeral_secrets_binds_public_identity_then_publishes() -> None:
    runtime, events, host, states, installer, secrets = _transition_runtime(
        _prepared_state()
    )
    assert states.snapshot is not None

    receipt = runtime.dispatch(_stage_request(states.snapshot.sha256))

    assert events == [
        "state.read",
        "host.verify_inert:true",
        "secret.create",
        "installer.stage_agent_authorized",
        "installer.verify_staged",
        "host.verify_inert:true",
        "secret.unlink",
        "state.publish",
    ]
    assert secrets.present is False
    assert secrets.private_bytes == b""
    assert secrets.ca_bytes == b""
    assert installer.stage_arguments == {
        "agent_image": CURRENT_AGENT,
        "agent_instance_id": AGENT_INSTANCE_ID,
        "builder_image": CURRENT_BUILDER,
        "ca_file": Path("/ephemeral/service-ca"),
        "expected_public_key_sha256": PUBLIC_KEY_SHA256,
        "key_id": AGENT_KEY_ID,
        "private_key": Path("/ephemeral/private-key"),
        "service_url": SERVICE_ORIGIN,
    }
    assert installer.staged_present
    assert states.snapshot is not None
    assert dict(states.snapshot.value) == _staged_state()
    assert receipt["phase"] == "staged"
    assert receipt["state"] == _staged_state()
    assert PRIVATE_SEED not in encode_receipt(receipt)
    assert SERVICE_CA not in encode_receipt(receipt)
    assert not host.dockerd_active and not host.agent_active and not host.nft_present


def test_stage_agent_requires_exact_prepared_hash_and_release_identity() -> None:
    runtime, events, _, states, _, _ = _transition_runtime(_prepared_state())
    assert states.snapshot is not None
    with pytest.raises(AuthorityError, match="state_changed"):
        runtime.dispatch(_stage_request("f" * 64))
    assert events == ["state.read"]

    request = _stage_request(states.snapshot.sha256)
    values = dict(request.header.as_mapping())
    values["agent_image"] = (
        "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
        + "e" * 64
    )
    with pytest.raises(AuthorityError, match="release_identity_invalid"):
        runtime.dispatch(
            AuthorityRequest(AuthorityRequestHeader.from_mapping(values), request.payload)
        )


@pytest.mark.parametrize(
    ("failure_kind", "failure_value"),
    [
        ("installer", "installer.stage_agent_authorized"),
        ("installer", "installer.verify_staged"),
        ("host", "host.verify_inert:2"),
        ("secret", True),
        ("state", True),
    ],
)
def test_stage_agent_failure_after_each_mutation_discards_stage_and_secrets(
    failure_kind: str,
    failure_value: object,
) -> None:
    arguments: dict[str, object] = {}
    if failure_kind == "installer":
        arguments["installer_fail"] = failure_value
    elif failure_kind == "host":
        arguments["host_fail"] = failure_value
    elif failure_kind == "secret":
        arguments["secret_unlink_fail"] = failure_value
    else:
        arguments["state_publish_fail"] = failure_value
    runtime, events, host, states, installer, secrets = _transition_runtime(
        _prepared_state(),
        **arguments,
    )
    original = states.snapshot
    assert original is not None

    with pytest.raises(AuthorityError):
        runtime.dispatch(_stage_request(original.sha256))

    assert "installer.discard_agent_stage" in events
    assert "host.stop_agent" in events
    assert "host.stop_dockerd" in events
    assert "host.delete_nft" in events
    assert not installer.staged_present
    assert not secrets.present
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot == original


def test_activate_publishes_only_after_both_services_and_exact_nft_verify() -> None:
    runtime, events, host, states, _, _ = _transition_runtime(_staged_state())
    assert states.snapshot is not None

    receipt = runtime.dispatch(_state_request("activate", states.snapshot.sha256))

    assert events == [
        "state.read",
        "host.verify_inert:true",
        "host.load_nft",
        "host.start_dockerd",
        "host.start_agent",
        "installer.verify_active",
        "state.publish",
    ]
    assert host.dockerd_active and host.agent_active and host.nft_present
    assert states.snapshot is not None
    assert dict(states.snapshot.value) == _active_state()
    assert receipt["phase"] == "active"
    assert receipt["dockerd_service"] == "active"
    assert receipt["agent_service"] == "active"
    assert receipt["nft_table"] == "present"


@pytest.mark.parametrize(
    ("failure_kind", "failure_value"),
    [
        ("host", "host.load_nft"),
        ("host", "host.start_dockerd"),
        ("host", "host.start_agent"),
        ("installer", "installer.verify_active"),
        ("state", True),
    ],
)
def test_activate_failure_after_each_mutation_restores_staged_inert_state(
    failure_kind: str,
    failure_value: object,
) -> None:
    arguments: dict[str, object] = {}
    if failure_kind == "host":
        arguments["host_fail"] = failure_value
    elif failure_kind == "installer":
        arguments["installer_fail"] = failure_value
    else:
        arguments["state_publish_fail"] = failure_value
    runtime, events, host, states, _, _ = _transition_runtime(
        _staged_state(),
        **arguments,
    )
    original = states.snapshot
    assert original is not None

    with pytest.raises(AuthorityError):
        runtime.dispatch(_state_request("activate", original.sha256))

    assert events[-4:] == [
        "host.stop_agent",
        "host.stop_dockerd",
        "host.delete_nft",
        "host.verify_inert:true",
    ]
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot == original


def test_remove_stops_only_exact_runtime_preserves_cache_and_removes_state_last() -> None:
    runtime, events, host, states, _, _ = _transition_runtime(
        _active_state(),
        active_host=True,
    )
    assert states.snapshot is not None

    receipt = runtime.dispatch(_state_request("remove", states.snapshot.sha256))

    assert events == [
        "state.read",
        "host.stop_agent",
        "host.stop_dockerd",
        "host.delete_nft",
        "installer.remove",
        "host.verify_inert:true",
        "state.remove",
    ]
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot is None
    assert receipt["phase"] == "inert"
    assert receipt["state"] is None
    assert receipt["state_sha256"] == ""
    assert receipt["executable_new_capacity"] == 0


def test_remove_starts_inactive_daemon_for_live_inventory_before_removal() -> None:
    runtime, events, host, states, _, _ = _transition_runtime(_prepared_state())
    assert states.snapshot is not None

    runtime.dispatch(_state_request("remove", states.snapshot.sha256))

    assert events == [
        "state.read",
        "host.start_dockerd",
        "host.stop_agent",
        "host.stop_dockerd",
        "host.delete_nft",
        "installer.remove",
        "host.verify_inert:true",
        "state.remove",
    ]
    assert not host.dockerd_active


def test_remove_rejects_managed_objects_before_mutation() -> None:
    runtime, events, host, states, _, _ = _transition_runtime(
        _active_state(),
        active_host=True,
    )
    host.managed_containers = 1
    assert states.snapshot is not None

    with pytest.raises(AuthorityError, match="managed_objects_present"):
        runtime.dispatch(_state_request("remove", states.snapshot.sha256))

    assert events == ["state.read"]
    assert host.agent_active and host.dockerd_active and host.nft_present


@pytest.mark.parametrize(
    ("failure_kind", "failure_value"),
    [
        ("host", "host.stop_agent"),
        ("host", "host.stop_dockerd"),
        ("host", "host.delete_nft"),
        ("installer", "installer.remove"),
        ("host", "host.verify_inert"),
        ("state", True),
    ],
)
def test_remove_failure_after_each_mutation_still_compensates_exact_host_scope(
    failure_kind: str,
    failure_value: object,
) -> None:
    arguments: dict[str, object] = {"active_host": True}
    if failure_kind == "host":
        arguments["host_fail"] = failure_value
    elif failure_kind == "installer":
        arguments["installer_fail"] = failure_value
    else:
        arguments["state_remove_fail"] = failure_value
    runtime, events, host, states, _, _ = _transition_runtime(
        _active_state(),
        **arguments,
    )
    original = states.snapshot
    assert original is not None

    with pytest.raises(AuthorityError):
        runtime.dispatch(_state_request("remove", original.sha256))

    assert "host.stop_agent" in events
    assert "host.stop_dockerd" in events
    assert "host.delete_nft" in events
    assert not host.dockerd_active and not host.agent_active and not host.nft_present
    assert states.snapshot == original


def test_remove_retries_same_state_hash_after_state_deletion_failure() -> None:
    runtime, events, _, states, _, _ = _transition_runtime(
        _active_state(),
        active_host=True,
        state_remove_fail=True,
    )
    original = states.snapshot
    assert original is not None
    request = _state_request("remove", original.sha256)

    with pytest.raises(AuthorityError):
        runtime.dispatch(request)

    assert states.snapshot == original
    states.fail_remove = False
    receipt = runtime.dispatch(request)

    assert receipt["phase"] == "inert"
    assert states.snapshot is None
    assert events.count("installer.remove") == 2


def test_ephemeral_secret_files_use_exclusive_fixed_modes_and_unlink_on_base_exception(
    tmp_path: Path,
) -> None:
    root = tmp_path / "secrets"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    files = EphemeralSecretFiles(
        root=root,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        private_key_mode=0o400,
    )

    with pytest.raises(KeyboardInterrupt):
        with files.files(
            PRIVATE_SEED + SERVICE_CA,
            private_key_length=len(PRIVATE_SEED),
            service_ca_length=len(SERVICE_CA),
            request_id=REQUEST_ID,
        ) as paths:
            assert paths.private_key.read_bytes() == PRIVATE_SEED
            assert paths.ca_file.read_bytes() == SERVICE_CA
            assert stat.S_IMODE(paths.private_key.stat().st_mode) == 0o400
            assert stat.S_IMODE(paths.ca_file.stat().st_mode) == 0o444
            assert paths.private_key.stat().st_nlink == 1
            assert paths.ca_file.stat().st_nlink == 1
            raise KeyboardInterrupt
    assert list(root.iterdir()) == []


def test_ephemeral_secret_files_unlink_a_file_when_its_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "secrets"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    files = EphemeralSecretFiles(
        root=root,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        private_key_mode=0o400,
    )

    def fail_write(_descriptor: int, _payload: object) -> int:
        raise OSError("injected write failure")

    monkeypatch.setattr(authority_module.os, "write", fail_write)
    with pytest.raises(AuthorityError, match="secret_stage_invalid"):
        with files.files(
            PRIVATE_SEED + SERVICE_CA,
            private_key_length=len(PRIVATE_SEED),
            service_ca_length=len(SERVICE_CA),
            request_id=REQUEST_ID,
        ):
            raise AssertionError("unreachable")

    assert list(root.iterdir()) == []


def test_ephemeral_secrets_keep_root_directory_fd_across_path_replacement(
    tmp_path: Path,
) -> None:
    root = tmp_path / "secrets"
    displaced = tmp_path / "secrets-opened"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    files = EphemeralSecretFiles(
        root=root,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        private_key_mode=0o400,
    )

    with files.files(
        PRIVATE_SEED + SERVICE_CA,
        private_key_length=len(PRIVATE_SEED),
        service_ca_length=len(SERVICE_CA),
        request_id=REQUEST_ID,
    ) as paths:
        root.rename(displaced)
        root.mkdir(mode=0o700)
        root.chmod(0o700)
        assert paths.private_key.read_bytes() == PRIVATE_SEED
        assert paths.ca_file.read_bytes() == SERVICE_CA

    assert list(displaced.iterdir()) == []
    assert list(root.iterdir()) == []
