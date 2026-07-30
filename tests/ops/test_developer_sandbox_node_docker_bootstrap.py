from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_sandbox_node_docker_bootstrap as bootstrap

SHA = "a" * 40
TREE = "b" * 40
BUNDLE_DIGEST = "c" * 64
OPERATION_ID = "d" * 64


def _request(
    *,
    action: str = "authority-bootstrap",
    node: str = "oldlab-1",
    inputs: dict[str, str] | None = None,
    operation_id: str = OPERATION_ID,
    transport_expectation: str | None = None,
) -> dict[str, Any]:
    expectation = (
        ("absent" if action == "readback" else "not-checked")
        if transport_expectation is None
        else transport_expectation
    )
    unsigned = {
        "schema_version": bootstrap.SCHEMA_VERSION,
        "kind": bootstrap.KIND,
        "operation_id": operation_id,
        "transport_expectation": expectation,
        "action": action,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "candidate_bundle_sha256": BUNDLE_DIGEST,
        "expected_node": node,
        "inputs": {} if inputs is None else inputs,
    }
    return {
        **unsigned,
        "request_id": hashlib.sha256(bootstrap._canonical(unsigned)).hexdigest(),
    }


def _write_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: dict[str, Any],
) -> Path:
    path = tmp_path / "request.json"
    path.write_bytes(bootstrap._canonical(request))
    path.chmod(0o600)
    monkeypatch.setattr(bootstrap, "REQUEST_PATH", path)
    return path


def _install_exact_transport_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    drift_program: bool = False,
    drift_routes: bool = False,
) -> Path:
    repository = Path(__file__).resolve().parents[2]
    stage = tmp_path / "stage"
    candidate_root = stage / "source"
    candidate_program = candidate_root / bootstrap.TRANSPORT_PROGRAM_RELATIVE
    candidate_routes = candidate_root / bootstrap.TRANSPORT_ROUTES_RELATIVE
    candidate_program.parent.mkdir(parents=True, exist_ok=True)
    candidate_routes.parent.mkdir(parents=True, exist_ok=True)
    candidate_program.write_bytes((repository / bootstrap.TRANSPORT_PROGRAM_RELATIVE).read_bytes())
    candidate_routes.write_bytes((repository / bootstrap.TRANSPORT_ROUTES_RELATIVE).read_bytes())
    candidate_program.chmod(0o755)
    candidate_routes.chmod(0o644)

    installed_program = tmp_path / "host/loom-developer-sandbox-node-transport"
    installed_routes = tmp_path / "host/routes.toml"
    installed_program.parent.mkdir(parents=True, exist_ok=True)
    installed_program.write_bytes(
        candidate_program.read_bytes() + (b"\n# stale candidate\n" if drift_program else b"")
    )
    installed_routes.write_bytes(
        candidate_routes.read_bytes() + (b"\n# stale candidate\n" if drift_routes else b"")
    )
    installed_program.chmod(0o755)
    installed_routes.chmod(0o644)
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_PROGRAM", installed_program)
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_ROUTES", installed_routes)
    return stage


def test_mountinfo_parser_decodes_host_root_and_read_only_inputs() -> None:
    records = bootstrap._parse_mountinfo(
        "41 1 8:1 / /host rw,relatime - ext4 /dev/root rw\n"
        "42 1 8:1 /handoff/request /run/loom-node-bootstrap/request.json ro - ext4 /dev/root rw\n"
        "43 1 8:1 /handoff/input\\040dir /run/loom-node-bootstrap/input ro - ext4 /dev/root rw\n",
    )

    assert records == (
        bootstrap.MountRecord("/", "/host", frozenset({"rw", "relatime"})),
        bootstrap.MountRecord(
            "/handoff/request",
            "/run/loom-node-bootstrap/request.json",
            frozenset({"ro"}),
        ),
        bootstrap.MountRecord(
            "/handoff/input dir",
            "/run/loom-node-bootstrap/input",
            frozenset({"ro"}),
        ),
    )


def test_mountinfo_parser_rejects_malformed_rows() -> None:
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="mountinfo is invalid"):
        bootstrap._parse_mountinfo("41 1 8:1 / /host rw")


@pytest.mark.parametrize(
    "node",
    [
        *(f"oldlab-{index}" for index in range(1, 6)),
        *(f"trt-gb10-{index}" for index in range(1, 16)),
    ],
)
def test_request_is_canonical_digest_bound_for_every_infrastructure_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    node: str,
) -> None:
    request = _request(node=node)
    _write_request(tmp_path, monkeypatch, request)

    assert bootstrap._load_request() == request


@pytest.mark.parametrize(
    "node",
    ["oldlab-0", "oldlab-6", "trt-gb10-0", "trt-gb10-16", "gb10-7"],
)
def test_request_rejects_nodes_outside_the_closed_infrastructure_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    node: str,
) -> None:
    forbidden = _request(node=node)
    _write_request(tmp_path, monkeypatch, forbidden)
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="binding is invalid"):
        bootstrap._load_request()


def test_request_rejects_noncanonical_or_unexpected_authority_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(inputs={"oldlab2-controller.pub": "d" * 64})
    _write_request(tmp_path, monkeypatch, request)
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="unexpected trust inputs"):
        bootstrap._load_request()

    request = _request()
    path = _write_request(tmp_path, monkeypatch, request)
    path.write_text(json.dumps(request, indent=2), encoding="ascii")
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="binding is invalid"):
        bootstrap._load_request()


@pytest.mark.parametrize("action", sorted(bootstrap.ENVIRONMENT_AUTHORITY_ACTIONS))
def test_environment_authority_request_is_pinned_to_oldlab_2_without_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    request = _request(action=action, node="oldlab-2")
    _write_request(tmp_path, monkeypatch, request)
    assert bootstrap._load_request() == request

    wrong_node = _request(action=action, node="oldlab-1")
    _write_request(tmp_path, monkeypatch, wrong_node)
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="pinned to oldlab-2"):
        bootstrap._load_request()

    with_input = _request(action=action, node="oldlab-2", inputs={"caller": "d" * 64})
    _write_request(tmp_path, monkeypatch, with_input)
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="unexpected trust inputs"):
        bootstrap._load_request()


def test_request_rejects_invalid_operation_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(operation_id="not-a-sha256")
    _write_request(tmp_path, monkeypatch, request)

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="binding is invalid"):
        bootstrap._load_request()


@pytest.mark.parametrize(
    ("action", "expectation"),
    [
        ("authority-bootstrap", "server"),
        ("readback", "not-checked"),
    ],
)
def test_request_rejects_transport_expectation_outside_its_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    expectation: str,
) -> None:
    request = _request(action=action, transport_expectation=expectation)
    _write_request(tmp_path, monkeypatch, request)

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="binding is invalid"):
        bootstrap._load_request()


@pytest.mark.parametrize("machine", ["x86_64", "aarch64"])
def test_runtime_accepts_both_supported_linux_architectures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    machine: str,
) -> None:
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "41 1 8:1 / /host rw - ext4 /dev/root rw\n"
        "42 1 8:1 /r /run/loom-node-bootstrap/request.json ro - ext4 /dev/root rw\n"
        "43 1 8:1 /b /run/loom-node-bootstrap/candidate.bundle ro - ext4 /dev/root rw\n"
        "44 1 8:1 /i /run/loom-node-bootstrap/input ro - ext4 /dev/root rw\n",
        encoding="ascii",
    )
    host_root = tmp_path / "host"
    host_root.mkdir()
    monkeypatch.setattr(bootstrap, "MOUNTINFO", mountinfo)
    monkeypatch.setattr(bootstrap, "HOST_ROOT", host_root)
    monkeypatch.setattr(bootstrap, "_container_identity", lambda: True)
    monkeypatch.setattr(bootstrap.sys, "argv", [bootstrap.sys.argv[0]])
    monkeypatch.setattr(bootstrap.os, "getuid", lambda: 0)
    monkeypatch.setattr(bootstrap.os, "geteuid", lambda: 0)
    current = os.uname()
    monkeypatch.setattr(
        bootstrap.os,
        "uname",
        lambda: current.__class__(
            (current.sysname, current.nodename, current.release, current.version, machine),
        ),
    )

    # The parsed mount target remains the production /host constant even
    # though filesystem lstat is redirected to a fixture.
    monkeypatch.setattr(
        bootstrap,
        "_single_mount",
        lambda records, path: next(
            record
            for record in records
            if record.mount_point == ("/host" if path == host_root else str(path))
        ),
    )

    bootstrap._validate_runtime()


def test_runtime_rejects_non_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bootstrap.sys, "argv", [bootstrap.sys.argv[0]])
    monkeypatch.setattr(bootstrap.os, "getuid", lambda: 0)
    monkeypatch.setattr(bootstrap.os, "geteuid", lambda: 0)
    monkeypatch.setattr(bootstrap, "_container_identity", lambda: False)
    current = os.uname()
    monkeypatch.setattr(
        bootstrap.os,
        "uname",
        lambda: current.__class__(
            (current.sysname, current.nodename, current.release, current.version, "x86_64"),
        ),
    )

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="container channel"):
        bootstrap._validate_runtime()


def test_existing_foreign_directory_is_rejected_without_chmod(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state"
    path.mkdir(mode=0o755)
    path.chmod(0o755)

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="state is unsafe"):
        bootstrap._ensure_directory(path, 0o700)

    assert path.stat().st_mode & 0o777 == 0o755


def test_trust_input_inventory_is_exact_and_rejects_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_root = tmp_path / "input"
    input_root.mkdir()
    public_key = input_root / "role.pub"
    public_key.write_text("public\n", encoding="ascii")
    request = _request(
        action="transport-server-bootstrap",
        inputs={"role.pub": hashlib.sha256(public_key.read_bytes()).hexdigest()},
    )
    monkeypatch.setattr(bootstrap, "INPUT_ROOT", input_root)

    bootstrap._validate_input_inventory(request)

    (input_root / "extra.pub").write_text("extra\n", encoding="ascii")
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="not closed"):
        bootstrap._validate_input_inventory(request)

    (input_root / "extra.pub").unlink()
    public_key.unlink()
    public_key.symlink_to(tmp_path / "missing")
    with pytest.raises(
        bootstrap.DockerNodeBootstrapError,
        match=r"inventory is (?:unsafe|unavailable)",
    ):
        bootstrap._validate_input_inventory(request)


def test_launcher_bytes_must_match_the_exact_candidate_blob(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    running = tmp_path / "running.py"
    source = tmp_path / "source"
    checked_out = source / bootstrap.LAUNCHER_RELATIVE
    checked_out.parent.mkdir(parents=True)
    running.write_bytes(b"exact\n")
    checked_out.write_bytes(b"exact\n")
    running.chmod(0o644)
    checked_out.chmod(0o644)
    monkeypatch.setattr(bootstrap, "__file__", str(running))
    monkeypatch.setattr(bootstrap, "_checked", lambda *_args, **_kwargs: b"exact\n")

    bootstrap._validate_launcher_identity(source, SHA)

    checked_out.write_bytes(b"changed\n")
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="exact candidate"):
        bootstrap._validate_launcher_identity(source, SHA)


def test_fixed_git_commands_accept_bounded_diagnostic_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=b"payload",
            stderr=b"bundle is okay\n",
        ),
    )
    assert bootstrap._checked("/usr/bin/git", "bundle", "verify") == b"payload"

    monkeypatch.setattr(
        bootstrap,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=b"",
            stderr=b"x" * (bootstrap.MAX_FIXED_COMMAND_STDERR_BYTES + 1),
        ),
    )
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="fixed command"):
        bootstrap._checked("/usr/bin/git", "bundle", "verify")


def test_main_reports_bounded_bootstrap_phase(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        bootstrap,
        "execute",
        lambda: (_ for _ in ()).throw(
            bootstrap.DockerNodeBootstrapError("fixed command failed safely"),
        ),
    )

    assert bootstrap.main() == 1
    assert capsys.readouterr().err == "error: fixed command failed safely\n"


def test_transport_input_argv_never_interprets_file_content_as_a_path(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    input_root.mkdir()
    for name in ("oldlab2-controller", "oldlab2-controller.pub", "known_hosts"):
        (input_root / name).write_text("/etc/shadow\n", encoding="ascii")

    identities, public_keys, known_hosts = bootstrap._transport_input_argv(input_root)

    assert identities == [
        "--identity",
        f"oldlab2-controller={input_root / 'oldlab2-controller'}",
    ]
    assert public_keys == [
        "--public-key",
        f"oldlab2-controller={input_root / 'oldlab2-controller.pub'}",
    ]
    assert known_hosts == str(input_root / "known_hosts")


@pytest.mark.parametrize(
    ("action", "input_names", "expected_command"),
    [
        ("authority-bootstrap", (), "bootstrap"),
        ("authority-upgrade", (), "upgrade"),
        ("transport-server-bootstrap", ("role.pub",), "bootstrap-server"),
        (
            "transport-client-bootstrap",
            ("role", "role.pub", "known_hosts"),
            "bootstrap-client",
        ),
        ("transport-upgrade", (), "upgrade"),
        ("readback", (), "validate-install"),
    ],
)
def test_fixed_action_argv_is_closed_to_host_python_and_candidate_scripts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    input_names: tuple[str, ...],
    expected_command: str,
) -> None:
    request = _request(action=action)
    host_root = tmp_path / "host"
    stage = host_root / "run/loom-developer-sandbox-node-bootstrap" / request["request_id"]
    source = stage / "source"
    input_root = stage / "input"
    source.mkdir(parents=True)
    input_root.mkdir()
    for name in input_names:
        (input_root / name).write_text("opaque\n", encoding="ascii")
    monkeypatch.setattr(bootstrap, "HOST_ROOT", host_root)

    argv, cwd = bootstrap._fixed_action_argv(request, stage)

    assert argv[:3] == ["/usr/bin/python3", "-I", "-B"]
    assert expected_command in argv
    assert str(argv[3]).startswith(str(cwd) + "/scripts/ops/")
    assert cwd == Path("/") / stage.relative_to(host_root) / "source"
    assert "--execute" in argv if action != "readback" else "--execute" not in argv


def test_client_bootstrap_requires_known_hosts_before_host_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="transport-client-bootstrap")
    host_root = tmp_path / "host"
    stage = host_root / "run/loom-developer-sandbox-node-bootstrap" / request["request_id"]
    source = stage / "source"
    input_root = stage / "input"
    source.mkdir(parents=True)
    input_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_ROOT", host_root)

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="known_hosts is missing"):
        bootstrap._fixed_action_argv(request, stage)


@pytest.mark.parametrize("action", sorted(bootstrap.ENVIRONMENT_AUTHORITY_ACTIONS))
def test_environment_authority_argv_is_fixed_to_candidate_installer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    request = _request(action=action, node="oldlab-2")
    host_root = tmp_path / "host"
    stage = host_root / "run/loom-developer-sandbox-node-bootstrap" / request["request_id"]
    (stage / "source/scripts/ops").mkdir(parents=True)
    (stage / "input").mkdir()
    monkeypatch.setattr(bootstrap, "HOST_ROOT", host_root)

    argv, cwd = bootstrap._fixed_action_argv(request, stage)

    assert argv == [
        "/usr/bin/python3",
        "-I",
        "-B",
        str(cwd / "scripts/ops/developer_environment_authority_installer.py"),
        action,
        "--candidate-sha",
        SHA,
        "--candidate-tree",
        TREE,
    ]
    assert not {"--execute", "--uid", "--port", "--path"} & set(argv)


def test_fixed_environment_action_passes_host_python_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="environment-authority-upgrade", node="oldlab-2")
    host_root = tmp_path / "host"
    stage = host_root / "run/loom-developer-sandbox-node-bootstrap" / request["request_id"]
    (stage / "source/scripts/ops").mkdir(parents=True)
    (stage / "input").mkdir()
    monkeypatch.setattr(bootstrap, "HOST_ROOT", host_root)
    argv, cwd = bootstrap._fixed_action_argv(request, stage)
    report = {
        "schema_version": 1,
        "action": "environment-authority-upgrade",
        "node": "oldlab-2",
        "source_sha": SHA,
        "source_tree": TREE,
        "status": "succeeded",
    }

    def run(received: list[str], **kwargs: Any) -> Any:
        assert received == argv
        assert callable(kwargs["preexec_fn"])
        return type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": bootstrap._canonical(report),
                "stderr": b"",
            },
        )()

    monkeypatch.setattr(bootstrap.subprocess, "run", run)

    assert bootstrap._run_host_python(argv, cwd) == report


def test_clean_environment_uses_fixed_xdg_stage_without_home() -> None:
    container = bootstrap._clean_env()
    host = bootstrap._clean_env(host_chroot=True)

    assert "HOME" not in container
    assert container["XDG_CONFIG_HOME"] == str(bootstrap.HOST_STAGE_ROOT / "xdg-config")
    assert host["XDG_CONFIG_HOME"] == ("/run/loom-developer-sandbox-node-bootstrap/xdg-config")


def test_host_python_requires_fixed_candidate_script_and_canonical_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd = Path("/run/loom-developer-sandbox-node-bootstrap/request/source")
    report = {
        "schema_version": 1,
        "action": "bootstrap",
        "node": "oldlab-1",
        "source_sha": SHA,
        "source_tree": TREE,
        "status": "succeeded",
    }
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: Any) -> Any:
        calls.append(argv)
        assert callable(kwargs["preexec_fn"])
        return type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": bootstrap._canonical(report),
                "stderr": b"",
            },
        )()

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    argv = [
        "/usr/bin/python3",
        "-I",
        "-B",
        str(cwd / "scripts/ops/developer_sandbox_node_authority.py"),
        "bootstrap",
    ]

    assert bootstrap._run_host_python(argv, cwd) == report
    assert calls == [argv]

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="outside authority"):
        bootstrap._run_host_python(["/bin/sh", "-c", "id", "extra", "extra"], cwd)

    def rejected(_argv: list[str], **_kwargs: Any) -> Any:
        return type(
            "Completed",
            (),
            {
                "returncode": 1,
                "stdout": b"",
                "stderr": b"error: persistent host-root systemd view is invalid\n",
            },
        )()

    monkeypatch.setattr(bootstrap.subprocess, "run", rejected)
    with pytest.raises(
        bootstrap.DockerNodeBootstrapError,
        match="persistent host-root systemd view is invalid",
    ):
        bootstrap._run_host_python(argv, cwd)


def test_action_result_requires_exact_candidate_and_node() -> None:
    request = _request(action="authority-bootstrap")
    good = {
        "node": "oldlab-1",
        "source_sha": SHA,
        "source_tree": TREE,
        "status": "succeeded",
    }

    bootstrap._validate_action_result(request, good)

    for field, value in (
        ("node", "oldlab-2"),
        ("source_sha", "d" * 40),
        ("source_tree", "e" * 40),
    ):
        changed = {**good, field: value}
        with pytest.raises(bootstrap.DockerNodeBootstrapError):
            bootstrap._validate_action_result(request, changed)


def test_client_action_result_is_bound_to_the_exact_initiator() -> None:
    request = _request(action="transport-client-bootstrap")
    good = {
        "initiator": "oldlab-1",
        "status": "succeeded",
    }

    bootstrap._validate_action_result(request, good)

    for changed in (
        {**good, "initiator": "oldlab-2"},
        {"node": "oldlab-1", "status": "succeeded"},
    ):
        with pytest.raises(bootstrap.DockerNodeBootstrapError):
            bootstrap._validate_action_result(request, changed)


def test_readback_binds_client_to_initiator_and_server_to_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="readback", transport_expectation="client-server")
    stage = _install_exact_transport_binding(tmp_path, monkeypatch)
    client_policy = tmp_path / "client-policy.json"
    server_policy = tmp_path / "server-policy.json"
    client_policy.touch()
    server_policy.touch()
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_CLIENT_POLICY", client_policy)
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_SERVER_POLICY", server_policy)
    monkeypatch.setattr(
        bootstrap,
        "_fixed_action_argv",
        lambda *_args, **_kwargs: ([], tmp_path / "source"),
    )
    reports = iter(
        (
            {
                "action": "validate-install",
                "node": "oldlab-1",
                "source_sha": SHA,
                "source_tree": TREE,
                "status": "succeeded",
            },
            {
                "action": "check-client",
                "initiator": "oldlab-1",
                "status": "succeeded",
            },
            {
                "action": "check-server",
                "node": "oldlab-1",
                "status": "succeeded",
            },
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_host_python",
        lambda *_args, **_kwargs: next(reports),
    )

    result = bootstrap._run_chroot_action(request, stage)

    assert result["transport_client"]["initiator"] == "oldlab-1"
    assert result["transport_server"]["node"] == "oldlab-1"
    assert result["transport_candidate_binding"] == {
        "schema_version": bootstrap.SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.transport-candidate-binding",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "program_sha256": hashlib.sha256(
            (stage / "source" / bootstrap.TRANSPORT_PROGRAM_RELATIVE).read_bytes()
        ).hexdigest(),
        "route_config_sha256": hashlib.sha256(
            (stage / "source" / bootstrap.TRANSPORT_ROUTES_RELATIVE).read_bytes()
        ).hexdigest(),
        "status": "succeeded",
    }


def test_readback_rejects_transport_client_initiator_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="readback", transport_expectation="client-server")
    stage = _install_exact_transport_binding(tmp_path, monkeypatch)
    client_policy = tmp_path / "client-policy.json"
    server_policy = tmp_path / "server-policy.json"
    client_policy.touch()
    server_policy.touch()
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_CLIENT_POLICY", client_policy)
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_SERVER_POLICY", server_policy)
    monkeypatch.setattr(
        bootstrap,
        "_fixed_action_argv",
        lambda *_args, **_kwargs: ([], tmp_path / "source"),
    )
    reports = iter(
        (
            {
                "action": "validate-install",
                "node": "oldlab-1",
                "source_sha": SHA,
                "source_tree": TREE,
                "status": "succeeded",
            },
            {
                "action": "check-client",
                "initiator": "oldlab-2",
                "status": "succeeded",
            },
            {
                "action": "check-server",
                "node": "oldlab-1",
                "status": "succeeded",
            },
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_host_python",
        lambda *_args, **_kwargs: next(reports),
    )

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="transport readback"):
        bootstrap._run_chroot_action(request, stage)


@pytest.mark.parametrize(
    ("drift_program", "drift_routes"),
    [(True, False), (False, True)],
)
def test_readback_rejects_self_consistent_transport_from_another_candidate(
    drift_program: bool,
    drift_routes: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="readback", transport_expectation="server")
    stage = _install_exact_transport_binding(
        tmp_path,
        monkeypatch,
        drift_program=drift_program,
        drift_routes=drift_routes,
    )
    server_policy = tmp_path / "server-policy.json"
    server_policy.touch()
    monkeypatch.setattr(
        bootstrap,
        "HOST_TRANSPORT_CLIENT_POLICY",
        tmp_path / "absent-client-policy.json",
    )
    monkeypatch.setattr(bootstrap, "HOST_TRANSPORT_SERVER_POLICY", server_policy)
    monkeypatch.setattr(
        bootstrap,
        "_fixed_action_argv",
        lambda *_args, **_kwargs: ([], tmp_path / "source"),
    )
    reports = iter(
        (
            {
                "action": "validate-install",
                "node": "oldlab-1",
                "source_sha": SHA,
                "source_tree": TREE,
                "status": "succeeded",
            },
            {
                "action": "check-server",
                "node": "oldlab-1",
                "status": "succeeded",
            },
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_host_python",
        lambda *_args, **_kwargs: next(reports),
    )

    with pytest.raises(
        bootstrap.DockerNodeBootstrapError,
        match="transport exact-candidate binding drifted",
    ):
        bootstrap._run_chroot_action(request, stage)


def test_readback_requires_the_bound_transport_install_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="readback", transport_expectation="server")
    monkeypatch.setattr(
        bootstrap,
        "HOST_TRANSPORT_CLIENT_POLICY",
        tmp_path / "absent-client-policy.json",
    )
    monkeypatch.setattr(
        bootstrap,
        "HOST_TRANSPORT_SERVER_POLICY",
        tmp_path / "absent-server-policy.json",
    )
    monkeypatch.setattr(
        bootstrap,
        "_fixed_action_argv",
        lambda *_args, **_kwargs: ([], tmp_path / "source"),
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_host_python",
        lambda *_args, **_kwargs: {
            "action": "validate-install",
            "node": "oldlab-1",
            "source_sha": SHA,
            "source_tree": TREE,
            "status": "succeeded",
        },
    )

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="readback is incomplete"):
        bootstrap._run_chroot_action(request, tmp_path / "stage")


def test_existing_receipt_is_replay_bound_without_rewriting_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    result = {
        "node": "oldlab-1",
        "source_sha": SHA,
        "source_tree": TREE,
        "status": "succeeded",
    }
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_RECEIPT_ROOT", receipt_root)

    first = bootstrap._write_receipt(request, result)
    second = bootstrap._write_receipt(request, result)

    assert second == first
    assert (receipt_root / f"{request['request_id']}.json").read_bytes() == bootstrap._canonical(
        first,
    )

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="replay drifted"):
        bootstrap._write_receipt(request, {**result, "idempotent": True})


def test_distinct_operation_ids_preserve_successive_readback_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_request = _request(action="readback", operation_id="1" * 64)
    second_request = _request(
        action="readback",
        operation_id="2" * 64,
        transport_expectation="client-server",
    )
    first_result = {
        "node": "oldlab-1",
        "source_sha": SHA,
        "source_tree": TREE,
        "transport_client": None,
        "transport_server": None,
        "transport_candidate_binding": None,
        "status": "succeeded",
    }
    second_result = {
        "node": "oldlab-1",
        "source_sha": SHA,
        "source_tree": TREE,
        "transport_client": {
            "initiator": "oldlab-1",
            "status": "succeeded",
        },
        "transport_server": {
            "node": "oldlab-1",
            "status": "succeeded",
        },
        "transport_candidate_binding": {
            "schema_version": bootstrap.SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.transport-candidate-binding",
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "program_sha256": "3" * 64,
            "route_config_sha256": "4" * 64,
            "status": "succeeded",
        },
        "status": "succeeded",
    }
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_RECEIPT_ROOT", receipt_root)

    first = bootstrap._write_receipt(first_request, first_result)
    second = bootstrap._write_receipt(second_request, second_result)

    assert first_request["request_id"] != second_request["request_id"]
    assert first["result_sha256"] != second["result_sha256"]
    assert first["operation_id"] == "1" * 64
    assert second["operation_id"] == "2" * 64
    assert {path.name for path in receipt_root.iterdir() if not path.name.startswith(".")} == {
        f"{first_request['request_id']}.json",
        f"{second_request['request_id']}.json",
    }


def test_environment_authority_receipt_binds_installed_assets_and_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="environment-authority-bootstrap", node="oldlab-2")
    result = {
        "schema_version": 1,
        "action": request["action"],
        "node": "oldlab-2",
        "source_sha": SHA,
        "source_tree": TREE,
        "installed_asset_digests": {"/usr/local/bin/tool": "e" * 64},
        "node_capacity_contract_sha256": "d" * 64,
        "registry_snapshot_sha256": "f" * 64,
        "status": "succeeded",
    }
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_RECEIPT_ROOT", receipt_root)

    receipt = bootstrap._write_receipt(request, result)

    assert receipt["installed_asset_digests"] == result["installed_asset_digests"]
    assert receipt["node_capacity_contract_sha256"] == "d" * 64
    assert receipt["registry_snapshot_sha256"] == "f" * 64
    assert bootstrap._write_receipt(request, result) == receipt

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="receipt evidence"):
        bootstrap._write_receipt(request, {**result, "registry_snapshot_sha256": "bad"})
    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="receipt evidence"):
        bootstrap._write_receipt(
            request,
            {**result, "node_capacity_contract_sha256": "bad"},
        )


def test_exact_request_replay_returns_persisted_environment_result_without_rerun(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(action="environment-authority-bootstrap", node="oldlab-2")
    result = {
        "schema_version": 1,
        "action": request["action"],
        "node": "oldlab-2",
        "source_sha": SHA,
        "source_tree": TREE,
        "installed_asset_digests": {"/usr/local/bin/tool": "e" * 64},
        "node_capacity_contract_sha256": "d" * 64,
        "registry_snapshot_sha256": "f" * 64,
        "idempotent": False,
        "status": "succeeded",
    }
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_RECEIPT_ROOT", receipt_root)
    calls = {"prepare": 0, "action": 0, "cleanup": 0}
    stage = tmp_path / "stage"

    def prepare(
        _request: dict[str, Any],
        _bundle: bytes,
    ) -> tuple[Path, Path]:
        calls["prepare"] += 1
        return stage, stage / "source"

    def action(
        _request: dict[str, Any],
        _stage: Path,
    ) -> dict[str, Any]:
        calls["action"] += 1
        return result

    monkeypatch.setattr(bootstrap, "_prepare_stage", prepare)
    monkeypatch.setattr(bootstrap, "_run_chroot_action", action)
    monkeypatch.setattr(
        bootstrap,
        "_remove_stage",
        lambda _stage: calls.__setitem__("cleanup", calls["cleanup"] + 1),
    )

    first = bootstrap._execute_locked(request, b"bundle")
    replay = bootstrap._execute_locked(request, b"bundle")

    assert replay == first
    assert replay["result"]["idempotent"] is False
    assert calls == {"prepare": 1, "action": 1, "cleanup": 1}


@pytest.mark.parametrize(
    "tamper",
    ("result", "result-sha", "projection"),
)
def test_receipt_tamper_fails_before_stage_or_host_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    request = _request(action="environment-authority-bootstrap", node="oldlab-2")
    result = {
        "schema_version": 1,
        "action": request["action"],
        "node": "oldlab-2",
        "source_sha": SHA,
        "source_tree": TREE,
        "installed_asset_digests": {"/usr/local/bin/tool": "e" * 64},
        "node_capacity_contract_sha256": "d" * 64,
        "registry_snapshot_sha256": "f" * 64,
        "idempotent": False,
        "status": "succeeded",
    }
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir()
    monkeypatch.setattr(bootstrap, "HOST_RECEIPT_ROOT", receipt_root)
    bootstrap._write_receipt(request, result)
    path = receipt_root / f"{request['request_id']}.json"
    receipt = json.loads(path.read_bytes())
    if tamper == "result":
        receipt["result"]["idempotent"] = True
    elif tamper == "result-sha":
        receipt["result_sha256"] = "0" * 64
    else:
        receipt["node_capacity_contract_sha256"] = "0" * 64
    path.write_bytes(bootstrap._canonical(receipt))
    path.chmod(0o600)
    monkeypatch.setattr(
        bootstrap,
        "_prepare_stage",
        lambda *_args: (_ for _ in ()).throw(AssertionError("stage must not run")),
    )
    monkeypatch.setattr(
        bootstrap,
        "_run_chroot_action",
        lambda *_args: (_ for _ in ()).throw(AssertionError("action must not run")),
    )

    with pytest.raises(bootstrap.DockerNodeBootstrapError, match="replay drifted"):
        bootstrap._execute_locked(request, b"bundle")


def test_containerfile_is_fixed_multiarch_one_shot_without_runtime_daemon() -> None:
    root = Path(__file__).resolve().parents[2]
    payload = (root / "deploy/developer-sandboxes/Containerfile.node-bootstrap").read_text(
        encoding="utf-8"
    )

    assert "amd64|arm64" in payload
    assert "ENTRYPOINT" in payload
    assert "developer_sandbox_node_docker_bootstrap.py" in payload
    assert "XDG_CONFIG_HOME=/run/loom-developer-sandbox-node-bootstrap/xdg-config" in payload
    assert "CMD " not in payload
    assert "systemd" not in payload
    assert "sshd" not in payload
