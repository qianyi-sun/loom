from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_node_transport as transport

RUNBOOK = Path(__file__).resolve().parents[2] / "docs/runbooks/developer-sandbox-node-transport.md"


def _public_key(seed: int = 1) -> bytes:
    algorithm = b"ssh-ed25519"
    raw = bytes([seed]) * 32
    blob = len(algorithm).to_bytes(4, "big") + algorithm + len(raw).to_bytes(4, "big") + raw
    return b"ssh-ed25519 " + base64.b64encode(blob) + b" test-fixture\n"


def _layout(tmp_path: Path) -> transport.Layout:
    libexec_dir = tmp_path / "libexec"
    libexec_dir.mkdir(mode=0o755)
    home = tmp_path / "qianyi"
    home.mkdir(mode=0o700)
    return transport.Layout(
        root=tmp_path / "transport",
        libexec=libexec_dir / "loom-developer-sandbox-node-transport",
        authorized_keys=home / ".ssh/authorized_keys",
        operator_uid=os.getuid(),
        operator_gid=os.getgid(),
    )


def _known_hosts(config: transport.TransportConfig, initiator: str) -> bytes:
    key = _public_key().decode("ascii").split()
    return "".join(
        f"{endpoint} {key[0]} {key[1]}\n"
        for endpoint in sorted(transport._required_known_hosts(config, initiator))
    ).encode("ascii")


def _fixture_file(path: Path, payload: bytes, mode: int) -> Path:
    path.write_bytes(payload)
    path.chmod(mode)
    return path


def _allow_ambient_tmp_ancestors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trust only pre-existing writable ancestors of pytest's temporary root."""
    safe_external_directory = transport._safe_external_directory
    ambient_writable_ancestors: set[tuple[int, int]] = set()
    for ancestor in tmp_path.parents:
        metadata = ancestor.stat()
        if (
            stat.S_ISDIR(metadata.st_mode)
            and metadata.st_uid in {0, os.getuid()}
            and stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            ambient_writable_ancestors.add((metadata.st_dev, metadata.st_ino))

    def safe(
        metadata: os.stat_result,
        *,
        expected_uid: int,
    ) -> bool:
        return (
            safe_external_directory(
                metadata,
                expected_uid=expected_uid,
            )
            or (metadata.st_dev, metadata.st_ino) in ambient_writable_ancestors
        )

    monkeypatch.setattr(transport, "_safe_external_directory", safe)


def _simulate_root_filesystem(monkeypatch: pytest.MonkeyPatch) -> None:
    uid = os.getuid()
    gid = os.getgid()
    safe_external = transport._safe_external_file
    ensure_directory = transport._ensure_directory
    install_once = transport._install_once

    def safe(
        path: Path,
        *,
        modes: frozenset[int],
        limit: int,
        expected_uid: int = 0,
        validate_parents: bool = False,
    ) -> bytes:
        return safe_external(
            path,
            modes=modes,
            limit=limit,
            expected_uid=uid if expected_uid == 0 else expected_uid,
            validate_parents=validate_parents,
        )

    def ensure(
        path: Path,
        *,
        mode: int,
        uid: int = 0,
        gid: int = 0,
    ) -> None:
        ensure_directory(
            path,
            mode=mode,
            uid=os.getuid() if uid == 0 else uid,
            gid=os.getgid() if gid == 0 else gid,
        )

    def install(
        path: Path,
        payload: bytes,
        *,
        mode: int,
        uid: int = 0,
        gid: int = 0,
    ) -> bool:
        return install_once(
            path,
            payload,
            mode=mode,
            uid=os.getuid() if uid == 0 else uid,
            gid=os.getgid() if gid == 0 else gid,
        )

    monkeypatch.setattr(transport, "_safe_external_file", safe)
    monkeypatch.setattr(transport, "_ensure_directory", ensure)
    monkeypatch.setattr(transport, "_install_once", install)
    monkeypatch.setattr(transport, "ROOT_UID", uid)
    monkeypatch.setattr(transport, "ROOT_GID", gid)
    monkeypatch.setattr(transport.os, "geteuid", lambda: 0)
    monkeypatch.setattr(transport, "_require_persistent_install_root", lambda: None)
    monkeypatch.setattr(transport, "_hostname", lambda: "trt-eai-oldlab-2")


def _bootstrap_oldlab2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[transport.Layout, dict[str, Path], dict[str, Path], Path]:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    _simulate_root_filesystem(monkeypatch)
    config = transport.load_config()
    layout = _layout(tmp_path)
    source = tmp_path / "external-root"
    source.mkdir(mode=0o700)
    client_roles = transport._client_roles(config, "oldlab-2")
    server_roles = transport._server_roles(config, "oldlab-2")
    all_roles = client_roles | server_roles
    public_keys = {
        role: _fixture_file(
            source / f"{role}.pub",
            _public_key(index),
            0o600,
        )
        for index, role in enumerate(sorted(all_roles), start=1)
    }
    identities = {
        role: _fixture_file(
            source / f"{role}.identity",
            f"private-fixture-{role}".encode(),
            0o600,
        )
        for role in client_roles
    }
    known_hosts = _fixture_file(
        source / "known_hosts",
        _known_hosts(config, "oldlab-2"),
        0o600,
    )
    transport.bootstrap_client(
        identity_sources=identities,
        public_key_sources={role: public_keys[role] for role in client_roles},
        known_hosts_source=known_hosts,
        execute=True,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: public_keys[path.name.removesuffix(".identity")].read_bytes(),
    )
    transport.bootstrap_server(
        public_key_sources={role: public_keys[role] for role in server_roles},
        execute=True,
        layout=layout,
        expected_root_uid=os.getuid(),
    )
    return layout, identities, public_keys, known_hosts


def test_checked_in_route_is_closed_and_includes_infrastructure_only_node() -> None:
    config = transport.load_config()

    assert len(config.nodes) == 20
    assert config.nodes["trt-gb10-7"].hostname == "gx10-0faf"
    assert config.roles["oldlab2-controller"].verbs == {"transact", "check"}
    assert "trt-gb10-7" in config.roles["oldlab2-controller"].targets
    assert config.roles["oldlab1-publisher"].verbs == {"transact", "check"}
    assert set(config.roles["oldlab1-publisher"].targets) == {
        "oldlab-1",
        "oldlab-2",
        "oldlab-3",
        "oldlab-4",
        "oldlab-5",
        "trt-gb10-1",
    }
    assert config.roles["gb10-1-publisher"].verbs == {"check"}
    assert set(config.roles["gb10-1-publisher"].targets) >= {
        "oldlab-1",
        "oldlab-2",
        "trt-gb10-7",
    }
    assert config.roles["gb10-1-oldlab-jump"].targets == ("oldlab-2",)
    assert config.roles["oldlab2-gb10-jump"].kind == "proxy"
    assert set(config.roles["oldlab2-gb10-jump"].targets) == {
        f"trt-gb10-{index}" for index in range(2, 16)
    }


def test_bootstrap_inventory_is_secret_free_canonical_and_machine_derived(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = transport.load_config()
    monkeypatch.setattr(
        transport,
        "_require_persistent_install_root",
        lambda: pytest.fail("read-only inventory attempted to require root"),
    )

    report = transport.bootstrap_inventory()

    assert set(report) == {
        "schema_version",
        "kind",
        "source",
        "mutation_authorized",
        "informational_only",
        "node_count",
        "initiator_count",
        "excluded_nodes",
        "nodes",
        "initiators",
    }
    assert report == {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.node-transport-bootstrap-inventory",
        "source": "deploy/developer-sandboxes/node-authority-transport.toml",
        "mutation_authorized": False,
        "informational_only": True,
        "node_count": 20,
        "initiator_count": 3,
        "excluded_nodes": [],
        "nodes": [
            {
                "node": node,
                "canonical_hostname": config.nodes[node].hostname,
                "server_roles": sorted(transport._server_roles(config, node)),
            }
            for node in config.nodes
        ],
        "initiators": [
            {
                "node": initiator,
                "canonical_hostname": config.nodes[initiator].hostname,
                "client_roles": sorted(transport._client_roles(config, initiator)),
                "required_known_hosts_endpoints": sorted(
                    transport._required_known_hosts(config, initiator),
                ),
            }
            for initiator in ("oldlab-1", "oldlab-2", "trt-gb10-1")
        ],
    }
    assert {row["node"] for row in report["nodes"]} == set(config.nodes)
    assert not {
        "request_id",
        "receipt",
        "status",
        "execute",
        "identity_path",
        "private_key",
        "public_key",
        "known_hosts_path",
    } & set(report)

    assert transport.main(["bootstrap-inventory"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == transport._canonical_json(report).decode("ascii")

    parser = transport._parser()
    for forbidden in ("--execute", "--identity", "--known-hosts", "/tmp/input"):
        with pytest.raises(SystemExit):
            parser.parse_args(["bootstrap-inventory", forbidden])


def test_runbook_role_matrix_and_publisher_verbs_match_checked_in_config() -> None:
    config = transport.load_config()
    lines = RUNBOOK.read_text(encoding="utf-8").splitlines()
    assert (
        "python3 scripts/ops/developer_sandbox_node_transport.py bootstrap-inventory"
        in "\n".join(lines)
    )
    assert "The output is informational inventory, not an authority receipt" in "\n".join(lines)
    groups = (
        ("`oldlab-1`", ("oldlab-1",)),
        ("`oldlab-2`", ("oldlab-2",)),
        (
            "`oldlab-3` through `oldlab-5`",
            tuple(f"oldlab-{index}" for index in range(3, 6)),
        ),
        ("`trt-gb10-1`", ("trt-gb10-1",)),
        (
            "`trt-gb10-2` through `trt-gb10-15`",
            tuple(f"trt-gb10-{index}" for index in range(2, 16)),
        ),
    )
    assert {node for _label, nodes in groups for node in nodes} == set(config.nodes)
    expected_role_table = [
        "| Server node(s) | Exact public-key roles |",
        "| --- | --- |",
    ]
    for label, nodes in groups:
        role_sets = {tuple(sorted(transport._server_roles(config, node))) for node in nodes}
        assert len(role_sets) == 1
        rendered_roles = ", ".join(f"`{role}`" for role in role_sets.pop())
        expected_role_table.append(f"| {label} | {rendered_roles} |")
    role_table_start = lines.index(expected_role_table[0])
    assert lines[role_table_start : role_table_start + len(expected_role_table)] == (
        expected_role_table
    )
    assert lines[role_table_start + len(expected_role_table)] == ""

    publisher_roles = ("oldlab1-publisher", "gb10-1-publisher")
    expected_verb_table = [
        "| Publisher authority role | Exact verbs |",
        "| --- | --- |",
        *[
            "| `{role}` | {verbs} |".format(
                role=role,
                verbs=", ".join(f"`{verb}`" for verb in sorted(config.roles[role].verbs)),
            )
            for role in publisher_roles
        ],
    ]
    verb_table_start = lines.index(expected_verb_table[0])
    assert lines[verb_table_start : verb_table_start + len(expected_verb_table)] == (
        expected_verb_table
    )
    assert lines[verb_table_start + len(expected_verb_table)] == ""
    assert config.roles["oldlab1-publisher"].verbs == {"check", "transact"}
    assert config.roles["gb10-1-publisher"].verbs == {"check"}
    assert "`oldlab1-publisher` may perform its fixed publication transactions" in "\n".join(lines)
    assert "`gb10-1-publisher` is check-only" in "\n".join(lines)


def test_routes_distinguish_oldlab2_jump_from_gb10_direct_access() -> None:
    config = transport.load_config()

    controller = config.route("oldlab-2", "trt-gb10-14")
    publisher = config.route("trt-gb10-1", "trt-gb10-14")

    assert config.route("oldlab-1", "oldlab-1") == transport.Route(
        initiator="oldlab-1",
        node="oldlab-1",
        address="192.168.50.103",
        port=22,
        jump=None,
    )
    assert config.route("oldlab-2", "oldlab-1") == transport.Route(
        initiator="oldlab-2",
        node="oldlab-1",
        address="192.168.50.103",
        port=22,
        jump=None,
    )
    assert controller == transport.Route(
        initiator="oldlab-2",
        node="trt-gb10-14",
        address="192.168.20.24",
        port=22,
        jump="trt-gb10-1",
    )
    assert publisher.jump is None
    assert config.route("oldlab-2", "trt-gb10-7") == transport.Route(
        initiator="oldlab-2",
        node="trt-gb10-7",
        address="192.168.20.77",
        port=22,
        jump="trt-gb10-1",
    )
    assert config.route("trt-gb10-1", "trt-gb10-7") == transport.Route(
        initiator="trt-gb10-1",
        node="trt-gb10-7",
        address="192.168.20.77",
        port=22,
        jump=None,
    )
    assert config.route("oldlab-2", "trt-gb10-1").port == 2221
    assert config.route("trt-gb10-1", "oldlab-1") == transport.Route(
        initiator="trt-gb10-1",
        node="oldlab-1",
        address="207.35.188.227",
        port=2321,
        jump=None,
    )
    assert config.route("trt-gb10-1", "oldlab-2").jump == "oldlab-1"


@pytest.mark.parametrize(
    ("role", "command", "expected"),
    [
        (
            "oldlab2-controller",
            "/usr/bin/sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority transact",
            ("authority", "transact"),
        ),
        (
            "oldlab2-controller",
            "/usr/bin/sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority check",
            ("authority", "check"),
        ),
        ("oldlab2-gb10-jump", "proxy trt-gb10-14", ("proxy", "trt-gb10-14")),
        ("oldlab2-gb10-jump", "proxy trt-gb10-7", ("proxy", "trt-gb10-7")),
    ],
)
def test_forced_dispatch_maps_only_closed_commands(
    role: str,
    command: str,
    expected: tuple[str, str],
) -> None:
    assert transport._forced_action(transport.load_config(), role, command) == expected


@pytest.mark.parametrize(
    ("role", "command"),
    [
        (
            "gb10-1-publisher",
            "/usr/bin/sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority transact",
        ),
        (
            "oldlab2-controller",
            "sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority check",
        ),
        (
            "oldlab2-controller",
            "/usr/bin/sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority check extra",
        ),
        ("oldlab2-gb10-jump", "proxy trt-gb10-16"),
        ("oldlab2-gb10-jump", "proxy trt-gb10-14 22"),
        ("oldlab2-gb10-jump", "sh"),
    ],
)
def test_forced_dispatch_rejects_shell_widening(role: str, command: str) -> None:
    with pytest.raises(transport.TransportError, match="not approved"):
        transport._forced_action(transport.load_config(), role, command)


def test_authorized_key_is_forced_and_restricted() -> None:
    config = transport.load_config()
    line = transport._authorized_key_line(
        config.roles["oldlab2-controller"],
        _public_key(),
    )

    assert line.startswith('restrict,command="/usr/local/libexec/')
    assert ' forced oldlab2-controller"' in line
    assert "ssh-ed25519" in line
    assert "loom-developer-sandbox-transport:oldlab2-controller" in line
    assert "permitopen" not in line


def test_ssh_argv_has_no_ambient_agent_password_or_user_override(
    tmp_path: Path,
) -> None:
    config = transport.load_config()
    layout = _layout(tmp_path)
    argv = transport._remote_ssh_argv(
        config,
        layout,
        initiator="oldlab-2",
        node="trt-gb10-14",
        verb="check",
    )

    joined = " ".join(argv)
    assert argv[0] == "/usr/bin/ssh"
    assert "-F /dev/null" in joined
    assert "IdentityAgent=none" in joined
    assert "IdentitiesOnly=yes" in joined
    assert "PasswordAuthentication=no" in joined
    assert "KbdInteractiveAuthentication=no" in joined
    assert "StrictHostKeyChecking=yes" in joined
    assert "GlobalKnownHostsFile=/dev/null" in joined
    assert "UpdateHostKeys=no" in joined
    assert "ClearAllForwardings=yes" in joined
    assert f"ProxyCommand={layout.libexec} proxy-client --node trt-gb10-14" in joined
    assert f"-i {layout.identity('oldlab2-controller')}" in joined
    assert "-l qianyi" in joined
    assert joined.endswith(
        "/usr/bin/sudo -n /usr/local/libexec/loom-developer-sandbox-node-authority check",
    )
    assert "SSH_AUTH_SOCK" not in transport._clean_env()


def test_local_authority_uses_qianyi_then_the_fixed_sudo_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    layout.root.mkdir(mode=0o700)
    layout.config.write_bytes(transport.CHECKED_IN_CONFIG.read_bytes())
    captured: dict[str, object] = {}

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, b'{"status":"succeeded"}\n', b"")

    monkeypatch.setattr(transport.os, "geteuid", lambda: 0)
    monkeypatch.setattr(transport, "_require_persistent_install_root", lambda: None)
    monkeypatch.setattr(transport, "_hostname", lambda: "trt-eai-oldlab-2")
    monkeypatch.setattr(transport, "validate_client_install", lambda _layout: {})

    transport.invoke("oldlab-2", "check", b"{}\n", layout=layout, run=run)

    assert captured["argv"] == [
        "/usr/sbin/runuser",
        "-u",
        "qianyi",
        "--",
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/loom-developer-sandbox-node-authority",
        "check",
    ]


def test_client_bootstrap_plan_pins_external_assets_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    config = transport.load_config()
    layout = _layout(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    identities: dict[str, Path] = {}
    public_keys: dict[str, Path] = {}
    for index, role in enumerate(
        ("oldlab2-controller", "oldlab2-gb10-jump"),
        start=1,
    ):
        identities[role] = _fixture_file(
            source / f"{role}.identity",
            f"fixture-private-{index}".encode(),
            0o600,
        )
        public_keys[role] = _fixture_file(
            source / f"{role}.pub",
            _public_key(index),
            0o600,
        )
    known_hosts = _fixture_file(
        source / "known_hosts",
        _known_hosts(config, "oldlab-2"),
        0o600,
    )

    monkeypatch.setattr(transport.os, "geteuid", lambda: 0)
    monkeypatch.setattr(transport, "_require_persistent_install_root", lambda: None)
    monkeypatch.setattr(transport, "_hostname", lambda: "trt-eai-oldlab-2")

    result = transport.bootstrap_client(
        identity_sources=identities,
        public_key_sources=public_keys,
        known_hosts_source=known_hosts,
        execute=False,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: public_keys[path.stem.replace(".identity", "")].read_bytes(),
    )

    assert result["mutation_authorized"] is False
    assert result["roles"] == ["oldlab2-controller", "oldlab2-gb10-jump"]
    assert not layout.root.exists()
    assert "fixture-private" not in str(result)


def test_server_bootstrap_plan_does_not_create_ssh_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    config = transport.load_config()
    layout = _layout(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    roles = transport._server_roles(config, "trt-gb10-1")
    public_keys = {
        role: _fixture_file(source / f"{role}.pub", _public_key(index), 0o600)
        for index, role in enumerate(sorted(roles), start=1)
    }

    monkeypatch.setattr(transport.os, "geteuid", lambda: 0)
    monkeypatch.setattr(transport, "_require_persistent_install_root", lambda: None)
    monkeypatch.setattr(transport, "_hostname", lambda: "gx10-01c7")

    result = transport.bootstrap_server(
        public_key_sources=public_keys,
        execute=False,
        layout=layout,
        expected_root_uid=os.getuid(),
    )

    assert result["mutation_authorized"] is False
    assert set(result["roles"]) == roles
    assert not layout.authorized_keys.parent.exists()


def test_known_hosts_must_match_exact_initiator_route_set(tmp_path: Path) -> None:
    config = transport.load_config()
    payload = _known_hosts(config, "oldlab-2")
    assert transport._known_hosts_endpoints(payload) == transport._required_known_hosts(
        config,
        "oldlab-2",
    )

    with pytest.raises(transport.TransportError, match="known_hosts"):
        transport._known_hosts_endpoints(payload + b"unexpected ssh-rsa AAAA\n")
    first = payload.splitlines(keepends=True)[0]
    with pytest.raises(transport.TransportError, match="duplicate"):
        transport._known_hosts_endpoints(payload + first)


def test_identity_and_public_key_must_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    config = transport.load_config()
    layout = _layout(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    roles = ("oldlab2-controller", "oldlab2-gb10-jump")
    identities = {
        role: _fixture_file(source / f"{role}.identity", b"opaque", 0o600) for role in roles
    }
    public_keys = {
        role: _fixture_file(source / f"{role}.pub", _public_key(1), 0o600) for role in roles
    }
    known_hosts = _fixture_file(
        source / "known_hosts",
        _known_hosts(config, "oldlab-2"),
        0o600,
    )
    monkeypatch.setattr(transport.os, "geteuid", lambda: 0)
    monkeypatch.setattr(transport, "_require_persistent_install_root", lambda: None)
    monkeypatch.setattr(transport, "_hostname", lambda: "trt-eai-oldlab-2")

    with pytest.raises(transport.TransportError, match="do not match"):
        transport.bootstrap_client(
            identity_sources=identities,
            public_key_sources=public_keys,
            known_hosts_source=known_hosts,
            execute=False,
            layout=layout,
            expected_root_uid=os.getuid(),
            public_resolver=lambda _path: _public_key(2),
        )


def test_upgrade_updates_client_and_server_transactionally_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, identities, public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    old_identity_digests = {
        role: transport._asset_digest(layout.identity(role).read_bytes()) for role in identities
    }
    candidate = _fixture_file(
        tmp_path / "candidate-transport.py",
        Path(transport.__file__).read_bytes() + b"\n# upgrade fixture\n",
        0o755,
    )
    monkeypatch.setattr(transport, "__file__", str(candidate))

    plan = transport.upgrade(
        identity_sources={},
        public_key_sources={},
        known_hosts_source=None,
        execute=False,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: public_keys[path.name.removesuffix(".identity")].read_bytes(),
    )
    assert plan["changed"] is True
    assert plan["mutation_authorized"] is False
    assert not layout.upgrade_root.exists()

    result = transport.upgrade(
        identity_sources={},
        public_key_sources={},
        known_hosts_source=None,
        execute=True,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: (
            public_keys[path.name].read_bytes()
            if path.name in public_keys
            else public_keys[path.name.removesuffix(".identity")].read_bytes()
        ),
    )

    assert result["status"] == "succeeded"
    assert result["changed"] is True
    assert transport.validate_client_install(layout)["status"] == "succeeded"
    assert transport.validate_server_install(layout)["status"] == "succeeded"
    assert {
        role: transport._asset_digest(layout.identity(role).read_bytes()) for role in identities
    } == old_identity_digests
    assert all(
        "private-fixture" not in path.read_text(encoding="utf-8", errors="ignore")
        for path in layout.upgrade_root.rglob("*")
        if path.is_file()
    )

    repeat = transport.upgrade(
        identity_sources={},
        public_key_sources={},
        known_hosts_source=None,
        execute=True,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: public_keys[path.name.removesuffix(".identity")].read_bytes(),
    )
    assert repeat["changed"] is False
    assert repeat["status"] == "succeeded"


def test_upgrade_failure_rolls_back_and_runtime_is_closed_while_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _identities, public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    old_program = layout.libexec.read_bytes()
    candidate = _fixture_file(
        tmp_path / "candidate-transport.py",
        Path(transport.__file__).read_bytes() + b"\n# rejected fixture\n",
        0o755,
    )
    monkeypatch.setattr(transport, "__file__", str(candidate))
    validate_client = transport.validate_client_install
    saw_closed_runtime = False

    def fail_new_install(
        current_layout: transport.Layout | None = None,
        *,
        _allow_upgrade: bool = False,
    ) -> dict[str, object]:
        nonlocal saw_closed_runtime
        assert current_layout is not None
        if current_layout.libexec.read_bytes() == candidate.read_bytes():
            with pytest.raises(transport.TransportError, match="admission"):
                validate_client(current_layout)
            saw_closed_runtime = True
            raise transport.TransportError("injected readback failure")
        return validate_client(current_layout, _allow_upgrade=_allow_upgrade)

    monkeypatch.setattr(transport, "validate_client_install", fail_new_install)
    with pytest.raises(transport.TransportError, match="rolled back"):
        transport.upgrade(
            identity_sources={},
            public_key_sources={},
            known_hosts_source=None,
            execute=True,
            layout=layout,
            expected_root_uid=os.getuid(),
            public_resolver=lambda path: public_keys[
                path.name.removesuffix(".identity")
            ].read_bytes(),
        )

    assert saw_closed_runtime is True
    assert layout.libexec.read_bytes() == old_program
    assert not layout.upgrade_active.exists()
    assert b'"phase":"rolled-back"' in layout.upgrade_journal.read_bytes()


def test_upgrade_recovers_prepared_crash_before_new_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _identities, public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    transport._ensure_upgrade_state(layout)
    config_payload = layout.config.read_bytes()
    config = transport.load_config(layout.config)
    client_roles = transport._client_roles(config, "oldlab-2")
    server_roles = transport._server_roles(config, "oldlab-2")
    roles = client_roles | server_roles
    snapshot = transport._prepare_upgrade_snapshot(
        layout,
        paths=transport._upgrade_paths(
            layout,
            roles,
            set(),
            client_installed=True,
            server_installed=True,
        ),
        roles=roles,
        new_identity_roles=set(),
        old_config_sha256=transport._asset_digest(config_payload),
        new_config_sha256=transport._asset_digest(config_payload),
        client_installed=True,
        server_installed=True,
    )
    transport._write_upgrade_active(layout, snapshot, "prepared")
    transport._append_upgrade_journal(layout, snapshot, "prepared")
    transport._replace_installed(
        layout.config,
        b"not a valid route config\n",
        mode=0o600,
        parent_mode=0o700,
    )

    result = transport.upgrade(
        identity_sources={},
        public_key_sources={},
        known_hosts_source=None,
        execute=True,
        layout=layout,
        expected_root_uid=os.getuid(),
        public_resolver=lambda path: public_keys[path.name.removesuffix(".identity")].read_bytes(),
    )

    assert result["recovered"] == "recovered-rolled-back"
    assert layout.config.read_bytes() == config_payload
    assert not layout.upgrade_active.exists()
    assert b'"phase":"recovered-rolled-back"' in layout.upgrade_journal.read_bytes()


@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_server_only_inventory_rejects_extra_or_missing_public_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    _simulate_root_filesystem(monkeypatch)
    monkeypatch.setattr(transport, "_hostname", lambda: "gx10-02c7")
    layout = _layout(tmp_path)
    config = transport.load_config()
    roles = transport._server_roles(config, "trt-gb10-2")
    layout.root.mkdir(mode=0o700)
    layout.public_keys.mkdir(mode=0o755)
    for index, role in enumerate(sorted(roles), start=1):
        _fixture_file(layout.public_key(role), _public_key(index), 0o644)
    if mutation == "extra":
        _fixture_file(layout.public_key("foreign-role"), _public_key(99), 0o644)
    else:
        layout.public_key(sorted(roles)[0]).unlink()

    with pytest.raises(transport.TransportError, match="inventory drifted"):
        transport._installed_roles(
            layout,
            config,
            "trt-gb10-2",
            client_installed=False,
            server_installed=True,
        )
    assert not layout.identities.exists()


def test_upgrade_role_drift_requires_exact_new_assets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _identities, _public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    original_load = transport.load_config
    old = original_load(layout.config)
    roles = dict(old.roles)
    roles["oldlab2-added"] = transport.Role(
        name="oldlab2-added",
        kind="authority",
        initiator="oldlab-2",
        verbs=frozenset({"check"}),
        targets=("oldlab-1",),
    )
    drifted = transport.TransportConfig(
        operator=old.operator,
        authority_program=old.authority_program,
        nodes=old.nodes,
        roles=roles,
        routes=old.routes,
    )
    monkeypatch.setattr(transport, "load_config", lambda _path=layout.config: old)
    monkeypatch.setattr(transport, "_load_config_payload", lambda _payload: drifted)

    with pytest.raises(transport.TransportError, match="identity input set"):
        transport.upgrade(
            identity_sources={},
            public_key_sources={},
            known_hosts_source=None,
            execute=False,
            layout=layout,
            expected_root_uid=os.getuid(),
        )


def test_upgrade_rejects_foreign_install_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _identities, _public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    _fixture_file(layout.root / "ambient-key-copy", b"foreign", 0o600)

    with pytest.raises(transport.TransportError, match="foreign state"):
        transport.upgrade(
            identity_sources={},
            public_key_sources={},
            known_hosts_source=None,
            execute=False,
            layout=layout,
            expected_root_uid=os.getuid(),
        )


def test_upgrade_errors_and_reports_never_expose_private_key_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _identities, _public_keys, _known_hosts_path = _bootstrap_oldlab2(
        tmp_path,
        monkeypatch,
    )
    secret = "highly-sensitive-private-fixture"
    identity = _fixture_file(tmp_path / "identity", secret.encode(), 0o600)

    def failed_keygen(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(argv, 1, b"", secret.encode())

    with pytest.raises(transport.TransportError) as caught:
        transport._derive_public_key(identity, run=failed_keygen)

    assert secret not in str(caught.value)
    assert secret not in str(
        {
            "action": "upgrade",
            "new_public_key_fingerprints": {},
            "status": "succeeded",
        },
    )
    assert secret not in layout.client_policy.read_text(encoding="ascii")


def test_external_asset_rejects_writable_or_symlink_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    writable = tmp_path / "writable"
    writable.mkdir(mode=0o777)
    writable.chmod(0o777)
    asset = _fixture_file(writable / "key", b"opaque", 0o600)
    with pytest.raises(transport.TransportError, match="path is unsafe"):
        transport._safe_external_file(
            asset,
            modes=frozenset({0o600}),
            limit=1024,
            expected_uid=os.getuid(),
            validate_parents=True,
        )

    safe = tmp_path / "safe"
    safe.mkdir(mode=0o700)
    real = safe / "real"
    real.mkdir(mode=0o700)
    linked = safe / "linked"
    linked.symlink_to(real, target_is_directory=True)
    linked_asset = _fixture_file(real / "key", b"opaque", 0o600)
    with pytest.raises(transport.TransportError, match="path is unsafe"):
        transport._safe_external_file(
            linked / linked_asset.name,
            modes=frozenset({0o600}),
            limit=1024,
            expected_uid=os.getuid(),
            validate_parents=True,
        )


def test_persistent_install_root_accepts_docker_chroot_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid1_comm = tmp_path / "comm"
    pid1_comm.write_text("systemd\n", encoding="ascii")
    monkeypatch.setenv("SUDO_USER", "qianyi")
    monkeypatch.setenv("container", "docker")

    transport._require_persistent_install_root(
        root_path=tmp_path,
        pid1_root_path=tmp_path,
        pid1_comm_path=pid1_comm,
        uid=0,
        euid=0,
    )


def test_persistent_install_root_rejects_nonroot_or_nonhost_systemd_view(
    tmp_path: Path,
) -> None:
    pid1_comm = tmp_path / "comm"
    pid1_comm.write_text("systemd\n", encoding="ascii")
    other_root = tmp_path / "other-root"
    other_root.mkdir()

    with pytest.raises(transport.TransportError, match="host-root authority"):
        transport._require_persistent_install_root(
            root_path=tmp_path,
            pid1_root_path=tmp_path,
            pid1_comm_path=pid1_comm,
            uid=1000,
            euid=1000,
        )
    with pytest.raises(transport.TransportError, match="systemd view is invalid"):
        transport._require_persistent_install_root(
            root_path=tmp_path,
            pid1_root_path=other_root,
            pid1_comm_path=pid1_comm,
            uid=0,
            euid=0,
        )


@pytest.mark.parametrize("mutation", ["rename", "rewrite"])
def test_external_asset_rejects_post_read_path_or_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    _allow_ambient_tmp_ancestors(tmp_path, monkeypatch)
    source = tmp_path / "safe-source"
    source.mkdir(mode=0o700)
    asset = _fixture_file(source / "asset", b"original", 0o600)
    replacement = _fixture_file(source / "replacement", b"changed!", 0o600)
    original_read = transport._read_external_fd_twice

    def mutate_after_read(descriptor: int, *, limit: int) -> bytes:
        payload = original_read(descriptor, limit=limit)
        if mutation == "rename":
            replacement.replace(asset)
        else:
            asset.write_bytes(b"rewritten-content")
            asset.chmod(0o600)
        return payload

    monkeypatch.setattr(transport, "_read_external_fd_twice", mutate_after_read)
    with pytest.raises(
        transport.TransportError,
        match=r"metadata is unsafe|changed during verification",
    ):
        transport._safe_external_file(
            asset,
            modes=frozenset({0o600}),
            limit=1024,
            expected_uid=os.getuid(),
            validate_parents=True,
        )


def test_parser_exposes_no_runtime_user_key_or_route_override() -> None:
    parser = transport._parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "invoke",
                "--node",
                "trt-gb10-2",
                "--verb",
                "check",
                "--user",
                "root",
            ],
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "invoke",
                "--node",
                "trt-gb10-2",
                "--verb",
                "check",
                "--identity",
                "/tmp/key",
            ],
        )


def test_only_closed_infrastructure_converge_envelope_gets_long_timeout() -> None:
    inner = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "convergence_id": "c" * 64,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    inner_raw = json.dumps(inner, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    unsigned = {
        "schema_version": 1,
        "action": "staging-infrastructure-converge",
        "node": "oldlab-2",
        "domain": "oldlab",
        "sandbox": "staging",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "payload_kind": "staging-infrastructure-converge-request",
        "payload_sha256": hashlib.sha256(inner_raw).hexdigest(),
        "payload_base64": base64.b64encode(inner_raw).decode("ascii"),
        "prior_request_id": None,
    }
    unsigned_raw = (
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    )
    envelope = {
        **unsigned,
        "request_id": hashlib.sha256(unsigned_raw).hexdigest(),
    }
    raw = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"

    assert (
        transport._invoke_timeout("oldlab-2", "transact", raw)
        == transport.INFRASTRUCTURE_CONVERGE_TIMEOUT_SECONDS
    )
    assert (
        transport._invoke_timeout("oldlab-2", "check", raw)
        == transport.DEFAULT_INVOKE_TIMEOUT_SECONDS
    )
    tampered = raw.replace(b'"sandbox":"staging"', b'"sandbox":"qianyi"')
    assert (
        transport._invoke_timeout("oldlab-2", "transact", tampered)
        == transport.DEFAULT_INVOKE_TIMEOUT_SECONDS
    )


def test_program_source_contains_no_key_generation_or_accept_new() -> None:
    source = Path(transport.__file__).read_text(encoding="utf-8")

    assert 'ssh-keygen", "-t' not in source
    assert "StrictHostKeyChecking=accept-new" not in source
    assert "SSH_AUTH_SOCK" not in source
    assert "PasswordAuthentication=yes" not in source
    assert "trt-gb10-7" in transport.CHECKED_IN_CONFIG.read_text(encoding="utf-8")
    assert stat.S_IMODE(Path(transport.__file__).stat().st_mode) == 0o755
    assert Path(transport.__file__).read_bytes().startswith(b"#!/usr/bin/python3 -I\n")
