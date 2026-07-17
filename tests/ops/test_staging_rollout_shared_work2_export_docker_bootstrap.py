from __future__ import annotations

import os
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_shared_work2_export_docker_bootstrap as bootstrap


def test_identity_requires_exact_approved_base(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_SEALED_SOURCE_SHA", "a" * 40)
    monkeypatch.setenv("LOOM_SEALED_SOURCE_TREE", "b" * 40)
    monkeypatch.setenv("LOOM_SEALED_SOURCE_BASE", "c" * 40)
    monkeypatch.setenv("LOOM_SEALED_BUNDLE_SHA256", "d" * 64)

    with pytest.raises(bootstrap.DockerBootstrapError, match="base is not approved"):
        bootstrap.Identity.from_environ()


def test_runtime_rejects_a_non_gb10_architecture(monkeypatch: pytest.MonkeyPatch) -> None:
    machine = os.uname()
    monkeypatch.setattr(
        bootstrap.os,
        "uname",
        lambda: machine.__class__(
            (
                machine.sysname,
                machine.nodename,
                machine.release,
                machine.version,
                "x86_64",
            )
        ),
    )
    monkeypatch.setattr(bootstrap.sys, "argv", [bootstrap.sys.argv[0]])
    monkeypatch.setattr(bootstrap.os, "geteuid", lambda: 0)
    monkeypatch.setattr(bootstrap.os, "getegid", lambda: 0)

    with pytest.raises(bootstrap.DockerBootstrapError, match="architecture is invalid"):
        bootstrap._validate_runtime()


def test_mountinfo_parser_decodes_and_preserves_bind_identity() -> None:
    records = bootstrap._parse_mountinfo(
        "42 41 8:1 /usr/local /usr/local rw,relatime - ext4 /dev/root rw\n"
        "43 41 8:1 /handoff/file\\040name /run/loom-handoff/loom.bundle ro - ext4 /dev/root rw\n"
    )

    assert records == (
        bootstrap.MountRecord("/usr/local", "/usr/local", frozenset({"rw", "relatime"})),
        bootstrap.MountRecord("/handoff/file name", str(bootstrap.BUNDLE), frozenset({"ro"})),
    )


def test_mountinfo_parser_rejects_missing_separator() -> None:
    with pytest.raises(bootstrap.DockerBootstrapError, match="mountinfo is invalid"):
        bootstrap._parse_mountinfo("42 41 8:1 / / rw")


def test_network_boundary_allows_only_down_kernel_devices_and_loopback_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    network_class = tmp_path / "net"
    for name, state in (("lo", "unknown"), ("gre0", "down")):
        interface = network_class / name
        interface.mkdir(parents=True)
        (interface / "operstate").write_text(f"{state}\n", encoding="ascii")
    (network_class / "bonding_masters").write_text("", encoding="ascii")
    ipv4_routes = tmp_path / "route"
    ipv4_routes.write_text(
        "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n",
        encoding="ascii",
    )
    ipv6_routes = tmp_path / "ipv6_route"
    ipv6_routes.write_text(
        "00000000000000000000000000000001 80 "
        "00000000000000000000000000000000 00 "
        "00000000000000000000000000000000 00000000 00000002 00000000 "
        "80200001 lo\n",
        encoding="ascii",
    )
    monkeypatch.setattr(bootstrap, "NETWORK_CLASS", network_class)
    monkeypatch.setattr(bootstrap, "IPV4_ROUTES", ipv4_routes)
    monkeypatch.setattr(bootstrap, "IPV6_ROUTES", ipv6_routes)

    bootstrap._validate_network_boundary()


@pytest.mark.parametrize(
    ("interface_state", "ipv4_row", "ipv6_row"),
    [
        ("up", "", ""),
        ("down", "eth0\t00000000\t00000000\t0001\n", ""),
        (
            "down",
            "",
            "00000000000000000000000000000000 00 "
            "00000000000000000000000000000000 00 "
            "00000000000000000000000000000000 00000000 00000001 00000000 "
            "00200200 eth0\n",
        ),
    ],
)
def test_network_boundary_rejects_usable_non_loopback_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interface_state: str,
    ipv4_row: str,
    ipv6_row: str,
) -> None:
    network_class = tmp_path / "net"
    for name, state in (("lo", "unknown"), ("eth0", interface_state)):
        interface = network_class / name
        interface.mkdir(parents=True)
        (interface / "operstate").write_text(f"{state}\n", encoding="ascii")
    ipv4_routes = tmp_path / "route"
    ipv4_routes.write_text(
        "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n" + ipv4_row,
        encoding="ascii",
    )
    ipv6_routes = tmp_path / "ipv6_route"
    ipv6_routes.write_text(ipv6_row, encoding="ascii")
    monkeypatch.setattr(bootstrap, "NETWORK_CLASS", network_class)
    monkeypatch.setattr(bootstrap, "IPV4_ROUTES", ipv4_routes)
    monkeypatch.setattr(bootstrap, "IPV6_ROUTES", ipv6_routes)

    with pytest.raises(bootstrap.DockerBootstrapError, match="network must be disabled"):
        bootstrap._validate_network_boundary()


def test_bundle_digest_rejects_group_writable_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "bundle"
    bundle.write_bytes(b"bundle")
    bundle.chmod(0o660)
    monkeypatch.setattr(bootstrap, "BUNDLE", bundle)

    with pytest.raises(bootstrap.DockerBootstrapError, match="bundle is unsafe"):
        bootstrap._bundle_digest()


def test_provision_source_reuses_only_an_exact_existing_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "authority"
    source = parent / "source"
    source.mkdir(parents=True)
    identity = bootstrap.Identity("a" * 40, "b" * 40, bootstrap.APPROVED_BASE_SHA, "c" * 64)
    validated: list[Path] = []
    monkeypatch.setattr(bootstrap, "SOURCE_PARENT", parent)
    monkeypatch.setattr(bootstrap, "SOURCE", source)
    monkeypatch.setattr(bootstrap, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bootstrap,
        "_validate_source",
        lambda path, _identity, *, name: validated.append(path),
    )

    assert bootstrap._provision_source(identity) is False
    assert validated == [source]


def test_provision_source_rolls_back_new_parent_on_clone_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "authority"
    source = parent / "source"
    identity = bootstrap.Identity("a" * 40, "b" * 40, bootstrap.APPROVED_BASE_SHA, "c" * 64)
    monkeypatch.setattr(bootstrap, "SOURCE_PARENT", parent)
    monkeypatch.setattr(bootstrap, "SOURCE", source)
    monkeypatch.setattr(bootstrap, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bootstrap,
        "_checked",
        lambda *_args: (_ for _ in ()).throw(bootstrap.DockerBootstrapError("clone failed")),
    )

    with pytest.raises(bootstrap.DockerBootstrapError, match="clone failed"):
        bootstrap._provision_source(identity)

    assert not parent.exists()


def test_execute_rolls_back_new_source_when_bootstrap_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "authority"
    source = parent / "source"
    source.mkdir(parents=True)
    identity = bootstrap.Identity("a" * 40, "b" * 40, bootstrap.APPROVED_BASE_SHA, "c" * 64)
    monkeypatch.setattr(bootstrap, "SOURCE_PARENT", parent)
    monkeypatch.setattr(bootstrap, "SOURCE", source)
    monkeypatch.setattr(bootstrap, "_validate_runtime", lambda: None)
    monkeypatch.setattr(bootstrap, "_validate_host_roots", lambda: None)
    monkeypatch.setattr(bootstrap, "_bundle_digest", lambda: identity.bundle_sha256)
    monkeypatch.setattr(bootstrap, "_provision_source", lambda _identity: True)
    monkeypatch.setattr(
        bootstrap,
        "_bootstrap",
        lambda _identity: (_ for _ in ()).throw(
            bootstrap.DockerBootstrapError("injected bootstrap failure")
        ),
    )

    with pytest.raises(bootstrap.DockerBootstrapError, match="injected bootstrap"):
        bootstrap.execute(identity)

    assert not parent.exists()


def test_provision_source_rolls_back_after_published_validation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "authority"
    source = parent / "source"
    identity = bootstrap.Identity("a" * 40, "b" * 40, bootstrap.APPROVED_BASE_SHA, "c" * 64)
    monkeypatch.setattr(bootstrap, "SOURCE_PARENT", parent)
    monkeypatch.setattr(bootstrap, "SOURCE", source)
    monkeypatch.setattr(bootstrap, "_safe_root_directory", lambda *_args, **_kwargs: None)

    def checked(*argv: str) -> str:
        if "clone" in argv:
            Path(argv[-1]).mkdir()
        return ""

    validations = 0

    def validate(_path: Path, _identity: bootstrap.Identity, *, name: str) -> None:
        nonlocal validations
        validations += 1
        if name.endswith("published"):
            raise bootstrap.DockerBootstrapError("injected published validation failure")

    monkeypatch.setattr(bootstrap, "_checked", checked)
    monkeypatch.setattr(bootstrap, "_validate_source", validate)
    monkeypatch.setattr(bootstrap, "_rename_noreplace", os.rename)

    with pytest.raises(bootstrap.DockerBootstrapError, match="published validation"):
        bootstrap._provision_source(identity)

    assert validations == 2
    assert not parent.exists()


def test_rename_noreplace_refuses_existing_destination(tmp_path: Path) -> None:
    if not hasattr(bootstrap.ctypes.CDLL(None), "renameat2"):
        pytest.skip("renameat2 is a Linux-only runtime contract")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()

    with pytest.raises(bootstrap.DockerBootstrapError, match="publication failed"):
        bootstrap._rename_noreplace(source, destination)

    assert source.is_dir()
    assert destination.is_dir()


def test_remove_checkout_refuses_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    os.symlink(target, link)

    with pytest.raises(bootstrap.DockerBootstrapError, match="rollback failed"):
        bootstrap._remove_checkout(link)
