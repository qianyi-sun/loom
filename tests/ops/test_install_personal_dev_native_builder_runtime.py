from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import stat
import tarfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

import pytest
import scripts.ops.install_personal_dev_native_builder_runtime as runtime_installer
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID
from scripts.ops.install_personal_dev_native_builder_runtime import (
    NativeBuilderCommandResult,
    NativeBuilderInstallContext,
    PersonalDevNativeBuilderRuntimeInstaller,
    PersonalDevNativeBuilderRuntimeInstallError,
    main,
)
from scripts.ops.personal_dev_native_builder_runtime_profile import (
    NativeBuilderRuntimeArchiveMember,
    NativeBuilderRuntimeProfile,
    load_native_builder_runtime_profile,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_PATH = _ROOT / "deploy/personal-dev-native-builder/runtime-profile-v1.json"


def test_ed25519_public_key_derivation_matches_rfc_8032_vector_1() -> None:
    seed = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")

    assert runtime_installer._derive_ed25519_public_key(seed).hex() == (
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    )


@dataclass(frozen=True, slots=True)
class ArchiveEntry:
    name: str
    payload: bytes = b""
    mode: int = 0o755
    kind: bytes = tarfile.REGTYPE
    linkname: str = ""


@dataclass(slots=True)
class HostRunner:
    host_name: str = "gx10-01c7"
    architecture: str = "aarch64"
    kvm_ready: bool = True
    cgroup_ready: bool = True
    docker_version: str = "28.3.3"
    docker_api_version: str = "1.51"
    storage_driver: str = "overlay2"
    cgroup_driver: str = "systemd"
    cpus: int = 20
    memory_bytes: int = 125_513_936 * 1024
    disk_bytes: int = 183_255_265_280
    routes: tuple[str, ...] = (
        "10.42.0.0/24",
        "172.17.0.0/16",
        "172.18.0.0/16",
        "172.19.0.0/16",
        "172.20.0.0/16",
        "192.168.20.0/24",
    )
    dockerd_active: bool = False
    agent_active: bool = False
    identity_conflict: bool = False
    dedicated_containers: tuple[str, ...] = ()
    dedicated_networks: tuple[str, ...] = ()
    runsc_version: str = "release-20260810.0"
    runsc_spec_version: str = "1.2.1"
    nft_table_output: str = ""
    sysusers_dry_run_stdout: str = ""
    calls: list[tuple[str, ...]] | None = None
    environments: list[dict[str, str]] | None = None

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> NativeBuilderCommandResult:
        assert env is not None
        call = tuple(argv)
        if self.calls is not None:
            self.calls.append(call)
        if self.environments is not None:
            self.environments.append(dict(env))
        executable = Path(call[0]).name
        result = NativeBuilderCommandResult(127, stderr="unexpected command")
        if executable == "hostname" and call[1:] == ("--fqdn",):
            result = NativeBuilderCommandResult(0, self.host_name + "\n")
        elif executable == "uname" and call[1:] == ("-m",):
            result = NativeBuilderCommandResult(0, self.architecture + "\n")
        elif executable == "test" and call[1:2] == ("-c",):
            result = NativeBuilderCommandResult(0 if self.kvm_ready else 1)
        elif executable == "test" and call[1:2] == ("-f",):
            result = NativeBuilderCommandResult(0 if self.cgroup_ready else 1)
        elif executable == "docker" and "version" in call:
            result = NativeBuilderCommandResult(
                0,
                json.dumps(
                    {
                        "api": self.docker_api_version,
                        "arch": "arm64",
                        "os": "linux",
                        "version": self.docker_version,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        elif executable == "docker" and "info" in call and "-H" not in call:
            result = NativeBuilderCommandResult(
                0,
                json.dumps(
                    {
                        "cgroup_driver": self.cgroup_driver,
                        "storage_driver": self.storage_driver,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        elif executable == "nproc":
            result = NativeBuilderCommandResult(0, f"{self.cpus}\n")
        elif executable == "awk":
            result = NativeBuilderCommandResult(0, f"{self.memory_bytes}\n")
        elif executable == "df":
            result = NativeBuilderCommandResult(0, f"Avail\n{self.disk_bytes}\n")
        elif executable == "ip":
            result = NativeBuilderCommandResult(
                0,
                json.dumps([{"dst": value} for value in self.routes]) + "\n",
            )
        elif executable == "getent":
            result = NativeBuilderCommandResult(
                0 if self.identity_conflict else 2,
                "collision\n" if self.identity_conflict else "",
            )
        elif executable == "systemctl":
            if call[1:2] == ("is-active",):
                active = self.dockerd_active if "builder-dockerd" in call[2] else self.agent_active
                result = NativeBuilderCommandResult(
                    0 if active else 3,
                    "active\n" if active else "inactive\n",
                )
            elif call[1:] == ("daemon-reload",):
                result = NativeBuilderCommandResult(0)
            elif call[1:2] == ("start",) and "builder-dockerd" in call[2]:
                self.dockerd_active = True
                result = NativeBuilderCommandResult(0)
            elif call[1:2] == ("stop",) and "builder-dockerd" in call[2]:
                self.dockerd_active = False
                result = NativeBuilderCommandResult(0)
        elif executable == "systemd-sysusers":
            result = NativeBuilderCommandResult(
                0,
                self.sysusers_dry_run_stdout if "--dry-run" in call else "",
            )
        elif executable == "nft":
            result = NativeBuilderCommandResult(
                0,
                self.nft_table_output if "list" in call else "",
            )
        elif executable in {"dockerd", "systemd-analyze"}:
            result = NativeBuilderCommandResult(0)
        elif executable == "runsc" and call[1:] == ("--version",):
            result = NativeBuilderCommandResult(
                0,
                f"runsc version {self.runsc_version}\nspec: {self.runsc_spec_version}\n",
            )
        elif executable == "docker" and "-H" in call and "ps" in call:
            result = NativeBuilderCommandResult(0, "\n".join(self.dedicated_containers))
        elif executable == "docker" and "-H" in call and "network" in call:
            result = NativeBuilderCommandResult(0, "\n".join(self.dedicated_networks))
        elif executable == "docker" and "-H" in call and "info" in call:
            result = NativeBuilderCommandResult(
                0,
                json.dumps(
                    {
                        "cgroup_driver": self.cgroup_driver,
                        "default_runtime": "runsc-personal-dev-native",
                        "driver": self.storage_driver,
                        "server_version": self.docker_version,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        if check and result.returncode != 0:
            raise PersonalDevNativeBuilderRuntimeInstallError("command_failed")
        return result


def _profile() -> NativeBuilderRuntimeProfile:
    return load_native_builder_runtime_profile(_PROFILE_PATH)


def _small_profile() -> tuple[NativeBuilderRuntimeProfile, dict[str, bytes]]:
    profile = _profile()
    payloads = {
        name: f"measured native payload for {name}\n".encode("ascii") for name in profile.members
    }
    members = {
        name: NativeBuilderRuntimeArchiveMember(
            size=len(payload),
            archive_mode=0o755,
            install_mode=0o555,
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        for name, payload in payloads.items()
    }
    return replace(profile, members=MappingProxyType(members)), payloads


def _write_archive(path: Path, entries: Sequence[ArchiveEntry]) -> str:
    with tarfile.open(path, mode="w:bz2", format=tarfile.GNU_FORMAT) as bundle:
        for entry in entries:
            member = tarfile.TarInfo(entry.name)
            member.mode = entry.mode
            member.type = entry.kind
            member.linkname = entry.linkname
            if entry.kind == tarfile.REGTYPE:
                member.size = len(entry.payload)
                bundle.addfile(member, io.BytesIO(entry.payload))
            else:
                member.size = 0
                bundle.addfile(member)
    path.chmod(0o600)
    return hashlib.sha512(path.read_bytes()).hexdigest()


def _archive(
    tmp_path: Path,
    *,
    mutate: Callable[[list[ArchiveEntry]], None] | None = None,
) -> tuple[NativeBuilderRuntimeProfile, Path]:
    profile, payloads = _small_profile()
    entries = [
        ArchiveEntry(name, payloads[name], profile.members[name].archive_mode)
        for name in profile.members
    ]
    entries.insert(2, ArchiveEntry("gvisor-bin", kind=tarfile.DIRTYPE))
    if mutate is not None:
        mutate(entries)
    archive = tmp_path / "gvisor.tar.bz2"
    digest = _write_archive(archive, entries)
    return replace(profile, archive_sha512=digest), archive


def _host_root(tmp_path: Path) -> Path:
    root = tmp_path / "host"
    (root / "var/lib").mkdir(parents=True)
    (root / "run").mkdir()
    (root / "sys/fs/cgroup").mkdir(parents=True)
    (root / "sys/fs/cgroup/cgroup.controllers").write_text("cpu memory pids\n")
    root.chmod(0o755)
    for directory in root.rglob("*"):
        if directory.is_dir():
            directory.chmod(0o755)
    return root


def _installer(
    tmp_path: Path,
    profile: NativeBuilderRuntimeProfile,
    *,
    runner: HostRunner | None = None,
    root: Path | None = None,
) -> PersonalDevNativeBuilderRuntimeInstaller:
    context = NativeBuilderInstallContext(
        root=_host_root(tmp_path) if root is None else root,
        authority_uid=os.getuid(),
        authority_gid=os.getgid(),
    )
    return PersonalDevNativeBuilderRuntimeInstaller(
        profile=profile,
        context=context,
        runner=runner or HostRunner(),
        effective_uid=os.geteuid(),
    )


def _mapped(
    installer: PersonalDevNativeBuilderRuntimeInstaller,
    path: Path,
) -> Path:
    return installer.context.path(path)


def test_preflight_verifies_archive_and_publishes_nothing(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)

    receipt = installer.preflight(archive)

    assert receipt == {
        "archive_sha512": profile.archive_sha512,
        "operation": "preflight",
        "profile_sha256": profile.sha256,
        "release": profile.version,
    }
    assert not _mapped(installer, profile.release_root).exists()
    assert not _mapped(installer, profile.profile_path).exists()


def _traversal(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], name="../runsc")


def _symlink(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], kind=tarfile.SYMTYPE, linkname="runsc")


def _hardlink(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], kind=tarfile.LNKTYPE, linkname="runsc")


def _missing(entries: list[ArchiveEntry]) -> None:
    entries.pop()


def _extra(entries: list[ArchiveEntry]) -> None:
    entries.append(ArchiveEntry("unexpected", b"foreign\n"))


def _changed(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], payload=entries[0].payload + b"x")


@pytest.mark.parametrize(
    "mutation",
    (_traversal, _symlink, _hardlink, _missing, _extra, _changed),
)
def test_preflight_rejects_malformed_archive_without_publication(
    tmp_path: Path,
    mutation: Callable[[list[ArchiveEntry]], None],
) -> None:
    profile, archive = _archive(tmp_path, mutate=mutation)
    installer = _installer(tmp_path, profile)

    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
        installer.preflight(archive)

    assert not _mapped(installer, profile.release_root).exists()


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ("hostname", "host_identity_invalid"),
        ("architecture", "host_identity_invalid"),
        ("kvm", "kvm_device_invalid"),
        ("cgroup", "cgroup_v2_invalid"),
        ("docker", "docker_identity_invalid"),
        ("storage", "docker_identity_invalid"),
        ("cpu", "host_capacity_invalid"),
        ("memory", "host_capacity_invalid"),
        ("disk", "host_capacity_invalid"),
        ("route", "address_pool_conflict"),
        ("identity", "identity_conflict"),
        ("dockerd-active", "service_state_invalid"),
        ("agent-active", "service_state_invalid"),
    ],
)
def test_preflight_rejects_each_host_or_authority_drift(
    tmp_path: Path,
    change: str,
    error: str,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner()
    if change == "hostname":
        runner.host_name = "other"
    elif change == "architecture":
        runner.architecture = "x86_64"
    elif change == "kvm":
        runner.kvm_ready = False
    elif change == "cgroup":
        runner.cgroup_ready = False
    elif change == "docker":
        runner.docker_version = "28.3.4"
    elif change == "storage":
        runner.storage_driver = "vfs"
    elif change == "cpu":
        runner.cpus = profile.minimum_cpus - 1
    elif change == "memory":
        runner.memory_bytes = profile.minimum_memory_bytes - 1
    elif change == "disk":
        runner.disk_bytes = profile.minimum_disk_free_bytes - 1
    elif change == "route":
        runner.routes = (*runner.routes, profile.address_pool)
    elif change == "identity":
        runner.identity_conflict = True
    elif change == "dockerd-active":
        runner.dockerd_active = True
    else:
        runner.agent_active = True
    installer = _installer(tmp_path, profile, runner=runner)

    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError, match=error):
        installer.preflight(archive)


def test_install_is_exact_idempotent_and_inactive(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(calls=[])
    installer = _installer(tmp_path, profile, runner=runner)

    first = installer.install(archive)
    second = installer.install(archive)

    assert (
        first
        == second
        == {
            "operation": "install",
            "profile_sha256": profile.sha256,
            "release": profile.version,
            "state": "staged",
        }
    )
    expected_files = {
        profile.profile_path: (profile.payload, 0o444),
        profile.runsc_config_path: (profile.runsc_toml, 0o444),
        profile.dockerd_config_path: (profile.dockerd_json, 0o444),
        profile.nftables_path: (profile.nftables, 0o444),
        profile.dockerd_service_path: (profile.dockerd_service, 0o444),
        profile.slice_unit_path: (profile.slice_unit, 0o444),
        profile.sysusers_path: (profile.sysusers, 0o444),
        profile.agent_service_template_path: (
            profile.agent_service_template,
            0o444,
        ),
    }
    for absolute, (payload, mode) in expected_files.items():
        path = _mapped(installer, absolute)
        assert path.read_bytes() == payload
        assert stat.S_IMODE(path.stat().st_mode) == mode
    assert not _mapped(installer, profile.agent_service_path).exists()
    assert not any("start" in call or "enable" in call for call in runner.calls or [])
    assert installer.verify_staged()["state"] == "staged"


def test_install_creates_exact_runtime_directories_and_applies_sysusers(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(calls=[])
    installer = _installer(tmp_path, profile, runner=runner)

    installer.install(archive)

    for absolute, mode in (
        (profile.data_root, 0o750),
        (profile.exec_root, 0o750),
        (profile.agent_state_path, 0o700),
    ):
        path = _mapped(installer, absolute)
        assert path.is_dir()
        assert stat.S_IMODE(path.stat().st_mode) == mode
        assert path.stat().st_uid == os.getuid()
        assert path.stat().st_gid == os.getgid()
    assert any(
        Path(call[0]).name == "systemd-sysusers" and "--dry-run" not in call
        for call in runner.calls or []
    )

    _mapped(installer, profile.exec_root).chmod(0o755)
    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="staged_state_invalid",
    ):
        installer.verify_staged()


def test_verify_staged_rejects_live_provider_nft_table(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner()
    installer = _installer(tmp_path, profile, runner=runner)
    installer.install(archive)
    runner.nft_table_output = f"table inet {profile.nft_table}\n"

    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="nftables_state_invalid",
    ):
        installer.verify_staged()


def test_preflight_accepts_safe_sysusers_dry_run_plan_output(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(
        sysusers_dry_run_stdout="Creating group 'loom-pdev-native' with GID 24850.\n"
    )
    installer = _installer(tmp_path, profile, runner=runner)

    assert installer.preflight(archive)["operation"] == "preflight"


def test_preflight_isolates_systemd_unit_validation_from_host_units(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(calls=[], environments=[])
    installer = _installer(tmp_path, profile, runner=runner)

    installer.preflight(archive)

    calls = runner.calls or []
    environments = runner.environments or []
    indexes = [index for index, call in enumerate(calls) if Path(call[0]).name == "systemd-analyze"]
    assert len(indexes) == 1
    unit_path = environments[indexes[0]].get("SYSTEMD_UNIT_PATH")
    assert unit_path
    assert not unit_path.endswith(":")


def test_install_fsyncs_files_and_publication_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    kinds: list[str] = []
    real_fsync = os.fsync

    def record(descriptor: int) -> None:
        kinds.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
        real_fsync(descriptor)

    monkeypatch.setattr(runtime_installer.os, "fsync", record)
    installer.install(archive)

    assert kinds.count("file") >= len(profile.members) + 8
    assert kinds.count("directory") >= 8


def _test_certificate(*, ca: bool, seed: int = 1) -> bytes:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([seed]) * 32)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"native test {seed}")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(seed)
        .not_valid_before(datetime(2026, 1, 1, tzinfo=UTC))
        .not_valid_after(datetime(2036, 1, 1, tzinfo=UTC))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .sign(private_key, algorithm=None)
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _mutated_test_certificate(original: bytes, replacement: bytes) -> bytes:
    payload = _test_certificate(ca=True)
    encoded = b"".join(payload.splitlines()[1:-1])
    der = base64.b64decode(encoded, validate=True)
    assert original in der
    invalid = der.replace(original, replacement, 1)
    assert invalid != der
    invalid_encoded = base64.b64encode(invalid)
    body = b"\n".join(
        invalid_encoded[index : index + 64]
        for index in range(0, len(invalid_encoded), 64)
    )
    return b"-----BEGIN CERTIFICATE-----\n" + body + b"\n-----END CERTIFICATE-----\n"


def _test_certificate_with_invalid_month() -> bytes:
    return _mutated_test_certificate(b"260101000000Z", b"261301000000Z")


def _test_certificate_with_invalid_name_tag() -> bytes:
    return _mutated_test_certificate(
        b"\x31\x16\x30\x14\x06\x03\x55\x04\x03",
        b"\xb1\x16\x30\x14\x06\x03\x55\x04\x03",
    )


def _test_certificate_with_invalid_name_value_tag() -> bytes:
    return _mutated_test_certificate(
        b"\x0c\x0dnative test 1",
        b"\x04\x0dnative test 1",
    )


def _stage_inputs(tmp_path: Path) -> tuple[Path, Path]:
    key = tmp_path / "agent-ed25519"
    key.write_bytes(b"k" * 32)
    key.chmod(0o400)
    ca = tmp_path / "service-ca.pem"
    ca.write_bytes(_test_certificate(ca=True, seed=1) + _test_certificate(ca=True, seed=2))
    ca.chmod(0o444)
    return key, ca


def test_stage_agent_renders_exact_inactive_secret_free_unit(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    arguments = {
        "agent_image": "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
        + "a" * 64,
        "builder_image": "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64,
        "service_url": "https://loom.example.invalid",
        "agent_instance_id": "00000000-0000-0000-0000-000000000001",
        "key_id": "native-builder-v1",
        "private_key": key,
        "ca_file": ca,
    }

    first = installer.stage_agent(**arguments)
    second = installer.stage_agent(**arguments)

    assert first == second
    unit = _mapped(installer, profile.agent_service_path).read_text(encoding="ascii")
    assert "@@" not in unit
    assert arguments["agent_image"] in unit
    assert arguments["builder_image"] in unit
    assert arguments["service_url"] in unit
    assert "k" * 32 not in unit
    assert ca.read_bytes() not in unit.encode("ascii")
    installed_key = _mapped(installer, profile.private_key_path)
    installed_ca = _mapped(installer, profile.ca_file_path)
    assert installed_key.read_bytes() == b"k" * 32
    assert stat.S_IMODE(installed_key.stat().st_mode) == profile.private_key_mode
    assert installed_ca.read_bytes() == ca.read_bytes()
    assert stat.S_IMODE(installed_ca.stat().st_mode) == 0o444
    manifest_path = _mapped(
        installer,
        Path("/etc/loom/personal-dev-native-builder/agent-stage-v1.json"),
    )
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    public_key = (
        Ed25519PrivateKey.from_private_bytes(b"k" * 32)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    assert manifest["public_key_sha256"] == hashlib.sha256(public_key).hexdigest()
    assert set(manifest) == {
        "agent_image",
        "agent_instance_id",
        "builder_image",
        "key_id",
        "profile_sha256",
        "public_key_sha256",
        "schema",
        "service_url",
        "unit_sha256",
    }


_INVALID_CA_BUNDLES = {
    "malformed-nonempty": b"not a PEM certificate\n",
    "truncated-nonempty": _test_certificate(ca=True)[:-24],
    "contains-non-ca": _test_certificate(ca=True) + _test_certificate(ca=False, seed=3),
    "invalid-calendar-time": _test_certificate_with_invalid_month(),
    "invalid-name-tag": _test_certificate_with_invalid_name_tag(),
    "invalid-name-value-tag": _test_certificate_with_invalid_name_value_tag(),
}


@pytest.mark.parametrize("invalid_kind", tuple(_INVALID_CA_BUNDLES))
def test_stage_agent_rejects_invalid_ca_bundle_before_any_publication(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    ca.chmod(0o644)
    ca.write_bytes(_INVALID_CA_BUNDLES[invalid_kind])
    ca.chmod(0o444)

    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="agent_material_invalid",
    ):
        installer.stage_agent(
            agent_image="example.invalid/agent@sha256:" + "a" * 64,
            builder_image="example.invalid/builder@sha256:" + "b" * 64,
            service_url="https://loom.example.invalid",
            agent_instance_id="00000000-0000-0000-0000-000000000001",
            key_id="native-builder-v1",
            private_key=key,
            ca_file=ca,
        )

    for path in (
        profile.private_key_path,
        profile.ca_file_path,
        profile.agent_service_path,
        Path("/etc/loom/personal-dev-native-builder/agent-stage-v1.json"),
    ):
        assert not _mapped(installer, path).exists()


def test_broker_stage_requires_expected_raw_ed25519_public_fingerprint(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    arguments = {
        "agent_image": "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
        + "a" * 64,
        "builder_image": "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64,
        "service_url": "https://loom.example.invalid",
        "agent_instance_id": "00000000-0000-0000-0000-000000000001",
        "key_id": "native-builder-v1",
        "private_key": key,
        "ca_file": ca,
    }

    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="public_key_invalid",
    ):
        installer.stage_agent_authorized(
            **arguments,
            expected_public_key_sha256="f" * 64,
        )

    public_key = (
        Ed25519PrivateKey.from_private_bytes(b"k" * 32)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    expected = hashlib.sha256(public_key).hexdigest()
    assert (
        installer.stage_agent_authorized(
            **arguments,
            expected_public_key_sha256=expected,
        )["state"]
        == "staged"
    )


def test_broker_discard_removes_only_inactive_agent_stage_files(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    installer.stage_agent(
        agent_image="ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64,
        builder_image="ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64,
        service_url="https://loom.example.invalid",
        agent_instance_id="00000000-0000-0000-0000-000000000001",
        key_id="native-builder-v1",
        private_key=key,
        ca_file=ca,
    )

    installer.discard_agent_stage()
    installer.discard_agent_stage()

    for path in (
        profile.agent_service_path,
        profile.private_key_path,
        profile.ca_file_path,
        Path("/etc/loom/personal-dev-native-builder/agent-stage-v1.json"),
    ):
        assert not _mapped(installer, path).exists()
    assert _mapped(installer, profile.release_root).is_dir()


@pytest.mark.parametrize("failure_phase", ("validate", "unlink"))
@pytest.mark.parametrize("failure_index", range(4))
def test_broker_discard_attempts_manifest_unit_ca_key_after_each_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
    failure_index: int,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    installer.stage_agent(
        agent_image="ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64,
        builder_image="ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64,
        service_url="https://loom.example.invalid",
        agent_instance_id="00000000-0000-0000-0000-000000000001",
        key_id="native-builder-v1",
        private_key=key,
        ca_file=ca,
    )
    ordered = (
        Path("/etc/loom/personal-dev-native-builder/agent-stage-v1.json"),
        profile.agent_service_path,
        profile.ca_file_path,
        profile.private_key_path,
    )
    observed: list[Path] = []
    original_read = runtime_installer._read_regular
    original_unlink = installer._unlink

    def injected_read(path: Path, **arguments: object) -> bytes:
        absolute = next(item for item in ordered if _mapped(installer, item) == path)
        observed.append(absolute)
        if failure_phase == "validate" and absolute == ordered[failure_index]:
            raise PersonalDevNativeBuilderRuntimeInstallError("injected_failure")
        return original_read(path, **arguments)  # type: ignore[arg-type]

    def injected_unlink(absolute: Path) -> None:
        original_unlink(absolute)
        if failure_phase == "unlink" and absolute == ordered[failure_index]:
            raise PersonalDevNativeBuilderRuntimeInstallError("injected_failure")

    monkeypatch.setattr(runtime_installer, "_read_regular", injected_read)
    monkeypatch.setattr(installer, "_unlink", injected_unlink)

    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
        installer.discard_agent_stage()

    assert observed == list(ordered)
    for index, absolute in enumerate(ordered):
        assert _mapped(installer, absolute).exists() is (
            failure_phase == "validate" and index == failure_index
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent_image", "repo:mutable"),
        ("builder_image", "repo:mutable"),
        ("service_url", "http://loom.example.invalid"),
        ("agent_instance_id", "not-a-uuid"),
        ("key_id", "bad key id"),
    ],
)
def test_stage_agent_rejects_mutable_or_malformed_release_bindings(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    arguments = {
        "agent_image": "example.invalid/agent@sha256:" + "a" * 64,
        "builder_image": "example.invalid/builder@sha256:" + "b" * 64,
        "service_url": "https://loom.example.invalid",
        "agent_instance_id": "00000000-0000-0000-0000-000000000001",
        "key_id": "native-builder-v1",
        "private_key": key,
        "ca_file": ca,
    }
    arguments[field] = value

    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
        installer.stage_agent(**arguments)


def test_verify_active_requires_exact_services_socket_runtime_and_empty_inventory(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(dockerd_active=True, agent_active=True)
    installer = _installer(tmp_path, profile, runner=runner)
    runner.dockerd_active = False
    runner.agent_active = False
    installer.install(archive)
    key, ca = _stage_inputs(tmp_path)
    installer.stage_agent(
        agent_image="example.invalid/agent@sha256:" + "a" * 64,
        builder_image="example.invalid/builder@sha256:" + "b" * 64,
        service_url="https://loom.example.invalid",
        agent_instance_id="00000000-0000-0000-0000-000000000001",
        key_id="native-builder-v1",
        private_key=key,
        ca_file=ca,
    )
    runner.dockerd_active = True
    runner.agent_active = True
    runner.nft_table_output = profile.nftables.decode("ascii")
    socket = _mapped(installer, profile.docker_socket)
    socket.parent.mkdir(parents=True, exist_ok=True)
    socket.touch(mode=profile.socket_mode)
    socket.chmod(profile.socket_mode)

    assert installer.verify_active()["state"] == "active"

    runner.nft_table_output = runner.nft_table_output.replace(
        "tcp dport { 80, 443 } accept",
        "tcp dport 443 accept",
    )
    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="nftables_state_invalid",
    ):
        installer.verify_active()
    runner.nft_table_output = profile.nftables.decode("ascii")

    runner.dedicated_containers = ("foreign",)
    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError, match="busy"):
        installer.verify_active()


def test_verify_active_requires_staged_agent_material(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner()
    installer = _installer(tmp_path, profile, runner=runner)
    installer.install(archive)
    runner.dockerd_active = True
    runner.agent_active = True
    socket = _mapped(installer, profile.docker_socket)
    socket.parent.mkdir(parents=True, exist_ok=True)
    socket.touch(mode=profile.socket_mode)
    socket.chmod(profile.socket_mode)

    with pytest.raises(
        PersonalDevNativeBuilderRuntimeInstallError,
        match="agent_stage_invalid",
    ):
        installer.verify_active()


def test_remove_refuses_busy_or_drifted_state_and_removes_only_exact_install(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner()
    installer = _installer(tmp_path, profile, runner=runner)
    installer.install(archive)
    runner.dedicated_networks = ("loom-pdev-foreign",)
    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError, match="busy"):
        installer.remove()
    runner.dedicated_networks = ()
    config = _mapped(installer, profile.dockerd_config_path)
    config.chmod(0o644)
    config.write_bytes(config.read_bytes() + b"drift")
    config.chmod(0o444)
    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
        installer.remove()
    config.chmod(0o644)
    config.write_bytes(profile.dockerd_json)
    config.chmod(0o444)

    assert installer.remove() == {
        "operation": "remove",
        "profile_sha256": profile.sha256,
        "release": profile.version,
        "retained": "dedicated-image-cache-and-system-identities",
        "state": "managed-files-absent",
    }
    assert not _mapped(installer, profile.release_root).exists()
    assert _mapped(installer, Path("/var/lib")).is_dir()


def test_remove_temporarily_starts_only_dedicated_daemon_for_inventory(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner(calls=[])
    installer = _installer(tmp_path, profile, runner=runner)
    installer.install(archive)

    assert installer.remove()["state"] == "managed-files-absent"

    service_calls = [
        call
        for call in runner.calls or []
        if Path(call[0]).name == "systemctl" and call[1] in {"start", "stop"}
    ]
    assert service_calls == [
        ("/usr/bin/systemctl", "start", profile.dockerd_service_path.name),
        ("/usr/bin/systemctl", "stop", profile.dockerd_service_path.name),
    ]
    assert all(profile.agent_service_path.name not in call for call in service_calls)
    assert (
        "/usr/bin/docker",
        "-H",
        f"unix://{profile.docker_socket}",
        "network",
        "ls",
        "--quiet",
        "--filter",
        "type=custom",
    ) in (runner.calls or [])


def test_remove_retry_accepts_only_the_exact_already_absent_installation(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)

    first = installer.remove()
    second = installer.remove()

    assert second == first
    partial = _mapped(installer, profile.dockerd_config_path)
    partial.parent.mkdir(parents=True, exist_ok=True)
    partial.write_bytes(profile.dockerd_json)
    partial.chmod(0o444)
    with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
        installer.remove()


def test_cli_emits_canonical_receipt_and_requires_operation_arguments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeInstaller:
        def verify_staged(self) -> dict[str, object]:
            return {"z": 1, "a": "safe"}

    assert (
        main(
            ["verify-staged", "--profile", str(_PROFILE_PATH)],
            installer_factory=lambda profile: FakeInstaller(),
        )
        == 0
    )
    assert capsys.readouterr().out == '{"a":"safe","z":1}\n'

    assert (
        main(
            ["install", "--profile", str(_PROFILE_PATH)],
            installer_factory=lambda profile: FakeInstaller(),
        )
        == 2
    )
    assert capsys.readouterr().err == "error:arguments_invalid\n"


def test_context_rejects_relative_or_parent_traversal_roots() -> None:
    for root in (Path("relative"), Path("/tmp/../unsafe")):
        with pytest.raises(PersonalDevNativeBuilderRuntimeInstallError):
            NativeBuilderInstallContext(root=root)
