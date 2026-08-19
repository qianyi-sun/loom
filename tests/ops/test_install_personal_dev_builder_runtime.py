from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import subprocess
import tarfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
import scripts.ops.install_personal_dev_builder_runtime as runtime_installer
from scripts.ops.install_personal_dev_builder_runtime import (
    CommandResult,
    InstallContext,
    PersonalDevBuilderRuntimeInstaller,
    PersonalDevBuilderRuntimeInstallError,
    Runner,
    SubprocessRunner,
    main,
)
from scripts.ops.personal_dev_builder_runtime_profile import (
    RuntimeArchiveMember,
    RuntimeProfile,
    load_runtime_profile,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_PATH = _REPO_ROOT / "deploy/dev-fleet/personal-dev-builder-runtime-profile.json"
_ACTIVE_CONTAINERD_CONFIG = Path("/var/lib/rancher/k3s/agent/etc/containerd/config.toml")
_SERVICE_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


@dataclass(frozen=True, slots=True)
class ArchiveEntry:
    name: str
    payload: bytes = b""
    mode: int = 0o755
    kind: bytes = tarfile.REGTYPE
    linkname: str = ""


@dataclass(slots=True)
class HostRunner:
    agent_active: bool = True
    server_active: bool = False
    device_ready: bool = True
    available_bytes: int = 30 * 1024**3
    k3s_version: str = "v1.36.2+k3s1"
    containerd_version: str = "v2.3.2-k3s2"
    runsc_version: str = "release-20260810.0"
    runsc_spec_version: str = "1.2.1"
    main_pid: int = 4242

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        del env
        call = tuple(argv)
        executable = Path(call[0]).name
        result = CommandResult(127, stderr="unexpected external command")
        if executable == "systemctl" and call[1:3] == ("is-active", "k3s-agent"):
            result = CommandResult(
                0 if self.agent_active else 3, "active\n" if self.agent_active else "inactive\n"
            )
        elif executable == "systemctl" and call[1:3] == ("is-active", "k3s"):
            result = CommandResult(
                0 if self.server_active else 3, "active\n" if self.server_active else "inactive\n"
            )
        elif executable == "systemctl" and "MainPID" in " ".join(call):
            result = CommandResult(0, f"{self.main_pid}\n")
        elif executable == "test" and call[1] == "-c":
            result = CommandResult(0 if self.device_ready else 1)
        elif executable == "df":
            result = CommandResult(0, f"Avail\n{self.available_bytes}\n")
        elif executable == "k3s" and call[1:] == ("--version",):
            result = CommandResult(
                0,
                f"k3s version {self.k3s_version} (test)\ngo version go1.25.1\n",
            )
        elif executable == "k3s" and call[1:] == ("ctr", "version"):
            result = CommandResult(
                0,
                "Client:\n"
                f"  Version:  {self.containerd_version}\n"
                "Server:\n"
                f"  Version:  {self.containerd_version}\n",
            )
        elif executable == "runsc" and call[1:] == ("--version",):
            result = CommandResult(
                0,
                f"runsc version {self.runsc_version}\n"
                f"spec: {self.runsc_spec_version}\n",
            )
        if check and result.returncode != 0:
            raise PersonalDevBuilderRuntimeInstallError("command_failed")
        return result


def _profile() -> RuntimeProfile:
    return load_runtime_profile(_PROFILE_PATH)


def _small_profile() -> tuple[RuntimeProfile, dict[str, bytes]]:
    profile = _profile()
    payloads = {name: f"measured payload for {name}\n".encode("ascii") for name in profile.members}
    members = {
        name: RuntimeArchiveMember(
            size=len(payload),
            archive_mode=0o755,
            install_mode=0o555,
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        for name, payload in payloads.items()
    }
    return replace(profile, members=MappingProxyType(members)), payloads


def _entries(profile: RuntimeProfile, payloads: dict[str, bytes]) -> list[ArchiveEntry]:
    entries = [
        ArchiveEntry(
            name=name,
            payload=payloads[name],
            mode=profile.members[name].archive_mode,
        )
        for name in profile.members
    ]
    entries.insert(
        2,
        ArchiveEntry(
            name="gvisor-bin",
            mode=0o755,
            kind=tarfile.DIRTYPE,
        ),
    )
    return entries


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
) -> tuple[RuntimeProfile, Path]:
    profile, payloads = _small_profile()
    entries = _entries(profile, payloads)
    if mutate is not None:
        mutate(entries)
    path = tmp_path / "gvisor.tar.bz2"
    digest = _write_archive(path, entries)
    return replace(profile, archive_sha512=digest), path


def _host_root(tmp_path: Path, profile: RuntimeProfile) -> Path:
    root = tmp_path / "host"
    modules = root / "proc/modules"
    modules.parent.mkdir(parents=True)
    modules.write_text(
        "kvm_intel 491520 0 - Live 0x0\nkvm 1499136 1 kvm_intel, Live 0x0\n",
        encoding="ascii",
    )
    environment = root / "proc/4242/environ"
    environment.parent.mkdir(parents=True)
    environment.write_bytes(f"PATH={_SERVICE_PATH}\0LANG=C.UTF-8\0".encode("ascii"))
    (root / "var/lib/rancher/k3s").mkdir(parents=True)
    (root / profile.device_path.relative_to("/")).parent.mkdir(parents=True)
    root.chmod(0o755)
    for directory in sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
    ):
        directory.chmod(0o755)
    return root


def _installer(
    tmp_path: Path,
    profile: RuntimeProfile,
    *,
    runner: Runner | None = None,
    root: Path | None = None,
    machine: str = "x86_64",
    effective_uid: int | None = None,
) -> PersonalDevBuilderRuntimeInstaller:
    host_root = _host_root(tmp_path, profile) if root is None else root
    context = InstallContext(
        root=host_root,
        authority_uid=os.getuid(),
        authority_gid=os.getgid(),
    )
    return PersonalDevBuilderRuntimeInstaller(
        profile=profile,
        context=context,
        runner=runner or HostRunner(),
        machine=machine,
        effective_uid=os.geteuid() if effective_uid is None else effective_uid,
    )


def _mapped(installer: PersonalDevBuilderRuntimeInstaller, path: Path) -> Path:
    return installer.context.path(path)


def _managed_paths(
    installer: PersonalDevBuilderRuntimeInstaller,
) -> tuple[Path, ...]:
    profile = installer.profile
    return (
        _mapped(installer, profile.release_root),
        _mapped(installer, profile.profile_path),
        _mapped(installer, profile.runsc_config_path),
        _mapped(installer, profile.k3s_template_path),
        _mapped(installer, profile.shim_link_path),
    )


def _assert_unpublished(installer: PersonalDevBuilderRuntimeInstaller) -> None:
    assert all(not path.exists() and not path.is_symlink() for path in _managed_paths(installer))


def test_preflight_streams_the_exact_archive_without_publishing(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)

    receipt = installer.preflight(archive)

    assert receipt == {
        "archive_sha512": profile.archive_sha512,
        "operation": "preflight",
        "profile_sha256": profile.sha256,
        "release": profile.version,
    }
    _assert_unpublished(installer)


def test_preflight_accepts_the_archives_explicit_parent_directory(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)

    receipt = installer.preflight(archive)

    assert receipt["archive_sha512"] == profile.archive_sha512
    _assert_unpublished(installer)


def test_preflight_rejects_a_missing_explicit_parent_directory(
    tmp_path: Path,
) -> None:
    def remove_explicit_parent(entries: list[ArchiveEntry]) -> None:
        entries[:] = [entry for entry in entries if entry.name != "gvisor-bin"]

    profile, archive = _archive(tmp_path, mutate=remove_explicit_parent)
    installer = _installer(tmp_path, profile)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="archive_invalid",
    ):
        installer.preflight(archive)

    _assert_unpublished(installer)


def test_preflight_reads_procfs_modules_when_stat_reports_zero_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    modules_path = _mapped(installer, Path("/proc/modules"))
    real_fstat = os.fstat

    def procfs_fstat(descriptor: int) -> os.stat_result:
        metadata = real_fstat(descriptor)
        try:
            opened_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        except OSError:
            return metadata
        if opened_path != modules_path:
            return metadata
        fields = list(metadata)
        fields[stat.ST_SIZE] = 0
        return os.stat_result(fields)

    monkeypatch.setattr(runtime_installer.os, "fstat", procfs_fstat)

    assert installer.preflight(archive)["operation"] == "preflight"


def _traversal(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], name="../runsc")


def _absolute(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], name="/runsc")


def _duplicate(entries: list[ArchiveEntry]) -> None:
    entries.append(entries[0])


def _symlink(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], kind=tarfile.SYMTYPE, linkname="runsc")


def _hardlink(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], kind=tarfile.LNKTYPE, linkname="runsc")


def _device(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], kind=tarfile.CHRTYPE)


def _missing(entries: list[ArchiveEntry]) -> None:
    entries.pop()


def _extra(entries: list[ArchiveEntry]) -> None:
    entries.append(ArchiveEntry("unexpected", b"unexpected\n"))


def _wrong_size(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], payload=entries[0].payload + b"x")


def _wrong_mode(entries: list[ArchiveEntry]) -> None:
    entries[0] = replace(entries[0], mode=0o555)


def _wrong_hash(entries: list[ArchiveEntry]) -> None:
    payload = entries[0].payload
    entries[0] = replace(entries[0], payload=bytes([payload[0] ^ 1]) + payload[1:])


def _unexpected_directory(entries: list[ArchiveEntry]) -> None:
    entries.append(ArchiveEntry("foreign", mode=0o755, kind=tarfile.DIRTYPE))


def _wrong_directory_mode(entries: list[ArchiveEntry]) -> None:
    index = next(index for index, entry in enumerate(entries) if entry.name == "gvisor-bin")
    entries[index] = replace(entries[index], mode=0o700)


def _duplicate_directory(entries: list[ArchiveEntry]) -> None:
    directory = next(entry for entry in entries if entry.name == "gvisor-bin")
    entries.append(directory)


@pytest.mark.parametrize(
    "mutate",
    (
        _traversal,
        _absolute,
        _duplicate,
        _symlink,
        _hardlink,
        _device,
        _missing,
        _extra,
        _wrong_size,
        _wrong_mode,
        _wrong_hash,
        _unexpected_directory,
        _wrong_directory_mode,
        _duplicate_directory,
    ),
)
def test_archive_drift_is_rejected_before_publication(
    tmp_path: Path,
    mutate: Callable[[list[ArchiveEntry]], None],
) -> None:
    profile, archive = _archive(tmp_path, mutate=mutate)
    installer = _installer(tmp_path, profile)

    with pytest.raises(PersonalDevBuilderRuntimeInstallError):
        installer.preflight(archive)

    _assert_unpublished(installer)


def test_wrong_archive_digest_is_rejected_before_publication(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    profile = replace(profile, archive_sha512="0" * 128)
    installer = _installer(tmp_path, profile)

    with pytest.raises(PersonalDevBuilderRuntimeInstallError):
        installer.install(archive)

    _assert_unpublished(installer)


def test_archive_must_remain_owner_only(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    archive.chmod(0o644)
    installer = _installer(tmp_path, profile)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="archive_invalid",
    ):
        installer.preflight(archive)

    _assert_unpublished(installer)


def test_archive_must_be_owned_by_the_install_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    real_fstat = os.fstat

    def foreign_archive_fstat(descriptor: int) -> os.stat_result:
        metadata = real_fstat(descriptor)
        try:
            opened_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        except OSError:
            return metadata
        if opened_path != archive:
            return metadata
        fields = list(metadata)
        fields[stat.ST_UID] = installer.context.authority_uid + 1
        return os.stat_result(fields)

    monkeypatch.setattr(runtime_installer.os, "fstat", foreign_archive_fstat)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="archive_invalid",
    ):
        installer.preflight(archive)

    _assert_unpublished(installer)


def test_archive_size_is_bounded_to_the_download_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    real_fstat = os.fstat

    def oversized_archive_fstat(descriptor: int) -> os.stat_result:
        metadata = real_fstat(descriptor)
        try:
            opened_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        except OSError:
            return metadata
        if opened_path != archive:
            return metadata
        fields = list(metadata)
        fields[stat.ST_SIZE] = 1024**3 + 1
        return os.stat_result(fields)

    monkeypatch.setattr(runtime_installer.os, "fstat", oversized_archive_fstat)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="archive_invalid",
    ):
        installer.preflight(archive)

    _assert_unpublished(installer)


def test_install_publishes_only_exact_immutable_state_and_is_idempotent(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)

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
    for name, member in profile.members.items():
        path = _mapped(installer, profile.release_root / name)
        metadata = os.lstat(path)
        assert stat.S_ISREG(metadata.st_mode)
        assert stat.S_IMODE(metadata.st_mode) == member.install_mode
        assert (metadata.st_uid, metadata.st_gid, metadata.st_nlink) == (
            installer.context.authority_uid,
            installer.context.authority_gid,
            1,
        )
        assert hashlib.sha256(path.read_bytes()).hexdigest() == member.sha256
    for absolute, payload in (
        (profile.profile_path, profile.payload),
        (profile.runsc_config_path, profile.runsc_toml),
        (profile.k3s_template_path, profile.k3s_template),
    ):
        path = _mapped(installer, absolute)
        metadata = os.lstat(path)
        assert stat.S_ISREG(metadata.st_mode)
        assert stat.S_IMODE(metadata.st_mode) == 0o444
        assert (metadata.st_uid, metadata.st_gid, metadata.st_nlink) == (
            installer.context.authority_uid,
            installer.context.authority_gid,
            1,
        )
        assert path.read_bytes() == payload
    link = _mapped(installer, profile.shim_link_path)
    metadata = os.lstat(link)
    assert stat.S_ISLNK(metadata.st_mode)
    assert (metadata.st_uid, metadata.st_gid) == (
        installer.context.authority_uid,
        installer.context.authority_gid,
    )
    assert os.readlink(link) == str(profile.shim_path)
    assert installer.verify_staged()["state"] == "staged"


def test_install_fsyncs_files_and_publication_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    synced_kinds: list[str] = []
    real_fsync = os.fsync

    def record(descriptor: int) -> None:
        mode = os.fstat(descriptor).st_mode
        synced_kinds.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", record)

    installer.install(archive)

    assert synced_kinds.count("file") >= len(profile.members) + 3
    assert synced_kinds.count("directory") >= 5


def test_install_does_not_reuse_a_predictable_shim_temporary_name(
    tmp_path: Path,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    parent = _mapped(installer, profile.shim_link_path.parent)
    parent.mkdir(parents=True)
    for directory in (parent.parent.parent, parent.parent, parent):
        directory.chmod(0o755)
    stale = parent / f".{profile.shim_link_path.name}.{os.getpid()}.tmp"
    stale.symlink_to("/foreign/stale-shim")

    assert installer.install(archive)["state"] == "staged"
    assert stale.is_symlink()
    assert os.readlink(stale) == "/foreign/stale-shim"


def _install(tmp_path: Path) -> tuple[RuntimeProfile, Path, PersonalDevBuilderRuntimeInstaller]:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    installer.install(archive)
    return profile, archive, installer


@pytest.mark.parametrize("mutation", ("partial", "writable", "hardlink", "wrong_link", "changed"))
def test_partial_or_foreign_destination_is_rejected(
    tmp_path: Path,
    mutation: str,
) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)
    if mutation == "partial":
        path = _mapped(installer, profile.profile_path)
        path.parent.mkdir(parents=True)
        path.write_bytes(profile.payload)
        path.chmod(0o444)
    else:
        installer.install(archive)
        if mutation == "writable":
            _mapped(installer, profile.runsc_path).chmod(0o755)
        elif mutation == "hardlink":
            os.link(
                _mapped(installer, profile.runsc_config_path),
                tmp_path / "second-link",
            )
        elif mutation == "wrong_link":
            link = _mapped(installer, profile.shim_link_path)
            link.unlink()
            link.symlink_to("/foreign/shim")
        else:
            path = _mapped(installer, profile.k3s_template_path)
            path.chmod(0o644)
            path.write_bytes(b"foreign\n")
            path.chmod(0o444)

    with pytest.raises(PersonalDevBuilderRuntimeInstallError):
        installer.preflight(archive)


def test_non_authority_owned_destination_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, archive, installer = _install(tmp_path)
    target = _mapped(installer, profile.runsc_path)
    real_fstat = os.fstat

    def foreign_fstat(descriptor: int) -> os.stat_result:
        metadata = real_fstat(descriptor)
        try:
            opened_path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        except OSError:
            return metadata
        if opened_path != target:
            return metadata
        fields = list(metadata)
        fields[stat.ST_UID] = os.getuid() + 1
        return os.stat_result(fields)

    monkeypatch.setattr(runtime_installer.os, "fstat", foreign_fstat)

    with pytest.raises(PersonalDevBuilderRuntimeInstallError):
        installer.preflight(archive)


@pytest.mark.parametrize(
    ("change", "error_code"),
    (
        ("control-plane", "service_state_invalid"),
        ("agent-inactive", "service_state_invalid"),
        ("device", "kvm_device_invalid"),
        ("modules", "kvm_modules_invalid"),
        ("disk", "disk_capacity_invalid"),
        ("path", "service_path_invalid"),
        ("k3s", "k3s_version_invalid"),
        ("containerd", "containerd_version_invalid"),
        ("architecture", "architecture_invalid"),
    ),
)
def test_preflight_rejects_each_unmeasured_host_prerequisite(
    tmp_path: Path,
    change: str,
    error_code: str,
) -> None:
    profile, archive = _archive(tmp_path)
    runner = HostRunner()
    machine = "x86_64"
    root = _host_root(tmp_path, profile)
    if change == "control-plane":
        runner.server_active = True
    elif change == "agent-inactive":
        runner.agent_active = False
    elif change == "device":
        runner.device_ready = False
    elif change == "modules":
        (root / "proc/modules").write_text("kvm 1 0 - Live 0x0\n", encoding="ascii")
    elif change == "disk":
        runner.available_bytes = 20 * 1024**3 - 1
    elif change == "path":
        (root / "proc/4242/environ").write_bytes(b"PATH=/usr/sbin:/usr/bin:/sbin:/bin\0")
    elif change == "k3s":
        runner.k3s_version = "v1.36.3+k3s1"
    elif change == "containerd":
        runner.containerd_version = "v2.3.3-k3s2"
    else:
        machine = "aarch64"
    installer = _installer(tmp_path, profile, runner=runner, root=root, machine=machine)

    with pytest.raises(PersonalDevBuilderRuntimeInstallError, match=error_code):
        installer.preflight(archive)


def test_production_context_requires_effective_root(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = PersonalDevBuilderRuntimeInstaller(
        profile=profile,
        context=InstallContext(),
        runner=HostRunner(),
        machine="x86_64",
        effective_uid=1,
    )

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="authority_invalid",
    ):
        installer.preflight(archive)


def _active_config(profile: RuntimeProfile) -> bytes:
    return (
        "version = 3\n\n"
        "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
        f"'{profile.handler}']\n"
        f'  runtime_type = "{profile.runtime_type}"\n'
        "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
        f"'{profile.handler}'.options]\n"
        '  TypeUrl = "io.containerd.runsc.v1.options"\n'
        f'  ConfigPath = "{profile.runsc_config_path}"\n'
    ).encode("ascii")


def test_active_verification_requires_exact_generated_containerd_runtime(
    tmp_path: Path,
) -> None:
    profile, _, installer = _install(tmp_path)
    active = _mapped(installer, _ACTIVE_CONTAINERD_CONFIG)
    active.parent.mkdir(parents=True, exist_ok=True)
    active.write_bytes(_active_config(profile))

    assert installer.verify_active() == {
        "operation": "verify-active",
        "profile_sha256": profile.sha256,
        "release": profile.version,
        "state": "active",
    }

    active.write_bytes(
        _active_config(profile).replace(b"io.containerd.runsc.v1", b"io.containerd.runc.v2", 1)
    )
    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="active_runtime_invalid",
    ):
        installer.verify_active()


@pytest.mark.parametrize(
    "runner",
    (
        HostRunner(runsc_version="release-20260811.0"),
        HostRunner(runsc_spec_version="1.1.0"),
    ),
)
def test_active_verification_requires_exact_runsc_release(
    tmp_path: Path,
    runner: HostRunner,
) -> None:
    profile, _, installer = _install(tmp_path)
    active = _mapped(installer, _ACTIVE_CONTAINERD_CONFIG)
    active.parent.mkdir(parents=True, exist_ok=True)
    active.write_bytes(_active_config(profile))
    installer.runner = runner

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="runsc_version_invalid",
    ):
        installer.verify_active()


def test_remove_deletes_only_an_exact_installation(tmp_path: Path) -> None:
    profile, _, installer = _install(tmp_path)

    receipt = installer.remove()

    assert receipt == {
        "operation": "remove",
        "profile_sha256": profile.sha256,
        "release": profile.version,
        "state": "absent",
    }
    _assert_unpublished(installer)
    assert _mapped(installer, Path("/var/lib/rancher/k3s")).is_dir()
    assert _mapped(installer, Path("/proc/modules")).is_file()


def test_remove_preserves_everything_when_any_managed_byte_drifted(tmp_path: Path) -> None:
    profile, _, installer = _install(tmp_path)
    config = _mapped(installer, profile.runsc_config_path)
    config.chmod(0o644)
    config.write_bytes(config.read_bytes() + b"# drift\n")
    config.chmod(0o444)
    before = tuple((path, path.exists(), path.is_symlink()) for path in _managed_paths(installer))

    with pytest.raises(PersonalDevBuilderRuntimeInstallError):
        installer.remove()

    assert (
        tuple((path, path.exists(), path.is_symlink()) for path in _managed_paths(installer))
        == before
    )


class _CliInstaller:
    def __init__(self, receipt: dict[str, object] | None = None, error: str | None = None) -> None:
        self.receipt = receipt or {"operation": "verify-staged", "state": "staged"}
        self.error = error

    def _result(self) -> dict[str, object]:
        if self.error is not None:
            raise PersonalDevBuilderRuntimeInstallError(self.error)
        return self.receipt

    def preflight(self, archive: Path) -> dict[str, object]:
        del archive
        return self._result()

    def install(self, archive: Path) -> dict[str, object]:
        del archive
        return self._result()

    def verify_staged(self) -> dict[str, object]:
        return self._result()

    def verify_active(self) -> dict[str, object]:
        return self._result()

    def remove(self) -> dict[str, object]:
        return self._result()


def test_cli_emits_one_sorted_compact_json_receipt(
    capsys: pytest.CaptureFixture[str],
) -> None:
    installer = _CliInstaller({"z": 1, "a": "safe"})

    result = main(
        ["verify-staged", "--profile", str(_PROFILE_PATH)],
        installer_factory=lambda profile: installer,
    )

    assert result == 0
    captured = capsys.readouterr()
    assert captured.out == '{"a":"safe","z":1}\n'
    assert captured.err == ""


@pytest.mark.parametrize(
    "argv",
    (
        ["preflight", "--profile", str(_PROFILE_PATH)],
        ["install", "--profile", str(_PROFILE_PATH)],
        [
            "verify-staged",
            "--profile",
            str(_PROFILE_PATH),
            "--archive",
            "/tmp/archive",
        ],
        [
            "verify-active",
            "--profile",
            str(_PROFILE_PATH),
            "--archive",
            "/tmp/archive",
        ],
        ["remove", "--profile", str(_PROFILE_PATH), "--archive", "/tmp/archive"],
    ),
)
def test_cli_requires_archive_only_for_archive_operations(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(argv, installer_factory=lambda profile: _CliInstaller())

    assert result == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error:arguments_invalid\n"


def test_cli_bounds_installer_errors_without_paths_or_command_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(
        ["verify-staged", "--profile", str(_PROFILE_PATH)],
        installer_factory=lambda profile: _CliInstaller(
            error="archive_invalid",
        ),
    )

    assert result == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error:archive_invalid\n"
    assert str(_PROFILE_PATH) not in captured.err


def test_context_rejects_relative_or_parent_traversal_roots() -> None:
    for root in (Path("relative"), Path("/tmp/../unsafe")):
        with pytest.raises(PersonalDevBuilderRuntimeInstallError):
            InstallContext(root=root)


def test_preflight_rejects_a_writable_host_root(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    root = _host_root(tmp_path, profile)
    root.chmod(0o777)
    installer = _installer(tmp_path, profile, root=root)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="parent_directory_invalid",
    ):
        installer.preflight(archive)


def test_subprocess_runner_contract_is_structural() -> None:
    assert isinstance(HostRunner(), Runner)


def test_subprocess_runner_fails_closed_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def time_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        raise subprocess.TimeoutExpired(["safe-command"], 30)

    monkeypatch.setattr(runtime_installer.subprocess, "run", time_out)

    with pytest.raises(
        PersonalDevBuilderRuntimeInstallError,
        match="command_timeout",
    ):
        SubprocessRunner().run(["/usr/bin/true"])


def test_receipts_remain_json_safe_and_bounded(tmp_path: Path) -> None:
    profile, archive = _archive(tmp_path)
    installer = _installer(tmp_path, profile)

    receipts: list[dict[str, Any]] = [
        installer.preflight(archive),
        installer.install(archive),
        installer.verify_staged(),
    ]

    for receipt in receipts:
        encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        assert len(encoded) < 1024
        assert str(tmp_path) not in encoded
