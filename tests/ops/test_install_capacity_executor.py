from __future__ import annotations

import io
import json
import os
import tarfile
from pathlib import Path
from typing import Any

import pytest
from scripts.ops.capacity_executor_release import record_release
from scripts.ops.install_capacity_executor import (
    CapacityExecutorInstallError,
    CommandResult,
    ControllerInstaller,
    InstallContext,
    _extract_release_tar,
    _validate_image_reference,
)

_DIGEST = "a" * 64
_IMAGE = f"ghcr.io/qianyi-sun/loom-capacity-executor@sha256:{_DIGEST}"
_SOURCE_SHA = "1" * 40
_REPO_ROOT = Path(__file__).resolve().parents[2]
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_TMPFILES = b"d /run/loom-capacity-executor 0700 loom_capacity_executor loom_capacity_executor -\n"


def _tar(entries: tuple[tuple[str, bytes | None, int, str], ...]) -> io.BytesIO:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        for name, payload, mode, kind in entries:
            member = tarfile.TarInfo(name)
            member.mode = mode
            if kind == "file":
                assert payload is not None
                member.size = len(payload)
                bundle.addfile(member, io.BytesIO(payload))
            elif kind == "dir":
                member.type = tarfile.DIRTYPE
                bundle.addfile(member)
            else:
                member.type = tarfile.SYMTYPE
                member.linkname = "release-manifest.json"
                bundle.addfile(member)
    archive.seek(0)
    return archive


def test_image_reference_requires_the_exact_executor_repository_and_digest() -> None:
    assert _validate_image_reference(_IMAGE) == _DIGEST

    for invalid in (
        "ghcr.io/qianyi-sun/loom-capacity-executor:latest",
        f"docker.io/qianyi-sun/loom-capacity-executor@sha256:{_DIGEST}",
        f"ghcr.io/qianyi-sun/loom-capacity-manager@sha256:{_DIGEST}",
        f"ghcr.io/qianyi-sun/loom-capacity-executor@sha256:{'A' * 64}",
    ):
        with pytest.raises(CapacityExecutorInstallError, match="digest reference"):
            _validate_image_reference(invalid)


def test_release_tar_extraction_preserves_exact_regular_file_bytes_and_modes(
    tmp_path: Path,
) -> None:
    stream = _tar(
        (
            ("payload", None, 0o555, "dir"),
            ("payload/wheelhouse", None, 0o555, "dir"),
            ("payload/wheelhouse/loom.whl", b"wheel", 0o444, "file"),
            ("release-manifest.json", b"{}\n", 0o444, "file"),
        )
    )

    _extract_release_tar(stream, tmp_path)

    wheel = tmp_path / "payload/wheelhouse/loom.whl"
    assert wheel.read_bytes() == b"wheel"
    assert wheel.stat().st_mode & 0o777 == 0o444
    assert (tmp_path / "payload").stat().st_mode & 0o777 == 0o555


@pytest.mark.parametrize(
    "entries",
    (
        (("../outside", b"escape", 0o444, "file"),),
        (("payload/link", None, 0o777, "symlink"),),
        (
            ("payload", None, 0o555, "dir"),
            ("payload/value", b"one", 0o444, "file"),
            ("payload/value", b"two", 0o444, "file"),
        ),
    ),
)
def test_release_tar_extraction_rejects_unsafe_or_duplicate_members(
    tmp_path: Path,
    entries: tuple[tuple[str, bytes | None, int, str], ...],
) -> None:
    with pytest.raises(CapacityExecutorInstallError, match="archive"):
        _extract_release_tar(_tar(entries), tmp_path)


class FakeHostRunner:
    def __init__(
        self,
        root: Path,
        *,
        image_architecture: str = "amd64",
        image_revision: str = _SOURCE_SHA,
        repo_digests: tuple[str, ...] = (_IMAGE,),
        active_units: tuple[str, ...] = (),
        enabled_units: tuple[str, ...] = (),
        unit_file_states: dict[str, str] | None = None,
        supplementary_gids: tuple[int, ...] = (),
    ) -> None:
        self.root = root
        self.image_architecture = image_architecture
        self.image_revision = image_revision
        self.repo_digests = repo_digests
        self.active_units = set(active_units)
        self.enabled_units = set(enabled_units)
        self.unit_file_states = unit_file_states or {}
        self.supplementary_gids = supplementary_gids
        self.group_present = False
        self.user_present = False
        self.units_verified = False
        self.runtime_probe_fails = False
        self.calls: list[tuple[str, ...]] = []

    def _path(self, absolute: str) -> Path:
        path = Path(absolute)
        assert path.is_absolute()
        return self.root.joinpath(*path.parts[1:])

    def run(
        self,
        argv: tuple[str, ...] | list[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        del env
        call = tuple(argv)
        self.calls.append(call)
        command = Path(call[0]).name
        result: CommandResult
        if command == "systemctl" and call[1] == "is-active":
            unit = call[2]
            active = unit in self.active_units
            result = CommandResult(0 if active else 3, "active\n" if active else "inactive\n")
        elif command == "systemctl" and call[1] == "is-enabled":
            unit = call[2]
            state = self.unit_file_states.get(
                unit,
                "enabled" if unit in self.enabled_units else "disabled",
            )
            result = CommandResult(0 if state != "disabled" else 1, f"{state}\n")
        elif command == "systemctl" and call[1:] == ("daemon-reload",):
            result = CommandResult(0)
        elif command == "systemd-analyze" and call[1] == "verify":
            assert {Path(path).name for path in call[2:]} == set(_UNITS)
            current = self.root / "opt/loom-capacity-executor"
            assert current.is_symlink()
            self.units_verified = True
            result = CommandResult(0)
        elif command == "docker" and call[1:3] == ("pull", "--quiet"):
            assert call[3] == _IMAGE
            result = CommandResult(0, f"{_IMAGE}\n")
        elif command == "docker" and call[1:3] == ("image", "inspect"):
            assert call[3] == _IMAGE
            result = CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "Architecture": self.image_architecture,
                            "Os": "linux",
                            "RepoDigests": list(self.repo_digests),
                            "Config": {
                                "Labels": {"org.opencontainers.image.revision": self.image_revision}
                            },
                        }
                    ]
                ),
            )
        elif command == "getent" and call[1:] == ("group", "loom_capacity_executor"):
            result = CommandResult(
                0 if self.group_present else 2,
                (f"loom_capacity_executor:x:{os.getegid()}:\n" if self.group_present else ""),
            )
        elif command == "getent" and call[1:] == ("passwd", "loom_capacity_executor"):
            result = CommandResult(
                0 if self.user_present else 2,
                (
                    "loom_capacity_executor:x:"
                    f"{os.geteuid()}:{os.getegid()}::/var/lib/loom-capacity-executor:"
                    "/usr/sbin/nologin\n"
                    if self.user_present
                    else ""
                ),
            )
        elif command == "id" and call[1:] == ("-u", "loom_capacity_executor"):
            result = CommandResult(0 if self.user_present else 1, f"{os.geteuid()}\n")
        elif command == "id" and call[1:] == ("-g", "loom_capacity_executor"):
            result = CommandResult(0 if self.user_present else 1, f"{os.getegid()}\n")
        elif command == "id" and call[1:] == ("-G", "loom_capacity_executor"):
            gids = (os.getegid(), *self.supplementary_gids)
            result = CommandResult(
                0 if self.user_present else 1,
                " ".join(str(gid) for gid in gids) + "\n",
            )
        elif command == "groupadd":
            self.group_present = True
            result = CommandResult(0)
        elif command == "useradd":
            assert self.group_present
            self.user_present = True
            result = CommandResult(0)
        elif command == "python3.12" and call[1:4] == ("-m", "venv", "--copies"):
            venv = self._path(call[4])
            (venv / "bin").mkdir(parents=True)
            (venv / "bin/python").write_bytes(b"python-copy")
            (venv / "bin/python").chmod(0o755)
            (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
            result = CommandResult(0)
        elif command == "python" and "/venv/bin/" in call[0]:
            if "loom-0.0.0-py3-none-any.whl" in " ".join(call):
                launcher = self._path(
                    str(Path(call[0]).with_name("loom-capacity-trusted-launcher"))
                )
                launcher.write_bytes(b"launcher")
                launcher.chmod(0o755)
            is_runtime_probe = "-I" in call
            result = CommandResult(1 if self.runtime_probe_fails and is_runtime_probe else 0)
        elif command == "loom-capacity-trusted-launcher":
            result = CommandResult(1 if self.runtime_probe_fails else 0)
        elif command == "systemd-tmpfiles":
            runtime = self._path("/run/loom-capacity-executor")
            runtime.mkdir(parents=True, exist_ok=True)
            runtime.chmod(0o700)
            result = CommandResult(0)
        else:
            raise AssertionError(f"unexpected command: {call}")
        if check and result.returncode != 0:
            raise CapacityExecutorInstallError(f"command failed safely: {command}")
        return result


def _fake_release_extractor(
    image: str,
    destination: Path,
    runner: Any,
    context: InstallContext,
) -> None:
    del runner, context
    assert image == _IMAGE
    payload = destination / "payload"
    wheelhouse = payload / "wheelhouse"
    units = payload / "units"
    tmpfiles = payload / "tmpfiles"
    wheelhouse.mkdir(parents=True)
    units.mkdir()
    tmpfiles.mkdir()
    files = {
        payload / "requirements.lock": b"dependency==1 --hash=sha256:" + b"b" * 64 + b"\n",
        wheelhouse / "dependency-1-py3-none-any.whl": b"dependency",
        wheelhouse / "loom-0.0.0-py3-none-any.whl": b"loom",
        # Docker COPY preserves the checked-in source filename in the release payload.
        tmpfiles / "loom-capacity-executor.tmpfiles": _TMPFILES,
    }
    for unit in _UNITS:
        files[units / unit] = (_REPO_ROOT / "deploy/dev-fleet" / unit).read_bytes()
    for path, value in files.items():
        path.write_bytes(value)
        path.chmod(0o444)
    record_release(destination, source_sha=_SOURCE_SHA, architecture="amd64")


def _context(tmp_path: Path) -> InstallContext:
    return InstallContext(
        root=tmp_path,
        command_prefix=(),
        authority_uid=os.geteuid(),
        authority_gid=os.getegid(),
    )


def test_installer_publishes_an_immutable_release_but_leaves_every_unit_inert(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    runner = FakeHostRunner(tmp_path)

    result = ControllerInstaller(
        context=context,
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    ).install(image=_IMAGE, source_sha=_SOURCE_SHA)

    expected_release = Path(f"/opt/loom-capacity-executor-releases/{_SOURCE_SHA}-amd64-{_DIGEST}")
    assert result.release_root == expected_release
    current = tmp_path / "opt/loom-capacity-executor"
    assert current.is_symlink()
    assert os.readlink(current) == str(expected_release)
    for unit in _UNITS:
        installed = tmp_path / "etc/systemd/system" / unit
        assert installed.read_bytes() == (_REPO_ROOT / "deploy/dev-fleet" / unit).read_bytes()
        assert installed.stat().st_mode & 0o777 == 0o644
        assert unit not in runner.active_units
        assert unit not in runner.enabled_units
    tmpfiles = tmp_path / "etc/tmpfiles.d/loom-capacity-executor.conf"
    assert tmpfiles.read_bytes() == _TMPFILES
    assert tmpfiles.stat().st_mode & 0o777 == 0o644
    for directory in (
        tmp_path / "etc/loom-capacity-executor",
        tmp_path / "run/loom-capacity-executor",
        tmp_path / "var/lib/loom-capacity-executor",
    ):
        assert directory.is_dir()
        assert directory.stat().st_mode & 0o777 == 0o700
    assert list((tmp_path / "etc/loom-capacity-executor").iterdir()) == []
    assert runner.units_verified is True
    release = tmp_path.joinpath(*expected_release.parts[1:])
    assert not (release / ".installing").exists()
    for path in release.rglob("*"):
        if not path.is_symlink():
            assert path.stat().st_mode & 0o022 == 0


@pytest.mark.parametrize(
    ("active_units", "enabled_units"),
    (((_UNITS[1],), ()), ((), (_UNITS[2],))),
)
def test_installer_refuses_existing_active_or_enabled_units_before_extraction(
    tmp_path: Path,
    active_units: tuple[str, ...],
    enabled_units: tuple[str, ...],
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    runner = FakeHostRunner(
        tmp_path,
        active_units=active_units,
        enabled_units=enabled_units,
    )
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="active or enabled"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False
    assert not (tmp_path / "opt/loom-capacity-executor-releases").exists()
    assert not (tmp_path / "etc/systemd/system").exists()


def test_installer_refuses_indirect_unit_enablement_before_extraction(
    tmp_path: Path,
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    runner = FakeHostRunner(
        tmp_path,
        unit_file_states={_UNITS[2]: "indirect"},
    )
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="active or enabled"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False


@pytest.mark.parametrize(
    ("runner_kwargs", "message"),
    (
        ({"image_architecture": "arm64"}, "architecture"),
        ({"image_revision": "2" * 40}, "revision"),
        ({"repo_digests": ()}, "digest"),
    ),
)
def test_installer_rejects_oci_identity_drift_before_extraction(
    tmp_path: Path,
    runner_kwargs: dict[str, object],
    message: str,
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path, **runner_kwargs),
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match=message):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False
    assert not (tmp_path / "opt/loom-capacity-executor-releases").exists()


def test_installer_refuses_an_intermediate_authority_symlink(
    tmp_path: Path,
) -> None:
    redirected = tmp_path / "redirected-etc"
    redirected.mkdir()
    (tmp_path / "etc").symlink_to(redirected, target_is_directory=True)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match=r"parent|directory"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert not (redirected / "systemd/system/loom-capacity-pool-executor.service").exists()


def test_installer_refuses_a_service_identity_with_supplementary_groups(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path, supplementary_gids=(4242,))
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="service identity"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert not (tmp_path / "opt/loom-capacity-executor").is_symlink()


def test_installer_reuses_only_the_same_complete_immutable_release(
    tmp_path: Path,
) -> None:
    extractions = 0

    def counted_extractor(
        image: str,
        destination: Path,
        runner: Any,
        context: InstallContext,
    ) -> None:
        nonlocal extractions
        extractions += 1
        _fake_release_extractor(image, destination, runner, context)

    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=counted_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    first = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    second = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert second == first
    assert extractions == 1


def test_installer_reprobes_an_existing_immutable_release(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )
    installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    runner.runtime_probe_fails = True

    with pytest.raises(CapacityExecutorInstallError, match="command failed safely"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)


def test_installer_rejects_drift_anywhere_in_an_existing_runtime_tree(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )
    result = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    drifted = tmp_path.joinpath(*result.release_root.parts[1:]) / "venv/pyvenv.cfg"
    drifted.chmod(0o666)

    with pytest.raises(CapacityExecutorInstallError, match="runtime authority"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)


def test_installer_rejects_a_foreign_current_release_symlink_before_extraction(
    tmp_path: Path,
) -> None:
    (tmp_path / "opt").mkdir()
    (tmp_path / "opt/loom-capacity-executor").symlink_to("/tmp/foreign-release")
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="foreign"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False


def test_installer_removes_a_manifest_drifted_incomplete_release(
    tmp_path: Path,
) -> None:
    def tampered_extractor(
        image: str,
        destination: Path,
        runner: Any,
        context: InstallContext,
    ) -> None:
        _fake_release_extractor(image, destination, runner, context)
        wheel = destination / "payload/wheelhouse/loom-0.0.0-py3-none-any.whl"
        wheel.chmod(0o644)
        wheel.write_bytes(b"tampered")
        wheel.chmod(0o444)

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=tampered_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="release verification"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    releases = tmp_path / "opt/loom-capacity-executor-releases"
    assert releases.is_dir()
    assert list(releases.iterdir()) == []
    assert not (tmp_path / "opt/loom-capacity-executor").exists()
