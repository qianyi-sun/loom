#!/usr/bin/env python3
"""Run the one-time exporter bootstrap through a constrained Docker channel."""

from __future__ import annotations

import ctypes
import hashlib
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Final

APPROVED_BASE_SHA: Final = "eed7ff5eb438cb1d9a715a8afa49da94e9fee5eb"
APPROVED_ORIGIN: Final = "https://github.com/qianyi-sun/loom.git"
BUNDLE: Final = Path("/run/loom-handoff/loom.bundle")
SOURCE_PARENT: Final = Path("/opt/loom-staging-exporter-authority")
SOURCE: Final = SOURCE_PARENT / "source"
AUTHORITY_RELATIVE: Final = Path("scripts/ops/staging_rollout_shared_work2_export_authority.py")
VALIDATOR_RELATIVE: Final = Path("scripts/ops/staging_rollout_sealed_source.py")
MOUNTINFO: Final = Path("/proc/self/mountinfo")
STATUS: Final = Path("/proc/self/status")
NETWORK_CLASS: Final = Path("/sys/class/net")
IPV4_ROUTES: Final = Path("/proc/net/route")
IPV6_ROUTES: Final = Path("/proc/net/ipv6_route")
EXPECTED_CAPABILITY_MASK: Final = (1 << 0) | (1 << 1) | (1 << 3)
EXPECTED_WRITABLE_BINDS: Final = (Path("/opt"), Path("/usr/local"), Path("/etc"), Path("/var/lib"))
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
MAX_BUNDLE_BYTES: Final = 64 * 1024 * 1024
RENAME_NOREPLACE: Final = 1
AT_FDCWD: Final = -100


class DockerBootstrapError(RuntimeError):
    """A bounded Docker bootstrap failure safe for operator output."""


@dataclass(frozen=True, slots=True)
class Identity:
    source_sha: str
    source_tree_sha: str
    source_base_sha: str
    bundle_sha256: str

    @classmethod
    def from_environ(cls) -> Identity:
        values = {
            "source_sha": os.environ.get("LOOM_SEALED_SOURCE_SHA", ""),
            "source_tree_sha": os.environ.get("LOOM_SEALED_SOURCE_TREE", ""),
            "source_base_sha": os.environ.get("LOOM_SEALED_SOURCE_BASE", ""),
            "bundle_sha256": os.environ.get("LOOM_SEALED_BUNDLE_SHA256", ""),
        }
        if any(SHA_RE.fullmatch(values[key]) is None for key in values if key != "bundle_sha256"):
            raise DockerBootstrapError("sealed Docker bootstrap identity is invalid")
        if SHA256_RE.fullmatch(values["bundle_sha256"]) is None:
            raise DockerBootstrapError("sealed Docker bootstrap bundle identity is invalid")
        if values["source_base_sha"] != APPROVED_BASE_SHA:
            raise DockerBootstrapError("sealed Docker bootstrap base is not approved")
        if len({values["source_sha"], values["source_tree_sha"], values["source_base_sha"]}) != 3:
            raise DockerBootstrapError("sealed Docker bootstrap identities must be distinct")
        return cls(**values)


@dataclass(frozen=True, slots=True)
class MountRecord:
    root: str
    mount_point: str
    options: frozenset[str]


def _decode_mount_path(value: str) -> str:
    replacements = {"\\040": " ", "\\011": "\t", "\\012": "\n", "\\134": "\\"}
    for encoded, decoded in replacements.items():
        value = value.replace(encoded, decoded)
    return value


def _parse_mountinfo(payload: str) -> tuple[MountRecord, ...]:
    records: list[MountRecord] = []
    for line in payload.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError as exc:
            raise DockerBootstrapError("Docker bootstrap mountinfo is invalid") from exc
        if separator < 6:
            raise DockerBootstrapError("Docker bootstrap mountinfo is invalid")
        records.append(
            MountRecord(
                root=_decode_mount_path(fields[3]),
                mount_point=_decode_mount_path(fields[4]),
                options=frozenset(fields[5].split(",")),
            )
        )
    return tuple(records)


def _single_mount(records: tuple[MountRecord, ...], target: Path) -> MountRecord:
    matches = [record for record in records if record.mount_point == str(target)]
    if len(matches) != 1:
        raise DockerBootstrapError("Docker bootstrap mount contract is incomplete")
    return matches[0]


def _validate_network_boundary() -> None:
    try:
        interfaces = {
            path.name: (path / "operstate").read_text(encoding="ascii").strip()
            for path in NETWORK_CLASS.iterdir()
            if (path / "operstate").is_file()
        }
        ipv4_rows = IPV4_ROUTES.read_text(encoding="ascii").splitlines()[1:]
        ipv6_rows = IPV6_ROUTES.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap network boundary is unavailable") from exc
    if "lo" not in interfaces:
        raise DockerBootstrapError("Docker bootstrap network must be disabled")
    if any(state != "down" for name, state in interfaces.items() if name != "lo"):
        raise DockerBootstrapError("Docker bootstrap network must be disabled")
    for row in ipv4_rows:
        fields = row.split()
        if fields and (len(fields) < 11 or fields[0] != "lo"):
            raise DockerBootstrapError("Docker bootstrap network must be disabled")
    for row in ipv6_rows:
        fields = row.split()
        if fields and (len(fields) != 10 or fields[-1] != "lo"):
            raise DockerBootstrapError("Docker bootstrap network must be disabled")


def _validate_runtime() -> None:
    if sys.argv != [sys.argv[0]] or os.geteuid() != 0 or os.getegid() != 0:
        raise DockerBootstrapError("Docker bootstrap invocation is not fixed root")
    if any(key.startswith("SUDO_") for key in os.environ):
        raise DockerBootstrapError("Docker bootstrap rejects a sudo-derived process")
    try:
        status = dict(
            line.split(":", 1)
            for line in STATUS.read_text(encoding="ascii").splitlines()
            if ":" in line
        )
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap process status is unavailable") from exc
    expected = f"{EXPECTED_CAPABILITY_MASK:016x}"
    for key in ("CapPrm", "CapEff", "CapBnd"):
        if status.get(key, "").strip().lower() != expected:
            raise DockerBootstrapError("Docker bootstrap capability boundary is invalid")
    for key in ("CapInh", "CapAmb"):
        if int(status.get(key, "-1").strip(), 16) != 0:
            raise DockerBootstrapError("Docker bootstrap inherited capability boundary is invalid")
    if status.get("NoNewPrivs", "").strip() != "1" or status.get("Seccomp", "").strip() != "2":
        raise DockerBootstrapError("Docker bootstrap process hardening is incomplete")
    _validate_network_boundary()
    try:
        records = _parse_mountinfo(MOUNTINFO.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap mountinfo is unavailable") from exc
    root = _single_mount(records, Path("/"))
    if "ro" not in root.options:
        raise DockerBootstrapError("Docker bootstrap container root must be read-only")
    for target in EXPECTED_WRITABLE_BINDS:
        record = _single_mount(records, target)
        if record.root != str(target) or "rw" not in record.options:
            raise DockerBootstrapError("Docker bootstrap host bind identity is invalid")
    bundle_mount = _single_mount(records, BUNDLE)
    if "ro" not in bundle_mount.options:
        raise DockerBootstrapError("Docker bootstrap bundle must be read-only")


def _safe_root_directory(path: Path, mode: int) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap host directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise DockerBootstrapError("Docker bootstrap host directory is unsafe")


def _validate_host_roots() -> None:
    for path in EXPECTED_WRITABLE_BINDS:
        _safe_root_directory(path, 0o755)


def _bundle_digest() -> str:
    try:
        metadata = os.lstat(BUNDLE)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o022
            or metadata.st_nlink != 1
            or not 0 < metadata.st_size <= MAX_BUNDLE_BYTES
        ):
            raise DockerBootstrapError("sealed Docker bootstrap bundle is unsafe")
        digest = hashlib.sha256()
        with BUNDLE.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise DockerBootstrapError("sealed Docker bootstrap bundle is unavailable") from exc
    return digest.hexdigest()


def _clean_env() -> dict[str, str]:
    return {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _run(*argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        cwd="/",
        env=_clean_env(),
    )


def _checked(*argv: str) -> str:
    result = _run(*argv)
    if result.returncode != 0:
        raise DockerBootstrapError("sealed Docker bootstrap command failed safely")
    return result.stdout.strip()


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise DockerBootstrapError("sealed Docker bootstrap module is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise DockerBootstrapError("sealed Docker bootstrap module failed safely") from exc
    return module


def _validate_source(path: Path, identity: Identity, *, name: str) -> None:
    validator = _load_module(path / VALIDATOR_RELATIVE, name)
    try:
        source = validator.SealedSource(
            path,
            identity.source_sha,
            identity.source_tree_sha,
            identity.source_base_sha,
        )
        validator.validate_sealed_source(source)
    except Exception as exc:
        raise DockerBootstrapError("sealed Docker bootstrap source failed validation") from exc


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise DockerBootstrapError("atomic Docker bootstrap source publication is unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(source),
        AT_FDCWD,
        os.fsencode(destination),
        RENAME_NOREPLACE,
    )
    if result != 0:
        raise DockerBootstrapError("atomic Docker bootstrap source publication failed safely")


def _remove_checkout(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap source rollback failed safely") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise DockerBootstrapError("Docker bootstrap source rollback failed safely")
    try:
        shutil.rmtree(path)
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap source rollback failed safely") from exc


def _provision_source(identity: Identity) -> bool:
    if SOURCE.exists() or SOURCE.is_symlink():
        _safe_root_directory(SOURCE_PARENT, 0o755)
        _validate_source(SOURCE, identity, name="loom_sealed_source_existing")
        return False
    created_parent = False
    if SOURCE_PARENT.exists() or SOURCE_PARENT.is_symlink():
        _safe_root_directory(SOURCE_PARENT, 0o755)
    else:
        try:
            os.mkdir(SOURCE_PARENT, 0o755)
            created_parent = True
        except OSError as exc:
            raise DockerBootstrapError(
                "Docker bootstrap source parent creation failed safely"
            ) from exc
        _safe_root_directory(SOURCE_PARENT, 0o755)
    temporary = SOURCE_PARENT / f".source.new-{os.getpid()}"
    if temporary.exists() or temporary.is_symlink():
        raise DockerBootstrapError("Docker bootstrap temporary source already exists")
    published = False
    try:
        _checked("/usr/bin/git", "clone", "--no-hardlinks", str(BUNDLE), str(temporary))
        _checked("/usr/bin/git", "-C", str(temporary), "bundle", "verify", str(BUNDLE))
        _checked(
            "/usr/bin/git", "-C", str(temporary), "remote", "set-url", "origin", APPROVED_ORIGIN
        )
        _checked("/usr/bin/git", "-C", str(temporary), "checkout", "--detach", identity.source_sha)
        os.chmod(temporary, 0o700)
        _validate_source(temporary, identity, name="loom_sealed_source_temporary")
        _rename_noreplace(temporary, SOURCE)
        published = True
        descriptor = os.open(SOURCE_PARENT, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _validate_source(SOURCE, identity, name="loom_sealed_source_published")
    except Exception:
        _remove_checkout(SOURCE if published else temporary)
        if created_parent:
            try:
                os.rmdir(SOURCE_PARENT)
            except OSError as exc:
                raise DockerBootstrapError(
                    "Docker bootstrap source parent rollback failed safely"
                ) from exc
        raise
    return True


def _bootstrap(identity: Identity) -> dict[str, object]:
    result = _run(
        "/usr/bin/python3",
        str(SOURCE / AUTHORITY_RELATIVE),
        "bootstrap",
        "--source-sha",
        identity.source_sha,
        "--source-tree-sha",
        identity.source_tree_sha,
    )
    if result.returncode != 0 or result.stderr:
        raise DockerBootstrapError("fixed exporter authority bootstrap failed safely")
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DockerBootstrapError(
            "fixed exporter authority bootstrap returned invalid evidence"
        ) from exc
    if (
        not isinstance(report, dict)
        or report.get("status") != "ok"
        or report.get("source_sha") != identity.source_sha
        or report.get("source_tree_sha") != identity.source_tree_sha
        or report.get("source_base_sha") != identity.source_base_sha
    ):
        raise DockerBootstrapError("fixed exporter authority bootstrap evidence is invalid")
    return report


def execute(identity: Identity) -> dict[str, object]:
    _validate_runtime()
    _validate_host_roots()
    if _bundle_digest() != identity.bundle_sha256:
        raise DockerBootstrapError("sealed Docker bootstrap bundle digest does not match")
    created_source = _provision_source(identity)
    try:
        report = _bootstrap(identity)
    except Exception:
        if created_source:
            _remove_checkout(SOURCE)
            try:
                os.rmdir(SOURCE_PARENT)
            except OSError as exc:
                raise DockerBootstrapError(
                    "Docker bootstrap source rollback failed safely"
                ) from exc
        raise
    source_metadata = os.lstat(SOURCE)
    source_mode = stat.S_IMODE(source_metadata.st_mode)
    return {
        "action": "docker-bootstrap",
        "bootstrap": report,
        "bundle_sha256": identity.bundle_sha256,
        "source_base_sha": identity.source_base_sha,
        "source_gid": source_metadata.st_gid,
        "source_mode": f"{source_mode:04o}",
        "source_origin": _checked("/usr/bin/git", "-C", str(SOURCE), "remote", "get-url", "origin"),
        "source_sha": identity.source_sha,
        "source_status": _checked(
            "/usr/bin/git", "-C", str(SOURCE), "status", "--porcelain=v1", "--untracked-files=all"
        ),
        "source_tree_sha": identity.source_tree_sha,
        "source_uid": source_metadata.st_uid,
        "status": "ok",
        # A successful sealed authority bootstrap includes both source-asset
        # and installed-file visudo validation in the same rollback scope.
        "visudo": "ok",
    }


def main() -> int:
    try:
        report = execute(Identity.from_environ())
    except (DockerBootstrapError, OSError, ValueError):
        print("error: Docker exporter bootstrap failed safely", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
