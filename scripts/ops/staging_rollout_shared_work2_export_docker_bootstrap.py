#!/usr/bin/env python3
"""Run the one-time exporter bootstrap through a constrained Docker channel."""

from __future__ import annotations

import ctypes
import hashlib
import importlib.machinery
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
INSTALLED_WRAPPER: Final = Path(
    "/usr/local/libexec/loom-staging-rollout-shared-work2-export-authority"
)
INSTALLED_VALIDATOR: Final = Path("/usr/local/libexec/staging_rollout_sealed_source.py")
INSTALLED_POLICY: Final = Path("/etc/loom/staging-rollout-shared-work2-export-authority.json")
INSTALLED_SUDOERS: Final = Path("/etc/sudoers.d/loom-staging-rollout-shared-work2-export-authority")
INSTALLED_STATE_ROOT: Final = Path("/var/lib/loom-staging-exporter-authority")
INSTALLED_LOCK: Final = INSTALLED_STATE_ROOT / "authority.lock"
INSTALLED_JOURNAL: Final = INSTALLED_STATE_ROOT / "journal.jsonl"
EXPORT_FRAGMENT: Final = Path("/etc/exports.d/loom-staging-rollout-platform-dev.exports")
NFS_STATE_DIRECTORY: Final = Path("/var/lib/nfs")
NFS_ETAB: Final = NFS_STATE_DIRECTORY / "etab"
AUTHORITY_RELATIVE: Final = Path("scripts/ops/staging_rollout_shared_work2_export_authority.py")
EXPORT_ASSET_RELATIVE: Final = Path(
    "deploy/worker-pools/gb10/loom-staging-rollout-platform-dev.exports"
)
VALIDATOR_RELATIVE: Final = Path("scripts/ops/staging_rollout_sealed_source.py")
MOUNTINFO: Final = Path("/proc/self/mountinfo")
STATUS: Final = Path("/proc/self/status")
NETWORK_CLASS: Final = Path("/sys/class/net")
IPV4_ROUTES: Final = Path("/proc/net/route")
IPV6_ROUTES: Final = Path("/proc/net/ipv6_route")
EXPECTED_CAPABILITY_MASK: Final = (1 << 0) | (1 << 1) | (1 << 3)
EXPECTED_MACHINE: Final = "aarch64"
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


@dataclass(frozen=True, slots=True)
class InstalledIdentity:
    source_sha: str
    source_tree_sha: str
    source_base_sha: str


@dataclass(frozen=True, slots=True)
class ProvisionedSource:
    mode: str
    created_parent: bool = False
    previous_identity: InstalledIdentity | None = None
    previous_lock_descriptor: int | None = None
    moved: tuple[tuple[Path, Path], ...] = ()


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
    if os.uname().machine != EXPECTED_MACHINE:
        raise DockerBootstrapError("Docker bootstrap host architecture is invalid")
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


def _safe_root_regular(path: Path, *, mode: int) -> bytes:
    try:
        lexical = os.lstat(path)
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise DockerBootstrapError("installed exporter authority is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (lexical.st_dev, lexical.st_ino)
            or metadata.st_size > MAX_BUNDLE_BYTES
        ):
            raise DockerBootstrapError("installed exporter authority is unsafe")
        chunks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise DockerBootstrapError("installed exporter authority is unsafe")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_installed_authority() -> ModuleType:
    _safe_root_regular(INSTALLED_WRAPPER, mode=0o755)
    name = "loom_installed_shared_work2_export_authority"
    loader = importlib.machinery.SourceFileLoader(name, str(INSTALLED_WRAPPER))
    spec = importlib.util.spec_from_loader(name, loader)
    if spec is None:
        raise DockerBootstrapError("installed exporter authority is unavailable")
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        loader.exec_module(module)
    except Exception as exc:
        raise DockerBootstrapError("installed exporter authority failed validation") from exc
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    return module


def _validate_unused_export_state() -> bool:
    try:
        os.lstat(EXPORT_FRAGMENT)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise DockerBootstrapError("export fragment state is unavailable") from exc
    try:
        expected_fragment = _safe_root_regular(SOURCE / EXPORT_ASSET_RELATIVE, mode=0o644)
        asset_line = expected_fragment.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise DockerBootstrapError("installed export asset is invalid") from exc
    asset_match = re.fullmatch(
        r"/shared_work2 192[.]168[.]50[.]103/32\(([^()]*)\)",
        asset_line,
    )
    if asset_match is None:
        raise DockerBootstrapError("installed export asset is invalid")
    expected_options = frozenset(asset_match.group(1).split(","))
    if _safe_root_regular(EXPORT_FRAGMENT, mode=0o644) != expected_fragment:
        raise DockerBootstrapError("partial exporter authority cannot be upgraded")
    _safe_root_directory(NFS_STATE_DIRECTORY, 0o755)
    try:
        etab = _safe_root_regular(NFS_ETAB, mode=0o644).decode("ascii")
    except UnicodeDecodeError as exc:
        raise DockerBootstrapError("NFS active export table is invalid") from exc
    matches: list[frozenset[str]] = []
    for line in etab.splitlines():
        match = re.fullmatch(
            r"/shared_work2\s+192[.]168[.]50[.]103/32\(([^()]*)\)",
            line,
        )
        if match is not None:
            matches.append(frozenset(match.group(1).split(",")))
    if len(matches) != 1 or matches[0] != expected_options:
        raise DockerBootstrapError("partial exporter authority cannot be upgraded")
    return True


def _lock_unused_installed_identity() -> tuple[InstalledIdentity, int]:
    authority = _load_installed_authority()
    try:
        descriptor = authority._open_lock(exclusive=True)
    except Exception as exc:
        raise DockerBootstrapError("installed exporter authority lock is unavailable") from exc
    try:
        policy = authority._read_policy()
        authority._validate_runtime_assets(policy)
        authority._validate_source(policy)
        authority._validate_journal(policy)
        journal = authority._regular_root_file(INSTALLED_JOURNAL, mode=0o600)
    except Exception as exc:
        os.close(descriptor)
        raise DockerBootstrapError("installed exporter authority failed validation") from exc
    if journal:
        os.close(descriptor)
        raise DockerBootstrapError("used exporter authority cannot be upgraded")
    try:
        _validate_unused_export_state()
    except DockerBootstrapError:
        os.close(descriptor)
        raise
    identity = InstalledIdentity(
        source_sha=policy.source_sha,
        source_tree_sha=policy.source_tree_sha,
        source_base_sha=policy.source_base_sha,
    )
    if (
        identity.source_base_sha != APPROVED_BASE_SHA
        or SHA_RE.fullmatch(identity.source_sha) is None
        or SHA_RE.fullmatch(identity.source_tree_sha) is None
    ):
        os.close(descriptor)
        raise DockerBootstrapError("installed exporter authority identity is invalid")
    return identity, descriptor


def _read_unused_installed_identity() -> InstalledIdentity:
    identity, descriptor = _lock_unused_installed_identity()
    os.close(descriptor)
    return identity


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


def _fsync_parent(path: Path) -> None:
    try:
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise DockerBootstrapError("Docker bootstrap publication sync failed safely") from exc


def _prepare_source(identity: Identity, *, created_parent: bool) -> Path:
    temporary = SOURCE_PARENT / f".source.new-{os.getpid()}"
    if temporary.exists() or temporary.is_symlink():
        raise DockerBootstrapError("Docker bootstrap temporary source already exists")
    try:
        _checked("/usr/bin/git", "clone", "--no-hardlinks", str(BUNDLE), str(temporary))
        _checked("/usr/bin/git", "-C", str(temporary), "bundle", "verify", str(BUNDLE))
        _checked(
            "/usr/bin/git", "-C", str(temporary), "remote", "set-url", "origin", APPROVED_ORIGIN
        )
        _checked("/usr/bin/git", "-C", str(temporary), "checkout", "--detach", identity.source_sha)
        os.chmod(temporary, 0o700)
        _validate_source(temporary, identity, name="loom_sealed_source_temporary")
        return temporary
    except Exception:
        _remove_checkout(temporary)
        if created_parent:
            try:
                os.rmdir(SOURCE_PARENT)
            except OSError as exc:
                raise DockerBootstrapError(
                    "Docker bootstrap source parent rollback failed safely"
                ) from exc
        raise


def _installed_assets() -> tuple[tuple[Path, int], ...]:
    return (
        (INSTALLED_SUDOERS, 0o440),
        (INSTALLED_WRAPPER, 0o755),
        (INSTALLED_VALIDATOR, 0o644),
        (INSTALLED_POLICY, 0o600),
        (INSTALLED_LOCK, 0o600),
        (INSTALLED_JOURNAL, 0o600),
    )


def _backup_path(path: Path, source_sha: str) -> Path:
    return path.parent / f".{path.name}.previous-{source_sha}"


def _reject_stale_backups() -> None:
    originals = (*tuple(path for path, _mode in _installed_assets()), SOURCE)
    scanned: set[tuple[Path, str]] = set()
    for original in originals:
        key = (original.parent, f".{original.name}.previous-")
        if key in scanned:
            continue
        scanned.add(key)
        try:
            entries = tuple(os.scandir(original.parent))
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise DockerBootstrapError("exporter authority upgrade backup is unavailable") from exc
        if any(entry.name.startswith(key[1]) for entry in entries):
            raise DockerBootstrapError("exporter authority upgrade backup already exists")


def _restore_moved(moved: tuple[tuple[Path, Path], ...]) -> None:
    failures: list[str] = []
    for original, backup in reversed(moved):
        try:
            os.lstat(original)
        except FileNotFoundError:
            pass
        except OSError:
            failures.append(str(original))
            continue
        else:
            failures.append(str(original))
            continue
        try:
            _rename_noreplace(backup, original)
            _fsync_parent(original)
        except (DockerBootstrapError, OSError):
            failures.append(str(original))
    if failures:
        raise DockerBootstrapError("exporter authority upgrade rollback failed safely")


def _move_unused_authority(previous: InstalledIdentity) -> tuple[tuple[Path, Path], ...]:
    originals = (*tuple(path for path, _mode in _installed_assets()), SOURCE)
    planned = tuple((path, _backup_path(path, previous.source_sha)) for path in originals)
    for _original, backup in planned:
        try:
            os.lstat(backup)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise DockerBootstrapError("exporter authority upgrade backup is unavailable") from exc
        raise DockerBootstrapError("exporter authority upgrade backup already exists")
    moved: list[tuple[Path, Path]] = []
    try:
        for original, backup in planned:
            _rename_noreplace(original, backup)
            moved.append((original, backup))
            _fsync_parent(original)
    except Exception:
        _restore_moved(tuple(moved))
        raise
    return tuple(moved)


def _is_descendant(checkout: Path, previous_sha: str, current_sha: str) -> None:
    result = _run(
        "/usr/bin/git",
        "-C",
        str(checkout),
        "merge-base",
        "--is-ancestor",
        previous_sha,
        current_sha,
    )
    if result.returncode != 0 or result.stdout or result.stderr:
        raise DockerBootstrapError("exporter authority upgrade is not a linear descendant")


def _provision_source(identity: Identity) -> ProvisionedSource:
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
    _reject_stale_backups()
    if SOURCE.exists() or SOURCE.is_symlink():
        try:
            _validate_source(SOURCE, identity, name="loom_sealed_source_existing")
        except DockerBootstrapError as exact_error:
            previous, previous_lock = _lock_unused_installed_identity()
            if previous.source_sha == identity.source_sha:
                os.close(previous_lock)
                raise DockerBootstrapError(
                    "installed exporter authority identity drifted"
                ) from exact_error
            temporary: Path | None = None
            try:
                temporary = _prepare_source(identity, created_parent=False)
                if previous.source_base_sha != identity.source_base_sha:
                    raise DockerBootstrapError("exporter authority upgrade base drifted")
                _is_descendant(temporary, previous.source_sha, identity.source_sha)
                moved = _move_unused_authority(previous)
                try:
                    _rename_noreplace(temporary, SOURCE)
                    _fsync_parent(SOURCE)
                    _validate_source(SOURCE, identity, name="loom_sealed_source_upgraded")
                except Exception:
                    _remove_checkout(SOURCE if SOURCE.exists() else temporary)
                    _restore_moved(moved)
                    raise
            except Exception:
                if temporary is not None:
                    _remove_checkout(temporary)
                os.close(previous_lock)
                raise
            return ProvisionedSource(
                mode="upgraded",
                previous_identity=previous,
                previous_lock_descriptor=previous_lock,
                moved=moved,
            )
        return ProvisionedSource(mode="existing")
    temporary = _prepare_source(identity, created_parent=created_parent)
    published = False
    try:
        _rename_noreplace(temporary, SOURCE)
        published = True
        _fsync_parent(SOURCE)
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
    return ProvisionedSource(mode="created", created_parent=created_parent)


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


def _rollback_provision(provision: ProvisionedSource, identity: Identity) -> None:
    if provision.mode == "created":
        _validate_source(SOURCE, identity, name="loom_sealed_source_created_rollback")
        _remove_checkout(SOURCE)
        if provision.created_parent:
            try:
                os.rmdir(SOURCE_PARENT)
            except OSError as exc:
                raise DockerBootstrapError(
                    "Docker bootstrap source rollback failed safely"
                ) from exc
        return
    if provision.mode != "upgraded" or provision.previous_identity is None:
        return
    if provision.previous_lock_descriptor is None:
        raise DockerBootstrapError("exporter authority upgrade lock is missing")
    try:
        _validate_source(SOURCE, identity, name="loom_sealed_source_upgrade_rollback")
        _remove_checkout(SOURCE)
        for path, _mode in _installed_assets():
            try:
                os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise DockerBootstrapError(
                    "exporter authority upgrade rollback state is unavailable"
                ) from exc
            raise DockerBootstrapError("exporter authority upgrade rollback left new assets")
        _restore_moved(provision.moved)
    finally:
        os.close(provision.previous_lock_descriptor)
    restored = _read_unused_installed_identity()
    if restored != provision.previous_identity:
        raise DockerBootstrapError("exporter authority upgrade rollback identity drifted")


def _finalize_upgrade(provision: ProvisionedSource, identity: Identity) -> None:
    if provision.mode != "upgraded" or provision.previous_identity is None:
        return
    if provision.previous_lock_descriptor is None:
        raise DockerBootstrapError("exporter authority upgrade lock is missing")
    os.close(provision.previous_lock_descriptor)
    installed = _read_unused_installed_identity()
    if installed != InstalledIdentity(
        identity.source_sha,
        identity.source_tree_sha,
        identity.source_base_sha,
    ):
        raise DockerBootstrapError("upgraded exporter authority identity drifted")
    previous = provision.previous_identity
    modes = dict(_installed_assets())
    source_backup: Path | None = None
    file_backups: list[Path] = []
    for original, backup in provision.moved:
        if original == SOURCE:
            source_backup = backup
            _validate_source(
                backup,
                Identity(
                    previous.source_sha,
                    previous.source_tree_sha,
                    previous.source_base_sha,
                    identity.bundle_sha256,
                ),
                name="loom_sealed_source_previous_cleanup",
            )
        else:
            _safe_root_regular(backup, mode=modes[original])
            file_backups.append(backup)
    if source_backup is None:
        raise DockerBootstrapError("exporter authority upgrade backup is incomplete")
    for backup in file_backups:
        try:
            os.unlink(backup)
            _fsync_parent(backup)
        except OSError as exc:
            raise DockerBootstrapError("exporter authority upgrade cleanup failed safely") from exc
    _remove_checkout(source_backup)
    _fsync_parent(source_backup)


def execute(identity: Identity) -> dict[str, object]:
    _validate_runtime()
    _validate_host_roots()
    if _bundle_digest() != identity.bundle_sha256:
        raise DockerBootstrapError("sealed Docker bootstrap bundle digest does not match")
    provision = _provision_source(identity)
    try:
        report = _bootstrap(identity)
    except Exception:
        _rollback_provision(provision, identity)
        raise
    _finalize_upgrade(provision, identity)
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
        "source_transition": provision.mode,
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
