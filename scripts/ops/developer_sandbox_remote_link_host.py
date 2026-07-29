#!/usr/bin/env python3
"""Root-managed lifecycle for developer sandbox candidate-bound mTLS links.

Mutation commands are plan-only unless ``--execute`` is present. Credentials
are generated and transferred only through root-private local paths; no command
prints a key, token, certificate body, or secret-bearing environment.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import http.client
import json
import os
import re
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_ROOT = REPO_ROOT / "deploy/developer-sandboxes/remote-links"
SOURCE_RELAY = REPO_ROOT / "scripts/ops/developer_sandbox_remote_link.py"
SOURCE_UNIT = REPO_ROOT / "deploy/developer-sandboxes/loom-developer-sandbox-link@.service"
INSTALL_ROOT = Path("/etc/loom/developer-sandbox-links")
SERVER_ROOT = INSTALL_ROOT / "server"
CLIENT_ROOT = INSTALL_ROOT / "clients"
ISSUANCE_ROOT = Path("/var/lib/loom/developer-sandbox-links/issuance")
ATTESTATION_ROOT = Path("/var/lib/loom-developer-sandbox-links/attestations")
COMBINED_RECEIPT_ROOT = Path("/var/lib/loom-shared-capacity/runtime-attestations")
TRANSACTION_ROOT = Path("/var/lib/loom-developer-sandbox-links/transactions")
TRANSACTION_LOCK_ROOT = Path("/run/loom-developer-sandbox-links")
INSTALLED_RELAY = Path("/usr/local/libexec/loom-developer-sandbox-remote-link")
INSTALLED_HOST = Path(
    "/usr/local/libexec/loom-developer-sandbox-remote-link-host",
)
UNIT_PATH = Path(
    "/etc/systemd/system/loom-developer-sandbox-link@.service",
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SANDBOXES = ("qianyi", "hongjian", "devansh")
PROFILE_VALUES = {
    "qianyi": (
        "192.168.50.14",
        ((26080, 20080), (26100, 20100), (26900, 20900)),
    ),
    "hongjian": (
        "192.168.50.14",
        ((27080, 21080), (27100, 21100), (27900, 21900)),
    ),
    "devansh": (
        "192.168.50.14",
        ((28080, 22080), (28100, 22100), (28900, 22900)),
    ),
}
OLDLAB_NODES = (
    "oldlab-1",
    "oldlab-2",
    "oldlab-3",
    "oldlab-4",
    "oldlab-5",
)
GB10_INFRASTRUCTURE_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
INFRASTRUCTURE_LINK_NODES = OLDLAB_NODES + GB10_INFRASTRUCTURE_NODES
CLIENT_FILES = ("ca.pem", "client.pem", "client-key.pem")
OPENSSL_PATH = shutil.which("openssl") or "openssl"
SERVICE_VALUES = {
    "control-plane": ("/healthz", False),
    "gateway": ("/healthz", False),
    "minio": ("/minio/health/live", True),
}
ATTESTATION_TTL_SECONDS = 900


class LinkHostError(RuntimeError):
    """A host operation could not preserve the remote-link safety contract."""


@dataclass(frozen=True, slots=True)
class ServiceProfile:
    name: str
    server_port: int
    target_port: int
    health_path: str
    allow_empty_health: bool


@dataclass(frozen=True, slots=True)
class Profile:
    sandbox: str
    server_address: str
    services: tuple[ServiceProfile, ...]

    def client_uri(self, candidate_sha: str) -> str:
        return f"spiffe://loom/developer-sandbox/{self.sandbox}/candidate/{candidate_sha}/worker"

    def service(self, name: str) -> ServiceProfile:
        for service in self.services:
            if service.name == name:
                return service
        raise LinkHostError("sandbox link service is absent")


def load_profile(sandbox: str) -> Profile:
    if sandbox not in SANDBOXES:
        raise LinkHostError("sandbox is not in the closed inventory")
    expected_address, expected_ports = PROFILE_VALUES[sandbox]
    expected_services = tuple(
        ServiceProfile(
            name=name,
            server_port=ports[0],
            target_port=ports[1],
            health_path=SERVICE_VALUES[name][0],
            allow_empty_health=SERVICE_VALUES[name][1],
        )
        for name, ports in zip(SERVICE_VALUES, expected_ports, strict=True)
    )
    path = PROFILE_ROOT / f"{sandbox}.toml"
    if not path.exists():
        return Profile(
            sandbox=sandbox,
            server_address=expected_address,
            services=expected_services,
        )
    try:
        with path.open("rb") as handle:
            raw: dict[str, Any] = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise LinkHostError("sandbox link profile is unavailable") from exc
    if (
        set(raw)
        != {
            "schema_version",
            "sandbox",
            "server_address",
            "services",
        }
        or raw.get("schema_version") != 1
    ):
        raise LinkHostError("sandbox link profile does not match schema version 1")
    if raw.get("sandbox") != sandbox or raw.get("server_address") != expected_address:
        raise LinkHostError("sandbox link identity or private address drifted")
    raw_services = raw.get("services")
    if not isinstance(raw_services, dict) or set(raw_services) != set(SERVICE_VALUES):
        raise LinkHostError("sandbox link services drifted")
    services: list[ServiceProfile] = []
    for expected in expected_services:
        raw_service = raw_services.get(expected.name)
        if not isinstance(raw_service, dict) or set(raw_service) != {
            "server_port",
            "target_port",
            "health_path",
            "allow_empty_health",
        }:
            raise LinkHostError("sandbox link service schema drifted")
        server_port = raw_service.get("server_port")
        target_port = raw_service.get("target_port")
        health_path = raw_service.get("health_path")
        allow_empty_health = raw_service.get("allow_empty_health")
        if (
            isinstance(server_port, bool)
            or not isinstance(server_port, int)
            or isinstance(target_port, bool)
            or not isinstance(target_port, int)
            or not isinstance(health_path, str)
            or type(allow_empty_health) is not bool
        ):
            raise LinkHostError("sandbox link service values drifted")
        actual = ServiceProfile(
            name=expected.name,
            server_port=server_port,
            target_port=target_port,
            health_path=health_path,
            allow_empty_health=allow_empty_health,
        )
        if actual != expected:
            raise LinkHostError("sandbox link service contract drifted")
        services.append(actual)
    return Profile(
        sandbox=sandbox,
        server_address=expected_address,
        services=tuple(services),
    )


def _candidate(value: str) -> str:
    if SHA_RE.fullmatch(value) is None:
        raise LinkHostError("candidate SHA must be 40 lowercase hex characters")
    return value


def _run(
    argv: Sequence[str],
    *,
    input_text: str | None = None,
    expected: frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(argv),
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
        env={"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"},
    )
    if completed.returncode not in expected:
        purpose = Path(argv[0]).name if argv else "command"
        raise LinkHostError(
            f"{purpose} failed safely with exit code {completed.returncode}",
        )
    return completed


def _ensure_root_dir(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chown(path, 0, 0)
    os.chmod(path, 0o700)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
    ):
        raise LinkHostError("root-private directory did not converge")


def _ensure_root_owned_parent(path: Path) -> None:
    path.mkdir(parents=True, mode=0o755, exist_ok=True)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or (metadata.st_mode & 0o022)
    ):
        raise LinkHostError("installation parent must be root-owned and non-writable")


def _atomic_copy(source: Path, target: Path, *, mode: int) -> None:
    _ensure_root_owned_parent(target.parent)
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise LinkHostError("credential source is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_mode & 0o022)
    ):
        raise LinkHostError("credential source is unsafe")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as input_file:
            shutil.copyfileobj(input_file, output)
            output.flush()
            os.fsync(output.fileno())
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write(path: Path, content: str, *, mode: int = 0o600) -> None:
    _ensure_root_dir(path.parent)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _install_immutable_copy(source: Path, target: Path, *, mode: int) -> None:
    if target.exists():
        metadata = target.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or source.read_bytes() != target.read_bytes()
        ):
            raise LinkHostError("installed candidate material is immutable")
        return
    _atomic_copy(source, target, mode=mode)


def _install_immutable_content(path: Path, content: str, *, mode: int = 0o600) -> None:
    if path.exists():
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or path.read_text(encoding="utf-8") != content
        ):
            raise LinkHostError("installed candidate metadata is immutable")
        return
    _atomic_write(path, content, mode=mode)


def _atomic_symlink(target_name: str, link: Path) -> None:
    _ensure_root_dir(link.parent)
    temporary = link.parent / f".{link.name}.{os.getpid()}"
    temporary.unlink(missing_ok=True)
    os.symlink(target_name, temporary)
    os.replace(temporary, link)
    _fsync_directory(link.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fingerprint(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _openssl(*argv: str) -> None:
    _run((OPENSSL_PATH, *argv))


@contextmanager
def _link_transaction(profile: Profile) -> Iterator[None]:
    _ensure_root_dir(TRANSACTION_LOCK_ROOT)
    lock_path = TRANSACTION_LOCK_ROOT / f"{profile.sandbox}.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0:
            raise LinkHostError("remote-link transaction lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _journal_path(profile: Profile) -> Path:
    return TRANSACTION_ROOT / f"{profile.sandbox}.json"


def _read_journal(profile: Profile) -> dict[str, Any] | None:
    path = _journal_path(profile)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LinkHostError("remote-link activation journal is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise LinkHostError("remote-link activation journal metadata is invalid")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LinkHostError("remote-link activation journal is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "sandbox",
            "candidate_sha",
            "previous_sha",
            "phase",
        }
        or payload["schema_version"] != 1
        or payload["sandbox"] != profile.sandbox
        or not isinstance(payload["candidate_sha"], str)
        or SHA_RE.fullmatch(payload["candidate_sha"]) is None
        or (
            payload["previous_sha"] is not None
            and (
                not isinstance(payload["previous_sha"], str)
                or SHA_RE.fullmatch(payload["previous_sha"]) is None
            )
        )
        or payload["phase"] not in {"switching", "switched", "restarting"}
    ):
        raise LinkHostError("remote-link activation journal binding is invalid")
    return payload


def _write_journal(
    profile: Profile,
    candidate_sha: str,
    previous_sha: str | None,
    phase: str,
) -> None:
    _ensure_root_dir(TRANSACTION_ROOT)
    _atomic_write(
        _journal_path(profile),
        json.dumps(
            {
                "schema_version": 1,
                "sandbox": profile.sandbox,
                "candidate_sha": candidate_sha,
                "previous_sha": previous_sha,
                "phase": phase,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        mode=0o600,
    )


def _remove_journal(profile: Profile) -> None:
    path = _journal_path(profile)
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


def _current_server_sha(profile: Profile) -> str | None:
    current = SERVER_ROOT / profile.sandbox / "current"
    try:
        metadata = current.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LinkHostError("sandbox relay current pointer is unavailable") from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise LinkHostError("sandbox relay current pointer is invalid")
    target = os.readlink(current)
    prefix = "candidates/"
    candidate_sha = target.removeprefix(prefix)
    if target != prefix + candidate_sha or SHA_RE.fullmatch(candidate_sha) is None:
        raise LinkHostError("sandbox relay current pointer is invalid")
    return candidate_sha


def _unlink_and_fsync(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode):
        raise LinkHostError("activation proof path is a directory")
    path.unlink()
    _fsync_directory(path.parent)


def _invalidate_activation_proofs(profile: Profile, candidate_sha: str) -> None:
    _unlink_and_fsync(ATTESTATION_ROOT / profile.sandbox / candidate_sha / "fleet.json")
    _unlink_and_fsync(
        COMBINED_RECEIPT_ROOT / profile.sandbox / candidate_sha / "combined.json",
    )


def _restore_activation(profile: Profile, previous_sha: str | None) -> None:
    current = SERVER_ROOT / profile.sandbox / "current"
    unit = f"loom-developer-sandbox-link@{profile.sandbox}.service"
    if previous_sha is None:
        if current.is_symlink():
            current.unlink()
            _fsync_directory(current.parent)
        _run(("systemctl", "disable", "--now", unit), expected=frozenset({0, 1, 5}))
        return
    previous_root = SERVER_ROOT / profile.sandbox / "candidates" / previous_sha
    _validate_installed_server_config(profile, previous_sha, previous_root)
    _atomic_symlink(f"candidates/{previous_sha}", current)
    _run(("systemctl", "restart", unit))
    check_server(profile, previous_sha)


def _recover_activation(profile: Profile) -> None:
    journal = _read_journal(profile)
    if journal is None:
        return
    _restore_activation(profile, journal["previous_sha"])
    _remove_journal(profile)


def prepare_rotation(profile: Profile, candidate_sha: str) -> Path:
    with _link_transaction(profile):
        _recover_activation(profile)
        _invalidate_activation_proofs(profile, candidate_sha)
        return _prepare_rotation_locked(profile, candidate_sha)


def _prepare_rotation_locked(profile: Profile, candidate_sha: str) -> Path:
    final_destination = ISSUANCE_ROOT / profile.sandbox / candidate_sha
    if final_destination.exists():
        _validate_existing_issuance(profile, candidate_sha, final_destination)
        return final_destination
    _ensure_root_dir(final_destination.parent)
    prefix = f".incoming-{candidate_sha}-"
    for orphan in final_destination.parent.glob(f"{prefix}*"):
        metadata = orphan.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or not orphan.name.startswith(prefix)
        ):
            raise LinkHostError("candidate issuance orphan metadata is invalid")
        shutil.rmtree(orphan)
        _fsync_directory(orphan.parent)
    destination = final_destination.parent / f"{prefix}{uuid.uuid4().hex}"
    _ensure_root_dir(destination)
    ca_key = destination / "ca-key.pem"
    ca_cert = destination / "ca.pem"
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "ed25519",
        "-nodes",
        "-days",
        "120",
        "-subj",
        f"/CN=loom-{profile.sandbox}-{candidate_sha[:12]}-ca",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_cert),
    )
    os.chmod(ca_key, 0o600)
    os.chmod(ca_cert, 0o644)

    server_root = destination / "server"
    _ensure_root_dir(server_root)
    server_key = server_root / "server-key.pem"
    server_csr = server_root / "server.csr"
    server_cert = server_root / "server.pem"
    server_ext = server_root / "server.ext"
    _atomic_write(
        server_ext,
        (
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth\n"
            f"subjectAltName=IP:{profile.server_address}\n"
        ),
    )
    _openssl(
        "req",
        "-newkey",
        "ed25519",
        "-nodes",
        "-subj",
        f"/CN={profile.server_address}",
        "-keyout",
        str(server_key),
        "-out",
        str(server_csr),
    )
    _openssl(
        "x509",
        "-req",
        "-in",
        str(server_csr),
        "-CA",
        str(ca_cert),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-days",
        "120",
        "-extfile",
        str(server_ext),
        "-out",
        str(server_cert),
    )
    os.chmod(server_key, 0o600)
    os.chmod(server_cert, 0o644)
    _atomic_copy(ca_cert, server_root / "ca.pem", mode=0o644)
    server_csr.unlink()
    server_ext.unlink()

    for node in INFRASTRUCTURE_LINK_NODES:
        node_root = destination / "clients" / node
        _ensure_root_dir(node_root)
        client_key = node_root / "client-key.pem"
        client_csr = node_root / "client.csr"
        client_cert = node_root / "client.pem"
        client_ext = node_root / "client.ext"
        _atomic_write(
            client_ext,
            (
                "basicConstraints=critical,CA:FALSE\n"
                "keyUsage=critical,digitalSignature\n"
                "extendedKeyUsage=clientAuth\n"
                f"subjectAltName=URI:{profile.client_uri(candidate_sha)},DNS:{node}\n"
            ),
        )
        _openssl(
            "req",
            "-newkey",
            "ed25519",
            "-nodes",
            "-subj",
            f"/CN=loom-{profile.sandbox}-{node}-{candidate_sha[:12]}",
            "-keyout",
            str(client_key),
            "-out",
            str(client_csr),
        )
        _openssl(
            "x509",
            "-req",
            "-in",
            str(client_csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-days",
            "120",
            "-extfile",
            str(client_ext),
            "-out",
            str(client_cert),
        )
        os.chmod(client_key, 0o600)
        os.chmod(client_cert, 0o644)
        _atomic_copy(ca_cert, node_root / "ca.pem", mode=0o644)
        client_csr.unlink()
        client_ext.unlink()
    _validate_existing_issuance(profile, candidate_sha, destination)
    os.rename(destination, final_destination)
    _fsync_directory(final_destination.parent)
    return final_destination


def _validate_issuance_file(path: Path, *, mode: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise LinkHostError("candidate issuance file is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise LinkHostError("candidate issuance file metadata is invalid")


def _validate_issuance_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise LinkHostError("candidate issuance directory is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise LinkHostError("candidate issuance directory metadata is invalid")


def _validate_existing_issuance(
    profile: Profile,
    candidate_sha: str,
    destination: Path,
) -> None:
    _validate_issuance_directory(destination)
    ca = destination / "ca.pem"
    _validate_issuance_file(destination / "ca-key.pem", mode=0o600)
    _validate_issuance_file(ca, mode=0o644)
    server = destination / "server"
    _validate_issuance_directory(server)
    _validate_issuance_file(server / "server-key.pem", mode=0o600)
    _validate_issuance_file(server / "server.pem", mode=0o644)
    _validate_issuance_file(server / "ca.pem", mode=0o644)
    _run((OPENSSL_PATH, "verify", "-CAfile", str(ca), str(server / "server.pem")))
    if _fingerprint(ca) != _fingerprint(server / "ca.pem"):
        raise LinkHostError("candidate issuance server CA binding is invalid")
    server_text = _certificate_text(server / "server.pem")
    if set(re.findall(r"IP Address:([^,\s]+)", server_text)) != {
        profile.server_address,
    }:
        raise LinkHostError("candidate issuance server identity is invalid")
    clients = destination / "clients"
    _validate_issuance_directory(clients)
    try:
        client_nodes = {path.name for path in clients.iterdir()}
    except OSError as exc:
        raise LinkHostError("candidate issuance clients are unavailable") from exc
    if client_nodes != set(INFRASTRUCTURE_LINK_NODES):
        raise LinkHostError("candidate issuance client inventory is incomplete")
    for node in INFRASTRUCTURE_LINK_NODES:
        root = clients / node
        _validate_issuance_directory(root)
        for name, mode in (
            ("client-key.pem", 0o600),
            ("client.pem", 0o644),
            ("ca.pem", 0o644),
        ):
            _validate_issuance_file(root / name, mode=mode)
        _run((OPENSSL_PATH, "verify", "-CAfile", str(ca), str(root / "client.pem")))
        if _fingerprint(ca) != _fingerprint(root / "ca.pem"):
            raise LinkHostError("candidate issuance client CA binding is invalid")
        text = _certificate_text(root / "client.pem")
        if set(re.findall(r"URI:([^,\s]+)", text)) != {profile.client_uri(candidate_sha)} or set(
            re.findall(r"DNS:([^,\s]+)", text)
        ) != {node}:
            raise LinkHostError("candidate issuance client identity is invalid")


def _install_programs() -> None:
    _atomic_copy(SOURCE_RELAY, INSTALLED_RELAY, mode=0o755)
    _atomic_copy(Path(__file__), INSTALLED_HOST, mode=0o755)
    _atomic_copy(SOURCE_UNIT, UNIT_PATH, mode=0o644)
    _run(("systemctl", "daemon-reload"))


def install_server(profile: Profile, candidate_sha: str, source: Path) -> Path:
    with _link_transaction(profile):
        _recover_activation(profile)
        _invalidate_activation_proofs(profile, candidate_sha)
        return _install_server_locked(profile, candidate_sha, source)


def _install_server_locked(profile: Profile, candidate_sha: str, source: Path) -> Path:
    _run(
        (
            OPENSSL_PATH,
            "verify",
            "-CAfile",
            str(source / "ca.pem"),
            str(source / "server.pem"),
        ),
    )
    server_text = _certificate_text(source / "server.pem")
    ip_sans = set(re.findall(r"IP Address:([^,\s]+)", server_text))
    if ip_sans != {profile.server_address}:
        raise LinkHostError("server certificate IP SAN is not exact")
    destination = SERVER_ROOT / profile.sandbox / "candidates" / candidate_sha
    _ensure_root_dir(destination)
    for name in ("ca.pem", "server.pem", "server-key.pem"):
        _install_immutable_copy(
            source / name,
            destination / name,
            mode=0o600 if name.endswith("-key.pem") else 0o644,
        )
    service_config = "".join(
        (
            f"\n[services.{service.name}]\n"
            f"bind_port = {service.server_port}\n"
            f"target_port = {service.target_port}\n"
            f'health_path = "{service.health_path}"\n'
            f"allow_empty_health = {str(service.allow_empty_health).lower()}\n"
        )
        for service in profile.services
    )
    config = (
        "schema_version = 1\n"
        f'sandbox = "{profile.sandbox}"\n'
        f'candidate_sha = "{candidate_sha}"\n'
        f'bind_address = "{profile.server_address}"\n'
        'target_host = "127.0.0.1"\n'
        f'ca_file = "{destination / "ca.pem"}"\n'
        f'cert_file = "{destination / "server.pem"}"\n'
        f'key_file = "{destination / "server-key.pem"}"\n'
        f"{service_config}"
    )
    _install_immutable_content(destination / "config.toml", config)
    _install_programs()
    return destination


def _certificate_text(path: Path) -> str:
    return _run((OPENSSL_PATH, "x509", "-in", str(path), "-noout", "-text")).stdout


def _validate_token_file(path: Path) -> None:
    try:
        token = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise LinkHostError("worker token file is unreadable") from exc
    token_value = token[:-1] if token.endswith("\n") else token
    if (
        not token_value.startswith("loom_w_")
        or token not in {token_value, token_value + "\n"}
        or token_value != token_value.strip()
        or "\n" in token_value
        or "\r" in token_value
        or len(token_value) > 4096
    ):
        raise LinkHostError("worker token file is malformed")


def _validate_opaque_secret_file(path: Path, *, label: str) -> None:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise LinkHostError(f"{label} file is unreadable") from exc
    value = raw[:-1] if raw.endswith("\n") else raw
    if (
        not value
        or raw not in {value, value + "\n"}
        or value != value.strip()
        or "\n" in value
        or "\r" in value
        or len(value) > 4096
    ):
        raise LinkHostError(f"{label} file is malformed")


def install_client(
    profile: Profile,
    candidate_sha: str,
    node: str,
    source: Path,
    token_source: Path,
    minio_access_key_source: Path,
    minio_secret_key_source: Path,
) -> Path:
    with _link_transaction(profile):
        _recover_activation(profile)
        _invalidate_activation_proofs(profile, candidate_sha)
        return _install_client_locked(
            profile,
            candidate_sha,
            node,
            source,
            token_source,
            minio_access_key_source,
            minio_secret_key_source,
        )


def _install_client_locked(
    profile: Profile,
    candidate_sha: str,
    node: str,
    source: Path,
    token_source: Path,
    minio_access_key_source: Path,
    minio_secret_key_source: Path,
) -> Path:
    if node not in INFRASTRUCTURE_LINK_NODES:
        raise LinkHostError("client node is not in the closed inventory")
    _run(
        (
            OPENSSL_PATH,
            "verify",
            "-CAfile",
            str(source / "ca.pem"),
            str(source / "client.pem"),
        ),
    )
    cert_text = _certificate_text(source / "client.pem")
    uri_sans = set(re.findall(r"URI:([^,\s]+)", cert_text))
    dns_sans = set(re.findall(r"DNS:([^,\s]+)", cert_text))
    if uri_sans != {profile.client_uri(candidate_sha)} or dns_sans != {node}:
        raise LinkHostError("client certificate identity does not match node/candidate")
    destination = CLIENT_ROOT / profile.sandbox / candidate_sha
    _ensure_root_dir(destination)
    for name in CLIENT_FILES:
        _install_immutable_copy(
            source / name,
            destination / name,
            mode=0o600 if name.endswith("-key.pem") else 0o644,
        )
    _install_immutable_copy(token_source, destination / "worker-token", mode=0o600)
    _install_immutable_copy(
        minio_access_key_source,
        destination / "minio-access-key",
        mode=0o600,
    )
    _install_immutable_copy(
        minio_secret_key_source,
        destination / "minio-secret-key",
        mode=0o600,
    )
    _validate_token_file(destination / "worker-token")
    _validate_opaque_secret_file(
        destination / "minio-access-key",
        label="MinIO access key",
    )
    _validate_opaque_secret_file(
        destination / "minio-secret-key",
        label="MinIO secret key",
    )
    metadata = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": candidate_sha,
        "node": node,
        "server_address": profile.server_address,
        "service_ports": {service.name: service.server_port for service in profile.services},
        "ca_fingerprint": _fingerprint(destination / "ca.pem"),
        "client_cert_fingerprint": _fingerprint(destination / "client.pem"),
    }
    _install_immutable_content(
        destination / "metadata.json",
        json.dumps(metadata, sort_keys=True, indent=2) + "\n",
    )
    _atomic_copy(Path(__file__), INSTALLED_HOST, mode=0o755)
    return destination


def activate_server(profile: Profile, candidate_sha: str) -> None:
    with _link_transaction(profile):
        _recover_activation(profile)
        _invalidate_activation_proofs(profile, candidate_sha)
        _activate_server_locked(profile, candidate_sha)


def _activate_server_locked(profile: Profile, candidate_sha: str) -> None:
    candidate_root = SERVER_ROOT / profile.sandbox / "candidates" / candidate_sha
    if not candidate_root.is_dir():
        raise LinkHostError("server candidate is not installed")
    _validate_installed_server_config(profile, candidate_sha, candidate_root)
    _run((str(INSTALLED_RELAY), "--config", str(candidate_root / "config.toml"), "--check"))
    previous_sha = _current_server_sha(profile)
    _write_journal(profile, candidate_sha, previous_sha, "switching")
    _atomic_symlink(f"candidates/{candidate_sha}", SERVER_ROOT / profile.sandbox / "current")
    _write_journal(profile, candidate_sha, previous_sha, "switched")
    unit = f"loom-developer-sandbox-link@{profile.sandbox}.service"
    try:
        _write_journal(profile, candidate_sha, previous_sha, "restarting")
        _run(("systemctl", "enable", unit))
        _run(("systemctl", "restart", unit))
        active = _run(("systemctl", "is-active", unit)).stdout.strip()
        if active != "active":
            raise LinkHostError("sandbox relay did not become active")
        current = (SERVER_ROOT / profile.sandbox / "current").resolve(strict=True)
        if current != candidate_root.resolve(strict=True):
            raise LinkHostError("sandbox relay candidate readback failed")
        check_server(profile, candidate_sha)
    except Exception:
        _restore_activation(profile, previous_sha)
        _remove_journal(profile)
        raise
    _remove_journal(profile)


def _validate_installed_server_config(
    profile: Profile,
    candidate_sha: str,
    candidate_root: Path,
) -> None:
    try:
        with (candidate_root / "config.toml").open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise LinkHostError("sandbox relay config is unavailable") from exc
    expected = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": candidate_sha,
        "bind_address": profile.server_address,
        "target_host": "127.0.0.1",
        "ca_file": str(candidate_root / "ca.pem"),
        "cert_file": str(candidate_root / "server.pem"),
        "key_file": str(candidate_root / "server-key.pem"),
        "services": {
            service.name: {
                "bind_port": service.server_port,
                "target_port": service.target_port,
                "health_path": service.health_path,
                "allow_empty_health": service.allow_empty_health,
            }
            for service in profile.services
        },
    }
    if raw != expected:
        raise LinkHostError("sandbox relay config drifted from the closed profile")


def check_server(profile: Profile, candidate_sha: str) -> dict[str, Any]:
    candidate_root = SERVER_ROOT / profile.sandbox / "candidates" / candidate_sha
    current_link = SERVER_ROOT / profile.sandbox / "current"
    try:
        current = current_link.resolve(strict=True)
    except OSError as exc:
        raise LinkHostError("sandbox relay current candidate is unavailable") from exc
    if current != candidate_root.resolve(strict=True):
        raise LinkHostError("sandbox relay active candidate does not match")
    unit = f"loom-developer-sandbox-link@{profile.sandbox}.service"
    if _run(("systemctl", "is-active", unit)).stdout.strip() != "active":
        raise LinkHostError("sandbox relay unit is not active")
    _validate_installed_server_config(profile, candidate_sha, candidate_root)
    _run(
        (
            str(INSTALLED_RELAY),
            "--config",
            str(candidate_root / "config.toml"),
            "--check",
        ),
    )
    listeners = _run(("ss", "-H", "-ltn")).stdout
    server_text = _certificate_text(candidate_root / "server.pem")
    if set(re.findall(r"IP Address:([^,\s]+)", server_text)) != {
        profile.server_address,
    }:
        raise LinkHostError("sandbox relay server certificate SAN drifted")
    services: dict[str, dict[str, Any]] = {}
    for service in profile.services:
        listener_marker = f"{profile.server_address}:{service.server_port}"
        if listener_marker not in listeners:
            raise LinkHostError(f"sandbox relay {service.name} listener is absent")
        services[service.name] = {
            "listener_port": service.server_port,
            "target_host": "127.0.0.1",
            "target_port": service.target_port,
            "health_path": service.health_path,
            "tls_version": "TLSv1.3",
            "status": "active",
        }
    return {
        "node": "oldlab-2",
        "address": profile.server_address,
        "unit": unit,
        "unit_active": True,
        "active_candidate_sha": candidate_sha,
        "ca_fingerprint": _fingerprint(candidate_root / "ca.pem"),
        "server_cert_fingerprint": _fingerprint(candidate_root / "server.pem"),
        "client_uri_san": profile.client_uri(candidate_sha),
        "services": services,
    }


def _client_paths(profile: Profile, candidate_sha: str) -> dict[str, Path]:
    root = CLIENT_ROOT / profile.sandbox / candidate_sha
    return {
        "ca": root / "ca.pem",
        "cert": root / "client.pem",
        "key": root / "client-key.pem",
        "token": root / "worker-token",
        "minio_access": root / "minio-access-key",
        "minio_secret": root / "minio-secret-key",
        "metadata": root / "metadata.json",
    }


def check_client(profile: Profile, candidate_sha: str, node: str) -> dict[str, Any]:
    paths = _client_paths(profile, candidate_sha)
    try:
        metadata = json.loads(paths["metadata"].read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LinkHostError("client metadata is unavailable") from exc
    if (
        not isinstance(metadata, dict)
        or set(metadata)
        != {
            "schema_version",
            "sandbox",
            "candidate_sha",
            "node",
            "server_address",
            "service_ports",
            "ca_fingerprint",
            "client_cert_fingerprint",
        }
        or metadata.get("schema_version") != 1
        or metadata.get("sandbox") != profile.sandbox
        or metadata.get("candidate_sha") != candidate_sha
        or metadata.get("node") != node
        or metadata.get("server_address") != profile.server_address
        or metadata.get("service_ports")
        != {service.name: service.server_port for service in profile.services}
    ):
        raise LinkHostError("client metadata identity drifted")
    for label, path in paths.items():
        file_metadata = path.lstat()
        expected_mode = (
            0o600
            if label in {"key", "token", "minio_access", "minio_secret", "metadata"}
            else 0o644
        )
        if (
            not stat.S_ISREG(file_metadata.st_mode)
            or stat.S_ISLNK(file_metadata.st_mode)
            or file_metadata.st_nlink != 1
            or (file_metadata.st_uid, file_metadata.st_gid) != (0, 0)
            or stat.S_IMODE(file_metadata.st_mode) != expected_mode
        ):
            raise LinkHostError("client credential readback failed")
    _validate_token_file(paths["token"])
    _validate_opaque_secret_file(paths["minio_access"], label="MinIO access key")
    _validate_opaque_secret_file(paths["minio_secret"], label="MinIO secret key")
    if metadata["ca_fingerprint"] != _fingerprint(paths["ca"]) or metadata[
        "client_cert_fingerprint"
    ] != _fingerprint(paths["cert"]):
        raise LinkHostError("client credential fingerprint drifted")
    _run(("ip", "route", "get", profile.server_address))
    context = ssl.create_default_context(cafile=str(paths["ca"]))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certfile=str(paths["cert"]), keyfile=str(paths["key"]))
    service_results: dict[str, dict[str, Any]] = {}
    for service in profile.services:
        connection = http.client.HTTPSConnection(
            profile.server_address,
            service.server_port,
            timeout=5.0,
            context=context,
        )
        try:
            connection.request("GET", service.health_path)
            response = connection.getresponse()
            body = response.read(4096)
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            raise LinkHostError(
                f"candidate-bound {service.name} TLS health probe failed",
            ) from exc
        finally:
            connection.close()
        if (
            response.status < 200
            or response.status >= 300
            or (not body and not service.allow_empty_health)
        ):
            raise LinkHostError(
                f"candidate-bound {service.name} TLS health probe was unhealthy",
            )
        service_results[service.name] = {
            "listener_port": service.server_port,
            "target_port": service.target_port,
            "health": "ok",
        }
    return {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": candidate_sha,
        "node": node,
        "route": "ok",
        "tls_version": "TLSv1.3",
        "services": service_results,
        "client_uri_san": profile.client_uri(candidate_sha),
        "secret_files": {
            "worker-token": {"uid": 0, "gid": 0, "mode": "0600", "present": True},
            "minio-access-key": {
                "uid": 0,
                "gid": 0,
                "mode": "0600",
                "present": True,
            },
            "minio-secret-key": {
                "uid": 0,
                "gid": 0,
                "mode": "0600",
                "present": True,
            },
            "client-key.pem": {
                "uid": 0,
                "gid": 0,
                "mode": "0600",
                "present": True,
            },
        },
        "ca_fingerprint": metadata["ca_fingerprint"],
        "client_cert_fingerprint": metadata["client_cert_fingerprint"],
    }


def validate_worker_env(
    profile: Profile,
    candidate_sha: str,
    path: Path,
) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise LinkHostError("worker env file is unavailable") from exc
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator != "=" or not key or key in values:
            raise LinkHostError("worker env file is malformed")
        values[key] = value
    for key, value in values.items():
        normalized = key.upper()
        if (
            value
            and any(
                marker in normalized
                for marker in ("TOKEN", "PASSWORD", "SECRET", "API_KEY", "ACCESS_KEY")
            )
            and not normalized.endswith("_FILE_HOST")
        ):
            raise LinkHostError("raw secrets are forbidden in sandbox env files")
    root = CLIENT_ROOT / profile.sandbox / candidate_sha
    control_plane = profile.service("control-plane")
    gateway = profile.service("gateway")
    minio = profile.service("minio")
    expected = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_SANDBOX_LINK_CP_UPSTREAM": (
            f"https://{profile.server_address}:{control_plane.server_port}"
        ),
        "LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM": (
            f"https://{profile.server_address}:{gateway.server_port}"
        ),
        "LOOM_SANDBOX_LINK_MINIO_UPSTREAM": (
            f"https://{profile.server_address}:{minio.server_port}"
        ),
        "LOOM_WORKER_TOKEN_FILE_HOST": str(root / "worker-token"),
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": str(root / "minio-access-key"),
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": str(root / "minio-secret-key"),
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": str(root / "ca.pem"),
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": str(root / "client.pem"),
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": str(root / "client-key.pem"),
    }
    for key, value in expected.items():
        if values.get(key) != value:
            raise LinkHostError(f"worker env {key} is not candidate-bound")
        if key.endswith("_HOST") and (
            value.startswith("/shared_work/") or not value.startswith(str(root) + "/")
        ):
            raise LinkHostError("worker security material must remain host-local")
    return expected


def _attestation_digest(payload: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    canonical = json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def _parse_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LinkHostError(f"fleet attestation {label} is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise LinkHostError(f"fleet attestation {label} is invalid") from exc
    if parsed.utcoffset() != timedelta(0):
        raise LinkHostError(f"fleet attestation {label} is invalid")
    return parsed.astimezone(UTC)


def persist_attestation(
    profile: Profile,
    candidate_sha: str,
    payload: dict[str, Any],
) -> Path:
    with _link_transaction(profile):
        _recover_activation(profile)
        return _persist_attestation_locked(profile, candidate_sha, payload)


def _persist_attestation_locked(
    profile: Profile,
    candidate_sha: str,
    payload: dict[str, Any],
) -> Path:
    expected_keys = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "generated_at",
        "expires_at",
        "eligible_nodes",
        "bundle_generation",
        "server",
        "nodes",
        "payload_sha256",
    }
    nodes = payload.get("nodes")
    if (
        set(payload) != expected_keys
        or payload.get("schema_version") != 1
        or payload.get("sandbox") != profile.sandbox
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("eligible_nodes") != list(INFRASTRUCTURE_LINK_NODES)
        or not isinstance(nodes, dict)
        or set(nodes) != set(INFRASTRUCTURE_LINK_NODES)
        or payload.get("payload_sha256") != _attestation_digest(payload)
    ):
        raise LinkHostError("fleet attestation does not match the closed schema")
    generated = _parse_utc(payload.get("generated_at"), label="generated_at")
    expires = _parse_utc(payload.get("expires_at"), label="expires_at")
    now = datetime.now(UTC)
    if (
        generated > now + timedelta(seconds=30)
        or now - generated > timedelta(seconds=60)
        or expires - generated != timedelta(seconds=ATTESTATION_TTL_SECONDS)
        or expires <= now
    ):
        raise LinkHostError("fleet attestation freshness is invalid")
    server = payload.get("server")
    bundle = payload.get("bundle_generation")
    expected_server_keys = {
        "node",
        "address",
        "unit",
        "unit_active",
        "active_candidate_sha",
        "ca_fingerprint",
        "server_cert_fingerprint",
        "client_uri_san",
        "services",
    }
    if (
        not isinstance(bundle, dict)
        or set(bundle) != {"candidate_sha", "ca_fingerprint", "client_uri_san"}
        or bundle.get("candidate_sha") != candidate_sha
        or FINGERPRINT_RE.fullmatch(str(bundle.get("ca_fingerprint"))) is None
        or bundle.get("client_uri_san") != profile.client_uri(candidate_sha)
        or not isinstance(server, dict)
        or set(server) != expected_server_keys
        or server.get("node") != "oldlab-2"
        or server.get("address") != profile.server_address
        or server.get("unit") != f"loom-developer-sandbox-link@{profile.sandbox}.service"
        or server.get("unit_active") is not True
        or server.get("active_candidate_sha") != candidate_sha
        or server.get("ca_fingerprint") != bundle.get("ca_fingerprint")
        or FINGERPRINT_RE.fullmatch(str(server.get("server_cert_fingerprint"))) is None
        or server.get("client_uri_san") != profile.client_uri(candidate_sha)
        or set(server.get("services", {})) != set(SERVICE_VALUES)
    ):
        raise LinkHostError("fleet attestation server binding is invalid")
    expected_secret_state = {
        "present": True,
        "uid": 0,
        "gid": 0,
        "mode": "0600",
    }
    expected_node_keys = {
        "node",
        "candidate_sha",
        "route",
        "tls_version",
        "client_uri_san",
        "ca_fingerprint",
        "client_cert_fingerprint",
        "secret_files",
        "services",
    }
    for node in INFRASTRUCTURE_LINK_NODES:
        node_payload = nodes.get(node)
        if (
            not isinstance(node_payload, dict)
            or set(node_payload) != expected_node_keys
            or node_payload.get("node") != node
            or node_payload.get("candidate_sha") != candidate_sha
            or node_payload.get("route") != {"destination": profile.server_address, "status": "ok"}
            or node_payload.get("tls_version") != "TLSv1.3"
            or node_payload.get("client_uri_san") != profile.client_uri(candidate_sha)
            or node_payload.get("ca_fingerprint") != server.get("ca_fingerprint")
            or FINGERPRINT_RE.fullmatch(
                str(node_payload.get("client_cert_fingerprint")),
            )
            is None
            or set(node_payload.get("services", {})) != set(SERVICE_VALUES)
            or set(node_payload.get("secret_files", {}))
            != {
                "worker-token",
                "minio-access-key",
                "minio-secret-key",
                "client-key.pem",
            }
            or any(
                state != expected_secret_state
                for state in node_payload.get("secret_files", {}).values()
            )
        ):
            raise LinkHostError("fleet attestation node binding is invalid")
        for service in profile.services:
            if node_payload["services"].get(service.name) != {
                "listener_port": service.server_port,
                "health": "ok",
            }:
                raise LinkHostError("fleet attestation service health is invalid")
            if server["services"].get(service.name) != {
                "listener_port": service.server_port,
                "target_host": "127.0.0.1",
                "target_port": service.target_port,
                "health_path": service.health_path,
                "tls_version": "TLSv1.3",
                "status": "active",
            }:
                raise LinkHostError("fleet attestation relay binding is invalid")
    for root in (
        ATTESTATION_ROOT,
        ATTESTATION_ROOT / profile.sandbox,
        ATTESTATION_ROOT / profile.sandbox / candidate_sha,
    ):
        _ensure_root_dir(root)
    destination = ATTESTATION_ROOT / profile.sandbox / candidate_sha / "fleet.json"
    _atomic_write(
        destination,
        json.dumps(payload, sort_keys=True, indent=2) + "\n",
        mode=0o600,
    )
    readback = json.loads(destination.read_text(encoding="utf-8"))
    if readback != payload or _attestation_digest(readback) != payload["payload_sha256"]:
        raise LinkHostError("fleet attestation readback failed")
    return destination


def _plan(args: argparse.Namespace, profile: Profile, candidate_sha: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "command": args.command,
        "execute": bool(getattr(args, "execute", False)),
        "sandbox": profile.sandbox,
        "candidate_sha": candidate_sha,
        "services": {
            service.name: {
                "server": f"{profile.server_address}:{service.server_port}",
                "target": f"127.0.0.1:{service.target_port}",
            }
            for service in profile.services
        },
        "eligible_nodes": list(INFRASTRUCTURE_LINK_NODES),
        "shared_filesystem_secrets": False,
        "requires_root": args.command != "validate-env",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in (
        "prepare-rotation",
        "install-server",
        "install-client",
        "activate-server",
        "rollback-server",
        "check-server",
        "check-client",
        "validate-env",
        "persist-attestation",
    ):
        child = subparsers.add_parser(command)
        child.add_argument("--sandbox", choices=SANDBOXES, required=True)
        child.add_argument("--candidate-sha", required=True)
        if command in {
            "prepare-rotation",
            "install-server",
            "install-client",
            "activate-server",
            "rollback-server",
            "persist-attestation",
        }:
            child.add_argument("--execute", action="store_true")
        if command == "install-server":
            child.add_argument("--credential-source", type=Path, required=True)
        if command == "install-client":
            child.add_argument(
                "--node",
                choices=INFRASTRUCTURE_LINK_NODES,
                required=True,
            )
            child.add_argument("--credential-source", type=Path, required=True)
            child.add_argument("--worker-token-file", type=Path, required=True)
            child.add_argument("--minio-access-key-file", type=Path, required=True)
            child.add_argument("--minio-secret-key-file", type=Path, required=True)
        if command == "check-client":
            child.add_argument(
                "--node",
                choices=INFRASTRUCTURE_LINK_NODES,
                required=True,
            )
        if command == "validate-env":
            child.add_argument("--env-file", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidate_sha = _candidate(args.candidate_sha)
    profile = load_profile(args.sandbox)
    plan = _plan(args, profile, candidate_sha)
    if hasattr(args, "execute") and not args.execute:
        print(json.dumps(plan, sort_keys=True))
        return 0
    if (
        args.command
        in {
            "prepare-rotation",
            "install-server",
            "install-client",
            "activate-server",
            "rollback-server",
            "check-server",
            "check-client",
            "persist-attestation",
        }
        and os.geteuid() != 0
    ):
        raise LinkHostError("this command requires root")
    if args.command == "prepare-rotation":
        destination = prepare_rotation(profile, candidate_sha)
        plan["issuance_root"] = str(destination)
    elif args.command == "install-server":
        destination = install_server(
            profile,
            candidate_sha,
            args.credential_source,
        )
        plan["installed_root"] = str(destination)
    elif args.command == "install-client":
        destination = install_client(
            profile,
            candidate_sha,
            args.node,
            args.credential_source,
            args.worker_token_file,
            args.minio_access_key_file,
            args.minio_secret_key_file,
        )
        plan["installed_root"] = str(destination)
        plan["node"] = args.node
    elif args.command in {"activate-server", "rollback-server"}:
        activate_server(profile, candidate_sha)
        plan["active_candidate_sha"] = candidate_sha
    elif args.command == "check-server":
        plan = check_server(profile, candidate_sha)
    elif args.command == "check-client":
        plan = check_client(profile, candidate_sha, args.node)
    elif args.command == "validate-env":
        plan["validated_references"] = validate_worker_env(
            profile,
            candidate_sha,
            args.env_file,
        )
    elif args.command == "persist-attestation":
        try:
            payload = json.loads(sys.stdin.read())
        except json.JSONDecodeError as exc:
            raise LinkHostError("fleet attestation input is invalid") from exc
        if not isinstance(payload, dict):
            raise LinkHostError("fleet attestation input must be an object")
        destination = persist_attestation(profile, candidate_sha, payload)
        plan = {
            "schema_version": 1,
            "sandbox": profile.sandbox,
            "candidate_sha": candidate_sha,
            "path": str(destination),
            "payload_sha256": payload["payload_sha256"],
        }
    else:
        raise AssertionError(args.command)
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LinkHostError as exc:
        print(json.dumps({"status": "failed", "reason": str(exc)}, sort_keys=True))
        raise SystemExit(2) from None
