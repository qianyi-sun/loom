#!/usr/bin/env python3
"""Converge exact developer candidates and private worker envs per NFS domain.

All mutating commands are plan-only unless ``--execute`` is supplied. The
program is intended to be installed and run by root on an NFS domain publisher
and its clients. It never prints or stores worker env values outside a
root-only rollback snapshot.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import ctypes
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

SCHEMA_VERSION = 1
ALLOWED_SANDBOXES = ("qianyi", "hongjian", "devansh")
ALLOWED_DOMAINS = ("oldlab", "gb10")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
_ATTESTATION_TTL = timedelta(minutes=15)
_ATTESTATION_KEY_ROOT = Path(
    "/etc/loom/developer-domain-runtime/attestation-keys",
)
_TRUSTED_KEY_ROOT = Path(
    "/etc/loom/developer-domain-runtime/trusted-attestation-keys",
)
_PUBLIC_ATTESTATION_ROOT = Path("/var/lib/loom-developer-domain-attestations")
_COMBINED_ROOT = Path("/var/lib/loom-shared-capacity/runtime-attestations")
_COLLECTOR_HOSTNAME = "trt-eai-oldlab-2"
_FLEET_ATTESTATION_ROOT = Path(
    "/var/lib/loom-developer-sandbox-links/attestations",
)
_NODE_TRANSPORT = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
_RUNTIME_PROOF_ARTIFACT_NAMES = frozenset(
    {
        "combined.json",
        "fleet.json",
        "oldlab.json",
        "oldlab.sig",
        "oldlab.pub",
        "gb10.json",
        "gb10.sig",
        "gb10.pub",
    },
)
_RUNTIME_PROOF_ARTIFACT_MAX_BYTES = 1 << 20
_INFRASTRUCTURE_LINK_NODES = (
    "oldlab-1",
    "oldlab-2",
    "oldlab-3",
    "oldlab-4",
    "oldlab-5",
    "trt-gb10-1",
    "trt-gb10-2",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)
_SANDBOX_TARGET_PORTS = {
    "qianyi": {
        "control-plane": 20080,
        "gateway": 20100,
        "minio": 20900,
    },
    "hongjian": {
        "control-plane": 21080,
        "gateway": 21100,
        "minio": 21900,
    },
    "devansh": {
        "control-plane": 22080,
        "gateway": 22100,
        "minio": 22900,
    },
}
_SANDBOX_LISTENER_PORTS = {
    "qianyi": {
        "control-plane": 26080,
        "gateway": 26100,
        "minio": 26900,
    },
    "hongjian": {
        "control-plane": 27080,
        "gateway": 27100,
        "minio": 27900,
    },
    "devansh": {
        "control-plane": 28080,
        "gateway": 28100,
        "minio": 28900,
    },
}
_SERVICE_HEALTH_PATHS = {
    "control-plane": "/healthz",
    "gateway": "/healthz",
    "minio": "/minio/health/live",
}


class ConvergenceError(RuntimeError):
    """A bounded, secret-safe convergence failure."""


@dataclass(frozen=True, slots=True)
class SandboxGroup:
    name: str
    uid: int
    gid: int
    member: str
    upstream_control_plane_port: int
    upstream_gateway_port: int
    upstream_minio_port: int


@dataclass(frozen=True, slots=True)
class Peer:
    ssh_target: str
    hostname: str


@dataclass(frozen=True, slots=True)
class Domain:
    name: str
    worker_env_name: str
    worker_pool_name: str
    worker_max_concurrent: int
    capacity_policy_source: str
    candidate_root: Path
    runtime_root: Path
    publisher_hostname: str
    peers: tuple[Peer, ...]


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    shared_group: str
    shared_gid: int
    state_root: Path
    installed_program: Path
    installed_config: Path
    sandbox_groups: Mapping[str, SandboxGroup]
    domains: Mapping[str, Domain]


@dataclass(frozen=True, slots=True)
class CandidateIdentity:
    sha: str
    tree: str


@dataclass(frozen=True, slots=True)
class RuntimePaths:
    candidate: Path
    env: Path


_TOP_LEVEL_KEYS = {
    "schema_version",
    "shared_group",
    "shared_gid",
    "state_root",
    "installed_program",
    "installed_config",
    "sandbox_groups",
    "domains",
}
_GROUP_KEYS = {
    "name",
    "uid",
    "gid",
    "member",
    "upstream_control_plane_port",
    "upstream_gateway_port",
    "upstream_minio_port",
}
_DOMAIN_KEYS = {
    "worker_env_name",
    "worker_pool_name",
    "worker_max_concurrent",
    "capacity_policy_source",
    "candidate_root",
    "runtime_root",
    "publisher_hostname",
    "peers",
}
_PEER_KEYS = {"ssh_target", "hostname"}


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ConvergenceError(
            f"{label} fields are invalid: missing={sorted(expected - actual)}, "
            f"unknown={sorted(actual - expected)}",
        )


def _absolute_path(value: object, label: str) -> Path:
    if not isinstance(value, str):
        raise ConvergenceError(f"{label} must be a string")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ConvergenceError(f"{label} must be an absolute normalized path")
    return path


def _safe_name(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_NAME_RE.fullmatch(value) is None:
        raise ConvergenceError(f"{label} is invalid")
    return value


def load_config(path: Path) -> RuntimeConfig:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConvergenceError("runtime-domain config is unavailable or invalid") from exc
    _exact_keys(raw, _TOP_LEVEL_KEYS, "config")
    if raw["schema_version"] != SCHEMA_VERSION:
        raise ConvergenceError(f"schema_version must be {SCHEMA_VERSION}")
    shared_group = _safe_name(raw["shared_group"], "shared_group")
    shared_gid = raw["shared_gid"]
    if type(shared_gid) is not int or not 1 <= shared_gid <= 60000:
        raise ConvergenceError("shared_gid is invalid")

    groups_raw = raw["sandbox_groups"]
    if not isinstance(groups_raw, dict) or set(groups_raw) != set(ALLOWED_SANDBOXES):
        raise ConvergenceError("sandbox_groups must define the three fixed sandboxes")
    groups: dict[str, SandboxGroup] = {}
    gids: set[int] = {shared_gid}
    for sandbox in ALLOWED_SANDBOXES:
        item = groups_raw[sandbox]
        if not isinstance(item, dict):
            raise ConvergenceError(f"sandbox_groups.{sandbox} must be a table")
        _exact_keys(item, _GROUP_KEYS, f"sandbox_groups.{sandbox}")
        name = _safe_name(item["name"], f"sandbox_groups.{sandbox}.name")
        member = _safe_name(item["member"], f"sandbox_groups.{sandbox}.member")
        uid = item["uid"]
        gid = item["gid"]
        upstream_control_plane_port = item["upstream_control_plane_port"]
        upstream_gateway_port = item["upstream_gateway_port"]
        upstream_minio_port = item["upstream_minio_port"]
        if (
            name != f"loom-sandbox-{sandbox}"
            or member != f"loom-sandbox-{sandbox}"
            or type(uid) is not int
            or not 1000 <= uid <= 60000
            or uid != gid
            or type(gid) is not int
            or not 1000 <= gid <= 60000
            or gid in gids
            or any(
                type(port) is not int or not 1024 <= port <= 65535
                for port in (
                    upstream_control_plane_port,
                    upstream_gateway_port,
                    upstream_minio_port,
                )
            )
            or len(
                {
                    upstream_control_plane_port,
                    upstream_gateway_port,
                    upstream_minio_port,
                },
            )
            != 3
            or {
                "control-plane": upstream_control_plane_port,
                "gateway": upstream_gateway_port,
                "minio": upstream_minio_port,
            }
            != _SANDBOX_LISTENER_PORTS[sandbox]
        ):
            raise ConvergenceError(f"sandbox_groups.{sandbox} identity is invalid")
        gids.add(gid)
        groups[sandbox] = SandboxGroup(
            name=name,
            uid=uid,
            gid=gid,
            member=member,
            upstream_control_plane_port=upstream_control_plane_port,
            upstream_gateway_port=upstream_gateway_port,
            upstream_minio_port=upstream_minio_port,
        )

    domains_raw = raw["domains"]
    if not isinstance(domains_raw, dict) or set(domains_raw) != set(ALLOWED_DOMAINS):
        raise ConvergenceError("domains must define oldlab and gb10")
    domains: dict[str, Domain] = {}
    for name in ALLOWED_DOMAINS:
        item = domains_raw[name]
        if not isinstance(item, dict):
            raise ConvergenceError(f"domains.{name} must be a table")
        _exact_keys(item, _DOMAIN_KEYS, f"domains.{name}")
        env_name = item["worker_env_name"]
        if env_name != f"worker-{name}.env":
            raise ConvergenceError(f"domains.{name}.worker_env_name is invalid")
        worker_pool_name = item["worker_pool_name"]
        worker_max_concurrent = item["worker_max_concurrent"]
        capacity_policy_source = item["capacity_policy_source"]
        expected_capacity_policy_source = (
            f"deploy/developer-sandboxes/shared-capacity-policies/{name}.toml"
        )
        expected_worker_max_concurrent = 4 if name == "oldlab" else 8
        if (
            worker_pool_name != name
            or type(worker_max_concurrent) is not int
            or worker_max_concurrent != expected_worker_max_concurrent
            or capacity_policy_source != expected_capacity_policy_source
        ):
            raise ConvergenceError(f"domains.{name} worker capacity binding is invalid")
        peers_raw = item["peers"]
        if not isinstance(peers_raw, list) or not peers_raw:
            raise ConvergenceError(f"domains.{name}.peers must not be empty")
        peers: list[Peer] = []
        peer_hosts: set[str] = set()
        peer_targets: set[str] = set()
        for index, peer_raw in enumerate(peers_raw):
            if not isinstance(peer_raw, dict):
                raise ConvergenceError(f"domains.{name}.peers[{index}] is invalid")
            _exact_keys(peer_raw, _PEER_KEYS, f"domains.{name}.peers[{index}]")
            target = _safe_name(peer_raw["ssh_target"], "peer ssh_target")
            hostname = _safe_name(peer_raw["hostname"], "peer hostname")
            if hostname in peer_hosts or target in peer_targets:
                raise ConvergenceError(f"domains.{name} peer identity is duplicated")
            peer_hosts.add(hostname)
            peer_targets.add(target)
            peers.append(Peer(ssh_target=target, hostname=hostname))
        publisher = _safe_name(
            item["publisher_hostname"],
            f"domains.{name}.publisher_hostname",
        )
        if publisher not in peer_hosts:
            raise ConvergenceError(f"domains.{name} publisher must be a peer")
        candidate_root = _absolute_path(
            item["candidate_root"],
            f"domains.{name}.candidate_root",
        )
        runtime_root = _absolute_path(item["runtime_root"], f"domains.{name}.runtime_root")
        expected_candidate = Path("/shared_work/loom/candidates/sandboxes")
        expected_runtime = Path("/shared_work/loom/runtime/sandboxes")
        if candidate_root != expected_candidate or runtime_root != expected_runtime:
            raise ConvergenceError(f"domains.{name} logical paths must remain canonical")
        domains[name] = Domain(
            name=name,
            worker_env_name=env_name,
            worker_pool_name=worker_pool_name,
            worker_max_concurrent=worker_max_concurrent,
            capacity_policy_source=capacity_policy_source,
            candidate_root=candidate_root,
            runtime_root=runtime_root,
            publisher_hostname=publisher,
            peers=tuple(peers),
        )

    return RuntimeConfig(
        shared_group=shared_group,
        shared_gid=shared_gid,
        state_root=_absolute_path(raw["state_root"], "state_root"),
        installed_program=_absolute_path(raw["installed_program"], "installed_program"),
        installed_config=_absolute_path(raw["installed_config"], "installed_config"),
        sandbox_groups=groups,
        domains=domains,
    )


def _hostname() -> str:
    return socket.gethostname().split(".", 1)[0].lower()


def _require_root() -> None:
    if os.geteuid() != 0:
        raise ConvergenceError("operation requires root; no unprivileged fallback exists")


def _run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        cwd=cwd,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        raise ConvergenceError(f"command failed safely: {Path(argv[0]).name}")
    return result


def _authority_envelope(
    *,
    action: str,
    node: str,
    domain: str,
    sandbox: str,
    identity: CandidateIdentity,
) -> str:
    if (
        action not in {"inspect-candidate", "inspect-local", "export-domain-attestation"}
        or node not in _INFRASTRUCTURE_LINK_NODES
        or domain not in ALLOWED_DOMAINS
        or sandbox not in ALLOWED_SANDBOXES
    ):
        raise ConvergenceError("node authority check identity is invalid")
    body: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "action": action,
        "node": node,
        "domain": domain,
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "payload_kind": "none",
        "payload_sha256": hashlib.sha256(b"").hexdigest(),
        "payload_base64": "",
        "prior_request_id": None,
    }
    request_id = hashlib.sha256(_canonical_json(body) + b"\n").hexdigest()
    body["request_id"] = request_id
    return (_canonical_json(body) + b"\n").decode("ascii")


def _authority_check(
    *,
    action: str,
    node: str,
    domain: str,
    sandbox: str,
    identity: CandidateIdentity,
) -> dict[str, Any]:
    result = _run(
        (
            str(_NODE_TRANSPORT),
            "invoke",
            "--node",
            node,
            "--verb",
            "check",
        ),
        input_text=_authority_envelope(
            action=action,
            node=node,
            domain=domain,
            sandbox=sandbox,
            identity=identity,
        ),
        env={
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "LANG": "C.UTF-8",
        },
    )
    if result.stderr:
        raise ConvergenceError("node authority check failed safely")
    try:
        envelope = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ConvergenceError("node authority check returned invalid output") from exc
    report = envelope.get("result") if isinstance(envelope, dict) else None
    if envelope.get("status") != "succeeded" or not isinstance(report, dict):
        raise ConvergenceError("node authority check returned invalid output")
    return report


def _git_run(*argv: str) -> subprocess.CompletedProcess[str]:
    return _run(
        (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            *argv,
        ),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
        },
    )


def _candidate_identity(source_repo: Path, sha: str) -> CandidateIdentity:
    if _SHA_RE.fullmatch(sha) is None:
        raise ConvergenceError("candidate SHA must be full lowercase 40-hex")
    try:
        source = source_repo.resolve(strict=True)
    except OSError as exc:
        raise ConvergenceError("candidate source repository is unavailable") from exc
    if not source.is_dir():
        raise ConvergenceError("candidate source repository is unavailable")
    resolved = _git_run(
        "-C",
        str(source),
        "rev-parse",
        "--verify",
        f"{sha}^{{commit}}",
    ).stdout.strip()
    if resolved != sha:
        raise ConvergenceError("candidate source does not contain the exact commit")
    tree = _git_run(
        "-C",
        str(source),
        "rev-parse",
        "--verify",
        f"{sha}^{{tree}}",
    ).stdout.strip()
    if _SHA_RE.fullmatch(tree) is None:
        raise ConvergenceError("candidate tree identity is invalid")
    return CandidateIdentity(sha=sha, tree=tree)


def _bundle_candidate_identity(
    source_bundle: Path,
    sha: str,
    expected_tree: str,
) -> CandidateIdentity:
    if _SHA_RE.fullmatch(sha) is None or _SHA_RE.fullmatch(expected_tree) is None:
        raise ConvergenceError("candidate bundle identity is invalid")
    try:
        metadata = source_bundle.lstat()
        parent = source_bundle.parent.lstat()
    except OSError as exc:
        raise ConvergenceError("candidate source bundle is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != 0
        or parent.st_gid != 0
        or stat.S_IMODE(parent.st_mode) != 0o700
    ):
        raise ConvergenceError("candidate source bundle metadata is invalid")
    heads = _git_run("bundle", "list-heads", str(source_bundle)).stdout.splitlines()
    if heads != [f"{sha} HEAD"]:
        raise ConvergenceError("candidate source bundle does not contain one exact HEAD")
    with tempfile.TemporaryDirectory() as temporary:
        checkout = Path(temporary) / "candidate"
        _git_run(
            "clone",
            "--quiet",
            "--no-checkout",
            str(source_bundle),
            str(checkout),
        )
        identity = _candidate_identity(checkout, sha)
    if identity.tree != expected_tree:
        raise ConvergenceError("candidate source bundle tree does not match")
    return identity


def _secure_seed(path: Path, *, require_root_owner: bool = False) -> Path:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ConvergenceError("worker env seed is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (require_root_owner and metadata.st_uid != 0)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise ConvergenceError(
            "worker env seed must be a root-owned private regular non-symlink file",
        )
    resolved = path.resolve(strict=True)
    if require_root_owner:
        parent = resolved.parent.stat()
        if (
            parent.st_uid != 0
            or not stat.S_ISDIR(parent.st_mode)
            or stat.S_IMODE(parent.st_mode) & 0o022
        ):
            raise ConvergenceError("worker env seed parent must be root-owned and not writable")
    return resolved


def _parse_env_references(
    path: Path,
    *,
    domain: Domain,
    sandbox: str,
    sha: str,
) -> dict[str, str]:
    """Validate a secret-free local-link env without dereferencing host files."""
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ConvergenceError("worker env seed is unavailable or invalid") from exc
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or key in values:
            raise ConvergenceError("worker env seed has an invalid or duplicate entry")
        values[key] = value
    bundle_root = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{sha}"
    expected = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_WORKER_SANDBOX_IDENTITY": sandbox,
        "LOOM_WORKER_CANDIDATE_SHA": sha,
        "LOOM_WORKER_POOL_NAME": domain.worker_pool_name,
        "LOOM_WORKER_MAX_CONCURRENT": str(domain.worker_max_concurrent),
        "LOOM_WORKER_TOKEN_FILE_HOST": f"{bundle_root}/worker-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": f"{bundle_root}/minio-access-key",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": f"{bundle_root}/minio-secret-key",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": f"{bundle_root}/ca.pem",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": f"{bundle_root}/client.pem",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": f"{bundle_root}/client-key.pem",
    }
    for key, expected_value in expected.items():
        if values.get(key) != expected_value:
            raise ConvergenceError(f"worker env seed {key} is not exact-bound")
    allowed_host_secret_references = {
        "LOOM_WORKER_TOKEN_FILE_HOST",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST",
    }
    forbidden_raw = sorted(
        key
        for key in values
        if key not in allowed_host_secret_references
        and (
            "TOKEN" in key
            or "PASSWORD" in key
            or "PROVIDER" in key
            or "API_KEY" in key
            or "ACCESS_KEY" in key
            or "SECRET_KEY" in key
            or "PRIVATE_KEY" in key
            or key.endswith("_KEY")
        )
    )
    if forbidden_raw:
        raise ConvergenceError(
            "worker env seed contains forbidden raw secret fields: " + ",".join(forbidden_raw),
        )
    if any("-----BEGIN " in value for value in values.values()):
        raise ConvergenceError("worker env seed contains inline key or certificate material")
    if set(values) != set(expected):
        raise ConvergenceError("worker env seed fields are not the exact closed schema")
    return values


def runtime_paths(domain: Domain, sandbox: str, sha: str) -> RuntimePaths:
    if sandbox not in ALLOWED_SANDBOXES or _SHA_RE.fullmatch(sha) is None:
        raise ConvergenceError("runtime identity is invalid")
    return RuntimePaths(
        candidate=domain.candidate_root / sandbox / sha,
        env=domain.runtime_root / sandbox / sha / domain.worker_env_name,
    )


def _service_identity_status(group: SandboxGroup) -> str:
    try:
        local_group = grp.getgrnam(group.name)
    except KeyError:
        try:
            group_collision = grp.getgrgid(group.gid)
        except KeyError:
            return "create-group"
        raise ConvergenceError(
            f"stable group GID is already owned by {group_collision.gr_name}",
        ) from None
    if local_group.gr_gid != group.gid:
        raise ConvergenceError(f"stable group {group.name} has a conflicting GID")
    if set(local_group.gr_mem):
        raise ConvergenceError(f"stable group {group.name} has unexpected explicit members")
    try:
        user = pwd.getpwnam(group.member)
    except KeyError:
        try:
            user_collision = pwd.getpwuid(group.uid)
        except KeyError:
            return "create-user"
        raise ConvergenceError(
            f"stable service UID is already owned by {user_collision.pw_name}",
        ) from None
    if (
        user.pw_uid != group.uid
        or user.pw_gid != group.gid
        or user.pw_dir != "/nonexistent"
        or user.pw_shell != "/usr/sbin/nologin"
    ):
        raise ConvergenceError(f"stable service identity {group.member} has conflicting metadata")
    return "ok"


def _batch_identity(group: SandboxGroup) -> pwd.struct_passwd:
    try:
        identity = pwd.getpwnam(group.member)
    except KeyError as exc:
        raise ConvergenceError("sandbox batch identity is unavailable") from exc
    if (
        identity.pw_uid != group.uid
        or identity.pw_gid != group.gid
        or identity.pw_dir != "/nonexistent"
        or identity.pw_shell != "/usr/sbin/nologin"
    ):
        raise ConvergenceError("sandbox batch identity metadata drifted")
    return identity


def _user_has_group(member: str, gid: int) -> bool:
    try:
        user = pwd.getpwnam(member)
    except KeyError as exc:
        raise ConvergenceError(f"required local account {member} is unavailable") from exc
    return gid in os.getgrouplist(member, user.pw_gid)


def identity_plan(config: RuntimeConfig, domain: Domain) -> dict[str, object]:
    local = _hostname()
    if local not in {peer.hostname for peer in domain.peers}:
        raise ConvergenceError("local hostname is not a declared domain peer")
    try:
        shared = grp.getgrnam(config.shared_group)
    except KeyError as exc:
        raise ConvergenceError("sharedwork group is unavailable") from exc
    if shared.gr_gid != config.shared_gid:
        raise ConvergenceError("sharedwork GID does not match the domain contract")
    groups = {
        sandbox: _service_identity_status(group) for sandbox, group in config.sandbox_groups.items()
    }
    sharedwork_membership = {
        sandbox: ("ok" if _user_has_group(group.member, config.shared_gid) else "add-member")
        for sandbox, group in config.sandbox_groups.items()
    }
    private_key, public_key = _attestation_key_paths(domain.name)
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "host-converge",
        "mode": "plan",
        "domain": domain.name,
        "hostname": local,
        "groups": groups,
        "sharedwork_membership": sharedwork_membership,
        "installed_program": str(config.installed_program),
        "installed_config": str(config.installed_config),
        "gb10_live_authority": (
            "root-required-fail-closed" if domain.name == "gb10" else "root-required"
        ),
        "attestation_key": (
            "create-or-verify" if local == domain.publisher_hostname else "publisher-host-only"
        ),
        "attestation_private_key_path": str(private_key),
        "attestation_public_key_path": str(public_key),
    }


def _atomic_install(source: Path, target: Path, mode: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chown(target.parent, 0, 0)
    os.chmod(target.parent, 0o755)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(source.read_bytes())
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_bytes(
    target: Path,
    content: bytes,
    *,
    mode: int,
    parent_mode: int = 0o700,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    os.chown(target.parent, 0, 0)
    os.chmod(target.parent, parent_mode)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _attestation_key_paths(domain: str) -> tuple[Path, Path]:
    return (
        _ATTESTATION_KEY_ROOT / f"{domain}.key",
        _ATTESTATION_KEY_ROOT / f"{domain}.pub",
    )


def _key_id(public_key: Path) -> str:
    try:
        metadata = public_key.lstat()
        content = public_key.read_bytes()
    except OSError as exc:
        raise ConvergenceError("attestation public key is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o644
    ):
        raise ConvergenceError("attestation public key metadata is invalid")
    return hashlib.sha256(content).hexdigest()


def _ensure_attestation_key(domain: str) -> str:
    private_key, public_key = _attestation_key_paths(domain)
    _ATTESTATION_KEY_ROOT.mkdir(parents=True, exist_ok=True)
    os.chown(_ATTESTATION_KEY_ROOT, 0, 0)
    os.chmod(_ATTESTATION_KEY_ROOT, 0o700)
    if private_key.exists() != public_key.exists():
        raise ConvergenceError("attestation keypair is incomplete")
    if not private_key.exists():
        with tempfile.TemporaryDirectory(dir=_ATTESTATION_KEY_ROOT) as temporary:
            temp_root = Path(temporary)
            generated_private = temp_root / "key"
            generated_public = temp_root / "key.pub"
            _run(
                (
                    "openssl",
                    "genpkey",
                    "-algorithm",
                    "ED25519",
                    "-out",
                    str(generated_private),
                ),
            )
            _run(
                (
                    "openssl",
                    "pkey",
                    "-in",
                    str(generated_private),
                    "-pubout",
                    "-out",
                    str(generated_public),
                ),
            )
            _atomic_bytes(private_key, generated_private.read_bytes(), mode=0o600)
            _atomic_bytes(public_key, generated_public.read_bytes(), mode=0o644)
    private_metadata = private_key.lstat()
    if (
        stat.S_ISLNK(private_metadata.st_mode)
        or not stat.S_ISREG(private_metadata.st_mode)
        or private_metadata.st_uid != 0
        or private_metadata.st_gid != 0
        or stat.S_IMODE(private_metadata.st_mode) != 0o600
    ):
        raise ConvergenceError("attestation private key metadata is invalid")
    return _key_id(public_key)


def converge_host(config_path: Path, config: RuntimeConfig, domain: Domain) -> dict[str, object]:
    _require_root()
    plan = identity_plan(config, domain)
    for sandbox, group in config.sandbox_groups.items():
        status = plan["groups"][sandbox]  # type: ignore[index]
        if status == "create-group":
            _run(("groupadd", "--gid", str(group.gid), group.name))
            status = "create-user"
        if status == "create-user":
            _run(
                (
                    "useradd",
                    "--uid",
                    str(group.uid),
                    "--gid",
                    group.name,
                    "--no-create-home",
                    "--home-dir",
                    "/nonexistent",
                    "--shell",
                    "/usr/sbin/nologin",
                    group.member,
                ),
            )
        if _service_identity_status(group) != "ok":
            raise ConvergenceError(f"stable service identity {group.member} did not converge")
        if not _user_has_group(group.member, config.shared_gid):
            _run(
                (
                    "usermod",
                    "--append",
                    "--groups",
                    config.shared_group,
                    group.member,
                ),
            )
        if not _user_has_group(group.member, config.shared_gid):
            raise ConvergenceError(
                f"{group.member} did not converge into {config.shared_group}",
            )
    _atomic_install(Path(__file__).resolve(), config.installed_program, 0o755)
    _atomic_install(config_path.resolve(strict=True), config.installed_config, 0o644)
    if _hostname() == domain.publisher_hostname:
        for path in (
            domain.candidate_root.parent.parent,
            domain.candidate_root.parent,
            domain.candidate_root,
        ):
            _ensure_directory(path, gid=config.shared_gid, mode=0o2750)
    report = dict(plan)
    report["mode"] = "applied"
    report["groups"] = {
        sandbox: _service_identity_status(group) for sandbox, group in config.sandbox_groups.items()
    }
    report["sharedwork_membership"] = {
        sandbox: ("ok" if _user_has_group(group.member, config.shared_gid) else "missing")
        for sandbox, group in config.sandbox_groups.items()
    }
    if _hostname() == domain.publisher_hostname:
        report["attestation_key_id"] = _ensure_attestation_key(domain.name)
        report["attestation_private_key"] = "installed-root-only"
        report["attestation_public_key"] = str(_attestation_key_paths(domain.name)[1])
    return report


def publish_plan(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
    env_seed: Path,
) -> dict[str, object]:
    paths = runtime_paths(domain, sandbox, identity.sha)
    seed = _secure_seed(env_seed)
    _parse_env_references(
        seed,
        domain=domain,
        sandbox=sandbox,
        sha=identity.sha,
    )
    candidate_action = "verify" if paths.candidate.exists() else "create"
    env_action = "create"
    if paths.env.exists():
        metadata = paths.env.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise ConvergenceError("published worker env is not a regular non-symlink file")
        env_action = "noop" if paths.env.read_bytes() == seed.read_bytes() else "replace"
    attestation_path, attestation_signature_path = _attestation_paths(
        domain.name,
        sandbox,
        identity.sha,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "publish",
        "mode": "plan",
        "domain": domain.name,
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "candidate_path": str(paths.candidate),
        "candidate_action": candidate_action,
        "env_path": str(paths.env),
        "env_action": env_action,
        "env_values": "redacted",
        "tls_bundle": "host-local-reference-only",
        "sandbox_group": config.sandbox_groups[sandbox].name,
        "peer_readback": [peer.ssh_target for peer in domain.peers],
        "attestation_path": str(attestation_path),
        "attestation_signature_path": str(attestation_signature_path),
        "attestation_ttl_seconds": int(_ATTESTATION_TTL.total_seconds()),
    }


def _ensure_directory(path: Path, *, gid: int, mode: int) -> None:
    if path.exists():
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ConvergenceError("runtime authority directory metadata is invalid")
        os.chown(path, 0, gid)
        os.chmod(path, mode)
        return
    path.mkdir(mode=0o700)
    os.chown(path, 0, gid)
    os.chmod(path, mode)


def _ensure_runtime_parents(domain: Domain, group_gid: int, sha: str, sandbox: str) -> None:
    # The public ancestors allow traversal but not listing. Sandbox and SHA
    # leaves are readable only by the stable per-sandbox group.
    _ensure_directory(domain.runtime_root.parent, gid=0, mode=0o711)
    _ensure_directory(domain.runtime_root, gid=0, mode=0o711)
    sandbox_root = domain.runtime_root / sandbox
    _ensure_directory(sandbox_root, gid=group_gid, mode=0o2750)
    _ensure_directory(sandbox_root / sha, gid=group_gid, mode=0o2750)


def _require_directory(path: Path, *, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ConvergenceError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ConvergenceError(f"{label} must be a non-symlink directory")


def _normalize_candidate(path: Path, shared_gid: int) -> None:
    for root, directories, files in os.walk(path, topdown=False, followlinks=False):
        root_path = Path(root)
        for name in files:
            item = root_path / name
            metadata = item.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                os.lchown(item, 0, shared_gid)
                continue
            mode = 0o750 if metadata.st_mode & 0o111 else 0o640
            os.chown(item, 0, shared_gid)
            os.chmod(item, mode)
        for name in directories:
            item = root_path / name
            if item.is_symlink():
                os.lchown(item, 0, shared_gid)
            else:
                os.chown(item, 0, shared_gid)
                os.chmod(item, 0o2750)
    os.chown(path, 0, shared_gid)
    os.chmod(path, 0o2750)


def _rename_noreplace(source: Path, target: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ConvergenceError("atomic no-replace publication is unavailable")
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(target),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error == 17:
            raise FileExistsError(target)
        raise ConvergenceError("atomic no-replace candidate publication failed")


def _verify_candidate(path: Path, identity: CandidateIdentity, shared_gid: int) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ConvergenceError("published candidate is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != shared_gid
        or stat.S_IMODE(metadata.st_mode) != 0o2750
    ):
        raise ConvergenceError("published candidate metadata is invalid")
    for root, directories, files in os.walk(path, followlinks=False):
        root_path = Path(root)
        for name in (*directories, *files):
            item_metadata = (root_path / name).lstat()
            if item_metadata.st_uid != 0 or item_metadata.st_gid != shared_gid:
                raise ConvergenceError("published candidate ownership is invalid")
            if not stat.S_ISLNK(item_metadata.st_mode) and (
                stat.S_IMODE(item_metadata.st_mode) & 0o022
            ):
                raise ConvergenceError("published candidate is group/world writable")
    head = _git_run("-C", str(path), "rev-parse", "--verify", "HEAD").stdout.strip()
    tree = _git_run(
        "-C",
        str(path),
        "rev-parse",
        "--verify",
        "HEAD^{tree}",
    ).stdout.strip()
    if head != identity.sha or tree != identity.tree:
        raise ConvergenceError("published candidate exact identity is invalid")
    _verify_candidate_raw_tree(path)
    return metadata


def _verify_candidate_raw_tree(path: Path) -> None:
    try:
        git_metadata = (path / ".git").lstat()
    except OSError as exc:
        raise ConvergenceError("candidate Git metadata is unavailable") from exc
    if stat.S_ISLNK(git_metadata.st_mode) or not stat.S_ISDIR(git_metadata.st_mode):
        raise ConvergenceError("candidate Git metadata is not self-contained")
    tree_rows = _git_run(
        "-C",
        str(path),
        "ls-tree",
        "-rz",
        "--full-tree",
        "HEAD",
    ).stdout.split("\0")
    expected: dict[str, tuple[str, str]] = {}
    expected_paths: set[str] = set()
    for row in tree_rows:
        if not row:
            continue
        header, separator, name = row.partition("\t")
        parts = header.split()
        if (
            not separator
            or len(parts) != 3
            or parts[1] != "blob"
            or parts[0] not in {"100644", "100755", "120000"}
            or _SHA_RE.fullmatch(parts[2]) is None
            or Path(name).is_absolute()
            or ".." in Path(name).parts
            or name in expected
        ):
            raise ConvergenceError("candidate commit tree contains an unsupported entry")
        expected[name] = (parts[0], parts[2])
        expected_paths.add(name)
        parent = Path(name).parent
        while parent != Path("."):
            expected_paths.add(parent.as_posix())
            parent = parent.parent

    index_rows = _git_run("-C", str(path), "ls-files", "--stage", "-z").stdout.split("\0")
    indexed: dict[str, tuple[str, str]] = {}
    for row in index_rows:
        if not row:
            continue
        header, separator, name = row.partition("\t")
        parts = header.split()
        if not separator or len(parts) != 3 or parts[2] != "0":
            raise ConvergenceError("candidate index contains an unsupported entry")
        indexed[name] = (parts[0], parts[1])
    flags = _git_run("-C", str(path), "ls-files", "-v", "-z").stdout.split("\0")
    if any(row and (len(row) < 3 or row[0] != "H" or row[1] != " ") for row in flags):
        raise ConvergenceError("candidate index contains hidden worktree flags")
    if indexed != expected:
        raise ConvergenceError("candidate index differs from the exact commit tree")

    actual_paths: set[str] = set()
    for root, directories, files in os.walk(path, topdown=True, followlinks=False):
        root_path = Path(root)
        if root_path == path and ".git" in directories:
            directories.remove(".git")
        for name in list(directories):
            item = root_path / name
            relative = item.relative_to(path).as_posix()
            actual_paths.add(relative)
            if item.is_symlink():
                directories.remove(name)
        for name in files:
            actual_paths.add((root_path / name).relative_to(path).as_posix())
    if actual_paths != expected_paths:
        raise ConvergenceError("candidate worktree contains extra or missing entries")

    for name, (mode, blob_sha) in expected.items():
        item = path / name
        metadata = item.lstat()
        if mode == "120000":
            if not stat.S_ISLNK(metadata.st_mode):
                raise ConvergenceError("candidate symlink differs from commit tree")
            content = os.fsencode(os.readlink(item))
        else:
            if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                raise ConvergenceError("candidate file differs from commit tree")
            executable = bool(stat.S_IMODE(metadata.st_mode) & 0o111)
            if executable != (mode == "100755"):
                raise ConvergenceError("candidate file mode differs from commit tree")
            content = item.read_bytes()
        actual_blob = hashlib.sha1(
            f"blob {len(content)}\0".encode() + content,
            usedforsecurity=False,
        ).hexdigest()
        if actual_blob != blob_sha:
            raise ConvergenceError("candidate tracked bytes differ from commit tree")


def _publish_candidate(
    source_repo: Path,
    target: Path,
    identity: CandidateIdentity,
    shared_gid: int,
) -> bool:
    if target.exists():
        _verify_candidate(target, identity, shared_gid)
        return False
    _require_directory(target.parent.parent, label="candidate domain root")
    _ensure_directory(target.parent, gid=shared_gid, mode=0o2750)
    temporary = target.parent / f".incoming-{target.name}-{uuid.uuid4().hex}"
    published = True
    try:
        _git_run(
            "clone",
            "--quiet",
            "--no-local",
            "--no-hardlinks",
            "--no-checkout",
            str(source_repo.resolve(strict=True)),
            str(temporary),
        )
        _git_run("-C", str(temporary), "checkout", "--quiet", "--detach", identity.sha)
        _git_run("-C", str(temporary), "remote", "remove", "origin")
        _normalize_candidate(temporary, shared_gid)
        try:
            _rename_noreplace(temporary, target)
        except FileExistsError:
            shutil.rmtree(temporary)
            published = False
        _verify_candidate(target, identity, shared_gid)
        return published
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def inspect_candidate_local(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
) -> dict[str, object]:
    _require_root()
    paths = runtime_paths(domain, sandbox, identity.sha)
    candidate = _verify_candidate(paths.candidate, identity, config.shared_gid)
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "inspect-candidate",
        "domain": domain.name,
        "hostname": _hostname(),
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "candidate_inode": candidate.st_ino,
        "candidate_device": candidate.st_dev,
        "candidate_uid": candidate.st_uid,
        "candidate_gid": candidate.st_gid,
        "candidate_mode": f"{stat.S_IMODE(candidate.st_mode):04o}",
        "candidate_clean": True,
    }


def _peer_candidate_readback(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
) -> list[dict[str, object]]:
    local = _hostname()
    reports: list[dict[str, object]] = []
    for peer in domain.peers:
        if peer.hostname == local:
            report = inspect_candidate_local(config, domain, sandbox, identity)
        else:
            report = _authority_check(
                action="inspect-candidate",
                node=peer.ssh_target,
                domain=domain.name,
                sandbox=sandbox,
                identity=identity,
            )
        if (
            report.get("hostname") != peer.hostname
            or report.get("candidate_uid") != 0
            or report.get("candidate_gid") != config.shared_gid
            or report.get("candidate_mode") != "2750"
            or report.get("candidate_clean") is not True
            or report.get("candidate_sha") != identity.sha
            or report.get("candidate_tree") != identity.tree
        ):
            raise ConvergenceError("candidate peer readback does not match the publication")
        reports.append(report)
    if len({item.get("candidate_inode") for item in reports}) != 1:
        raise ConvergenceError("NFS candidate peer inode identity is inconsistent")
    return reports


def _materialization_path(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    sha: str,
) -> Path:
    return config.state_root / "materializations" / domain.name / sandbox / f"{sha}.json"


def converge_materialize(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    source_bundle: Path,
    identity: CandidateIdentity,
) -> dict[str, object]:
    _require_root()
    if _hostname() != domain.publisher_hostname:
        raise ConvergenceError("materialize must run on the declared NFS domain publisher")
    with _transaction_lock(config, domain.name, sandbox, identity.sha):
        path = _materialization_path(config, domain, sandbox, identity.sha)
        journal: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "operation": "materialize",
            "status": "prepared",
            "domain": domain.name,
            "sandbox": sandbox,
            "candidate_sha": identity.sha,
            "candidate_tree": identity.tree,
            "candidate_created": False,
        }
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ConvergenceError("candidate materialization journal is invalid") from exc
            if (
                not isinstance(existing, dict)
                or existing.get("schema_version") != SCHEMA_VERSION
                or existing.get("operation") != "materialize"
                or existing.get("domain") != domain.name
                or existing.get("sandbox") != sandbox
                or existing.get("candidate_sha") != identity.sha
                or existing.get("candidate_tree") != identity.tree
                or existing.get("status") not in {"prepared", "published", "committed"}
            ):
                raise ConvergenceError("candidate materialization journal binding is invalid")
            journal = cast(dict[str, object], existing)
        else:
            _write_json(path, journal)
        paths = runtime_paths(domain, sandbox, identity.sha)
        if journal["status"] == "prepared":
            _require_directory(paths.candidate.parent.parent, label="candidate domain root")
            created = _publish_candidate(
                source_bundle,
                paths.candidate,
                identity,
                config.shared_gid,
            )
            journal["candidate_created"] = created
            journal["status"] = "published"
            _write_json(path, journal)
        else:
            _verify_candidate(paths.candidate, identity, config.shared_gid)
        reports = _peer_candidate_readback(config, domain, sandbox, identity)
        journal["status"] = "committed"
        journal["peer_hostnames"] = [item["hostname"] for item in reports]
        journal["candidate_inode"] = reports[0]["candidate_inode"]
        _write_json(path, journal)
        return {
            "schema_version": SCHEMA_VERSION,
            "operation": "materialize",
            "mode": "applied",
            "domain": domain.name,
            "sandbox": sandbox,
            "candidate_sha": identity.sha,
            "candidate_tree": identity.tree,
            "candidate_path": str(paths.candidate),
            "candidate_created": journal["candidate_created"],
            "candidate_inode": reports[0]["candidate_inode"],
            "peer_hostnames": journal["peer_hostnames"],
            "fleet_attestation": "not-read",
            "runtime_env": "not-written",
            "domain_attestation": "not-written",
            "journal": str(path),
        }


def _atomic_env_write(source: Path, target: Path, owner_uid: int, owner_gid: int) -> None:
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(source.read_bytes())
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chown(temporary, owner_uid, owner_gid)
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _write_json(path: Path, payload: Mapping[str, object], mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _create_receipt(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
    env_path: Path,
) -> tuple[Path, dict[str, object]]:
    if config.state_root.exists():
        metadata = config.state_root.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
        ):
            raise ConvergenceError("local transaction state root is invalid")
        os.chmod(config.state_root, 0o700)
    else:
        config.state_root.mkdir(parents=True, mode=0o700)
        os.chown(config.state_root, 0, 0)
        os.chmod(config.state_root, 0o700)
    receipt_dir = (
        config.state_root / domain.name / sandbox / identity.sha / f"transaction-{uuid.uuid4().hex}"
    )
    receipt_dir.mkdir(parents=True, mode=0o700)
    os.chown(receipt_dir, 0, 0)
    os.chmod(receipt_dir, 0o700)
    previous = env_path.exists()
    previous_env_sha256: str | None = None
    if previous:
        backup = receipt_dir / "previous.env"
        previous_bytes = env_path.read_bytes()
        backup.write_bytes(previous_bytes)
        os.chown(backup, 0, 0)
        os.chmod(backup, 0o600)
        previous_env_sha256 = hashlib.sha256(previous_bytes).hexdigest()
    manifest_path, signature_path = _attestation_paths(domain.name, sandbox, identity.sha)
    prior_attestation = manifest_path.exists() or signature_path.exists()
    if manifest_path.exists() != signature_path.exists():
        raise ConvergenceError("existing domain attestation pair is incomplete")
    previous_attestation_payload_sha256: str | None = None
    previous_attestation_signature_sha256: str | None = None
    if prior_attestation:
        previous_manifest_bytes = manifest_path.read_bytes()
        previous_signature_bytes = signature_path.read_bytes()
        _atomic_bytes(
            receipt_dir / "previous-attestation.json",
            previous_manifest_bytes,
            mode=0o600,
        )
        _atomic_bytes(
            receipt_dir / "previous-attestation.sig",
            previous_signature_bytes,
            mode=0o600,
        )
        previous_attestation_payload_sha256 = hashlib.sha256(
            previous_manifest_bytes,
        ).hexdigest()
        previous_attestation_signature_sha256 = hashlib.sha256(
            previous_signature_bytes,
        ).hexdigest()
    receipt: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "status": "prepared",
        "domain": domain.name,
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "candidate_created": False,
        "env_previously_existed": previous,
        "previous_env_sha256": previous_env_sha256,
        "published_env_sha256": None,
        "attestation_previously_existed": prior_attestation,
        "previous_attestation_payload_sha256": previous_attestation_payload_sha256,
        "previous_attestation_signature_sha256": previous_attestation_signature_sha256,
        "env_values": "redacted",
    }
    receipt_path = receipt_dir / "receipt.json"
    _write_json(receipt_path, receipt)
    return receipt_path, receipt


@contextmanager
def _transaction_lock(
    config: RuntimeConfig,
    domain: str,
    sandbox: str,
    sha: str,
) -> Iterator[None]:
    if not config.state_root.exists():
        config.state_root.mkdir(parents=True, mode=0o700)
        os.chown(config.state_root, 0, 0)
        os.chmod(config.state_root, 0o700)
    state_metadata = config.state_root.lstat()
    if (
        stat.S_ISLNK(state_metadata.st_mode)
        or not stat.S_ISDIR(state_metadata.st_mode)
        or state_metadata.st_uid != 0
        or state_metadata.st_gid != 0
    ):
        raise ConvergenceError("transaction state root metadata is invalid")
    os.chmod(config.state_root, 0o700)
    lock_root = config.state_root / "locks" / domain / sandbox
    lock_root.mkdir(parents=True, exist_ok=True)
    os.chown(lock_root, 0, 0)
    os.chmod(lock_root, 0o700)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lock_root / f"{sha}.lock", flags, 0o600)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0:
            raise ConvergenceError("transaction lock metadata is invalid")
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ConvergenceError("another publication transaction is active") from exc
        yield
    finally:
        os.close(fd)


def inspect_local(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
) -> dict[str, object]:
    _require_root()
    group = config.sandbox_groups[sandbox]
    if _service_identity_status(group) != "ok":
        raise ConvergenceError("stable sandbox service identity is not converged")
    batch_identity = _batch_identity(group)
    paths = runtime_paths(domain, sandbox, identity.sha)
    candidate = _verify_candidate(paths.candidate, identity, config.shared_gid)
    try:
        env = paths.env.lstat()
    except OSError as exc:
        raise ConvergenceError("published worker env is unavailable") from exc
    if (
        stat.S_ISLNK(env.st_mode)
        or not stat.S_ISREG(env.st_mode)
        or env.st_uid != batch_identity.pw_uid
        or env.st_gid != batch_identity.pw_gid
        or stat.S_IMODE(env.st_mode) != 0o600
    ):
        raise ConvergenceError("published worker env metadata is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "inspect-local",
        "domain": domain.name,
        "hostname": _hostname(),
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "candidate_inode": candidate.st_ino,
        "candidate_device": candidate.st_dev,
        "candidate_uid": candidate.st_uid,
        "candidate_gid": candidate.st_gid,
        "candidate_mode": f"{stat.S_IMODE(candidate.st_mode):04o}",
        "env_inode": env.st_ino,
        "env_device": env.st_dev,
        "env_uid": env.st_uid,
        "env_gid": env.st_gid,
        "env_mode": f"{stat.S_IMODE(env.st_mode):04o}",
        "env_values": "not-read",
    }


def _peer_readback(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
) -> list[dict[str, object]]:
    local = _hostname()
    reports: list[dict[str, object]] = []
    for peer in domain.peers:
        if peer.hostname == local:
            report = inspect_local(config, domain, sandbox, identity)
        else:
            report = _authority_check(
                action="inspect-local",
                node=peer.ssh_target,
                domain=domain.name,
                sandbox=sandbox,
                identity=identity,
            )
        if report.get("hostname") != peer.hostname:
            raise ConvergenceError("peer readback hostname does not match inventory")
        reports.append(report)
    candidate_inodes = {item.get("candidate_inode") for item in reports}
    env_inodes = {item.get("env_inode") for item in reports}
    if len(candidate_inodes) != 1 or len(env_inodes) != 1:
        raise ConvergenceError("NFS peer readback inode identity is inconsistent")
    return reports


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def _fingerprint(value: object, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ConvergenceError(f"{label} is invalid")
    return value


def _fleet_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ConvergenceError(f"{label} is invalid")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ConvergenceError(f"{label} must be UTC second-level Z time") from exc


def _listener_ports(group: SandboxGroup) -> dict[str, int]:
    return {
        "control-plane": group.upstream_control_plane_port,
        "gateway": group.upstream_gateway_port,
        "minio": group.upstream_minio_port,
    }


def _fleet_attestation_path(sandbox: str, sha: str) -> Path:
    return _FLEET_ATTESTATION_ROOT / sandbox / sha / "fleet.json"


def _read_fleet_attestation_bytes(
    sandbox: str,
    sha: str,
    seed: Path | None = None,
) -> bytes:
    path = _fleet_attestation_path(sandbox, sha) if seed is None else seed
    if seed is None and _hostname() != _COLLECTOR_HOSTNAME:
        raise ConvergenceError("publisher requires a transported fleet attestation seed")
    try:
        metadata = path.lstat()
        content = path.read_bytes()
    except OSError as exc:
        raise ConvergenceError("remote-link fleet attestation is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or not content
        or len(content) > (1 << 20)
    ):
        raise ConvergenceError("remote-link fleet attestation metadata is invalid")
    return content


def _verify_fleet_attestation(
    config: RuntimeConfig,
    sandbox: str,
    sha: str,
    content: bytes,
    *,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, object]]:
    try:
        fleet = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ConvergenceError("remote-link fleet attestation is invalid") from exc
    if not isinstance(fleet, dict):
        raise ConvergenceError("remote-link fleet attestation is invalid")
    _exact_keys(
        fleet,
        {
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
        },
        "remote-link fleet attestation",
    )
    if (
        fleet["schema_version"] != SCHEMA_VERSION
        or fleet["sandbox"] != sandbox
        or fleet["candidate_sha"] != sha
        or fleet["eligible_nodes"] != list(_INFRASTRUCTURE_LINK_NODES)
    ):
        raise ConvergenceError("remote-link fleet identity is invalid")
    digest = fleet.pop("payload_sha256")
    expected_digest = "sha256:" + hashlib.sha256(_canonical_json(fleet)).hexdigest()
    fleet["payload_sha256"] = digest
    if digest != expected_digest:
        raise ConvergenceError("remote-link fleet payload digest is invalid")
    generated_at = _fleet_timestamp(fleet["generated_at"], "fleet generated_at")
    expires_at = _fleet_timestamp(fleet["expires_at"], "fleet expires_at")
    if (
        expires_at - generated_at != _ATTESTATION_TTL
        or generated_at > now + timedelta(seconds=30)
        or generated_at < now - timedelta(seconds=60)
        or expires_at <= now
    ):
        raise ConvergenceError("remote-link fleet attestation is stale or expired")

    group = config.sandbox_groups[sandbox]
    listener_ports = _listener_ports(group)
    client_uri = f"spiffe://loom/developer-sandbox/{sandbox}/candidate/{sha}/worker"
    bundle = fleet["bundle_generation"]
    server = fleet["server"]
    nodes = fleet["nodes"]
    if not all(isinstance(item, dict) for item in (bundle, server, nodes)):
        raise ConvergenceError("remote-link fleet sections are invalid")
    _exact_keys(
        bundle,
        {"candidate_sha", "ca_fingerprint", "client_uri_san"},
        "remote-link bundle generation",
    )
    ca_fingerprint = _fingerprint(bundle["ca_fingerprint"], "fleet CA fingerprint")
    if bundle != {
        "candidate_sha": sha,
        "ca_fingerprint": ca_fingerprint,
        "client_uri_san": client_uri,
    }:
        raise ConvergenceError("remote-link bundle generation is invalid")
    _exact_keys(
        server,
        {
            "node",
            "address",
            "unit",
            "unit_active",
            "active_candidate_sha",
            "ca_fingerprint",
            "server_cert_fingerprint",
            "client_uri_san",
            "services",
        },
        "remote-link server",
    )
    _fingerprint(server["server_cert_fingerprint"], "server certificate fingerprint")
    if (
        server["node"] != "oldlab-2"
        or server["address"] != "192.168.50.14"
        or server["unit"] != f"loom-developer-sandbox-link@{sandbox}.service"
        or server["unit_active"] is not True
        or server["active_candidate_sha"] != sha
        or server["ca_fingerprint"] != ca_fingerprint
        or server["client_uri_san"] != client_uri
        or not isinstance(server["services"], dict)
        or set(server["services"]) != set(_SERVICE_HEALTH_PATHS)
    ):
        raise ConvergenceError("remote-link server identity is invalid")
    for service, health_path in _SERVICE_HEALTH_PATHS.items():
        row = server["services"][service]
        if not isinstance(row, dict):
            raise ConvergenceError("remote-link server service is invalid")
        _exact_keys(
            row,
            {
                "listener_port",
                "target_host",
                "target_port",
                "health_path",
                "tls_version",
                "status",
            },
            "remote-link server service",
        )
        if row != {
            "listener_port": listener_ports[service],
            "target_host": "127.0.0.1",
            "target_port": _SANDBOX_TARGET_PORTS[sandbox][service],
            "health_path": health_path,
            "tls_version": "TLSv1.3",
            "status": "active",
        }:
            raise ConvergenceError("remote-link server service contract is invalid")

    if set(nodes) != set(_INFRASTRUCTURE_LINK_NODES):
        raise ConvergenceError("remote-link fleet node set is incomplete")
    secret_files = {
        name: {"present": True, "uid": 0, "gid": 0, "mode": "0600"}
        for name in (
            "worker-token",
            "minio-access-key",
            "minio-secret-key",
            "client-key.pem",
        )
    }
    service_readback = {
        name: {"listener_port": port, "health": "ok"} for name, port in listener_ports.items()
    }
    for node in _INFRASTRUCTURE_LINK_NODES:
        row = nodes[node]
        if not isinstance(row, dict):
            raise ConvergenceError("remote-link fleet node is invalid")
        _exact_keys(
            row,
            {
                "node",
                "candidate_sha",
                "route",
                "tls_version",
                "client_uri_san",
                "ca_fingerprint",
                "client_cert_fingerprint",
                "secret_files",
                "services",
            },
            "remote-link fleet node",
        )
        _fingerprint(row["client_cert_fingerprint"], "client certificate fingerprint")
        if row != {
            "node": node,
            "candidate_sha": sha,
            "route": {"destination": "192.168.50.14", "status": "ok"},
            "tls_version": "TLSv1.3",
            "client_uri_san": client_uri,
            "ca_fingerprint": ca_fingerprint,
            "client_cert_fingerprint": row["client_cert_fingerprint"],
            "secret_files": secret_files,
            "services": service_readback,
        }:
            raise ConvergenceError("remote-link fleet node contract is invalid")
    reference: dict[str, object] = {
        "path": str(_fleet_attestation_path(sandbox, sha)),
        "payload_sha256": digest,
        "generated_at": fleet["generated_at"],
        "expires_at": fleet["expires_at"],
    }
    return fleet, reference


def _read_and_verify_fleet(
    config: RuntimeConfig,
    sandbox: str,
    sha: str,
    *,
    now: datetime,
    seed: Path | None = None,
) -> tuple[dict[str, Any], dict[str, object]]:
    return _verify_fleet_attestation(
        config,
        sandbox,
        sha,
        _read_fleet_attestation_bytes(sandbox, sha, seed),
        now=now,
    )


def _generation_path(config: RuntimeConfig, domain: str, sandbox: str) -> Path:
    return config.state_root / "generations" / domain / f"{sandbox}.json"


def _next_generation(config: RuntimeConfig, domain: str, sandbox: str) -> int:
    path = _generation_path(config, domain, sandbox)
    generation = 0
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            generation = payload["generation"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ConvergenceError("publisher generation state is invalid") from exc
        if type(generation) is not int or generation < 0:
            raise ConvergenceError("publisher generation state is invalid")
    generation += 1
    _write_json(path, {"schema_version": SCHEMA_VERSION, "generation": generation})
    return generation


def _attestation_paths(
    domain: str,
    sandbox: str,
    sha: str,
) -> tuple[Path, Path]:
    root = _PUBLIC_ATTESTATION_ROOT / sandbox / sha
    return root / f"{domain}.json", root / f"{domain}.sig"


def export_domain_attestation(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
) -> dict[str, object]:
    _require_root()
    if _hostname() != domain.publisher_hostname:
        raise ConvergenceError("attestation export must run on the declared publisher")
    manifest_path, signature_path = _attestation_paths(
        domain.name,
        sandbox,
        identity.sha,
    )
    payloads: list[bytes] = []
    for path in (manifest_path, signature_path):
        try:
            metadata = path.lstat()
            payload = path.read_bytes()
        except OSError as exc:
            raise ConvergenceError("domain attestation export is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o644
            or metadata.st_nlink != 1
            or not payload
            or len(payload) > (1 << 20)
        ):
            raise ConvergenceError("domain attestation export metadata is invalid")
        payloads.append(payload)
    manifest_bytes, signature_bytes = payloads
    try:
        manifest = json.loads(manifest_bytes)
        decoded_signature = base64.b64decode(signature_bytes.strip(), validate=True)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ConvergenceError("domain attestation export payload is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("kind") != "loom.developer-runtime-domain-attestation"
        or manifest.get("domain") != domain.name
        or manifest.get("sandbox") != sandbox
        or not isinstance(manifest.get("candidate"), dict)
        or manifest["candidate"].get("sha") != identity.sha
        or manifest["candidate"].get("tree") != identity.tree
        or not isinstance(manifest.get("publisher"), dict)
        or manifest["publisher"].get("hostname") != domain.publisher_hostname
        or len(decoded_signature) != 64
    ):
        raise ConvergenceError("domain attestation export identity is invalid")
    digest = manifest.get("payload_sha256")
    unsigned = dict(manifest)
    unsigned.pop("payload_sha256", None)
    if (
        not isinstance(digest, str)
        or hashlib.sha256(_canonical_json(unsigned)).hexdigest() != digest
    ):
        raise ConvergenceError("domain attestation export digest is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "export-domain-attestation",
        "domain": domain.name,
        "hostname": _hostname(),
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "manifest_path": str(manifest_path),
        "signature_path": str(signature_path),
        "manifest_base64": base64.b64encode(manifest_bytes).decode("ascii"),
        "signature_base64": base64.b64encode(signature_bytes).decode("ascii"),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "signature_sha256": hashlib.sha256(signature_bytes).hexdigest(),
    }


def _runtime_proof_artifact_identity(
    artifact_id: str,
) -> tuple[str, str, str, str]:
    fields = artifact_id.split("/")
    if (
        len(fields) != 7
        or fields[:2] != ["runtime-proof", "v1"]
        or fields[2] not in ALLOWED_SANDBOXES
        or _SHA_RE.fullmatch(fields[3]) is None
        or _SHA_RE.fullmatch(fields[4]) is None
        or fields[5] != "artifact"
        or fields[6] not in _RUNTIME_PROOF_ARTIFACT_NAMES
    ):
        raise ConvergenceError("runtime proof artifact identity is invalid")
    return fields[2], fields[3], fields[4], fields[6]


def _runtime_proof_artifact_path(
    artifact_name: str,
    sandbox: str,
    candidate_sha: str,
) -> tuple[Path, int, str, str, str]:
    if artifact_name == "combined.json":
        return (
            _COMBINED_ROOT / sandbox / candidate_sha / artifact_name,
            0o600,
            "oldlab",
            "oldlab-2",
            _COLLECTOR_HOSTNAME,
        )
    if artifact_name == "fleet.json":
        return (
            _fleet_attestation_path(sandbox, candidate_sha),
            0o600,
            "oldlab",
            "oldlab-2",
            _COLLECTOR_HOSTNAME,
        )
    domain_name, suffix = artifact_name.split(".", 1)
    if domain_name not in ALLOWED_DOMAINS or suffix not in {"json", "sig", "pub"}:
        raise ConvergenceError("runtime proof artifact identity is invalid")
    domain = "oldlab" if domain_name == "oldlab" else "gb10"
    node = "oldlab-1" if domain == "oldlab" else "trt-gb10-1"
    hostname = "trt-eai-oldlab-1" if domain == "oldlab" else "gx10-01c7"
    if suffix == "pub":
        path = _ATTESTATION_KEY_ROOT / artifact_name
    else:
        path = _PUBLIC_ATTESTATION_ROOT / sandbox / candidate_sha / artifact_name
    return path, 0o644, domain, node, hostname


def _read_runtime_proof_artifact(path: Path, *, mode: int) -> bytes:
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise ConvergenceError("runtime proof artifact is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        content = bytearray()
        while len(content) <= _RUNTIME_PROOF_ARTIFACT_MAX_BYTES:
            chunk = os.read(
                descriptor,
                min(
                    65_536,
                    _RUNTIME_PROOF_ARTIFACT_MAX_BYTES + 1 - len(content),
                ),
            )
            if not chunk:
                break
            content.extend(chunk)
        after = os.fstat(descriptor)
        if (
            len(content) > _RUNTIME_PROOF_ARTIFACT_MAX_BYTES
            or not content
            or stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(lexical.st_mode) != mode
            or stat.S_IMODE(opened.st_mode) != mode
            or lexical.st_uid != 0
            or lexical.st_gid != 0
            or opened.st_uid != 0
            or opened.st_gid != 0
            or lexical.st_nlink != 1
            or opened.st_nlink != 1
            or (lexical.st_dev, lexical.st_ino) != (opened.st_dev, opened.st_ino)
            or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            )
        ):
            raise ConvergenceError("runtime proof artifact metadata is invalid")
        return bytes(content)
    finally:
        os.close(descriptor)


def export_runtime_proof_artifact(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
    artifact_id: str,
) -> dict[str, object]:
    _require_root()
    bound_sandbox, bound_sha, bound_tree, artifact_name = _runtime_proof_artifact_identity(
        artifact_id
    )
    path, mode, required_domain, node, hostname = _runtime_proof_artifact_path(
        artifact_name,
        sandbox,
        identity.sha,
    )
    if (
        (bound_sandbox, bound_sha, bound_tree) != (sandbox, identity.sha, identity.tree)
        or domain.name != required_domain
        or _hostname() != hostname
    ):
        raise ConvergenceError("runtime proof artifact binding is invalid")
    content = _read_runtime_proof_artifact(path, mode=mode)
    if artifact_name.endswith(".json"):
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ConvergenceError("runtime proof artifact JSON is invalid") from exc
        if (
            not isinstance(payload, dict)
            or content != _canonical_json(payload) + b"\n"
            or (
                artifact_name == "combined.json"
                and (
                    payload.get("kind") != "loom.developer-runtime-combined-activation"
                    or payload.get("sandbox") != sandbox
                    or payload.get("candidate_sha") != identity.sha
                    or payload.get("candidate_tree") != identity.tree
                    or not isinstance(payload.get("collector"), dict)
                    or payload["collector"].get("hostname") != hostname
                )
            )
            or (artifact_name == "fleet.json" and payload.get("candidate_sha") != identity.sha)
            or (
                artifact_name in {"oldlab.json", "gb10.json"}
                and (
                    payload.get("kind") != "loom.developer-runtime-domain-attestation"
                    or payload.get("domain") != required_domain
                    or payload.get("sandbox") != sandbox
                    or not isinstance(payload.get("candidate"), dict)
                    or payload["candidate"].get("sha") != identity.sha
                    or payload["candidate"].get("tree") != identity.tree
                    or not isinstance(payload.get("publisher"), dict)
                    or payload["publisher"].get("hostname") != hostname
                )
            )
        ):
            raise ConvergenceError("runtime proof artifact identity is invalid")
    elif artifact_name.endswith(".sig"):
        try:
            signature = base64.b64decode(content.strip(), validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ConvergenceError("runtime proof signature artifact is invalid") from exc
        if content != base64.b64encode(signature) + b"\n" or len(signature) != 64:
            raise ConvergenceError("runtime proof signature artifact is invalid")
    else:
        with tempfile.TemporaryDirectory() as temporary:
            public_key = Path(temporary) / "public.pem"
            public_key.write_bytes(content)
            description = _run(
                (
                    "openssl",
                    "pkey",
                    "-pubin",
                    "-in",
                    str(public_key),
                    "-text_pub",
                    "-noout",
                ),
            ).stdout
        if "ED25519" not in description.upper():
            raise ConvergenceError("runtime proof public key artifact is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "export-runtime-proof-artifact",
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "node": node,
        "hostname": hostname,
        "domain": required_domain,
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "content_size": len(content),
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "content_base64": base64.b64encode(content).decode("ascii"),
    }


def _sign_bytes(content: bytes, private_key: Path, public_key: Path) -> bytes:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        payload_path = root / "payload.json"
        signature_path = root / "payload.sig"
        payload_path.write_bytes(content)
        _run(
            (
                "openssl",
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                str(private_key),
                "-in",
                str(payload_path),
                "-out",
                str(signature_path),
            ),
        )
        _run(
            (
                "openssl",
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                str(public_key),
                "-in",
                str(payload_path),
                "-sigfile",
                str(signature_path),
            ),
        )
        return signature_path.read_bytes()


def _emit_attestation(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    identity: CandidateIdentity,
    reports: Sequence[Mapping[str, object]],
    *,
    fleet_attestation_seed: Path | None = None,
) -> dict[str, object]:
    private_key, public_key = _attestation_key_paths(domain.name)
    key_id = _key_id(public_key)
    try:
        private_metadata = private_key.lstat()
    except OSError as exc:
        raise ConvergenceError("attestation private key is unavailable") from exc
    if (
        stat.S_ISLNK(private_metadata.st_mode)
        or not stat.S_ISREG(private_metadata.st_mode)
        or private_metadata.st_uid != 0
        or private_metadata.st_gid != 0
        or stat.S_IMODE(private_metadata.st_mode) != 0o600
    ):
        raise ConvergenceError("attestation private key metadata is invalid")
    now = datetime.now(UTC)
    _, fleet_reference = _read_and_verify_fleet(
        config,
        sandbox,
        identity.sha,
        now=now,
        seed=fleet_attestation_seed,
    )
    generation = _next_generation(config, domain.name, sandbox)
    group = config.sandbox_groups[sandbox]
    paths = runtime_paths(domain, sandbox, identity.sha)
    peer_rows = [
        {
            "hostname": report["hostname"],
            "candidate_inode": report["candidate_inode"],
            "env_inode": report["env_inode"],
            "env_uid": report["env_uid"],
            "env_gid": report["env_gid"],
            "result": "verified",
        }
        for report in reports
    ]
    bundle_root = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{identity.sha}"
    expires_at = (now + _ATTESTATION_TTL).isoformat()
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-runtime-domain-attestation",
        "domain": domain.name,
        "sandbox": sandbox,
        "candidate": {
            "sha": identity.sha,
            "tree": identity.tree,
            "path": str(paths.candidate),
            "uid": 0,
            "gid": config.shared_gid,
            "group": config.shared_group,
            "mode": "2750",
        },
        "runtime_env": {
            "schema_version": SCHEMA_VERSION,
            "path": str(paths.env),
            "uid": group.uid,
            "gid": group.gid,
            "user": group.member,
            "mode": "0600",
            "candidate_sha": identity.sha,
            "worker_pool_name": domain.worker_pool_name,
            "worker_max_concurrent": domain.worker_max_concurrent,
            "capacity_policy_source": domain.capacity_policy_source,
            "local_urls": {
                "control-plane": "http://sandbox-link:8080",
                "gateway": "http://sandbox-link:9100",
                "minio": "http://sandbox-link:9000",
            },
            "oldlab2_upstreams": {
                service: f"https://192.168.50.14:{port}"
                for service, port in _listener_ports(group).items()
            },
            "host_references": {
                "worker-token": f"{bundle_root}/worker-token",
                "minio-access-key": f"{bundle_root}/minio-access-key",
                "minio-secret-key": f"{bundle_root}/minio-secret-key",
                "ca": f"{bundle_root}/ca.pem",
                "cert": f"{bundle_root}/client.pem",
                "key": f"{bundle_root}/client-key.pem",
            },
        },
        "fleet_attestation": fleet_reference,
        "publisher": {
            "hostname": domain.publisher_hostname,
            "generation": generation,
            "published_at": now.isoformat(),
            "expires_at": expires_at,
            "signature_algorithm": "ed25519",
            "key_id": key_id,
        },
        "eligible_peers": peer_rows,
    }
    payload_digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    payload["payload_sha256"] = payload_digest
    encoded = _canonical_json(payload) + b"\n"
    signature = _sign_bytes(encoded, private_key, public_key)
    manifest_path, signature_path = _attestation_paths(
        domain.name,
        sandbox,
        identity.sha,
    )
    for directory in (
        _PUBLIC_ATTESTATION_ROOT,
        _PUBLIC_ATTESTATION_ROOT / sandbox,
        manifest_path.parent,
    ):
        _ensure_directory(directory, gid=0, mode=0o755)
    _atomic_bytes(manifest_path, encoded, mode=0o644, parent_mode=0o755)
    _atomic_bytes(
        signature_path,
        base64.b64encode(signature) + b"\n",
        mode=0o644,
        parent_mode=0o755,
    )
    return {
        "path": str(manifest_path),
        "signature_path": str(signature_path),
        "payload_sha256": payload_digest,
        "signature_sha256": hashlib.sha256(signature).hexdigest(),
        "generation": generation,
        "key_id": key_id,
        "expires_at": expires_at,
        "fleet_payload_sha256": fleet_reference["payload_sha256"],
    }


def _verify_signature(content: bytes, signature: bytes, public_key: Path) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        payload_path = root / "payload.json"
        signature_path = root / "payload.sig"
        payload_path.write_bytes(content)
        signature_path.write_bytes(signature)
        _run(
            (
                "openssl",
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                str(public_key),
                "-in",
                str(payload_path),
                "-sigfile",
                str(signature_path),
            ),
        )


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ConvergenceError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConvergenceError(f"{label} is invalid") from exc
    if parsed.tzinfo is None:
        raise ConvergenceError(f"{label} must include timezone")
    return parsed.astimezone(UTC)


def _verify_domain_attestation(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    sha: str,
    content: bytes,
    encoded_signature: bytes,
    *,
    now: datetime,
    fleet_reference: Mapping[str, object],
) -> dict[str, Any]:
    try:
        manifest = json.loads(content)
        signature = base64.b64decode(encoded_signature, validate=True)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ConvergenceError("domain attestation encoding is invalid") from exc
    if not isinstance(manifest, dict):
        raise ConvergenceError("domain attestation is invalid")
    _exact_keys(
        manifest,
        {
            "schema_version",
            "kind",
            "domain",
            "sandbox",
            "candidate",
            "runtime_env",
            "fleet_attestation",
            "publisher",
            "eligible_peers",
            "payload_sha256",
        },
        "domain attestation",
    )
    if (
        manifest["schema_version"] != SCHEMA_VERSION
        or manifest["kind"] != "loom.developer-runtime-domain-attestation"
        or manifest["domain"] != domain.name
        or manifest["sandbox"] != sandbox
    ):
        raise ConvergenceError("domain attestation identity is invalid")
    digest = manifest.pop("payload_sha256")
    if (
        not isinstance(digest, str)
        or hashlib.sha256(_canonical_json(manifest)).hexdigest() != digest
    ):
        raise ConvergenceError("domain attestation payload digest is invalid")
    manifest["payload_sha256"] = digest
    pinned_key = _TRUSTED_KEY_ROOT / f"{domain.name}.pub"
    key_id = _key_id(pinned_key)
    _verify_signature(content, signature, pinned_key)

    candidate = manifest["candidate"]
    runtime_env = manifest["runtime_env"]
    publisher = manifest["publisher"]
    peers = manifest["eligible_peers"]
    fleet_attestation = manifest["fleet_attestation"]
    if not all(
        isinstance(item, dict) for item in (candidate, runtime_env, publisher, fleet_attestation)
    ):
        raise ConvergenceError("domain attestation sections are invalid")
    if fleet_attestation != fleet_reference:
        raise ConvergenceError("domain attestation fleet binding is invalid")
    _exact_keys(
        candidate,
        {"sha", "tree", "path", "uid", "gid", "group", "mode"},
        "candidate attestation",
    )
    group = config.sandbox_groups[sandbox]
    paths = runtime_paths(domain, sandbox, sha)
    if (
        candidate["sha"] != sha
        or _SHA_RE.fullmatch(str(candidate["tree"])) is None
        or candidate["path"] != str(paths.candidate)
        or candidate["uid"] != 0
        or candidate["gid"] != config.shared_gid
        or candidate["group"] != config.shared_group
        or candidate["mode"] != "2750"
    ):
        raise ConvergenceError("candidate attestation contract is invalid")
    _exact_keys(
        runtime_env,
        {
            "schema_version",
            "path",
            "uid",
            "gid",
            "user",
            "mode",
            "candidate_sha",
            "worker_pool_name",
            "worker_max_concurrent",
            "capacity_policy_source",
            "local_urls",
            "oldlab2_upstreams",
            "host_references",
        },
        "runtime env attestation",
    )
    bundle_root = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{sha}"
    if (
        runtime_env["schema_version"] != SCHEMA_VERSION
        or runtime_env["path"] != str(paths.env)
        or runtime_env["uid"] != group.uid
        or runtime_env["gid"] != group.gid
        or runtime_env["user"] != group.member
        or runtime_env["mode"] != "0600"
        or runtime_env["candidate_sha"] != sha
        or runtime_env["worker_pool_name"] != domain.worker_pool_name
        or runtime_env["worker_max_concurrent"] != domain.worker_max_concurrent
        or runtime_env["capacity_policy_source"] != domain.capacity_policy_source
        or runtime_env["local_urls"]
        != {
            "control-plane": "http://sandbox-link:8080",
            "gateway": "http://sandbox-link:9100",
            "minio": "http://sandbox-link:9000",
        }
        or runtime_env["oldlab2_upstreams"]
        != {
            service: f"https://192.168.50.14:{port}"
            for service, port in _listener_ports(group).items()
        }
        or runtime_env["host_references"]
        != {
            "worker-token": f"{bundle_root}/worker-token",
            "minio-access-key": f"{bundle_root}/minio-access-key",
            "minio-secret-key": f"{bundle_root}/minio-secret-key",
            "ca": f"{bundle_root}/ca.pem",
            "cert": f"{bundle_root}/client.pem",
            "key": f"{bundle_root}/client-key.pem",
        }
    ):
        raise ConvergenceError("runtime env attestation contract is invalid")
    _exact_keys(
        publisher,
        {
            "hostname",
            "generation",
            "published_at",
            "expires_at",
            "signature_algorithm",
            "key_id",
        },
        "publisher attestation",
    )
    published_at = _parse_timestamp(publisher["published_at"], "published_at")
    expires_at = _parse_timestamp(publisher["expires_at"], "expires_at")
    if (
        publisher["hostname"] != domain.publisher_hostname
        or type(publisher["generation"]) is not int
        or publisher["generation"] < 1
        or publisher["signature_algorithm"] != "ed25519"
        or publisher["key_id"] != key_id
        or expires_at - published_at != _ATTESTATION_TTL
        or published_at > now + timedelta(seconds=30)
        or expires_at <= now
        or now - published_at > _ATTESTATION_TTL
    ):
        raise ConvergenceError("publisher attestation contract is invalid or stale")
    if not isinstance(peers, list):
        raise ConvergenceError("eligible peer attestation is invalid")
    expected_hosts = [peer.hostname for peer in domain.peers]
    actual_hosts: list[str] = []
    candidate_inodes: set[int] = set()
    env_inodes: set[int] = set()
    for item in peers:
        if not isinstance(item, dict):
            raise ConvergenceError("eligible peer attestation is invalid")
        _exact_keys(
            item,
            {
                "hostname",
                "candidate_inode",
                "env_inode",
                "env_uid",
                "env_gid",
                "result",
            },
            "eligible peer attestation",
        )
        if (
            not isinstance(item["candidate_inode"], int)
            or not isinstance(item["env_inode"], int)
            or item["env_uid"] != group.uid
            or item["env_gid"] != group.gid
            or item["result"] != "verified"
        ):
            raise ConvergenceError("eligible peer attestation is invalid")
        actual_hosts.append(item["hostname"])
        candidate_inodes.add(item["candidate_inode"])
        env_inodes.add(item["env_inode"])
    if actual_hosts != expected_hosts:
        raise ConvergenceError("eligible peer attestation is incomplete")
    if len(candidate_inodes) != 1 or len(env_inodes) != 1:
        raise ConvergenceError("eligible peer NFS inode readback is inconsistent")
    manifest["_verified_signature_sha256"] = hashlib.sha256(signature).hexdigest()
    return manifest


def _remote_attestation(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    sha: str,
    tree: str,
) -> tuple[bytes, bytes, str, str]:
    manifest_path, signature_path = _attestation_paths(domain.name, sandbox, sha)
    target = next(
        peer.ssh_target for peer in domain.peers if peer.hostname == domain.publisher_hostname
    )
    report = _authority_check(
        action="export-domain-attestation",
        node=target,
        domain=domain.name,
        sandbox=sandbox,
        identity=CandidateIdentity(sha, tree),
    )
    if (
        report.get("operation") != "export-domain-attestation"
        or report.get("domain") != domain.name
        or report.get("hostname") != domain.publisher_hostname
        or report.get("sandbox") != sandbox
        or report.get("candidate_sha") != sha
        or report.get("candidate_tree") != tree
        or report.get("manifest_path") != str(manifest_path)
        or report.get("signature_path") != str(signature_path)
        or not isinstance(report.get("manifest_base64"), str)
        or not isinstance(report.get("signature_base64"), str)
    ):
        raise ConvergenceError("domain attestation export returned invalid identity")
    try:
        content = base64.b64decode(report["manifest_base64"], validate=True)
        encoded_signature = base64.b64decode(report["signature_base64"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ConvergenceError("domain attestation export returned invalid payload") from exc
    if hashlib.sha256(content).hexdigest() != report.get("manifest_sha256") or hashlib.sha256(
        encoded_signature
    ).hexdigest() != report.get("signature_sha256"):
        raise ConvergenceError("domain attestation export digest is invalid")
    return content, encoded_signature.strip(), str(manifest_path), str(signature_path)


def pin_key_plan(domain: str, public_key: Path) -> dict[str, object]:
    try:
        metadata = public_key.lstat()
    except OSError as exc:
        raise ConvergenceError("public key source is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ConvergenceError("public key source must be a regular non-symlink file")
    _run(("openssl", "pkey", "-pubin", "-in", str(public_key), "-noout"))
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "pin-key",
        "mode": "plan",
        "domain": domain,
        "source_key_id": hashlib.sha256(public_key.read_bytes()).hexdigest(),
        "target": str(_TRUSTED_KEY_ROOT / f"{domain}.pub"),
    }


def pin_key(domain: str, public_key: Path) -> dict[str, object]:
    _require_root()
    if _hostname() != _COLLECTOR_HOSTNAME:
        raise ConvergenceError("trusted keys may only be pinned on the collector host")
    plan = pin_key_plan(domain, public_key)
    metadata = public_key.lstat()
    if metadata.st_uid != 0:
        raise ConvergenceError("executed public key source must be root-owned")
    target = _TRUSTED_KEY_ROOT / f"{domain}.pub"
    _atomic_bytes(target, public_key.read_bytes(), mode=0o644)
    if _key_id(target) != plan["source_key_id"]:
        raise ConvergenceError("pinned public key readback failed")
    result = dict(plan)
    result["mode"] = "applied"
    return result


def _collect_attestation_checks(
    config: RuntimeConfig,
    sandbox: str,
    sha: str,
    tree: str,
    *,
    execute: bool,
) -> dict[str, object]:
    now = datetime.now(UTC)
    _, fleet_reference = _read_and_verify_fleet(
        config,
        sandbox,
        sha,
        now=now,
    )
    verified: dict[str, dict[str, Any]] = {}
    inputs: dict[str, dict[str, object]] = {}
    for name in ALLOWED_DOMAINS:
        domain = config.domains[name]
        content, signature, manifest_path, signature_path = _remote_attestation(
            config,
            domain,
            sandbox,
            sha,
            tree,
        )
        verified[name] = _verify_domain_attestation(
            config,
            domain,
            sandbox,
            sha,
            content,
            signature,
            now=now,
            fleet_reference=fleet_reference,
        )
        publisher = verified[name]["publisher"]
        inputs[name] = {
            "manifest_path": manifest_path,
            "signature_path": signature_path,
            "payload_sha256": verified[name]["payload_sha256"],
            "signature_sha256": verified[name].pop("_verified_signature_sha256"),
            "key_id": publisher["key_id"],
            "generation": publisher["generation"],
            "published_at": publisher["published_at"],
            "expires_at": publisher["expires_at"],
        }
    trees = {item["candidate"]["tree"] for item in verified.values()}
    if trees != {tree}:
        raise ConvergenceError("OLDLAB and GB10 candidate trees do not match")
    generation_state = _COMBINED_ROOT / "generation-state.json"
    previous: dict[str, object] = {}
    if generation_state.exists():
        try:
            state = json.loads(generation_state.read_text(encoding="utf-8"))
            previous = state.get(sandbox, {})
        except (OSError, json.JSONDecodeError, AttributeError) as exc:
            raise ConvergenceError("combined generation state is invalid") from exc
        if not isinstance(previous, dict):
            raise ConvergenceError("combined generation state is invalid")
    for input_domain, item in inputs.items():
        prior_value = previous.get(
            input_domain,
            {"generation": 0, "payload_sha256": ""},
        )
        if not isinstance(prior_value, dict):
            raise ConvergenceError("combined generation state is invalid")
        prior = prior_value.get("generation")
        prior_digest = prior_value.get("payload_sha256")
        generation = item["generation"]
        digest = item["payload_sha256"]
        if (
            type(prior) is not int
            or not isinstance(prior_digest, str)
            or type(generation) is not int
            or not isinstance(digest, str)
            or generation < prior
            or (generation == prior and prior > 0 and digest != prior_digest)
        ):
            raise ConvergenceError("domain attestation generation regressed")
    input_expiries = [
        _parse_timestamp(item["expires_at"], "input expires_at") for item in inputs.values()
    ]
    expires_at = min(
        min(input_expiries),
        _fleet_timestamp(fleet_reference["expires_at"], "fleet expires_at"),
        now + _ATTESTATION_TTL,
    )
    combined: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-runtime-combined-activation",
        "sandbox": sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "collector": {
            "hostname": _COLLECTOR_HOSTNAME,
            "collected_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        "fleet_attestation": fleet_reference,
        "domains": inputs,
    }
    combined["payload_sha256"] = hashlib.sha256(_canonical_json(combined)).hexdigest()
    target = _COMBINED_ROOT / sandbox / sha / "combined.json"
    result = {
        "schema_version": SCHEMA_VERSION,
        "operation": "collect",
        "mode": "plan",
        "target": str(target),
        "combined": combined,
    }
    if not execute:
        return result
    new_state: dict[str, object] = {}
    if generation_state.exists():
        new_state = json.loads(generation_state.read_text(encoding="utf-8"))
    new_state[sandbox] = {
        domain: {
            "generation": item["generation"],
            "payload_sha256": item["payload_sha256"],
        }
        for domain, item in inputs.items()
    }
    _atomic_bytes(
        generation_state,
        _canonical_json(new_state) + b"\n",
        mode=0o600,
    )
    _atomic_bytes(target, _canonical_json(combined) + b"\n", mode=0o600)
    _fsync_file_and_parent(generation_state)
    _fsync_file_and_parent(target)
    result["mode"] = "applied"
    return result


def _require_capacity_root() -> None:
    capacity_root = _COMBINED_ROOT.parent
    try:
        capacity_metadata = capacity_root.lstat()
    except OSError as exc:
        raise ConvergenceError("shared capacity root is unavailable") from exc
    if (
        stat.S_ISLNK(capacity_metadata.st_mode)
        or not stat.S_ISDIR(capacity_metadata.st_mode)
        or capacity_metadata.st_uid != 0
        or capacity_metadata.st_gid != 0
        or stat.S_IMODE(capacity_metadata.st_mode) != 0o700
    ):
        raise ConvergenceError("shared capacity root metadata is invalid")


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_file_and_parent(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_directory(path.parent)


@contextmanager
def _collector_lock() -> Iterator[None]:
    _ensure_directory(_COMBINED_ROOT, gid=0, mode=0o700)
    lock_path = _COMBINED_ROOT / ".collector.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lock_path, flags, 0o600)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0:
            raise ConvergenceError("activation collector lock metadata is invalid")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _invalidate_combined_receipt(target: Path) -> None:
    _ensure_directory(target.parent.parent, gid=0, mode=0o700)
    _ensure_directory(target.parent, gid=0, mode=0o700)
    try:
        metadata = target.lstat()
    except FileNotFoundError:
        _fsync_directory(target.parent)
        return
    if stat.S_ISDIR(metadata.st_mode):
        raise ConvergenceError("combined activation receipt path is a directory")
    target.unlink()
    _fsync_directory(target.parent)


def collect_attestations(
    config: RuntimeConfig,
    sandbox: str,
    sha: str,
    tree: str,
    *,
    execute: bool,
) -> dict[str, object]:
    _require_root()
    if _hostname() != _COLLECTOR_HOSTNAME:
        raise ConvergenceError("activation collector must run on oldlab2")
    if not execute:
        return _collect_attestation_checks(
            config,
            sandbox,
            sha,
            tree,
            execute=False,
        )
    _require_capacity_root()
    target = _COMBINED_ROOT / sandbox / sha / "combined.json"
    with _collector_lock():
        _invalidate_combined_receipt(target)
        return _collect_attestation_checks(
            config,
            sandbox,
            sha,
            tree,
            execute=True,
        )


def _rollback(
    config: RuntimeConfig,
    receipt_path: Path,
    *,
    allow_committed: bool,
) -> dict[str, object]:
    receipt = _read_rollback_receipt(config, receipt_path)
    domain_name = receipt.get("domain")
    sandbox = receipt.get("sandbox")
    sha = receipt.get("candidate_sha")
    if (
        not isinstance(domain_name, str)
        or domain_name not in config.domains
        or not isinstance(sandbox, str)
        or sandbox not in ALLOWED_SANDBOXES
        or not isinstance(sha, str)
        or _SHA_RE.fullmatch(sha) is None
    ):
        raise ConvergenceError("rollback receipt identity is invalid")
    with _transaction_lock(config, domain_name, sandbox, sha):
        return _rollback_locked(
            config,
            receipt_path,
            allow_committed=allow_committed,
        )


def _read_rollback_receipt(
    config: RuntimeConfig,
    receipt_path: Path,
) -> dict[str, object]:
    try:
        original_metadata = receipt_path.lstat()
        resolved_receipt = receipt_path.resolve(strict=True)
        resolved_root = config.state_root.resolve(strict=True)
    except OSError as exc:
        raise ConvergenceError("rollback receipt is unavailable or invalid") from exc
    if (
        stat.S_ISLNK(original_metadata.st_mode)
        or not stat.S_ISREG(original_metadata.st_mode)
        or original_metadata.st_uid != 0
        or original_metadata.st_gid != 0
        or stat.S_IMODE(original_metadata.st_mode) != 0o600
        or resolved_root not in resolved_receipt.parents
    ):
        raise ConvergenceError("rollback receipt is outside the transaction state root")
    parent = resolved_receipt.parent.lstat()
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != 0
        or parent.st_gid != 0
        or stat.S_IMODE(parent.st_mode) != 0o700
    ):
        raise ConvergenceError("rollback receipt parent metadata is invalid")
    try:
        raw_receipt = json.loads(resolved_receipt.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConvergenceError("rollback receipt is unavailable or invalid") from exc
    if not isinstance(raw_receipt, dict):
        raise ConvergenceError("rollback receipt is unavailable or invalid")
    return cast(dict[str, object], raw_receipt)


def _secure_snapshot(path: Path, expected_digest: object, label: str) -> bytes:
    try:
        metadata = path.lstat()
        content = path.read_bytes()
    except OSError as exc:
        raise ConvergenceError(f"{label} is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not isinstance(expected_digest, str)
        or hashlib.sha256(content).hexdigest() != expected_digest
    ):
        raise ConvergenceError(f"{label} metadata or digest is invalid")
    return content


def _rollback_locked(
    config: RuntimeConfig,
    receipt_path: Path,
    *,
    allow_committed: bool,
) -> dict[str, object]:
    receipt = _read_rollback_receipt(config, receipt_path)
    if receipt.get("schema_version") != SCHEMA_VERSION:
        raise ConvergenceError("rollback receipt schema is invalid")
    status = receipt.get("status")
    if status == "rolled-back":
        return receipt
    if status == "committed" and not allow_committed:
        raise ConvergenceError("committed transaction requires explicit rollback command")
    if status not in {"prepared", "mutating", "committed", "rolling-back"}:
        raise ConvergenceError("rollback receipt status is invalid")
    domain_name = receipt.get("domain")
    sandbox = receipt.get("sandbox")
    sha = receipt.get("candidate_sha")
    tree = receipt.get("candidate_tree")
    if (
        not isinstance(domain_name, str)
        or domain_name not in config.domains
        or not isinstance(sandbox, str)
        or sandbox not in ALLOWED_SANDBOXES
        or not isinstance(sha, str)
        or _SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or _SHA_RE.fullmatch(tree) is None
    ):
        raise ConvergenceError("rollback receipt identity is invalid")
    domain = config.domains[domain_name]
    identity = CandidateIdentity(sha=sha, tree=tree)
    paths = runtime_paths(domain, sandbox, sha)
    backup = receipt_path.parent / "previous.env"
    phase = receipt.get("rollback_phase")
    if phase not in {None, "preflighted", "attestation-restored", "env-restored"}:
        raise ConvergenceError("rollback receipt phase is invalid")
    previous_env: bytes | None = None
    if receipt.get("env_previously_existed") is True:
        previous_env = _secure_snapshot(
            backup,
            receipt.get("previous_env_sha256"),
            "rollback env snapshot",
        )
    elif receipt.get("previous_env_sha256") is not None:
        raise ConvergenceError("rollback env snapshot binding is invalid")
    if phase in {None, "preflighted", "attestation-restored"}:
        published_digest = receipt.get("published_env_sha256")
        if published_digest is not None:
            try:
                current_env = paths.env.read_bytes()
            except OSError as exc:
                raise ConvergenceError("published env is unavailable during rollback") from exc
            if (
                not isinstance(published_digest, str)
                or hashlib.sha256(current_env).hexdigest() != published_digest
            ):
                raise ConvergenceError("published env no longer matches rollback receipt")
    if receipt.get("candidate_created") is True and phase != "env-restored":
        _verify_candidate(paths.candidate, identity, config.shared_gid)
    attestation = receipt.get("attestation")
    attestation_pending = receipt.get("attestation_pending") is True
    manifest_path, signature_path = _attestation_paths(domain.name, sandbox, sha)
    previous_manifest: bytes | None = None
    previous_signature: bytes | None = None
    if isinstance(attestation, dict):
        if phase is None:
            try:
                current = json.loads(manifest_path.read_text(encoding="utf-8"))
                encoded_signature = signature_path.read_bytes().strip()
                decoded_signature = base64.b64decode(encoded_signature, validate=True)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                raise ConvergenceError(
                    "current attestation is invalid during rollback",
                ) from exc
            publisher = current.get("publisher")
            if (
                current.get("payload_sha256") != attestation.get("payload_sha256")
                or not isinstance(publisher, dict)
                or publisher.get("generation") != attestation.get("generation")
                or hashlib.sha256(decoded_signature).hexdigest()
                != attestation.get("signature_sha256")
            ):
                raise ConvergenceError("current attestation no longer matches receipt")
        if receipt.get("attestation_previously_existed") is True:
            previous_manifest = _secure_snapshot(
                receipt_path.parent / "previous-attestation.json",
                receipt.get("previous_attestation_payload_sha256"),
                "previous attestation snapshot",
            )
            previous_signature = _secure_snapshot(
                receipt_path.parent / "previous-attestation.sig",
                receipt.get("previous_attestation_signature_sha256"),
                "previous attestation signature snapshot",
            )
        elif (
            receipt.get("previous_attestation_payload_sha256") is not None
            or receipt.get("previous_attestation_signature_sha256") is not None
        ):
            raise ConvergenceError("previous attestation snapshot binding is invalid")
    elif receipt.get("attestation_previously_existed") is True:
        previous_manifest = _secure_snapshot(
            receipt_path.parent / "previous-attestation.json",
            receipt.get("previous_attestation_payload_sha256"),
            "previous attestation snapshot",
        )
        previous_signature = _secure_snapshot(
            receipt_path.parent / "previous-attestation.sig",
            receipt.get("previous_attestation_signature_sha256"),
            "previous attestation signature snapshot",
        )
        if (
            not attestation_pending
            and phase is None
            and (
                not manifest_path.is_file()
                or not signature_path.is_file()
                or hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                != receipt.get("previous_attestation_payload_sha256")
                or hashlib.sha256(signature_path.read_bytes()).hexdigest()
                != receipt.get("previous_attestation_signature_sha256")
            )
        ):
            raise ConvergenceError("current attestation changed before rollback")
    elif (
        not attestation_pending
        and phase is None
        and (manifest_path.exists() or signature_path.exists())
    ):
        raise ConvergenceError("rollback receipt attestation binding is incomplete")

    if phase is None:
        receipt["status"] = "rolling-back"
        receipt["rollback_phase"] = "preflighted"
        _write_json(receipt_path, receipt)
        phase = "preflighted"
    if phase == "preflighted":
        if (isinstance(attestation, dict) or attestation_pending) and receipt.get(
            "attestation_previously_existed"
        ) is True:
            assert previous_manifest is not None and previous_signature is not None
            _atomic_bytes(
                manifest_path,
                previous_manifest,
                mode=0o644,
                parent_mode=0o755,
            )
            _atomic_bytes(
                signature_path,
                previous_signature,
                mode=0o644,
                parent_mode=0o755,
            )
        elif isinstance(attestation, dict) or attestation_pending:
            manifest_path.unlink(missing_ok=True)
            signature_path.unlink(missing_ok=True)
        receipt["rollback_phase"] = "attestation-restored"
        _write_json(receipt_path, receipt)
        phase = "attestation-restored"
    if phase == "attestation-restored":
        if previous_env is not None:
            group = config.sandbox_groups[sandbox]
            _atomic_env_write(
                backup,
                paths.env,
                group.uid,
                group.gid,
            )
        else:
            paths.env.unlink(missing_ok=True)
        receipt["rollback_phase"] = "env-restored"
        _write_json(receipt_path, receipt)
        phase = "env-restored"
    if phase == "env-restored" and receipt.get("candidate_created") is True:
        if paths.candidate.exists():
            _verify_candidate(paths.candidate, identity, config.shared_gid)
            shutil.rmtree(paths.candidate)
    receipt["status"] = "rolled-back"
    receipt["attestation_pending"] = False
    receipt.pop("rollback_phase", None)
    _write_json(receipt_path, receipt)
    return receipt


def _recover_orphaned_transactions(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    sha: str,
) -> None:
    root = config.state_root / domain.name / sandbox / sha
    if not root.exists():
        return
    for receipt_path in sorted(root.glob("transaction-*/receipt.json")):
        receipt = _read_rollback_receipt(config, receipt_path)
        status = receipt.get("status")
        if status in {"prepared", "mutating", "rolling-back"}:
            _rollback_locked(config, receipt_path, allow_committed=False)


def _converge_publish_locked(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    source_repo: Path,
    env_seed: Path,
    identity: CandidateIdentity,
    fleet_attestation_seed: Path | None = None,
) -> dict[str, object]:
    _require_root()
    if _hostname() != domain.publisher_hostname:
        raise ConvergenceError("publish must run on the declared NFS domain publisher")
    _recover_orphaned_transactions(
        config,
        domain,
        sandbox,
        identity.sha,
    )
    plan = publish_plan(config, domain, sandbox, identity, env_seed)
    group = config.sandbox_groups[sandbox]
    if _service_identity_status(group) != "ok":
        raise ConvergenceError("stable sandbox service identity is not converged")
    paths = runtime_paths(domain, sandbox, identity.sha)
    _ensure_runtime_parents(domain, group.gid, identity.sha, sandbox)
    receipt_path, receipt = _create_receipt(
        config,
        domain,
        sandbox,
        identity,
        paths.env,
    )
    try:
        created = _publish_candidate(
            source_repo,
            paths.candidate,
            identity,
            config.shared_gid,
        )
        receipt["candidate_created"] = created
        receipt["status"] = "mutating"
        _write_json(receipt_path, receipt)
        seed = _secure_seed(env_seed, require_root_owner=True)
        _parse_env_references(
            seed,
            domain=domain,
            sandbox=sandbox,
            sha=identity.sha,
        )
        if not paths.env.exists() or paths.env.read_bytes() != seed.read_bytes():
            _atomic_env_write(
                seed,
                paths.env,
                group.uid,
                group.gid,
            )
        receipt["published_env_sha256"] = hashlib.sha256(paths.env.read_bytes()).hexdigest()
        _write_json(receipt_path, receipt)
        reports = _peer_readback(config, domain, sandbox, identity)
        manifest_path, signature_path = _attestation_paths(
            domain.name,
            sandbox,
            identity.sha,
        )
        prior_attestation = manifest_path.exists() or signature_path.exists()
        if manifest_path.exists() != signature_path.exists():
            raise ConvergenceError("existing domain attestation pair is incomplete")
        receipt["attestation_previously_existed"] = prior_attestation
        receipt["previous_attestation_payload_sha256"] = None
        receipt["previous_attestation_signature_sha256"] = None
        if prior_attestation:
            previous_manifest = receipt_path.parent / "previous-attestation.json"
            previous_signature = receipt_path.parent / "previous-attestation.sig"
            previous_manifest_bytes = manifest_path.read_bytes()
            previous_signature_bytes = signature_path.read_bytes()
            _atomic_bytes(previous_manifest, previous_manifest_bytes, mode=0o600)
            _atomic_bytes(previous_signature, previous_signature_bytes, mode=0o600)
            receipt["previous_attestation_payload_sha256"] = hashlib.sha256(
                previous_manifest_bytes,
            ).hexdigest()
            receipt["previous_attestation_signature_sha256"] = hashlib.sha256(
                previous_signature_bytes,
            ).hexdigest()
        receipt["attestation_pending"] = True
        _write_json(receipt_path, receipt)
        try:
            attestation = _emit_attestation(
                config,
                domain,
                sandbox,
                identity,
                reports,
                fleet_attestation_seed=fleet_attestation_seed,
            )
        except Exception:
            if prior_attestation:
                _atomic_bytes(
                    manifest_path,
                    previous_manifest.read_bytes(),
                    mode=0o644,
                    parent_mode=0o755,
                )
                _atomic_bytes(
                    signature_path,
                    previous_signature.read_bytes(),
                    mode=0o644,
                    parent_mode=0o755,
                )
            else:
                manifest_path.unlink(missing_ok=True)
                signature_path.unlink(missing_ok=True)
            raise
        receipt["attestation"] = attestation
        receipt["attestation_pending"] = False
        _write_json(receipt_path, receipt)
        receipt["status"] = "committed"
        receipt["peer_hostnames"] = [item["hostname"] for item in reports]
        _write_json(receipt_path, receipt)
    except Exception:
        _rollback_locked(config, receipt_path, allow_committed=False)
        raise
    result = dict(plan)
    result["mode"] = "applied"
    result["receipt"] = str(receipt_path)
    result["attestation"] = attestation
    result["peer_readback"] = [
        {
            "hostname": item["hostname"],
            "candidate_inode": item["candidate_inode"],
            "env_inode": item["env_inode"],
            "env_values": "not-read",
        }
        for item in reports
    ]
    return result


def attest_plan(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    env_seed: Path,
    identity: CandidateIdentity,
) -> dict[str, object]:
    paths = runtime_paths(domain, sandbox, identity.sha)
    _verify_candidate(paths.candidate, identity, config.shared_gid)
    seed = _secure_seed(env_seed)
    _parse_env_references(seed, domain=domain, sandbox=sandbox, sha=identity.sha)
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "attest",
        "mode": "plan",
        "domain": domain.name,
        "sandbox": sandbox,
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "candidate_path": str(paths.candidate),
        "candidate_action": "verify",
        "env_path": str(paths.env),
        "env_action": (
            "noop"
            if paths.env.exists() and paths.env.read_bytes() == seed.read_bytes()
            else ("replace" if paths.env.exists() else "create")
        ),
        "fleet_attestation": "required-fresh-before-mutation",
        "env_values": "redacted",
    }


def _converge_attest_locked(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    env_seed: Path,
    identity: CandidateIdentity,
    fleet_attestation_seed: Path | None = None,
) -> dict[str, object]:
    _recover_orphaned_transactions(config, domain, sandbox, identity.sha)
    # Fleet proof is deliberately read before any env or attestation mutation.
    # This makes the materialize -> relay fleet -> attest dependency explicit.
    _read_and_verify_fleet(
        config,
        sandbox,
        identity.sha,
        now=datetime.now(UTC),
        seed=fleet_attestation_seed,
    )
    plan = attest_plan(config, domain, sandbox, env_seed, identity)
    group = config.sandbox_groups[sandbox]
    if _service_identity_status(group) != "ok":
        raise ConvergenceError("stable sandbox service identity is not converged")
    paths = runtime_paths(domain, sandbox, identity.sha)
    _ensure_runtime_parents(domain, group.gid, identity.sha, sandbox)
    receipt_path, receipt = _create_receipt(
        config,
        domain,
        sandbox,
        identity,
        paths.env,
    )
    try:
        receipt["candidate_created"] = False
        receipt["status"] = "mutating"
        _write_json(receipt_path, receipt)
        seed = _secure_seed(env_seed, require_root_owner=True)
        _parse_env_references(
            seed,
            domain=domain,
            sandbox=sandbox,
            sha=identity.sha,
        )
        if not paths.env.exists() or paths.env.read_bytes() != seed.read_bytes():
            _atomic_env_write(
                seed,
                paths.env,
                group.uid,
                group.gid,
            )
        receipt["published_env_sha256"] = hashlib.sha256(paths.env.read_bytes()).hexdigest()
        _write_json(receipt_path, receipt)
        reports = _peer_readback(config, domain, sandbox, identity)
        receipt["attestation_pending"] = True
        _write_json(receipt_path, receipt)
        attestation = _emit_attestation(
            config,
            domain,
            sandbox,
            identity,
            reports,
            fleet_attestation_seed=fleet_attestation_seed,
        )
        receipt["attestation"] = attestation
        receipt["attestation_pending"] = False
        receipt["status"] = "committed"
        receipt["peer_hostnames"] = [item["hostname"] for item in reports]
        _write_json(receipt_path, receipt)
    except Exception:
        _rollback_locked(config, receipt_path, allow_committed=False)
        raise
    result = dict(plan)
    result["mode"] = "applied"
    result["receipt"] = str(receipt_path)
    result["attestation"] = attestation
    result["peer_readback"] = [
        {
            "hostname": item["hostname"],
            "candidate_inode": item["candidate_inode"],
            "env_inode": item["env_inode"],
            "env_values": "not-read",
        }
        for item in reports
    ]
    return result


def converge_attest(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    env_seed: Path,
    identity: CandidateIdentity,
    fleet_attestation_seed: Path | None = None,
) -> dict[str, object]:
    _require_root()
    if _hostname() != domain.publisher_hostname:
        raise ConvergenceError("attest must run on the declared NFS domain publisher")
    with _transaction_lock(config, domain.name, sandbox, identity.sha):
        return _converge_attest_locked(
            config,
            domain,
            sandbox,
            env_seed,
            identity,
            fleet_attestation_seed,
        )


def converge_publish(
    config: RuntimeConfig,
    domain: Domain,
    sandbox: str,
    source_repo: Path,
    env_seed: Path,
    identity: CandidateIdentity,
    fleet_attestation_seed: Path | None = None,
) -> dict[str, object]:
    _require_root()
    if _hostname() != domain.publisher_hostname:
        raise ConvergenceError("publish must run on the declared NFS domain publisher")
    with _transaction_lock(config, domain.name, sandbox, identity.sha):
        return _converge_publish_locked(
            config,
            domain,
            sandbox,
            source_repo,
            env_seed,
            identity,
            fleet_attestation_seed,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "host-converge",
            "materialize",
            "attest",
            "publish",
            "inspect-candidate",
            "inspect-local",
            "export-domain-attestation",
            "export-runtime-proof-artifact",
            "rollback",
            "pin-key",
            "collect",
        ),
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--domain", choices=ALLOWED_DOMAINS)
    parser.add_argument("--sandbox", choices=ALLOWED_SANDBOXES)
    parser.add_argument("--candidate-sha")
    parser.add_argument("--candidate-tree")
    parser.add_argument("--source-repo", type=Path)
    parser.add_argument("--source-bundle", type=Path)
    parser.add_argument("--worker-env-seed", type=Path)
    parser.add_argument("--fleet-attestation-seed", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--public-key", type=Path)
    parser.add_argument("--artifact-id")
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "pin-key":
            if args.domain is None or args.public_key is None:
                raise ConvergenceError("pin-key requires --domain and --public-key")
            report = (
                pin_key(args.domain, args.public_key)
                if args.execute
                else pin_key_plan(args.domain, args.public_key)
            )
        elif args.command == "collect":
            if (
                args.sandbox is None
                or args.candidate_sha is None
                or _SHA_RE.fullmatch(args.candidate_sha) is None
                or args.candidate_tree is None
                or _SHA_RE.fullmatch(args.candidate_tree) is None
            ):
                raise ConvergenceError(
                    "collect requires sandbox and exact candidate SHA/tree",
                )
            report = collect_attestations(
                config,
                args.sandbox,
                args.candidate_sha,
                args.candidate_tree,
                execute=args.execute,
            )
        elif args.command == "rollback":
            if args.receipt is None:
                raise ConvergenceError("rollback requires --receipt")
            if not args.execute:
                report = {
                    "schema_version": SCHEMA_VERSION,
                    "operation": "rollback",
                    "mode": "plan",
                    "receipt": str(args.receipt),
                }
            else:
                _require_root()
                report = _rollback(config, args.receipt, allow_committed=True)
        else:
            if args.domain is None:
                raise ConvergenceError(f"{args.command} requires --domain")
            domain = config.domains[args.domain]
            if args.command == "host-converge":
                report = (
                    converge_host(args.config, config, domain)
                    if args.execute
                    else identity_plan(config, domain)
                )
            else:
                if (
                    args.sandbox is None
                    or args.candidate_sha is None
                    or _SHA_RE.fullmatch(args.candidate_sha) is None
                ):
                    raise ConvergenceError(
                        f"{args.command} requires sandbox and exact candidate SHA",
                    )
                if args.command in {
                    "inspect-candidate",
                    "inspect-local",
                    "export-domain-attestation",
                    "export-runtime-proof-artifact",
                }:
                    if (
                        args.candidate_tree is None
                        or _SHA_RE.fullmatch(args.candidate_tree) is None
                    ):
                        raise ConvergenceError(
                            f"{args.command} requires exact candidate tree",
                        )
                    identity = CandidateIdentity(args.candidate_sha, args.candidate_tree)
                    if args.command == "inspect-candidate":
                        report = inspect_candidate_local(
                            config,
                            domain,
                            args.sandbox,
                            identity,
                        )
                    elif args.command == "inspect-local":
                        report = inspect_local(
                            config,
                            domain,
                            args.sandbox,
                            identity,
                        )
                    elif args.command == "export-domain-attestation":
                        report = export_domain_attestation(
                            config,
                            domain,
                            args.sandbox,
                            identity,
                        )
                    else:
                        if args.artifact_id is None:
                            raise ConvergenceError(
                                "export-runtime-proof-artifact requires artifact ID",
                            )
                        report = export_runtime_proof_artifact(
                            config,
                            domain,
                            args.sandbox,
                            identity,
                            args.artifact_id,
                        )
                elif args.command == "materialize":
                    if (
                        args.source_bundle is None
                        or args.candidate_tree is None
                        or _SHA_RE.fullmatch(args.candidate_tree) is None
                    ):
                        raise ConvergenceError(
                            "materialize requires source bundle and exact candidate tree",
                        )
                    identity = _bundle_candidate_identity(
                        args.source_bundle,
                        args.candidate_sha,
                        args.candidate_tree,
                    )
                    report = (
                        converge_materialize(
                            config,
                            domain,
                            args.sandbox,
                            args.source_bundle,
                            identity,
                        )
                        if args.execute
                        else {
                            "schema_version": SCHEMA_VERSION,
                            "operation": "materialize",
                            "mode": "plan",
                            "domain": domain.name,
                            "sandbox": args.sandbox,
                            "candidate_sha": identity.sha,
                            "candidate_tree": identity.tree,
                            "candidate_path": str(
                                runtime_paths(domain, args.sandbox, identity.sha).candidate,
                            ),
                            "fleet_attestation": "not-read",
                        }
                    )
                elif args.command == "attest":
                    if (
                        args.worker_env_seed is None
                        or args.fleet_attestation_seed is None
                        or args.candidate_tree is None
                        or _SHA_RE.fullmatch(args.candidate_tree) is None
                    ):
                        raise ConvergenceError(
                            "attest requires worker/fleet seeds and exact candidate tree",
                        )
                    identity = CandidateIdentity(args.candidate_sha, args.candidate_tree)
                    report = (
                        converge_attest(
                            config,
                            domain,
                            args.sandbox,
                            args.worker_env_seed,
                            identity,
                            args.fleet_attestation_seed,
                        )
                        if args.execute
                        else attest_plan(
                            config,
                            domain,
                            args.sandbox,
                            args.worker_env_seed,
                            identity,
                        )
                    )
                else:
                    if (
                        args.source_repo is None
                        or args.worker_env_seed is None
                        or args.fleet_attestation_seed is None
                    ):
                        raise ConvergenceError(
                            "publish requires source repository and worker/fleet seeds",
                        )
                    identity = _candidate_identity(args.source_repo, args.candidate_sha)
                    report = (
                        converge_publish(
                            config,
                            domain,
                            args.sandbox,
                            args.source_repo,
                            args.worker_env_seed,
                            identity,
                            args.fleet_attestation_seed,
                        )
                        if args.execute
                        else publish_plan(
                            config,
                            domain,
                            args.sandbox,
                            identity,
                            args.worker_env_seed,
                        )
                    )
        print(json.dumps(report, sort_keys=True))
        return 0
    except ConvergenceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
