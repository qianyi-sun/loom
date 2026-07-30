#!/usr/bin/python3 -I
"""Root Unix-socket authority for developer environment registry transactions."""

from __future__ import annotations

import array
import base64
import grp
import hashlib
import importlib
import importlib.util
import json
import os
import pwd
import re
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Final, cast

SCHEMA_VERSION: Final = 1
SOCKET_PATH: Final = Path("/run/loom-developer-environment-authority/authority.sock")
STAGE_ROOT: Final = Path("/run/loom-developer-environment-authority/imports")
INSTALLED_REGISTRY: Final = Path("/usr/local/libexec/scripts/ops/developer_environment_registry.py")
INSTALLED_DEPLOYER: Final = Path("/usr/local/libexec/loom-developer-environment-deploy")
ALLOWED_GROUPS: Final = frozenset({"loom-developers"})
MAX_REQUEST_BYTES: Final = 64 * 1024
MAX_GIT_OUTPUT_BYTES: Final = 64 * 1024
MAX_NODE_INVENTORY_RESPONSE_BYTES: Final = 4 * 1024 * 1024
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
NODE_TRANSPORT: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-transport",
)
NODE_AUTHORITY_POLICY: Final = Path(
    "/etc/loom/developer-sandbox-node-authority.json",
)
FLEET_BOOTSTRAP_SCOPE: Final = "fleet-bootstrap"
FLEET_INVENTORY_PROCESS_LOCK: Final = threading.Lock()

REGISTER_KIND: Final = "loom.developer-environment.register"
IMPORT_KIND: Final = "loom.developer-environment.candidate-import"
STATUS_KIND: Final = "loom.developer-environment.status"
SNAPSHOT_KIND: Final = "loom.developer-environment.snapshot"
BEGIN_DEPLOY_KIND: Final = "loom.developer-environment.begin-deploy"
CREATE_KIND: Final = "loom.developer-environment.create"
UPDATE_KIND: Final = "loom.developer-environment.update"
CHECK_KIND: Final = "loom.developer-environment.check"
ROLLBACK_KIND: Final = "loom.developer-environment.rollback"
DESTROY_KIND: Final = "loom.developer-environment.destroy"

REQUEST_FIELDS: Final = {
    REGISTER_KIND: {
        "schema_version",
        "kind",
        "idempotency_key",
        "display_name",
    },
    IMPORT_KIND: {
        "schema_version",
        "kind",
        "idempotency_key",
        "env_id",
        "candidate_sha",
        "candidate_tree",
        "bundle_sha256",
        "bundle_size",
        "image_digests",
    },
    STATUS_KIND: {"schema_version", "kind", "env_id"},
    SNAPSHOT_KIND: {"schema_version", "kind"},
    BEGIN_DEPLOY_KIND: {
        "schema_version",
        "kind",
        "idempotency_key",
        "env_id",
        "candidate_id",
        "expected_resource_generation",
    },
    CREATE_KIND: {
        "schema_version",
        "kind",
        "idempotency_key",
        "display_name",
        "candidate_sha",
        "candidate_tree",
        "bundle_sha256",
        "bundle_size",
        "image_digests",
    },
    UPDATE_KIND: {
        "schema_version",
        "kind",
        "idempotency_key",
        "candidate_sha",
        "candidate_tree",
        "bundle_sha256",
        "bundle_size",
        "image_digests",
    },
    CHECK_KIND: {"schema_version", "kind"},
    ROLLBACK_KIND: {"schema_version", "kind", "idempotency_key"},
    DESTROY_KIND: {"schema_version", "kind", "idempotency_key"},
}


class AuthorityError(RuntimeError):
    """A bounded error whose message is safe for an untrusted caller."""


@dataclass(frozen=True, slots=True)
class PeerIdentity:
    pid: int
    uid: int
    gid: int
    username: str
    principal_id: str
    groups: frozenset[str]


PeerResolver = Callable[[socket.socket], PeerIdentity]


def _load_registry() -> ModuleType:
    try:
        return importlib.import_module("scripts.ops.developer_environment_registry")
    except ImportError:
        try:
            specification = importlib.util.spec_from_file_location(
                "_loom_developer_environment_registry",
                INSTALLED_REGISTRY,
            )
            if specification is None or specification.loader is None:
                raise ImportError
            module = importlib.util.module_from_spec(specification)
            sys.modules[specification.name] = module
            specification.loader.exec_module(module)
            return module
        except (ImportError, OSError) as exc:
            raise AuthorityError("registry implementation is unavailable") from exc


registry: Any = _load_registry()


def _load_deployer() -> ModuleType:
    try:
        return importlib.import_module("scripts.ops.developer_environment_deploy")
    except ImportError:
        try:
            specification = importlib.util.spec_from_file_location(
                "_loom_developer_environment_deploy",
                INSTALLED_DEPLOYER,
            )
            if specification is None or specification.loader is None:
                raise ImportError
            module = importlib.util.module_from_spec(specification)
            sys.modules[specification.name] = module
            specification.loader.exec_module(module)
            return module
        except (ImportError, OSError, RuntimeError) as exc:
            raise AuthorityError("deployment implementation is unavailable") from exc


def _canonical(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise AuthorityError("request is not canonical JSON") from exc


def _peer_identity(connection: socket.socket) -> PeerIdentity:
    option = getattr(socket, "SO_PEERCRED", None)
    if option is None:
        raise AuthorityError("peer credentials are unavailable")
    try:
        raw = connection.getsockopt(socket.SOL_SOCKET, option, struct.calcsize("3i"))
        pid, uid, gid = struct.unpack("3i", raw)
        account = pwd.getpwuid(uid)
        group_ids = os.getgrouplist(account.pw_name, account.pw_gid)
        groups = frozenset(grp.getgrgid(group_id).gr_name for group_id in group_ids)
    except (KeyError, OSError, struct.error) as exc:
        raise AuthorityError("peer credentials are unavailable") from exc
    return PeerIdentity(
        pid=pid,
        uid=uid,
        gid=gid,
        username=account.pw_name,
        principal_id=f"unix-uid:{uid}",
        groups=groups,
    )


def _authorized_peer(peer: PeerIdentity) -> None:
    if (
        peer.pid < 1
        or peer.uid < 0
        or peer.gid < 0
        or not peer.username
        or peer.principal_id != f"unix-uid:{peer.uid}"
        or not peer.groups.intersection(ALLOWED_GROUPS)
    ):
        raise AuthorityError("peer is not authorized")


def _decode_request(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_REQUEST_BYTES:
        raise AuthorityError("request size is invalid")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("request is invalid") from exc
    if not isinstance(payload, dict):
        raise AuthorityError("request is invalid")
    kind_value = payload.get("kind")
    kind = kind_value if isinstance(kind_value, str) else ""
    fields = REQUEST_FIELDS.get(kind)
    if (
        fields is None
        or set(payload) != fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or raw != _canonical(payload)
    ):
        raise AuthorityError("request binding is invalid")
    return payload


def _receive_request(
    connection: socket.socket,
) -> tuple[dict[str, Any], list[int]]:
    item_size = array.array("i").itemsize
    raw, ancillary, flags, _address = connection.recvmsg(
        MAX_REQUEST_BYTES + 1,
        socket.CMSG_SPACE(item_size * 4),
    )
    descriptors: list[int] = []
    try:
        if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
            raise AuthorityError("request transport is invalid")
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                raise AuthorityError("request transport is invalid")
            if len(data) == 0 or len(data) % item_size:
                raise AuthorityError("request transport is invalid")
            received = array.array("i")
            received.frombytes(data)
            descriptors.extend(received.tolist())
        request = _decode_request(raw)
        expected_count = 1 if request["kind"] in {IMPORT_KIND, CREATE_KIND, UPDATE_KIND} else 0
        if len(descriptors) != expected_count:
            raise AuthorityError("request descriptor count is invalid")
        return request, descriptors
    except Exception:
        for descriptor in descriptors:
            os.close(descriptor)
        raise


def _stable_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _validate_stage_root(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
        metadata = path.lstat()
    except OSError as exc:
        raise AuthorityError("bundle staging is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise AuthorityError("bundle staging is unsafe")


def _copy_bundle(
    descriptor: int,
    destination: Path,
    *,
    peer_uid: int,
    declared_size: object,
    declared_digest: object,
    max_size: int,
) -> None:
    if (
        not isinstance(declared_size, int)
        or isinstance(declared_size, bool)
        or not 0 < declared_size <= max_size
        or not isinstance(declared_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", declared_digest) is None
    ):
        raise AuthorityError("bundle binding is invalid")
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != peer_uid
            or before.st_size != declared_size
        ):
            raise AuthorityError("bundle metadata is unsafe")
        output = os.open(
            destination,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        digest = hashlib.sha256()
        offset = 0
        try:
            while offset < declared_size:
                chunk = os.pread(
                    descriptor,
                    min(1024 * 1024, declared_size - offset),
                    offset,
                )
                if not chunk:
                    raise AuthorityError("bundle content is incomplete")
                view = memoryview(chunk)
                while view:
                    written = os.write(output, view)
                    if written < 1:
                        raise AuthorityError("bundle staging failed")
                    view = view[written:]
                digest.update(chunk)
                offset += len(chunk)
            if os.pread(descriptor, 1, declared_size):
                raise AuthorityError("bundle size changed during import")
            os.fsync(output)
        finally:
            os.close(output)
        after = os.fstat(descriptor)
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("bundle import failed") from exc
    if _stable_identity(before) != _stable_identity(after) or digest.hexdigest() != declared_digest:
        raise AuthorityError("bundle content binding is invalid")


def _run_git(arguments: list[str], *, cwd: Path) -> str:
    config_root = cwd / ".xdg-config"
    try:
        config_root.mkdir(mode=0o700, exist_ok=True)
        config_root.chmod(0o700)
    except OSError as exc:
        raise AuthorityError("candidate verification failed") from exc
    environment = {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "XDG_CONFIG_HOME": str(config_root),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }
    try:
        completed = subprocess.run(
            ["/usr/bin/git", *arguments],
            cwd=cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AuthorityError("candidate verification failed") from exc
    if (
        completed.returncode != 0
        or len(completed.stdout) > MAX_GIT_OUTPUT_BYTES
        or len(completed.stderr) > MAX_GIT_OUTPUT_BYTES
    ):
        raise AuthorityError("candidate verification failed")
    try:
        return completed.stdout.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise AuthorityError("candidate verification failed") from exc


def _verify_git_bundle(
    bundle: Path,
    *,
    candidate_sha: object,
    candidate_tree: object,
    working_root: Path,
) -> None:
    if (
        not isinstance(candidate_sha, str)
        or SHA_RE.fullmatch(candidate_sha) is None
        or not isinstance(candidate_tree, str)
        or SHA_RE.fullmatch(candidate_tree) is None
    ):
        raise AuthorityError("candidate binding is invalid")
    heads = _run_git(["bundle", "list-heads", str(bundle)], cwd=working_root)
    if heads.splitlines() != [f"{candidate_sha} HEAD"]:
        raise AuthorityError("candidate advertised HEAD is invalid")
    repository = working_root / "verification.git"
    _run_git(["init", "--bare", str(repository)], cwd=working_root)
    _run_git(["-C", str(repository), "bundle", "verify", str(bundle)], cwd=working_root)
    _run_git(
        ["-C", str(repository), "fetch", "--no-tags", str(bundle), "HEAD"],
        cwd=working_root,
    )
    if (
        _run_git(["-C", str(repository), "rev-parse", "FETCH_HEAD"], cwd=working_root)
        != candidate_sha
        or _run_git(
            ["-C", str(repository), "rev-parse", "FETCH_HEAD^{tree}"],
            cwd=working_root,
        )
        != candidate_tree
    ):
        raise AuthorityError("candidate object binding is invalid")
    _run_git(
        ["-C", str(repository), "fsck", "--strict", "--no-dangling"],
        cwd=working_root,
    )


def _validated_candidate_path(candidate: Any, candidate_root: Path) -> Path:
    expected = candidate_root / str(candidate.candidate_id) / "candidate.bundle"
    if (
        not candidate_root.is_absolute()
        or Path(candidate.bundle_path) != expected
        or expected.name != "candidate.bundle"
    ):
        raise AuthorityError("candidate storage binding is invalid")
    return expected


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_DIRECTORY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        os.fsync(descriptor)
    except OSError as exc:
        raise AuthorityError("candidate persistence failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_persisted_bundle(candidate: Any, candidate_root: Path) -> Path:
    path = _validated_candidate_path(candidate, candidate_root)
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size != candidate.bundle_size
        ):
            raise AuthorityError("candidate storage metadata is unsafe")
        digest = hashlib.sha256()
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(
                descriptor,
                min(1024 * 1024, before.st_size - offset),
                offset,
            )
            if not chunk:
                raise AuthorityError("candidate storage content is incomplete")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        current = path.lstat()
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("candidate storage is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        len(
            {
                _stable_identity(lexical),
                _stable_identity(before),
                _stable_identity(after),
                _stable_identity(current),
            }
        )
        != 1
        or digest.hexdigest() != candidate.bundle_sha256
    ):
        raise AuthorityError("candidate storage content binding is invalid")
    return path


def _persist_verified_bundle(
    staged_bundle: Path,
    candidate: Any,
    candidate_root: Path,
) -> Path:
    target = _validated_candidate_path(candidate, candidate_root)
    try:
        candidate_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        candidate_root.chmod(0o700)
        root_metadata = candidate_root.lstat()
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or stat.S_ISLNK(root_metadata.st_mode)
            or root_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(root_metadata.st_mode) != 0o700
        ):
            raise AuthorityError("candidate storage root is unsafe")
        target.parent.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("candidate storage is unavailable") from exc
    try:
        directory_metadata = target.parent.lstat()
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or stat.S_ISLNK(directory_metadata.st_mode)
            or directory_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(directory_metadata.st_mode) != 0o700
        ):
            raise AuthorityError("candidate storage directory is unsafe")
        _fsync_directory(candidate_root.parent)
        _fsync_directory(candidate_root)
        if target.exists():
            return _validate_persisted_bundle(candidate, candidate_root)
        temporary: Path | None = None
        try:
            temporary_descriptor, temporary_name = tempfile.mkstemp(
                prefix=".candidate-",
                suffix=".tmp",
                dir=target.parent,
            )
            temporary = Path(temporary_name)
            source_descriptor = -1
            try:
                os.fchmod(temporary_descriptor, 0o600)
                source_descriptor = os.open(
                    staged_bundle,
                    os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                )
                offset = 0
                while offset < candidate.bundle_size:
                    chunk = os.pread(
                        source_descriptor,
                        min(1024 * 1024, candidate.bundle_size - offset),
                        offset,
                    )
                    if not chunk:
                        raise AuthorityError("candidate persistence failed")
                    view = memoryview(chunk)
                    while view:
                        written = os.write(temporary_descriptor, view)
                        if written < 1:
                            raise AuthorityError("candidate persistence failed")
                        view = view[written:]
                    offset += len(chunk)
                os.fsync(temporary_descriptor)
            finally:
                if source_descriptor >= 0:
                    os.close(source_descriptor)
                os.close(temporary_descriptor)
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        _fsync_directory(target.parent)
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("candidate persistence failed") from exc
    return _validate_persisted_bundle(candidate, candidate_root)


def _owned_environment(
    authority: Any,
    *,
    env_id: object,
    principal_id: str,
) -> Any:
    if not isinstance(env_id, str):
        raise AuthorityError("environment binding is invalid")
    try:
        return authority.lookup(env_id, principal_id=principal_id)
    except registry.RegistryError as exc:
        raise AuthorityError("environment is unavailable") from exc


def _principal_snapshot(authority: Any, principal_id: str) -> dict[str, Any]:
    snapshot = authority.snapshot()
    environments = [
        item for item in snapshot["environments"] if item["principal_id"] == principal_id
    ]
    candidates = [item for item in snapshot["candidates"] if item["principal_id"] == principal_id]
    deployments = [item for item in snapshot["deployments"] if item["principal_id"] == principal_id]
    return {
        "registry_generation": snapshot["generation"],
        "environments": environments,
        "candidates": candidates,
        "deployments": deployments,
    }


def _principal_environment(authority: Any, principal_id: str) -> Any:
    environments = authority.list_environments(principal_id=principal_id)
    if len(environments) != 1:
        raise AuthorityError("developer environment is unavailable")
    return environments[0]


def _internal_idempotency_key(
    *,
    principal_id: str,
    public_key: object,
    phase: str,
) -> str:
    if not isinstance(public_key, str):
        raise AuthorityError("idempotency binding is invalid")
    return (
        "self-"
        + hashlib.sha256(
            _canonical(
                {
                    "principal_id": principal_id,
                    "public_idempotency_key": public_key,
                    "phase": phase,
                }
            )
        ).hexdigest()
    )


def _deployment_result(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AuthorityError("deployment result is invalid")
    return cast(dict[str, Any], value)


def _read_installed_node_policy() -> dict[str, Any]:
    descriptor = -1
    try:
        lexical = NODE_AUTHORITY_POLICY.lstat()
        descriptor = os.open(
            NODE_AUTHORITY_POLICY,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        raw = os.read(descriptor, 1 << 20)
        if os.read(descriptor, 1):
            raise AuthorityError("installed node authority policy exceeds its size bound")
        rebound = os.fstat(descriptor)
        current = NODE_AUTHORITY_POLICY.lstat()
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("installed node authority policy is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        len(
            {
                _stable_identity(lexical),
                _stable_identity(opened),
                _stable_identity(rebound),
                _stable_identity(current),
            },
        )
        != 1
        or not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or opened.st_nlink != 1
        or (opened.st_uid, opened.st_gid) != (os.geteuid(), os.getegid())
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise AuthorityError("installed node authority policy metadata is unsafe")
    try:
        policy = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("installed node authority policy is invalid") from exc
    if (
        not isinstance(policy, dict)
        or set(policy) != {"schema_version", "source_sha", "source_tree", "node", "asset_sha256"}
        or policy.get("schema_version") != SCHEMA_VERSION
        or SHA_RE.fullmatch(str(policy.get("source_sha"))) is None
        or SHA_RE.fullmatch(str(policy.get("source_tree"))) is None
        or policy.get("node") not in registry.FLEET_NODES
        or not isinstance(policy.get("asset_sha256"), dict)
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
            for digest in policy["asset_sha256"].values()
        )
        or raw != _canonical(policy)
    ):
        raise AuthorityError("installed node authority policy is invalid")
    return cast(dict[str, Any], policy)


def _fleet_inventory_source(
    snapshot: Mapping[str, Any],
    installed_policy: Mapping[str, Any],
) -> tuple[str, str, str]:
    if (
        not isinstance(snapshot.get("generation"), int)
        or re.fullmatch(r"[0-9a-f]{64}", str(snapshot.get("payload_sha256"))) is None
    ):
        raise AuthorityError("fleet identity inventory registry binding is invalid")
    return (
        FLEET_BOOTSTRAP_SCOPE,
        str(installed_policy["source_sha"]),
        str(installed_policy["source_tree"]),
    )


def _fleet_inventory_envelope(
    snapshot: Mapping[str, Any],
    installed_policy: Mapping[str, Any],
    *,
    node: str,
) -> bytes:
    sandbox, candidate_sha, candidate_tree = _fleet_inventory_source(
        snapshot,
        installed_policy,
    )
    domain = "oldlab" if node.startswith("oldlab-") else "gb10"
    inner = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-environment.identity-inventory-request",
        "uid_start": 32_000,
        "uid_end": 60_000,
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
    }
    payload = _canonical(inner)
    outer: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": "slurm-identity-inventory",
        "node": node,
        "domain": domain,
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "payload_kind": "developer-environment-identity-inventory-json",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "prior_request_id": None,
    }
    outer["request_id"] = hashlib.sha256(_canonical(outer)).hexdigest()
    return _canonical(outer)


def _collect_fleet_inventory_node(
    snapshot: Mapping[str, Any],
    installed_policy: Mapping[str, Any],
    node: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> dict[str, Any]:
    envelope = _fleet_inventory_envelope(snapshot, installed_policy, node=node)
    request_id = str(json.loads(envelope)["request_id"])
    try:
        completed = run(
            [
                str(NODE_TRANSPORT),
                "invoke",
                "--node",
                node,
                "--verb",
                "check",
            ],
            input=envelope,
            env={
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            },
            check=False,
            capture_output=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("fleet identity inventory collection failed") from exc
    if (
        completed.returncode != 0
        or completed.stderr
        or not completed.stdout
        or len(completed.stdout) > MAX_NODE_INVENTORY_RESPONSE_BYTES
    ):
        raise AuthorityError("fleet identity inventory collection failed")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("fleet identity inventory response is invalid") from exc
    if (
        not isinstance(response, dict)
        or set(response) != {"schema_version", "request_id", "status", "result"}
        or response.get("schema_version") != SCHEMA_VERSION
        or response.get("request_id") != request_id
        or response.get("status") != "succeeded"
        or not isinstance(response.get("result"), dict)
        or completed.stdout != _canonical(response)
    ):
        raise AuthorityError("fleet identity inventory response binding is invalid")
    result = cast(dict[str, Any], response["result"])
    if result.get("node") != node:
        raise AuthorityError("fleet identity inventory response binding is invalid")
    return result


def _refresh_fleet_identity_inventory(
    authority: Any,
    *,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> None:
    if not bool(getattr(authority, "system_mode", False)):
        return
    with FLEET_INVENTORY_PROCESS_LOCK:
        snapshot = authority.snapshot()
        installed_policy = _read_installed_node_policy()
        with ThreadPoolExecutor(
            max_workers=len(registry.FLEET_NODES),
            thread_name_prefix="loom-identity-inventory",
        ) as executor:
            futures = [
                executor.submit(
                    _collect_fleet_inventory_node,
                    snapshot,
                    installed_policy,
                    node,
                    run=run,
                )
                for node in registry.FLEET_NODES
            ]
            node_results = [future.result() for future in futures]
        authority.publish_fleet_identity_inventory(
            node_results,
            registry_generation=snapshot["generation"],
            registry_payload_sha256=snapshot["payload_sha256"],
        )


def _import_for_environment(
    request: Mapping[str, Any],
    descriptor: int,
    *,
    principal_id: str,
    peer_uid: int,
    environment: Any,
    authority: Any,
    stage_root: Path,
    idempotency_key: str | None = None,
) -> Any:
    _validate_stage_root(stage_root)
    working_root = Path(tempfile.mkdtemp(prefix="candidate-", dir=stage_root))
    try:
        bundle = working_root / "candidate.bundle"
        _copy_bundle(
            descriptor,
            bundle,
            peer_uid=peer_uid,
            declared_size=request["bundle_size"],
            declared_digest=request["bundle_sha256"],
            max_size=authority.policy.max_bundle_bytes,
        )
        _verify_git_bundle(
            bundle,
            candidate_sha=request["candidate_sha"],
            candidate_tree=request["candidate_tree"],
            working_root=working_root,
        )
        record = authority.import_candidate(
            {
                "schema_version": request["schema_version"],
                "kind": registry.CANDIDATE_KIND,
                "principal_id": principal_id,
                "idempotency_key": (
                    request["idempotency_key"] if idempotency_key is None else idempotency_key
                ),
                "env_id": environment.env_id,
                "candidate_sha": request["candidate_sha"],
                "candidate_tree": request["candidate_tree"],
                "bundle_sha256": request["bundle_sha256"],
                "bundle_size": request["bundle_size"],
                "image_digests": request["image_digests"],
            }
        )
        _persist_verified_bundle(
            bundle,
            record,
            Path(authority.candidate_root),
        )
        return record
    finally:
        shutil.rmtree(working_root, ignore_errors=True)


def _preflight_revival_candidate(
    request: Mapping[str, Any],
    descriptor: int,
    *,
    principal_id: str,
    peer_uid: int,
    environment: Any,
    authority: Any,
    stage_root: Path,
) -> None:
    """Verify exact candidate bytes and novelty before changing retired state."""

    _validate_stage_root(stage_root)
    working_root = Path(tempfile.mkdtemp(prefix="revival-preflight-", dir=stage_root))
    try:
        bundle = working_root / "candidate.bundle"
        _copy_bundle(
            descriptor,
            bundle,
            peer_uid=peer_uid,
            declared_size=request["bundle_size"],
            declared_digest=request["bundle_sha256"],
            max_size=authority.policy.max_bundle_bytes,
        )
        _verify_git_bundle(
            bundle,
            candidate_sha=request["candidate_sha"],
            candidate_tree=request["candidate_tree"],
            working_root=working_root,
        )
        authority.validate_revival_candidate_content(
            environment.env_id,
            principal_id=principal_id,
            candidate_sha=str(request["candidate_sha"]),
            candidate_tree=str(request["candidate_tree"]),
            bundle_sha256=str(request["bundle_sha256"]),
        )
    finally:
        shutil.rmtree(working_root, ignore_errors=True)


def _dispatch(
    request: Mapping[str, Any],
    descriptors: list[int],
    *,
    peer: PeerIdentity,
    authority: Any,
    stage_root: Path,
    deployer: Any | None = None,
) -> dict[str, Any]:
    principal_id = peer.principal_id
    kind = str(request["kind"])
    if kind == REGISTER_KIND:
        _refresh_fleet_identity_inventory(authority)
        record = authority.register({**request, "principal_id": principal_id})
        return asdict(record)
    if kind == STATUS_KIND:
        return asdict(
            _owned_environment(
                authority,
                env_id=request["env_id"],
                principal_id=principal_id,
            )
        )
    if kind == SNAPSHOT_KIND:
        return _principal_snapshot(authority, principal_id)
    if kind == IMPORT_KIND:
        environment = _owned_environment(
            authority,
            env_id=request["env_id"],
            principal_id=principal_id,
        )
        return asdict(
            _import_for_environment(
                request,
                descriptors[0],
                principal_id=principal_id,
                peer_uid=peer.uid,
                environment=environment,
                authority=authority,
                stage_root=stage_root,
            )
        )
    if kind == BEGIN_DEPLOY_KIND:
        _owned_environment(
            authority,
            env_id=request["env_id"],
            principal_id=principal_id,
        )
        try:
            candidate = authority.lookup_candidate(
                str(request["candidate_id"]),
                principal_id=principal_id,
                env_id=str(request["env_id"]),
            )
            _validate_persisted_bundle(
                candidate,
                Path(authority.candidate_root),
            )
            record = authority.begin_deployment(
                {
                    "schema_version": request["schema_version"],
                    "kind": registry.DEPLOY_KIND,
                    "principal_id": principal_id,
                    "idempotency_key": request["idempotency_key"],
                    "env_id": request["env_id"],
                    "candidate_id": request["candidate_id"],
                    "expected_resource_generation": request["expected_resource_generation"],
                }
            )
        except registry.RegistryError as exc:
            raise AuthorityError("deployment request failed") from exc
        if record.phase != "requested":
            raise AuthorityError("deployment transaction state is invalid")
        return {
            **asdict(record),
            "deployed": False,
            "mutation_started": False,
        }
    if kind in {CREATE_KIND, UPDATE_KIND}:
        selected = (
            deployer
            if deployer is not None
            else _load_deployer().DeveloperEnvironmentDeployer(authority)
        )
        if kind == CREATE_KIND:
            _refresh_fleet_identity_inventory(authority)
            registration_key = _internal_idempotency_key(
                principal_id=principal_id,
                public_key=request["idempotency_key"],
                phase="register",
            )
            existing = authority.list_environments(principal_id=principal_id)
            retired = len(existing) == 1 and existing[0].state == "retired"
            replay = authority.registration_idempotency_replay(
                principal_id=principal_id,
                idempotency_key=registration_key,
            )
            if retired and replay:
                raise AuthorityError("retired environment requires a new create idempotency key")
            if retired:
                _preflight_revival_candidate(
                    request,
                    descriptors[0],
                    principal_id=principal_id,
                    peer_uid=peer.uid,
                    environment=existing[0],
                    authority=authority,
                    stage_root=stage_root,
                )
            environment = authority.register(
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": registry.REGISTER_KIND,
                    "principal_id": principal_id,
                    "idempotency_key": registration_key,
                    "display_name": request["display_name"],
                }
            )
            if retired:
                try:
                    selected.revive(
                        env_id=environment.env_id,
                        principal_id=principal_id,
                        idempotency_key=_internal_idempotency_key(
                            principal_id=principal_id,
                            public_key=request["idempotency_key"],
                            phase="revive",
                        ),
                        registration_idempotency_key=registration_key,
                    )
                except Exception as exc:
                    deployment_error = _load_deployer().DeploymentError
                    if isinstance(exc, deployment_error):
                        raise AuthorityError("developer environment revival failed") from exc
                    raise
                environment = _principal_environment(authority, principal_id)
        else:
            environment = _principal_environment(authority, principal_id)
        candidate = _import_for_environment(
            request,
            descriptors[0],
            principal_id=principal_id,
            peer_uid=peer.uid,
            environment=environment,
            authority=authority,
            stage_root=stage_root,
            idempotency_key=_internal_idempotency_key(
                principal_id=principal_id,
                public_key=request["idempotency_key"],
                phase="candidate-import",
            ),
        )
        try:
            return _deployment_result(
                selected.converge(
                    env_id=environment.env_id,
                    principal_id=principal_id,
                    candidate_id=candidate.candidate_id,
                    idempotency_key=_internal_idempotency_key(
                        principal_id=principal_id,
                        public_key=request["idempotency_key"],
                        phase="deploy",
                    ),
                    operation="create" if kind == CREATE_KIND else "update",
                )
            )
        except Exception as exc:
            deployment_error = _load_deployer().DeploymentError
            if isinstance(exc, deployment_error):
                raise AuthorityError("developer environment deployment failed") from exc
            raise
    if kind in {CHECK_KIND, ROLLBACK_KIND, DESTROY_KIND}:
        environment = _principal_environment(authority, principal_id)
        selected = (
            deployer
            if deployer is not None
            else _load_deployer().DeveloperEnvironmentDeployer(authority)
        )
        try:
            if kind == CHECK_KIND:
                return _deployment_result(
                    selected.check(
                        env_id=environment.env_id,
                        principal_id=principal_id,
                    )
                )
            if kind == ROLLBACK_KIND:
                return _deployment_result(
                    selected.rollback(
                        env_id=environment.env_id,
                        principal_id=principal_id,
                        idempotency_key=_internal_idempotency_key(
                            principal_id=principal_id,
                            public_key=request["idempotency_key"],
                            phase="rollback",
                        ),
                    )
                )
            return _deployment_result(
                selected.retire(
                    env_id=environment.env_id,
                    principal_id=principal_id,
                    idempotency_key=_internal_idempotency_key(
                        principal_id=principal_id,
                        public_key=request["idempotency_key"],
                        phase="retire",
                    ),
                )
            )
        except Exception as exc:
            deployment_error = _load_deployer().DeploymentError
            if isinstance(exc, deployment_error):
                raise AuthorityError("developer environment operation failed") from exc
            raise
    raise AuthorityError("request kind is unsupported")


def handle_connection(
    connection: socket.socket,
    authority: Any,
    *,
    peer_resolver: PeerResolver = _peer_identity,
    stage_root: Path = STAGE_ROOT,
    deployer: Any | None = None,
) -> None:
    descriptors: list[int] = []
    try:
        peer = peer_resolver(connection)
        _authorized_peer(peer)
        request, descriptors = _receive_request(connection)
        result = _dispatch(
            request,
            descriptors,
            peer=peer,
            authority=authority,
            stage_root=stage_root,
            deployer=deployer,
        )
        response = {
            "schema_version": SCHEMA_VERSION,
            "kind": f"{request['kind']}.response",
            "status": "succeeded",
            "result": result,
        }
    except (AuthorityError, registry.RegistryError):
        response = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.error",
            "status": "failed",
            "error": "request failed safely",
        }
    except Exception:
        response = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.error",
            "status": "failed",
            "error": "request failed safely",
        }
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
    try:
        connection.sendall(_canonical(response))
    except (AuthorityError, OSError):
        return


def _activation_socket() -> socket.socket:
    if (
        os.getuid() != 0
        or os.geteuid() != 0
        or os.environ.get("LISTEN_PID") != str(os.getpid())
        or os.environ.get("LISTEN_FDS") != "1"
    ):
        raise AuthorityError("socket activation is invalid")
    try:
        listener = socket.socket(fileno=3)
        if (
            listener.family != socket.AF_UNIX
            or listener.type & socket.SOCK_SEQPACKET != socket.SOCK_SEQPACKET
            or listener.getsockname() != str(SOCKET_PATH)
        ):
            raise AuthorityError("socket activation is invalid")
    except OSError as exc:
        raise AuthorityError("socket activation is invalid") from exc
    return listener


def _serve_connection(
    connection: socket.socket,
    authority: Any,
    slots: threading.BoundedSemaphore,
) -> None:
    try:
        handle_connection(connection, authority)
    finally:
        connection.close()
        slots.release()


def serve(listener: socket.socket, authority: Any) -> None:
    slots = threading.BoundedSemaphore(16)
    with ThreadPoolExecutor(
        max_workers=16,
        thread_name_prefix="loom-developer-environment",
    ) as executor:
        while True:
            slots.acquire()
            try:
                connection, _address = listener.accept()
            except Exception:
                slots.release()
                raise
            executor.submit(_serve_connection, connection, authority, slots)


def main() -> int:
    if len(sys.argv) != 1:
        sys.stderr.write("error: authority accepts no command-line arguments\n")
        return 2
    try:
        listener = _activation_socket()
        authority = registry.DeveloperEnvironmentRegistry.open_system()
        serve(listener, authority)
    except (AuthorityError, registry.RegistryError):
        sys.stderr.write("error: developer environment authority failed safely\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
