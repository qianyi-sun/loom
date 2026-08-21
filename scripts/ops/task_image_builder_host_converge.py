#!/usr/bin/env python3
"""Converge one inert rootless task-image-builder host with rollback evidence."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import shlex
import socket
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol, cast

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import task_image_builder_authority as authority  # noqa: E402
from scripts.ops import task_image_builder_host_release as host_release  # noqa: E402

MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_RUNTIME_BINARY_BYTES = 1024 * 1024 * 1024
ZERO_HASH = "0" * 64
PACKAGE_VERSIONS = {
    "libsubid4": "1:4.13+dfsg1-4ubuntu3.2",
    "uidmap": "1:4.13+dfsg1-4ubuntu3.2",
    "quota": "4.06-1build6",
}
PACKAGE_ORDER = ("libsubid4", "uidmap", "quota")
DESIRED_CGROUP = (
    b"CgroupPlugin=autodetect\n"
    b"ConstrainCores=yes\n"
    b"ConstrainRAMSpace=yes\n"
    b"ConstrainSwapSpace=yes\n"
    b"ConstrainDevices=yes\n"
)
INERT_BLOCKER = "phase2_guard_provider_release_missing"


class HostConvergenceError(RuntimeError):
    """The requested host operation is unsafe or incomplete."""


@dataclass(frozen=True)
class HostReleasePaths:
    policy: Path
    release: Path
    runtime_manifest: Path
    node_installer: Path
    wrapper: Path


DEFAULT_PATHS = HostReleasePaths(
    policy=ROOT / "deploy/task-image-builder/prerequisites-v1.toml",
    release=ROOT / "deploy/task-image-builder/host-release-v2.json",
    runtime_manifest=ROOT / "deploy/task-image-builder/rootless-runtime-v1.json",
    node_installer=(
        ROOT / "deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh"
    ),
    wrapper=Path(__file__).resolve(),
)


@dataclass(frozen=True)
class HostPolicy:
    cluster_id: str
    host_release_manifest: str
    architecture: str
    slurm_node: str
    builder_nodes: tuple[str, ...]
    cgroup_config: Path
    cgroup_transition: str
    cgroup_observed_path: Path
    cgroup_observed_sha256: str
    storage_root: Path
    storage_mountpoint: Path
    project_id: int
    scratch_bytes: int
    scratch_inodes: int
    raw_cluster: Mapping[str, object]


@dataclass(frozen=True)
class HostFacts:
    architecture: str
    slurm_node: str
    bundle_digest: str
    packages: Mapping[str, str | None]
    helpers_exact: bool
    identity_exact: bool
    runtime_exact: bool
    quota_exact: bool
    quota_state: Mapping[str, object]
    storage_exact: bool
    kernel_exact: bool
    forbidden_sockets_absent: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def prepared_except_cgroup(self) -> bool:
        return (
            self.architecture != ""
            and self.slurm_node != ""
            and dict(self.packages) == PACKAGE_VERSIONS
            and self.helpers_exact
            and self.identity_exact
            and self.runtime_exact
            and self.quota_exact
            and self.storage_exact
            and self.kernel_exact
            and self.forbidden_sockets_absent
        )


class HostBackend(Protocol):
    def preflight(self, policy: HostPolicy, bundle: Path) -> HostFacts: ...

    def recovery_preflight(self, policy: HostPolicy, bundle: Path) -> HostFacts: ...

    def install_packages(self, policy: HostPolicy, bundle: Path) -> None: ...

    def apply_quota(self, policy: HostPolicy) -> None: ...

    def install_inert_prerequisites(self, policy: HostPolicy, bundle: Path) -> None: ...

    def observe(self, policy: HostPolicy, bundle: Path) -> HostFacts: ...

    def restore_quota(
        self,
        policy: HostPolicy,
        quota_prestate: Mapping[str, object],
    ) -> None: ...

    def quota_matches(
        self,
        policy: HostPolicy,
        expected: Mapping[str, object],
    ) -> bool: ...

    def close(self) -> None: ...


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _inert() -> dict[str, object]:
    return {
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": [INERT_BLOCKER],
    }


def _read_regular(
    path: Path,
    label: str,
    *,
    required_owner: int | None = None,
    reject_group_world_write: bool = False,
) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise HostConvergenceError(f"{label} is unavailable") from exc
    try:
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or initial.st_size > MAX_FILE_BYTES
            or (
                required_owner is not None
                and (initial.st_uid != required_owner or initial.st_gid != required_owner)
            )
            or (reject_group_world_write and bool(initial.st_mode & 0o022))
        ):
            raise HostConvergenceError(f"{label} is unsafe")
        chunks: list[bytes] = []
        remaining = MAX_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        if len(payload) != initial.st_size or (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise HostConvergenceError(f"{label} changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _installed_runtime_digest(path: Path, owner: int, mode: int) -> str:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise HostConvergenceError("installed rootless runtime drift is unsafe") from exc
    try:
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or initial.st_uid != owner
            or initial.st_gid != owner
            or stat.S_IMODE(initial.st_mode) != mode
            or initial.st_size > MAX_RUNTIME_BINARY_BYTES
        ):
            raise HostConvergenceError("installed rootless runtime drift is unsafe")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        final = os.fstat(descriptor)
        if (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise HostConvergenceError("installed rootless runtime changed during readback")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _load_policy(path: Path, cluster_id: str, slurm_node: str) -> tuple[HostPolicy, bytes]:
    payload = _read_regular(path, "prerequisite policy")
    try:
        raw = tomllib.loads(payload.decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergenceError("prerequisite policy is invalid") from exc
    if raw.get("schema") != "loom.task-image-builder-prerequisites/v1":
        raise HostConvergenceError("prerequisite policy schema is invalid")
    if raw.get("production_certification_allowed") is not False or raw.get(
        "certified_nodes"
    ) != []:
        raise HostConvergenceError("prerequisite policy is not inert")
    if raw.get("unconditional_blockers") != [INERT_BLOCKER]:
        raise HostConvergenceError("prerequisite blocker is invalid")
    release_manifest = raw.get("host_release_manifest")
    if (
        not isinstance(release_manifest, str)
        or not release_manifest
        or "/" in release_manifest
        or "\\" in release_manifest
        or Path(release_manifest).name != release_manifest
    ):
        raise HostConvergenceError("host release manifest path is invalid")
    clusters = [
        item
        for item in raw.get("clusters", [])
        if isinstance(item, dict) and item.get("id") == cluster_id
    ]
    if len(clusters) != 1:
        raise HostConvergenceError("cluster policy is not unique")
    cluster = clusters[0]
    nodes_raw = cluster.get("builder_nodes")
    if not isinstance(nodes_raw, list) or not all(isinstance(item, str) for item in nodes_raw):
        raise HostConvergenceError("builder node inventory is invalid")
    nodes = tuple(nodes_raw)
    if len(nodes) != len(set(nodes)) or slurm_node not in nodes:
        raise HostConvergenceError("Slurm node is outside the builder inventory")
    storage = raw.get("storage")
    resources = raw.get("resource_profile", {})
    if not isinstance(storage, dict) or not isinstance(resources, dict):
        raise HostConvergenceError("storage or resource policy is invalid")
    if (
        storage.get("project_id") != 300993
        or storage.get("site_filesystem") != "ext4"
        or storage.get("required_mount_options") != ["prjquota"]
        or storage.get("automatic_block_device_changes") is not False
        or storage.get("reject_root_filesystem") is not True
        or storage.get("reject_network_filesystem") is not True
        or storage.get("cache_enabled") is not False
        or storage.get("cleanup_required") is not True
    ):
        raise HostConvergenceError("storage policy is not fail closed")
    transition = cluster.get("cgroup_transition")
    if transition not in {"shared_symlink_to_node_local", "node_local"}:
        raise HostConvergenceError("cgroup transition is invalid")
    observed_digest = cluster.get("cgroup_observed_sha256")
    if not isinstance(observed_digest, str) or len(observed_digest) != 64:
        raise HostConvergenceError("cgroup observed digest is invalid")
    return (
        HostPolicy(
            cluster_id=cluster_id,
            host_release_manifest=release_manifest,
            architecture=str(cluster.get("architecture")),
            slurm_node=slurm_node,
            builder_nodes=nodes,
            cgroup_config=Path(str(cluster.get("cgroup_config"))),
            cgroup_transition=transition,
            cgroup_observed_path=Path(str(cluster.get("cgroup_observed_path"))),
            cgroup_observed_sha256=observed_digest,
            storage_root=Path(str(storage.get("root"))),
            storage_mountpoint=Path(str(storage.get("mountpoint"))),
            project_id=300993,
            scratch_bytes=int(resources.get("scratch_bytes", 107374182400)),
            scratch_inodes=int(resources.get("scratch_inodes", 1000000)),
            raw_cluster=cluster,
        ),
        payload,
    )


def _file_metadata(path: Path) -> dict[str, int]:
    metadata = path.lstat()
    return {
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
    }


def _inspect_cgroup(policy: HostPolicy) -> tuple[str, dict[str, object]]:
    path = policy.cgroup_config
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergenceError("cgroup configuration is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode):
        if policy.cgroup_transition != "shared_symlink_to_node_local":
            raise HostConvergenceError("unexpected cgroup symlink transition")
        target = os.readlink(path)
        if Path(target) != policy.cgroup_observed_path:
            raise HostConvergenceError("cgroup symlink target drift is unsafe")
        observed = _read_regular(policy.cgroup_observed_path, "shared cgroup configuration")
        if _sha(observed) != policy.cgroup_observed_sha256:
            raise HostConvergenceError("shared cgroup configuration drift is unsafe")
        return (
            "observed",
            {
                "kind": "symlink",
                "target": target,
                "sha256": _sha(observed),
                **_file_metadata(path),
            },
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise HostConvergenceError("cgroup configuration type is unsafe")
    payload = _read_regular(path, "cgroup configuration")
    if payload == DESIRED_CGROUP:
        return (
            "desired",
            {
                "kind": "regular",
                "payload_b64": base64.b64encode(payload).decode(),
                "sha256": _sha(payload),
                **_file_metadata(path),
            },
        )
    if policy.cgroup_transition == "node_local" and _sha(payload) == policy.cgroup_observed_sha256:
        return (
            "observed",
            {
                "kind": "regular",
                "payload_b64": base64.b64encode(payload).decode(),
                "sha256": _sha(payload),
                **_file_metadata(path),
            },
        )
    raise HostConvergenceError("cgroup configuration drift is unsafe")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_regular(
    path: Path,
    payload: bytes,
    *,
    mode: int,
    uid: int,
    gid: int,
) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise HostConvergenceError("cgroup configuration write failed")
            view = view[written:]
        os.fchmod(descriptor, mode)
        os.fchown(descriptor, uid, gid)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()


def _apply_cgroup(policy: HostPolicy, owner: int) -> None:
    _atomic_regular(
        policy.cgroup_config,
        DESIRED_CGROUP,
        mode=0o644,
        uid=owner,
        gid=owner,
    )


def _restore_cgroup(policy: HostPolicy, prestate: Mapping[str, object]) -> None:
    if prestate.get("kind") == "symlink":
        target = prestate.get("target")
        if not isinstance(target, str):
            raise HostConvergenceError("cgroup rollback symlink target is invalid")
        temporary = policy.cgroup_config.parent / f".{policy.cgroup_config.name}.{uuid.uuid4()}"
        try:
            temporary.symlink_to(target)
            uid = prestate.get("uid")
            gid = prestate.get("gid")
            if not isinstance(uid, int) or isinstance(uid, bool) or not isinstance(gid, int) or isinstance(gid, bool):
                raise HostConvergenceError("cgroup rollback symlink metadata is invalid")
            os.chown(temporary, uid, gid, follow_symlinks=False)
            os.replace(temporary, policy.cgroup_config)
            _fsync_directory(policy.cgroup_config.parent)
        finally:
            if temporary.exists() or temporary.is_symlink():
                temporary.unlink()
        return
    encoded = prestate.get("payload_b64")
    if not isinstance(encoded, str):
        raise HostConvergenceError("cgroup rollback payload is invalid")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise HostConvergenceError("cgroup rollback payload is invalid") from exc
    if _sha(payload) != prestate.get("sha256"):
        raise HostConvergenceError("cgroup rollback payload digest is invalid")
    mode = prestate.get("mode")
    uid = prestate.get("uid")
    gid = prestate.get("gid")
    if (
        not isinstance(mode, int)
        or isinstance(mode, bool)
        or not isinstance(uid, int)
        or isinstance(uid, bool)
        or not isinstance(gid, int)
        or isinstance(gid, bool)
    ):
        raise HostConvergenceError("cgroup rollback metadata is invalid")
    _atomic_regular(
        policy.cgroup_config,
        payload,
        mode=mode,
        uid=uid,
        gid=gid,
    )


def _cgroup_matches(policy: HostPolicy, prestate: Mapping[str, object]) -> bool:
    try:
        if prestate.get("kind") == "symlink":
            return (
                policy.cgroup_config.is_symlink()
                and os.readlink(policy.cgroup_config) == prestate.get("target")
                and _file_metadata(policy.cgroup_config)
                == {
                    "mode": prestate.get("mode"),
                    "uid": prestate.get("uid"),
                    "gid": prestate.get("gid"),
                }
                and _sha(
                    _read_regular(
                        policy.cgroup_observed_path,
                        "restored shared cgroup configuration",
                    )
                )
                == prestate.get("sha256")
            )
        return (
            not policy.cgroup_config.is_symlink()
            and _sha(_read_regular(policy.cgroup_config, "restored cgroup configuration"))
            == prestate.get("sha256")
            and _file_metadata(policy.cgroup_config)
            == {
                "mode": prestate.get("mode"),
                "uid": prestate.get("uid"),
                "gid": prestate.get("gid"),
            }
        )
    except (OSError, HostConvergenceError):
        return False


def _changes(facts: HostFacts, cgroup_state: str) -> list[str]:
    changes: list[str] = []
    if dict(facts.packages) != PACKAGE_VERSIONS:
        changes.append("packages")
    if not facts.helpers_exact:
        changes.append("helpers")
    if cgroup_state != "desired":
        changes.append("cgroup")
    if not facts.quota_exact:
        changes.append("quota")
    if not facts.identity_exact:
        changes.append("identity")
    if not facts.runtime_exact:
        changes.append("runtime")
    return changes


def _quota_state_classification(
    policy: HostPolicy,
    state: Mapping[str, object],
) -> str:
    numeric_keys = (
        "block_used",
        "block_soft_limit",
        "block_hard_limit",
        "inode_used",
        "inode_soft_limit",
        "inode_hard_limit",
    )
    if any(
        not isinstance(state.get(key), int) or isinstance(state.get(key), bool)
        for key in numeric_keys
    ):
        raise HostConvergenceError("project quota readback is invalid")
    exact = (
        state.get("storage_root_exists") is True
        and state.get("storage_root_uid") == 993
        and state.get("storage_root_gid") == 980
        and state.get("storage_root_mode") == 0o700
        and state.get("storage_root_entries") == []
        and state.get("project_id") == policy.project_id
        and state.get("project_inherit") is True
        and state.get("block_soft_limit") == 0
        and state.get("block_hard_limit") == policy.scratch_bytes // 1024
        and state.get("inode_soft_limit") == 0
        and state.get("inode_hard_limit") == policy.scratch_inodes
    )
    if exact:
        return "exact"
    legacy = (
        state.get("storage_root_exists") is True
        and type(state.get("storage_root_uid")) is int
        and state.get("storage_root_uid") == 0
        and type(state.get("storage_root_gid")) is int
        and state.get("storage_root_gid") == 0
        and state.get("storage_root_mode") == 0o700
        and state.get("storage_root_entries") == []
        and state.get("project_id") == policy.project_id
        and state.get("project_inherit") is True
        and state.get("block_soft_limit") == 0
        and state.get("block_hard_limit") == policy.scratch_bytes // 1024
        and state.get("inode_soft_limit") == 0
        and state.get("inode_hard_limit") == policy.scratch_inodes
    )
    if legacy:
        return "legacy"
    root_absent = (
        state.get("storage_root_exists") is False
        and state.get("storage_root_uid") is None
        and state.get("storage_root_gid") is None
        and state.get("storage_root_mode") is None
        and state.get("storage_root_entries") is None
        and state.get("project_id") is None
        and state.get("project_inherit") is False
    )
    root_unassigned = (
        state.get("storage_root_exists") is True
        and isinstance(state.get("storage_root_uid"), int)
        and isinstance(state.get("storage_root_gid"), int)
        and isinstance(state.get("storage_root_mode"), int)
        and state.get("storage_root_entries") == []
        and state.get("project_id") == 0
        and state.get("project_inherit") is False
    )
    no_quota = all(state.get(key) == 0 for key in numeric_keys)
    if (root_absent or root_unassigned) and no_quota:
        return "absent"
    raise HostConvergenceError("existing project quota drift is unsafe")


def _validate_interrupted_quota_state(
    policy: HostPolicy,
    prestate: Mapping[str, object],
    current: Mapping[str, object],
) -> None:
    preclassification = _quota_state_classification(policy, prestate)
    numeric_keys = (
        "block_used",
        "block_soft_limit",
        "block_hard_limit",
        "inode_used",
        "inode_soft_limit",
        "inode_hard_limit",
    )
    numeric_values = tuple(current.get(key) for key in numeric_keys)
    if set(current) != set(prestate) or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in numeric_values
    ):
        raise HostConvergenceError("interrupted host quota state is unsafe")
    if current == prestate:
        return
    try:
        current_classification = _quota_state_classification(policy, current)
    except HostConvergenceError:
        current_classification = None
    if current_classification == "exact":
        return
    zero_limit_keys = (
        "block_soft_limit",
        "block_hard_limit",
        "inode_soft_limit",
        "inode_hard_limit",
    )
    recoverable_partial_apply = (
        preclassification == "absent"
        and current.get("storage_root_exists") is True
        and isinstance(current.get("project_id"), int)
        and not isinstance(current.get("project_id"), bool)
        and current.get("project_id") in {0, policy.project_id}
        and type(current.get("project_inherit")) is bool
        and all(current.get(key) == 0 for key in zero_limit_keys)
    )
    if not recoverable_partial_apply:
        raise HostConvergenceError("interrupted host quota state is unsafe")


def _validate_facts(policy: HostPolicy, facts: HostFacts) -> None:
    if facts.architecture != policy.architecture or facts.slurm_node != policy.slurm_node:
        raise HostConvergenceError("host architecture or Slurm node binding failed")
    if not facts.storage_exact:
        raise HostConvergenceError("dedicated project-quota storage preflight failed")
    if not facts.kernel_exact:
        raise HostConvergenceError("kernel containment prerequisite preflight failed")
    if not facts.forbidden_sockets_absent:
        raise HostConvergenceError("forbidden host socket is accessible")


def _authority_release_path(paths: HostReleasePaths, policy: HostPolicy) -> Path:
    return paths.policy.parent / policy.host_release_manifest


def _bound_release_payload(
    paths: HostReleasePaths,
    policy: HostPolicy,
) -> tuple[Path, bytes]:
    authority_release = _authority_release_path(paths, policy)
    authority_payload = _read_regular(authority_release, "policy host release")
    if paths.release != authority_release and _read_regular(
        paths.release,
        "host release",
    ) != authority_payload:
        raise HostConvergenceError("host release path does not match the policy binding")
    return authority_release, authority_payload


def _digests(
    paths: HostReleasePaths,
    policy_payload: bytes,
    policy: HostPolicy,
    authority_payload: bytes | None = None,
) -> dict[str, object]:
    if authority_payload is None:
        authority_payload = _read_regular(
            _authority_release_path(paths, policy),
            "policy host release",
        )
    components: dict[str, object] = {
        "policy": _sha(policy_payload),
        "release": _sha(authority_payload),
        "runtime": _sha(_read_regular(paths.runtime_manifest, "runtime manifest")),
    }
    try:
        binding = authority.load_authority_binding(ROOT)
    except authority.AuthorityError as exc:
        raise HostConvergenceError("authority component binding is invalid") from exc
    components.update(binding.as_dict())
    return {
        "candidate_digest": _sha(_canonical(components)),
        "policy_digest": _sha(policy_payload),
        "release_digest": _sha(authority_payload),
        "cluster_digest": _sha(_canonical(policy.raw_cluster)),
        **binding.as_dict(),
    }


def _validate_receipt_root(path: Path, owner: int) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != owner
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise HostConvergenceError("host receipt root must be owner-only")


def _event(document: dict[str, object], event_type: str, data: object) -> None:
    events = document["events"]
    if not isinstance(events, list):
        raise HostConvergenceError("host receipt events are invalid")
    previous = ZERO_HASH if not events else str(events[-1]["event_hash"])
    item: dict[str, object] = {
        "sequence": len(events),
        "type": event_type,
        "previous_hash": previous,
        "data": data,
    }
    item["event_hash"] = _sha(_canonical(item))
    events.append(item)


def _write_document(path: Path, document: Mapping[str, object], *, exclusive: bool) -> None:
    payload = _canonical(document) + b"\n"

    def write_payload(descriptor: int) -> None:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise HostConvergenceError("host receipt write failed")
            view = view[written:]
        os.fsync(descriptor)

    if exclusive:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        try:
            write_payload(descriptor)
        except BaseException:
            os.close(descriptor)
            descriptor = -1
            try:
                path.unlink()
            finally:
                _fsync_directory(path.parent)
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        _fsync_directory(path.parent)
        return

    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        raise HostConvergenceError("host receipt metadata is unsafe")
    temporary = path.parent / f".{path.name}.{uuid.uuid4()}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    try:
        write_payload(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()


def _receipt_document(
    policy: HostPolicy,
    digests: Mapping[str, object],
    operation_id: str,
    facts: HostFacts,
    cgroup_prestate: Mapping[str, object],
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema": "loom.task-image-builder-host-receipt/v1",
        "operation_id": operation_id,
        "cluster_id": policy.cluster_id,
        "slurm_node": policy.slurm_node,
        **digests,
        **_inert(),
        "bundle_digest": facts.bundle_digest,
        "pre_state": facts.as_dict(),
        "post_state": None,
        "cgroup_prestate": dict(cgroup_prestate),
        "cgroup_poststate": None,
        "created_inert_artifacts": [],
        "activation_required": False,
        "rollback_verified": None,
        "rollback_source_state": None,
        "terminal_state": "applying",
        "failure": None,
        "events": [],
    }
    _event(
        document,
        "pre_state",
        {
            "binding": {
                key: document[key]
                for key in (
                    "operation_id",
                    "cluster_id",
                    "slurm_node",
                    "candidate_digest",
                    "policy_digest",
                    "release_digest",
                    "cluster_digest",
                    "authority_manifest_sha256",
                    "authority_component_digests",
                    "bundle_digest",
                )
            },
            "facts": facts.as_dict(),
            "cgroup": cgroup_prestate,
        },
    )
    return document


def _validate_receipt_event_binding(value: Mapping[str, object]) -> None:
    events = value.get("events")
    if not isinstance(events, list) or not events or not isinstance(events[0], dict):
        raise HostConvergenceError("host receipt event chain is invalid")
    binding = {
        key: value.get(key)
        for key in (
            "operation_id",
            "cluster_id",
            "slurm_node",
            "candidate_digest",
            "policy_digest",
            "release_digest",
            "cluster_digest",
            "authority_manifest_sha256",
            "authority_component_digests",
            "bundle_digest",
        )
    }
    if events[0].get("type") != "pre_state" or events[0].get("data") != {
        "binding": binding,
        "facts": value.get("pre_state"),
        "cgroup": value.get("cgroup_prestate"),
    }:
        raise HostConvergenceError(
            "host receipt event chain digest/state binding is invalid"
        )
    if (
        value.get("production_certification_allowed") is not False
        or value.get("certified_nodes") != []
        or value.get("blockers") != [INERT_BLOCKER]
    ):
        raise HostConvergenceError("host receipt event chain inert boundary is invalid")

    terminal = value.get("terminal_state")
    if terminal == "applying":
        event_types = [
            event.get("type") if isinstance(event, dict) else None for event in events
        ]
        if (
            event_types not in (["pre_state"], ["pre_state", "intent"])
            or value.get("post_state") is not None
            or value.get("cgroup_poststate") is not None
            or value.get("activation_required") is not False
            or value.get("rollback_verified") is not None
            or value.get("rollback_source_state") is not None
            or value.get("failure") is not None
        ):
            raise HostConvergenceError("host receipt applying event chain is invalid")
        return
    if terminal == "host_prepared":
        if (
            len(events) < 2
            or not isinstance(events[-2], dict)
            or not isinstance(events[-1], dict)
            or events[-2].get("type") != "post_state"
            or events[-2].get("data")
            != {
                "facts": value.get("post_state"),
                "cgroup": value.get("cgroup_poststate"),
            }
            or events[-1].get("type") != "host_prepared"
            or events[-1].get("data")
            != {
                "activation_required": value.get("activation_required"),
                "created_inert_artifacts": value.get("created_inert_artifacts"),
            }
        ):
            raise HostConvergenceError("host receipt event chain is not terminal-state-bound")
        return
    if terminal in {"rolled_back", "rollback_failed"}:
        if (
            not isinstance(events[-1], dict)
            or events[-1].get("type") != terminal
            or not isinstance(events[-1].get("data"), dict)
            or events[-1]["data"].get("verified") != value.get("rollback_verified")
            or events[-1]["data"].get("source_state")
            != value.get("rollback_source_state")
            or value.get("activation_required") is not False
        ):
            raise HostConvergenceError("host receipt event chain is not terminal-state-bound")
        failure = value.get("failure")
        prepared_indexes = [
            index
            for index, event in enumerate(events[:-1])
            if isinstance(event, dict) and event.get("type") == "host_prepared"
        ]
        if prepared_indexes:
            prepared_index = prepared_indexes[0]
            if (
                len(prepared_indexes) != 1
                or prepared_index == 0
                or not isinstance(events[prepared_index - 1], dict)
                or events[prepared_index - 1].get("type") != "post_state"
                or events[prepared_index - 1].get("data")
                != {
                    "facts": value.get("post_state"),
                    "cgroup": value.get("cgroup_poststate"),
                }
                or events[prepared_index].get("data")
                != {
                    "activation_required": True,
                    "created_inert_artifacts": value.get("created_inert_artifacts"),
                }
            ):
                raise HostConvergenceError(
                    "host receipt event chain prepared history is invalid"
                )
        failed_events = [
            event
            for event in events
            if isinstance(event, dict) and event.get("type") == "apply_failed"
        ]
        if failure is None:
            source_state = value.get("rollback_source_state")
            if source_state == "host_prepared" and len(prepared_indexes) != 1:
                raise HostConvergenceError("host receipt event chain is not rollback-bound")
            if source_state == "applying" and prepared_indexes:
                raise HostConvergenceError("host receipt event chain is not rollback-bound")
            if source_state not in {"host_prepared", "applying"}:
                raise HostConvergenceError("host receipt event chain is not rollback-bound")
        elif (
            len(failed_events) != 1
            or not isinstance(failed_events[0].get("data"), dict)
            or failed_events[0]["data"]
            != {
                "error": failure,
                "post_state": value.get("post_state"),
                "created_inert_artifacts": value.get("created_inert_artifacts"),
            }
        ):
            raise HostConvergenceError("host receipt event chain is not failure-bound")


def _read_receipt(path: Path, owner: int) -> dict[str, object]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise HostConvergenceError("host receipt metadata is unsafe")
    raw = _read_regular(path, "host receipt")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergenceError("host receipt is invalid") from exc
    if not isinstance(value, dict) or value.get("schema") != "loom.task-image-builder-host-receipt/v1":
        raise HostConvergenceError("host receipt schema is invalid")
    if raw != _canonical(value) + b"\n":
        raise HostConvergenceError("host receipt is not canonical")
    events = value.get("events")
    if not isinstance(events, list):
        raise HostConvergenceError("host receipt event chain is invalid")
    previous = ZERO_HASH
    for sequence, event_raw in enumerate(events):
        if not isinstance(event_raw, dict):
            raise HostConvergenceError("host receipt event chain is invalid")
        event = dict(event_raw)
        event_hash = event.pop("event_hash", None)
        if (
            event.get("sequence") != sequence
            or event.get("previous_hash") != previous
            or event_hash != _sha(_canonical(event))
        ):
            raise HostConvergenceError("host receipt event chain is invalid")
        previous = str(event_hash)
    _validate_receipt_event_binding(value)
    return value


def _desired_cgroup_has_receipt(
    receipt_dir: Path,
    policy: HostPolicy,
    digests: Mapping[str, object],
    bundle_digest: str,
    cgroup_poststate: Mapping[str, object],
    owner: int,
) -> bool:
    try:
        entries = sorted(receipt_dir.iterdir())
    except OSError as exc:
        raise HostConvergenceError("host receipt root is unavailable") from exc
    for entry in entries:
        if entry.suffix != ".json":
            continue
        receipt = _read_receipt(entry, owner)
        if (
            receipt.get("terminal_state") == "host_prepared"
            and receipt.get("activation_required") is True
            and receipt.get("cluster_id") == policy.cluster_id
            and receipt.get("slurm_node") == policy.slurm_node
            and receipt.get("bundle_digest") == bundle_digest
            and receipt.get("cgroup_poststate") == cgroup_poststate
            and all(receipt.get(key) == value for key, value in digests.items())
        ):
            return True
    return False


def _rollback(
    policy: HostPolicy,
    backend: HostBackend,
    document: dict[str, object],
    receipt_path: Path,
) -> bool:
    prestate = document.get("cgroup_prestate")
    pre_facts = document.get("pre_state")
    if not isinstance(prestate, dict) or not isinstance(pre_facts, dict):
        raise HostConvergenceError("host receipt rollback state is incomplete")
    quota_prestate = pre_facts.get("quota_state")
    if not isinstance(quota_prestate, dict):
        raise HostConvergenceError("host receipt quota rollback state is incomplete")
    errors: list[str] = []
    try:
        _restore_cgroup(policy, prestate)
    except (OSError, HostConvergenceError) as exc:
        errors.append(str(exc))
    try:
        backend.restore_quota(policy, quota_prestate)
    except HostConvergenceError as exc:
        errors.append(str(exc))
    try:
        quota_verified = backend.quota_matches(policy, quota_prestate)
    except HostConvergenceError as exc:
        errors.append(str(exc))
        quota_verified = False
    verified = _cgroup_matches(policy, prestate) and quota_verified
    if errors:
        verified = False
    source_state = document.get("terminal_state")
    if source_state not in {"applying", "host_prepared"}:
        raise HostConvergenceError("host receipt rollback source state is invalid")
    document["activation_required"] = False
    document["rollback_verified"] = verified
    document["rollback_source_state"] = source_state
    document["terminal_state"] = "rolled_back" if verified else "rollback_failed"
    _event(
        document,
        str(document["terminal_state"]),
        {"verified": verified, "errors": errors, "source_state": source_state},
    )
    _write_document(receipt_path, document, exclusive=False)
    return verified


def _converge_host_once(
    action: str,
    cluster_id: str,
    slurm_node: str,
    bundle: Path,
    receipt_dir: Path,
    backend: HostBackend,
    paths: HostReleasePaths = DEFAULT_PATHS,
    *,
    operation_id: str | None = None,
    effective_uid: int | None = None,
    required_owner: int = 0,
) -> dict[str, object]:
    if action not in {"plan", "check", "apply", "rollback"}:
        raise HostConvergenceError("host convergence action is invalid")
    policy, policy_payload = _load_policy(paths.policy, cluster_id, slurm_node)
    _, authority_payload = _bound_release_payload(paths, policy)
    selected_operation = operation_id or str(uuid.uuid4())
    try:
        parsed_operation = uuid.UUID(selected_operation)
    except ValueError as exc:
        raise HostConvergenceError("operation ID is invalid") from exc
    if parsed_operation.version != 4 or str(parsed_operation) != selected_operation:
        raise HostConvergenceError("operation ID is invalid")
    receipt_path = receipt_dir / f"{selected_operation}.json"
    owner = os.geteuid() if effective_uid is None else effective_uid
    digests = _digests(paths, policy_payload, policy, authority_payload)

    if action == "rollback":
        if owner != required_owner:
            raise HostConvergenceError("host rollback requires root")
        _validate_receipt_root(receipt_dir, required_owner)
        document = _read_receipt(receipt_path, required_owner)
        if document.get("cluster_id") != cluster_id or document.get("slurm_node") != slurm_node:
            raise HostConvergenceError("host receipt binding does not match rollback target")
        if any(document.get(key) != value for key, value in digests.items()):
            raise HostConvergenceError("host receipt candidate or release digest is invalid")
        terminal_state = document.get("terminal_state")
        if terminal_state not in {"applying", "host_prepared"}:
            raise HostConvergenceError("host receipt is not a recoverable rollback source")
        rollback_facts = (
            backend.preflight(policy, bundle)
            if terminal_state == "host_prepared"
            else backend.recovery_preflight(policy, bundle)
        )
        _validate_facts(policy, rollback_facts)
        if rollback_facts.bundle_digest != document.get("bundle_digest"):
            raise HostConvergenceError("host receipt bundle digest is invalid")
        rollback_cgroup_state, rollback_cgroup = _inspect_cgroup(policy)
        if terminal_state == "host_prepared":
            if rollback_facts.as_dict() != document.get("post_state"):
                raise HostConvergenceError("current host post-state does not match receipt")
            if rollback_cgroup_state != "desired":
                raise HostConvergenceError(
                    "current cgroup state is not the receipted desired state"
                )
            if (
                rollback_cgroup.get("mode") != 0o644
                or rollback_cgroup.get("uid") != required_owner
                or rollback_cgroup.get("gid") != required_owner
            ):
                raise HostConvergenceError(
                    "desired cgroup configuration metadata is unsafe"
                )
            if rollback_cgroup != document.get("cgroup_poststate"):
                raise HostConvergenceError(
                    "current cgroup post-state does not match receipt"
                )
        else:
            cgroup_prestate = document.get("cgroup_prestate")
            pre_facts = document.get("pre_state")
            if not isinstance(cgroup_prestate, dict) or not isinstance(pre_facts, dict):
                raise HostConvergenceError("interrupted host rollback state is incomplete")
            if rollback_cgroup_state == "observed":
                if not _cgroup_matches(policy, cgroup_prestate):
                    raise HostConvergenceError(
                        "interrupted host cgroup pre-state does not match receipt"
                    )
            elif rollback_cgroup_state == "desired":
                if (
                    rollback_cgroup.get("mode") != 0o644
                    or rollback_cgroup.get("uid") != required_owner
                    or rollback_cgroup.get("gid") != required_owner
                ):
                    raise HostConvergenceError(
                        "desired cgroup configuration metadata is unsafe"
                    )
            else:
                raise HostConvergenceError("interrupted host cgroup state is unsafe")
            quota_prestate = pre_facts.get("quota_state")
            if not isinstance(quota_prestate, dict):
                raise HostConvergenceError("interrupted host quota state is incomplete")
            _validate_interrupted_quota_state(
                policy,
                quota_prestate,
                rollback_facts.quota_state,
            )
        if not _rollback(policy, backend, document, receipt_path):
            raise HostConvergenceError("host rollback failed")
        return {**_inert(), "state": "rolled_back", "receipt": str(receipt_path)}

    facts = backend.preflight(policy, bundle)
    _validate_facts(policy, facts)
    cgroup_state, cgroup_prestate = _inspect_cgroup(policy)
    if cgroup_state == "desired" and (
        cgroup_prestate.get("mode") != 0o644
        or cgroup_prestate.get("uid") != required_owner
        or cgroup_prestate.get("gid") != required_owner
    ):
        raise HostConvergenceError("desired cgroup configuration metadata is unsafe")
    if cgroup_state == "desired":
        _validate_receipt_root(receipt_dir, required_owner)
        if not _desired_cgroup_has_receipt(
            receipt_dir,
            policy,
            digests,
            facts.bundle_digest,
            cgroup_prestate,
            required_owner,
        ):
            raise HostConvergenceError(
                "desired cgroup state has no matching successful receipt"
            )
    changes = _changes(facts, cgroup_state)
    if action == "plan":
        return {**_inert(), **digests, "changes": changes, "state": "planned"}
    if action == "check":
        if changes:
            raise HostConvergenceError("host prerequisites are not prepared")
        return {**_inert(), **digests, "changes": [], "state": "host_prepared"}

    if owner != required_owner:
        raise HostConvergenceError("host convergence requires root")
    _validate_receipt_root(receipt_dir, required_owner)
    document = _receipt_document(
        policy,
        digests,
        selected_operation,
        facts,
        cgroup_prestate,
    )
    _write_document(receipt_path, document, exclusive=True)
    try:
        _event(document, "intent", {"changes": changes})
        _write_document(receipt_path, document, exclusive=False)
        if dict(facts.packages) != PACKAGE_VERSIONS or not facts.helpers_exact:
            backend.install_packages(policy, bundle)
        if cgroup_state != "desired":
            _apply_cgroup(policy, required_owner)
        if not facts.quota_exact:
            backend.apply_quota(policy)
        if not facts.identity_exact or not facts.runtime_exact:
            backend.install_inert_prerequisites(policy, bundle)
        observed = backend.observe(policy, bundle)
        _validate_facts(policy, observed)
        observed_cgroup, observed_cgroup_state = _inspect_cgroup(policy)
        if not observed.prepared_except_cgroup() or observed_cgroup != "desired":
            raise HostConvergenceError("host prerequisite readback did not converge")
        document["post_state"] = observed.as_dict()
        document["cgroup_poststate"] = observed_cgroup_state
        created: list[str] = []
        if dict(facts.packages) != PACKAGE_VERSIONS or not facts.helpers_exact:
            created.append("packages")
        if not facts.identity_exact:
            created.append("identity")
        if not facts.runtime_exact:
            created.append("runtime")
        if not facts.quota_exact:
            created.append("project_quota")
        if cgroup_state != "desired":
            created.append("node_local_cgroup")
        document["created_inert_artifacts"] = created
        document["activation_required"] = True
        document["terminal_state"] = "host_prepared"
        _event(
            document,
            "post_state",
            {"facts": observed.as_dict(), "cgroup": observed_cgroup_state},
        )
        _event(
            document,
            "host_prepared",
            {"activation_required": True, "created_inert_artifacts": created},
        )
        _write_document(receipt_path, document, exclusive=False)
    except (OSError, HostConvergenceError) as exc:
        document["failure"] = str(exc)
        try:
            failed_observation = backend.observe(policy, bundle)
            document["post_state"] = failed_observation.as_dict()
            failure_created: list[str] = []
            if dict(failed_observation.packages) != dict(facts.packages):
                failure_created.append("packages")
            if failed_observation.identity_exact and not facts.identity_exact:
                failure_created.append("identity")
            if failed_observation.runtime_exact and not facts.runtime_exact:
                failure_created.append("runtime")
            document["created_inert_artifacts"] = failure_created
        except (OSError, HostConvergenceError) as observation_error:
            document["post_state"] = None
            document["post_readback_error"] = str(observation_error)
        _event(
            document,
            "apply_failed",
            {
                "error": str(exc),
                "post_state": document["post_state"],
                "created_inert_artifacts": document["created_inert_artifacts"],
            },
        )
        try:
            _write_document(receipt_path, document, exclusive=False)
        except (OSError, HostConvergenceError) as receipt_error:
            document["failure_receipt_write_error"] = str(receipt_error)
            _event(
                document,
                "receipt_write_failed",
                {"error": str(receipt_error), "continuing_with_rollback": True},
            )
        if _rollback(policy, backend, document, receipt_path):
            raise HostConvergenceError(f"host convergence failed and rolled back: {exc}") from exc
        raise HostConvergenceError(f"host convergence rollback failed: {exc}") from exc
    return {
        **_inert(),
        "state": "host_prepared",
        "activation_required": True,
        "receipt": str(receipt_path),
    }


def converge_host(
    action: str,
    cluster_id: str,
    slurm_node: str,
    bundle: Path,
    receipt_dir: Path,
    backend: HostBackend,
    paths: HostReleasePaths = DEFAULT_PATHS,
    *,
    operation_id: str | None = None,
    effective_uid: int | None = None,
    required_owner: int = 0,
) -> dict[str, object]:
    try:
        return _converge_host_once(
            action,
            cluster_id,
            slurm_node,
            bundle,
            receipt_dir,
            backend,
            paths,
            operation_id=operation_id,
            effective_uid=effective_uid,
            required_owner=required_owner,
        )
    finally:
        backend.close()


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class SystemHostBackend:
    """Bounded Linux implementation; every mutating command is an explicit vector."""

    def __init__(self, paths: HostReleasePaths = DEFAULT_PATHS) -> None:
        self.paths = paths
        self._verified: host_release.VerifiedHostBundle | None = None

    @staticmethod
    def _run(args: tuple[str, ...]) -> CommandResult:
        try:
            result = subprocess.run(
                args,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                env={**os.environ, "LC_ALL": "C"},
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HostConvergenceError("host command failed to execute") from exc
        if len(result.stdout) > 4 * 1024 * 1024 or len(result.stderr) > 4 * 1024 * 1024:
            raise HostConvergenceError("host command output exceeds its limit")
        return CommandResult(
            result.returncode,
            result.stdout.decode(errors="strict"),
            result.stderr.decode(errors="replace"),
        )

    def _verify_bundle(self, policy: HostPolicy, bundle: Path) -> str:
        if self._verified is not None:
            if self._verified.architecture != policy.architecture:
                raise HostConvergenceError("verified bundle architecture changed")
            return self._verified.bundle_digest
        release = host_release.load_host_release(_authority_release_path(self.paths, policy))

        class Adapter:
            def run(
                self,
                args: Sequence[str],
                *,
                input_bytes: bytes | None = None,
            ) -> host_release.CommandResult:
                del self, input_bytes
                result = SystemHostBackend._run(tuple(args))
                return host_release.CommandResult(result.returncode, result.stdout, result.stderr)

        try:
            verified = host_release.verify_host_bundle(
                bundle,
                release,
                policy.architecture,
                Adapter(),
                runtime_manifest_path=self.paths.runtime_manifest,
            )
        except host_release.HostReleaseError as exc:
            raise HostConvergenceError("offline host bundle verification failed") from exc
        self._verified = verified
        return verified.bundle_digest

    def _node_binding(self, policy: HostPolicy) -> None:
        result = self._run(("/usr/bin/scontrol", "show", "node", policy.slurm_node, "-o"))
        if result.returncode != 0:
            raise HostConvergenceError("Slurm node binding is unavailable")
        fields = dict(
            token.split("=", 1)
            for token in shlex.split(result.stdout)
            if "=" in token
        )
        if fields.get("NodeName") != policy.slurm_node:
            raise HostConvergenceError("Slurm node binding failed")
        node_addr = fields.get("NodeAddr")
        node_hostname = fields.get("NodeHostName")
        if not node_addr or not node_hostname:
            raise HostConvergenceError("Slurm node binding is incomplete")
        try:
            resolved = {item[4][0] for item in socket.getaddrinfo(node_addr, None)}
        except socket.gaierror as exc:
            raise HostConvergenceError("Slurm node address cannot be resolved") from exc
        addresses = self._run(("/usr/sbin/ip", "-j", "address", "show", "scope", "global"))
        aliases = self._run(("/bin/hostname", "-A"))
        if addresses.returncode != 0 or aliases.returncode != 0:
            raise HostConvergenceError("local host identity readback is unavailable")
        try:
            interfaces = json.loads(addresses.stdout)
            local = {
                address["local"]
                for interface in interfaces
                for address in interface.get("addr_info", [])
                if address.get("family") in {"inet", "inet6"} and "local" in address
            }
        except (TypeError, KeyError, json.JSONDecodeError) as exc:
            raise HostConvergenceError("local address readback is invalid") from exc
        hostnames = {
            socket.gethostname().casefold(),
            socket.getfqdn().casefold(),
            *(item.casefold() for item in aliases.stdout.split()),
        }
        if not resolved or not resolved.issubset(local) or node_hostname.casefold() not in hostnames:
            raise HostConvergenceError("Slurm node binding does not match this host")

    @staticmethod
    def _package_states(policy: HostPolicy) -> dict[str, str | None]:
        states: dict[str, str | None] = {}
        debian_arch = {"x86_64": "amd64", "aarch64": "arm64"}[policy.architecture]
        for package, version in PACKAGE_VERSIONS.items():
            result = SystemHostBackend._run(
                (
                    "/usr/bin/dpkg-query",
                    "--show",
                    "--showformat=${db:Status-Abbrev}|${Version}|${Architecture}",
                    package,
                )
            )
            if result.returncode != 0:
                states[package] = None
                continue
            if result.stdout != f"ii |{version}|{debian_arch}":
                raise HostConvergenceError(f"installed {package} version drift is unsafe")
            states[package] = version
        return states

    @staticmethod
    def _helpers_exact() -> bool:
        for helper in ("/usr/bin/newuidmap", "/usr/bin/newgidmap"):
            try:
                metadata = Path(helper).stat()
            except OSError:
                return False
            if (
                metadata.st_uid != 0
                or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) != 0o4755
            ):
                raise HostConvergenceError("setuid mapping helper metadata is unsafe")
            capabilities = SystemHostBackend._run(("/usr/sbin/getcap", helper))
            if capabilities.returncode != 0 or capabilities.stdout:
                raise HostConvergenceError("setuid mapping helper capabilities are unsafe")
        return True

    @staticmethod
    def _identity_exact(
        *,
        passwd_path: Path = Path("/etc/passwd"),
        group_path: Path = Path("/etc/group"),
        subuid_path: Path = Path("/etc/subuid"),
        subgid_path: Path = Path("/etc/subgid"),
        required_owner: int = 0,
    ) -> bool:
        def database_lines(path: Path, label: str) -> list[str]:
            payload = _read_regular(
                path,
                f"{label} database",
                required_owner=required_owner,
                reject_group_world_write=True,
            )
            try:
                return payload.decode("utf-8").splitlines()
            except UnicodeDecodeError as exc:
                raise HostConvergenceError(f"{label} database is malformed") from exc

        passwd = database_lines(passwd_path, "passwd")
        group = database_lines(group_path, "group")
        subuid = database_lines(subuid_path, "subuid")
        subgid = database_lines(subgid_path, "subgid")
        passwd_fields = [row.split(":") for row in passwd]
        group_fields = [row.split(":") for row in group]
        if any(len(row) != 7 for row in passwd_fields) or any(
            len(row) != 4 for row in group_fields
        ):
            raise HostConvergenceError("local identity database is malformed")
        user_rows = [row for row in passwd if row.startswith("loom-builder:")]
        group_rows = [row for row in group if row.startswith("loom-task-builder:")]
        foreign_uid = [
            row for row in passwd_fields if row[2] == "993" and row[0] != "loom-builder"
        ]
        foreign_gid = [
            row for row in group_fields if row[2] == "980" and row[0] != "loom-task-builder"
        ]
        if foreign_uid or foreign_gid:
            raise HostConvergenceError("builder UID or GID conflict")
        if user_rows not in ([], ["loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin"]):
            raise HostConvergenceError("builder identity drift is unsafe")
        if group_rows not in ([], ["loom-task-builder:x:980:"]):
            raise HostConvergenceError("builder group drift is unsafe")
        if any(
            "loom-builder" in row[3].split(",")
            for row in group_fields
            if row[0] != "loom-task-builder"
        ):
            raise HostConvergenceError("builder supplementary group membership is unsafe")

        def subid_exact(rows: list[str], label: str) -> bool:
            wanted_start = 3000000
            wanted_end = wanted_start + 65536 - 1
            own: list[tuple[int, int]] = []
            for raw in rows:
                fields = raw.split(":")
                if len(fields) != 3:
                    raise HostConvergenceError(f"{label} database is malformed")
                try:
                    start, count = int(fields[1]), int(fields[2])
                except ValueError as exc:
                    raise HostConvergenceError(f"{label} database is malformed") from exc
                if count <= 0:
                    raise HostConvergenceError(f"{label} database is malformed")
                end = start + count - 1
                if fields[0] == "loom-builder":
                    own.append((start, count))
                elif start <= wanted_end and end >= wanted_start:
                    raise HostConvergenceError(f"{label} range conflict")
            if own not in ([], [(wanted_start, 65536)]):
                raise HostConvergenceError(f"{label} mapping drift is unsafe")
            return own == [(wanted_start, 65536)]

        subuid_exact = subid_exact(subuid, "subuid")
        subgid_exact = subid_exact(subgid, "subgid")
        subids_exact = subuid_exact and subgid_exact
        if user_rows:
            groups = SystemHostBackend._run(("/usr/bin/id", "-G", "loom-builder"))
            if groups.returncode != 0 or groups.stdout.strip() != "980":
                raise HostConvergenceError("builder supplementary group membership is unsafe")
        return bool(user_rows and group_rows and subids_exact)

    @staticmethod
    def _kernel_exact() -> bool:
        try:
            controllers = Path("/sys/fs/cgroup/cgroup.controllers").read_text().split()
        except OSError as exc:
            raise HostConvergenceError("cgroup v2 controllers are unavailable") from exc
        if not {"cpu", "cpuset", "io", "memory", "pids"}.issubset(controllers):
            raise HostConvergenceError("required cgroup controllers are unavailable")
        bpffs = SystemHostBackend._run(
            ("/usr/bin/findmnt", "-n", "-o", "FSTYPE,OPTIONS", "/sys/fs/bpf")
        )
        if bpffs.returncode != 0 or not bpffs.stdout.startswith("bpf ") or "mode=700" not in bpffs.stdout:
            raise HostConvergenceError("root-only bpffs is unavailable")
        return True

    @staticmethod
    def _storage_exact(policy: HostPolicy) -> bool:
        result = SystemHostBackend._run(
            (
                "/usr/bin/findmnt",
                "--json",
                "--target",
                str(policy.storage_mountpoint),
                "--output",
                "TARGET,SOURCE,FSTYPE,OPTIONS",
            )
        )
        if result.returncode != 0:
            raise HostConvergenceError("dedicated builder mount is unavailable")
        try:
            filesystems = json.loads(result.stdout)["filesystems"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise HostConvergenceError("builder mount readback is invalid") from exc
        if len(filesystems) != 1:
            raise HostConvergenceError("builder mount readback is ambiguous")
        item = filesystems[0]
        source = str(item.get("source", ""))
        options = str(item.get("options", "")).split(",")
        if (
            item.get("target") != str(policy.storage_mountpoint)
            or item.get("fstype") != "ext4"
            or "prjquota" not in options
            or not source.startswith("/dev/")
            or source.startswith("/dev/loop")
        ):
            raise HostConvergenceError("builder storage is not a dedicated ext4 quota mount")
        block = SystemHostBackend._run(
            ("/usr/bin/lsblk", "--noheadings", "--output", "TYPE", source)
        )
        if block.returncode != 0 or block.stdout.strip() not in {"disk", "part", "lvm"}:
            raise HostConvergenceError("builder storage is not backed by a block device")
        free = os.statvfs(policy.storage_mountpoint)
        if free.f_bavail * free.f_frsize < policy.scratch_bytes or free.f_favail < policy.scratch_inodes:
            raise HostConvergenceError("builder storage has insufficient free capacity")
        return True

    @staticmethod
    def _quota_state(policy: HostPolicy) -> dict[str, object]:
        try:
            metadata = policy.storage_root.lstat()
        except FileNotFoundError:
            storage_root_exists = False
            storage_root_uid: int | None = None
            storage_root_gid: int | None = None
            storage_root_mode: int | None = None
            storage_root_entries: list[str] | None = None
            project_id: int | None = None
            project_inherit = False
        except OSError as exc:
            raise HostConvergenceError("builder storage root is unavailable") from exc
        else:
            if not stat.S_ISDIR(metadata.st_mode):
                raise HostConvergenceError("builder storage root type is unsafe")
            storage_root_exists = True
            storage_root_uid = metadata.st_uid
            storage_root_gid = metadata.st_gid
            storage_root_mode = stat.S_IMODE(metadata.st_mode)
            try:
                descriptor = os.open(
                    policy.storage_root,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                )
                try:
                    storage_root_entries = sorted(os.listdir(descriptor))
                finally:
                    os.close(descriptor)
            except OSError as exc:
                raise HostConvergenceError("builder storage root is unavailable") from exc
            attributes = SystemHostBackend._run(
                ("/usr/bin/lsattr", "-pd", str(policy.storage_root))
            )
            if attributes.returncode != 0:
                raise HostConvergenceError("project quota readback is unavailable")
            fields = attributes.stdout.strip().split(maxsplit=2)
            if len(fields) != 3 or fields[2] != str(policy.storage_root):
                raise HostConvergenceError("project attribute readback is invalid")
            try:
                project_id = int(fields[0])
            except ValueError as exc:
                raise HostConvergenceError("project attribute readback is invalid") from exc
            project_inherit = "P" in fields[1]
        quota = SystemHostBackend._run(
            (
                "/usr/sbin/repquota",
                "-v",
                "-n",
                "-p",
                "-P",
                "-O",
                "csv",
                str(policy.storage_mountpoint),
            )
        )
        if quota.returncode != 0:
            raise HostConvergenceError("project quota readback is unavailable")
        rows = list(csv.reader(quota.stdout.splitlines()))
        expected_header = [
            "Project",
            "BlockStatus",
            "FileStatus",
            "BlockUsed",
            "BlockSoftLimit",
            "BlockHardLimit",
            "BlockGrace",
            "FileUsed",
            "FileSoftLimit",
            "FileHardLimit",
            "FileGrace",
        ]
        if not rows or rows[0] != expected_header:
            raise HostConvergenceError("project quota CSV header is invalid")
        matches: list[list[str]] = []
        for row in rows[1:]:
            if not row:
                continue
            if len(row) != len(expected_header):
                raise HostConvergenceError("project quota CSV row is invalid")
            try:
                row_project = int(row[0])
            except ValueError as exc:
                raise HostConvergenceError("project quota CSV row is invalid") from exc
            if row_project == policy.project_id:
                matches.append(row)
        if len(matches) > 1:
            raise HostConvergenceError("project quota readback is ambiguous")
        quota_values = [0, 0, 0, 0, 0, 0]
        if matches:
            try:
                quota_values = [int(matches[0][index]) for index in (3, 4, 5, 7, 8, 9)]
            except ValueError as exc:
                raise HostConvergenceError("project quota CSV row is invalid") from exc
            if any(value < 0 for value in quota_values):
                raise HostConvergenceError("project quota CSV row is invalid")
        return {
            "storage_root_exists": storage_root_exists,
            "storage_root_uid": storage_root_uid,
            "storage_root_gid": storage_root_gid,
            "storage_root_mode": storage_root_mode,
            "storage_root_entries": storage_root_entries,
            "project_id": project_id,
            "project_inherit": project_inherit,
            "block_used": quota_values[0],
            "block_soft_limit": quota_values[1],
            "block_hard_limit": quota_values[2],
            "inode_used": quota_values[3],
            "inode_soft_limit": quota_values[4],
            "inode_hard_limit": quota_values[5],
        }

    @staticmethod
    def _runtime_exact(
        runtime_manifest: Path,
        architecture: str,
        *,
        install_base: Path = Path("/opt/loom-task-builder"),
        required_owner: int = 0,
    ) -> bool:
        manifest_payload = _read_regular(runtime_manifest, "runtime manifest")
        try:
            manifest = json.loads(manifest_payload)
            release_name = manifest["release"]
            binaries = manifest["architectures"][architecture]["binaries"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise HostConvergenceError("runtime manifest is invalid") from exc
        if (
            manifest.get("schema") != "loom.task-image-builder-rootless-runtime/v1"
            or not isinstance(release_name, str)
            or not release_name
            or not isinstance(binaries, dict)
            or not binaries
            or not all(
                isinstance(name, str)
                and name
                and "/" not in name
                and isinstance(digest, str)
                and len(digest) == 64
                for name, digest in binaries.items()
            )
        ):
            raise HostConvergenceError("runtime manifest is invalid")
        release = install_base / "releases" / release_name
        receipt = release / "receipt.json"
        binary_dir = release / "bin"
        current = install_base / "current"
        release_present = release.exists() or release.is_symlink()
        current_present = current.exists() or current.is_symlink()
        if not release_present and not current_present:
            return False
        try:
            release_metadata = release.lstat()
            binary_dir_metadata = binary_dir.lstat()
            current_metadata = current.lstat()
        except OSError as exc:
            raise HostConvergenceError("installed rootless runtime drift is unsafe") from exc
        if (
            not stat.S_ISDIR(release_metadata.st_mode)
            or release_metadata.st_uid != required_owner
            or release_metadata.st_gid != required_owner
            or stat.S_IMODE(release_metadata.st_mode) != 0o755
            or not stat.S_ISDIR(binary_dir_metadata.st_mode)
            or binary_dir_metadata.st_uid != required_owner
            or binary_dir_metadata.st_gid != required_owner
            or stat.S_IMODE(binary_dir_metadata.st_mode) != 0o755
            or not stat.S_ISLNK(current_metadata.st_mode)
            or current_metadata.st_uid != required_owner
            or current_metadata.st_gid != required_owner
            or os.readlink(current) != str(Path("releases") / release_name)
        ):
            raise HostConvergenceError("installed rootless runtime drift is unsafe")
        try:
            release_entries = {item.name for item in release.iterdir()}
            binary_entries = {item.name for item in binary_dir.iterdir()}
        except OSError as exc:
            raise HostConvergenceError("installed rootless runtime drift is unsafe") from exc
        if release_entries != {"bin", "receipt.json"} or binary_entries != set(binaries):
            raise HostConvergenceError("installed rootless runtime drift is unsafe")
        for name, digest in binaries.items():
            if _installed_runtime_digest(
                binary_dir / name,
                required_owner,
                0o755,
            ) != digest:
                raise HostConvergenceError("installed rootless runtime drift is unsafe")
        expected_receipt = {
            "schema": "loom.task-image-builder-installed-runtime/v1",
            "release": release_name,
            "architecture": architecture,
            "manifest_sha256": _sha(manifest_payload),
            "binary_sha256": binaries,
        }
        if (
            _installed_runtime_digest(receipt, required_owner, 0o644)
            != _sha(_canonical(expected_receipt) + b"\n")
        ):
            raise HostConvergenceError("installed rootless runtime drift is unsafe")
        return True

    @staticmethod
    def _forbidden_sockets_safe(paths: Sequence[Path]) -> bool:
        for path in paths:
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise HostConvergenceError("forbidden host socket cannot be inspected") from exc
            if not stat.S_ISSOCK(metadata.st_mode):
                raise HostConvergenceError("forbidden host socket path has an unsafe type")
            try:
                attributes = os.listxattr(path, follow_symlinks=False)
            except OSError as exc:
                raise HostConvergenceError(
                    "forbidden host socket ACL cannot be inspected"
                ) from exc
            if "system.posix_acl_access" in attributes:
                raise HostConvergenceError("forbidden host socket has an unsafe ACL")
            mode = stat.S_IMODE(metadata.st_mode)
            accessible = (
                (metadata.st_uid == 993 and bool(mode & stat.S_IWUSR))
                or (metadata.st_gid == 980 and bool(mode & stat.S_IWGRP))
                or bool(mode & stat.S_IWOTH)
            )
            if accessible:
                raise HostConvergenceError("forbidden host socket is builder-accessible")
        return True

    def _preflight_facts(
        self,
        policy: HostPolicy,
        bundle: Path,
        *,
        allow_interrupted_quota: bool,
    ) -> HostFacts:
        if os.uname().machine != policy.architecture:
            raise HostConvergenceError("host architecture does not match policy")
        bundle_digest = self._verify_bundle(policy, bundle)
        self._node_binding(policy)
        sockets_safe = self._forbidden_sockets_safe(
            (Path("/var/run/docker.sock"), Path("/run/containerd/containerd.sock"))
        )
        packages = self._package_states(policy)
        quota_state = self._quota_state(policy)
        try:
            quota_classification = _quota_state_classification(policy, quota_state)
        except HostConvergenceError:
            if not allow_interrupted_quota:
                raise
            quota_classification = "interrupted"
        return HostFacts(
            architecture=policy.architecture,
            slurm_node=policy.slurm_node,
            bundle_digest=bundle_digest,
            packages=packages,
            helpers_exact=self._helpers_exact() if all(packages.values()) else False,
            identity_exact=self._identity_exact(),
            runtime_exact=self._runtime_exact(
                self.paths.runtime_manifest,
                policy.architecture,
            ),
            quota_exact=quota_classification == "exact",
            quota_state=quota_state,
            storage_exact=self._storage_exact(policy),
            kernel_exact=self._kernel_exact(),
            forbidden_sockets_absent=sockets_safe,
        )

    def preflight(self, policy: HostPolicy, bundle: Path) -> HostFacts:
        return self._preflight_facts(
            policy,
            bundle,
            allow_interrupted_quota=False,
        )

    def recovery_preflight(self, policy: HostPolicy, bundle: Path) -> HostFacts:
        return self._preflight_facts(
            policy,
            bundle,
            allow_interrupted_quota=True,
        )

    def install_packages(self, policy: HostPolicy, bundle: Path) -> None:
        del policy, bundle
        if self._verified is None:
            raise HostConvergenceError("verified bundle state is unavailable")
        by_name = {path.name.split("_", 1)[0]: path for path in self._verified.package_paths}
        for package in PACKAGE_ORDER:
            if self._run(("/usr/bin/dpkg", "--install", str(by_name[package]))).returncode != 0:
                raise HostConvergenceError(f"offline {package} installation failed")

    def apply_quota(self, policy: HostPolicy) -> None:
        policy.storage_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                policy.storage_root,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
        except OSError as exc:
            raise HostConvergenceError("builder jobs root cannot be secured") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise HostConvergenceError("builder jobs root type is unsafe")
            if os.listdir(descriptor):
                raise HostConvergenceError("builder jobs root is not empty")
            os.fchown(descriptor, 993, 980)
            os.fchmod(descriptor, 0o700)
        except OSError as exc:
            raise HostConvergenceError("builder jobs root cannot be secured") from exc
        finally:
            os.close(descriptor)
        commands = (
            ("/usr/bin/chattr", "-p", str(policy.project_id), "+P", str(policy.storage_root)),
            (
                "/usr/sbin/setquota",
                "-P",
                str(policy.project_id),
                "0",
                str(policy.scratch_bytes // 1024),
                "0",
                str(policy.scratch_inodes),
                str(policy.storage_mountpoint),
            ),
        )
        for command in commands:
            if self._run(command).returncode != 0:
                raise HostConvergenceError("project quota application failed")

    def install_inert_prerequisites(self, policy: HostPolicy, bundle: Path) -> None:
        del bundle
        if self._verified is None:
            raise HostConvergenceError("verified bundle state is unavailable")
        result = self._run(
            (
                str(self.paths.node_installer),
                "apply",
                policy.cluster_id,
                policy.slurm_node,
                str(self._verified.snapshot_root / "runtime"),
            )
        )
        if result.returncode != 0:
            raise HostConvergenceError("inert node prerequisite installation failed")

    def observe(self, policy: HostPolicy, bundle: Path) -> HostFacts:
        return self.preflight(policy, bundle)

    def restore_quota(
        self,
        policy: HostPolicy,
        quota_prestate: Mapping[str, object],
    ) -> None:
        preclassification = _quota_state_classification(policy, quota_prestate)
        if self._quota_state(policy) == dict(quota_prestate):
            return
        if preclassification == "exact":
            return
        storage_root_uid = cast(int, quota_prestate["storage_root_uid"])
        storage_root_gid = cast(int, quota_prestate["storage_root_gid"])
        storage_root_mode = cast(int, quota_prestate["storage_root_mode"])
        if quota_prestate["storage_root_exists"] is True and (
            not isinstance(storage_root_uid, int)
            or not isinstance(storage_root_gid, int)
            or not isinstance(storage_root_mode, int)
        ):
            raise HostConvergenceError("builder jobs root restoration receipt is invalid")
        descriptor: int | None = None
        if quota_prestate["storage_root_exists"] is True:
            try:
                descriptor = os.open(
                    policy.storage_root,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                )
            except OSError as exc:
                raise HostConvergenceError(
                    "builder jobs root is unsafe during restoration"
                ) from exc
        commands = [
            (
                "/usr/sbin/setquota",
                "-P",
                str(policy.project_id),
                str(quota_prestate["block_soft_limit"]),
                str(quota_prestate["block_hard_limit"]),
                str(quota_prestate["inode_soft_limit"]),
                str(quota_prestate["inode_hard_limit"]),
                str(policy.storage_mountpoint),
            ),
            (
                "/usr/bin/chattr",
                "-p",
                str(quota_prestate["project_id"] or 0),
                "+P" if quota_prestate["project_inherit"] is True else "-P",
                str(policy.storage_root),
            ),
        ]
        try:
            if descriptor is not None:
                self._verify_jobs_root_restoration_target(policy, descriptor)
            for command in commands:
                if self._run(command).returncode != 0:
                    raise HostConvergenceError("project quota restoration failed")
                if descriptor is not None:
                    self._verify_jobs_root_restoration_target(policy, descriptor)
            if descriptor is not None:
                try:
                    os.fchown(
                        descriptor,
                        storage_root_uid,
                        storage_root_gid,
                    )
                    self._verify_jobs_root_restoration_target(policy, descriptor)
                    os.fchmod(
                        descriptor,
                        storage_root_mode,
                    )
                except OSError as exc:
                    raise HostConvergenceError(
                        "builder jobs root metadata restoration failed"
                    ) from exc
                self._verify_jobs_root_restoration_target(policy, descriptor)
            else:
                policy.storage_root.rmdir()
        except OSError as exc:
            raise HostConvergenceError("created builder storage root is not empty") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @staticmethod
    def _verify_jobs_root_restoration_target(
        policy: HostPolicy,
        descriptor: int,
    ) -> None:
        try:
            held = os.fstat(descriptor)
            current = policy.storage_root.lstat()
            entries = os.listdir(descriptor)
        except OSError as exc:
            raise HostConvergenceError(
                "builder jobs root is unsafe during restoration"
            ) from exc
        if (
            not stat.S_ISDIR(held.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or entries
            or (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise HostConvergenceError(
                "builder jobs root is unsafe during restoration"
            )

    def quota_matches(
        self,
        policy: HostPolicy,
        expected: Mapping[str, object],
    ) -> bool:
        return self._quota_state(policy) == dict(expected)

    def close(self) -> None:
        if self._verified is None:
            return
        verified = self._verified
        self._verified = None
        try:
            verified.close()
        except host_release.HostReleaseError as exc:
            raise HostConvergenceError("verified bundle snapshot cleanup failed") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "check", "apply", "rollback"))
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--slurm-node", required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--receipt-dir", type=Path, required=True)
    parser.add_argument("--operation-id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = converge_host(
            arguments.action,
            arguments.cluster_id,
            arguments.slurm_node,
            arguments.bundle,
            arguments.receipt_dir,
            SystemHostBackend(),
            operation_id=arguments.operation_id,
        )
    except (HostConvergenceError, OSError) as exc:
        print(json.dumps({**_inert(), "state": "failed", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
