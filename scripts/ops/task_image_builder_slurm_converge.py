#!/usr/bin/env python3
"""Journal additive Phase 1 task-image-builder Slurm convergence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import stat
import subprocess
import sys
import tomllib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import task_image_builder_authority as authority  # noqa: E402
from scripts.ops import task_image_builder_slurm_readback as readback  # noqa: E402

MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_COMMAND_BYTES = 1024 * 1024
ZERO_HASH = "0" * 64
EXPECTED_IDENTITY = {
    "user": "loom-builder",
    "group": "loom-task-builder",
    "uid": 993,
    "gid": 980,
    "subid_start": 3000000,
    "subid_count": 65536,
    "home": "/nonexistent",
    "shell": "/usr/sbin/nologin",
    "forbidden_supplementary_groups": ["docker", "root", "sudo"],
}
EXPECTED_LEGACY = {
    "qos": "loom-task-image-builder",
    "reservation": "loom-task-image-builder",
    "account": "loom-staging",
    "user": "loom-rollout",
    "max_jobs_per_user": 1,
    "max_submit_jobs_per_user": 1,
    "max_wall": "04:00:00",
}


class ConvergenceError(RuntimeError):
    """The Slurm convergence request is unsafe or incomplete."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(self, args: tuple[str, ...]) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(self, args: tuple[str, ...]) -> CommandResult:
        try:
            result = subprocess.run(
                args,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ConvergenceError("Slurm command execution failed") from exc
        if len(result.stdout) > MAX_COMMAND_BYTES or len(result.stderr) > MAX_COMMAND_BYTES:
            raise ConvergenceError("Slurm command output exceeds its limit")
        try:
            stdout = result.stdout.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConvergenceError("Slurm command output is not UTF-8") from exc
        return CommandResult(
            result.returncode,
            stdout,
            result.stderr.decode("utf-8", errors="replace"),
        )


@dataclass(frozen=True)
class ReleasePaths:
    policy: Path
    converger: Path
    controller_installer: Path
    readback: Path
    wrapper: Path
    durable_backup: Path


DEFAULT_PATHS = ReleasePaths(
    policy=ROOT / "deploy/task-image-builder/prerequisites-v1.toml",
    converger=ROOT / "deploy/slurm/converge-loom-task-image-builder-prerequisites.sh",
    controller_installer=(
        ROOT / "deploy/slurm/install-loom-task-image-builder-controller-identity.sh"
    ),
    readback=ROOT / "scripts/ops/task_image_builder_slurm_readback.py",
    wrapper=Path(__file__).resolve(),
    durable_backup=Path(
        "/var/lib/loom-task-builder/slurm-authority/slurm.conf.before-loom-task-builder"
    ),
)


@dataclass(frozen=True)
class ClusterPolicy:
    raw: Mapping[str, object]
    cluster_id: str
    slurm_cluster: str
    architecture: str
    controller: str
    trial_partition: str
    builder_partition: str
    trial_anchor: str
    builder_line: str
    slurm_config: Path
    account: str
    qos: str
    builder_user: str
    cpus: int
    memory_mib: int
    wall_time: str
    max_jobs: int
    max_submit: int
    legacy_base_qos: str
    legacy_reservation_node: str
    legacy_reservation_partition: str

    @property
    def association_name(self) -> str:
        return (
            f"{self.slurm_cluster}/{self.account}/{self.builder_user}/"
            f"{self.builder_partition}"
        )


@dataclass(frozen=True)
class SlurmSnapshot:
    partition: Mapping[str, object] | None
    account: Mapping[str, object] | None
    qos: Mapping[str, object] | None
    association: Mapping[str, object] | None
    legacy: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "partition": self.partition,
            "account": self.account,
            "qos": self.qos,
            "association": self.association,
            "legacy": self.legacy,
        }

    def is_converged(self) -> bool:
        return all(
            item is not None
            for item in (self.partition, self.account, self.qos, self.association)
        )


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _inert_output() -> dict[str, object]:
    return {
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
    }


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_regular(path: Path, label: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ConvergenceError(f"{label} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_FILE_BYTES:
            raise ConvergenceError(f"{label} is unsafe")
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
        if len(payload) > MAX_FILE_BYTES or (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise ConvergenceError(f"{label} changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ConvergenceError(f"{label} is invalid")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ConvergenceError(f"{label} is invalid")
    return value


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConvergenceError(f"{label} is invalid")
    return value


def _load_policy(path: Path, cluster_id: str) -> tuple[ClusterPolicy, bytes]:
    payload = _read_regular(path, "prerequisite policy")
    try:
        raw = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConvergenceError("prerequisite policy is invalid") from exc
    if raw.get("schema") != "loom.task-image-builder-prerequisites/v1":
        raise ConvergenceError("prerequisite policy schema is invalid")
    if raw.get("production_certification_allowed") is not False or raw.get(
        "certified_nodes"
    ) != []:
        raise ConvergenceError("prerequisite policy is not inert")
    if raw.get("unconditional_blockers") != ["phase2_guard_provider_release_missing"]:
        raise ConvergenceError("prerequisite policy blocker is invalid")
    if raw.get("identity") != EXPECTED_IDENTITY or raw.get("legacy_guard") != EXPECTED_LEGACY:
        raise ConvergenceError("prerequisite identity or legacy policy is invalid")
    clusters = [
        item
        for item in raw.get("clusters", [])
        if isinstance(item, dict) and item.get("id") == cluster_id
    ]
    if len(clusters) != 1:
        raise ConvergenceError("cluster policy is not unique")
    cluster = _mapping(clusters[0], "cluster policy")
    resources = _mapping(raw.get("resource_profile"), "resource profile")
    return (
        ClusterPolicy(
            raw=cluster,
            cluster_id=cluster_id,
            slurm_cluster=_string(cluster.get("slurm_cluster"), "Slurm cluster"),
            architecture=_string(cluster.get("architecture"), "cluster architecture"),
            controller=_string(cluster.get("controller"), "cluster controller"),
            trial_partition=_string(cluster.get("trial_partition"), "trial partition"),
            builder_partition=_string(
                cluster.get("builder_partition"), "builder partition"
            ),
            trial_anchor=_string(cluster.get("trial_partition_anchor"), "trial anchor"),
            builder_line=_string(cluster.get("builder_partition_line"), "builder line"),
            slurm_config=Path(_string(cluster.get("slurm_config"), "Slurm config")),
            account=_string(cluster.get("slurm_account"), "builder account"),
            qos=_string(cluster.get("slurm_qos"), "builder QoS"),
            builder_user="loom-builder",
            cpus=_integer(resources.get("cpus"), "builder CPUs"),
            memory_mib=_integer(resources.get("memory_mib"), "builder memory"),
            wall_time=_string(resources.get("wall_time"), "builder wall time"),
            max_jobs=_integer(resources.get("max_jobs_per_user"), "builder max jobs"),
            max_submit=_integer(
                resources.get("max_submit_jobs_per_user"), "builder max submit"
            ),
            legacy_base_qos=_string(cluster.get("legacy_base_qos"), "legacy base QoS"),
            legacy_reservation_node=_string(
                cluster.get("legacy_reservation_node"), "legacy reservation node"
            ),
            legacy_reservation_partition=_string(
                cluster.get("legacy_reservation_partition"),
                "legacy reservation partition",
            ),
        ),
        payload,
    )


def _required(runner: CommandRunner, command: tuple[str, ...], label: str) -> str:
    result = runner.run(command)
    if result.returncode != 0:
        raise ConvergenceError(f"{label} readback is unavailable")
    if len(result.stdout.encode("utf-8")) > MAX_COMMAND_BYTES:
        raise ConvergenceError(f"{label} readback exceeds its limit")
    return result.stdout


def _validate_controller(policy: ClusterPolicy, runner: CommandRunner) -> None:
    live = _required(runner, ("/usr/bin/scontrol", "show", "config"), "controller")
    cluster_rows = re.findall(r"^ClusterName\s*=\s*(\S+)\s*$", live, re.MULTILINE)
    controller_rows = re.findall(
        r"^SlurmctldHost\[0\]\s*=\s*([^\s(]+)(?:\([^)]*\))?\s*$",
        live,
        re.MULTILINE,
    )
    if cluster_rows != [policy.slurm_cluster] or controller_rows != [policy.controller]:
        raise ConvergenceError("controller readback does not match policy")


def _partition_state(policy: ClusterPolicy) -> Mapping[str, object] | None:
    try:
        text = _read_regular(policy.slurm_config, "Slurm configuration").decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConvergenceError("Slurm configuration is not UTF-8") from exc
    lines = text.splitlines()
    if lines.count(policy.trial_anchor) != 1:
        raise ConvergenceError("trial partition anchor drift is unsafe")
    named = [
        line
        for line in lines
        if line == f"PartitionName={policy.builder_partition}"
        or line.startswith(f"PartitionName={policy.builder_partition} ")
    ]
    exact_count = lines.count(policy.builder_line)
    if not named and exact_count == 0:
        return None
    if named == [policy.builder_line] and exact_count == 1:
        return {"name": policy.builder_partition, "line": policy.builder_line}
    raise ConvergenceError("builder partition drift is unsafe")


def _account_state(
    policy: ClusterPolicy,
    runner: CommandRunner,
) -> Mapping[str, object] | None:
    account_raw = _required(
        runner,
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "account",
            "where",
            f"name={policy.account}",
            "format=Account",
        ),
        "rootless account",
    )
    try:
        return readback.verify_account(
            account_raw,
            name=policy.account,
            allow_absent=True,
        )
    except readback.ReadbackError as exc:
        raise ConvergenceError("rootless account readback drift is unsafe") from exc


def _qos_state(
    policy: ClusterPolicy,
    runner: CommandRunner,
) -> Mapping[str, object] | None:
    qos_raw = _required(
        runner,
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "qos",
            "where",
            f"name={policy.qos}",
            "format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES",
        ),
        "rootless QoS",
    )
    try:
        return readback.verify_qos(
            qos_raw,
            name=policy.qos,
            flags=("DenyOnLimit",),
            priority=0,
            max_jobs_per_user=policy.max_jobs,
            max_submit_jobs_per_user=policy.max_submit,
            max_wall=policy.wall_time,
            group_tres={"cpu": policy.cpus, "memory_mib": policy.memory_mib, "nodes": 1},
            allow_absent=True,
        )
    except readback.ReadbackError as exc:
        raise ConvergenceError("rootless QoS readback drift is unsafe") from exc


def _association_state(
    policy: ClusterPolicy,
    runner: CommandRunner,
) -> Mapping[str, object] | None:
    association_raw = _required(
        runner,
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "association",
            "where",
            f"cluster={policy.slurm_cluster}",
            f"account={policy.account}",
            f"user={policy.builder_user}",
            f"partition={policy.builder_partition}",
            "format=Cluster,Account,User,Partition,QOS,DefaultQOS",
        ),
        "rootless association",
    )
    try:
        return readback.verify_association(
            association_raw,
            cluster=policy.slurm_cluster,
            account=policy.account,
            user=policy.builder_user,
            partition=policy.builder_partition,
            qos=(policy.qos,),
            default_qos=policy.qos,
            allow_absent=True,
        )
    except readback.ReadbackError as exc:
        raise ConvergenceError("rootless association readback drift is unsafe") from exc


def _legacy_state(policy: ClusterPolicy, runner: CommandRunner) -> Mapping[str, object]:
    qos_raw = _required(
        runner,
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "qos",
            "where",
            f"name={EXPECTED_LEGACY['qos']}",
            "format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES",
        ),
        "legacy QoS",
    )
    association_raw = _required(
        runner,
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "association",
            "where",
            f"cluster={policy.slurm_cluster}",
            f"account={EXPECTED_LEGACY['account']}",
            f"user={EXPECTED_LEGACY['user']}",
            "format=Cluster,Account,User,QOS,DefaultQOS",
        ),
        "legacy association",
    )
    reservation_raw = _required(
        runner,
        (
            "/usr/bin/scontrol",
            "show",
            "reservation",
            str(EXPECTED_LEGACY["reservation"]),
            "-o",
        ),
        "legacy reservation",
    )
    try:
        qos = readback.verify_qos(
            qos_raw,
            name=str(EXPECTED_LEGACY["qos"]),
            flags=("DenyOnLimit",),
            priority=0,
            max_jobs_per_user=1,
            max_submit_jobs_per_user=1,
            max_wall=str(EXPECTED_LEGACY["max_wall"]),
            group_tres={},
            allow_absent=False,
        )
        association = readback.verify_association(
            association_raw,
            cluster=policy.slurm_cluster,
            account=str(EXPECTED_LEGACY["account"]),
            user=str(EXPECTED_LEGACY["user"]),
            partition=None,
            qos=(policy.legacy_base_qos, str(EXPECTED_LEGACY["qos"])),
            default_qos=policy.legacy_base_qos,
            allow_absent=False,
        )
        reservation = readback.verify_reservation(
            reservation_raw,
            name=str(EXPECTED_LEGACY["reservation"]),
            node=policy.legacy_reservation_node,
            node_count=1,
            partition=policy.legacy_reservation_partition,
            users=(str(EXPECTED_LEGACY["user"]),),
            accounts=(str(EXPECTED_LEGACY["account"]),),
            state="ACTIVE",
            flags=("IGNORE_JOBS", "SPEC_NODES"),
        )
    except readback.ReadbackError as exc:
        raise ConvergenceError("legacy Slurm readback drift is unsafe") from exc
    return {"qos": qos, "association": association, "reservation": reservation}


def _snapshot(policy: ClusterPolicy, runner: CommandRunner) -> SlurmSnapshot:
    _validate_controller(policy, runner)
    partition = _partition_state(policy)
    account = _account_state(policy, runner)
    qos = _qos_state(policy, runner)
    association = _association_state(policy, runner)
    legacy = _legacy_state(policy, runner)
    return SlurmSnapshot(partition, account, qos, association, legacy)


def _observe_post_state(
    policy: ClusterPolicy,
    runner: CommandRunner,
) -> tuple[dict[str, object], dict[str, str], SlurmSnapshot | None]:
    errors: dict[str, str] = {}
    partition: Mapping[str, object] | None = None
    account: Mapping[str, object] | None = None
    qos: Mapping[str, object] | None = None
    association: Mapping[str, object] | None = None
    legacy: Mapping[str, object] | None = None

    try:
        _validate_controller(policy, runner)
    except ConvergenceError as exc:
        errors["controller"] = str(exc)
    try:
        partition = _partition_state(policy)
    except ConvergenceError as exc:
        errors["partition"] = str(exc)
    try:
        account = _account_state(policy, runner)
    except ConvergenceError as exc:
        errors["account"] = str(exc)
    try:
        qos = _qos_state(policy, runner)
    except ConvergenceError as exc:
        errors["qos"] = str(exc)
    try:
        association = _association_state(policy, runner)
    except ConvergenceError as exc:
        errors["association"] = str(exc)
    try:
        legacy = _legacy_state(policy, runner)
    except ConvergenceError as exc:
        errors["legacy"] = str(exc)

    state: dict[str, object] = {
        "partition": partition,
        "account": account,
        "qos": qos,
        "association": association,
        "legacy": legacy,
    }
    snapshot = (
        SlurmSnapshot(partition, account, qos, association, legacy)
        if not errors and legacy is not None
        else None
    )
    return state, errors, snapshot


def _fingerprint(value: object) -> str:
    return _sha256(_canonical_json(value))


def _missing(snapshot: SlurmSnapshot) -> list[str]:
    return [
        name
        for name, value in (
            ("partition", snapshot.partition),
            ("account", snapshot.account),
            ("qos", snapshot.qos),
            ("association", snapshot.association),
        )
        if value is None
    ]


def _created_objects(
    policy: ClusterPolicy,
    before: SlurmSnapshot,
    after: Mapping[str, object],
) -> list[dict[str, str]]:
    names = {
        "partition": policy.builder_partition,
        "account": policy.account,
        "qos": policy.qos,
        "association": policy.association_name,
    }
    return [
        {"kind": kind, "name": names[kind]}
        for kind, before_value, after_value in (
            ("partition", before.partition, after.get("partition")),
            ("account", before.account, after.get("account")),
            ("qos", before.qos, after.get("qos")),
            ("association", before.association, after.get("association")),
        )
        if before_value is None and after_value is not None
    ]


def _release_digests(
    paths: ReleasePaths,
    policy_payload: bytes,
    policy: ClusterPolicy,
) -> dict[str, object]:
    del paths
    try:
        binding = authority.load_authority_binding(ROOT)
    except authority.AuthorityError as exc:
        raise ConvergenceError("authority component binding is invalid") from exc
    candidate_components = {"policy": _sha256(policy_payload), **binding.as_dict()}
    return {
        "candidate_digest": _fingerprint(candidate_components),
        "policy_digest": _sha256(policy_payload),
        "controller_digest": binding.component_digests["controller_identity_installer"],
        "cluster_digest": _fingerprint(policy.raw),
        **binding.as_dict(),
    }


def _backup_digest(path: Path) -> str | None:
    if not path.exists() and not path.is_symlink():
        return None
    return _sha256(_read_regular(path, "durable Slurm backup"))


class ReceiptJournal:
    def __init__(self, path: Path, document: dict[str, object]) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise ConvergenceError("Slurm receipt cannot be created exclusively") from exc
        self.path = path
        self.document = document
        self._owner = os.fstat(descriptor).st_uid
        try:
            self._write_payload(descriptor)
        finally:
            os.close(descriptor)
        try:
            directory = os.open(
                path.parent,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            raise

    def _write_payload(self, descriptor: int) -> None:
        payload = _canonical_json(self.document) + b"\n"
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ConvergenceError("Slurm receipt write failed")
            view = view[written:]
        os.fsync(descriptor)

    def _persist(self) -> None:
        metadata = self.path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self._owner
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise ConvergenceError("Slurm receipt metadata is unsafe")
        temporary = self.path.parent / f".{self.path.name}.{uuid.uuid4()}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        try:
            temporary_metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(temporary_metadata.st_mode)
                or temporary_metadata.st_uid != self._owner
                or stat.S_IMODE(temporary_metadata.st_mode) != 0o600
                or temporary_metadata.st_nlink != 1
            ):
                raise ConvergenceError("Slurm receipt revision metadata is unsafe")
            self._write_payload(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, self.path)
            directory = os.open(
                self.path.parent,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def append(self, event_type: str, data: Mapping[str, object]) -> None:
        events = self.document["events"]
        if not isinstance(events, list):
            raise ConvergenceError("Slurm receipt event list is invalid")
        previous = ZERO_HASH if not events else str(events[-1]["event_hash"])
        event: dict[str, object] = {
            "sequence": len(events),
            "type": event_type,
            "previous_hash": previous,
            "data": dict(data),
        }
        event["event_hash"] = _sha256(_canonical_json(event))
        events.append(event)
        self._persist()

    def close(self) -> None:
        return None


def _validate_authority(
    receipt_dir: Path,
    policy: ClusterPolicy,
    *,
    effective_uid: int,
    controller_host: str,
    host_arch: str,
    required_receipt_owner: int,
) -> None:
    if effective_uid != required_receipt_owner:
        raise ConvergenceError("Slurm convergence requires controller root")
    if controller_host != policy.controller or host_arch != policy.architecture:
        raise ConvergenceError("local controller authority does not match policy")
    try:
        metadata = receipt_dir.lstat()
    except OSError as exc:
        raise ConvergenceError("Slurm receipt root is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != required_receipt_owner
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ConvergenceError("Slurm receipt root must be owner-only")


def _operation_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ConvergenceError("operation ID is invalid") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise ConvergenceError("operation ID is invalid")
    return value


def converge_slurm(
    action: str,
    cluster_id: str,
    receipt_dir: Path,
    runner: CommandRunner,
    paths: ReleasePaths = DEFAULT_PATHS,
    *,
    operation_id: str | None = None,
    effective_uid: int | None = None,
    controller_host: str | None = None,
    host_arch: str | None = None,
    required_receipt_owner: int = 0,
) -> dict[str, object]:
    if action not in {"plan", "check", "apply"}:
        raise ConvergenceError("Slurm convergence action is invalid")
    policy, policy_payload = _load_policy(paths.policy, cluster_id)
    if (controller_host or socket.gethostname().split(".", 1)[0]) != policy.controller:
        raise ConvergenceError("controller hostname does not match policy")
    if (host_arch or os.uname().machine) != policy.architecture:
        raise ConvergenceError("controller architecture does not match policy")
    before = _snapshot(policy, runner)
    digests = _release_digests(paths, policy_payload, policy)
    changes = _missing(before)
    if action == "plan":
        return {
            **digests,
            **_inert_output(),
            "changes": changes,
            "cluster_id": cluster_id,
            "state": "planned",
        }
    if action == "check":
        if changes:
            raise ConvergenceError("task-image builder Slurm prerequisites are not converged")
        delegated_check = runner.run((str(paths.converger), "check", cluster_id))
        if delegated_check.returncode != 0:
            raise ConvergenceError("full Slurm prerequisite check failed")
        return {
            **digests,
            **_inert_output(),
            "changes": [],
            "cluster_id": cluster_id,
            "state": "converged",
        }

    actual_uid = os.geteuid() if effective_uid is None else effective_uid
    actual_controller = controller_host or socket.gethostname().split(".", 1)[0]
    actual_arch = host_arch or os.uname().machine
    _validate_authority(
        receipt_dir,
        policy,
        effective_uid=actual_uid,
        controller_host=actual_controller,
        host_arch=actual_arch,
        required_receipt_owner=required_receipt_owner,
    )
    selected_operation = _operation_id(operation_id or str(uuid.uuid4()))
    receipt_path = receipt_dir / f"{selected_operation}.json"
    before_dict = before.as_dict()
    legacy_before = _fingerprint(before.legacy)
    document: dict[str, object] = {
        "schema": "loom.task-image-builder-slurm-receipt/v1",
        "operation_id": selected_operation,
        "cluster_id": cluster_id,
        **digests,
        **_inert_output(),
        "pre_state": before_dict,
        "post_state": None,
        "legacy_pre_fingerprint": legacy_before,
        "legacy_post_fingerprint": None,
        "created_objects": [],
        "durable_config_backup_digest": None,
        "command_outcome": None,
        "post_readback_error": None,
        "terminal_state": "in_progress",
        "events": [],
    }
    journal = ReceiptJournal(receipt_path, document)
    command_result: CommandResult | None = None
    after: SlurmSnapshot | None = None
    post_errors: dict[str, str] = {}
    try:
        journal.append("pre_state", {"state": before_dict})
        journal.append(
            "intent",
            {"action": "apply", "delegate": str(paths.converger), "cluster_id": cluster_id},
        )
        try:
            command_result = runner.run((str(paths.converger), "apply", cluster_id))
        except ConvergenceError as exc:
            command_result = CommandResult(127, "", str(exc))
            post_errors["delegate"] = str(exc)

        post_state, observed_errors, after = _observe_post_state(policy, runner)
        post_errors.update(observed_errors)
        legacy_observation = post_state.get("legacy")
        legacy_post = (
            _fingerprint(legacy_observation)
            if isinstance(legacy_observation, Mapping)
            else None
        )
        try:
            backup_digest = _backup_digest(paths.durable_backup)
        except ConvergenceError as exc:
            backup_digest = None
            post_errors["durable_backup"] = str(exc)

        document["post_state"] = post_state
        document["legacy_post_fingerprint"] = legacy_post
        document["created_objects"] = _created_objects(policy, before, post_state)
        document["durable_config_backup_digest"] = backup_digest
        document["command_outcome"] = {
            "returncode": command_result.returncode,
            "stdout": command_result.stdout[:MAX_COMMAND_BYTES],
            "stderr": command_result.stderr[:MAX_COMMAND_BYTES],
        }
        document["post_readback_error"] = post_errors or None
        failed = (
            command_result.returncode != 0
            or after is None
            or not after.is_converged()
            or legacy_post != legacy_before
            or bool(post_errors)
        )
        document["terminal_state"] = "failed" if failed else "converged"
        journal.append(
            "post_state",
            {
                "state": document["post_state"],
                "readback_error": document["post_readback_error"],
                "created_objects": document["created_objects"],
            },
        )
        journal.append(
            str(document["terminal_state"]),
            {
                "returncode": command_result.returncode,
                "legacy_unchanged": legacy_post == legacy_before,
            },
        )
    finally:
        journal.close()

    if document["terminal_state"] != "converged":
        raise ConvergenceError(f"Slurm convergence failed; receipt={receipt_path}")
    receipt_digest = _sha256(_read_regular(receipt_path, "Slurm receipt"))
    return {
        **_inert_output(),
        "cluster_id": cluster_id,
        "receipt": str(receipt_path),
        "receipt_digest": receipt_digest,
        "state": "converged",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "check", "apply"))
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--receipt-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = converge_slurm(
            arguments.action,
            arguments.cluster_id,
            arguments.receipt_dir,
            SubprocessCommandRunner(),
        )
    except ConvergenceError as exc:
        print(
            json.dumps(
                {**_inert_output(), "error": str(exc), "state": "failed"},
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
