"""Discover the delegated Slurm job cgroup used as Docker's cgroup parent.

Non-exclusive workers are safe only when every host-daemon container is nested
below the allocation cgroup.  This module deliberately supports only unified
cgroup v2 and fails closed when the current process is not in an identifiable,
delegated Slurm job subtree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

_JOB_ID_RE = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
_REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_MAX_WAIT_SECONDS = 60.0
_POLL_INTERVAL_SECONDS = 0.1
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_SLICE_RE = re.compile(r"^loom-job-[1-9][0-9]*-[0-9a-f]{40}\.slice$")
_DEFAULT_SLICE_RECEIPT_ROOT = Path(
    "/run/loom-developer-sandbox-slurm-policy/systemd-slices",
)
_DEFAULT_SYSTEMD_UNIT_ROOT = Path("/run/systemd/system")
_SLICE_RECEIPT_FIELDS = {
    "schema_version",
    "kind",
    "systemd_slice",
    "slice_identity_sha256",
    "unit_sha256",
    "job_id",
    "job_start_time",
    "cluster",
    "node_list",
    "account",
    "env_id",
    "resource_generation",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "cpu_max",
    "memory_max",
    "memory_swap_max_source",
    "memory_swap_max_effective",
    "pids_max",
    "cpuset_cpus",
    "cpuset_mems",
    "gpu_tres",
    "gpu_detail",
    "payload_sha256",
}


class SlurmJobCgroupError(ValueError):
    """Raised when a safe allocation-owned Docker parent cannot be proven."""


def _unified_cgroup_path(proc_cgroup: Path) -> PurePosixPath:
    try:
        rows = proc_cgroup.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SlurmJobCgroupError("cannot read the current process cgroup") from exc

    matches: list[str] = []
    for row in rows:
        hierarchy, separator, remainder = row.partition(":")
        controllers, second_separator, raw_path = remainder.partition(":")
        if separator and second_separator and hierarchy == "0" and controllers == "":
            matches.append(raw_path)
    if len(matches) != 1:
        raise SlurmJobCgroupError("exactly one unified cgroup v2 entry is required")

    raw_path = matches[0]
    if "\x00" in raw_path or "\n" in raw_path or "\r" in raw_path:
        raise SlurmJobCgroupError("the unified cgroup path is malformed")
    path = PurePosixPath(raw_path)
    if not path.is_absolute() or path == PurePosixPath("/"):
        raise SlurmJobCgroupError("the unified cgroup path must be non-root and absolute")
    if any(part in {".", ".."} for part in raw_path.split("/")):
        raise SlurmJobCgroupError("the unified cgroup path contains traversal")
    return path


def _slurm_job_scope(path: PurePosixPath, job_id: str) -> PurePosixPath:
    parts = path.parts[1:]
    slurm_marker_indexes = [
        index
        for index, part in enumerate(parts)
        if part == "slurm" or part == "slurmstepd.scope" or part.endswith("_slurmstepd.scope")
    ]
    if not slurm_marker_indexes:
        raise SlurmJobCgroupError(
            "the unified cgroup path has no identifiable Slurm job scope",
        )

    expected_job_components = {f"job_{job_id}"}
    if "_" in job_id:
        expected_job_components.add(f"job_{job_id.split('_', 1)[0]}")

    for index, part in enumerate(parts):
        if not any(marker_index < index for marker_index in slurm_marker_indexes):
            continue
        if part in expected_job_components:
            return PurePosixPath("/", *parts[: index + 1])
        if part.startswith("job_"):
            raise SlurmJobCgroupError(
                "the current Slurm cgroup belongs to a different job",
            )

    raise SlurmJobCgroupError(
        "the unified cgroup path does not bind the requested Slurm job ID",
    )


def _read_words(path: Path, *, description: str) -> set[str]:
    try:
        return set(path.read_text(encoding="utf-8").split())
    except OSError as exc:
        raise SlurmJobCgroupError(f"cannot read {description}") from exc


def _wait_for_pids_max(
    path: Path,
    *,
    expected: int,
    wait_seconds: float,
) -> None:
    """Wait only for the root guard's pids.max write to converge."""

    deadline = time.monotonic() + wait_seconds
    expected_text = str(expected)
    while True:
        if path.is_symlink():
            raise SlurmJobCgroupError("symlinks are forbidden for Slurm pids.max")
        try:
            actual = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            actual = None
        except OSError as exc:
            raise SlurmJobCgroupError("cannot read the Slurm job cgroup pids.max") from exc
        if actual == expected_text:
            return

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SlurmJobCgroupError(
                "the Slurm job cgroup pids.max did not converge to the requested "
                "aggregate limit before the bounded wait expired",
            )
        time.sleep(min(_POLL_INTERVAL_SECONDS, remaining))


def discover_slurm_job_cgroup(
    *,
    job_id: str,
    pids_max: int,
    wait_seconds: float = 0.0,
    proc_cgroup: Path = Path("/proc/self/cgroup"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> str:
    """Return an allocation-owned, delegated cgroup v2 path.

    The returned value is the host cgroup namespace path consumed by Docker's
    ``CgroupParent``.  There is intentionally no fallback to a user, systemd,
    Docker, or root cgroup.
    """

    if _JOB_ID_RE.fullmatch(job_id) is None:
        raise SlurmJobCgroupError("Slurm job ID has an unsupported format")
    if type(pids_max) is not int or pids_max <= 0:
        raise SlurmJobCgroupError("the expected job pids.max must be a positive integer")
    if (
        isinstance(wait_seconds, bool)
        or not isinstance(wait_seconds, (int, float))
        or not math.isfinite(wait_seconds)
        or wait_seconds < 0
        or wait_seconds > _MAX_WAIT_SECONDS
    ):
        raise SlurmJobCgroupError(
            f"wait_seconds must be between 0 and {_MAX_WAIT_SECONDS:g}",
        )

    process_path = _unified_cgroup_path(proc_cgroup)
    job_path = _slurm_job_scope(process_path, job_id)
    if job_path == process_path or job_path not in process_path.parents:
        raise SlurmJobCgroupError(
            "the Slurm batch process is not below a dedicated job scope",
        )

    try:
        root = cgroup_root.resolve(strict=True)
        expected_host_job_path = root / job_path.relative_to("/")
        expected_host_process_path = root / process_path.relative_to("/")
        host_job_path = expected_host_job_path.resolve(strict=True)
        host_process_path = expected_host_process_path.resolve(strict=True)
        host_job_path.relative_to(root)
        host_process_path.relative_to(host_job_path)
    except (OSError, ValueError) as exc:
        raise SlurmJobCgroupError(
            "the Slurm cgroup path is missing or escapes the cgroup v2 mount",
        ) from exc

    if host_job_path != expected_host_job_path or host_process_path != expected_host_process_path:
        raise SlurmJobCgroupError("symlinks are forbidden in the Slurm cgroup path")
    if not host_process_path.is_dir() or not host_job_path.is_dir():
        raise SlurmJobCgroupError("the Slurm cgroup path is not a directory")

    controllers = _read_words(
        host_job_path / "cgroup.controllers",
        description="Slurm job cgroup controllers",
    )
    subtree_control = _read_words(
        host_job_path / "cgroup.subtree_control",
        description="Slurm job cgroup subtree controls",
    )
    if not _REQUIRED_CONTROLLERS.issubset(controllers):
        raise SlurmJobCgroupError(
            "the Slurm job cgroup does not expose cpu, memory, and pids",
        )
    if not _REQUIRED_CONTROLLERS.issubset(subtree_control):
        raise SlurmJobCgroupError(
            "the Slurm job cgroup is not delegated for cpu, memory, and pids",
        )

    try:
        cgroup_type = (
            (host_job_path / "cgroup.type")
            .read_text(
                encoding="utf-8",
            )
            .strip()
        )
        resident_processes = (
            (host_job_path / "cgroup.procs")
            .read_text(
                encoding="utf-8",
            )
            .split()
        )
    except OSError as exc:
        raise SlurmJobCgroupError("cannot verify the Slurm job cgroup type") from exc
    if cgroup_type != "domain":
        raise SlurmJobCgroupError("the Slurm job cgroup must be a domain cgroup")
    if resident_processes:
        raise SlurmJobCgroupError(
            "the delegated Slurm job cgroup contains internal processes",
        )
    _wait_for_pids_max(
        host_job_path / "pids.max",
        expected=pids_max,
        wait_seconds=float(wait_seconds),
    )

    return job_path.as_posix()


def _bound_file(
    path: Path,
    *,
    mode: int,
    expected_uid: int,
    expected_gid: int,
) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        raw = os.read(descriptor, 1 << 20)
        if os.read(descriptor, 1):
            raise SlurmJobCgroupError("systemd slice authority file is too large")
        rebound = path.lstat()
    except OSError as exc:
        raise SlurmJobCgroupError("systemd slice authority file is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = {
        (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_uid,
            item.st_gid,
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
        for item in (lexical, opened, rebound)
    }
    if (
        len(identities) != 1
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != expected_uid
        or opened.st_gid != expected_gid
        or stat.S_IMODE(opened.st_mode) != mode
        or len(raw) != opened.st_size
    ):
        raise SlurmJobCgroupError("systemd slice authority metadata is unsafe")
    return raw


def _systemd_slice_identity(
    *,
    cluster: str,
    node: str,
    job_id: str,
    job_start_time: str,
    account: str,
    env_id: str,
    resource_generation: int,
    runtime_id: str,
    candidate_id: str,
    candidate_sha: str,
    candidate_tree: str,
) -> tuple[str, str]:
    identity = {
        "cluster": cluster,
        "node": node.lower(),
        "job_id": job_id,
        "job_start_time": job_start_time,
        "account": account,
        "env_id": env_id,
        "resource_generation": resource_generation,
        "runtime_id": runtime_id,
        "candidate_id": candidate_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
    }
    try:
        serialized = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (UnicodeEncodeError, TypeError) as exc:
        raise SlurmJobCgroupError("systemd slice identity is invalid") from exc
    digest = hashlib.sha256(serialized).hexdigest()
    unit = f"loom-job-{job_id}-{digest[:40]}.slice"
    if _SLICE_RE.fullmatch(unit) is None:
        raise SlurmJobCgroupError("systemd slice identity is invalid")
    return unit, digest


def discover_docker_cgroup_parent(
    *,
    docker_driver: str,
    job_id: str,
    pids_max: int,
    wait_seconds: float = 0.0,
    proc_cgroup: Path = Path("/proc/self/cgroup"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    cluster: str = "",
    node: str = "",
    job_start_time: str = "",
    account: str = "",
    env_id: str = "",
    resource_generation: int = 0,
    runtime_id: str = "",
    candidate_id: str = "",
    candidate_sha: str = "",
    candidate_tree: str = "",
    receipt_root: Path = _DEFAULT_SLICE_RECEIPT_ROOT,
    unit_root: Path = _DEFAULT_SYSTEMD_UNIT_ROOT,
    expected_authority_uid: int = 0,
    expected_authority_gid: int = 0,
) -> str:
    """Return a driver-compatible parent with an exact allocation proof."""

    deadline = time.monotonic() + float(wait_seconds)
    job_path = discover_slurm_job_cgroup(
        job_id=job_id,
        pids_max=pids_max,
        wait_seconds=wait_seconds,
        proc_cgroup=proc_cgroup,
        cgroup_root=cgroup_root,
    )
    if docker_driver == "cgroupfs":
        return job_path
    if docker_driver != "systemd":
        raise SlurmJobCgroupError("Docker cgroup driver is unsupported")
    if (
        not cluster
        or not node
        or not job_start_time
        or not account
        or not env_id
        or type(resource_generation) is not int
        or resource_generation < 1
        or not runtime_id
        or not candidate_id
        or not candidate_sha
        or not candidate_tree
    ):
        raise SlurmJobCgroupError("systemd slice allocation binding is incomplete")
    unit, identity_sha256 = _systemd_slice_identity(
        cluster=cluster,
        node=node,
        job_id=job_id,
        job_start_time=job_start_time,
        account=account,
        env_id=env_id,
        resource_generation=resource_generation,
        runtime_id=runtime_id,
        candidate_id=candidate_id,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    receipt_path = receipt_root / f"{unit}.json"
    while True:
        try:
            receipt_path.lstat()
        except FileNotFoundError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SlurmJobCgroupError(
                    "systemd slice receipt did not appear before the bounded wait expired",
                ) from None
            time.sleep(min(_POLL_INTERVAL_SECONDS, remaining))
            continue
        except OSError as exc:
            raise SlurmJobCgroupError("systemd slice receipt is unavailable") from exc
        raw = _bound_file(
            receipt_path,
            mode=0o444,
            expected_uid=expected_authority_uid,
            expected_gid=expected_authority_gid,
        )
        break
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SlurmJobCgroupError("systemd slice receipt is invalid") from exc
    if not isinstance(receipt, dict):
        raise SlurmJobCgroupError("systemd slice receipt is invalid")
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    expected = {
        "systemd_slice": unit,
        "slice_identity_sha256": identity_sha256,
        "job_id": job_id,
        "job_start_time": job_start_time,
        "cluster": cluster,
        "node_list": node,
        "account": account,
        "env_id": env_id,
        "resource_generation": resource_generation,
        "runtime_id": runtime_id,
        "candidate_id": candidate_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "pids_max": str(pids_max),
    }
    if (
        set(receipt) != _SLICE_RECEIPT_FIELDS
        or raw != json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.slurm-systemd-slice-receipt"
        or any(receipt.get(key) != value for key, value in expected.items())
        or _DIGEST_RE.fullmatch(str(receipt.get("unit_sha256"))) is None
        or _DIGEST_RE.fullmatch(str(receipt.get("slice_identity_sha256"))) is None
        or not isinstance(receipt.get("cpu_max"), str)
        or not isinstance(receipt.get("memory_max"), str)
        or not isinstance(receipt.get("memory_swap_max_source"), str)
        or not isinstance(receipt.get("memory_swap_max_effective"), str)
        or not isinstance(receipt.get("pids_max"), str)
        or not isinstance(receipt.get("cpuset_cpus"), str)
        or not isinstance(receipt.get("cpuset_mems"), str)
        or not isinstance(receipt.get("gpu_tres"), str)
        or not isinstance(receipt.get("gpu_detail"), str)
        or receipt.get("payload_sha256")
        != hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
    ):
        raise SlurmJobCgroupError("systemd slice receipt binding is invalid")
    unit_bytes = _bound_file(
        unit_root / unit,
        mode=0o644,
        expected_uid=expected_authority_uid,
        expected_gid=expected_authority_gid,
    )
    if hashlib.sha256(unit_bytes).hexdigest() != receipt["unit_sha256"]:
        raise SlurmJobCgroupError("systemd slice unit digest drifted")
    host_job_path = cgroup_root / job_path.removeprefix("/")
    for field, filename in (
        ("cpu_max", "cpu.max"),
        ("memory_max", "memory.max"),
        ("memory_swap_max_source", "memory.swap.max"),
        ("pids_max", "pids.max"),
        ("cpuset_cpus", "cpuset.cpus.effective"),
        ("cpuset_mems", "cpuset.mems.effective"),
    ):
        try:
            observed = (host_job_path / filename).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise SlurmJobCgroupError("Slurm allocation limit readback is unavailable") from exc
        if receipt.get(field) != observed:
            raise SlurmJobCgroupError("systemd slice receipt limit binding drifted")
    source_swap = str(receipt.get("memory_swap_max_source"))
    expected_effective_swap = "0" if source_swap == "max" else source_swap
    if receipt.get("memory_swap_max_effective") != expected_effective_swap:
        raise SlurmJobCgroupError("systemd slice swap ceiling is not equal or stricter")
    return unit


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print the current delegated Slurm job cgroup or fail closed.",
    )
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--pids-max", required=True, type=int)
    parser.add_argument("--wait-seconds", type=float, default=0.0)
    parser.add_argument(
        "--docker-driver",
        choices=("cgroupfs", "systemd"),
        default="cgroupfs",
    )
    parser.add_argument("--cluster", default="")
    parser.add_argument("--node", default="")
    parser.add_argument("--job-start-time", default="")
    parser.add_argument("--account", default="")
    parser.add_argument("--env-id", default="")
    parser.add_argument("--resource-generation", type=int, default=0)
    parser.add_argument("--runtime-id", default="")
    parser.add_argument("--candidate-id", default="")
    parser.add_argument("--candidate-sha", default="")
    parser.add_argument("--candidate-tree", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        print(
            discover_docker_cgroup_parent(
                docker_driver=args.docker_driver,
                job_id=args.job_id,
                pids_max=args.pids_max,
                wait_seconds=args.wait_seconds,
                cluster=args.cluster,
                node=args.node,
                job_start_time=args.job_start_time,
                account=args.account,
                env_id=args.env_id,
                resource_generation=args.resource_generation,
                runtime_id=args.runtime_id,
                candidate_id=args.candidate_id,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
            ),
        )
    except SlurmJobCgroupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
