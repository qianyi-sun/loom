"""Discover the delegated Slurm job cgroup used as Docker's cgroup parent.

Non-exclusive workers are safe only when every host-daemon container is nested
below the allocation cgroup.  This module deliberately supports only unified
cgroup v2 and fails closed when the current process is not in an identifiable,
delegated Slurm job subtree.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

_JOB_ID_RE = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
_REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_MAX_WAIT_SECONDS = 60.0
_POLL_INTERVAL_SECONDS = 0.1


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print the current delegated Slurm job cgroup or fail closed.",
    )
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--pids-max", required=True, type=int)
    parser.add_argument("--wait-seconds", type=float, default=0.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        print(
            discover_slurm_job_cgroup(
                job_id=args.job_id,
                pids_max=args.pids_max,
                wait_seconds=args.wait_seconds,
            ),
        )
    except SlurmJobCgroupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
