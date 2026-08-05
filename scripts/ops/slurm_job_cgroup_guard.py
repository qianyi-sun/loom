#!/usr/bin/env python3
"""Administrator-owned root guard for non-exclusive Loom Slurm workers (#896).

Slurm 23.11 runs the administrator Prolog before it creates the contained job
cgroup, so a Prolog cannot safely delegate controllers or size a systemd slice
for the eventual allocation.  This root service instead polls Slurm for jobs on
the local node that opt in through the closed comment
``loom-cgroup-v1:pids=<N>`` and, for each one:

* delegates ``cpu memory pids`` into the ``job_<id>`` cgroup and writes the
  reviewed ``pids.max`` so the batch wrapper's readback converges; and
* registers ``loom-job-<id>.slice`` capped to the allocation
  (``AllowedCPUs`` = the job cpuset, ``TasksMax`` = the aggregate PID ceiling,
  ``MemoryMax`` = the allocated memory) so Docker's systemd cgroup driver has a
  valid, allocation-bound ``.slice`` parent.

Only ``root`` can register a system slice, which is what lets the unprivileged
worker trust a slice it did not create.  The guard only ever touches cgroups
that carry the reviewed comment and slices named ``loom-job-<digits>.slice``;
it tears a slice down once its job leaves the queue.  It never edits
``slurm.conf``, ``cgroup.conf``, or any co-tenant cgroup.
"""

from __future__ import annotations

import argparse
import re
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_JOB_ID_RE = re.compile(r"^[1-9][0-9]*$")
_COMMENT_RE = re.compile(r"^loom-cgroup-v1:pids=([1-9][0-9]{0,8})$")
_SLICE_RE = re.compile(r"^loom-job-([1-9][0-9]*)\.slice$")
_ALLOC_MEM_RE = re.compile(r"(?:^|,)mem=([0-9]+)([KMGT]?)(?:,|$)")
_REQUIRED_CONTROLLERS = ("cpu", "memory", "pids")
_MAX_WALK_DIRECTORIES = 200_000
_MEM_UNIT_BYTES = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


@dataclass(frozen=True)
class GuardConfig:
    node: str
    cgroup_root: Path
    poll_interval_seconds: float
    command_timeout_seconds: float
    squeue_path: str
    scontrol_path: str
    systemctl_path: str


@dataclass(frozen=True)
class JobIntent:
    job_id: str
    pids_max: int
    cpu_max_percent: int
    memory_max_bytes: int


class GuardError(RuntimeError):
    """A recoverable per-job failure; logged and retried on the next pass."""


def _log(message: str) -> None:
    print(f"loom-slurm-job-cgroup-guard: {message}", file=sys.stderr, flush=True)


def _run(config: GuardConfig, args: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            timeout=config.command_timeout_seconds,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardError(f"command failed: {args[0]}: {exc}") from exc
    return completed.stdout


def _parse_alloc_memory_bytes(alloc_tres: str) -> int:
    match = _ALLOC_MEM_RE.search(alloc_tres.replace(" ", ""))
    if match is None:
        return 0
    return int(match.group(1)) * _MEM_UNIT_BYTES[match.group(2)]


def _scontrol_fields(config: GuardConfig, job_id: str) -> dict[str, str]:
    raw = _run(config, (config.scontrol_path, "show", "job", job_id, "--oneliner"))
    fields: dict[str, str] = {}
    for token in raw.split():
        key, separator, value = token.partition("=")
        if separator:
            fields[key] = value
    return fields


def discover_job_intents(config: GuardConfig) -> dict[str, JobIntent]:
    """Return reviewed job intents currently running on this node."""

    raw = _run(
        config,
        (
            config.squeue_path,
            "--noheader",
            "--states=RUNNING",
            f"--nodelist={config.node}",
            "--format=%i|%k",
        ),
    )
    intents: dict[str, JobIntent] = {}
    for line in raw.splitlines():
        job_id, separator, comment = line.strip().partition("|")
        if not separator or _JOB_ID_RE.fullmatch(job_id) is None:
            continue
        comment_match = _COMMENT_RE.fullmatch(comment.strip())
        if comment_match is None:
            continue
        try:
            fields = _scontrol_fields(config, job_id)
        except GuardError as exc:
            _log(f"job {job_id}: {exc}")
            continue
        memory_bytes = _parse_alloc_memory_bytes(fields.get("AllocTRES", ""))
        if memory_bytes <= 0:
            _log(f"job {job_id}: no allocated memory in AllocTRES; skipping")
            continue
        intents[job_id] = JobIntent(
            job_id=job_id,
            pids_max=int(comment_match.group(1)),
            cpu_max_percent=0,
            memory_max_bytes=memory_bytes,
        )
    return intents


def find_job_cgroup(cgroup_root: Path, job_id: str) -> Path | None:
    """Return the ``job_<id>`` cgroup directory below a slurmstepd scope."""

    target = f"job_{job_id}"
    walked = 0
    stack = [cgroup_root]
    while stack:
        current = stack.pop()
        try:
            children = list(current.iterdir())
        except OSError:
            continue
        for child in children:
            if not child.is_dir() or child.is_symlink():
                continue
            walked += 1
            if walked > _MAX_WALK_DIRECTORIES:
                return None
            if child.name == target and "slurmstepd.scope" in str(current):
                return child
            if (
                child.name.endswith(".scope")
                or child.name.endswith(".slice")
                or child.name.startswith("job_")
            ):
                stack.append(child)
            elif child.name in {"system.slice"}:
                stack.append(child)
    return None


def _write(path: Path, value: str) -> None:
    try:
        path.write_text(value, encoding="utf-8")
    except OSError as exc:
        raise GuardError(f"cannot write {path}: {exc}") from exc


def delegate_pids(job_cgroup: Path, pids_max: int) -> None:
    """Delegate the required controllers into the job cgroup and cap pids."""

    parent_subtree = job_cgroup.parent / "cgroup.subtree_control"
    try:
        parent_controls = parent_subtree.read_text(encoding="utf-8").split()
    except OSError as exc:
        raise GuardError(f"cannot read {parent_subtree}: {exc}") from exc
    if "pids" not in parent_controls:
        _write(parent_subtree, "+pids")

    subtree = job_cgroup / "cgroup.subtree_control"
    try:
        controls = subtree.read_text(encoding="utf-8").split()
    except OSError as exc:
        raise GuardError(f"cannot read {subtree}: {exc}") from exc
    missing = [f"+{name}" for name in _REQUIRED_CONTROLLERS if name not in controls]
    if missing:
        _write(subtree, " ".join(missing))
    _write(job_cgroup / "pids.max", str(pids_max))


def _read_scalar(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise GuardError(f"cannot read {path}: {exc}") from exc


def ensure_slice(config: GuardConfig, intent: JobIntent, job_cgroup: Path) -> None:
    """Register ``loom-job-<id>.slice`` capped to the allocation."""

    cpuset = _read_scalar(job_cgroup / "cpuset.cpus.effective")
    if not cpuset:
        raise GuardError(f"job {intent.job_id}: empty cpuset; refusing to size the slice")
    unit = f"loom-job-{intent.job_id}.slice"
    _run(
        config,
        (
            config.systemctl_path,
            "set-property",
            "--runtime",
            unit,
            f"AllowedCPUs={cpuset}",
            f"TasksMax={intent.pids_max}",
            f"MemoryMax={intent.memory_max_bytes}",
            "MemorySwapMax=0",
        ),
    )
    _run(config, (config.systemctl_path, "start", unit))


def active_loom_slices(config: GuardConfig) -> dict[str, str]:
    """Return active ``loom-job-<id>.slice`` units keyed by job id."""

    raw = _run(
        config,
        (
            config.systemctl_path,
            "list-units",
            "--type=slice",
            "--all",
            "--no-legend",
            "--plain",
            "loom-job-*.slice",
        ),
    )
    units: dict[str, str] = {}
    for line in raw.splitlines():
        unit = line.split()[0] if line.split() else ""
        match = _SLICE_RE.fullmatch(unit)
        if match is not None:
            units[match.group(1)] = unit
    return units


def teardown_stale_slices(config: GuardConfig, active_job_ids: set[str]) -> None:
    for job_id, unit in active_loom_slices(config).items():
        if job_id in active_job_ids:
            continue
        try:
            _run(config, (config.systemctl_path, "stop", unit))
            _log(f"job {job_id}: tore down stale slice {unit}")
        except GuardError as exc:
            _log(f"job {job_id}: could not stop {unit}: {exc}")


def run_once(config: GuardConfig) -> int:
    """Reconcile every opted-in job on the node once; return the count applied."""

    intents = discover_job_intents(config)
    applied = 0
    for job_id, intent in intents.items():
        job_cgroup = find_job_cgroup(config.cgroup_root, job_id)
        if job_cgroup is None:
            _log(f"job {job_id}: cgroup not present yet")
            continue
        try:
            delegate_pids(job_cgroup, intent.pids_max)
            ensure_slice(config, intent, job_cgroup)
            applied += 1
        except GuardError as exc:
            _log(f"job {job_id}: {exc}")
    teardown_stale_slices(config, set(intents))
    return applied


def _build_config(args: argparse.Namespace) -> GuardConfig:
    def _resolve(name: str, fallback: str) -> str:
        return getattr(args, name) or shutil.which(fallback) or fallback

    return GuardConfig(
        node=args.node,
        cgroup_root=Path(args.cgroup_root),
        poll_interval_seconds=args.poll_interval_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
        squeue_path=_resolve("squeue_path", "squeue"),
        scontrol_path=_resolve("scontrol_path", "scontrol"),
        systemctl_path=_resolve("systemctl_path", "systemctl"),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True, help="exact Slurm NodeName for this host")
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup")
    parser.add_argument("--poll-interval-seconds", type=float, default=2.0)
    parser.add_argument("--command-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--squeue-path", default="")
    parser.add_argument("--scontrol-path", default="")
    parser.add_argument("--systemctl-path", default="")
    parser.add_argument("--once", action="store_true", help="reconcile a single pass then exit")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = _build_config(args)
    if args.once:
        run_once(config)
        return 0

    stopping = {"value": False}

    def _stop(_signum: int, _frame: object) -> None:
        stopping["value"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    _log(f"started for node {config.node}")
    while not stopping["value"]:
        try:
            run_once(config)
        except Exception as exc:
            _log(f"pass failed: {exc}")
        time.sleep(config.poll_interval_seconds)
    _log("stopping")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
