#!/usr/bin/env python3
"""Create a remote-worker capacity plan from worker_pool_inventory.sh output."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path

_KV_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)")


@dataclass(frozen=True)
class HostInventory:
    label: str
    fields: dict[str, str]

    @property
    def host(self) -> str:
        return self.fields.get("host") or self.label


@dataclass(frozen=True)
class HostPlan:
    host: str
    status: str
    cpus: int | None
    mem_total_mib: int | None
    docker_cpus: int | None
    recommended_concurrency: int
    reason: str


def _as_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def parse_inventory(text: str) -> list[HostInventory]:
    hosts: list[HostInventory] = []
    current_label: str | None = None
    current_fields: dict[str, str] = {}

    def flush() -> None:
        nonlocal current_label, current_fields
        if current_label is not None:
            hosts.append(HostInventory(label=current_label, fields=current_fields))
        current_label = None
        current_fields = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            flush()
            current_label = line[3:].strip()
            continue
        if current_label is None:
            continue
        for key, value in _KV_RE.findall(line):
            current_fields[key] = value
    flush()
    return hosts


def recommend_concurrency(
    host: HostInventory,
    *,
    cpu_per_trial: int,
    mem_mib_per_trial: int,
    max_per_host: int,
) -> HostPlan:
    fields = host.fields
    cpus = _as_int(fields.get("cpus"))
    mem_total_mib = _as_int(fields.get("mem_total_mib"))
    docker_cpus = _as_int(fields.get("docker_cpus"))

    if fields.get("ssh") == "failed":
        return HostPlan(host.host, "exclude", cpus, mem_total_mib, docker_cpus, 0, "ssh failed")
    failed_endpoint = next(
        (name for name in ("control_plane", "gateway", "minio") if fields.get(name) != "ok"),
        None,
    )
    if failed_endpoint is not None:
        return HostPlan(
            host.host,
            "exclude",
            cpus,
            mem_total_mib,
            docker_cpus,
            0,
            f"{failed_endpoint} reachability failed",
        )
    if fields.get("docker") == "missing" or fields.get("docker_info") == "failed":
        return HostPlan(host.host, "exclude", cpus, mem_total_mib, docker_cpus, 0, "docker unavailable")

    cpu_source = docker_cpus or cpus
    if cpu_source is None or mem_total_mib is None:
        return HostPlan(host.host, "exclude", cpus, mem_total_mib, docker_cpus, 0, "missing capacity")

    cpu_limit = max(1, cpu_source // cpu_per_trial)
    mem_limit = max(1, mem_total_mib // mem_mib_per_trial)
    recommended = max(1, min(cpu_limit, mem_limit, max_per_host))
    return HostPlan(host.host, "include", cpus, mem_total_mib, docker_cpus, recommended, "")


def render_csv(plans: list[HostPlan]) -> str:
    from io import StringIO

    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([
        "host",
        "status",
        "cpus",
        "mem_total_mib",
        "docker_cpus",
        "recommended_concurrency",
        "reason",
    ])
    for plan in plans:
        writer.writerow([
            plan.host,
            plan.status,
            "" if plan.cpus is None else plan.cpus,
            "" if plan.mem_total_mib is None else plan.mem_total_mib,
            "" if plan.docker_cpus is None else plan.docker_cpus,
            plan.recommended_concurrency,
            plan.reason,
        ])
    return buf.getvalue()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--cpu-per-trial", type=int, default=2)
    parser.add_argument("--mem-mib-per-trial", type=int, default=8192)
    parser.add_argument("--max-per-host", type=int, default=96)
    args = parser.parse_args(argv)
    if args.cpu_per_trial < 1:
        parser.error("--cpu-per-trial must be >= 1")
    if args.mem_mib_per_trial < 1:
        parser.error("--mem-mib-per-trial must be >= 1")
    if args.max_per_host < 1:
        parser.error("--max-per-host must be >= 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    text = args.inventory.read_text(encoding="utf-8")
    plans = [
        recommend_concurrency(
            host,
            cpu_per_trial=args.cpu_per_trial,
            mem_mib_per_trial=args.mem_mib_per_trial,
            max_per_host=args.max_per_host,
        )
        for host in parse_inventory(text)
    ]
    sys.stdout.write(render_csv(plans))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
