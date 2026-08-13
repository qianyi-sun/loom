"""Deterministic local Slurm command processes for executor backend tests."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_COMMANDS = ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")

_FAKE_PROCESS = r"""#!/usr/bin/python3
from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path

invoked_path = sys.argv[0]
if invoked_path.startswith("/proc/self/fd/"):
    invoked_path = os.readlink(invoked_path).removesuffix(" (deleted)")
root = Path(invoked_path).parent.parent
state_path = root / "state.json"
calls_path = root / "calls.jsonl"
command = Path(invoked_path).name


def secure_append(path: Path, payload: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError("fake evidence is not a regular single-link file")
        os.fchmod(fd, 0o600)
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)


def secure_write(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".new")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        os.write(fd, json.dumps(value, sort_keys=True).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)


state = json.loads(state_path.read_text(encoding="utf-8"))
secure_append(
    calls_path,
    json.dumps(
        {
            "executable": invoked_path,
            "argv": sys.argv[1:],
            "environment": dict(os.environ),
            "shell": False,
        },
        sort_keys=True,
    ).encode("utf-8") + b"\n",
)

fault = state.get("faults", {}).get(command)
if fault == "timeout":
    time.sleep(5)
if fault == "descendant_pipe":
    child = os.fork()
    if child == 0:
        time.sleep(5)
        os._exit(0)
    raise SystemExit(0)
if fault == "oversize":
    sys.stdout.write("x" * 1048576)
    raise SystemExit(0)
if fault == "failure":
    sys.stderr.write("controlled fake failure")
    raise SystemExit(2)
override = state.get("outputs", {}).get(command)
if override is not None:
    sys.stdout.write(override)
    raise SystemExit(0)

if command == "scontrol":
    sys.stdout.write(
        "Configuration data as of 2026-08-13T12:00:00\n"
        "AccountingStorageType = accounting_storage/slurmdbd\n"
        "AuthType = auth/munge\n"
        "ClusterName = " + state["cluster"] + "\n"
        "ControlMachine = (null)\n"
        "SlurmctldHost[0] = " + state["controller"] + "(192.0.2.10)\n"
        "SlurmctldHost[1] = ctl-backup.oldlab.internal(192.0.2.11)\n"
        "SlurmctldPort = 6817\n"
        "SlurmUser = slurm(64030)\n"
    )
elif command == "sacctmgr":
    sys.stdout.write("|".join((
        state["cluster"],
        state["account"],
        state["submitter"],
        state["partition"],
        state["qos"],
    )) + "\n")
elif command == "squeue":
    requested = None
    for argument in sys.argv[1:]:
        if argument.startswith("--jobs="):
            requested = argument.split("=", 1)[1]
    jobs = state["jobs"].values()
    if requested is not None:
        jobs = (job for job in jobs if job["job_id"] == requested)
    for job in jobs:
        reason_or_nodes = (
            job["pending_reason"] if job["state"] == "PENDING" else ",".join(job["nodes"])
        )
        sys.stdout.write("|".join((
            job["job_id"], job["state"], job["submitter"], job["account"],
            job["partition"], str(job["cpus"]), str(job["memory_bytes"] // 1048576) + "M",
            job.get("gres", "gpu:" + str(job["gpus"]) if job["gpus"] else "N/A"),
            ",".join(job["nodes"]), reason_or_nodes,
            job["ownership_token"],
        )) + "\n")
elif command == "sbatch":
    def option(prefix: str, default: str = "") -> str:
        return next((item.split("=", 1)[1] for item in sys.argv[1:] if item.startswith(prefix)), default)
    generic_tres = {}
    gres_records = []
    gpu_spec = option("--gpus=", "0")
    if gpu_spec.isdigit():
        gpus = int(gpu_spec)
        if gpus:
            gres_records.append("gpu:" + str(gpus))
    else:
        gpus = 0
        for record in gpu_spec.split(","):
            gpu_type, count_text = record.rsplit(":", 1)
            count = int(count_text)
            gpus += count
            generic_tres["gres/gpu:" + gpu_type] = count
            gres_records.append("gpu:" + gpu_type + ":" + str(count))
    gres_spec = option("--gres=")
    if gres_spec:
        for record in gres_spec.split(","):
            name, count_text = record.rsplit(":", 1)
            count = int(count_text)
            generic_tres["gres/" + name] = count
            gres_records.append(name + ":" + str(count))
    job_id = str(state["next_job_id"])
    state["next_job_id"] += 1
    state["jobs"][job_id] = {
        "job_id": job_id,
        "state": "PENDING",
        "submitter": state["submitter"],
        "account": option("--account="),
        "partition": option("--partition="),
        "cpus": int(option("--cpus-per-task=")),
        "memory_bytes": int(option("--mem=")[:-1]) * 1024 * 1024,
        "gpus": gpus,
        "generic_tres": generic_tres,
        "gres": ",".join(gres_records) if gres_records else "N/A",
        "nodes": option("--nodelist=").split(","),
        "pending_reason": "Resources",
        "ownership_token": option("--comment="),
    }
    secure_write(state_path, state)
    sys.stdout.write(job_id + ";" + state["cluster"] + "\n")
elif command == "scancel":
    job_id = sys.argv[-1]
    if job_id in state["jobs"]:
        state["jobs"][job_id]["state"] = "CANCELLED"
        state["jobs"][job_id]["pending_reason"] = ""
        secure_write(state_path, state)
elif command == "sacct":
    for job in state["terminal_jobs"]:
        allocated_tres = [
            "cpu=" + str(job["cpus"]),
            "mem=" + str(job["memory_bytes"] // 1048576) + "M",
            "node=" + str(len(job["nodes"])),
        ]
        if job["gpus"]:
            allocated_tres.append("gres/gpu=" + str(job["gpus"]))
        allocated_tres.extend(
            name + "=" + str(value)
            for name, value in sorted(job.get("generic_tres", {}).items())
        )
        sys.stdout.write("|".join((
            job["job_id"], job["state"], job["submitter"], job["account"],
            state["cluster"], job["submitted_at"], job["started_at"],
            job["ended_at"], str(job["elapsed_seconds"]), job["exit_code"],
            str(job["cpus"]), str(job["memory_bytes"] // 1048576) + "M",
            ",".join(allocated_tres),
            ",".join(job["nodes"]), job["ownership_token"],
        )) + "\n")
else:
    raise SystemExit(64)
"""


@dataclass(frozen=True, slots=True)
class FakeSlurmCall:
    executable: str
    argv: tuple[str, ...]
    environment: dict[str, str]
    shell: bool


class FakeSlurm:
    """Own local command files and mutable fake controller state."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir(parents=True)
        self._state_path = root / "state.json"
        self._calls_path = root / "calls.jsonl"
        self._state: dict[str, Any] = {
            "cluster": "oldlab",
            "controller": "ctl.oldlab.internal",
            "partition": "loom",
            "account": "loom-executor",
            "submitter": "loom-oldlab",
            "qos": "loom",
            "next_job_id": 101,
            "jobs": {},
            "terminal_jobs": [],
            "faults": {},
            "outputs": {},
        }
        self._write_state()
        for command in _COMMANDS:
            path = self.bin / command
            path.write_text(_FAKE_PROCESS, encoding="utf-8")
            path.chmod(0o700)
        self.mutable_launcher = self.bin / "trusted-launcher"
        self.mutable_launcher.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.mutable_launcher.chmod(0o700)
        self.launcher = Path("/usr/bin/true")

    def _write_state(self) -> None:
        descriptor = os.open(
            self._state_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
            os.write(descriptor, json.dumps(self._state, sort_keys=True).encode("utf-8"))
        finally:
            os.close(descriptor)

    def _load_state(self) -> None:
        self._state = json.loads(self._state_path.read_text(encoding="utf-8"))

    @staticmethod
    def _digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def backend(self, **authority_changes: object):  # type: ignore[no-untyped-def]
        from loom_capacity_executor.slurm_backend import AsyncSlurmBackend
        from loom_capacity_executor.slurm_contracts import (
            SlurmAuthorityV2,
            SlurmExecutableIdentityV2,
            SlurmExecutablesV2,
            SlurmResourceV2,
        )

        def identity(command: str) -> SlurmExecutableIdentityV2:
            return SlurmExecutableIdentityV2(
                path=str(self.bin / command),
                sha256=self._digest(self.bin / command),
                owner_uid=os.geteuid(),
            )

        authority = SlurmAuthorityV2(
            cluster="oldlab",
            controller_host="ctl.oldlab.internal",
            partition="loom",
            account="loom-executor",
            submitter="loom-oldlab",
            qos="loom",
            local_uid=os.geteuid(),
            executables=SlurmExecutablesV2(
                scontrol=identity("scontrol"),
                sacctmgr=identity("sacctmgr"),
                squeue=identity("squeue"),
                sbatch=identity("sbatch"),
                scancel=identity("scancel"),
                sacct=identity("sacct"),
            ),
            resource_ceiling=SlurmResourceV2(
                cpus=64,
                memory_bytes=512 * 1024 * 1024 * 1024,
                gpus=8,
            ),
            command_timeout_seconds=0.5,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ).model_copy(update=authority_changes)
        return AsyncSlurmBackend(authority)

    @property
    def launcher_sha256(self) -> str:
        return self._digest(self.launcher)

    @property
    def calls(self) -> tuple[FakeSlurmCall, ...]:
        if not self._calls_path.exists():
            return ()
        return tuple(
            FakeSlurmCall(
                executable=value["executable"],
                argv=tuple(value["argv"]),
                environment=value["environment"],
                shell=value["shell"],
            )
            for line in self._calls_path.read_text(encoding="utf-8").splitlines()
            if (value := json.loads(line))
        )

    @property
    def sbatch_calls(self) -> tuple[FakeSlurmCall, ...]:
        return tuple(call for call in self.calls if Path(call.executable).name == "sbatch")

    @property
    def scancel_calls(self) -> tuple[FakeSlurmCall, ...]:
        return tuple(call for call in self.calls if Path(call.executable).name == "scancel")

    def add_job(
        self,
        job_id: str = "101",
        *,
        state: str = "PENDING",
        submitter: str | None = None,
        account: str | None = None,
    ) -> None:
        self._state["jobs"][job_id] = {
            "job_id": job_id,
            "state": state,
            "submitter": submitter or self._state["submitter"],
            "account": account or self._state["account"],
            "partition": self._state["partition"],
            "cpus": 16,
            "memory_bytes": 64 * 1024 * 1024 * 1024,
            "gpus": 2,
            "generic_tres": {},
            "gres": "gpu:2",
            "nodes": ["oldlab-5"],
            "pending_reason": "Resources" if state == "PENDING" else "",
            "ownership_token": "A" * 43,
        }
        self._write_state()

    def set_job_state(self, job_id: str, state: str) -> None:
        self._state["jobs"][job_id]["state"] = state
        self._state["jobs"][job_id]["pending_reason"] = "Resources" if state == "PENDING" else ""
        self._write_state()

    def set_controller(self, *, cluster: str | None = None, host: str | None = None) -> None:
        if cluster is not None:
            self._state["cluster"] = cluster
        if host is not None:
            self._state["controller"] = host
        self._write_state()

    def set_output(self, command: str, output: str) -> None:
        self._state["outputs"][command] = output
        self._write_state()

    def set_fault(self, command: str, fault: str) -> None:
        self._state["faults"][command] = fault
        self._write_state()

    def add_terminal_job(self, *, job_id: str = "99", state: str = "COMPLETED") -> None:
        self._state["terminal_jobs"].append(
            {
                "job_id": job_id,
                "state": state,
                "submitter": self._state["submitter"],
                "account": self._state["account"],
                "submitted_at": "2026-08-13T12:00:00Z",
                "started_at": "2026-08-13T12:01:00Z",
                "ended_at": "2026-08-13T12:03:00Z",
                "elapsed_seconds": 120,
                "exit_code": "0:0",
                "cpus": 16,
                "memory_bytes": 64 * 1024 * 1024 * 1024,
                "gpus": 2,
                "generic_tres": {},
                "nodes": ["oldlab-5"],
                "ownership_token": "A" * 43,
            }
        )
        self._write_state()

    def terminalize_job(self, job_id: str) -> None:
        self._load_state()
        job = dict(self._state["jobs"][job_id])
        job.update(
            {
                "state": "COMPLETED",
                "submitted_at": "2026-08-13T12:00:00Z",
                "started_at": "2026-08-13T12:01:00Z",
                "ended_at": "2026-08-13T12:03:00Z",
                "elapsed_seconds": 120,
                "exit_code": "0:0",
            }
        )
        self._state["terminal_jobs"].append(job)
        self._write_state()

    def evidence_paths(self) -> tuple[Path, ...]:
        return tuple(path for path in (self._state_path, self._calls_path) if path.exists())


def assert_secure_evidence(path: Path) -> None:
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode)
    assert info.st_nlink == 1
    assert stat.S_IMODE(info.st_mode) == 0o600
