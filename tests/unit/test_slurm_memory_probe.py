"""Candidate-bound memory observation, without contacting a real Slurm fleet."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_control_plane import elastic_slurm_worker_controller as controller
from loom_control_plane import slurm_memory_probe as probe
from tests.unit.test_elastic_slurm_worker_controller import _config


@pytest.mark.parametrize("allocated", [32768, 110000])
async def test_gb10_memory_probe_failure_never_admits_or_probes_exhausted_node(
    monkeypatch: pytest.MonkeyPatch, allocated: int
) -> None:
    probes: list[str] = []

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        if args[0] == "scontrol":
            return controller._CommandResult(
                json.dumps(
                    {
                        "errors": [],
                        "warnings": [],
                        "nodes": [{"name": "trt-gb10-4", "state": ["MIXED"]}],
                    }
                ),
                "",
            )
        return controller._CommandResult(
            f"trt-gb10-4 {allocated}\n"
            if "-O" in args
            else "trt-gb10-4|mixed|20|110000|100000|1.0|8/12/0/20\n",
            "",
        )

    async def fail_probe(self, node):  # type: ignore[no-untyped-def]
        probes.append(node)
        raise RuntimeError("probe unavailable")

    monkeypatch.setattr(controller, "_run_command", run)
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", fail_probe
    )
    config = _config(
        pool_name="gb10",
        partition="loom-staging",
        allowed_nodes=("trt-gb10-4",),
        candidate_sha="a" * 40,
        resource_aware=True,
        probe_mem_available=True,
        reserved_cpus=1,
        reserved_memory_mib=0,
        memory_mib_per_slot=11500,
    )
    resources = (
        await controller.SubprocessSlurmCommandRunner()
        .bind_config(config)
        .query_node_resources(("trt-gb10-4",))
    )
    plan = controller.compute_node_capacity_plan(
        config, node="trt-gb10-4", resource=resources["trt-gb10-4"], active_nodes=set()
    )
    assert plan.safe_slots == 0
    assert probes == ([] if allocated == 110000 else ["trt-gb10-4"])


async def test_memory_probe_rejects_unbound_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return "111219\n"

    monkeypatch.setattr(probe, "_run_probe", run)
    runner = controller.SubprocessSlurmCommandRunner().bind_config(
        _config(
            partition="loom-staging",
            candidate_sha="a" * 40,
            slurm_cluster_name="trt-gb10",
            allowed_nodes=("trt-gb10-4",),
            resource_aware=True,
        )
    )
    with pytest.raises(RuntimeError):
        await runner._query_node_available_memory("trt-gb10-4")


def _output(nonce: str = "nonce", available: int = 113888512) -> str:
    return f"v1|{nonce}|trt-gb10-4|loom-staging|trt-gb10|loom-staging|loom-staging|{os.geteuid()}|123|{'a' * 40}|128000000|{available}|trt-gb10-4\n"


def _parse(output: str, *, started_at: float | None = None) -> probe.MemoryObservation:
    return probe._parse_observation(
        output,
        node="trt-gb10-4",
        partition="loom-staging",
        cluster_name="trt-gb10",
        account="loom-staging",
        qos="loom-staging",
        candidate_sha="a" * 40,
        nonce="nonce",
        uid=os.geteuid(),
        started_at=time.monotonic() if started_at is None else started_at,
    )


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "v0"),
        (1, "old-nonce"),
        (2, "foreign-node"),
        (3, "foreign-partition"),
        (4, "foreign-cluster"),
        (4, ""),
        (5, "foreign-account"),
        (5, ""),
        (6, "boost"),
        (6, ""),
        (7, "999999"),
        (8, "0"),
        (8, "nan"),
        (9, "b" * 40),
        (10, "0"),
        (10, "-1"),
        (11, "-1"),
        (11, "nan"),
        (11, "999999999"),
        (12, "trt-gb10-[4-5]"),
    ],
)
def test_probe_rejects_wrong_identity_or_memory(index: int, value: str) -> None:
    fields = _output().strip().split("|")
    fields[index] = value
    with pytest.raises(RuntimeError):
        _parse("|".join(fields) + "\n")


@pytest.mark.parametrize(
    "output", ["", "111219\n", _output() + _output(), _output() + "extra", "x" * 4097]
)
def test_probe_rejects_missing_extra_or_oversized_output(output: str) -> None:
    with pytest.raises(RuntimeError):
        _parse(output)


@pytest.mark.parametrize("age", [-1.0, 16.0])
def test_probe_rejects_future_or_stale_local_observation(age: float) -> None:
    with pytest.raises(RuntimeError, match="expired"):
        _parse(_output(), started_at=time.monotonic() - age)


@pytest.mark.parametrize("available,slots", [(113888512, 5), (1000000, 0), (0, 0)])
async def test_gb10_cache_heavy_and_real_pressure_primary_path(
    monkeypatch: pytest.MonkeyPatch,
    available: int,
    slots: int,
) -> None:
    commands: list[tuple[str, ...]] = []

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        if args[0] == "scontrol":
            return controller._CommandResult(
                json.dumps(
                    {
                        "errors": [],
                        "warnings": [],
                        "nodes": [{"name": "trt-gb10-4", "state": ["MIXED"]}],
                    }
                ),
                "",
            )
        return controller._CommandResult(
            "trt-gb10-4 32768\n"
            if "-O" in args
            else "trt-gb10-4|mixed|20|110000|1800|1.0|8/12/0/20\n",
            "",
        )

    async def memory(args, *, timeout):  # type: ignore[no-untyped-def]
        commands.append(args)
        assert timeout <= 10
        return _output(args[-2], available)

    monkeypatch.setattr(controller, "_run_command", run)
    monkeypatch.setattr(probe, "_run_probe", memory)
    config = _config(
        pool_name="gb10",
        environment="staging",
        allowed_nodes=("trt-gb10-4",),
        partition="loom-staging",
        slurm_cluster_name="trt-gb10",
        candidate_sha="a" * 40,
        slurm_account="loom-staging",
        slurm_qos="loom-staging",
        resource_aware=True,
        probe_mem_available=True,
        reserved_cpus=1,
        reserved_memory_mib=0,
        memory_mib_per_slot=11500,
    )
    resources = (
        await controller.SubprocessSlurmCommandRunner()
        .bind_config(config)
        .query_node_resources(config.allowed_nodes)
    )
    assert (
        controller.compute_node_capacity_plan(
            config, node="trt-gb10-4", resource=resources["trt-gb10-4"], active_nodes=set()
        ).safe_slots
        == slots
    )
    assert len(commands) == 1
    args = commands[0]
    for required in (
        "--immediate=3",
        "--nodes=1",
        "--ntasks=1",
        "--nodelist=trt-gb10-4",
        "--cpus-per-task=1",
        "--mem=16M",
        "--time=00:01:00",
        "--partition=loom-staging",
        "--account=loom-staging",
        "--qos=loom-staging",
        "--export=NONE",
        "/usr/bin/timeout",
        "5s",
        "--kill-after=1s",
    ):
        assert required in args
    assert not any(
        arg.startswith(
            (
                "--exclusive",
                "--reservation",
                "--oversubscribe",
                "--overlap",
                "--jobid",
                "--gres",
                "--gpus",
            )
        )
        for arg in args
    )
    assert f"--comment=loom-memory-probe:{'a' * 40}" in args
    assert any(arg.startswith("--job-name=loom-mem-staging-gb10-") for arg in args)


@pytest.mark.parametrize(
    "change", ["reserved", "cpu", "load", "allocmem", "stale", "postread-error"]
)
async def test_memory_uplift_cannot_override_post_probe_drift(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    resource = controller.SlurmNodeResource(
        "trt-gb10-4",
        "mixed",
        20,
        1800,
        1.0,
        12,
        total_memory_mib=110000,
        schedulable_memory_mib=77232,
    )
    reads = 0

    async def snapshot(self, nodes):  # type: ignore[no-untyped-def]
        nonlocal reads
        reads += 1
        current = resource
        if reads == 3:
            if change == "postread-error":
                raise RuntimeError("unavailable")
            current = replace(
                resource,
                **{
                    "reserved": {"state": "MIXED+RESERVED"},
                    "cpu": {"idle_cpus": 1},
                    "load": {"cpu_load": 25.0},
                    "allocmem": {"schedulable_memory_mib": 0},
                    "stale": {},
                }[change],
            )
        return {"trt-gb10-4": current}

    async def memory(self, node):  # type: ignore[no-untyped-def]
        return probe.MemoryObservation(111219, time.monotonic() - (16 if change == "stale" else 0))

    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_resource_snapshot", snapshot
    )
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", memory
    )
    config = _config(allowed_nodes=("trt-gb10-4",), resource_aware=True, probe_mem_available=True)
    resources = (
        await controller.SubprocessSlurmCommandRunner()
        .bind_config(config)
        .query_node_resources(config.allowed_nodes)
    )
    assert (
        controller.compute_node_capacity_plan(
            config, node="trt-gb10-4", resource=resources["trt-gb10-4"], active_nodes=set()
        ).safe_slots
        == 0
    )


async def test_probe_sweep_stops_at_bounded_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [0.0]
    calls: list[str] = []

    async def snapshot(self, nodes):  # type: ignore[no-untyped-def]
        return {
            node: controller.SlurmNodeResource(
                node, "mixed", 24, 100000, 1.0, 24, schedulable_memory_mib=100000
            )
            for node in nodes
        }

    async def memory(self, node):  # type: ignore[no-untyped-def]
        calls.append(node)
        clock[0] += 12
        return probe.MemoryObservation(100000, time.monotonic())

    monkeypatch.setattr(controller, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_resource_snapshot", snapshot
    )
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", memory
    )
    config = _config(resource_aware=True, probe_mem_available=True)
    resources = (
        await controller.SubprocessSlurmCommandRunner()
        .bind_config(config)
        .query_node_resources(config.allowed_nodes)
    )
    assert calls == ["oldlab-1", "oldlab-2"]
    assert resources["oldlab-3"].available_memory_mib is None


async def test_probe_subprocess_strips_foreign_allocation_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLURM_JOB_ID", "foreign-job")
    monkeypatch.setenv("SRUN_EXCLUSIVE", "1")
    monkeypatch.setenv("SBATCH_RESERVATION", "foreign-reservation")
    monkeypatch.setenv("MODEL_PROVIDER_API_KEY", "must-not-export")
    output = await probe._run_probe(
        (sys.executable, "-c", "import os,json; print(json.dumps(dict(os.environ)))"), timeout=2
    )
    env = json.loads(output)
    for key in ("SLURM_JOB_ID", "SRUN_EXCLUSIVE", "SBATCH_RESERVATION", "MODEL_PROVIDER_API_KEY"):
        assert key not in env


@pytest.mark.parametrize(
    "cancel,ignore_term", [(False, False), (False, True), (True, False), (True, True)]
)
async def test_probe_timeout_and_cancellation_reap_child(
    monkeypatch: pytest.MonkeyPatch, cancel: bool, ignore_term: bool
) -> None:
    processes: list[asyncio.subprocess.Process] = []
    create = asyncio.create_subprocess_exec

    async def capture(*args, **kwargs):  # type: ignore[no-untyped-def]
        process = await create(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(probe.asyncio, "create_subprocess_exec", capture)
    monkeypatch.setattr(probe, "_CLEANUP_SECONDS", 0.02)
    code = (
        "import signal,time; "
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
        + "time.sleep(60)"
    )
    task = asyncio.create_task(probe._run_probe((sys.executable, "-c", code), timeout=0.1))
    if cancel:
        await asyncio.sleep(0.06)
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await task
    assert len(processes) == 1 and processes[0].returncode is not None
    if ignore_term:
        assert processes[0].returncode == -9


@pytest.mark.parametrize(
    "code",
    ["print('x' * 5000)", "import sys; sys.stderr.write('x' * 5000)", "import sys; sys.exit(2)"],
)
async def test_probe_subprocess_rejects_error_and_bounded_output(code: str) -> None:
    with pytest.raises(RuntimeError):
        await probe._run_probe((sys.executable, "-c", code), timeout=2)


@pytest.mark.parametrize(
    "meminfo",
    [
        "MemTotal: 128000000 kB\nMemAvailable: 113888512 kB\n",
        "MemTotal: 128000000 kB\n",
        "MemTotal: 128000000 kB\nMemAvailable: 113888512 MB\n",
        "MemTotal: 128000000 kB\nMemAvailable: 100 kB\nMemAvailable: 200 kB\n",
    ],
)
def test_remote_reader_script_real_process(tmp_path: Path, meminfo: str) -> None:
    source = tmp_path / "meminfo"
    source.write_text(meminfo)
    script = probe._PROBE_SCRIPT.replace("/proc/meminfo", shlex.quote(str(source)))
    result = subprocess.run(
        ("/bin/sh", "-c", script, "loom-memory-probe", "nonce", "a" * 40),
        env={
            "SLURMD_NODENAME": "trt-gb10-4",
            "SLURM_JOB_NODELIST": "trt-gb10-4",
            "SLURM_JOB_PARTITION": "loom-staging",
            "SLURM_CLUSTER_NAME": "trt-gb10",
            "SLURM_JOB_ACCOUNT": "loom-staging",
            "SLURM_JOB_QOS": "loom-staging",
            "SLURM_JOB_ID": "123",
        },
        capture_output=True,
        text=True,
        timeout=2,
        check=False,
    )
    if meminfo.count("MemAvailable:") == 1 and "MB" not in meminfo:
        assert result.returncode == 0
        assert _parse(result.stdout).available_memory_mib == 111219
    else:
        assert result.returncode != 0


@pytest.mark.parametrize(
    "state,load,idle,allocated",
    [
        ("MIXED+RESERVED", 1.0, 12, 0),
        ("DOWN", 1.0, 12, 0),
        ("mixed", float("nan"), 12, 0),
        ("mixed", float("-inf"), 12, 0),
        ("mixed", -1.0, 12, 0),
        ("mixed", 25.0, 12, 0),
        ("mixed", None, 12, 0),
        ("mixed", 1.0, 1, 0),
        ("mixed", 1.0, 12, 110000),
    ],
)
async def test_ineligible_nodes_never_allocate_probe(
    monkeypatch: pytest.MonkeyPatch, state: str, load: float | None, idle: int, allocated: int
) -> None:
    async def snapshot(self, nodes):  # type: ignore[no-untyped-def]
        return {
            node: controller.SlurmNodeResource(
                node, state, 20, 100000, load, idle, schedulable_memory_mib=110000 - allocated
            )
            for node in nodes
        }

    async def forbidden(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        pytest.fail("ineligible node must never allocate probe")

    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_resource_snapshot", snapshot
    )
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", forbidden
    )
    config = _config(resource_aware=True, probe_mem_available=True)
    await (
        controller.SubprocessSlurmCommandRunner()
        .bind_config(config)
        .query_node_resources(config.allowed_nodes)
    )


def test_legacy_unconfigured_account_and_qos_accept_slurm_defaults() -> None:
    observation = probe._parse_observation(
        _output(),
        node="trt-gb10-4",
        partition="loom-staging",
        cluster_name="trt-gb10",
        account="",
        qos="",
        candidate_sha="a" * 40,
        nonce="nonce",
        uid=os.geteuid(),
        started_at=time.monotonic(),
    )
    assert observation.available_memory_mib == 111219


@pytest.mark.parametrize("field", ["partition", "slurm_cluster_name"])
@pytest.mark.parametrize("value", ["", "   "])
def test_memory_probe_requires_configured_identities(field: str, value: str) -> None:
    fields = asdict(
        _config(probe_mem_available=True, partition="loom-staging", slurm_cluster_name="trt-gb10")
    )
    fields["allowed_nodes_csv"] = ",".join(fields.pop("allowed_nodes"))
    fields.pop("requested_gpus")
    fields[field] = value
    with pytest.raises(ValueError, match=field):
        controller.build_controller_config(enabled=True, **fields)


@pytest.mark.parametrize(
    "change",
    ["none", "stale", "candidate", "qos", "headroom", "requested-memory", "missing", "replay"],
)
async def test_submission_requires_unconsumed_fresh_policy_bound_probe(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    async def snapshot(self, nodes):  # type: ignore[no-untyped-def]
        return {
            node: controller.SlurmNodeResource(
                node, "mixed", 24, 100000, 1.0, 24, schedulable_memory_mib=100000
            )
            for node in nodes
        }

    async def memory(self, node):  # type: ignore[no-untyped-def]
        return probe.MemoryObservation(100000, time.monotonic() - (16 if change == "stale" else 0))

    submitted: list[object] = []

    async def run(args, **_kwargs):  # type: ignore[no-untyped-def]
        submitted.append(args)
        return controller._CommandResult("123\n", "")

    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_resource_snapshot", snapshot
    )
    monkeypatch.setattr(
        controller.SubprocessSlurmCommandRunner, "_query_node_available_memory", memory
    )
    monkeypatch.setattr(controller, "_run_command", run)
    config = _config(
        allowed_nodes=("oldlab-1",),
        resource_aware=True,
        probe_mem_available=True,
        requested_cpus=2,
        requested_memory_mib=8192,
        requested_concurrency=1,
        job_pids_max=4096,
        candidate_sha="a" * 40,
        container_cpus=2,
        container_memory_mib=8192,
        container_pids=512,
    )
    runner = controller.SubprocessSlurmCommandRunner().bind_config(config)
    if change != "missing":
        resources = await runner.query_node_resources(config.allowed_nodes)
        # Returned snapshots must not let callers mutate the dispatch evidence.
        resources["oldlab-1"] = replace(resources["oldlab-1"], state="DOWN")
    submitted_config = replace(
        config,
        **{
            "candidate": {"candidate_sha": "b" * 40},
            "qos": {"slurm_qos": "boost"},
            "headroom": {"reserved_memory_mib": 0},
            "requested-memory": {"requested_memory_mib": 1000000},
        }.get(change, {}),
    )
    if change in {"none", "replay"}:
        assert await runner.submit_worker(node="oldlab-1", config=submitted_config) == "123"
        assert len(submitted) == 1
    else:
        with pytest.raises(RuntimeError, match="fresh candidate-bound"):
            await runner.submit_worker(node="oldlab-1", config=submitted_config)
        assert submitted == []
    if change == "replay":
        with pytest.raises(RuntimeError, match="fresh candidate-bound"):
            await runner.submit_worker(node="oldlab-1", config=submitted_config)
        assert len(submitted) == 1


@pytest.mark.parametrize("timeout", [float("nan"), float("inf")])
async def test_nonfinite_command_timeout_rejected_before_probe(
    monkeypatch: pytest.MonkeyPatch, timeout: float
) -> None:
    fields = asdict(
        _config(
            container_cpus=2,
            container_memory_mib=8192,
            container_pids=512,
            candidate_sha="a" * 40,
            job_output_dir="/tmp/loom",
            max_concurrency_per_node=6,
            command_timeout_seconds=timeout,
        )
    )
    fields["allowed_nodes_csv"] = ",".join(fields.pop("allowed_nodes"))
    fields.pop("requested_gpus")
    with pytest.raises(ValueError, match="command_timeout_seconds"):
        controller.build_controller_config(enabled=True, **fields)


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 0.0, -1.0])
async def test_direct_probe_rejects_unbounded_timeout(
    monkeypatch: pytest.MonkeyPatch, timeout: float
) -> None:
    async def fake_run(args, **_kwargs):  # type: ignore[no-untyped-def]
        return _output(args[-2])

    monkeypatch.setattr(probe, "_run_probe", fake_run)
    with pytest.raises(RuntimeError, match="timeout"):
        await probe.probe_node_memory(
            node="trt-gb10-4",
            partition="loom-staging",
            cluster_name="trt-gb10",
            environment="staging",
            pool_name="gb10",
            candidate_sha="a" * 40,
            account="loom-staging",
            qos="loom-staging",
            srun_path="srun",
            command_timeout_seconds=timeout,
        )
