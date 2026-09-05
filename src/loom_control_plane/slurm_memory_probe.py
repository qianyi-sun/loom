"""Bounded, non-exclusive Slurm memory observation for an installed candidate."""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import re
import time
from dataclasses import dataclass
from uuid import uuid4

MAX_OBSERVATION_AGE_SECONDS = 15.0
_OUTPUT_LIMIT = 4096
_CLEANUP_SECONDS = 3.0
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\Z")

# Positional arguments are data, never interpolated into shell source. No repo,
# env file, credentials, GPU, container, or shared filesystem is used remotely.
_PROBE_SCRIPT = r"""set -eu
uid=$(/usr/bin/id -u)
exec /usr/bin/awk -v nonce="$1" -v candidate="$2" -v uid="$uid" '
  $1 == "MemTotal:" { total=$2; totals++; if ($3 != "kB") bad=1 }
  $1 == "MemAvailable:" { available=$2; values++; if ($3 != "kB") bad=1 }
  END {
    if (bad || totals != 1 || values != 1 || total !~ /^[0-9]+$/ || available !~ /^[0-9]+$/) exit 1;
    printf "v1|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n", nonce, ENVIRON["SLURMD_NODENAME"], ENVIRON["SLURM_JOB_PARTITION"], ENVIRON["SLURM_CLUSTER_NAME"], ENVIRON["SLURM_JOB_ACCOUNT"], ENVIRON["SLURM_JOB_QOS"], uid, ENVIRON["SLURM_JOB_ID"], candidate, total, available, ENVIRON["SLURM_JOB_NODELIST"];
  }' /proc/meminfo
"""


@dataclass(frozen=True)
class MemoryObservation:
    available_memory_mib: int
    observed_at: float


def observation_is_fresh(observation: MemoryObservation) -> bool:
    return 0 <= time.monotonic() - observation.observed_at <= MAX_OBSERVATION_AGE_SECONDS


def _parse_observation(
    output: str,
    *,
    node: str,
    partition: str,
    candidate_sha: str,
    nonce: str,
    uid: int,
    started_at: float,
    cluster_name: str,
    account: str,
    qos: str,
) -> MemoryObservation:
    fields = output.removesuffix("\n").split("|")
    if len(output) > _OUTPUT_LIMIT or len(fields) != 13 or "\n" in "".join(fields):
        raise RuntimeError("Slurm memory probe output is invalid")
    if (
        fields[:5] != ["v1", nonce, node, partition, cluster_name]
        or fields[7] != str(uid)
        or fields[9] != candidate_sha
        or fields[12] != node
        or (account and fields[5] != account)
        or (qos and fields[6] != qos)
    ):
        raise RuntimeError("Slurm memory probe identity is invalid")
    if not all(re.fullmatch(r"[0-9]{1,16}", fields[index]) for index in (8, 10, 11)):
        raise RuntimeError("Slurm memory probe counts are invalid")
    job_id, total_kib, available_kib = (int(fields[index]) for index in (8, 10, 11))
    if job_id <= 0 or not 0 <= available_kib <= total_kib or not 0 < total_kib <= 2**40:
        raise RuntimeError("Slurm memory probe counts are invalid")
    observation = MemoryObservation(available_kib // 1024, started_at)
    if not observation_is_fresh(observation):
        raise RuntimeError("Slurm memory probe observation expired")
    return observation


async def _stop_probe(proc: asyncio.subprocess.Process) -> None:
    # SIGTERM lets synchronous srun forward termination and release its own
    # allocation. Do not scancel by name or touch any other user's jobs.
    if proc.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=_CLEANUP_SECONDS)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()


async def _run_probe(args: tuple[str, ...], *, timeout: float) -> str:
    # Never inherit SLURM_JOB_ID/reservation/exclusive/overlap or user export
    # settings: this must create a new bounded allocation, not enter an old one.
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
    # Preserve only the administrator's client configuration location.
    if "SLURM_CONF" in os.environ:
        env["SLURM_CONF"] = os.environ["SLURM_CONF"]
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    assert proc.stdout is not None and proc.stderr is not None

    async def read_bounded(stream: asyncio.StreamReader) -> bytes:
        data = await stream.read(_OUTPUT_LIMIT + 1)
        if len(data) > _OUTPUT_LIMIT:
            raise RuntimeError("Slurm memory probe output exceeds limit")
        # read() may return before EOF; retain the same total bound.
        while chunk := await stream.read(_OUTPUT_LIMIT + 1 - len(data)):
            data += chunk
            if len(data) > _OUTPUT_LIMIT:
                raise RuntimeError("Slurm memory probe output exceeds limit")
        return data

    readers = [asyncio.create_task(read_bounded(stream)) for stream in (proc.stdout, proc.stderr)]
    try:
        async with asyncio.timeout(timeout):
            stdout, _stderr = await asyncio.gather(*readers)
            await proc.wait()
        if proc.returncode != 0:
            raise RuntimeError("Slurm memory probe command failed")
        return stdout.decode("ascii")
    except (TimeoutError, UnicodeError) as exc:
        raise RuntimeError("Slurm memory probe did not return valid bounded evidence") from exc
    finally:
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        cleanup = asyncio.create_task(_stop_probe(proc))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise


async def probe_node_memory(
    *,
    node: str,
    partition: str,
    candidate_sha: str,
    environment: str,
    pool_name: str,
    cluster_name: str,
    srun_path: str,
    command_timeout_seconds: float,
    account: str = "",
    qos: str = "",
) -> MemoryObservation:
    if not math.isfinite(command_timeout_seconds) or command_timeout_seconds <= 0:
        raise RuntimeError("Slurm memory probe requires a finite positive timeout")
    if not all(
        _NAME_RE.fullmatch(value)
        for value in (node, partition, environment, pool_name, cluster_name)
    ):
        raise RuntimeError("Slurm memory probe requires bounded node and policy identities")
    if re.fullmatch(r"[0-9a-f]{40}", candidate_sha) is None:
        raise RuntimeError("Slurm memory probe requires an installed candidate identity")
    nonce = uuid4().hex
    args = [
        srun_path,
        "--immediate=3",
        "--nodes=1",
        "--ntasks=1",
        f"--nodelist={node}",
        "--cpus-per-task=1",
        "--mem=16M",
        "--time=00:01:00",
        f"--job-name=loom-mem-{environment}-{pool_name}-{nonce}",
        f"--comment=loom-memory-probe:{candidate_sha}",
        "--kill-on-bad-exit=1",
        "--chdir=/tmp",
        "--export=NONE",
        f"--partition={partition}",
    ]
    if account:
        args.append(f"--account={account}")
    if qos:
        args.append(f"--qos={qos}")
    # Local timeout is not the only protection if the client loses contact:
    # GNU timeout bounds the remote reader; Slurm also owns a one-minute limit
    # (plus site OverTimeLimit/KillWait). Never claim a ten-second Slurm limit.
    args.extend(
        (
            "/usr/bin/timeout",
            "--signal=TERM",
            "--kill-after=1s",
            "5s",
            "/bin/sh",
            "-c",
            _PROBE_SCRIPT,
            "loom-memory-probe",
            nonce,
            candidate_sha,
        )
    )
    started_at = time.monotonic()
    output = await _run_probe(tuple(args), timeout=min(command_timeout_seconds, 10.0))
    return _parse_observation(
        output,
        node=node,
        partition=partition,
        candidate_sha=candidate_sha,
        nonce=nonce,
        uid=os.geteuid(),
        started_at=started_at,
        cluster_name=cluster_name,
        account=account,
        qos=qos,
    )
