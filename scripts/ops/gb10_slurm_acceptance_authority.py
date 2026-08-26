#!/usr/bin/env python3
"""Root-installed, candidate-bound acceptance authority for GB10 Slurm."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import stat
import subprocess
import sys
import tempfile
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SERVICE_USER = "loom-rollout"
SERVICE_UID = 995
SERVICE_GID = 2007
SLURM_ACCOUNT = "loom-staging"
SLURM_QOS = "loom-staging"
SLURM_PARTITION = "loom-staging"
CLUSTER_NAME = "trt-gb10"
CONTROLLER_HOST = "gx10-01c7"
# Loom's canonical GB10 inventory remains nodes 1-15 even while a node is busy
# in another partition.  Node 16 is outside Loom's allocation boundary.
SLURM_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
LEGACY_AGENT_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
PRIVATE_WORKER_SERVICE_ENV = {
    pool_name: {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://192.168.50.103:18081",
        "LOOM_WORKER_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_SUBPROCESS_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://192.168.50.103:19000",
        "LOOM_WORKER_TRAJECTORIES_BUCKET": "loom-staging-trajectories",
        "LOOM_WORKER_ARTIFACTS_BUCKET": "loom-staging-artifacts",
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO": ("192.168.50.103:5443/loom-trial-cache"),
    }
    for pool_name in ("gb10", "oldlab")
}
TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"
TRIAL_CACHE_CA_SHA256 = "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
TRIAL_CACHE_CANARY_TAG = "transport-canary"
TRIAL_CACHE_CANARY_DIGEST = (
    "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e"
)
TRIAL_CACHE_CA_RELATIVE_PATH = Path("deploy/worker-pools/trial-cache/staging-ca.crt")
TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH = Path(
    "scripts/ops/staging_trial_cache_registry_node_probe.py"
)
CANDIDATE_ROOT = Path("/opt/loom-staging-runner/candidates")
INSTALLED_PATH = Path("/usr/local/libexec/loom-gb10-slurm-acceptance-authority")
STATE_ROOT = Path("/var/lib/loom-gb10-slurm-authority")
ARTIFACT_PATH = STATE_ROOT / "current.json"
SHA_RE = re.compile(r"[0-9a-f]{40}")
JOB_ID_RE = re.compile(r"[1-9][0-9]*")


class AcceptanceError(RuntimeError):
    """A secret-safe acceptance failure."""


def _run(
    argv: list[str],
    *,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise AcceptanceError(f"command timed out safely: {Path(argv[0]).name}") from exc
    if check and result.returncode != 0:
        raise AcceptanceError(f"command failed safely: {Path(argv[0]).name}")
    return result


def _replace_release_values(value: Any, variables: dict[str, str]) -> Any:
    if isinstance(value, str):
        resolved = value
        for name, replacement in variables.items():
            resolved = resolved.replace(f"${{{name}}}", replacement)
        if "${" in resolved:
            raise AcceptanceError("profile contains an unresolved release value")
        return resolved
    if isinstance(value, list):
        return [_replace_release_values(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _replace_release_values(item, variables) for key, item in value.items()}
    return value


def _one_row(rows: object, *, pool_name: str) -> dict[str, Any]:
    matches = (
        [row for row in rows if isinstance(row, dict) and row.get("pool_name") == pool_name]
        if isinstance(rows, list)
        else []
    )
    if len(matches) != 1:
        raise AcceptanceError(f"profile must contain one {pool_name} row")
    return matches[0]


def _load_contract(candidate_sha: str, image_tag: str) -> dict[str, Any]:
    profile_path = CANDIDATE_ROOT / candidate_sha / "repo/deploy/environment-state/staging.toml"
    try:
        profile_bytes = profile_path.read_bytes()
        raw = tomllib.loads(profile_bytes.decode("utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise AcceptanceError("exact candidate profile is unavailable") from exc
    profile = _replace_release_values(
        raw,
        {
            "IMAGE_TAG": image_tag,
            "ENV_CONFIG_VERSION": image_tag,
            "GIT_SHA": candidate_sha,
        },
    )
    policy = _one_row(profile.get("worker_pool_autoscaler_policies"), pool_name="gb10")
    config = policy.get("actuator_config")
    if not isinstance(config, dict):
        raise AcceptanceError("GB10 Slurm actuator configuration is unavailable")
    expected_config = {
        "slurm_cluster_name": CLUSTER_NAME,
        "slurm_controller_host": CONTROLLER_HOST,
        "partition": SLURM_PARTITION,
        "external_runner": True,
        "slurm_account": SLURM_ACCOUNT,
        "qos_normal": SLURM_QOS,
        "candidate_sha": candidate_sha,
        "max_jobs": len(SLURM_NODES),
    }
    if (
        profile.get("environment") != "staging"
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not True
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != 150
        or any(config.get(key) != value for key, value in expected_config.items())
        or config.get("allowed_nodes") != list(SLURM_NODES)
    ):
        raise AcceptanceError("GB10 Slurm policy does not match the accepted contract")
    desired = _one_row(profile.get("gb10_worker_pool_desired_states"), pool_name="gb10")
    if desired.get("target_slots") != 0 or desired.get("host_intents") != {
        node: "stopped" for node in LEGACY_AGENT_NODES
    }:
        raise AcceptanceError("legacy GB10 node-agent authority is not retired")
    prerequisites = profile.get("external_slurm_runner_prerequisites")
    if not isinstance(prerequisites, dict) or (
        prerequisites.get("materialize") is not True
        or prerequisites.get("require_external_allocation_authority") is not True
        or type(prerequisites.get("manager_witness_export_bootstrap", False)) is not bool
        or "gb10" not in prerequisites.get("pools", [])
        or prerequisites.get("worker_service_env") != PRIVATE_WORKER_SERVICE_ENV
    ):
        raise AcceptanceError("external Slurm authority prerequisites are incomplete")
    supervisors = profile.get("external_slurm_autoscaler_supervisors")
    supervisor = _one_row(supervisors, pool_name="gb10")
    manager_bootstrap = prerequisites.get("manager_witness_export_bootstrap") is True
    if supervisor.get("execution_host") != CONTROLLER_HOST:
        raise AcceptanceError("GB10 supervisor is not controller-bound and active")
    if manager_bootstrap:
        if not isinstance(supervisors, list) or any(
            not isinstance(row, dict)
            or row.get("enabled") is not False
            or row.get("active") is not False
            for row in supervisors
        ):
            raise AcceptanceError("manager witness bootstrap supervisors are not inert")
    elif supervisor.get("enabled") is not True or supervisor.get("active") is not True:
        raise AcceptanceError("GB10 supervisor is not controller-bound and active")
    return {
        "profile_path": profile_path,
        "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
        "repo_dir": Path(str(config.get("repo_dir", ""))),
        "env_file": Path(str(config.get("env_file", ""))),
    }


def _verify_installed_authority() -> None:
    source = Path(__file__)
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise AcceptanceError("installed authority source is unavailable") from exc
    if (
        source != INSTALLED_PATH
        or source.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise AcceptanceError("authority is not the fixed root-installed executable")


def _verify_controller() -> None:
    if os.geteuid() != 0:
        raise AcceptanceError("GB10 acceptance authority requires root")
    if platform.machine() != "aarch64" or platform.node().split(".", 1)[0] != CONTROLLER_HOST:
        raise AcceptanceError("GB10 acceptance authority is controller-only")
    config = _run(["/usr/bin/scontrol", "show", "config"]).stdout
    if (
        re.search(rf"^ClusterName\s*=\s*{CLUSTER_NAME}$", config, re.MULTILINE) is None
        or re.search(
            rf"^SlurmctldHost(?:\[0\])?\s*=\s*{CONTROLLER_HOST}(?:\([^)]*\))?$",
            config,
            re.MULTILINE,
        )
        is None
    ):
        raise AcceptanceError("local Slurm controller authority does not match GB10")
    identity = _run(["/usr/bin/id", "-u", SERVICE_USER]).stdout.strip()
    group = _run(["/usr/bin/id", "-g", SERVICE_USER]).stdout.strip()
    groups = _run(["/usr/bin/id", "-nG", SERVICE_USER]).stdout.split()
    if identity != str(SERVICE_UID) or group != str(SERVICE_GID) or "docker" not in groups:
        raise AcceptanceError("GB10 service identity does not match the fixed contract")


def _git_identity(repo: Path, *, uid: int, gid: int, mode: int, sha: str) -> str:
    try:
        metadata = repo.lstat()
    except OSError as exc:
        raise AcceptanceError("candidate repository is unavailable") from exc
    if (
        repo.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise AcceptanceError("candidate repository metadata is invalid")
    git = ["/usr/bin/git"]
    if uid == SERVICE_UID:
        git = ["runuser", "-u", SERVICE_USER, "--", "/usr/bin/git"]
    git_timeout = 120 if uid == SERVICE_UID else 30
    head = _run([*git, "-C", str(repo), "rev-parse", "HEAD"], timeout=git_timeout).stdout.strip()
    tree = _run(
        [*git, "-C", str(repo), "rev-parse", "HEAD^{tree}"], timeout=git_timeout
    ).stdout.strip()
    dirty = _run(
        [*git, "-C", str(repo), "status", "--porcelain", "--untracked-files=no"],
        timeout=git_timeout,
    ).stdout
    if head != sha or SHA_RE.fullmatch(tree) is None or dirty:
        raise AcceptanceError("candidate repository identity does not match")
    return tree


def _verify_inputs(candidate_sha: str, contract: dict[str, Any]) -> str:
    runtime_repo = CANDIDATE_ROOT / candidate_sha / "repo"
    candidate_tree = _git_identity(
        runtime_repo,
        uid=0,
        gid=0,
        mode=0o755,
        sha=candidate_sha,
    )
    worker_repo = contract["repo_dir"]
    worker_tree = _git_identity(
        worker_repo,
        uid=SERVICE_UID,
        gid=SERVICE_GID,
        mode=0o750,
        sha=candidate_sha,
    )
    env_file = contract["env_file"]
    try:
        env_metadata = env_file.lstat()
    except OSError as exc:
        raise AcceptanceError("candidate worker environment is unavailable") from exc
    if (
        env_file.is_symlink()
        or not stat.S_ISREG(env_metadata.st_mode)
        or env_metadata.st_uid != SERVICE_UID
        or env_metadata.st_gid != SERVICE_GID
        or stat.S_IMODE(env_metadata.st_mode) != 0o600
        or not 0 < env_metadata.st_size <= 1024 * 1024
        or worker_tree != candidate_tree
    ):
        raise AcceptanceError("candidate worker inputs are unsafe or divergent")
    return candidate_tree


_NODE_PROBE = r"""
import json, os, subprocess, sys
(
    node,
    repo,
    env_file,
    expected_sha,
    registry_repo,
    ca_sha256,
    canary_digest,
    registry_probe_path,
    registry_ca_path,
) = sys.argv[1:]
actual_node = subprocess.check_output(
    ["/usr/bin/scontrol", "show", "hostnames", os.environ["SLURM_JOB_NODELIST"]],
    text=True,
).strip()
assert actual_node == node
assert os.geteuid() == 995 and os.getegid() == 2007
assert "docker" in subprocess.check_output(["/usr/bin/id", "-nG"], text=True).split()
assert subprocess.run(
    ["/usr/bin/systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
).returncode == 0
assert subprocess.run(
    ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
).returncode == 0
assert subprocess.check_output(
    ["/usr/bin/git", "-C", repo, "rev-parse", "HEAD"], text=True
).strip() == expected_sha
assert os.path.isfile(env_file) and os.access(env_file, os.R_OK)
registry_probe = subprocess.check_output(
    [
        "/usr/bin/python3",
        registry_probe_path,
        "--env-file",
        env_file,
        "--ca-file",
        registry_ca_path,
        "--docker-bin",
        "/usr/bin/docker",
        "--expected-registry-repo",
        registry_repo,
        "--expected-ca-sha256",
        ca_sha256,
        "--canary-digest",
        canary_digest,
    ],
    text=True,
)
print(json.dumps(
    {
        "node": node,
        "candidate_sha": expected_sha,
        "trial_cache_registry": json.loads(registry_probe),
    },
    sort_keys=True,
))
"""


def _cleanup_probe_jobs(job_name: str) -> None:
    queue_command = [
        "runuser",
        "-u",
        SERVICE_USER,
        "--",
        "/usr/bin/squeue",
        "--noheader",
        f"--user={SERVICE_USER}",
        f"--name={job_name}",
        "--format=%A|%j",
    ]
    queued = _run(queue_command, timeout=10, check=False)
    if queued.returncode != 0:
        raise AcceptanceError("could not verify acceptance-probe cleanup")
    job_ids: list[str] = []
    for raw_line in queued.stdout.splitlines():
        job_id, separator, queued_name = raw_line.strip().partition("|")
        if not separator or queued_name != job_name or JOB_ID_RE.fullmatch(job_id) is None:
            raise AcceptanceError("acceptance-probe cleanup evidence is invalid")
        job_ids.append(job_id)
    if job_ids:
        _run(
            [
                "runuser",
                "-u",
                SERVICE_USER,
                "--",
                "/usr/bin/scancel",
                *job_ids,
            ],
            timeout=10,
        )
        readback = _run(queue_command, timeout=10, check=False)
        if readback.returncode != 0 or readback.stdout.strip():
            raise AcceptanceError("acceptance-probe cleanup did not converge")


def _node_is_deferred_busy(
    *,
    node_config: str,
    result: subprocess.CompletedProcess[str],
) -> bool:
    state_match = re.search(r"(?:^| )State=([A-Z]+)", node_config)
    cpu_match = re.search(r"(?:^| )CPUAlloc=([0-9]+)", node_config)
    memory_match = re.search(r"(?:^| )AllocMem=([0-9]+)", node_config)
    busy_error = (
        "Requested nodes are busy" in result.stderr
        or "temporarily unable to accept work" in result.stderr
    )
    return bool(
        result.returncode != 0
        and busy_error
        and state_match is not None
        and state_match.group(1) in {"ALLOCATED", "MIXED"}
        and (
            (cpu_match is not None and int(cpu_match.group(1)) > 0)
            or (memory_match is not None and int(memory_match.group(1)) > 0)
        )
    )


def _probe_nodes(
    candidate_sha: str,
    contract: dict[str, Any],
) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    deferred_busy: list[str] = []
    for index, node in enumerate(SLURM_NODES, start=1):
        node_config = _run(["/usr/bin/scontrol", "show", "node", node, "-o"]).stdout
        partition_pattern = rf"(?:^| )Partitions=[^ ]*\b{re.escape(SLURM_PARTITION)}\b"
        if re.search(partition_pattern, node_config) is None:
            raise AcceptanceError(
                f"canonical node is outside {SLURM_PARTITION} partition: {node}"
            )
        job_name = f"loom-accept-{candidate_sha[:7]}-{index}-{os.getpid()}"
        command = [
            "runuser",
            "-u",
            SERVICE_USER,
            "--",
            "srun",
            "--quiet",
            "--immediate=15",
            f"--job-name={job_name}",
            "--nodes=1",
            "--ntasks=1",
            "--cpus-per-task=1",
            "--mem=128M",
            "--time=00:02:00",
            f"--partition={SLURM_PARTITION}",
            f"--account={SLURM_ACCOUNT}",
            f"--qos={SLURM_QOS}",
            f"--nodelist={node}",
            "/usr/bin/python3",
            "-c",
            _NODE_PROBE,
            node,
            str(contract["repo_dir"]),
            str(contract["env_file"]),
            candidate_sha,
            TRIAL_CACHE_REGISTRY_REPO,
            TRIAL_CACHE_CA_SHA256,
            TRIAL_CACHE_CANARY_DIGEST,
            str(contract["repo_dir"] / TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH),
            str(contract["repo_dir"] / TRIAL_CACHE_CA_RELATIVE_PATH),
        ]
        try:
            result = _run(command, timeout=60, check=False)
        finally:
            _cleanup_probe_jobs(job_name)
        if _node_is_deferred_busy(node_config=node_config, result=result):
            deferred_busy.append(node)
            continue
        if result.returncode != 0:
            raise AcceptanceError(f"node allocation failed safely: {node}")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AcceptanceError(f"node allocation returned invalid evidence: {node}") from exc
        if payload != {
            "candidate_sha": candidate_sha,
            "node": node,
            "trial_cache_registry": {
                "ca_sha256": TRIAL_CACHE_CA_SHA256,
                "registry_image": (f"{TRIAL_CACHE_REGISTRY_REPO}:{TRIAL_CACHE_CANARY_TAG}"),
                "repo_digest": (f"{TRIAL_CACHE_REGISTRY_REPO}@{TRIAL_CACHE_CANARY_DIGEST}"),
            },
        }:
            raise AcceptanceError(f"node allocation evidence mismatched: {node}")
        passed.append(node)
    if not passed:
        raise AcceptanceError("no GB10 node accepted the real Slurm allocation probe")
    return passed, deferred_busy


def _write_artifact(payload: dict[str, Any]) -> None:
    STATE_ROOT.mkdir(mode=0o755, parents=True, exist_ok=True)
    metadata = STATE_ROOT.lstat()
    if (
        STATE_ROOT.is_symlink()
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise AcceptanceError("authority state root metadata is invalid")
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=".current.", dir=STATE_ROOT)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o644)
        os.write(descriptor, encoded)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, ARTIFACT_PATH)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--image-tag", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if SHA_RE.fullmatch(args.candidate_sha) is None:
        raise AcceptanceError("candidate SHA must be exact")
    if args.image_tag != f"staging-{args.candidate_sha[:7]}":
        raise AcceptanceError("image tag does not match the exact candidate")
    _verify_installed_authority()
    _verify_controller()
    contract = _load_contract(args.candidate_sha, args.image_tag)
    candidate_tree = _verify_inputs(args.candidate_sha, contract)
    probed_nodes, deferred_busy_nodes = _probe_nodes(args.candidate_sha, contract)
    generated_at = datetime.now(UTC)
    artifact = {
        "schema_version": 1,
        "kind": "loom_gb10_slurm_acceptance",
        "result": "pass",
        "candidate_sha": args.candidate_sha,
        "candidate_tree": candidate_tree,
        "profile_sha256": contract["profile_sha256"],
        "cluster_name": CLUSTER_NAME,
        "controller_host": CONTROLLER_HOST,
        "service_identity": {
            "user": SERVICE_USER,
            "uid": SERVICE_UID,
            "gid": SERVICE_GID,
            "account": SLURM_ACCOUNT,
            "qos": SLURM_QOS,
        },
        "nodes": list(SLURM_NODES),
        "node_count": len(SLURM_NODES),
        "probed_nodes": probed_nodes,
        "probed_node_count": len(probed_nodes),
        "deferred_busy_nodes": deferred_busy_nodes,
        "trial_cache_registry": {
            "ca_sha256": TRIAL_CACHE_CA_SHA256,
            "canary_digest": TRIAL_CACHE_CANARY_DIGEST,
            "repository": TRIAL_CACHE_REGISTRY_REPO,
        },
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=30)).isoformat(),
    }
    _write_artifact(artifact)
    print(
        f"accepted candidate={args.candidate_sha} "
        f"probed={len(probed_nodes)} deferred_busy={len(deferred_busy_nodes)}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AcceptanceError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
