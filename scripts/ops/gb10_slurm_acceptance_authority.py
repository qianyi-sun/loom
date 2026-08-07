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
CLUSTER_NAME = "trt-gb10"
CONTROLLER_HOST = "gx10-01c7"
NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
CANDIDATE_ROOT = Path("/opt/loom-staging-runner/candidates")
INSTALLED_PATH = Path("/usr/local/libexec/loom-gb10-slurm-acceptance-authority")
STATE_ROOT = Path("/var/lib/loom-gb10-slurm-authority")
ARTIFACT_PATH = STATE_ROOT / "current.json"
SHA_RE = re.compile(r"[0-9a-f]{40}")


class AcceptanceError(RuntimeError):
    """A secret-safe acceptance failure."""


def _run(
    argv: list[str],
    *,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        argv,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )
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
        return {
            key: _replace_release_values(item, variables)
            for key, item in value.items()
        }
    return value


def _one_row(rows: object, *, pool_name: str) -> dict[str, Any]:
    matches = [
        row
        for row in rows if isinstance(row, dict) and row.get("pool_name") == pool_name
    ] if isinstance(rows, list) else []
    if len(matches) != 1:
        raise AcceptanceError(f"profile must contain one {pool_name} row")
    return matches[0]


def _load_contract(candidate_sha: str, image_tag: str) -> dict[str, Any]:
    profile_path = (
        CANDIDATE_ROOT
        / candidate_sha
        / "repo/deploy/environment-state/staging.toml"
    )
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
        "partition": "gb10",
        "external_runner": True,
        "slurm_account": SLURM_ACCOUNT,
        "qos_normal": SLURM_QOS,
        "candidate_sha": candidate_sha,
    }
    if (
        profile.get("environment") != "staging"
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not True
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != 150
        or any(config.get(key) != value for key, value in expected_config.items())
        or config.get("allowed_nodes") != list(NODES)
    ):
        raise AcceptanceError("GB10 Slurm policy does not match the accepted contract")
    desired = _one_row(profile.get("gb10_worker_pool_desired_states"), pool_name="gb10")
    if desired.get("target_slots") != 0 or desired.get("host_intents") != {
        node: "stopped" for node in NODES
    }:
        raise AcceptanceError("legacy GB10 node-agent authority is not retired")
    prerequisites = profile.get("external_slurm_runner_prerequisites")
    if not isinstance(prerequisites, dict) or (
        prerequisites.get("materialize") is not True
        or prerequisites.get("require_external_allocation_authority") is not True
        or "gb10" not in prerequisites.get("pools", [])
    ):
        raise AcceptanceError("external Slurm authority prerequisites are incomplete")
    supervisors = profile.get("external_slurm_autoscaler_supervisors")
    supervisor = _one_row(supervisors, pool_name="gb10")
    if (
        supervisor.get("execution_host") != CONTROLLER_HOST
        or supervisor.get("enabled") is not True
        or supervisor.get("active") is not True
    ):
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
    head = _run([*git, "-C", str(repo), "rev-parse", "HEAD"]).stdout.strip()
    tree = _run([*git, "-C", str(repo), "rev-parse", "HEAD^{tree}"]).stdout.strip()
    dirty = _run(
        [*git, "-C", str(repo), "status", "--porcelain", "--untracked-files=no"]
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
node, repo, env_file, expected_sha = sys.argv[1:]
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
print(json.dumps({"node": node, "candidate_sha": expected_sha}, sort_keys=True))
"""


def _probe_nodes(candidate_sha: str, contract: dict[str, Any]) -> list[str]:
    passed: list[str] = []
    for node in NODES:
        node_config = _run(["/usr/bin/scontrol", "show", "node", node, "-o"]).stdout
        if re.search(r"(?:^| )Partitions=[^ ]*\bgb10\b", node_config) is None:
            raise AcceptanceError(f"canonical node is outside GB10 partition: {node}")
        command = [
            "runuser", "-u", SERVICE_USER, "--",
            "srun",
            "--quiet",
            "--nodes=1",
            "--ntasks=1",
            "--cpus-per-task=1",
            "--mem=128M",
            "--time=00:02:00",
            "--partition=gb10",
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
        ]
        result = _run(command, timeout=180)
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AcceptanceError(f"node allocation returned invalid evidence: {node}") from exc
        if payload != {"candidate_sha": candidate_sha, "node": node}:
            raise AcceptanceError(f"node allocation evidence mismatched: {node}")
        passed.append(node)
    return passed


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
    nodes = _probe_nodes(args.candidate_sha, contract)
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
        "nodes": nodes,
        "node_count": len(nodes),
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=30)).isoformat(),
    }
    _write_artifact(artifact)
    print(f"accepted candidate={args.candidate_sha} nodes={len(nodes)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AcceptanceError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
