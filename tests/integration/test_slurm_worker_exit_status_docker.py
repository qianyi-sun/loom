"""Exercise the generated Slurm script against real Compose process semantics."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from loom_control_plane.elastic_slurm_worker_controller import build_sbatch_request
from tests.unit.test_elastic_slurm_worker_controller import _config

pytestmark = pytest.mark.docker


@pytest.mark.parametrize("worker_exit", [0, 17])
@pytest.mark.parametrize("launcher", ["controller", "operator-script"])
def test_slurm_script_preserves_worker_exit_and_cleans_resources(
    tmp_path: Path, worker_exit: int, launcher: str,
) -> None:
    repo = tmp_path / "repo"
    (repo / "deploy").mkdir(parents=True)
    (repo / "deploy/docker-compose.remote-worker.yml").write_text(
        f"""services:
  worker:
    image: alpine:3.19
    command: [sh, -c, 'exit {worker_exit}']
    restart: 'no'
    network_mode: none
    cpus: 0.1
    mem_limit: 64m
    pids_limit: 32
    volumes:
      - remote_worker_trajectories:/trajectories
      - remote_worker_benchmarks:/benchmarks
volumes:
  remote_worker_trajectories:
  remote_worker_benchmarks:
""",
    )
    env_file = tmp_path / "worker.env"
    env_file.write_text("")
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    job_id = uuid4().hex
    project = f"loom-exit-test-aaaaaaaaaaaa-{job_id}"
    request = build_sbatch_request(
        _config(env_file=str(env_file), repo_dir=str(repo)), node="oldlab-3",
    )
    script = request.stdin
    if launcher == "operator-script":
        plan = tmp_path / "plan.csv"
        plan.write_text(
            "host,status,cpus,mem_total_mib,docker_cpus,recommended_concurrency,reason\n"
            "worker-a,include,2,4096,2,1,\n",
        )
        rendered = subprocess.run(
            [
                "bash", str(Path(__file__).resolve().parents[2]
                            / "scripts/ops/worker_pool_slurm_submit.sh"), str(plan),
                "--env-file", str(env_file), "--repo-dir", str(repo),
                "--sandbox-identity", "exit-test", "--candidate-sha", "a" * 40,
                "--container-cpus", "0.1", "--container-memory-mib", "64",
                "--container-pids", "32", "--dry-run",
            ],
            capture_output=True, text=True, check=True, timeout=15,
        )
        lines = rendered.stdout.splitlines()
        assert lines[0].startswith("sbatch ") and lines[-1] == "SLURM"
        script = "\n".join(lines[1:-1]) + "\n"
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("LOOM_", "COMPOSE_", "SLURM_"))
    }
    environment.update({
        "SLURM_JOB_ID": job_id,
        "SLURM_TMPDIR": str(runtime_root),
        "LOOM_WORKER_SANDBOX_IDENTITY": "exit-test",
        "LOOM_WORKER_CANDIDATE_SHA": "a" * 40,
        "LOOM_WORKER_SLURM_ALLOCATED_GPUS": "0",
        "LOOM_WORKER_REQUIRE_CGROUP_PARENT": "0",
        "LOOM_REMOTE_WORKER_ENV_FILE": str(env_file),
        "LOOM_REMOTE_WORKER_REPO_DIR": str(repo),
    })
    compose = [
        "docker", "compose", "--project-name", project,
        "-f", str(repo / "deploy/docker-compose.remote-worker.yml"),
    ]
    try:
        result = subprocess.run(
            ["bash"], input=script, env=environment,
            capture_output=True, text=True, timeout=90,
        )
        assert result.returncode == worker_exit, result.stdout + result.stderr
        for command in (
            ["docker", "ps", "-aq"],
            ["docker", "volume", "ls", "-q"],
            ["docker", "network", "ls", "-q"],
        ):
            remaining = subprocess.run(
                [*command, "--filter", f"label=com.docker.compose.project={project}"],
                capture_output=True, text=True, check=True, timeout=15,
            )
            assert not remaining.stdout.strip(), remaining.stdout
        assert list(runtime_root.iterdir()) == []
    finally:
        subprocess.run(
            [*compose, "down", "--volumes", "--remove-orphans"],
            capture_output=True, check=True, timeout=30,
        )
