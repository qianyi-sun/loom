import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "ops" / "worker_pool_inventory.sh"
_PLAN_SCRIPT = _REPO_ROOT / "scripts" / "ops" / "worker_pool_plan.py"
_SLURM_SCRIPT = _REPO_ROOT / "scripts" / "ops" / "worker_pool_slurm_submit.sh"
_TUNNEL_SCRIPT = _REPO_ROOT / "scripts" / "ops" / "worker_service_tunnels.py"


def test_worker_pool_inventory_script_is_valid_bash() -> None:
    subprocess.run(["bash", "-n", str(_SCRIPT)], check=True)


def test_worker_pool_slurm_submit_script_is_valid_bash() -> None:
    subprocess.run(["bash", "-n", str(_SLURM_SCRIPT)], check=True)


def test_worker_service_tunnel_script_compiles() -> None:
    subprocess.run([sys.executable, "-m", "py_compile", str(_TUNNEL_SCRIPT)], check=True)


def test_worker_pool_inventory_script_has_no_environment_specific_hosts() -> None:
    text = (
        _SCRIPT.read_text(encoding="utf-8")
        + _SLURM_SCRIPT.read_text(encoding="utf-8")
        + _TUNNEL_SCRIPT.read_text(encoding="utf-8")
    )
    forbidden = (
        "OLD" + "LAB",
        "192" + ".168.",
        "10" + ".",
        "172" + ".16.",
        "platform" + "-dev",
    )
    assert not any(marker in text for marker in forbidden)


def test_worker_pool_inventory_collects_open_file_and_docker_disk_signals() -> None:
    text = _SCRIPT.read_text(encoding="utf-8")
    assert "ulimit_nofile=" in text
    assert "docker_root_dir=" in text
    assert "docker_disk_avail=" in text


def test_worker_pool_plan_recommends_every_healthy_host(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.txt"
    inventory.write_text(
        "\n".join([
            "## worker-a",
            "host=worker-a",
            "cpus=64",
            "mem_total_mib=262144 mem_available_mib=200000",
            "docker_version=26.1 docker_cpus=64 docker_mem_bytes=270000000000",
            "control_plane=ok",
            "gateway=ok",
            "minio=ok",
            "ulimit_nofile=1048576",
            "docker_disk_avail=500G",
            "## worker-b",
            "host=worker-b",
            "cpus=24",
            "mem_total_mib=65536 mem_available_mib=50000",
            "docker_version=26.1 docker_cpus=24 docker_mem_bytes=69000000000",
            "control_plane=ok",
            "gateway=ok",
            "minio=ok",
            "ulimit_nofile=65536",
            "docker_disk_avail=100G",
            "## worker-c",
            "ssh=failed",
            "",
        ]),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(_PLAN_SCRIPT),
            "--inventory",
            str(inventory),
            "--cpu-per-trial",
            "2",
            "--mem-mib-per-trial",
            "8192",
            "--max-per-host",
            "96",
        ],
        text=True,
        check=True,
        capture_output=True,
    )

    lines = result.stdout.strip().splitlines()
    assert lines[0] == (
        "host,status,cpus,mem_total_mib,docker_cpus,"
        "recommended_concurrency,reason"
    )
    assert "worker-a,include,64,262144,64,32," in lines
    assert "worker-b,include,24,65536,24,8," in lines
    assert "worker-c,exclude,,,,0,ssh failed" in lines


def test_worker_pool_slurm_submit_dry_run_uses_plan_rows(tmp_path: Path) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text(
        "\n".join([
            "host,status,cpus,mem_total_mib,docker_cpus,recommended_concurrency,reason",
            "worker-a,include,64,262144,64,32,",
            "worker-b,exclude,,,,0,ssh failed",
            "",
        ]),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "bash",
            str(_SLURM_SCRIPT),
            str(plan),
            "--env-file",
            "/secure/.env.remote-worker",
            "--repo-dir",
            "/opt/loom",
            "--sandbox-identity",
            "staging",
            "--candidate-sha",
            "a" * 40,
            "--container-cpus",
            "2",
            "--container-memory-mib",
            "4096",
            "--container-pids",
            "512",
            "--dry-run",
        ],
        text=True,
        check=True,
        capture_output=True,
    )

    assert "--nodelist=worker-a" in result.stdout
    assert "LOOM_WORKER_MAX_CONCURRENT=32" in result.stdout
    assert "worker-b" not in result.stdout
    assert "sbatch" in result.stdout
    assert "--exclusive" not in result.stdout
    assert "LOOM_WORKER_CONTAINER_CPUS=2" in result.stdout


def test_worker_pool_slurm_submit_dry_run_cleans_up_compose_on_exit(
    tmp_path: Path,
) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text(
        "\n".join([
            "host,status,cpus,mem_total_mib,docker_cpus,recommended_concurrency,reason",
            "worker-a,include,64,262144,64,32,",
            "",
        ]),
        encoding="utf-8",
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    env_file = tmp_path / "worker.env"
    env_file.write_text("LOOM_IMAGE_TAG=test\n", encoding="utf-8")
    result = subprocess.run(
        [
            "bash",
            str(_SLURM_SCRIPT),
            str(plan),
            "--env-file",
            str(env_file),
            "--repo-dir",
            str(repo),
            "--sandbox-identity",
            "staging",
            "--candidate-sha",
            "a" * 40,
            "--container-cpus",
            "2",
            "--container-memory-mib",
            "4096",
            "--container-pids",
            "512",
            "--dry-run",
        ],
        text=True,
        check=True,
        capture_output=True,
    )

    assert "trap cleanup EXIT" in result.stdout
    assert "trap 'cleanup 130' INT" in result.stdout
    assert "trap 'cleanup 143' TERM" in result.stdout
    assert 'docker compose "${compose_args[@]}" up --build &' in result.stdout
    assert "compose_pid=$!" in result.stdout
    assert 'wait "$compose_pid"' in result.stdout
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in result.stdout

    emitted_script = result.stdout.split("<<'SLURM'\n", 1)[1].rsplit("\nSLURM", 1)[0]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_log = tmp_path / "docker.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
if [ "${FAKE_DOCKER_FAILURE:-}" = "volume-rm" ] \
    && [ "${1:-}" = "volume" ] && [ "${2:-}" = "rm" ]; then
  exit 17
fi
if [ "${FAKE_DOCKER_FAILURE:-}" = "compose-down" ] \
    && [ "${1:-}" = "compose" ]; then
  for arg in "$@"; do
    if [ "$arg" = "down" ]; then
      exit 17
    fi
  done
fi
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = {
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "SLURM_JOB_ID": "4242",
        "LOOM_WORKER_SANDBOX_IDENTITY": "staging",
        "LOOM_WORKER_CANDIDATE_SHA": "a" * 40,
        "FAKE_DOCKER_LOG": str(docker_log),
    }

    emitted = subprocess.run(
        ["/bin/bash"],
        input=emitted_script,
        cwd=repo,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert emitted.returncode == 0, emitted.stderr
    project = "loom-staging-aaaaaaaaaaaa-4242"
    assert docker_log.read_text(encoding="utf-8").splitlines()[-4:] == [
        f"volume inspect {project}_remote_worker_trajectories",
        f"volume rm {project}_remote_worker_trajectories",
        f"volume inspect {project}_remote_worker_benchmarks",
        f"volume rm {project}_remote_worker_benchmarks",
    ]

    for failure in ("compose-down", "volume-rm"):
        failed_environment = {
            **environment,
            "FAKE_DOCKER_FAILURE": failure,
            "SLURM_JOB_ID": f"4243-{failure}",
        }
        failed = subprocess.run(
            ["/bin/bash"],
            input=emitted_script,
            cwd=repo,
            env=failed_environment,
            text=True,
            capture_output=True,
            check=False,
        )

        assert failed.returncode == 17, failure
