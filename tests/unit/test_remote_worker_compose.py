from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose.remote-worker.yml"
_CGROUP_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose.remote-worker.cgroup-parent.yml"
_DEV_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose.dev.yml"
_WORKER_NOFILE_LIMIT = 65_536


def _env_map(raw: object) -> dict[str, str | None]:
    if isinstance(raw, dict):
        return raw
    assert isinstance(raw, list)
    env: dict[str, str | None] = {}
    for item in raw:
        assert isinstance(item, str)
        key, separator, value = item.partition("=")
        env[key] = value if separator else None
    return env


def _worker_service(compose_file: Path) -> dict[str, object]:
    data = yaml.safe_load(compose_file.read_text(encoding="utf-8"))
    return data["services"]["worker"]


def test_remote_worker_compose_runs_only_worker() -> None:
    data = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    assert set(data["services"]) == {"worker"}
    assert set(data["volumes"]) == {
        "remote_worker_trajectories",
        "remote_worker_benchmarks",
    }


def test_remote_worker_compose_uses_operator_supplied_endpoints() -> None:
    env = _env_map(_worker_service(_COMPOSE)["environment"])
    assert env["LOOM_WORKER_CONTROL_PLANE_URL"].startswith(
        "${LOOM_WORKER_CONTROL_PLANE_URL:?",
    )
    assert env["LOOM_WORKER_GATEWAY_URL"].startswith(
        "${LOOM_WORKER_GATEWAY_URL:?",
    )
    assert env["LOOM_WORKER_MINIO_ENDPOINT"].startswith(
        "${LOOM_WORKER_MINIO_ENDPOINT:?",
    )
    assert env["LOOM_WORKER_MAX_CONCURRENT"] == "${LOOM_WORKER_MAX_CONCURRENT:-5}"
    assert env["LOOM_WORKER_DOCKER_API_TIMEOUT_SEC"] == (
        "${LOOM_WORKER_DOCKER_API_TIMEOUT_SEC:-1800}"
    )
    assert env["LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS"] == (
        "${LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS:-256}"
    )
    assert env["LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC"] == (
        "${LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC:-300}"
    )
    assert env["LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC"] == (
        "${LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC:-300}"
    )
    assert "HF_TOKEN" not in env
    assert env["LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS"] == (
        "${LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS:-}"
    )


def test_remote_worker_compose_container_caps_passthrough() -> None:
    # #896: the worker PROCESS reads the per-container caps so it can
    # re-apply them to the trial/sidecar containers it creates. Empty
    # default → pydantic uses the schema default (0/unbounded).
    worker = _worker_service(_COMPOSE)
    env = _env_map(worker["environment"])
    # Numeric 0 default (== schema default). NOT empty — the WorkerSettings
    # float/int fields cannot parse "" and would crash worker startup.
    assert env["LOOM_WORKER_CONTAINER_CPUS"] == "${LOOM_WORKER_CONTAINER_CPUS:-0}"
    assert env["LOOM_WORKER_CONTAINER_MEMORY_MIB"] == ("${LOOM_WORKER_CONTAINER_MEMORY_MIB:-0}")
    assert env["LOOM_WORKER_CONTAINER_PIDS"] == "${LOOM_WORKER_CONTAINER_PIDS:-0}"
    # The compose-level caps bound THIS container (unbounded by default).
    assert worker["cpus"] == "${LOOM_WORKER_CONTAINER_CPUS:-0}"
    assert worker["mem_limit"] == "${LOOM_WORKER_CONTAINER_MEMORY_MIB:-0}m"
    assert worker["pids_limit"] == "${LOOM_WORKER_CONTAINER_PIDS:--1}"
    assert env["LOOM_WORKER_REQUIRE_CGROUP_PARENT"] == (
        "${LOOM_WORKER_REQUIRE_CGROUP_PARENT:-false}"
    )
    assert env["LOOM_WORKER_CGROUP_PARENT"] == "${LOOM_WORKER_CGROUP_PARENT:-}"


def test_nonexclusive_compose_overlay_requires_exact_cgroup_parent() -> None:
    worker = _worker_service(_CGROUP_COMPOSE)

    assert worker == {
        "cgroup_parent": ("${LOOM_WORKER_CGROUP_PARENT:?set by the Slurm batch controller}"),
    }


def test_remote_worker_compose_stamps_slurm_identity_and_disables_restart() -> None:
    worker = _worker_service(_COMPOSE)
    env = _env_map(worker["environment"])

    assert env["LOOM_WORKER_SANDBOX_IDENTITY"] == (
        "${LOOM_WORKER_SANDBOX_IDENTITY:-manual}"
    )
    assert env["LOOM_WORKER_CANDIDATE_SHA"] == (
        "${LOOM_WORKER_CANDIDATE_SHA:-legacy}"
    )
    assert env["LOOM_WORKER_SLURM_JOB_ID"] == "${LOOM_WORKER_SLURM_JOB_ID:-none}"
    assert env["LOOM_WORKER_COMPOSE_PROJECT"] == (
        "${LOOM_WORKER_COMPOSE_PROJECT:-manual}"
    )
    assert env["LOOM_WORKER_SLURM_ALLOCATED_GPUS"] == ("${LOOM_WORKER_SLURM_ALLOCATED_GPUS:--1}")
    assert worker["labels"] == {
        "loom.sandbox": "${LOOM_WORKER_SANDBOX_IDENTITY:-manual}",
        "loom.candidate_sha": "${LOOM_WORKER_CANDIDATE_SHA:-legacy}",
        "loom.slurm_job_id": "${LOOM_WORKER_SLURM_JOB_ID:-none}",
        "loom.compose_project": "${LOOM_WORKER_COMPOSE_PROJECT:-manual}",
    }
    assert worker["restart"] == "${LOOM_WORKER_RESTART_POLICY:-on-failure}"


def test_compose_workers_raise_nofile_limit_for_concurrent_sandboxes() -> None:
    for compose_file in (_COMPOSE, _DEV_COMPOSE):
        worker = _worker_service(compose_file)
        assert worker["ulimits"]["nofile"] == {
            "soft": _WORKER_NOFILE_LIMIT,
            "hard": _WORKER_NOFILE_LIMIT,
        }


def test_dev_compose_published_ports_default_to_loopback() -> None:
    """Dev compose is not the public deployment path; every host port
    must bind to loopback unless an operator opts into wider exposure."""
    data = yaml.safe_load(_DEV_COMPOSE.read_text(encoding="utf-8"))
    published: list[str] = []
    for service in data["services"].values():
        published.extend(service.get("ports", []))

    assert published, "expected dev compose to publish local development ports"
    assert all(
        isinstance(port, str) and port.startswith("${LOOM_DEV_BIND_ADDR:-127.0.0.1}:")
        for port in published
    )


def test_remote_worker_compose_has_no_environment_specific_hosts() -> None:
    text = _COMPOSE.read_text(encoding="utf-8")
    forbidden = (
        "OLD" + "LAB",
        "192" + ".168.",
        "10" + ".",
        "172" + ".16.",
        "platform" + "-dev",
    )
    assert not any(marker in text for marker in forbidden)
