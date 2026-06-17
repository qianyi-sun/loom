from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose.remote-worker.yml"


def test_remote_worker_compose_runs_only_worker() -> None:
    data = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    assert set(data["services"]) == {"worker"}
    assert set(data["volumes"]) == {
        "remote_worker_trajectories",
        "remote_worker_benchmarks",
    }


def test_remote_worker_compose_uses_operator_supplied_endpoints() -> None:
    data = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    env = data["services"]["worker"]["environment"]
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
