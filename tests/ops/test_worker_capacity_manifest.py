from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/worker_capacity_manifest.py"
DEFAULT_VARS = [
    "--var",
    "PROD_IMAGE_TAG=prod-v1.0.0",
    "--var",
    "PROD_SOURCE_COMMIT=1111111111111111111111111111111111111111",
    "--var",
    "BETA_IMAGE_TAG=dev-2222222",
    "--var",
    "BETA_SOURCE_COMMIT=2222222222222222222222222222222222222222",
]


def _run_capacity(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_manifest(path: Path, hosts: str) -> None:
    path.write_text(
        f"""
schema_version = 1

[defaults]
prod_gets_remaining = true
default_beta_slots = 0
beta_slot_limit_per_host = 1

[environments.prod]
name = "production"
api_url = "https://yylx.world/prod/api"
image_tag = "prod-v1.0.0"
source_commit = "1111111111111111111111111111111111111111"
compose_service = "loom-prod-worker"
k8s_deployment = "loom-prod-worker"
k8s_namespace = "loom-prod"

[environments.beta]
name = "development"
api_url = "https://yylx.world/dev/api"
image_tag = "dev-2222222"
source_commit = "2222222222222222222222222222222222222222"
compose_service = "loom-dev-worker"
k8s_deployment = "loom-dev-worker"
k8s_namespace = "loom-dev"

{hosts}
""".lstrip(),
        encoding="utf-8",
    )


def test_default_manifest_is_prod_first_and_secret_safe() -> None:
    completed = _run_capacity(*DEFAULT_VARS)

    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report["status"] == "pass"
    assert report["summary"]["prod_slots"] == 180
    assert report["summary"]["beta_slots"] == 0
    assert report["summary"]["state_counts"]["unreachable"] == 1

    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["trt-gb10-1"]["prod_slots"] == 10
    assert hosts["trt-gb10-1"]["beta_slots"] == 0
    assert hosts["trt-gb10-14"]["state"] == "unreachable"
    assert hosts["trt-gb10-14"]["prod_slots"] == 0
    assert hosts["trt-gb10-14"]["beta_slots"] == 0
    assert "Bearer" not in completed.stdout
    assert "sk-" not in completed.stdout
    assert "loom_api_" not in completed.stdout


def test_manifest_expresses_beta_lease_and_draining_states(tmp_path: Path) -> None:
    manifest = tmp_path / "lease.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
beta_slots = 1

[[hosts]]
name = "gb10-2"
pool = "gb10-arm64"
total_slots = 10
state = "beta_draining"
beta_slots = 1

[[hosts]]
name = "gb10-3"
pool = "gb10-arm64"
total_slots = 10
state = "host_draining"

[[hosts]]
name = "gb10-4"
pool = "gb10-arm64"
total_slots = 10
state = "unreachable"
""",
    )

    completed = _run_capacity("--manifest", manifest)

    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["prod_slots"] == 9
    assert hosts["gb10-1"]["beta_slots"] == 1
    assert hosts["gb10-1"]["beta"]["drain_state"] == "leased"
    assert hosts["gb10-2"]["beta"]["drain_state"] == "draining"
    assert hosts["gb10-3"]["prod_slots"] == 0
    assert hosts["gb10-4"]["state"] == "unreachable"


def test_observed_cross_environment_drift_redacts_secrets(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
""",
    )
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "worker-prod-1",
                        "host": "gb10-1",
                        "environment": "production",
                        "api_url": "https://yylx.world/dev/api?token=loom_api_livevalue",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "compose_service": "loom-dev-worker",
                        "k8s_deployment": "loom-dev-worker",
                        "max_concurrent": 10,
                        "service_token": "Bearer sk-live-secret-value",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    completed = _run_capacity("--manifest", manifest, "--observed-json", observed)

    assert completed.returncode == 1
    assert "loom_api_livevalue" not in completed.stdout
    assert "sk-live-secret-value" not in completed.stdout
    assert "<redacted>" in completed.stdout
    report = json.loads(completed.stdout)
    paths = {item["path"] for item in report["drift"]}
    assert "workers[0].api_url" in paths
    assert "workers[0].image_tag" in paths
    assert "workers[0].source_commit" in paths
    assert "workers[0].compose_service" in paths
    assert "workers[0].k8s_deployment" in paths


def test_cli_writes_evidence_and_detects_worker_identity_conflict(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
prod_slots = 9
beta_slots = 1
""",
    )
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "shared-worker-id",
                        "host": "gb10-1",
                        "environment": "production",
                        "api_url": "https://yylx.world/prod/api",
                        "image_tag": "prod-v1.0.0",
                        "source_git_commit": "1111111111111111111111111111111111111111",
                        "max_concurrent": 9,
                    },
                    {
                        "worker_id": "shared-worker-id",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    evidence = tmp_path / "release-evidence" / "worker-capacity.json"

    completed = _run_capacity(
        "--manifest",
        manifest,
        "--observed-json",
        observed,
        "--evidence-out",
        evidence,
        "--format",
        "markdown",
    )

    assert completed.returncode == 1
    assert evidence.is_file()
    report = json.loads(evidence.read_text(encoding="utf-8"))
    assert report["status"] == "fail"
    assert any(item["path"] == "workers[1].worker_id" for item in report["drift"])
    assert "shared-worker-id" in completed.stdout


def test_cli_markdown_failure_path_redacts_secret_bearing_manifest_keys(tmp_path: Path) -> None:
    manifest = tmp_path / "bad.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
api_token = "sk-do-not-print-this-value"
""",
    )

    completed = _run_capacity("--manifest", manifest, "--format", "markdown")

    assert completed.returncode == 1
    assert "# Worker Capacity Desired State" in completed.stdout
    assert "api_token is not allowed" in completed.stdout
    assert "sk-do-not-print-this-value" not in completed.stdout
