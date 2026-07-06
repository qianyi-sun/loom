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


def test_lease_beta_previews_before_apply_and_writes_bounded_lease(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
""",
    )
    before = manifest.read_text(encoding="utf-8")

    preview = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "public beta rollout smoke",
        "--ttl",
        "30m",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "2",
        "--preemptible",
        "--now",
        "2026-07-06T12:00:00Z",
    )

    assert preview.returncode == 0, preview.stderr
    assert manifest.read_text(encoding="utf-8") == before
    preview_report = json.loads(preview.stdout)
    assert preview_report["applied"] is False
    assert preview_report["lease"]["state"] == "active"
    assert preview_report["lease"]["expires_at"] == "2026-07-06T12:30:00Z"
    assert preview_report["summary"]["beta_slots"] == 2
    assert preview_report["summary"]["prod_slots"] == 18
    assert preview_report["new_beta_claims_allowed"] is True

    leased_manifest = tmp_path / "leased-capacity.toml"
    applied = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "public beta rollout smoke",
        "--ttl",
        "30m",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "2",
        "--preemptible",
        "--now",
        "2026-07-06T12:00:00Z",
        "--apply",
        "--output-manifest",
        leased_manifest,
    )

    assert applied.returncode == 0, applied.stderr
    assert leased_manifest.is_file()
    status = _run_capacity(
        "status",
        "--manifest",
        leased_manifest,
        "--now",
        "2026-07-06T12:05:00Z",
    )
    assert status.returncode == 0, status.stderr
    status_report = json.loads(status.stdout)
    assert status_report["lease"]["state"] == "active"
    assert status_report["summary"]["beta_slots"] == 2


def test_lease_beta_rejects_unbounded_ttl_multi_slot_and_non_preemptible(
    tmp_path: Path,
) -> None:
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

    missing_ttl = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "smoke",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "1",
        "--preemptible",
    )
    assert missing_ttl.returncode == 2
    assert "ttl" in missing_ttl.stderr.lower()

    multi_slot = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "smoke",
        "--ttl",
        "30m",
        "--slots-per-host",
        "2",
        "--max-total-slots",
        "2",
        "--preemptible",
    )
    assert multi_slot.returncode == 2
    assert "slots-per-host" in multi_slot.stderr
    assert "1" in multi_slot.stderr

    non_preemptible = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "smoke",
        "--ttl",
        "30m",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "1",
        "--non-preemptible",
    )
    assert non_preemptible.returncode == 2
    assert "non-preemptible" in non_preemptible.stderr
    assert "--allow-non-preemptible" in non_preemptible.stderr


def test_status_expires_beta_lease_and_reports_running_vs_idle_drain_slots(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    lease = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "short smoke",
        "--ttl",
        "1s",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "2",
        "--preemptible",
        "--now",
        "2026-07-06T12:00:00Z",
        "--apply",
        "--output-manifest",
        leased_manifest,
    )
    assert lease.returncode == 0, lease.stderr
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "beta-worker-1",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                        "running_trials": 1,
                    },
                    {
                        "worker_id": "beta-worker-2",
                        "host": "gb10-2",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                        "running_trials": 0,
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    expired = _run_capacity(
        "status",
        "--manifest",
        leased_manifest,
        "--observed-json",
        observed,
        "--now",
        "2026-07-06T12:00:02Z",
    )

    assert expired.returncode == 0, expired.stderr
    report = json.loads(expired.stdout)
    assert report["lease"]["state"] == "expired"
    assert report["new_beta_claims_allowed"] is False
    assert report["drain"]["running_beta_trials"] == 1
    assert report["drain"]["idle_leased_slots"] == 1
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["state"] == "beta_draining"
    assert hosts["gb10-1"]["beta_slots"] == 1
    assert hosts["gb10-2"]["state"] == "eligible"
    assert hosts["gb10-2"]["beta_slots"] == 0

    expired_manifest = tmp_path / "expired.toml"
    applied_expiry = _run_capacity(
        "status",
        "--manifest",
        leased_manifest,
        "--observed-json",
        observed,
        "--now",
        "2026-07-06T12:00:02Z",
        "--apply",
        "--output-manifest",
        expired_manifest,
    )
    assert applied_expiry.returncode == 0, applied_expiry.stderr
    assert expired_manifest.is_file()
    applied_report = json.loads(applied_expiry.stdout)
    assert applied_report["applied"] is True
    assert applied_report["lease"]["state"] == "expired"


def test_release_beta_is_idempotent_and_returns_beta_slots_to_zero(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    release_one_manifest = tmp_path / "released-once.toml"
    release_two_manifest = tmp_path / "released-twice.toml"
    lease = _run_capacity(
        "lease-beta",
        "--manifest",
        manifest,
        "--reason",
        "validation",
        "--ttl",
        "30m",
        "--slots-per-host",
        "1",
        "--max-total-slots",
        "2",
        "--preemptible",
        "--now",
        "2026-07-06T12:00:00Z",
        "--apply",
        "--output-manifest",
        leased_manifest,
    )
    assert lease.returncode == 0, lease.stderr

    release_one = _run_capacity(
        "release-beta",
        "--manifest",
        leased_manifest,
        "--reason",
        "validation complete",
        "--now",
        "2026-07-06T12:10:00Z",
        "--apply",
        "--output-manifest",
        release_one_manifest,
    )
    assert release_one.returncode == 0, release_one.stderr
    first_report = json.loads(release_one.stdout)
    assert first_report["summary"]["beta_slots"] == 0
    assert first_report["summary"]["prod_slots"] == 20
    assert first_report["lease"]["state"] == "released"
    assert first_report["new_beta_claims_allowed"] is False

    release_two = _run_capacity(
        "release-beta",
        "--manifest",
        release_one_manifest,
        "--reason",
        "validation complete",
        "--now",
        "2026-07-06T12:11:00Z",
        "--apply",
        "--output-manifest",
        release_two_manifest,
    )
    assert release_two.returncode == 0, release_two.stderr
    second_report = json.loads(release_two.stdout)
    assert second_report["summary"]["beta_slots"] == 0
    assert second_report["changes"]["changed_host_count"] == 0
    assert release_two_manifest.read_text(encoding="utf-8") == release_one_manifest.read_text(
        encoding="utf-8",
    )


def test_drain_beta_redacts_command_and_evidence_output(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10-arm64"
total_slots = 10
state = "eligible"
beta_slots = 1
""",
    )
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "beta-worker-1",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api?token=loom_api_livevalue",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                        "running_trials": 1,
                    },
                ],
            },
        ),
        encoding="utf-8",
    )
    evidence = tmp_path / "evidence" / "drain.json"
    secret_reason = "incident Bearer sk-live-secret-value"

    drain = _run_capacity(
        "drain-beta",
        "--manifest",
        manifest,
        "--observed-json",
        observed,
        "--reason",
        secret_reason,
        "--now",
        "2026-07-06T12:00:00Z",
        "--evidence-out",
        evidence,
    )

    assert drain.returncode == 0, drain.stderr
    evidence_text = evidence.read_text(encoding="utf-8")
    combined = drain.stdout + drain.stderr + evidence_text
    assert "sk-live-secret-value" not in combined
    assert "loom_api_livevalue" not in combined
    assert "<redacted>" in combined
    report = json.loads(drain.stdout)
    assert report["applied"] is False
    assert report["drain"]["running_beta_trials"] == 1
    assert report["command"]["argv"][0] == "drain-beta"
