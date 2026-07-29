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
    "STAGING_IMAGE_TAG=dev-2222222",
    "--var",
    "STAGING_SOURCE_COMMIT=2222222222222222222222222222222222222222",
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
default_staging_slots = 0
staging_slot_limit_per_host = 1

[environments.prod]
name = "production"
api_url = "https://yylx.world/prod/api"
image_tag = "prod-v1.0.0"
source_commit = "1111111111111111111111111111111111111111"
compose_service = "loom-prod-worker"
k8s_deployment = "loom-prod-worker"
k8s_namespace = "loom-prod"

[environments.staging]
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
    assert report["summary"]["total_slots"] == 190
    assert report["summary"]["prod_slots"] == 190
    assert report["summary"]["staging_slots"] == 0
    assert report["summary"]["state_counts"].get("unreachable", 0) == 0

    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["trt-gb10-1"]["prod_slots"] == 10
    assert hosts["trt-gb10-1"]["staging_slots"] == 0
    assert hosts["trt-gb10-14"]["state"] == "eligible"
    assert hosts["trt-gb10-14"]["prod_slots"] == 10
    assert hosts["trt-gb10-14"]["staging_slots"] == 0
    assert hosts["trt-gb10-7"]["state"] == "eligible"
    assert hosts["trt-gb10-7"]["prod_slots"] == 10
    assert hosts["trt-gb10-7"]["staging_slots"] == 0
    assert "Bearer" not in completed.stdout
    assert "sk-" not in completed.stdout
    assert "loom_api_" not in completed.stdout


def test_manifest_rejects_cross_environment_target_identity(tmp_path: Path) -> None:
    manifest = tmp_path / "cross-environment.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'name = "development"',
            'name = "production"',
            1,
        ),
        encoding="utf-8",
    )

    completed = _run_capacity("--manifest", manifest)

    assert completed.returncode == 1
    assert "environments.prod.name and staging.name must differ" in completed.stdout


def test_manifest_expresses_staging_lease_and_draining_states(tmp_path: Path) -> None:
    manifest = tmp_path / "lease.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"
staging_slots = 1

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "staging_draining"
staging_slots = 1

[[hosts]]
name = "gb10-3"
pool = "gb10"
total_slots = 10
state = "host_draining"

[[hosts]]
name = "gb10-4"
pool = "gb10"
total_slots = 10
state = "unreachable"
""",
    )

    completed = _run_capacity("--manifest", manifest)

    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["prod_slots"] == 9
    assert hosts["gb10-1"]["staging_slots"] == 1
    assert hosts["gb10-1"]["staging"]["drain_state"] == "leased"
    assert hosts["gb10-2"]["staging"]["drain_state"] == "draining"
    assert hosts["gb10-3"]["prod_slots"] == 0
    assert hosts["gb10-4"]["state"] == "unreachable"


def test_observed_cross_environment_drift_redacts_secrets(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
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
pool = "gb10"
total_slots = 10
state = "eligible"
prod_slots = 9
staging_slots = 1
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
pool = "gb10"
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


def test_lease_staging_previews_before_apply_and_writes_bounded_lease(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    before = manifest.read_text(encoding="utf-8")

    preview = _run_capacity(
        "lease-staging",
        "--manifest",
        manifest,
        "--reason",
        "staging rollout smoke",
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
    assert preview_report["summary"]["staging_slots"] == 2
    assert preview_report["summary"]["prod_slots"] == 18
    assert preview_report["new_staging_claims_allowed"] is True

    leased_manifest = tmp_path / "leased-capacity.toml"
    applied = _run_capacity(
        "lease-staging",
        "--manifest",
        manifest,
        "--reason",
        "staging rollout smoke",
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
    assert status_report["summary"]["staging_slots"] == 2


def test_lease_staging_rejects_unbounded_ttl_multi_slot_and_non_preemptible(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )

    missing_ttl = _run_capacity(
        "lease-staging",
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
        "lease-staging",
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
        "lease-staging",
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


def test_status_expires_staging_lease_and_reports_running_vs_idle_drain_slots(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    lease = _run_capacity(
        "lease-staging",
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
                        "worker_id": "staging-worker-1",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                        "running_trials": 1,
                    },
                    {
                        "worker_id": "staging-worker-2",
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
    assert report["new_staging_claims_allowed"] is False
    assert report["drain"]["running_staging_trials"] == 1
    assert report["drain"]["idle_leased_slots"] == 1
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["state"] == "staging_draining"
    assert hosts["gb10-1"]["staging_slots"] == 1
    assert hosts["gb10-2"]["state"] == "eligible"
    assert hosts["gb10-2"]["staging_slots"] == 0

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


def test_release_staging_is_idempotent_and_returns_staging_slots_to_zero(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    release_one_manifest = tmp_path / "released-once.toml"
    release_two_manifest = tmp_path / "released-twice.toml"
    lease = _run_capacity(
        "lease-staging",
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
        "release-staging",
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
    assert first_report["summary"]["staging_slots"] == 0
    assert first_report["summary"]["prod_slots"] == 20
    assert first_report["lease"]["state"] == "released"
    assert first_report["new_staging_claims_allowed"] is False

    release_two = _run_capacity(
        "release-staging",
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
    assert second_report["summary"]["staging_slots"] == 0
    assert second_report["changes"]["changed_host_count"] == 0
    assert release_two_manifest.read_text(encoding="utf-8") == release_one_manifest.read_text(
        encoding="utf-8",
    )


def test_drain_staging_redacts_command_and_evidence_output(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"
staging_slots = 1
""",
    )
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "staging-worker-1",
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
        "drain-staging",
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
    assert report["drain"]["running_staging_trials"] == 1
    assert report["command"]["argv"][0] == "drain-staging"


def test_status_keeps_active_staging_lease_when_prod_pressure_is_absent(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    lease = _run_capacity(
        "lease-staging",
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

    status = _run_capacity(
        "status",
        "--manifest",
        leased_manifest,
        "--prod-pending-count",
        "0",
        "--prod-active-count",
        "0",
        "--now",
        "2026-07-06T12:05:00Z",
    )

    assert status.returncode == 0, status.stderr
    report = json.loads(status.stdout)
    assert report["lease"]["state"] == "active"
    assert report["summary"]["staging_slots"] == 2
    assert report["new_staging_claims_allowed"] is True
    assert report["prod_pressure"]["has_pressure"] is False
    assert report["prod_pressure"]["cause"] == "none"


def test_status_auto_drains_staging_capacity_under_prod_pressure(tmp_path: Path) -> None:
    manifest = tmp_path / "capacity.toml"
    _write_manifest(
        manifest,
        """
[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "eligible"

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
""",
    )
    leased_manifest = tmp_path / "leased.toml"
    lease = _run_capacity(
        "lease-staging",
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
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "staging-worker-1",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
                        "image_tag": "dev-2222222",
                        "source_git_commit": "2222222222222222222222222222222222222222",
                        "max_concurrent": 1,
                        "running_trials": 1,
                    },
                    {
                        "worker_id": "staging-worker-2",
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

    pressure = _run_capacity(
        "status",
        "--manifest",
        leased_manifest,
        "--observed-json",
        observed,
        "--prod-pending-count",
        "3",
        "--prod-active-count",
        "1",
        "--prod-pressure-source",
        "prod queue https://user:secret@example.invalid",
        "--now",
        "2026-07-06T12:05:00Z",
    )

    assert pressure.returncode == 0, pressure.stderr
    assert "secret@example" not in pressure.stdout
    report = json.loads(pressure.stdout)
    assert report["lease"]["state"] == "prod_pressure_draining"
    assert report["new_staging_claims_allowed"] is False
    assert report["prod_pressure"]["has_pressure"] is True
    assert report["prod_pressure"]["cause"] == "prod_capacity_pressure"
    assert report["prod_pressure"]["prod_pending_count"] == 3
    assert report["prod_pressure"]["prod_active_count"] == 1
    assert "<redacted>" in report["prod_pressure"]["source"]
    assert report["drain"]["running_staging_trials"] == 1
    assert report["drain"]["idle_leased_slots"] == 1
    assert report["drain"]["released_idle_hosts"] == ["gb10-2"]
    assert report["drain"]["preemptible"]["action"] == "wait"
    assert report["drain"]["preemptible"]["eligible_running_staging_trials"] == 0
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["state"] == "staging_draining"
    assert hosts["gb10-1"]["staging_slots"] == 1
    assert hosts["gb10-2"]["state"] == "eligible"
    assert hosts["gb10-2"]["staging_slots"] == 0


def test_prod_pressure_grace_period_marks_preemptible_staging_retryable_after_grace(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "pressure-draining.toml"
    _write_manifest(
        manifest,
        """
[staging_capacity_lease]
state = "prod_pressure_draining"
reason = "validation"
created_at = "2026-07-06T11:55:00Z"
expires_at = "2026-07-06T12:55:00Z"
ttl_seconds = 3600
slots_per_host = 1
max_total_slots = 1
preemptible = true
leased_hosts = ["gb10-1"]
stopped_new_claims = true
prod_pressure_started_at = "2026-07-06T12:00:00Z"
prod_pressure_reason = "prod capacity pressure"

[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "staging_draining"
staging_slots = 1
""",
    )
    observed = tmp_path / "observed.json"
    observed.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "staging-worker-1",
                        "host": "gb10-1",
                        "environment": "development",
                        "api_url": "https://yylx.world/dev/api",
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

    pressure = _run_capacity(
        "status",
        "--manifest",
        manifest,
        "--observed-json",
        observed,
        "--prod-pending-count",
        "1",
        "--preemptible-grace-period",
        "10m",
        "--now",
        "2026-07-06T12:11:00Z",
    )

    assert pressure.returncode == 0, pressure.stderr
    report = json.loads(pressure.stdout)
    preemptible = report["drain"]["preemptible"]
    assert preemptible["enabled"] is True
    assert preemptible["grace_period_seconds"] == 600
    assert preemptible["cancel_after"] == "2026-07-06T12:10:00Z"
    assert preemptible["eligible_running_staging_trials"] == 1
    assert preemptible["action"] == "cancel_retryable"
    assert preemptible["reason"] == "prod_capacity_pressure_grace_period_elapsed"


def test_prod_pressure_drained_lease_recovers_after_pressure_clears(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "pressure-draining.toml"
    _write_manifest(
        manifest,
        """
[staging_capacity_lease]
state = "prod_pressure_draining"
reason = "validation"
created_at = "2026-07-06T12:00:00Z"
expires_at = "2026-07-06T12:30:00Z"
ttl_seconds = 1800
slots_per_host = 1
max_total_slots = 2
preemptible = true
leased_hosts = ["gb10-1", "gb10-2"]
stopped_new_claims = true
prod_pressure_started_at = "2026-07-06T12:05:00Z"
prod_pressure_reason = "prod capacity pressure"

[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "staging_draining"
staging_slots = 1

[[hosts]]
name = "gb10-2"
pool = "gb10"
total_slots = 10
state = "eligible"
staging_slots = 0
""",
    )

    recovered = _run_capacity(
        "status",
        "--manifest",
        manifest,
        "--prod-pending-count",
        "0",
        "--prod-active-count",
        "0",
        "--now",
        "2026-07-06T12:10:00Z",
    )

    assert recovered.returncode == 0, recovered.stderr
    report = json.loads(recovered.stdout)
    assert report["lease"]["state"] == "active"
    assert report["lease"]["recovery_reason"] == "prod pressure cleared"
    assert report["prod_pressure"]["has_pressure"] is False
    assert report["prod_pressure"]["recovered"] is True
    assert report["summary"]["staging_slots"] == 2
    assert report["new_staging_claims_allowed"] is True
    hosts = {item["host"]: item for item in report["desired_hosts"]}
    assert hosts["gb10-1"]["state"] == "eligible"
    assert hosts["gb10-1"]["staging_slots"] == 1
    assert hosts["gb10-2"]["state"] == "eligible"
    assert hosts["gb10-2"]["staging_slots"] == 1


def test_prod_pressure_drained_lease_expires_instead_of_recovering_after_ttl(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "pressure-draining.toml"
    _write_manifest(
        manifest,
        """
[staging_capacity_lease]
state = "prod_pressure_draining"
reason = "validation"
created_at = "2026-07-06T12:00:00Z"
expires_at = "2026-07-06T12:30:00Z"
ttl_seconds = 1800
slots_per_host = 1
max_total_slots = 1
preemptible = true
leased_hosts = ["gb10-1"]
stopped_new_claims = true
prod_pressure_started_at = "2026-07-06T12:05:00Z"
prod_pressure_reason = "prod capacity pressure"

[[hosts]]
name = "gb10-1"
pool = "gb10"
total_slots = 10
state = "staging_draining"
staging_slots = 1
""",
    )

    expired = _run_capacity(
        "status",
        "--manifest",
        manifest,
        "--prod-pending-count",
        "0",
        "--now",
        "2026-07-06T12:31:00Z",
    )

    assert expired.returncode == 0, expired.stderr
    report = json.loads(expired.stdout)
    assert report["lease"]["state"] == "expired"
    assert report["prod_pressure"]["recovered"] is False
    assert report["summary"]["staging_slots"] == 0
    assert report["new_staging_claims_allowed"] is False
