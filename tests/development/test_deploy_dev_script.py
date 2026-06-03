from __future__ import annotations

import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DEPLOY_SCRIPT = ROOT_DIR / "scripts" / "deploy-dev.sh"


def _script_text() -> str:
    return DEPLOY_SCRIPT.read_text(encoding="utf-8")


class DeployDevScriptTests(unittest.TestCase):
    def test_shared_dev_deploy_stops_scheduler_before_migrations(self) -> None:
        script = _script_text()

        self.assertIn("compose stop api scheduler worker", script)
        self.assertIn(
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" '
            "stop api scheduler worker",
            script,
        )

    def test_shared_dev_deploy_runs_scheduler_as_long_lived_service(self) -> None:
        script = _script_text()

        self.assertIn("compose up -d --build api scheduler worker", script)
        self.assertIn(
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" '
            "up -d --build api scheduler worker",
            script,
        )

    def test_shared_dev_deploy_recovers_stale_active_runs_before_api_smoke(self) -> None:
        script = _script_text()

        self.assertIn('DEPLOY_STALE_ACTIVE_RECOVERY_SECONDS="${DEPLOY_STALE_ACTIVE_RECOVERY_SECONDS:-60}"', script)
        self.assertIn("Recovering stale active runs before API/frontend smokes", script)
        self.assertIn(
            'SCHEDULER_STALE_ACTIVE_HEARTBEAT_TIMEOUT_SECONDS="$DEPLOY_STALE_ACTIVE_RECOVERY_SECONDS" '
            "compose run --rm -T scheduler "
            "python -m agentic_data_platform.scheduler.service --recover-once --scheduler-id deploy-dev-recovery",
            script,
        )
        self.assertIn(
            'SCHEDULER_STALE_ACTIVE_HEARTBEAT_TIMEOUT_SECONDS="$DEPLOY_STALE_ACTIVE_RECOVERY_SECONDS" '
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" run --rm -T scheduler '
            "python -m agentic_data_platform.scheduler.service --recover-once --scheduler-id deploy-dev-recovery",
            script,
        )
        self.assertLess(
            script.index("Recovering stale active runs before API/frontend smokes"),
            script.index("Checking authenticated API-created Docker sandbox run"),
        )

    def test_shared_dev_deploy_verifies_scheduler_docker_cleanup_before_api_smoke(self) -> None:
        script = _script_text()

        self.assertIn(
            'DEPLOY_RUN_SCHEDULER_DOCKER_CLEANUP_SMOKE="${DEPLOY_RUN_SCHEDULER_DOCKER_CLEANUP_SMOKE:-1}"',
            script,
        )
        self.assertIn("Checking scheduler Docker cleanup recovery smoke", script)
        self.assertIn(
            "SCHEDULER_DOCKER_CLEANUP_ENABLED=true "
            "compose run --rm -T scheduler "
            "python -m agentic_data_platform.scheduler.cleanup_smoke --scheduler-id deploy-dev-cleanup-smoke",
            script,
        )
        self.assertIn(
            "SCHEDULER_DOCKER_CLEANUP_ENABLED=true "
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" run --rm -T scheduler '
            "python -m agentic_data_platform.scheduler.cleanup_smoke --scheduler-id deploy-dev-cleanup-smoke",
            script,
        )
        self.assertLess(
            script.index("Checking scheduler Docker cleanup recovery smoke"),
            script.index("Checking authenticated API-created Docker sandbox run"),
        )

    def test_shared_dev_deploy_verifies_parent_death_docker_cleanup_before_api_smoke(self) -> None:
        script = _script_text()

        self.assertIn(
            'DEPLOY_RUN_SCHEDULER_PARENT_DEATH_CLEANUP_SMOKE="${DEPLOY_RUN_SCHEDULER_PARENT_DEATH_CLEANUP_SMOKE:-1}"',
            script,
        )
        self.assertIn("Checking scheduler parent-death Docker cleanup smoke", script)
        self.assertIn(
            "SCHEDULER_DOCKER_CLEANUP_ENABLED=true "
            "compose run --rm -T scheduler "
            "python -m agentic_data_platform.scheduler.cleanup_smoke "
            "--mode parent-death --scheduler-id deploy-dev-parent-death-cleanup-smoke",
            script,
        )
        self.assertIn(
            "SCHEDULER_DOCKER_CLEANUP_ENABLED=true "
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" run --rm -T scheduler '
            "python -m agentic_data_platform.scheduler.cleanup_smoke "
            "--mode parent-death --scheduler-id deploy-dev-parent-death-cleanup-smoke",
            script,
        )
        self.assertLess(
            script.index("Checking scheduler parent-death Docker cleanup smoke"),
            script.index("Checking authenticated API-created Docker sandbox run"),
        )

    def test_shared_dev_deploy_verifies_scheduler_race_smoke_before_api_smoke(self) -> None:
        script = _script_text()

        self.assertIn(
            'DEPLOY_RUN_SCHEDULER_RACE_SMOKE="${DEPLOY_RUN_SCHEDULER_RACE_SMOKE:-1}"',
            script,
        )
        self.assertIn("Checking scheduler multi-instance race smoke", script)
        self.assertIn(
            "compose run --rm -T scheduler "
            "python -m agentic_data_platform.scheduler.race_smoke --scheduler-id-prefix deploy-dev-race-smoke",
            script,
        )
        self.assertIn(
            'docker compose -p "$DEPLOY_PROJECT_NAME" -f "$COMPOSE_FILE" run --rm -T scheduler '
            "python -m agentic_data_platform.scheduler.race_smoke --scheduler-id-prefix deploy-dev-race-smoke",
            script,
        )
        self.assertLess(
            script.index("Checking scheduler multi-instance race smoke"),
            script.index("Checking authenticated API-created Docker sandbox run"),
        )

    def test_shared_dev_deploy_audits_real_smoke_run_container_leaks(self) -> None:
        script = _script_text()

        self.assertIn(
            'DEPLOY_RUN_CONTAINER_LEAK_AUDIT="${DEPLOY_RUN_CONTAINER_LEAK_AUDIT:-1}"',
            script,
        )
        self.assertIn(
            'DEPLOY_CONTAINER_LEAK_AUDIT_ATTEMPTS="${DEPLOY_CONTAINER_LEAK_AUDIT_ATTEMPTS:-3}"',
            script,
        )
        self.assertIn(
            'DEPLOY_CONTAINER_LEAK_AUDIT_POLL_SECONDS="${DEPLOY_CONTAINER_LEAK_AUDIT_POLL_SECONDS:-5}"',
            script,
        )
        self.assertIn("run_container_leak_audit", script)
        self.assertIn("agentic_data_platform.scheduler.container_leak_audit", script)
        self.assertIn("--run-id \"$worker_smoke_run_id\"", script)
        self.assertIn('--max-attempts "$DEPLOY_CONTAINER_LEAK_AUDIT_ATTEMPTS"', script)
        self.assertIn('--poll-interval-seconds "$DEPLOY_CONTAINER_LEAK_AUDIT_POLL_SECONDS"', script)
        self.assertIn("-e API_SMOKE_RUN_ID=\"$api_smoke_run_id\"", script)
        self.assertIn("-e FRONTEND_SMOKE_RUN_ID=\"$frontend_smoke_run_id\"", script)
        self.assertLess(
            script.index("Checking authenticated API-created Docker sandbox run"),
            script.index("run_container_leak_audit \"$api_smoke_run_id\""),
        )
        self.assertLess(
            script.index("Checking frontend login, launch, telemetry, and artifact download smoke"),
            script.index("run_container_leak_audit \"$frontend_smoke_run_id\""),
        )

    def test_shared_dev_deploy_runs_aggregate_container_leak_audit_after_real_smokes(self) -> None:
        script = _script_text()

        self.assertIn(
            'DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_ATTEMPTS="${DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_ATTEMPTS:-6}"',
            script,
        )
        self.assertIn(
            'DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_POLL_SECONDS="${DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_POLL_SECONDS:-10}"',
            script,
        )
        self.assertIn("SMOKE_RUN_IDS=()", script)
        self.assertIn("record_smoke_run_id \"$worker_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"$harbor_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"$api_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"$frontend_smoke_run_id\"", script)
        self.assertIn("Checking aggregate Docker container leak audit", script)
        self.assertIn("run_aggregate_container_leak_audit", script)
        self.assertIn('"${leak_audit_args[@]}"', script)
        self.assertIn(
            '--max-attempts "$DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_ATTEMPTS"',
            script,
        )
        self.assertIn(
            '--poll-interval-seconds "$DEPLOY_CONTAINER_LEAK_AUDIT_FINAL_POLL_SECONDS"',
            script,
        )
        aggregate_call_index = script.rindex("run_aggregate_container_leak_audit")
        self.assertLess(
            script.index("run_container_leak_audit \"$frontend_smoke_run_id\""),
            aggregate_call_index,
        )
        self.assertIn("smoke_run_ids=()", script)
        self.assertIn("record_smoke_run_id \"\\$worker_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"\\$harbor_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"\\$api_smoke_run_id\"", script)
        self.assertIn("record_smoke_run_id \"\\$frontend_smoke_run_id\"", script)


if __name__ == "__main__":
    unittest.main()
