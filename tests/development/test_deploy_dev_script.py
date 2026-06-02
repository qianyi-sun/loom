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


if __name__ == "__main__":
    unittest.main()
