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


if __name__ == "__main__":
    unittest.main()
