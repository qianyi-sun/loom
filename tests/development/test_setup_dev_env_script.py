from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SETUP_SCRIPT = ROOT_DIR / "scripts" / "setup-dev-env.sh"


class SetupDevEnvScriptTests(unittest.TestCase):
    def test_created_env_local_gets_host_visible_sandbox_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            _write_fixture_repo(repo)

            subprocess.run([str(repo / "scripts" / "setup-dev-env.sh")], check=True, cwd=repo)

            env_local = (repo / ".env.local").read_text(encoding="utf-8")
            self.assertIn(f"SANDBOX_HOST_WORKSPACE_ROOT={repo}/.runtime/sandbox-workspaces", env_local)

    def test_existing_blank_sandbox_root_is_filled_without_overwriting_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            _write_fixture_repo(repo)
            (repo / ".env.local").write_text(
                "\n".join(
                    [
                        "MODEL_PROVIDER_API_KEY=secret-test-key",
                        "SANDBOX_HOST_WORKSPACE_ROOT=",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            subprocess.run([str(repo / "scripts" / "setup-dev-env.sh")], check=True, cwd=repo)

            env_local = (repo / ".env.local").read_text(encoding="utf-8")
            self.assertIn("MODEL_PROVIDER_API_KEY=secret-test-key", env_local)
            self.assertIn(f"SANDBOX_HOST_WORKSPACE_ROOT={repo}/.runtime/sandbox-workspaces", env_local)

    def test_existing_nonblank_sandbox_root_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            _write_fixture_repo(repo)
            (repo / ".env.local").write_text(
                "SANDBOX_HOST_WORKSPACE_ROOT=/already/configured\n",
                encoding="utf-8",
            )

            subprocess.run([str(repo / "scripts" / "setup-dev-env.sh")], check=True, cwd=repo)

            env_local = (repo / ".env.local").read_text(encoding="utf-8")
            self.assertEqual(env_local, "SANDBOX_HOST_WORKSPACE_ROOT=/already/configured\n")


def _write_fixture_repo(repo: Path) -> None:
    scripts_dir = repo / "scripts"
    scripts_dir.mkdir()
    setup_script = scripts_dir / "setup-dev-env.sh"
    setup_script.write_text(SETUP_SCRIPT.read_text(encoding="utf-8"), encoding="utf-8")
    setup_script.chmod(0o755)
    (repo / ".env.example").write_text(
        "\n".join(
            [
                "APP_ENV=dev",
                "SANDBOX_HOST_WORKSPACE_ROOT=",
                "MODEL_PROVIDER_API_KEY=",
                "",
            ]
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
