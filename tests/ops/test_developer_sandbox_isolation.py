from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_validator(*extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "scripts/validate_developer_sandbox_isolation.py",
            "--profiles-dir",
            "deploy/developer-sandboxes",
            *extra,
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_developer_sandbox_profiles_are_pairwise_isolated() -> None:
    completed = _run_validator("--json")
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report["status"] == "pass"
    profiles = {row["sandbox"]: row for row in report["profiles"]}
    assert set(profiles) == {"qianyi", "hongjian", "devansh"}

    assert len({row["compose_project"] for row in profiles.values()}) == 3
    assert len({row["database_name"] for row in profiles.values()}) == 3
    assert len({row["task_bucket"] for row in profiles.values()}) == 3
    assert len({row["trajectories_bucket"] for row in profiles.values()}) == 3
    assert len({row["artifacts_bucket"] for row in profiles.values()}) == 3
    assert len({row["provider_connection_namespace"] for row in profiles.values()}) == 3

    assert profiles["qianyi"]["compose_project"] == "loom-sandbox-qianyi"
    assert profiles["qianyi"]["database_name"] == "loom_sandbox_qianyi"
    assert profiles["qianyi"]["task_bucket"] == "loom-sandbox-qianyi-tasks"
    assert profiles["qianyi"]["provider_connection_namespace"] == "sandbox-qianyi"


def test_developer_sandbox_isolation_dry_run_is_secret_free(tmp_path: Path) -> None:
    artifact = tmp_path / "developer-sandbox-isolation-dry-run.json"
    completed = _run_validator("--write-dry-run", str(artifact), "--json")
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "developer-sandbox-isolation-dry-run"
    assert len(payload["profiles"]) == 3
    rendered = json.dumps(payload)
    for forbidden in (
        "Bearer ",
        "loom_w_",
        "loom_admin_",
        "sk-",
        "AKIA",
        "BEGIN PRIVATE KEY",
        "password=",
        "secret=",
    ):
        assert forbidden.lower() not in rendered.lower()


def test_developer_sandbox_isolation_rejects_port_collision(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    shutil.copytree(REPO_ROOT / "deploy" / "developer-sandboxes", profiles_dir)
    # Keep only the three TOML profiles; ignore any extra adapter assets.
    for path in list(profiles_dir.iterdir()):
        if path.suffix != ".toml" or path.stem not in {"qianyi", "hongjian", "devansh"}:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

    hongjian = profiles_dir / "hongjian.toml"
    text = hongjian.read_text(encoding="utf-8")
    text = text.replace("postgres = 21532", "postgres = 20532")
    hongjian.write_text(text, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/validate_developer_sandbox_isolation.py",
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 1
    assert "host ports must be pairwise distinct" in completed.stderr


def test_developer_sandbox_isolation_rejects_shared_bucket(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    shutil.copytree(REPO_ROOT / "deploy" / "developer-sandboxes", profiles_dir)
    for path in list(profiles_dir.iterdir()):
        if path.suffix != ".toml" or path.stem not in {"qianyi", "hongjian", "devansh"}:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

    hongjian = profiles_dir / "hongjian.toml"
    text = hongjian.read_text(encoding="utf-8")
    text = text.replace(
        'task_bucket = "loom-sandbox-hongjian-tasks"',
        'task_bucket = "loom-sandbox-qianyi-tasks"',
    )
    hongjian.write_text(text, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/validate_developer_sandbox_isolation.py",
            "--profiles-dir",
            str(profiles_dir),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 1
    assert "task_bucket must be pairwise distinct" in completed.stderr
