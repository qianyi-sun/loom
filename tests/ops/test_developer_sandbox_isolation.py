from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_SANDBOXES = ("qianyi", "hongjian", "devansh")


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


def _run_validator_for(
    profiles_dir: Path,
    *extra: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "scripts/validate_developer_sandbox_isolation.py",
            "--profiles-dir",
            str(profiles_dir),
            *extra,
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _copy_seed_profiles(profiles_dir: Path) -> None:
    profiles_dir.mkdir()
    source_dir = REPO_ROOT / "deploy" / "developer-sandboxes"
    for owner in SEED_SANDBOXES:
        shutil.copy2(source_dir / f"{owner}.toml", profiles_dir)


def _add_profile(
    profiles_dir: Path,
    *,
    owner: str,
    source_owner: str = "qianyi",
    port_offset: int = 3_000,
) -> Path:
    source = (profiles_dir / f"{source_owner}.toml").read_text(encoding="utf-8")
    source = source.replace(source_owner, owner)
    for source_port in (
        20_532,
        20_900,
        20_901,
        20_080,
        20_090,
        20_100,
        20_800,
        20_443,
        20_991,
        20_173,
    ):
        source = source.replace(
            f"= {source_port}",
            f"= {source_port + port_offset}",
        )
    destination = profiles_dir / f"{owner}.toml"
    destination.write_text(source, encoding="utf-8")
    return destination


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


def test_validator_discovers_fourth_and_nth_profiles_in_stable_order(
    tmp_path: Path,
) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)
    _add_profile(profiles_dir, owner="zoe", port_offset=3_000)
    _add_profile(profiles_dir, owner="alice-7", port_offset=4_000)
    (profiles_dir / "platform.toml").write_text(
        'schema_version = 1\n[routing]\nsandbox = "staging"\n',
        encoding="utf-8",
    )

    completed = _run_validator_for(profiles_dir, "--json")

    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert [row["sandbox"] for row in report["profiles"]] == [
        "alice-7",
        "devansh",
        "hongjian",
        "qianyi",
        "zoe",
    ]


def test_validator_rejects_invalid_sandbox_id(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)
    invalid = _add_profile(profiles_dir, owner="alice_7", port_offset=3_000)

    completed = _run_validator_for(profiles_dir)

    assert completed.returncode == 1
    assert str(invalid) in completed.stderr
    assert "sandbox must match" in completed.stderr


def test_validator_rejects_filename_identity_mismatch(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)
    (profiles_dir / "qianyi.toml").rename(profiles_dir / "alice.toml")

    completed = _run_validator_for(profiles_dir)

    assert completed.returncode == 1
    assert "filename must equal sandbox identity" in completed.stderr


def test_validator_requires_at_least_one_marked_profile(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "platform.toml").write_text(
        'schema_version = 1\n[routing]\nsandbox = "staging"\n',
        encoding="utf-8",
    )

    completed = _run_validator_for(profiles_dir)

    assert completed.returncode == 1
    assert "no developer sandbox profiles" in completed.stderr


def test_validator_ignores_unmarked_malformed_toml(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)
    (profiles_dir / "platform.toml").write_text(
        'sandbox = "platform"\nthis is not valid TOML\n',
        encoding="utf-8",
    )

    completed = _run_validator_for(profiles_dir, "--json")

    assert completed.returncode == 0, completed.stderr
    assert {row["sandbox"] for row in json.loads(completed.stdout)["profiles"]} == set(
        SEED_SANDBOXES
    )


def test_validator_rejects_marked_malformed_toml(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)
    (profiles_dir / "broken.toml").write_text(
        'kind = "loom.developer-sandbox.profile"\nthis is not valid TOML\n',
        encoding="utf-8",
    )

    completed = _run_validator_for(profiles_dir)

    assert completed.returncode == 1
    assert "invalid marked sandbox profile TOML" in completed.stderr


def test_validator_rejects_symlink_in_directory_ancestry(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    profiles_dir = real_root / "profiles"
    real_root.mkdir()
    _copy_seed_profiles(profiles_dir)
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(real_root, target_is_directory=True)

    completed = _run_validator_for(linked_root / "profiles")

    assert completed.returncode == 1
    assert "must not contain symlinks" in completed.stderr


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
    _copy_seed_profiles(profiles_dir)

    hongjian = profiles_dir / "hongjian.toml"
    text = hongjian.read_text(encoding="utf-8")
    text = text.replace("postgres = 21532", "postgres = 20532")
    hongjian.write_text(text, encoding="utf-8")

    completed = _run_validator_for(profiles_dir)
    assert completed.returncode == 1
    assert "host ports must be pairwise distinct" in completed.stderr


def test_developer_sandbox_isolation_rejects_shared_bucket(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    _copy_seed_profiles(profiles_dir)

    hongjian = profiles_dir / "hongjian.toml"
    text = hongjian.read_text(encoding="utf-8")
    text = text.replace(
        'task_bucket = "loom-sandbox-hongjian-tasks"',
        'task_bucket = "loom-sandbox-qianyi-tasks"',
    )
    hongjian.write_text(text, encoding="utf-8")

    completed = _run_validator_for(profiles_dir)
    assert completed.returncode == 1
    assert "task_bucket must be pairwise distinct" in completed.stderr
