from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli import datasets_cmd

_TASK_TOML = """\
schema_version = "1"

[task]
id = "{tid}"
name = "Sample task {tid}"

[environment]
os = "linux"
docker_image = "python:3.11-alpine"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def _write_task(root: Path, tid: str) -> None:
    bundle = root / tid
    bundle.mkdir(parents=True)
    (bundle / "task.toml").write_text(_TASK_TOML.format(tid=tid))
    (bundle / "instruction.md").write_text(f"do {tid}\n")


def _write_benchmark_toml(root: Path) -> None:
    (root / "benchmark.toml").write_text(
        "schema_version = 1\n"
        "id = \"team-evals\"\n"
        "display_name = \"Team evaluations\"\n"
        "series = \"internal\"\n"
        "license_spdx = \"MIT\"\n",
    )


def test_validate_local_benchmark_toml_tasks_layout_outputs_config_snippet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "team-evals"
    tasks_root = root / "tasks"
    tasks_root.mkdir(parents=True)
    _write_benchmark_toml(root)
    _write_task(tasks_root, "alpha")

    rc = datasets_cmd.dispatch(["validate-local", str(root)])

    assert rc == 0
    out = capsys.readouterr().out
    assert "benchmark_id: team-evals" in out
    assert "tasks:        1 valid" in out
    assert "[[local]]" in out
    assert 'source_subdir = "tasks"' in out


def test_validate_local_direct_layout_requires_metadata_flags(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "team-evals"
    root.mkdir()
    _write_task(root, "alpha")

    rc = datasets_cmd.dispatch(["validate-local", str(root)])

    assert rc == 2
    err = capsys.readouterr().err
    assert "benchmark.toml" in err
    assert "--id" in err


def test_validate_local_direct_layout_accepts_metadata_flags(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "team-evals"
    root.mkdir()
    _write_task(root, "alpha")

    rc = datasets_cmd.dispatch([
        "validate-local",
        str(root),
        "--id", "team-evals",
        "--display-name", "Team evaluations",
        "--series", "internal",
        "--license-spdx", "MIT",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "benchmark_id: team-evals" in out
    assert "source_subdir:" not in out
    assert "source_subdir" not in out.split("config snippet:", 1)[1]


def test_validate_local_invalid_task_toml_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "team-evals"
    tasks_root = root / "tasks"
    broken = tasks_root / "broken"
    broken.mkdir(parents=True)
    _write_benchmark_toml(root)
    (broken / "task.toml").write_text("this is = NOT = valid TOML\n")

    rc = datasets_cmd.dispatch(["validate-local", str(root)])

    assert rc == 1
    err = capsys.readouterr().err
    assert "invalid task.toml" in err
    assert "broken" in err


def test_publish_local_missing_runtime_config_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for env in (
        "LOOM_DB_URL",
        "LOOM_MINIO_ENDPOINT",
        "LOOM_MINIO_ACCESS_KEY",
        "LOOM_MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(env, raising=False)

    root = tmp_path / "team-evals"
    root.mkdir()
    rc = datasets_cmd.dispatch(["publish-local", str(root)])

    assert rc == 2
    err = capsys.readouterr().err
    assert "publish-local requires" in err
    assert "database URL" in err
    assert "object-store" in err
