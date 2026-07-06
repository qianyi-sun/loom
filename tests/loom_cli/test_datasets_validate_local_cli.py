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


_TB_TASK_TOML = """\
version = "1"

[metadata]
id = "{tid}"
name = "TB task {tid}"

[environment]
cpus = 2
memory = "4G"
storage = "10G"
dockerfile = "Dockerfile"
"""


def test_validate_local_accepts_terminal_bench_shape(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """#341: TB-shaped bundles are auto-normalized before validation."""
    root = tmp_path / "src-useful"
    tasks_root = root / "tasks"
    bundle = tasks_root / "task-1"
    bundle.mkdir(parents=True)
    (bundle / "task.toml").write_text(_TB_TASK_TOML.format(tid="task-1"))
    (bundle / "instruction.md").write_text("do task-1\n")
    (bundle / "Dockerfile").write_text("FROM alpine:3.19\n")
    (root / "benchmark.toml").write_text(
        "schema_version = 1\n"
        "id = \"src-useful\"\n"
        "display_name = \"Source Useful\"\n"
        "series = \"internal\"\n"
        "license_spdx = \"MIT\"\n",
    )

    rc = datasets_cmd.dispatch(["validate-local", str(root)])

    assert rc == 0
    err = capsys.readouterr().err
    assert "invalid task.toml" not in err


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
        "LOOM_SVC_DB_URL",
        "LOOM_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_MINIO_SECRET_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(env, raising=False)

    root = tmp_path / "team-evals"
    root.mkdir()
    rc = datasets_cmd.dispatch(["publish-local", str(root)])

    assert rc == 2
    err = capsys.readouterr().err
    assert "publish-local requires" in err
    assert "--db-url / LOOM_DB_URL / LOOM_SVC_DB_URL" in err
    assert "--minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT" in err


def test_publish_local_help_points_secret_flags_to_env_references(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        datasets_cmd.dispatch(["publish-local", "--help"])

    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "env:LOOM_DB_URL" in out
    assert "env:LOOM_MINIO_ACCESS_KEY" in out
    assert "env:LOOM_MINIO_SECRET_KEY" in out
    assert "literal values are rejected" in out.lower()


def test_publish_local_rejects_literal_secret_flags_before_upload(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    root.mkdir()

    class _Store:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            pytest.fail("object store should not be created for unsafe argv secrets")

    async def fake_publish_local_benchmark(*args, **kwargs):  # type: ignore[no-untyped-def]
        pytest.fail("publish should not start for unsafe argv secrets")

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", _Store)
    monkeypatch.setattr(
        "loom_cli.local_benchmark_publish.publish_local_benchmark",
        fake_publish_local_benchmark,
    )

    rc = datasets_cmd.dispatch([
        "publish-local",
        str(root),
        "--db-url",
        "postgresql://loom:argv-db-secret@db.example/loom",
        "--minio-endpoint",
        "https://minio.example",
        "--minio-access-key",
        "argv-access-secret",
        "--minio-secret-key",
        "argv-minio-secret",
    ])

    assert rc == 2
    captured = capsys.readouterr()
    assert "publish-local refuses secret values in command-line argv" in captured.err
    assert "--db-url" in captured.err
    assert "--minio-access-key" in captured.err
    assert "--minio-secret-key" in captured.err
    assert "argv-db-secret" not in captured.err
    assert "argv-access-secret" not in captured.err
    assert "argv-minio-secret" not in captured.err
    assert captured.out == ""


def test_publish_local_resolves_env_secret_references_without_logging_values(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    root.mkdir()
    captured: dict[str, object] = {}

    monkeypatch.setenv(
        "SAFE_PUBLISH_DB_URL",
        "postgresql://loom:resolved-db-secret@db.example/loom",
    )
    monkeypatch.setenv("SAFE_PUBLISH_MINIO_ACCESS_KEY", "resolved-access-secret")
    monkeypatch.setenv("SAFE_PUBLISH_MINIO_SECRET_KEY", "resolved-minio-secret")

    class _Stats:
        benchmark_id = "team-evals"
        task_count = 1
        inserted = 1
        updated = 0
        unchanged = 0
        uploaded_objects = 4
        compat_flattened_files = 0
        source_prefix = "s3://loom-benchmarks/team-evals/"

    class _Store:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            captured["store"] = kwargs

    async def fake_publish_local_benchmark(*args, **kwargs):  # type: ignore[no-untyped-def]
        captured["db_url"] = kwargs["db_url"]
        return _Stats()

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", _Store)
    monkeypatch.setattr(
        "loom_cli.local_benchmark_publish.publish_local_benchmark",
        fake_publish_local_benchmark,
    )

    rc = datasets_cmd.dispatch([
        "publish-local",
        str(root),
        "--db-url",
        "env:SAFE_PUBLISH_DB_URL",
        "--minio-endpoint",
        "https://minio.example",
        "--minio-access-key",
        "env:SAFE_PUBLISH_MINIO_ACCESS_KEY",
        "--minio-secret-key",
        "env:SAFE_PUBLISH_MINIO_SECRET_KEY",
    ])

    assert rc == 0
    assert captured["db_url"] == "postgresql://loom:resolved-db-secret@db.example/loom"
    assert captured["store"] == {
        "endpoint_url": "https://minio.example",
        "access_key": "resolved-access-secret",
        "secret_key": "resolved-minio-secret",
    }
    output = capsys.readouterr()
    assert "resolved-db-secret" not in output.out
    assert "resolved-access-secret" not in output.out
    assert "resolved-minio-secret" not in output.out
    assert output.err == ""


def test_publish_local_explicit_flatten_override_is_visible_in_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    root.mkdir()

    class _Stats:
        benchmark_id = "team-evals"
        task_count = 1
        inserted = 1
        updated = 0
        unchanged = 0
        uploaded_objects = 4
        compat_flattened_files = 2
        source_prefix = "s3://loom-benchmarks/team-evals/"

    async def fake_publish_local_benchmark(*args, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["compat_flatten_environment"] is True
        return _Stats()

    class _Store:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            pass

    monkeypatch.setattr(
        "loom_cli.local_benchmark_publish.publish_local_benchmark",
        fake_publish_local_benchmark,
    )
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", _Store)
    monkeypatch.setenv("LOOM_DB_URL", "postgresql://loom/loom")
    monkeypatch.setenv("LOOM_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("LOOM_MINIO_SECRET_KEY", "secret")

    rc = datasets_cmd.dispatch([
        "publish-local",
        str(root),
        "--compat-flatten-environment",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "compat_flattened_files=2" in out
