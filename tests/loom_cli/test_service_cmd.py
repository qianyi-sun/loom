"""`loom service {up,down,status}` argparse + dispatch.

The actual docker-compose / alembic / seed_test_data calls are
subprocess invocations against a real docker daemon — exercised by
manual smoke + tests/system/ (gated). Here we only verify the CLI
surface, missing-compose-file handling, and that the right argv
shapes flow into our subprocess wrappers.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from loom_cli.__main__ import main
from loom_cli.service_cmd import _compose_args


def test_help_lists_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["service", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "up" in out and "down" in out and "status" in out


def test_up_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "up",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_down_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "down",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_status_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "status",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_compose_args_includes_env_file_when_present(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env = tmp_path / ".env"
    env.write_text("FOO=bar\n")
    args = _compose_args(compose, env)
    assert "--env-file" in args
    assert str(env) in args
    assert "-f" in args
    assert str(compose) in args


def test_compose_args_omits_env_file_when_missing(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    missing_env = tmp_path / "does-not-exist.env"
    args = _compose_args(compose, missing_env)
    assert "--env-file" not in args
    assert "-f" in args
    assert str(compose) in args


def test_up_invokes_docker_compose_up(
    tmp_path: Path,
) -> None:
    """Verify the happy-path invocation chain — `docker compose up -d`
    runs first; on its failure we bail before alembic + seed."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    with patch("loom_cli.service_cmd._run") as mock_run, \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=False) as mock_wait:
        # _run returns CompletedProcess-like; we need .returncode = 0
        from subprocess import CompletedProcess
        mock_run.return_value = CompletedProcess([], 0, "", "")
        rc = main([
            "service", "up",
            "--compose-file", str(compose),
            "--env-file", str(tmp_path / "absent.env"),
        ])
        # postgres didn't go healthy → exit 1, no alembic call
        assert rc == 1
        # First call should be `docker compose ... up -d`
        first_args = mock_run.call_args_list[0].args[0]
        assert "up" in first_args and "-d" in first_args
        assert mock_wait.called
