"""`loom service {up,down,status}` argparse + dispatch.

The actual docker-compose / alembic / seed_test_data calls are
subprocess invocations against a real docker daemon — exercised by
manual smoke + tests/system/ (gated). Here we only verify the CLI
surface, missing-compose-file handling, and that the right argv
shapes flow into our subprocess wrappers.
"""

from __future__ import annotations

import io
import stat
import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest

from loom_cli.__main__ import main
from loom_cli.service_cmd import _compose_args


def _read_admin_token(secret_file: Path) -> str:
    data = tomllib.loads(secret_file.read_text(encoding="utf-8"))
    return data["admin"]["token"]


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


def test_seed_test_data_parses_all_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--print all` emits `<label>: <token>` per line; the wrapper
    parses team/worker dev tokens while admin comes from a secret file."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    fake_stdout = (
        "team: loom_team_aaaaaa\n"
        "worker: loom_w_bbbbbb\n"
    )

    def _fake_run(*_args, **_kwargs):
        return CompletedProcess([], 0, fake_stdout, "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert tokens == {
        "team": "loom_team_aaaaaa",
        "worker": "loom_w_bbbbbb",
    }


def test_seed_test_data_invokes_dev_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`loom service up` must call the seed script with `--mode dev`
    so the Benchmarks table is populated and no placeholder
    (hello-world Task, card-e2e RateCard) is seeded into the dev
    stack."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    captured_argv: list[list[str]] = []

    def _fake_run(argv, *_args, **_kwargs):
        captured_argv.append(list(argv))
        return CompletedProcess([], 0, "team: t\nworker: w\n", "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, _tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert captured_argv, "expected at least one subprocess.run call"
    argv = captured_argv[0]
    assert "--mode" in argv
    assert argv[argv.index("--mode") + 1] == "dev"
    assert "--print" in argv
    assert argv[argv.index("--print") + 1] == "all"


def test_print_summary_labels_admin_as_file_backed_dev_singleton(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The summary must call out that admin comes from the dev secret file."""
    from loom_cli.service_cmd import _print_summary

    _print_summary({"team": "loom_team_x", "admin": "loom_admin_y"})
    out = capsys.readouterr().out
    assert "loom_team_x" in out
    assert "loom_admin_y" in out
    assert "DEV-ONLY" in out
    assert "file-backed" in out


def test_ensure_dev_admin_secret_creates_0600_and_returns_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"

    token = _ensure_dev_admin_secret(secret_file)

    assert token == _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


def test_ensure_dev_admin_secret_preserves_existing_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"
    existing = "loom_admin_" + "E" * 43
    secret_file.parent.mkdir(parents=True)
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{existing}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    token = _ensure_dev_admin_secret(secret_file)

    assert token == existing


def test_write_env_tokens_creates_file_when_absent(tmp_path) -> None:
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(env_file, {
        "team": "loom_team_aaa",
        "worker": "loom_w_bbb",
        "admin": "loom_admin_ccc",
    })
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_aaa" in content
    assert "LOOM_WORKER_TOKEN=loom_w_bbb" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_ccc" in content


def test_write_env_tokens_replaces_existing_keys_preserving_others(
    tmp_path,
) -> None:
    """Idempotent overwrite: existing token lines get the new value,
    unrelated lines (comments, custom env vars) survive verbatim."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text(
        "# Local dev tokens\n"
        "LOOM_WORKER_TOKEN=loom_w_old\n"
        "LOOM_TEAM_TOKEN=loom_team_old\n"
        "LOOM_ADMIN_TOKEN=loom_admin_old\n"
        "MY_CUSTOM_VAR=please-keep-me\n",
    )
    _write_env_tokens(env_file, {
        "team": "loom_team_new",
        "worker": "loom_w_new",
        "admin": "loom_admin_new",
    })
    lines = env_file.read_text().splitlines()
    assert "# Local dev tokens" in lines
    assert "LOOM_TEAM_TOKEN=loom_team_new" in lines
    assert "LOOM_WORKER_TOKEN=loom_w_new" in lines
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in lines
    assert "MY_CUSTOM_VAR=please-keep-me" in lines
    # Old values are gone; no duplicates of any key.
    assert not any(line.endswith("=loom_team_old") for line in lines)
    keys = [
        line.split("=", 1)[0]
        for line in lines
        if "=" in line and not line.lstrip().startswith("#")
    ]
    assert len(keys) == len(set(keys))


def test_write_env_tokens_appends_missing_keys(tmp_path) -> None:
    """If only one key exists in .env, the other two must be appended
    rather than silently dropped."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text("LOOM_TEAM_TOKEN=loom_team_only\n")
    _write_env_tokens(env_file, {
        "team": "loom_team_new",
        "worker": "loom_w_new",
        "admin": "loom_admin_new",
    })
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_new" in content
    assert "LOOM_WORKER_TOKEN=loom_w_new" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in content


def test_up_recreates_worker_after_seeding_fresh_tokens(
    tmp_path: Path,
) -> None:
    """After `_seed_test_data` mints fresh tokens and `_write_env_tokens`
    persists them to .env, the worker container is still running with
    the STALE token it booted with — `docker restart` reuses the old env,
    only `up --force-recreate` re-reads .env. `loom service up` MUST issue
    that recreate, otherwise the worker keeps rejecting control-plane
    requests with 401 until the operator notices.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
        "admin": "loom_admin_db_ignored",
    }
    admin_secret_token = "loom_admin_" + "U" * 43

    with patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token):
        rc = main([
            "service", "up",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    assert env_file.exists(), "env_file should have been written"
    assert f"LOOM_ADMIN_TOKEN={admin_secret_token}" in env_file.read_text()
    assert "loom_admin_db_ignored" not in env_file.read_text()

    # Find the recreate call — must come after the initial `up -d` and
    # carry --force-recreate + --no-deps + worker target.
    recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv and "worker" in argv
    ]
    assert len(recreate_calls) == 1, (
        f"expected exactly one --force-recreate worker call; "
        f"got {len(recreate_calls)} of {len(captured_run_calls)} _run calls. "
        f"All argvs: {captured_run_calls!r}"
    )
    recreate_argv = recreate_calls[0]
    assert "up" in recreate_argv
    assert "-d" in recreate_argv
    assert "--no-deps" in recreate_argv

    # Order check: the recreate must come AFTER _write_env_tokens
    # has run (i.e., after the .env file has the new token). We assert
    # this by confirming the recreate is the LAST _run call (since
    # _write_env_tokens is between _seed_test_data and the recreate
    # in _up).
    assert captured_run_calls[-1] == recreate_argv, (
        "worker recreate must be the last subprocess call, AFTER "
        "_write_env_tokens has persisted fresh tokens"
    )


def test_up_skips_worker_recreate_when_no_env_file(
    tmp_path: Path,
) -> None:
    """When operator runs without --env-file, _write_env_tokens is
    skipped — there's no .env to update, so there's no stale token to
    flush. Recreating the worker would be pointless work.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
    }

    with patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value="loom_admin_" + "N" * 43):
        # argparse defaults --env-file to <compose_dir>/.env if not
        # explicitly None, so we have to pass --env-file pointing
        # somewhere AND ensure the loader treats it as "absent".
        # The CLI doesn't actually have a "no env file" mode — it
        # defaults to .env next to the compose file. So this test
        # documents that "env_file is None" is actually an internal
        # branch reachable only via the API, not the CLI flag set.
        import argparse

        from loom_cli.service_cmd import _up
        args = argparse.Namespace(
            compose_file=compose,
            env_file=None,
            db_url="postgresql://x/y",
            admin_secret_file=tmp_path / ".loom" / "admin" / "secrets.toml",
        )
        rc = _up(args)

    assert rc == 0
    # No recreate should have happened.
    recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv
    ]
    assert recreate_calls == [], (
        f"unexpected --force-recreate when env_file is None: "
        f"{recreate_calls!r}"
    )


def test_init_admin_secret_writes_0600_without_printing_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"

    rc = main([
        "service", "init-admin", "--secret-file", str(secret_file),
    ])

    assert rc == 0
    token = _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert len(token.removeprefix("loom_admin_")) >= 32
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    captured = capsys.readouterr()
    assert token not in captured.out
    assert token not in captured.err
    assert str(secret_file) in captured.out


def test_reveal_admin_requires_confirmation_unless_yes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    token = "loom_admin_" + "R" * 43
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{token}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    monkeypatch.setattr("sys.stdin", io.StringIO("no\n"))
    denied = main([
        "service", "reveal-admin", "--secret-file", str(secret_file),
    ])
    denied_output = capsys.readouterr()

    approved = main([
        "service", "reveal-admin", "--secret-file", str(secret_file), "--yes",
    ])
    approved_output = capsys.readouterr()

    assert denied == 2
    assert token not in denied_output.out
    assert token not in denied_output.err
    assert approved == 0
    assert token in approved_output.out


def test_rotate_admin_replaces_secret_without_printing_new_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    old_token = "loom_admin_" + "O" * 43
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{old_token}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    rc = main([
        "service", "rotate-admin", "--secret-file", str(secret_file),
    ])

    assert rc == 0
    new_token = _read_admin_token(secret_file)
    assert new_token.startswith("loom_admin_")
    assert new_token != old_token
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    out = capsys.readouterr().out
    assert new_token not in out
    assert old_token not in out
    assert "restart" in out.lower()
