"""Argv-secret indirection forms (`env:VAR | file:PATH | -`)."""

from __future__ import annotations

import argparse
from io import StringIO
from pathlib import Path

import pytest

from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)

# ──────────────────────────────────────────────────────────────────────
# resolve_secret_source
# ──────────────────────────────────────────────────────────────────────


def test_env_source_returns_env_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MY_TOKEN", "sk-abc123")
    assert resolve_secret_source("env:MY_TOKEN", flag_name="--token") == "sk-abc123"


def test_env_source_strips_whitespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MY_TOKEN", "  sk-abc123\n\n")
    assert resolve_secret_source("env:MY_TOKEN", flag_name="--token") == "sk-abc123"


def test_env_source_missing_var_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NOPE_VAR", raising=False)
    with pytest.raises(SecretSourceError, match="env var 'NOPE_VAR' is not set"):
        resolve_secret_source("env:NOPE_VAR", flag_name="--token")


def test_env_source_empty_var_name_raises() -> None:
    with pytest.raises(SecretSourceError, match="env: source requires a variable name"):
        resolve_secret_source("env:", flag_name="--token")


def test_file_source_reads_file_stripped(tmp_path: Path) -> None:
    f = tmp_path / "secret.txt"
    f.write_text("  sk-from-file  \n")
    result = resolve_secret_source(f"file:{f}", flag_name="--token")
    assert result == "sk-from-file"


def test_file_source_expands_user(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """~ in the path should expand to HOME."""
    monkeypatch.setenv("HOME", str(tmp_path))
    f = tmp_path / "secret.txt"
    f.write_text("home-relative-secret")
    result = resolve_secret_source("file:~/secret.txt", flag_name="--token")
    assert result == "home-relative-secret"


def test_file_source_missing_path_raises() -> None:
    with pytest.raises(SecretSourceError, match="file: source requires a path"):
        resolve_secret_source("file:", flag_name="--token")


def test_file_source_nonexistent_file_raises(tmp_path: Path) -> None:
    with pytest.raises(SecretSourceError, match="cannot read"):
        resolve_secret_source(
            f"file:{tmp_path / 'nope.txt'}", flag_name="--token",
        )


def test_stdin_source_reads_stripped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdin", StringIO("piped-secret\n"))
    assert resolve_secret_source("-", flag_name="--token") == "piped-secret"


def test_stdin_source_empty_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdin", StringIO("   \n"))
    with pytest.raises(SecretSourceError, match="resolved value is empty"):
        resolve_secret_source("-", flag_name="--token")


def test_literal_value_rejected() -> None:
    """The whole point: passing the raw value is forbidden, even if it
    looks like a normal string."""
    with pytest.raises(SecretSourceError, match="literal values are rejected"):
        resolve_secret_source("sk-actually-the-token", flag_name="--token")


def test_literal_value_rejected_even_if_resembles_indirection() -> None:
    """A token that happens to start with 'env' or 'file' (without the
    colon) is still a literal."""
    with pytest.raises(SecretSourceError):
        resolve_secret_source("envoy-token", flag_name="--token")
    with pytest.raises(SecretSourceError):
        resolve_secret_source("filewatcher", flag_name="--token")


def test_flag_name_in_error_messages() -> None:
    """Error messages name the flag so users know what they got wrong."""
    with pytest.raises(SecretSourceError, match=r"--api-key"):
        resolve_secret_source("literal", flag_name="--api-key")


# ──────────────────────────────────────────────────────────────────────
# secret_source_argparse_type (shape-only validator)
# ──────────────────────────────────────────────────────────────────────


def test_argparse_type_accepts_indirections() -> None:
    check = secret_source_argparse_type("--token")
    assert check("env:VAR") == "env:VAR"
    assert check("file:/path/to/secret") == "file:/path/to/secret"
    assert check("-") == "-"


def test_argparse_type_rejects_literal() -> None:
    check = secret_source_argparse_type("--token")
    with pytest.raises(argparse.ArgumentTypeError, match="literal values"):
        check("plain-token-value")


def test_argparse_type_doesnt_resolve_value() -> None:
    """The argparse type only validates SHAPE — it must not actually
    read the env var / file / stdin (argparse runs at parse-time, far
    too early to do I/O). `loom auth login --help` shouldn't crash on
    `--token env:DOES_NOT_EXIST`."""
    check = secret_source_argparse_type("--token")
    # No exception — env var lookup deferred to handler.
    assert check("env:DEFINITELY_NOT_SET_VAR_123") == "env:DEFINITELY_NOT_SET_VAR_123"
