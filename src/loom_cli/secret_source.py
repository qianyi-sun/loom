"""Resolve secret-bearing CLI flag values.

Per cluster-deploy.md §CLI surface argv-hygiene rule: every
secret-bearing flag (`--token`, `--api-key`, `--passphrase`, ...)
accepts only `env:VAR`, `file:PATH`, or `-` (stdin). Literal values
are rejected at argparse-time with a clear error pointing at the
indirection forms. Rationale:

- Shell history (`.bash_history`, `.zsh_history`, etc.) records argv.
- `ps -ef` lists argv to anyone with read access on /proc.
- CI log capture often includes the full command line.
- No interactive-paste fallback: scripts can't accidentally hang waiting
  for stdin; humans get a clear "missing required argument" rather than
  a silent prompt.

This module is the canonical resolver — every secret-bearing CLI flag
must route through `resolve_secret_source()` so the rule is enforced
once.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path

# Accepted forms documented in `--token` / `--api-key` help text.
_VALID_FORMS = "env:VAR | file:PATH | -"


class SecretSourceError(Exception):
    """The given source doesn't parse as a permitted indirection form."""


def resolve_secret_source(source: str, *, flag_name: str) -> str:
    """Read the actual secret value from one of:

    - ``env:VAR`` — value is os.environ[VAR]; missing env var raises.
    - ``file:PATH`` — value is the file's content, stripped.
    - ``-`` — value is read from stdin until EOF, stripped.

    Any other input raises :class:`SecretSourceError` — including the
    literal value itself (callers must use one of the three indirections,
    even for "obvious" non-secret-looking values, so the rule is
    consistent + tooling can't accidentally accept a literal).

    `flag_name` is the argv-facing flag (e.g. ``"--token"``) — included
    in error messages so users know which flag was rejected.
    """
    if source == "-":
        value = sys.stdin.read()
    elif source.startswith("env:"):
        var = source[len("env:"):]
        if not var:
            raise SecretSourceError(
                f"{flag_name}: env: source requires a variable name "
                f"(use {flag_name} env:VAR)",
            )
        if var not in os.environ:
            raise SecretSourceError(
                f"{flag_name}: env var {var!r} is not set",
            )
        value = os.environ[var]
    elif source.startswith("file:"):
        path_str = source[len("file:"):]
        if not path_str:
            raise SecretSourceError(
                f"{flag_name}: file: source requires a path "
                f"(use {flag_name} file:PATH)",
            )
        path = Path(path_str).expanduser()
        try:
            value = path.read_text()
        except OSError as e:
            raise SecretSourceError(
                f"{flag_name}: cannot read {path}: {e}",
            ) from e
    else:
        raise SecretSourceError(
            f"{flag_name}: literal values are rejected; use one of "
            f"{{{_VALID_FORMS}}}. Why: literal argv leaks via shell "
            f"history, `ps -ef`, and CI log capture.",
        )

    value = value.strip()
    if not value:
        raise SecretSourceError(
            f"{flag_name}: resolved value is empty (whitespace-only?)",
        )
    return value


def secret_source_argparse_type(flag_name: str) -> Callable[[str], str]:
    """Build an argparse `type=` callable that validates the indirection
    form at parse time (BEFORE the handler runs) and raises
    `argparse.ArgumentTypeError` on bad input.

    Note: this validates SHAPE only — it doesn't actually read the env
    var / file / stdin (argparse runs early, before the handler). The
    handler still calls `resolve_secret_source()` to read the value.

    Why two-phase: (1) catches "you passed a literal" at the help-printable
    layer with a clean error, (2) lets the handler do the I/O when it's
    actually needed (so `--token env:VAR` printed in `--help` doesn't try
    to read the env var).
    """
    def _check(s: str) -> str:
        if s == "-" or s.startswith(("env:", "file:")):
            return s
        raise argparse.ArgumentTypeError(
            f"{flag_name}: literal values are rejected; use one of "
            f"{{{_VALID_FORMS}}}",
        )
    return _check
