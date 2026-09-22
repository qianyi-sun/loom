"""Compatibility handling for the retired ``--backend`` batch flag.

Hosted Loom runs every batch on Nebius, so submissions never select a backend.
The flag is kept only so existing scripts do not break: it is hidden from
``--help`` and prints a deprecation warning. The value is still forwarded, and
the service is the single authority on it: an omitted or explicit ``nebius``
backend resolves to Nebius, other values are rejected with an actionable 400
outside disposable local execution (``LOOM_LOCAL_EXECUTION=1``), where the
service default is a worker backend. Dropping the value here would silently
reinterpret an explicit ``nebius`` request on such a stack.
"""

from __future__ import annotations

import argparse
import sys


def add_legacy_backend_flag(parser: argparse.ArgumentParser) -> None:
    """Register the hidden, deprecated ``--backend`` flag on ``parser``."""
    parser.add_argument("--backend", default=None, help=argparse.SUPPRESS)


def warn_legacy_backend_flag(value: str | None) -> None:
    """Print the deprecation warning when the legacy flag was supplied."""
    if value is None:
        return
    sys.stderr.write(
        "warning: --backend is deprecated; hosted Loom runs on Nebius only. "
        "Remove the flag: the service rejects other values.\n",
    )
