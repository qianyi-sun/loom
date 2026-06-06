"""Structured logging configuration for Loom services.

Spec §7.2: JSON to stdout, no f-strings in log messages, correlation IDs via
contextvars. Use as:

    import structlog
    from loom.log import bind_trial_context, configure_logging

    configure_logging(level="info")
    log = structlog.get_logger(__name__)

    with bind_trial_context(trial_id=tid, step_id="main"):
        log.info("trial_state_changed", from_state="queued", to_state="claimed")
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO, Any
from uuid import UUID

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
)

from loom.models.types import LogLevel


def configure_logging(
    *,
    level: LogLevel = "info",
    out: IO[str] | None = None,
) -> None:
    """Configure stdlib + structlog so structlog emits JSON to `out`.

    `out` defaults to `sys.stdout` per spec §7.2. Pass a `StringIO` in tests.
    """
    out = out if out is not None else sys.stdout

    log_level = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "error": logging.ERROR,
        "fatal": logging.CRITICAL,
    }[level]

    handler = logging.StreamHandler(out)
    handler.setLevel(log_level)
    handler.setFormatter(logging.Formatter("%(message)s"))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)

    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


@contextmanager
def bind_trial_context(
    *,
    trial_id: UUID | None = None,
    step_id: str | None = None,
    worker_id: UUID | None = None,
    team_id: UUID | None = None,
) -> Iterator[None]:
    """Bind trial-correlation fields onto every log emitted in the with-block.

    v1 semantics: exiting clears ALL contextvars, not just the fields this
    call bound. Do NOT nest `bind_trial_context` calls — the inner exit will
    wipe fields the outer scope set. v2 may switch to
    `unbind_contextvars(*fields)` for true nested-scope support; for now,
    callers should bind everything they need at the outermost layer.
    """
    fields: dict[str, Any] = {}
    if trial_id is not None:
        fields["trial_id"] = str(trial_id)
    if step_id is not None:
        fields["step_id"] = step_id
    if worker_id is not None:
        fields["worker_id"] = str(worker_id)
    if team_id is not None:
        fields["team_id"] = str(team_id)

    bind_contextvars(**fields)
    try:
        yield
    finally:
        # v1: blunt clear. See docstring caveat about nesting.
        clear_contextvars()
