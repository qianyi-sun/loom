"""Process-local, explicit CLI config selection; no shared active-context file."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_CONTEXT: ContextVar[str | None] = ContextVar("loom_cli_context", default=None)
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,95}")


def current_context() -> str | None:
    return _CONTEXT.get()


@contextmanager
def selected_context(name: str | None) -> Iterator[None]:
    if name is not None and (not isinstance(name, str) or _NAME.fullmatch(name) is None):
        raise ValueError("context must contain 1-96 lowercase letters, digits, underscores or hyphens")
    token = _CONTEXT.set(name)
    try:
        yield
    finally:
        _CONTEXT.reset(token)
