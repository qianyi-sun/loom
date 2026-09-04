"""Bounded public failures for the root-owned node guard."""

from __future__ import annotations

import re

_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class GuardError(RuntimeError):
    """A failure whose printable form never reflects untrusted detail."""

    def __init__(self, code: str, *, detail: object | None = None) -> None:
        del detail
        safe = code if isinstance(code, str) and _ERROR_CODE.fullmatch(code) else "guard_failed"
        self.code = safe
        super().__init__(safe)


__all__ = ["GuardError"]
