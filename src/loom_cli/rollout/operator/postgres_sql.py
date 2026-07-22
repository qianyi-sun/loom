"""Fail-closed rendering for checked-in PostgreSQL statements passed as argv."""

from __future__ import annotations


def single_line_sql(statement: str) -> str:
    """Render one bounded checked-in statement for the protected argv runner.

    The protected subprocess contract rejects newline-bearing argv. SQL line
    comments cannot be flattened safely because their scope is newline-bound,
    so checked-in statements using either comment form are rejected.
    """

    if (
        not statement.strip()
        or "\x00" in statement
        or "--" in statement
        or "/*" in statement
        or "*/" in statement
    ):
        raise ValueError("protected PostgreSQL statement is unsafe")
    rendered = " ".join(line.strip() for line in statement.splitlines() if line.strip())
    if not rendered or len(rendered.encode()) > 64 * 1024 or "\n" in rendered:
        raise ValueError("protected PostgreSQL statement is unbounded")
    return rendered


__all__ = ["single_line_sql"]
