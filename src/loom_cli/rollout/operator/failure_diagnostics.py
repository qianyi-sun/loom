"""Secret-safe diagnostics for unclassified (uncoded) rollout failures.

Foundation for the structured-error principle in the rollout redesign
(#1085, phase 1 — un-mask failures): an *unclassified* failure — one that
escapes without a curated code — must still surface *what* and *where* (the
exception type and its raise-site) so it is never a dead-end ``failed safely``.

It must do so **without** emitting the exception *message*: arbitrary
exception text cannot be reliably pattern-redacted (the #1077 lesson — a plain
secret-bearing string slips through), so only source coordinates and the
exception type — neither of which carries runtime values — are surfaced.
Failure paths with known-safe context should instead raise a coded error (e.g.
``BackupError``) carrying its own curated, secret-safe diagnostic.
"""

from __future__ import annotations

import traceback
from pathlib import Path


def unclassified_failure_location(error: BaseException) -> str:
    """Return the secret-safe raise-site ``at <file>:<line> in <func>``.

    Only source coordinates are used (never runtime values), so the result is
    always safe to surface. Returns ``""`` when no traceback is available.
    """
    frames = traceback.extract_tb(error.__traceback__)
    if not frames:
        return ""
    last = frames[-1]
    return f" at {Path(last.filename).name}:{last.lineno} in {last.name}"


def unclassified_failure_diagnostic(error: BaseException, *, activity: str) -> str:
    """One-line secret-safe diagnostic for an unclassified failure.

    Surfaces the exception *type* and *raise-site* only; the message is
    deliberately withheld because arbitrary exception text is not
    assumable-safe (#1077). Use at last-resort ``except`` boundaries where no
    coded reason is available; coded paths surface their own curated
    diagnostic instead.
    """
    return (
        f"unclassified {activity} failure: "
        f"{type(error).__name__}{unclassified_failure_location(error)}"
    )
