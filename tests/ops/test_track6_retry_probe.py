"""Disposable #1130 Track 6 transient-retry acceptance probe.

This branch must never merge. Attempt 1 fails intentionally; a governed retry
must make attempt 2 pass without changing the head.
"""

from __future__ import annotations

import os


def test_disposable_retry_probe_recovers_on_attempt_two() -> None:
    assert not (
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"
    ), "controlled platform-transient retry probe"
