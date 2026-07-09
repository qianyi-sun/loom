"""Legacy terminus-2 launcher tests — migration stub (#744)."""

from __future__ import annotations

from loom_launcher import get_adapter
from loom_launcher import terminus_2_runner


def test_terminus_2_no_longer_registered_as_adapter() -> None:
    assert get_adapter("terminus-2") is None


def test_legacy_runner_exits_with_migration_message() -> None:
    rc = terminus_2_runner.main([])
    assert rc == 2
