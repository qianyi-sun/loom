"""Shared pytest fixtures for the TB-2 adapter test suite.

NOTE: no `tests/__init__.py` — per feedback-package-tests-no-init,
sibling packages must keep `tests/` as a non-package directory so it
does not collide with the main repo's tests/ root during collection.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return _FIXTURES
