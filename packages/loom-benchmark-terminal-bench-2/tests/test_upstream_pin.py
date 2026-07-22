"""Native TB2.1 rev-6 source and verifier bridge pin guards."""

from __future__ import annotations

import hashlib
from importlib.resources import files

from loom_benchmark_terminal_bench_2.upstream import (
    TB21_HUB_METADATA_VERSION,
    TB21_REVISION,
    TB21_SOURCE_REVISION,
)

EXPECTED_REVISION = "6"
EXPECTED_SOURCE_SNAPSHOT = "dde3cd95b80ff25af5abd99a80b6513a018ad3b4"
EXPECTED_HUB_METADATA_VERSION = (
    "sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a"
)

# SHA-256 of the verifier bridge vetted against the native TB2.1
# `tests/test.sh` reward-file contract. A shim change must re-review numeric
# zero and invalid-evidence semantics in `test_verifier_shim_contract.py`.
EXPECTED_SHIM_SHA256 = "3feaa3f52fb9c7f05bc4857a6346836d3e96f375b8be5556163d64fcaa4764c9"


def test_tb21_harbor_revision_and_metadata_are_pinned() -> None:
    assert TB21_REVISION == EXPECTED_REVISION
    assert TB21_HUB_METADATA_VERSION == EXPECTED_HUB_METADATA_VERSION


def test_tb21_source_reference_is_full_sha() -> None:
    assert TB21_SOURCE_REVISION == EXPECTED_SOURCE_SNAPSHOT
    assert len(TB21_SOURCE_REVISION) == 40
    assert all(c in "0123456789abcdef" for c in TB21_SOURCE_REVISION)


def test_verifier_shim_hash_matches_pinned_upstream() -> None:
    """Co-review guard for the native test/reward contract.

    Behavior is asserted in `test_verifier_shim_contract.py`; this hash check
    is the human-in-the-loop gate when the bridge changes.
    """
    shim_bytes = files("loom_benchmark_terminal_bench_2").joinpath("verifier_shim.sh").read_bytes()
    actual = hashlib.sha256(shim_bytes).hexdigest()
    assert actual == EXPECTED_SHIM_SHA256, (
        "verifier_shim.sh changed without updating EXPECTED_SHIM_SHA256. "
        "If this is intentional: (a) re-confirm the shim's "
        "numeric-reward and invalid-evidence semantics against native "
        "TB2.1 tests/test.sh, (b) update EXPECTED_SHIM_SHA256. "
        f"observed hash: {actual}"
    )
