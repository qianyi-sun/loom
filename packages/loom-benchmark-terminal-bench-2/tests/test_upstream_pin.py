"""Pin guard — UPSTREAM_REVISION must match the SHA pinned literally
in this test. Silent drift in upstream.py fails the assertion below.
"""

from __future__ import annotations

import hashlib
from importlib.resources import files

from loom_benchmark_terminal_bench_2 import UPSTREAM_REVISION

EXPECTED_SHA = "91e10457b5410f16c44364da1a34cb6de8c488a5"

# SHA-256 of the verifier_shim.sh bytes that were vetted against the
# pinned upstream's `run-tests.sh` contract (TB-2 core v0.1.1). When
# either the shim or the upstream pin changes, this hash will mismatch
# and force a deliberate co-review of the bridge contract — see
# `test_verifier_shim_contract.py` for the shape the shim must
# preserve.
EXPECTED_SHIM_SHA256 = (
    "33d6e4c386203e939bcdd385ea5cba34f626e26f19396061ca30999f9a3063db"
)


def test_upstream_revision_is_pinned() -> None:
    assert UPSTREAM_REVISION == EXPECTED_SHA, (
        "UPSTREAM_REVISION drifted; if intentional, bump the probe notes too."
    )


def test_upstream_revision_is_full_sha() -> None:
    assert len(UPSTREAM_REVISION) == 40
    assert all(c in "0123456789abcdef" for c in UPSTREAM_REVISION)


def test_verifier_shim_hash_matches_pinned_upstream() -> None:
    """Co-review guard: if either the upstream pin or the shim moves,
    the operator must update BOTH the pin and this hash to assert the
    shim's contract was re-checked against the new run-tests.sh
    semantics. Without this, an upstream bump that changes exit-code
    conventions would silently emit wrong rewards.

    Behavior is still asserted in
    `test_verifier_shim_contract.py`; this hash check is the human-in-
    the-loop gate during a pin bump."""
    shim_bytes = (
        files("loom_benchmark_terminal_bench_2")
        .joinpath("verifier_shim.sh")
        .read_bytes()
    )
    actual = hashlib.sha256(shim_bytes).hexdigest()
    assert actual == EXPECTED_SHIM_SHA256, (
        "verifier_shim.sh changed without updating EXPECTED_SHIM_SHA256. "
        "If this is intentional: (a) re-confirm the shim's "
        "`{rewards,checks}` contract still matches the upstream "
        f"`run-tests.sh` semantics at SHA {EXPECTED_SHA}, "
        "(b) update EXPECTED_SHIM_SHA256 to the new hash. "
        f"observed hash: {actual}"
    )
