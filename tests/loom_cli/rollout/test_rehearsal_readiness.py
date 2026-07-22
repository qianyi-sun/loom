from __future__ import annotations

import hashlib

import pytest

from loom_cli.rollout.rehearsal_readiness import (
    REHEARSAL_CHECK_IDS,
    IsolatedRehearsalSession,
    RehearsalResult,
)


def _result(check_id: str, *, cleanup: bool = False) -> RehearsalResult:
    return RehearsalResult(
        check_id=check_id,
        isolation_id="rehearsal-abc123",
        candidate_sha="a" * 40,
        mutation_epoch=8,
        evidence_digest=hashlib.sha256(check_id.encode()).hexdigest(),
        journal_digest=hashlib.sha256((check_id + "-journal").encode()).hexdigest(),
        protected_mutation=False,
        cleanup_verified=cleanup,
        blockers={},
    )


def test_rehearsal_session_executes_each_action_once() -> None:
    calls: list[str] = []
    session = IsolatedRehearsalSession(
        {
            check_id: lambda check_id=check_id: (
                calls.append(check_id) or _result(check_id, cleanup=check_id == "rehearsal.cleanup")
            )
            for check_id in REHEARSAL_CHECK_IDS
        },
        isolation_id="rehearsal-abc123",
        candidate_sha="a" * 40,
        mutation_epoch=8,
    )

    for check_id in REHEARSAL_CHECK_IDS:
        assert session.execute(check_id).ready
        assert session.execute(check_id).ready

    assert calls == list(REHEARSAL_CHECK_IDS)


def test_rehearsal_rejects_any_claimed_protected_mutation() -> None:
    with pytest.raises(ValueError, match="evidence is invalid"):
        RehearsalResult(
            check_id="rehearsal.namespace",
            isolation_id="rehearsal-abc123",
            candidate_sha="a" * 40,
            mutation_epoch=8,
            evidence_digest="b" * 64,
            journal_digest="c" * 64,
            protected_mutation=True,
            cleanup_verified=False,
            blockers={},
        )
