from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.writer_lease import (
    Lease,
    LeaseError,
    LeaseHeldError,
    SingleWriterLease,
)

_T0 = "2026-07-30T20:00:00Z"
_T1 = "2026-07-30T20:00:30Z"  # +30s
_T2 = "2026-07-30T20:01:00Z"  # +60s
_T3 = "2026-07-30T20:02:00Z"  # +120s


def _lease(tmp_path: Path, *, environment: str = "staging") -> SingleWriterLease:
    return SingleWriterLease(tmp_path / "writer.lease", environment=environment)


def test_absent_lease_reads_none(tmp_path: Path) -> None:
    assert _lease(tmp_path).read() is None


def test_acquire_free_lease_mints_token_one(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    held = lease.acquire("reconciler-a", now=_T0, expires_at=_T2)
    assert held.holder == "reconciler-a"
    assert held.fencing_token == 1
    assert lease.read() == held


def test_second_holder_blocked_while_unexpired(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    lease.acquire("a", now=_T0, expires_at=_T2)  # expires at +60s
    with pytest.raises(LeaseHeldError, match="held by 'a'"):
        lease.acquire("b", now=_T1, expires_at=_T3)  # +30s, still held


def test_expired_lease_can_be_taken_over_and_token_advances(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    lease.acquire("a", now=_T0, expires_at=_T1)  # expires at +30s
    taken = lease.acquire("b", now=_T2, expires_at=_T3)  # +60s > +30s → expired
    assert taken.holder == "b"
    assert taken.fencing_token == 2  # fencing advances — 'a' is now stale


def test_reacquire_by_same_holder_still_advances_token(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    lease.acquire("a", now=_T0, expires_at=_T1)
    again = lease.acquire("a", now=_T2, expires_at=_T3)
    assert again.fencing_token == 2


def test_renew_extends_and_keeps_token(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    held = lease.acquire("a", now=_T0, expires_at=_T1)
    renewed = lease.renew("a", fencing_token=held.fencing_token, expires_at=_T3)
    assert renewed.fencing_token == held.fencing_token
    assert renewed.expires_at == _T3
    assert renewed.acquired_at == _T0  # unchanged


def test_renew_rejects_wrong_holder_or_token(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    lease.acquire("a", now=_T0, expires_at=_T2)
    with pytest.raises(LeaseError, match="not currently held"):
        lease.renew("b", fencing_token=1, expires_at=_T3)
    with pytest.raises(LeaseError, match="not currently held"):
        lease.renew("a", fencing_token=99, expires_at=_T3)


def test_release_only_by_current_holder_and_token(tmp_path: Path) -> None:
    lease = _lease(tmp_path)
    lease.acquire("a", now=_T0, expires_at=_T2)
    lease.release("b", fencing_token=1)  # wrong holder → no-op
    assert lease.read() is not None
    lease.release("a", fencing_token=99)  # wrong token → no-op
    assert lease.read() is not None
    lease.release("a", fencing_token=1)  # correct → released
    assert lease.read() is None


def test_acquire_rejects_expiry_not_after_now(tmp_path: Path) -> None:
    with pytest.raises(LeaseError, match="expires_at must be after now"):
        _lease(tmp_path).acquire("a", now=_T2, expires_at=_T0)


def test_environment_mismatch_is_refused(tmp_path: Path) -> None:
    _lease(tmp_path, environment="staging").acquire("a", now=_T0, expires_at=_T2)
    with pytest.raises(LeaseError, match="does not match 'prod'"):
        _lease(tmp_path, environment="prod").read()


def test_lease_round_trip() -> None:
    lease = Lease("a", 3, _T0, _T2)
    assert Lease.from_dict(lease.to_dict()) == lease
