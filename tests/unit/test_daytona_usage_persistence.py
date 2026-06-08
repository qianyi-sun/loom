from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from loom_drivers.daytona.usage import CloudComputeRecord, compute_record


def test_compute_record_fields() -> None:
    start = datetime(2026, 6, 8, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=42, microseconds=500000)
    rec = compute_record(
        team_id=uuid4(),
        trial_id=uuid4(),
        sandbox_id="sb-xyz",
        image="python:3.12-slim",
        started_at=start,
        stopped_at=end,
        per_second_usd=Decimal("0.0005"),
    )
    assert rec.compute_seconds == 42.5
    # 42.5 * 0.0005 = 0.021250, 6dp half-even
    assert rec.cost_usd == Decimal("0.021250")
    assert rec.sandbox_id == "sb-xyz"
    assert rec.image == "python:3.12-slim"
    assert rec.cloud_provider == "daytona"  # default per A26.1


def test_compute_record_accepts_modal_provider() -> None:
    """Plan 27 (Modal driver) shares this helper via cloud_provider arg."""
    start = datetime(2026, 6, 8, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=10)
    rec = compute_record(
        team_id=uuid4(),
        trial_id=uuid4(),
        sandbox_id="modal-x",
        image="img",
        started_at=start,
        stopped_at=end,
        per_second_usd=Decimal("0.0002"),
        cloud_provider="modal",
    )
    assert rec.cloud_provider == "modal"
    assert rec.cost_usd == Decimal("0.002000")


def test_dataclass_is_frozen() -> None:
    """Records are immutable so they can be safely shared across coroutines."""
    rec = CloudComputeRecord(
        team_id=uuid4(),
        trial_id=uuid4(),
        cloud_provider="daytona",
        sandbox_id="x",
        image="i",
        started_at=datetime(2026, 6, 8, tzinfo=UTC),
        stopped_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=UTC),
        compute_seconds=1.0,
        cost_usd=Decimal("0.000100"),
    )
    import dataclasses
    with __import__("pytest").raises((dataclasses.FrozenInstanceError, AttributeError)):
        rec.sandbox_id = "y"  # type: ignore[misc]
