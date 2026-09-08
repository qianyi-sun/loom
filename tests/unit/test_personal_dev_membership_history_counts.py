"""A per-report input limit must not become a personal environment lifetime quota."""

from datetime import UTC, datetime
from types import SimpleNamespace

from loom_capacity_manager import membership_subject_status as status
from loom_capacity_manager.models import CapacityReservationTranche
from tests.unit.test_capacity_membership_subject_status import _query


async def test_lifetime_counts_are_not_limited_by_single_demand_report(monkeypatch):
    monkeypatch.setattr(status, "MAX_FIXED_CLAIMS_PER_REPORT", 1, raising=False)
    subject = _query().membership_receipt.result.member.configuration
    rows = [
        SimpleNamespace(
            state="closed",
            closed_at=datetime.now(UTC),
            deployment_generation=subject.deployment_generation,
        ),
        SimpleNamespace(
            state="closed",
            closed_at=datetime.now(UTC),
            deployment_generation=subject.deployment_generation + 1,
        ),
    ]

    class Session:
        async def scalars(self, statement):
            values = (
                rows
                if statement.column_descriptions[0]["entity"] is CapacityReservationTranche
                else []
            )
            return SimpleNamespace(all=lambda: values)

        async def stream_scalars(self, statement):
            values = (
                rows
                if statement.column_descriptions[0]["entity"] is CapacityReservationTranche
                else []
            )

            async def stream():
                for row in values:
                    yield row

            return stream()

    total, deployment = await status._work_counts(Session(), subject)
    assert total.legacy_reservations == 2
    assert deployment.legacy_reservations == 1
    assert total.unreleased_legacy_reservations == 0
