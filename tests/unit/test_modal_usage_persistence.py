"""modal_compute_record — wraps calc_modal_cost into a CloudComputeRecord."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4


def test_modal_record_tagged_modal() -> None:
    from loom_drivers.modal.usage import modal_compute_record

    start = datetime(2026, 6, 9, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=60)
    rec = modal_compute_record(
        team_id=uuid4(),
        trial_id=uuid4(),
        sandbox_id="sb-modal-1",
        image="python:3.12-slim",
        started_at=start,
        stopped_at=end,
        cpu=1.0,
        memory_mb=1024,
        gpu=None,
    )
    assert rec.cloud_provider == "modal"
    assert rec.compute_seconds == 60.0
    assert rec.cost_usd > Decimal("0")
    # CPU-only @ 1 vCPU + 1 GiB for 60s should be well under $0.01
    assert rec.cost_usd < Decimal("0.01")


def test_modal_record_gpu_cost_higher_than_cpu_only() -> None:
    from loom_drivers.modal.usage import modal_compute_record

    start = datetime(2026, 6, 9, 12, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=60)
    base_kwargs = {
        "team_id": uuid4(),
        "trial_id": uuid4(),
        "sandbox_id": "sb-x",
        "image": "img",
        "started_at": start,
        "stopped_at": end,
        "cpu": 1.0,
        "memory_mb": 1024,
    }
    cpu_only = modal_compute_record(gpu=None, **base_kwargs)
    h100 = modal_compute_record(gpu="H100", **base_kwargs)
    assert h100.cost_usd > cpu_only.cost_usd * 20
