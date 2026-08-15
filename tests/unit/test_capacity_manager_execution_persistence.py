from __future__ import annotations

from loom_capacity_manager.models import CapacityAuthorityState


def test_capacity_authority_persists_the_exact_execution_epoch_fence() -> None:
    """Prepared execution must be durable without granting executable capacity."""

    columns = CapacityAuthorityState.__table__.columns
    assert {
        "execution_epoch",
        "execution_state",
        "execution_manifest_sha256",
    } <= set(columns.keys())
    assert str(columns["execution_epoch"].server_default.arg) == "0"
    assert str(columns["execution_state"].server_default.arg) == "'shadow'"
