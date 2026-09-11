"""Schema-facing persistence contract for personal membership lifecycle state."""

from loom.db.schema import DevInstance, DevLifecycleOperation
from loom.personal_dev_environment import PersonalDevCapacityMode


def test_membership_storage_columns_default_to_shadow_mode() -> None:
    operation = DevLifecycleOperation.__table__.columns
    environment = DevInstance.__table__.columns

    assert operation.capacity_mode.server_default.arg.text == "'shadow-v1'"
    assert operation.capacity_membership_envelope.nullable is True
    assert environment.accepted_capacity_mode.server_default.arg.text == "'shadow-v1'"
    assert environment.accepted_capacity_membership_checkpoint.nullable is True


def test_capacity_mode_type_supports_only_versioned_modes() -> None:
    shadow: PersonalDevCapacityMode = "shadow-v1"
    membership: PersonalDevCapacityMode = "membership-v1"
    assert {shadow, membership} == {"shadow-v1", "membership-v1"}
