"""One validated storage-binding deserializer for every management registry view."""

import json

from loom.db.schema import DevInstance, DevLifecycleOperation
from loom.personal_dev_incarnation_storage import (
    PersonalDevStorageBindingV1,
    parse_personal_dev_storage_binding,
)


def personal_dev_storage_record(
    row: DevInstance | DevLifecycleOperation,
) -> PersonalDevStorageBindingV1 | None:
    if row.storage_binding is None:
        if row.storage_binding_sha256 is not None:
            raise ValueError("personal storage binding is incomplete")
        return None
    binding = parse_personal_dev_storage_binding(
        json.dumps(row.storage_binding, sort_keys=True, separators=(",", ":")).encode("ascii"),
        expected_sha256=row.storage_binding_sha256 or "",
    )
    name = row.name if isinstance(row, DevInstance) else row.environment_name
    if (
        binding.environment_name != name
        or binding.subject_id != row.subject_id
        or binding.subject_incarnation != row.subject_incarnation
        or binding.owner_user_id != row.owner_user_id
        or binding.owner_team_id != row.owner_team_id
        or binding.layout != "incarnation-v1"
    ):
        raise ValueError("personal storage binding differs from durable ownership")
    return binding
