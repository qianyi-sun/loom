"""Pure re-export preserves own-event anchors through empty successor epochs."""

import json
from importlib import import_module
from types import SimpleNamespace

import pytest

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4, PersonalMembershipSnapshotV2
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_capacity_successor_preparation_origins import preparation_payload


def empty_history(preparation, *, epoch):
    snapshot = PersonalMembershipSnapshotV2(namespace_id=preparation.personal_membership.namespace_id,
        revision=0, head_sha256="0" * 64)
    return SimpleNamespace(preparation=preparation, events=(), results=(),
        epoch=SimpleNamespace(execution_epoch=epoch, execution_manifest_sha256=canonical_executable_digest(preparation)),
        snapshot=lambda: snapshot)


def test_reexport_keeps_all_members_and_real_anchor_through_repeated_empty_epochs():
    preparation = ExecutionPreparationV4.model_validate_json(json.dumps(preparation_payload()))
    initial = {origin.configuration.subject_id: origin for origin in (*preparation.managed_application_origins,
        *preparation.managed_build_origins)}
    module = import_module("loom_capacity_manager.retired_member_export")
    for epoch in (43, 44, 45):
        result = module._origins_from_authenticated_history(empty_history(preparation, epoch=epoch))
        assert result.source.execution_epoch == epoch
        assert result.source.revision == 0
        assert len(result.applications) == 2 and len(result.builds) == 1
        for origin in (*result.applications, *result.builds):
            old = initial[origin.configuration.subject_id]
            if hasattr(origin, "inherited"):
                assert origin.inherited.source == result.source
                assert origin.inherited.anchor == old.inherited.anchor
                assert origin.inherited.original_origin == old.inherited.original_origin
                assert origin.installation_projection == old.installation_projection
            else:
                assert origin == old
        preparation = ExecutionPreparationV4.model_validate(preparation.model_dump(mode="python") | {
            "retired_source": result.source, "managed_application_origins": result.applications,
            "managed_build_origins": result.builds, "configuration_epoch": preparation.configuration_epoch + 1})


@pytest.mark.parametrize("epoch", (41, 42))
def test_reexport_cannot_create_non_descending_inheritance(epoch):
    preparation = ExecutionPreparationV4.model_validate_json(json.dumps(preparation_payload()))
    module = import_module("loom_capacity_manager.retired_member_export")
    with pytest.raises(ValueError, match="descend"):
        module._origins_from_authenticated_history(empty_history(preparation, epoch=epoch))
