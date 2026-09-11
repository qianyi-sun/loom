"""Successor source authentication cannot silently omit another owner's work."""

from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.store import ConfigurationConflictError
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_retired_application_import import retire
from tests.integration.test_capacity_retired_member_export import export
from tests.integration.test_capacity_typed_managed_base_history import prepared


async def successor(session, *, empty=False, resized=False):
    management, preparation, _fleet, execution = await (prepared if empty else typed_sql_execution)(session)
    if not empty:
        await apply(session, application_request(preparation, execution))
        request = build_request(preparation, execution, owner=88011, revision=1)
        await apply(session, request, key=980001)
        if resized:
            await apply(session, transition(request, "capacity", revision=2), key=980002)
    snapshot = await CapacityTypedMembershipStore().snapshot(session, execution.execution_epoch)
    await retire(session, management, preparation, execution)
    exported = await export(session, execution, snapshot)
    origins = (*exported.applications, *exported.builds)
    identities = tuple(origin.configuration.subject_id for origin in origins)
    candidate = ExecutionPreparationV4.model_validate(preparation.model_dump(mode="python") | {
        "configuration_epoch": preparation.configuration_epoch + 1,
        "retired_source": exported.source,
        "managed_application_origins": exported.applications, "managed_build_origins": exported.builds,
        "personal_membership": preparation.personal_membership.model_copy(update={"managed_base_subject_ids": identities}),
        "subject_acknowledgements": tuple(ack for ack in preparation.subject_acknowledgements if ack.subject_id not in identities)
            + tuple(origin.acknowledgement for origin in origins),
    })
    return candidate, exported


async def verify(session, candidate, *, epoch=43):
    module = import_module("loom_capacity_manager.successor_source_verification")
    return await module.verify_successor_source(session, candidate, execution_epoch=epoch)


@pytest.mark.parametrize("empty", (False, True))
async def test_exact_successor_authenticates_complete_source_even_without_events(capacity_session, empty):
    candidate, exported = await successor(capacity_session, empty=empty)
    assert await verify(capacity_session, candidate) == exported


@pytest.mark.parametrize("omission", ("application", "build", "all"))
async def test_self_consistent_successor_cannot_omit_any_source_members(capacity_session, omission):
    candidate, _exported = await successor(capacity_session)
    apps = () if omission in {"application", "all"} else candidate.managed_application_origins
    builds = () if omission in {"build", "all"} else candidate.managed_build_origins
    kept = {origin.configuration.subject_id for origin in (*apps, *builds)}
    removed = set(candidate.personal_membership.managed_base_subject_ids) - kept
    candidate = ExecutionPreparationV4.model_validate(candidate.model_dump(mode="python") | {
        "managed_application_origins": apps, "managed_build_origins": builds,
        "personal_membership": candidate.personal_membership.model_copy(update={"managed_base_subject_ids": tuple(kept)}),
        "subject_acknowledgements": tuple(ack for ack in candidate.subject_acknowledgements if ack.subject_id not in removed),
    })
    with pytest.raises(ConfigurationConflictError, match="complete"):
        await verify(capacity_session, candidate)


@pytest.mark.parametrize("boundary", ("root", "source-head", "future-epoch", "same-epoch", "authority", "configuration", "fleet", "release"))
async def test_source_verification_rejects_forged_or_non_descending_history(capacity_session, boundary):
    candidate, _exported = await successor(capacity_session, resized=boundary == "root")
    epoch = 43
    if boundary in {"future-epoch", "same-epoch"}:
        epoch = 41 if boundary == "future-epoch" else 42
    elif boundary == "authority":
        candidate = candidate.model_copy(update={"authority_incarnation": UUID(int=980002)})
    elif boundary == "configuration":
        candidate = candidate.model_copy(update={"configuration_epoch": candidate.configuration_epoch - 1})
    elif boundary == "fleet":
        candidate = candidate.model_copy(update={"fleet_digest": "f" * 64})
    elif boundary == "release":
        candidate = candidate.model_copy(update={"trusted_fleet_release_sha256": "f" * 64,
            "managed_build_origins": tuple(origin.model_copy(update={"trusted_fleet_release_sha256": "f" * 64})
                for origin in candidate.managed_build_origins)})
    elif boundary == "source-head":
        source = candidate.retired_source.model_copy(update={"head_sha256": "f" * 64,
            "revision": candidate.retired_source.revision + 1})
        candidate = candidate.model_copy(update={"retired_source": source,
            "managed_application_origins": tuple(origin.model_copy(update={"inherited": origin.inherited.model_copy(update={"source": source})}) for origin in candidate.managed_application_origins),
            "managed_build_origins": tuple(origin.model_copy(update={"inherited": origin.inherited.model_copy(update={"source": source})}) for origin in candidate.managed_build_origins)})
    else:
        origin = candidate.managed_build_origins[0]
        candidate = candidate.model_copy(update={"managed_build_origins": (origin.model_copy(update={"inherited":
            origin.inherited.model_copy(update={"original_origin": origin.inherited.original_origin.model_copy(update={"digest": "f" * 64})})}),)})
    # These forged claims must remain structurally valid so the database-backed
    # comparison, not only Pydantic consistency, detects the fabricated history.
    candidate = ExecutionPreparationV4.model_validate_json(candidate.model_dump_json())
    error = {"root": "complete retired source", "source-head": "final snapshot"}.get(boundary)
    with pytest.raises(ConfigurationConflictError, match=error):
        await verify(capacity_session, candidate, epoch=epoch)
