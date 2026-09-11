"""Bounded transport of exact manager-resolved launch facts."""

import json
from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import UUID

import pytest

from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def response(*, large=False):
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    context = typed_context()
    value = module.ExecutableLaunchSubjectV3(
        binding=context.binding,
        configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement,
        authority=context.subject.authority,
    )
    if not large:
        return value
    from loom_capacity_manager.contracts import canonical_digest

    profile = value.configuration.profiles[0]
    shapes = tuple(
        sorted(
            (
                *profile.worker_shapes,
                *(
                    profile.worker_shapes[0].model_copy(update={"shape_id": f"extra-{index:04d}"})
                    for index in range(200)
                ),
            ),
            key=lambda shape: shape.shape_id,
        )
    )
    configuration = value.configuration.model_copy(
        update={
            "profiles": (
                profile.model_copy(update={"worker_shapes": shapes}),
                *value.configuration.profiles[1:],
            )
        }
    )
    authority = value.authority.model_copy(
        update={
            "configuration": value.authority.configuration.model_copy(
                update={"digest": canonical_digest(configuration)}
            )
        }
    )
    return module.ExecutableLaunchSubjectV3(
        binding=value.binding,
        configuration=configuration,
        acknowledgement=value.acknowledgement,
        authority=authority,
    )


def test_launch_subject_round_trip_preserves_full_facts():
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    value = response()
    encoded = module.canonical_launch_subject_bytes(value)
    assert module.parse_launch_subject(encoded) == value
    assert value.authority.purpose == "personal-build-worker"


@pytest.mark.parametrize(
    "field",
    ("candidate", "account", "configuration", "acknowledgement", "profile", "nodes", "schema"),
)
def test_launch_subject_rejects_mismatched_or_noncanonical_facts(field):
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    value = response().model_dump(mode="json")
    if field == "candidate":
        value["binding"]["candidate"]["publication_sha256"] = "f" * 64
    elif field == "account":
        value["binding"]["account_id"] = "dev-owner-foreign"
    elif field == "configuration":
        value["configuration"]["configuration_generation"] += 1
    elif field == "acknowledgement":
        value["acknowledgement"]["acknowledgement_sha256"] = "f" * 64
    elif field == "profile":
        value["binding"]["profile_generation"] += 1
    elif field == "nodes":
        value["binding"]["node_ids"].append("oldlab-6")
    else:
        value["configuration"]["schema_version"] = 1.0
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    with pytest.raises(ValueError):
        module.parse_launch_subject(encoded)


def test_launch_subject_rejects_oversize_and_noncanonical_transport():
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    value = response()
    for payload in (b" " * (1024 * 1024), value.model_dump_json(indent=2).encode("ascii")):
        with pytest.raises(ValueError):
            module.parse_launch_subject(payload)


def current_application_observation():
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    from loom_capacity_manager.executable_contracts import ExecutableLaunchPermitV2
    context = typed_context(purpose="application-worker")
    subject = module.ExecutableLaunchSubjectV3(
        binding=context.binding, configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement, authority=context.subject.authority,
    )
    now = datetime.now(UTC)
    return module.CurrentApplicationAllocationV3(
        subject=subject,
        permit=ExecutableLaunchPermitV2(
            binding=subject.binding, permit_id=UUID(int=991), permit_epoch=1,
            launch_rank=1, expires_at=now + timedelta(seconds=15),
            bootstrap_registration_epoch=1, bootstrap_evidence_sha256="8" * 64,
        ),
        permit_consumed_at=now - timedelta(seconds=1), observed_at=now,
        expires_at=now + timedelta(seconds=10),
    )


def test_current_application_observation_round_trip():
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    value = current_application_observation()
    assert module.parse_current_application_allocation(module.canonical_current_application_allocation_bytes(value)) == value
    assert value.executable is False


@pytest.mark.parametrize("changed", ("build-purpose", "binding", "schema-alias", "executable-alias", "expired", "overlong", "consumption-after-expiry", "naive"))
def test_current_application_observation_rejects_invalid_evidence(changed):
    module = import_module("loom_capacity_manager.launch_subject_contracts")
    original = current_application_observation()
    value = original.model_dump(mode="json")
    if changed == "build-purpose":
        value["subject"] = response().model_dump(mode="json")
        value["permit"]["binding"] = value["subject"]["binding"]
    elif changed == "binding":
        value["permit"]["binding"]["intent_id"] = str(UUID(int=993))
    elif changed == "schema-alias":
        value["schema_version"] = 3.0
    elif changed == "executable-alias":
        value["executable"] = 0
    elif changed == "expired":
        value["expires_at"] = value["observed_at"]
    elif changed == "overlong":
        value["expires_at"] = (original.observed_at + timedelta(seconds=11)).isoformat()
    elif changed == "consumption-after-expiry":
        value["permit"]["expires_at"] = value["permit_consumed_at"]
    else:
        value["observed_at"] = original.observed_at.replace(tzinfo=None).isoformat()
    with pytest.raises(ValueError):
        module.CurrentApplicationAllocationV3.model_validate_json(json.dumps(value))
