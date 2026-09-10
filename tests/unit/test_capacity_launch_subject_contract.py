"""Bounded transport of exact manager-resolved launch facts."""

import json
from importlib import import_module

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
