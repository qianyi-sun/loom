"""Inherited provenance does not rewrite epoch-local recreation certificates."""

import json
from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import ConfigurationGenerationRefV1, canonical_digest
from tests.unit.test_capacity_typed_membership_events import event_row


def origin_payload(*, build=True):
    _value, _request, result, row = event_row(build=build)
    config = result.member.configuration
    root = ConfigurationGenerationRefV1(scope="subject", subject_id=config.subject_id,
        subject_incarnation=config.subject_incarnation, generation=config.configuration_generation,
        digest=canonical_digest(config))
    return {
        "schema_version": 1,
        "source": {"schema_version": 1, "namespace_id": str(row.namespace_id),
            "execution_epoch": row.execution_epoch, "execution_manifest_sha256": row.execution_manifest_sha256,
            "revision": row.revision + 1, "head_sha256": "e" * 64},
        "anchor": {"schema_version": 1, "execution_epoch": row.execution_epoch,
            "execution_manifest_sha256": row.execution_manifest_sha256,
            "revision": row.revision, "head_sha256": row.head_sha256,
            "member": result.member.model_dump(mode="json")},
        "original_origin": root.model_dump(mode="json"),
    }


def parse(payload):
    module = import_module("loom_capacity_manager.retired_member_origin_contracts")
    return module.RetiredPersonalMemberOriginV1.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("build", (False, True))
def test_retired_origin_separates_member_event_from_global_final_head(build):
    payload = origin_payload(build=build)
    value = parse(payload)
    assert value.anchor.revision < value.source.revision
    assert value.anchor.head_sha256 != value.source.head_sha256
    assert value.anchor.member.purpose == ("personal-build-worker" if build else "personal-application")
    assert value.model_dump(mode="json") == payload


@pytest.mark.parametrize("build", (False, True))
def test_untouched_member_can_keep_real_anchor_through_an_empty_retired_epoch(build):
    payload = origin_payload(build=build)
    payload["source"].update(execution_epoch=payload["source"]["execution_epoch"] + 1,
        execution_manifest_sha256="d" * 64, revision=0, head_sha256="0" * 64)
    value = parse(payload)
    assert value.source.revision == 0 and value.anchor.revision == 1
    assert value.anchor.execution_epoch < value.source.execution_epoch


@pytest.mark.parametrize("boundary", (
    "future-anchor", "member-revision", "same-epoch-manifest", "future-revision", "same-revision-head",
    "empty-head", "zero-namespace", "zero-root-incarnation", "root-subject", "root-generation", "root-digest",
    "float-version", "float-anchor-version", "foreign-build-namespace",
))
def test_retired_origin_rejects_inconsistent_structural_claims(boundary):
    payload = origin_payload()
    source, anchor, root = payload["source"], payload["anchor"], payload["original_origin"]
    if boundary == "future-anchor":
        anchor["execution_epoch"] = source["execution_epoch"] + 1
    elif boundary == "member-revision":
        anchor["member"]["revision"] += 1
    elif boundary == "same-epoch-manifest":
        anchor["execution_manifest_sha256"] = "c" * 64
    elif boundary == "future-revision":
        source["revision"] = 0
        source["head_sha256"] = "0" * 64
    elif boundary == "same-revision-head":
        source["revision"] = anchor["revision"]
    elif boundary == "empty-head":
        source["head_sha256"] = "0" * 64
    elif boundary == "zero-namespace":
        source["namespace_id"] = str(UUID(int=0))
    elif boundary == "foreign-build-namespace":
        source["namespace_id"] = str(UUID(int=999))
    elif boundary == "zero-root-incarnation":
        root["subject_incarnation"] = str(UUID(int=0))
    elif boundary == "root-subject":
        root["subject_id"] = str(UUID(int=999))
    elif boundary == "root-generation":
        root["generation"] += 1
    elif boundary == "root-digest":
        root["digest"] = "f" * 64
    elif boundary == "float-version":
        payload["schema_version"] = 1.0
    else:
        anchor["schema_version"] = 1.0
    with pytest.raises(ValueError):
        parse(payload)
