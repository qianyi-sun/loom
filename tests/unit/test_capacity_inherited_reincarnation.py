"""Cross-epoch recreation keeps the real retired predecessor event coordinates."""

import copy
import json
from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
from tests.unit.test_capacity_successor_member_origins import successor_payload


def inherited_evidence_payload(*, build=True, empty_source=False):
    origin = successor_payload(build=build, operation="destroy")
    inherited = origin["inherited"]
    source = copy.deepcopy(inherited["source"])
    if empty_source:
        source.update(execution_epoch=43, execution_manifest_sha256="d" * 64, revision=0, head_sha256="0" * 64)
    return dict(schema_version=2, namespace_id=source["namespace_id"],
        execution_epoch=source["execution_epoch"] + 1, execution_manifest_sha256="c" * 64,
        source=source, origin=inherited["original_origin"], predecessor=origin["configuration"],
        predecessor_execution_epoch=inherited["anchor"]["execution_epoch"],
        predecessor_execution_manifest_sha256=inherited["anchor"]["execution_manifest_sha256"],
        predecessor_revision=inherited["anchor"]["revision"], predecessor_head_sha256=inherited["anchor"]["head_sha256"],
        admission_revision=1, successor_incarnation=str(UUID(int=99700)), release_set_sha256="f" * 64)


def parse(payload):
    module = import_module("loom_capacity_manager.inherited_reincarnation_contracts")
    return module.PersonalInheritedReincarnationEvidenceV2.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("empty_source", (False, True))
def test_first_successor_admission_preserves_real_source_revision(build, empty_source):
    payload = inherited_evidence_payload(build=build, empty_source=empty_source)
    value = parse(payload)
    assert value.admission_revision == 1 < value.predecessor_revision
    assert value.model_dump(mode="json") == payload
    assert value.predecessor_execution_epoch <= value.source.execution_epoch < value.execution_epoch
    if empty_source:
        assert value.source.revision == 0 and value.predecessor_revision == 2


@pytest.mark.parametrize("boundary", (
    "float-version", "wrong-namespace", "current-source-epoch", "future-own-epoch",
    "own-manifest", "own-head", "own-revision", "active-predecessor", "zero-release",
    "zero-root", "root-subject", "same-incarnation", "zero-own-manifest",
))
def test_inherited_evidence_rejects_inconsistent_epoch_or_predecessor(boundary):
    payload = inherited_evidence_payload()
    if boundary == "float-version":
        payload["schema_version"] = 2.0
    elif boundary == "wrong-namespace":
        payload["namespace_id"] = str(UUID(int=99701))
    elif boundary == "current-source-epoch":
        payload["execution_epoch"] = payload["source"]["execution_epoch"]
    elif boundary == "future-own-epoch":
        payload["predecessor_execution_epoch"] = payload["source"]["execution_epoch"] + 1
    elif boundary == "own-manifest":
        payload["predecessor_execution_manifest_sha256"] = "e" * 64
    elif boundary == "own-head":
        payload["predecessor_head_sha256"] = "e" * 64
    elif boundary == "own-revision":
        payload["predecessor_revision"] += 1
    elif boundary == "active-predecessor":
        payload["predecessor"]["lifecycle_state"] = "active"
    elif boundary == "zero-release":
        payload["release_set_sha256"] = "0" * 64
    elif boundary == "zero-root":
        payload["origin"]["digest"] = "0" * 64
    elif boundary == "root-subject":
        payload["origin"]["subject_id"] = str(UUID(int=99702))
    elif boundary == "same-incarnation":
        payload["successor_incarnation"] = payload["predecessor"]["subject_incarnation"]
    else:
        payload["predecessor_execution_manifest_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        parse(payload)


def test_legacy_certificate_cannot_relabel_source_revision_as_local():
    payload = inherited_evidence_payload()
    for field in ("source", "execution_epoch", "predecessor_execution_epoch", "predecessor_execution_manifest_sha256"):
        del payload[field]
    payload["schema_version"] = 1
    with pytest.raises(ValueError, match="predecessor binding"):
        PersonalReincarnationEvidenceV1.model_validate_json(json.dumps(payload))


def test_legacy_evidence_type_rejects_new_python_instance():
    payload = inherited_evidence_payload()
    payload["admission_revision"] = 3
    value = parse(payload)
    with pytest.raises(ValueError):
        PersonalReincarnationEvidenceV1.model_validate(value)


def test_legacy_serialization_cannot_truncate_new_evidence_fields():
    from pydantic import TypeAdapter
    from pydantic_core import PydanticSerializationError
    value = parse(inherited_evidence_payload())
    with pytest.raises(PydanticSerializationError):
        TypeAdapter(PersonalReincarnationEvidenceV1).dump_json(value)
    assert value.model_dump(mode="json")["source"] == inherited_evidence_payload()["source"]


@pytest.mark.parametrize("field", ("digest", "subject_incarnation"))
def test_new_evidence_equal_generation_root_is_exact(field):
    payload = inherited_evidence_payload()
    from loom_capacity_manager.contracts import SubjectConfigurationV1, canonical_digest
    predecessor = SubjectConfigurationV1.model_validate_json(json.dumps(payload["predecessor"]))
    payload["origin"].update(generation=predecessor.configuration_generation,
        digest=canonical_digest(predecessor), subject_incarnation=str(predecessor.subject_incarnation))
    parse(payload)
    payload["origin"][field] = "e" * 64 if field == "digest" else str(UUID(int=99702))
    with pytest.raises(ValueError):
        parse(payload)
