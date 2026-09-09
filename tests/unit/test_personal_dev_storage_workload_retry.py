"""A new reconciliation attempt cannot alter immutable execution identity."""

from dataclasses import replace
from uuid import uuid4

from loom.dev_instance_manifest import dev_instance_manifest_documents
from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


def test_bound_retry_keeps_immutable_workload_fields_but_updates_attempt_evidence():
    identity = _bound_claim().operation.storage_binding.identity
    config = _immutable_config()
    config = replace(config, lifecycle_binding=replace(
        config.lifecycle_binding, subject_id=identity.storage_binding.subject_id,
        subject_incarnation=identity.storage_incarnation,
    ))
    retry = replace(config, lifecycle_binding=replace(config.lifecycle_binding, attempt_id=uuid4()))
    first = dev_instance_manifest_documents(identity, config)
    second = dev_instance_manifest_documents(identity, retry)
    checked = set()
    for old, new in zip(first, second, strict=True):
        if old["kind"] not in {"Deployment", "Job"}:
            continue
        checked.add(old["kind"])
        assert new["metadata"]["labels"]["loom.dev/attempt"] == str(retry.lifecycle_binding.attempt_id)
        assert old["metadata"]["labels"]["loom.dev/attempt"] != new["metadata"]["labels"]["loom.dev/attempt"]
        if old["kind"] == "Deployment":
            assert old["spec"]["selector"] == new["spec"]["selector"]
            assert new["spec"]["template"]["metadata"]["labels"]["loom.dev/attempt"] == str(retry.lifecycle_binding.attempt_id)
        else:
            assert old["spec"]["template"] == new["spec"]["template"]
    assert checked == {"Job", "Deployment"}
