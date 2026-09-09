"""A new reconciliation attempt cannot alter immutable execution identity."""

from dataclasses import replace
from uuid import uuid4

import pytest
import yaml

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


@pytest.mark.parametrize("consumer", ("candidate", "capacity"))
async def test_bound_workload_support_is_installed_before_activation(monkeypatch, consumer):
    from loom.dev_instance_runtime import KubectlCandidateGenerationProvisioner
    from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller

    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    config = _immutable_config()
    config = replace(config, lifecycle_binding=replace(
        config.lifecycle_binding, subject_id=identity.storage_binding.subject_id,
        subject_incarnation=identity.storage_incarnation,
    ))
    events = []

    class Cluster:
        async def apply(self, payload):
            events.extend(item["kind"] for item in yaml.safe_load_all(payload))

        async def wait_job(self, *_args):
            pass

        async def wait_deployment(self, *_args):
            pass

    async def writer(kubectl, supplied_identity, document, *, operation_epoch):
        assert supplied_identity == identity
        events.append("activate-" + document["kind"])

    async def observe(*_args):
        return None

    monkeypatch.setattr("loom.personal_dev_storage_workload_write.write_storage_workload", writer)
    monkeypatch.setattr("loom.dev_instance_runtime.observe_personal_dev_candidate_generation", observe)
    if consumer == "candidate":
        await KubectlCandidateGenerationProvisioner(Cluster()).prepare(identity, config)
    else:
        await KubectlPersonalDevCapacityInstaller(kubectl=Cluster(), database=None, config=None)._apply_manifests(
            claim, identity, ({"kind": "Deployment"}, {"kind": "NetworkPolicy"}),
        )
    assert events.index("NetworkPolicy") < next(index for index, event in enumerate(events) if event.startswith("activate-"))
