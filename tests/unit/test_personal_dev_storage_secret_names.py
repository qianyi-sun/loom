"""Personal workload credentials must never alias across namespace lifetimes."""

import json
from dataclasses import replace
from importlib import import_module
from uuid import uuid4

import pytest

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import dev_instance_manifest_documents
from tests.unit.test_dev_instance_manifest import _config, _immutable_config
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _Cluster, _vault

_PURPOSES = (
    "loom-secrets", "loom-admin-secret", "loom-protected-worker-runtime",
    "loom-capacity-agent", "loom-capacity-agent-credentials",
)


@pytest.mark.parametrize("purpose", _PURPOSES)
def test_secret_names_preserve_legacy_and_use_full_bound_incarnation(purpose):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    binding = _bound_claim().operation.storage_binding
    first = binding.identity
    successor = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    assert resolve(derive_identity(first.name), purpose) == purpose
    assert resolve(first, purpose) == f"{purpose}-{binding.subject_incarnation.hex}"
    assert resolve(first, purpose) != resolve(successor, purpose)
    # Secret names use the 253-character DNS-subdomain bound, not the
    # 63-character label/volume-name bound. Logical volume names stay stable.
    assert 1 <= len(resolve(first, purpose)) <= 253
    assert first.namespace == successor.namespace == f"loom-dev-{first.name}"


@pytest.mark.parametrize("defect", ("missing_binding", "wrong_database", "wrong_owner_name"))
def test_secret_name_rejects_noncanonical_identity(defect):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    identity = _bound_claim().operation.storage_binding.identity
    if defect == "missing_binding":
        identity = replace(identity, storage_binding=None)
    elif defect == "wrong_database":
        identity = replace(identity, database="forged")
    else:
        identity = replace(identity, name="another")
    with pytest.raises(ValueError):
        resolve(identity, "loom-secrets")


@pytest.mark.parametrize("purpose", ("", "foreign", "loom-secrets-other", "../loom-secrets"))
def test_secret_names_reject_unknown_purposes(purpose):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    with pytest.raises(ValueError):
        resolve(_bound_claim().operation.storage_binding.identity, purpose)


@pytest.mark.parametrize("protected", (False, True))
def test_bound_manifest_mounts_only_its_own_incarnation_credentials(protected):
    identity = _bound_claim().operation.storage_binding.identity
    config = _immutable_config() if protected else _config()
    if protected:
        config = replace(config, lifecycle_binding=replace(
            config.lifecycle_binding,
            subject_id=identity.storage_binding.subject_id,
            subject_incarnation=identity.storage_incarnation,
        ))
    documents = dev_instance_manifest_documents(identity, config)
    references = set()
    for document in documents:
        if document["kind"] not in ("Deployment", "Job"):
            continue
        pod = document["spec"]["template"]["spec"]
        for volume in pod.get("volumes", []):
            if "secret" in volume:
                references.add(volume["secret"]["secretName"])
                assert len(volume["name"]) <= 63
        for container in (*pod.get("containers", []), *pod.get("initContainers", [])):
            for variable in container.get("env", []):
                reference = variable.get("valueFrom", {}).get("secretKeyRef")
                if reference:
                    references.add(reference["name"])
    assert references == {
        f"{name}-{identity.storage_incarnation.hex}"
        for name in (
            "loom-secrets", "loom-admin-secret",
            *(("loom-protected-worker-runtime",) if protected else ()),
        )
    }


async def test_bound_vault_stages_incarnation_named_credentials_and_returns_exact_ref():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(cluster)
    secret_ref = await vault.store(identity, "b" * 32)
    expected = {
        f"{name}-{identity.storage_incarnation.hex}"
        for name in ("loom-secrets", "loom-admin-secret", "loom-protected-worker-runtime")
    }
    assert set(cluster.secrets) == expected
    assert secret_ref == f"k8s-secret://{identity.namespace}/loom-secrets-{identity.storage_incarnation.hex}"
    for argv, payload in cluster.writes:
        if "create" not in argv:
            continue
        # Namespace creates use YAML; staged Secret writes are JSON.
        if payload.lstrip().startswith("{"):
            document = json.loads(payload)
            if document["kind"] == "Secret":
                assert document.get("data", {}) == {}
                assert not document.get("stringData")
    before = dict(cluster.secrets)
    assert await _vault(cluster).store(identity, "b" * 32) == secret_ref
    assert cluster.secrets == before


def test_storage_namespace_pins_secret_suffix_without_parsing_canonical_json_in_cel():
    from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations

    identity = _bound_claim().operation.storage_binding.identity
    assert personal_dev_storage_annotations(identity)["loom.dev/storage-incarnation"] == identity.storage_incarnation.hex
    assert personal_dev_storage_annotations(derive_identity(identity.name)) == {}
