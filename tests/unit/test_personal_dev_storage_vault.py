"""Fixed-name Kubernetes Secrets must never cross storage incarnations."""

import base64
import json
from dataclasses import replace
from uuid import uuid4

import pytest
import yaml

from loom.dev_instance import derive_identity
from loom.dev_instance_runtime import (
    CommandResult,
    DevInstanceRuntimeError,
    KubectlClient,
    KubectlMinioTenantProvisioner,
    KubectlSecretVault,
    instance_database_url,
)
from loom.personal_dev_capacity_identity import PROTECTED_WORKER_RUNTIME_SECRET_NAME
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim

_JSON = "storage-binding.json"
_SHA = "storage-binding.sha256"
_ANN_JSON = "loom.dev/storage-binding"
_ANN_SHA = "loom.dev/storage-binding-sha256"
_ADMIN = "postgresql://admin:fixture-only@database.example:5433/postgres?sslmode=require"
_PASSWORD = "b" * 32


class _Cluster:
    def __init__(self):
        self.namespace = None
        self.secrets = {}
        self.secret_metadata = {}
        self.secret_immutable = {}
        self.version = 0
        self.writes = []

    async def run(self, argv, *, stdin=None, timeout_seconds=120):
        if "get" in argv:
            index = argv.index("get")
            if argv[index + 1] == "namespace":
                result = self.namespace or {}
            else:
                data = self.secrets.get(argv[index + 2])
                name = argv[index + 2]
                result = {} if data is None else {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
                    "metadata": self.secret_metadata.get(name, {}),
                    "immutable": self.secret_immutable.get(name, False), "data": {
                    key: base64.b64encode(value).decode() for key, value in data.items()
                }}
            return CommandResult(json.dumps(result), "")
        self.writes.append((argv, stdin))
        for document in yaml.safe_load_all(stdin or ""):
            if document["kind"] == "Namespace":
                if "create" in argv and self.namespace is not None:
                    raise DevInstanceRuntimeError("namespace already exists")
                self.namespace = document
                self.namespace["metadata"]["uid"] = "fixture-namespace-uid"
            else:
                name = document["metadata"]["name"]
                if "create" in argv and name in self.secrets:
                    raise DevInstanceRuntimeError("secret already exists")
                old = self.secret_metadata.get(name)
                if "replace" in argv and (name not in self.secrets or old is None or any(
                    document["metadata"].get(key) != old.get(key) for key in ("uid", "resourceVersion")
                )):
                    raise DevInstanceRuntimeError("secret update was superseded")
                self.secrets[document["metadata"]["name"]] = (
                    {key: value.encode() for key, value in document["stringData"].items()}
                    if "stringData" in document else
                    {key: base64.b64decode(value) for key, value in document["data"].items()}
                )
                self.version += 1
                self.secret_metadata[name] = {**document["metadata"],
                                              "uid": old["uid"] if old else str(uuid4()),
                                              "resourceVersion": str(self.version)}
                self.secret_immutable[name] = document.get("immutable", False)
                document = {**document, "metadata": self.secret_metadata[name]}
        return CommandResult(json.dumps(document), "")


def _vault(cluster):
    return KubectlSecretVault(KubectlClient("kubectl", runner=cluster), _ADMIN,
                             protected_worker_runtime=True)


async def test_bound_vault_writes_one_canonical_binding_on_all_secrets_and_namespace():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(cluster)
    await vault.store(identity, _PASSWORD)
    expected = canonical_bytes(identity.storage_binding)
    digest = canonical_digest(identity.storage_binding)
    assert set(cluster.secrets) == {"loom-secrets", "loom-admin-secret", PROTECTED_WORKER_RUNTIME_SECRET_NAME}
    for data in cluster.secrets.values():
        assert data[_JSON] == expected
        assert data[_SHA] == digest.encode()
    assert cluster.namespace["metadata"]["annotations"][_ANN_JSON] == expected.decode()
    assert cluster.namespace["metadata"]["annotations"][_ANN_SHA] == digest
    assert cluster.secrets["loom-secrets"]["minio-access-key"].decode() == identity.storage_binding.object_store_identity[0]
    assert KubectlMinioTenantProvisioner._names(identity) == identity.storage_binding.object_store_identity
    fresh = _vault(cluster)
    assert await fresh.database_password(identity) == _PASSWORD
    assert await fresh.admin_token(identity) == await vault.admin_token(identity)
    assert await fresh.object_credentials(identity) == await vault.object_credentials(identity)


@pytest.mark.parametrize("method", ("database_password", "admin_token", "object_credentials", "store"))
@pytest.mark.parametrize("secret_name", ("loom-secrets", "loom-admin-secret", PROTECTED_WORKER_RUNTIME_SECRET_NAME))
async def test_bound_vault_checks_all_secret_bindings_even_after_cache_fill(method, secret_name):
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(cluster)
    await vault.store(identity, _PASSWORD)
    cluster.secrets[secret_name][_SHA] = b"0" * 64
    before = list(cluster.writes)
    with pytest.raises(DevInstanceRuntimeError):
        await getattr(vault, method)(identity, *([_PASSWORD] if method == "store" else []))
    assert cluster.writes == before


@pytest.mark.parametrize("key,value", (
    ("cp-db-url", instance_database_url(_ADMIN, derive_identity("alice"), _PASSWORD)),
    ("svc-db-url", "postgresql://intruder:password@elsewhere/other"),
    ("gw-db-url", "postgresql://intruder:password@elsewhere/other"),
    ("minio-access-key", "loomdev-alice"),
))
async def test_bound_vault_rejects_wrong_physical_credential_targets_before_sql(key, value):
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(cluster)
    await vault.store(identity, _PASSWORD)
    cluster.secrets["loom-secrets"][key] = value.encode()
    with pytest.raises(DevInstanceRuntimeError):
        await vault.database_password(identity)


@pytest.mark.parametrize("method", ("database_password", "admin_token", "object_credentials", "store"))
async def test_same_name_new_incarnation_cannot_reuse_cached_or_persisted_credentials(method):
    cluster = _Cluster()
    binding = _bound_claim().operation.storage_binding
    vault = _vault(cluster)
    await vault.store(binding.identity, _PASSWORD)
    new_identity = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    with pytest.raises(DevInstanceRuntimeError):
        await getattr(vault, method)(new_identity, *([_PASSWORD] if method == "store" else []))


@pytest.mark.parametrize("method", ("database_password", "admin_token", "object_credentials", "store"))
async def test_bound_vault_rejects_namespace_provenance_loss_with_cached_credentials(method):
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(cluster)
    await vault.store(identity, _PASSWORD)
    cluster.namespace["metadata"]["annotations"] = {}
    with pytest.raises(DevInstanceRuntimeError):
        await getattr(vault, method)(identity, *([_PASSWORD] if method == "store" else []))


async def test_bound_vault_never_adopts_an_existing_unbound_namespace():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    cluster.namespace = {"metadata": {"name": identity.namespace, "uid": "legacy-uid"}}
    with pytest.raises(DevInstanceRuntimeError):
        await _vault(cluster).store(identity, _PASSWORD)
    assert cluster.writes == []


async def test_partial_identity_is_rejected_before_any_credential_write():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    with pytest.raises(ValueError, match="storage"):
        await _vault(cluster).store(replace(identity, storage_binding=None), _PASSWORD)
    assert cluster.writes == []


async def test_new_incarnation_gets_disjoint_cache_values_after_old_namespace_is_removed():
    cluster = _Cluster()
    binding = _bound_claim().operation.storage_binding
    vault = _vault(cluster)
    await vault.store(binding.identity, _PASSWORD)
    old_token = await vault.admin_token(binding.identity)
    cluster.namespace = None
    cluster.secrets.clear()
    new_identity = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    assert await vault.database_password(new_identity) is None
    await vault.store(new_identity, "c" * 32)
    assert await vault.database_password(new_identity) == "c" * 32
    assert await vault.admin_token(new_identity) != old_token
    assert (await vault.object_credentials(new_identity))[0] != binding.object_store_identity[0]


@pytest.mark.parametrize("method", ("database_password", "admin_token", "object_credentials", "store"))
async def test_legacy_vault_cannot_reinterpret_bound_secrets(method):
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    with pytest.raises(DevInstanceRuntimeError):
        await getattr(_vault(cluster), method)(derive_identity(identity.name), *([_PASSWORD] if method == "store" else []))


async def test_namespace_creation_race_never_attaches_binding_to_concurrent_namespace():
    class RacingCluster(_Cluster):
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "create" in argv:
                self.namespace = {"metadata": {"name": "loom-dev-alice", "uid": "concurrent"}}
                raise DevInstanceRuntimeError("namespace already exists")
            return await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    cluster = RacingCluster()
    identity = _bound_claim().operation.storage_binding.identity
    with pytest.raises(DevInstanceRuntimeError):
        await _vault(cluster).store(identity, _PASSWORD)
    assert cluster.secrets == {}
    assert "annotations" not in cluster.namespace["metadata"]


@pytest.mark.parametrize("method", ("database_password", "admin_token", "object_credentials"))
async def test_cached_legacy_identity_is_rejected_after_namespace_becomes_bound(method):
    cluster = _Cluster()
    legacy = derive_identity("alice")
    old_vault = _vault(cluster)
    await old_vault.store(legacy, _PASSWORD)
    await getattr(old_vault, method)(legacy)
    cluster.namespace = None
    cluster.secrets.clear()
    await _vault(cluster).store(_bound_claim().operation.storage_binding.identity, "c" * 32)
    with pytest.raises(DevInstanceRuntimeError):
        await getattr(old_vault, method)(legacy)


@pytest.mark.parametrize("fail_after", ("loom-secrets", "loom-admin-secret", PROTECTED_WORKER_RUNTIME_SECRET_NAME))
async def test_partial_bound_secret_write_recovers_without_rotating_persisted_material(fail_after):
    class InterruptedCluster(_Cluster):
        interrupted = False

        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if stdin and not self.interrupted:
                documents = list(yaml.safe_load_all(stdin))
                for index, document in enumerate(documents):
                    if document["metadata"]["name"] == fail_after:
                        self.interrupted = True
                        await super().run(argv, stdin=yaml.safe_dump_all(documents[:index + 1]),
                                          timeout_seconds=timeout_seconds)
                        raise DevInstanceRuntimeError("transport lost after server persistence")
            return await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    cluster = InterruptedCluster()
    identity = _bound_claim().operation.storage_binding.identity
    with pytest.raises(DevInstanceRuntimeError):
        await _vault(cluster).store(identity, _PASSWORD)
    persisted = {name: dict(data) for name, data in cluster.secrets.items()}
    retry = _vault(cluster)
    assert await retry.database_password(identity) == _PASSWORD
    await retry.store(identity, _PASSWORD)
    assert all(cluster.secrets[name] == data for name, data in persisted.items())
    assert await retry.admin_token(identity)
    assert await retry.object_credentials(identity)


async def test_partial_recovery_with_mismatched_password_has_no_side_effects():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    del cluster.secrets["loom-admin-secret"]
    before = list(cluster.writes)
    with pytest.raises(ValueError, match="password binding changed"):
        await _vault(cluster).store(identity, "c" * 32)
    assert cluster.writes == before
    assert "loom-admin-secret" not in cluster.secrets


async def test_partial_recovery_rejects_malformed_main_credentials_before_mutation():
    cluster = _Cluster()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    del cluster.secrets["loom-admin-secret"]
    cluster.secrets["loom-secrets"]["minio-secret-key"] = b"\xff"
    before = list(cluster.writes)
    with pytest.raises(DevInstanceRuntimeError):
        await _vault(cluster).store(identity, _PASSWORD)
    assert cluster.writes == before
