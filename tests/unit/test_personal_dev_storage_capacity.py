"""Protected capacity consumers retain the same immutable storage identity."""

from dataclasses import fields, replace
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import KubectlClient
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller, PersonalDevCapacityInstallationError
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_personal_dev_reconciler import _installation
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _Cluster, _PASSWORD, _vault


@pytest.mark.parametrize("method", ("converge", "verify_publishing", "seal", "destroy"))
async def test_capacity_entrypoints_resolve_bound_storage(method):
    claim = _bound_claim()
    seen = []

    class Database:
        async def seal(self, identity):
            seen.append(identity)

        async def destroy(self, identity):
            seen.append(identity)

    class Installer(KubectlPersonalDevCapacityInstaller):
        async def _credentials(self, claim, identity):
            seen.append(identity)
            raise RuntimeError("stop before write")

        async def _assert_installed_credentials(self, claim, installation, identity):
            seen.append(identity)
            raise RuntimeError("stop before write")

    installer = Installer(kubectl=None, database=Database(), config=None)
    if method in ("seal", "destroy"):
        claim = replace(claim, operation=replace(claim.operation, kind="destroy"))
        await getattr(installer, method)(claim)
    else:
        with pytest.raises(RuntimeError, match="stop before"):
            await getattr(installer, method)(claim, *([_installation()] if method == "verify_publishing" else []))
    assert seen == [claim.operation.storage_binding.identity]


@pytest.mark.parametrize("secret_name", ("loom-protected-worker-runtime", "loom-capacity-agent-credentials"))
async def test_capacity_credentials_and_seed_pin_full_storage_binding(secret_name):
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    cluster = _Cluster()
    await _vault(cluster).store(identity, _PASSWORD)
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=cluster), database=None, config=None,
    )
    credentials = await installer._credentials(claim, identity)
    await installer._persist_credentials(claim, identity, credentials)
    seed = cluster.secrets["loom-capacity-agent-credentials"]
    assert seed["storage-binding.json"] == canonical_bytes(identity.storage_binding)
    assert seed["storage-binding.sha256"] == canonical_digest(identity.storage_binding).encode()
    assert (await installer._credentials(claim, identity)).reporter_token == credentials.reporter_token
    cluster.secrets[secret_name]["storage-binding.sha256"] = b"0" * 64
    before = list(cluster.writes)
    with pytest.raises(PersonalDevCapacityInstallationError):
        await installer._credentials(claim, identity)
    assert cluster.writes == before


@pytest.mark.parametrize("tamper", (None, "database", "namespace", "subject_id", "subject_incarnation", "secret"))
async def test_status_uses_bound_observer_role_and_fences_mixed_coordinates(monkeypatch, tamper):
    from loom.personal_dev_capacity_runtime import PersonalDevCapacityStatusReader
    from loom.personal_dev_capacity_identity import capacity_role_names

    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    cluster = _Cluster()
    await _vault(cluster).store(identity, _PASSWORD)
    kubectl = KubectlClient("kubectl", runner=cluster)
    installer = KubectlPersonalDevCapacityInstaller(kubectl=kubectl, database=None, config=None)
    credentials = await installer._credentials(claim, identity)
    await installer._persist_credentials(claim, identity, credentials)
    arguments = dict(namespace=identity.namespace, database=identity.database,
                     subject_id=claim.operation.subject_id, subject_incarnation=claim.operation.subject_incarnation,
                     deployment_generation=1, storage_binding=identity.storage_binding)
    if tamper in ("database", "namespace"):
        arguments[tamper] = "other-resource"
    elif tamper in ("subject_id", "subject_incarnation"):
        arguments[tamper] = uuid4()
    elif tamper == "secret":
        cluster.secrets["loom-capacity-agent-credentials"]["storage-binding.sha256"] = b"0" * 64

    class Projector:
        async def subject_status(self, **kwargs):
            return SimpleNamespace(checkpoint=SimpleNamespace(execution_state="active"),
                                   active_bindings=(SimpleNamespace(intent_id=uuid4()),))

    connections = []

    async def connect(url):
        connections.append(url)
        raise RuntimeError("stop before live connection")

    monkeypatch.setattr("loom.personal_dev_capacity_runtime.psycopg.AsyncConnection.connect", connect)
    result = await PersonalDevCapacityStatusReader(
        kubectl=kubectl, database_admin_url="postgresql://admin:fixture@db.example/postgres", projector=Projector(),
    ).read(**arguments)
    assert not result.worker_available
    if tamper:
        assert connections == []
    else:
        assert len(connections) == 1
        assert urlsplit(connections[0]).username == capacity_role_names(identity)[4]
        assert urlsplit(connections[0]).path == "/" + identity.database


async def test_owner_status_passes_persisted_binding_to_reader():
    from loom.dev_instance_provisioner import DevInstanceRecord
    from loom.personal_dev_capacity import PersonalDevCapacityAvailability
    from loom_service.routes.dev_instances import _enriched_response

    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    values = {field.name: getattr(claim.environment, field.name) for field in fields(DevInstanceRecord)
              if hasattr(claim.environment, field.name)}
    record = DevInstanceRecord(**{**values, "capacity_namespace": identity.namespace, "capacity_database": identity.database,
                                  "storage_binding": identity.storage_binding})
    calls = []

    class Reader:
        async def read(self, **kwargs):
            calls.append(kwargs)
            return PersonalDevCapacityAvailability("waiting", True, False)

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(personal_dev_capacity_status_reader=Reader())))
    await _enriched_response(request, record)
    assert calls[0]["storage_binding"] == identity.storage_binding


def test_generic_owner_store_preserves_and_validates_bound_storage():
    from loom.db.schema import DevInstance
    from loom.dev_instance_store import _record

    claim = _bound_claim()
    binding = claim.operation.storage_binding
    row = DevInstance(**{f.name: getattr(claim.environment, f.name) for f in fields(claim.environment)
                         if f.name in DevInstance.__table__.columns and f.name != "storage_binding"})
    row.storage_binding = binding.model_dump(mode="json")
    row.storage_binding_sha256 = canonical_digest(binding)
    assert _record(row).storage_binding == binding
    row.storage_binding_sha256 = "0" * 64
    with pytest.raises(ValueError, match="storage"):
        _record(row)
