"""Protected capacity consumers retain the same immutable storage identity."""

from dataclasses import replace

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
