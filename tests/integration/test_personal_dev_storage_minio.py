"""Exercise production static-IAM tenant scripts against release-pinned MinIO."""

import io
import json
from pathlib import Path
from uuid import uuid4

import pytest
from minio import Minio
from minio.error import S3Error
from testcontainers.core.container import DockerContainer
from testcontainers.minio import MinioContainer

from loom.dev_instance_runtime import (
    AsyncCommandRunner,
    KubectlClient,
    KubectlMinioTenantProvisioner,
)
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from tests.unit.test_personal_dev_storage_vault import _PASSWORD, _Cluster, _vault


class _MinioRunner:
    """Replace only kubectl transport; execute the exact production shell/stdin."""

    def __init__(self, server, client_container):
        self.server = server
        self.client_container = client_container

    async def run(self, argv, *, stdin=None, timeout_seconds=60):
        assert "exec" in argv and argv[-4:-1] == ["/bin/sh", "-eu", "-c"]
        return await AsyncCommandRunner().run([
            "docker", "exec", "-i", self.client_container.get_wrapped_container().id,
            "/bin/sh", "-eu", "-c", argv[-1],
        ], stdin=stdin, timeout_seconds=timeout_seconds)


@pytest.fixture
def pinned_minio():
    images = json.loads((Path(__file__).parents[2] / "deploy/dev-fleet/personal-dev-external-images.json").read_text())["images"]
    with MinioContainer(image=images["minio"]["reference"]) as server:
        # Production execs in a long-lived admin container. Preserve exact
        # script/stdin semantics without cold-starting one container per read.
        client = DockerContainer(images["minio_client"]["reference"]).with_kwargs(
            entrypoint="/bin/sh", network_mode=f"container:{server.get_wrapped_container().id}",
        ).with_env("MINIO_ROOT_USER", "minioadmin").with_env("MINIO_ROOT_PASSWORD", "minioadmin").with_command(["-c", "sleep 86400"])
        with client:
            yield server, client


async def test_full_length_incarnation_tenants_isolate_owners_and_stale_cleanup(pinned_minio):
    server, client_container = pinned_minio
    first = PersonalDevStorageBindingV1(
        layout="incarnation-v1", environment_name="a" * 20,
        subject_id=uuid4(), subject_incarnation=uuid4(), owner_user_id=uuid4(), owner_team_id=uuid4(),
    )
    successor = first.model_copy(update={"subject_incarnation": uuid4()})
    other = first.model_copy(update={"environment_name": "b" * 20, "subject_id": uuid4(),
                                    "subject_incarnation": uuid4(), "owner_user_id": uuid4(), "owner_team_id": uuid4()})
    admin = server.get_client()
    tenants = []
    for binding in (first, successor, other):
        cluster = _Cluster()
        vault = _vault(cluster)
        identity = binding.identity
        await vault.store(identity, _PASSWORD)
        provisioner = KubectlMinioTenantProvisioner(
            KubectlClient("kubectl", runner=_MinioRunner(server, client_container)), vault,
        )
        for bucket in (identity.task_bucket, identity.trajectories_bucket, identity.artifacts_bucket):
            admin.make_bucket(bucket)
        await provisioner.converge(identity)
        access, secret = await vault.object_credentials(identity)
        assert len(access) == 56
        client = Minio(server.get_config()["endpoint"], access_key=access, secret_key=secret, secure=False)
        client.put_object(identity.task_bucket, "probe", io.BytesIO(b"owned"), 5)
        tenants.append((identity, provisioner, client))

    for identity, _, client in tenants:
        for foreign, _, _ in tenants:
            if foreign != identity:
                with pytest.raises(S3Error) as denied:
                    client.put_object(foreign.task_bucket, "intrusion", io.BytesIO(b"bad"), 3)
                assert denied.value.code == "AccessDenied"
                with pytest.raises(S3Error):
                    client.get_object(foreign.task_bucket, "probe")

    old_identity, old_provisioner, old_client = tenants[0]
    await old_provisioner.delete(old_identity)
    await old_provisioner.delete(old_identity)
    with pytest.raises(S3Error):
        old_client.put_object(old_identity.task_bucket, "after-delete", io.BytesIO(b"bad"), 3)
    # Tenant deletion revokes credentials; it does not remove retained data or
    # the same-name new incarnation's user/policy.
    assert admin.stat_object(old_identity.task_bucket, "probe").size == 5
    for identity, provisioner, client in tenants[1:]:
        await provisioner.converge(identity)
        client.put_object(identity.task_bucket, "after-old-cleanup", io.BytesIO(b"safe"), 4)
