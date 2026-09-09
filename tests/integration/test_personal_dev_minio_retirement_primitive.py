"""Pinned IAM primitives; wiring retirement still requires separate tests."""

import asyncio
import base64
import io
import json
import shlex

import pytest
from minio import Minio
from minio.error import S3Error

from loom.dev_instance_runtime import KubectlClient, KubectlMinioTenantProvisioner
from tests.integration.test_personal_dev_storage_minio import (
    _MinioRunner,
    pinned_minio,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _PASSWORD, _Cluster, _vault


async def test_iam_deny_survives_credential_updates_and_concurrent_allow_attachment(pinned_minio):  # noqa: F811
    server, client_image = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    kubectl = KubectlClient("kubectl", runner=_MinioRunner(server, client_image))
    provisioner = KubectlMinioTenantProvisioner(kubectl, vault)
    admin = server.get_client()
    admin.make_bucket(identity.task_bucket)
    await provisioner.converge(identity)
    access, secret = await vault.object_credentials(identity)
    tenant = Minio(server.get_config()["endpoint"], access_key=access, secret_key=secret, secure=False)
    tenant.put_object(identity.task_bucket, "retained", io.BytesIO(b"owned"), 5)
    prelude = 'export MC_HOST_fixture="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"\n'

    async def execute(script, stdin=None):
        await kubectl.exec_stdin(namespace="loom-dev", pod="loom-dev-minio-0", container="admin",
                                 script=prelude + script, stdin=stdin)

    deny = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Action": ["s3:*"], "Resource": ["*"]}]}
    payload = base64.b64encode(json.dumps(deny).encode()).decode()
    await execute(
        "umask 077\npolicy_file=$(mktemp)\ntrap 'rm -f -- \"$policy_file\"' EXIT HUP INT TERM\n"
        f'printf %s {shlex.quote(payload)} | base64 -d >"$policy_file"\n'
        'mc admin policy create fixture loom-retired-fixture "$policy_file" >/dev/null'
    )

    async def attach_deny():
        await execute(f"mc admin policy attach fixture loom-retired-fixture --user {shlex.quote(access)} >/dev/null")

    async def update_credentials_and_allow():
        await execute(
            "IFS= read -r access_key\nIFS= read -r secret_key\n"
            'printf "%s\\n%s\\n" "$access_key" "$secret_key" | mc admin user add fixture >/dev/null\n'
            f'mc admin policy attach fixture {shlex.quote(access)} --user "$access_key" >/dev/null',
            stdin=f"{access}\n{secret}\n",
        )

    await attach_deny()
    with pytest.raises(S3Error) as denied:
        tenant.put_object(identity.task_bucket, "after-deny", io.BytesIO(b"bad"), 3)
    assert denied.value.code == "AccessDenied"
    await update_credentials_and_allow()
    with pytest.raises(S3Error) as after_update:
        tenant.put_object(identity.task_bucket, "after-update", io.BytesIO(b"bad"), 3)
    assert after_update.value.code == "AccessDenied"
    await asyncio.gather(attach_deny(), update_credentials_and_allow())
    with pytest.raises(S3Error) as after_concurrent:
        tenant.get_object(identity.task_bucket, "retained")
    assert after_concurrent.value.code == "AccessDenied"
    assert admin.stat_object(identity.task_bucket, "retained").size == 5
