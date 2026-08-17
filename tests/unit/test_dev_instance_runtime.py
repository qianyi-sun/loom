from __future__ import annotations

import base64
import json
from typing import Any
from uuid import UUID

import httpx
import pytest
import yaml

from loom.dev_instance import RequestedPolicy, derive_identity
from loom.dev_instance_manifest import DevInstanceManifestConfig, PersonalDevManifestBinding
from loom.dev_instance_runtime import (
    CommandResult,
    HttpControlPlanePolicyRegistrar,
    KubectlCandidateGenerationProvisioner,
    KubectlClient,
    KubectlClusterProvisioner,
    KubectlMinioTenantProvisioner,
    KubectlSecretVault,
    S3BucketEnsurer,
    instance_database_url,
)

_SHA = "a1b2c3d" + "0" * 33


def _manifest_config() -> DevInstanceManifestConfig:
    return DevInstanceManifestConfig(
        image_tag="dev-a1b2c3d",
        candidate_sha=_SHA,
        deployment_generation=3,
        container_registry="registry.example/loom",
        minio_endpoint="https://minio.example",
    )


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str | None]] = []

    async def run(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> CommandResult:
        del timeout_seconds
        self.calls.append((argv, stdin))
        return CommandResult(stdout="{}", stderr="")


def test_instance_database_url_replaces_only_instance_identity() -> None:
    url = instance_database_url(
        "postgresql+psycopg://admin:old@db.internal:5433/postgres?sslmode=require",
        derive_identity("alice-smith"),
        "a" * 20,
    )
    assert url == (
        "postgresql+psycopg://loom_dev_alice_smith:aaaaaaaaaaaaaaaaaaaa@"
        "db.internal:5433/loom_dev_alice_smith?sslmode=require"
    )


async def test_kubectl_vault_sends_secrets_only_over_stdin() -> None:
    runner = _Runner()
    kubectl = KubectlClient("/usr/local/bin/kubectl", runner=runner)
    vault = KubectlSecretVault(
        kubectl=kubectl,
        database_admin_url="postgresql://admin:fixture-secret@db.internal/postgres",
        manifest_config=_manifest_config(),
    )

    ref = await vault.store(derive_identity("alice"), "b" * 20)

    assert ref == "k8s-secret://loom-dev-alice/loom-secrets"
    assert len(runner.calls) == 4
    flattened_argv = " ".join(part for argv, _stdin in runner.calls for part in argv)
    assert "fixture-secret" not in flattened_argv
    assert "minio-secret" not in flattened_argv
    secret_input = runner.calls[-1][1]
    assert secret_input is not None
    assert "fixture-secret" not in secret_input  # admin credential is never copied
    assert "loomdev-alice" in secret_input
    assert "bbbbbbbbbbbbbbbbbbbb" in secret_input
    assert await vault.admin_token(derive_identity("alice"))


class _ExistingSecretRunner(_Runner):
    async def run(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> CommandResult:
        del timeout_seconds
        self.calls.append((argv, stdin))
        if "namespace" in argv:
            return CommandResult(stdout='{"metadata":{"name":"loom-dev-alice"}}', stderr="")
        name = argv[argv.index("secret") + 1]
        values = (
            {
                "cp-db-url": ("postgresql://loom_dev_alice:bbbbbbbbbbbbbbbbbbbb@db/loom_dev_alice"),
                "svc-db-url": "svc-url",
                "gw-db-url": "gw-url",
                "step-jwt-signing-key": "step-key",
                "minio-access-key": "loomdev-alice",
                "minio-secret-key": "object-secret",
                "secret-store-master-key": "master-key",
            }
            if name == "loom-secrets"
            else {"secrets.toml": '[admin]\ntoken = "loom_admin_existing"\n'}
        )
        data = {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}
        return CommandResult(stdout=json.dumps({"data": data}), stderr="")


async def test_kubectl_vault_treats_absent_namespace_as_absent_secret() -> None:
    runner = _Runner()
    vault = KubectlSecretVault(
        kubectl=KubectlClient("kubectl", runner=runner),
        database_admin_url="postgresql://admin:fixture-secret@db/postgres",
    )

    assert await vault.database_password(derive_identity("alice")) is None
    assert len(runner.calls) == 1
    assert "namespace" in runner.calls[0][0]
    assert "secret" not in runner.calls[0][0]


async def test_kubectl_vault_reuses_existing_generation_secrets_without_rotation() -> None:
    runner = _ExistingSecretRunner()
    vault = KubectlSecretVault(
        kubectl=KubectlClient("kubectl", runner=runner),
        database_admin_url="postgresql://admin:fixture-secret@db/postgres",
        manifest_config=_manifest_config(),
    )
    identity = derive_identity("alice")

    assert await vault.database_password(identity) == "b" * 20
    assert await vault.store(identity, "b" * 20) == ("k8s-secret://loom-dev-alice/loom-secrets")
    assert all("apply" not in argv for argv, _stdin in runner.calls)
    assert await vault.admin_token(identity) == "loom_admin_existing"

    with pytest.raises(ValueError, match="password binding"):
        await vault.store(identity, "c" * 20)


async def test_minio_tenant_is_bucket_scoped_and_secrets_use_stdin_only() -> None:
    runner = _Runner()
    kubectl = KubectlClient("kubectl", runner=runner)
    vault = KubectlSecretVault(
        kubectl=kubectl,
        database_admin_url="postgresql://admin:fixture-secret@db.internal/postgres",
        manifest_config=_manifest_config(),
    )
    identity = derive_identity("alice")
    await vault.store(identity, "b" * 20)
    access_key, secret_key = await vault.object_credentials(identity)

    tenant = KubectlMinioTenantProvisioner(kubectl=kubectl, vault=vault)
    await tenant.converge(identity)

    argv, tenant_input = runner.calls[-1]
    assert access_key not in " ".join(argv[:-1])
    assert secret_key not in " ".join(argv)
    assert tenant_input == f"{access_key}\n{secret_key}\n"
    policy = json.dumps(tenant._policy(identity))
    assert "loom-dev-alice-artifacts" in policy
    assert "loom-dev-alice-trajectories" in policy
    assert "loom-dev-alice-tasks" in policy


async def test_cluster_executor_applies_migration_then_runtime_and_waits() -> None:
    runner = _Runner()
    provisioner = KubectlClusterProvisioner(
        kubectl=KubectlClient("kubectl", context="dev-fleet", runner=runner),
        base_manifest_config=_manifest_config(),
    )
    identity = derive_identity("alice")

    await provisioner.deploy(
        identity,
        deployment_generation=3,
        candidate_sha=_SHA,
    )

    assert runner.calls[0][0][-4:] == ["--field-manager", "loom-dev-instance", "-f", "-"]
    assert "kind: Job" in (runner.calls[0][1] or "")
    assert "loom-migrate-a1b2c3d-g3" in " ".join(runner.calls[1][0])
    assert "kind: Deployment" in (runner.calls[2][1] or "")
    rollout_commands = [argv for argv, _stdin in runner.calls if "rollout" in argv]
    assert len(rollout_commands) == 4
    assert all("fixture-secret" not in " ".join(argv) for argv, _stdin in runner.calls)


class _CandidateRunner(_Runner):
    async def run(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> CommandResult:
        del timeout_seconds
        self.calls.append((argv, stdin))
        if "get" in argv and "deployment" in argv:
            name = argv[argv.index("deployment") + 1]
            component = {
                "loom-control-plane-g8": "control-plane",
                "loom-llm-gateway-g8": "llm-gateway",
                "loom-service-g8": "service",
                "loom-web-g8": "web",
            }[name]
            binding = _personal_manifest_config().lifecycle_binding
            assert binding is not None
            labels = {
                "loom.dev/subject": str(binding.subject_id),
                "loom.dev/incarnation": str(binding.subject_incarnation),
                "loom.dev/operation": str(binding.operation_id),
                "loom.dev/attempt": str(binding.attempt_id),
                "loom.dev/operation-epoch": str(binding.operation_epoch),
                "loom.dev/generation": "8",
            }
            return CommandResult(
                stdout=json.dumps(
                    {
                        "metadata": {
                            "uid": f"uid-{name}",
                            "resourceVersion": "17",
                            "generation": 1,
                            "labels": labels,
                        },
                        "spec": {
                            "replicas": 1,
                            "template": {
                                "metadata": {"labels": labels},
                                "spec": {
                                    "containers": [
                                        {"image": _personal_manifest_config().image(component)}
                                    ]
                                },
                            },
                        },
                        "status": {
                            "observedGeneration": 1,
                            "availableReplicas": 1,
                            "updatedReplicas": 1,
                        },
                    }
                ),
                stderr="",
            )
        return CommandResult(stdout="{}", stderr="")


def _personal_manifest_config() -> DevInstanceManifestConfig:
    from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS

    return DevInstanceManifestConfig(
        image_tag="",
        candidate_sha="b" * 64,
        deployment_generation=8,
        container_registry="",
        minio_endpoint="https://minio.example",
        image_references={
            component: f"registry.example/loom-{component}@sha256:{index:064x}"
            for index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1)
        },
        lifecycle_binding=PersonalDevManifestBinding(
            subject_id=UUID("00000000-0000-0000-0000-000000000001"),
            subject_incarnation=UUID("00000000-0000-0000-0000-000000000002"),
            operation_id=UUID("00000000-0000-0000-0000-000000000003"),
            attempt_id=UUID("00000000-0000-0000-0000-000000000004"),
            operation_epoch=5,
        ),
    )


async def test_candidate_generation_prepares_exact_images_without_switching_routes() -> None:
    runner = _CandidateRunner()
    provisioner = KubectlCandidateGenerationProvisioner(
        kubectl=KubectlClient("kubectl", runner=runner),
    )

    observation = await provisioner.prepare(
        derive_identity("alice"),
        _personal_manifest_config(),
    )

    applied = "\n".join(stdin or "" for _argv, stdin in runner.calls)
    assert "kind: Ingress" not in applied
    assert "metadata:\n  name: loom-service\n" not in applied
    assert "name: loom-service-g8" in applied
    assert observation.deployed_images["web"] == _personal_manifest_config().image("web")
    assert len(observation.resource_evidence_sha256) == 64
    rollout_commands = [argv for argv, _stdin in runner.calls if "rollout" in argv]
    assert len(rollout_commands) == 4


async def test_candidate_generation_binds_manager_before_waiting_for_migration() -> None:
    runner = _CandidateRunner()
    provisioner = KubectlCandidateGenerationProvisioner(
        kubectl=KubectlClient("kubectl", runner=runner),
    )

    await provisioner.prepare(derive_identity("alice"), _personal_manifest_config())

    events: list[str] = []
    for argv, stdin in runner.calls:
        if "apply" in argv:
            for document in yaml.safe_load_all(stdin or ""):
                identity = (document["kind"], document["metadata"]["name"])
                if identity == ("RoleBinding", "loom-personal-dev-management"):
                    events.append("management-binding")
                elif document["kind"] == "Job":
                    events.append("migration-apply")
        elif "wait" in argv:
            events.append("migration-wait")
    assert events[:3] == [
        "management-binding",
        "migration-apply",
        "migration-wait",
    ]


class _S3:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.created: list[str] = []
        self.deleted: list[str] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        if Bucket not in self.buckets:
            raise RuntimeError("missing")

    def create_bucket(self, **kwargs: Any) -> None:
        bucket = str(kwargs["Bucket"])
        self.buckets.add(bucket)
        self.created.append(bucket)

    def get_paginator(self, name: str) -> Any:
        assert name in {
            "list_multipart_uploads",
            "list_objects_v2",
            "list_object_versions",
        }

        class _Paginator:
            def paginate(self, *, Bucket: str):  # noqa: N803
                del Bucket
                return [{}]

        return _Paginator()

    def delete_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self.buckets.remove(Bucket)
        self.deleted.append(Bucket)

    def delete_objects(self, **_kwargs: Any) -> dict[str, object]:
        return {}

    def abort_multipart_upload(self, **_kwargs: Any) -> None:
        return None


async def test_bucket_executor_is_idempotent() -> None:
    client = _S3()
    executor = S3BucketEnsurer(client)
    identity = derive_identity("alice")
    buckets = [identity.task_bucket, identity.artifacts_bucket]

    await executor.ensure_buckets(identity, buckets)
    await executor.ensure_buckets(identity, buckets)
    assert client.created == buckets
    await executor.remove_buckets(identity, buckets)
    assert client.deleted == buckets


async def test_bucket_cleanup_removes_uploads_unversioned_objects_and_versions() -> None:
    class _PopulatedS3(_S3):
        def __init__(self) -> None:
            super().__init__()
            self.aborted: list[tuple[str, str, str]] = []
            self.object_deletes: list[list[dict[str, str]]] = []

        def get_paginator(self, name: str) -> Any:
            class _Paginator:
                def paginate(self, *, Bucket: str):  # noqa: N803
                    if name == "list_multipart_uploads":
                        return [{"Uploads": [{"Key": "partial", "UploadId": "upload-1"}]}]
                    if name == "list_objects_v2":
                        return [{"Contents": [{"Key": "current"}]}]
                    assert name == "list_object_versions"
                    return [
                        {
                            "Versions": [{"Key": "history", "VersionId": "v1"}],
                            "DeleteMarkers": [{"Key": "history", "VersionId": "marker"}],
                        }
                    ]

            return _Paginator()

        def abort_multipart_upload(
            self,
            *,
            Bucket: str,  # noqa: N803
            Key: str,  # noqa: N803
            UploadId: str,  # noqa: N803
        ) -> None:
            self.aborted.append((Bucket, Key, UploadId))

        def delete_objects(self, **kwargs: Any) -> dict[str, object]:
            self.object_deletes.append(kwargs["Delete"]["Objects"])
            return {"Errors": []}

    client = _PopulatedS3()
    identity = derive_identity("alice")
    client.buckets.add(identity.task_bucket)

    await S3BucketEnsurer(client).remove_buckets(identity, [identity.task_bucket])

    assert client.aborted == [(identity.task_bucket, "partial", "upload-1")]
    assert client.object_deletes == [
        [{"Key": "current"}],
        [
            {"Key": "history", "VersionId": "v1"},
            {"Key": "history", "VersionId": "marker"},
        ],
    ]
    assert client.deleted == [identity.task_bucket]


class _Vault:
    async def admin_token(self, identity) -> str:
        return "loom_admin_" + "x" * 32


async def test_policy_registrar_uses_instance_cp_and_drains_before_delete() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"].startswith("Bearer loom_admin_")
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "policies": [
                        {
                            "last_actual_slots": 0,
                            "last_pending_slots": 0,
                            "last_draining_slots": 0,
                        }
                    ]
                },
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={})

    registrar = HttpControlPlanePolicyRegistrar(
        vault=_Vault(),  # type: ignore[arg-type]
        control_plane_url_template=("http://loom-control-plane.{namespace}.svc.cluster.local:8080"),
        actuator_config={"candidate_sha": _SHA, "sbatch_path": "/usr/bin/sbatch"},
        transport=httpx.MockTransport(handler),
    )
    identity = derive_identity("alice")
    await registrar.upsert_dev_policy(
        identity,
        RequestedPolicy(actuator="slurm", min_slots=0, max_slots=2),
    )
    await registrar.drop_dev_policy(identity)

    assert [request.method for request in requests] == [
        "PUT",
        "PUT",
        "GET",
        "DELETE",
        "DELETE",
    ]
    first = json.loads(requests[0].content)
    assert first["actuator_config"]["external_runner"] is True
    assert first["max_slots"] == 2
    drain = json.loads(requests[1].content)
    assert drain["enabled"] is True
    assert drain["max_slots"] == 0
    assert requests[-2].url.path == "/admin/worker-tokens"
    assert requests[-1].url.host == "loom-control-plane.loom-dev-alice.svc.cluster.local"
