"""Management installation must not become another task-execution stack."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def management_inputs(platform_inputs):
    config, candidate, profile = platform_inputs
    candidate["source_ref"] = "refs/heads/dev"
    candidate.update(schema_version="loom.nebius-candidate.v1", workflow_path=".github/workflows/nebius-candidate.yml",
                     run_id=123, registry_prefix="cr.eu-north1.nebius.cloud/test")
    installation = {
        "schema_version": "loom.nebius-management-installation.v1",
        "foundation": foundation_from(config).model_dump(mode="json"),
        "registry_prefix": "cr.eu-north1.nebius.cloud/test",
        "keyring": {"schema_version": 1, "keys": []},
        "publications": [],
        "platform_budget": {"cpu_millis": 2000, "memory_mib": 6000,
                            "storage_mib": 30000, "ephemeral_storage_mib": 30000},
        "provider_runtime": {
            "concurrency": 4, "poll_seconds": 5,
            "kubernetes": {"endpoint": config["kubernetes_api_server"],
                           "ca_file": "/var/run/loom-management-kubernetes/ca.crt",
                           "credentials_file": "/var/run/loom-management-kubernetes/credentials.json"},
            "cloud_credentials_file": "/var/run/loom-management-cloud/credentials.json",
        },
    }
    installation["foundation"]["provisioning_project_id"] = "project-managed-storage"
    deployment = {
        "schema_version": "loom.nebius-management-deployment.v1",
        "installation_id": "30000000-0000-4000-8000-000000000001",
        "namespace": "loom-nebius-management",
        "public_host": "manage.example.com",
        "postgres_storage_gi": 10,
        "backup_bucket": "loom-management-backup",
        "installation": installation,
    }
    return deployment, candidate, profile


def render(inputs):
    from loom_service.environment_management.deployment import (
        ManagementDeployment,
        render_management,
    )

    config, candidate, profile = inputs
    return render_management(ManagementDeployment.model_validate(config), candidate=candidate,
                             profile=profile, repo_root=ROOT)


def documents(result):
    return [doc for docs in result.files.values() for doc in docs]


def pod(doc):
    spec = doc["spec"]
    if doc["kind"] == "CronJob":
        spec = spec["jobTemplate"]["spec"]
    return spec["template"]["spec"]


def test_manager_has_only_its_own_database_api_backup_and_shared_ingress(management_inputs):
    before = copy.deepcopy(management_inputs)
    result = render(management_inputs)
    docs = documents(result)
    assert [(d["kind"], d["metadata"]["name"]) for d in docs
            if d["kind"] in {"Deployment", "StatefulSet", "Namespace"}] == [
        ("Namespace", "loom-nebius-management"), ("StatefulSet", "loom-postgres"),
        ("Deployment", "loom-service"),
    ]
    assert all(d["metadata"].get("namespace", "loom-nebius-management") == "loom-nebius-management" for d in docs)
    assert not any(d["kind"] in {"Secret", "ClusterRole", "ClusterRoleBinding"} for d in docs)
    assert not any(d.get("spec", {}).get("type") == "LoadBalancer" for d in docs)
    assert len([d for d in docs if d["kind"] == "Job"]) == 1
    assert len([d for d in docs if d["kind"] == "CronJob"]) == 1
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "loom-shared"
    assert ingress["spec"]["rules"] == [{"host": "manage.example.com", "http": {"paths": [{
        "path": "/", "pathType": "Prefix", "backend": {"service": {"name": "loom-service", "port": {"number": 8090}}},
    }]}}]
    assert ingress["spec"]["tls"] == [{"hosts": ["manage.example.com"]}]
    assert management_inputs == before


def test_runtime_mounts_only_explicit_separate_authorities(management_inputs):
    docs = documents(render(management_inputs))
    service = next(d for d in docs if d["kind"] == "Deployment")
    template = pod(service)
    env = {row["name"]: row for row in template["containers"][0]["env"]}
    assert env["LOOM_SVC_SERVICE_MODE"]["value"] == "management"
    assert env["LOOM_SVC_AUTH_LOCAL_HTTP"]["value"] == "false"
    assert env["LOOM_SVC_PUBLIC_BASE_URL"]["value"] == "https://manage.example.com"
    assert env["LOOM_SVC_ENVIRONMENT_MANAGEMENT_CONFIG_FILE"]["value"] == "/var/run/loom-management/installation.json"
    assert env["LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "loom-management-publications", "key": "token",
    }
    assert env["LOOM_SVC_DB_URL"]["valueFrom"]["secretKeyRef"]["key"] == "service-url"
    assert not any("SOURCE" in name or "MINIO" in name or "BATCH_RUNNER" in name for name in env)
    assert template["automountServiceAccountToken"] is False
    volumes = {v["name"]: v for v in template["volumes"]}
    assert volumes["management-cloud"]["secret"]["secretName"] == "loom-management-cloud"
    assert volumes["management-kubernetes"]["secret"]["secretName"] == "loom-management-kubernetes"
    assert volumes["management-cloud"]["secret"]["defaultMode"] == 0o440
    assert template["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/api/v1/health/ready"
    for doc in docs:
        if doc["kind"] in {"StatefulSet", "Job", "CronJob"}:
            text = json.dumps(pod(doc))
            assert "loom-management-cloud" not in text
            assert "loom-management-kubernetes" not in text
            assert "loom-management-publications" not in text
            for volume in pod(doc).get("volumes", []):
                if volume.get("configMap", {}).get("name") == "loom-platform-config":
                    assert volume["configMap"]["items"] == [{"key": "environment.json", "path": "environment.json"}]


def test_migration_and_backup_never_start_task_authorities_or_share_child_data(management_inputs):
    docs = documents(render(management_inputs))
    job = next(d for d in docs if d["kind"] == "Job")
    container = pod(job)["containers"][0]
    assert container["command"] == ["python", "-m", "loom.nebius_platform_bootstrap", "management-database"]
    assert {e["name"] for e in container["env"]} == {"LOOM_PLATFORM_CONFIG", "LOOM_DB_URL", "LOOM_DB_SERVICE_PASSWORD"}
    db = next(d for d in docs if d["kind"] == "StatefulSet")
    assert db["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"] == "10Gi"
    assert db["spec"]["persistentVolumeClaimRetentionPolicy"] == {"whenDeleted": "Retain", "whenScaled": "Retain"}
    backup = next(d for d in docs if d["kind"] == "CronJob")
    backup_pod = pod(backup)
    assert backup["spec"]["concurrencyPolicy"] == "Forbid"
    assert backup_pod["initContainers"][0]["command"][0] == "pg_dump"
    assert all("PGPASSWORD" != e["name"] for e in backup_pod["containers"][0]["env"])
    assert not any("LOOM_BACKUP" in e["name"] for e in backup_pod["initContainers"][0]["env"])
    configmap = next(d for d in docs if d["kind"] == "ConfigMap")
    config = json.loads(configmap["data"]["environment.json"])
    assert config["namespace"] == "loom-nebius-management"
    assert config["buckets"]["backup"] == "loom-management-backup"
    assert json.loads(configmap["data"]["installation.json"]) == management_inputs[0]["installation"]


def test_only_shared_ingress_can_reach_management_and_database_is_namespace_local(management_inputs):
    docs = documents(render(management_inputs))
    policies = {d["metadata"]["name"]: d["spec"] for d in docs if d["kind"] == "NetworkPolicy"}
    assert policies["default-deny-ingress"]["ingress"] == []
    assert policies["management-api"]["ingress"] == [{"from": [{
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "loom-ingress"}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-ingress"}},
    }], "ports": [{"protocol": "TCP", "port": 8090}]}]
    assert policies["postgres-private"]["ingress"][0]["from"] == [{
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "loom-nebius-management"}},
    }]


def test_management_envelope_counts_rollout_migration_database_and_backup_scratch(management_inputs):
    result = render(management_inputs)
    # Service steady+surge (2*100), database100, migration100, backup max100.
    assert result.platform_envelope.cpu_millis == 500
    assert result.platform_envelope.memory_mib == 1280
    assert result.platform_envelope.storage_mib == 10240
    assert result.platform_envelope.ephemeral_storage_mib == 11264
    for doc in documents(result):
        if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}:
            p = pod(doc)
            assert p["nodeSelector"] == {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}
            assert p["automountServiceAccountToken"] is False
            for c in p.get("initContainers", []) + p["containers"]:
                assert int(c["resources"]["requests"]["ephemeral-storage"].removesuffix("Mi")) > 0


@pytest.mark.parametrize("path,value", [
    (("namespace",), "loom-nebius-platform"),
    (("public_host",), "alice.dev.example.com"),
    (("public_host",), "nebius.yylx.world"),
    (("backup_bucket",), "loom-integration-backup"),
    (("installation_id",), "00000000-0000-0000-0000-000000000000"),
    (("postgres_storage_gi",), True),
    (("installation", "provider_runtime"), None),
    (("installation", "provider_runtime", "cloud_credentials_file"), "/root/operator.json"),
    (("installation", "provider_runtime", "kubernetes", "endpoint"), "https://other.example.com"),
    (("installation", "provider_runtime", "kubernetes", "credentials_file"), "/root/kubeconfig"),
])
def test_rejects_shared_bindings_unmounted_credentials_and_wrong_cluster(management_inputs, path, value):
    data = management_inputs[0]
    for key in path[:-1]:
        data = data[key]
    data[path[-1]] = value
    with pytest.raises(ValueError):
        render(management_inputs)


def test_management_config_changes_roll_service_and_migration_together(management_inputs):
    first = render(management_inputs)
    management_inputs[0]["installation"]["platform_budget"]["cpu_millis"] += 100
    second = render(management_inputs)
    assert first.revision != second.revision
    for result in (first, second):
        for doc in documents(result):
            assert doc["metadata"]["labels"]["loom.nebius/management-installation"] == "30000000-0000-4000-8000-000000000001"
            if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}:
                spec = doc["spec"]["jobTemplate"]["spec"] if doc["kind"] == "CronJob" else doc["spec"]
                assert spec["template"]["metadata"]["annotations"]["loom.nebius/configuration-revision"] == result.revision


@pytest.mark.parametrize("source", ["refs/heads/feature/test", "refs/heads/codex/nebius-main"])
def test_manager_cannot_be_installed_from_personal_or_retired_source(management_inputs, source):
    management_inputs[1]["source_ref"] = source
    with pytest.raises(ValueError, match="protected dev"):
        render(management_inputs)


def test_management_material_is_fresh_and_does_not_include_worker_or_cloud_credentials():
    from loom_service.environment_management.credentials import generate_management_material

    first = generate_management_material(namespace="loom-nebius-management")
    second = generate_management_material(namespace="loom-nebius-management")
    assert set(first) == {"loom-platform-db", "loom-management-db-tls", "loom-platform-auth", "loom-admin-secret"}
    assert set(first["loom-platform-db"]) == {"ca.crt", "postgres-password", "admin-url", "service-password", "service-url"}
    assert set(first["loom-platform-auth"]) == {"secret-store-master-key"}
    assert first["loom-platform-auth"] != second["loom-platform-auth"]
    assert first["loom-admin-secret"] != second["loom-admin-secret"]
    assert first["loom-platform-db"]["service-password"] != second["loom-platform-db"]["service-password"]


@pytest.mark.parametrize("image", ["cr.eu-north1.nebius.cloud/test/service:dev",
                                  "cr.eu-north1.nebius.cloud/other/service@sha256:" + "b" * 64])
def test_management_image_requires_bound_registry_and_immutable_digest(management_inputs, image):
    management_inputs[1]["images"]["service"]["image_ref"] = image
    management_inputs[2]["task_image_ref"] = image
    with pytest.raises(ValueError, match="management image"):
        render(management_inputs)
