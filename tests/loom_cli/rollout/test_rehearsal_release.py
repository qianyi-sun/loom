from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.rehearsal_action_source import (
    RehearsalPlan,
    RehearsalResources,
    RehearsalSmokeAuthority,
)
from loom_cli.rollout.rehearsal_release import build_rehearsal_release_artifact
from tests.loom_cli.rollout.rehearsal_fixtures import gb10_rehearsal_authority


def _deployment(name: str, image: str, *, with_admin_secret: bool) -> dict[str, object]:
    pod: dict[str, object] = {
        "containers": [
            {
                "env": [
                    {"name": "LOOM_ENV", "value": "staging"},
                    {"name": "LOOM_NAMESPACE", "value": "loom-staging"},
                    {"name": "LOOM_FRONTEND_ROUTE_PATH", "value": "/dev"},
                    {"name": "LOOM_FRONTEND_API_BASE", "value": "/dev"},
                    {"name": "LOOM_FRONTEND_PUBLIC_ORIGIN", "value": "https://yylx.world"},
                    {"name": "LOOM_SVC_PUBLIC_BASE_URL", "value": "https://yylx.world/dev"},
                ],
                "image": image,
                "name": name,
            }
        ],
    }
    if with_admin_secret:
        pod["containers"][0]["volumeMounts"] = [  # type: ignore[index]
            {"mountPath": "/var/run/loom/secrets/admin", "name": "loom-admin-secret"}
        ]
        pod["volumes"] = [
            {
                "name": "loom-admin-secret",
                "secret": {"defaultMode": 0o400, "secretName": "loom-admin-secret"},
            }
        ]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": "loom-staging"},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": {"app": name}}, "spec": pod},
        },
    }


def _rendered(image_tag: str) -> str:
    resources: list[dict[str, object]] = []
    for name, image_name, secret in (
        ("loom-control-plane", "loom-control-plane", True),
        ("loom-llm-gateway", "loom-llm-gateway", True),
        ("loom-service", "loom-service", True),
        ("loom-web", "loom-web", False),
    ):
        resources.append(_deployment(name, f"{image_name}:{image_tag}", with_admin_secret=secret))
    for name in (
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-postgres",
        "loom-service",
        "loom-web",
    ):
        resources.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": name, "namespace": "loom-staging"},
                "spec": {"ports": [{"port": 80}], "selector": {"app": name}},
            }
        )
    return yaml.safe_dump_all(resources, sort_keys=True)


def _plan(tmp_path: Path) -> RehearsalPlan:
    image_tag = "staging-aaaaaaaa"
    rendered = _rendered(image_tag)
    artifact_root = tmp_path / "preflight-artifacts" / ("6" * 64)
    artifact_root.mkdir(parents=True, mode=0o700)
    rendered_path = artifact_root / "rendered.yaml"
    rendered_path.write_text(rendered)
    rendered_path.chmod(0o600)
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        cluster_name="loom-staging",
        checkpoint_request_id="req-abcdefgh",
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={
            "loom-control-plane": "sha256:" + "8" * 64,
            "loom-egress-xds": "sha256:" + "3" * 64,
            "loom-family-orchestrator": "sha256:" + "4" * 64,
            "loom-pipeline-orchestrator": "sha256:" + "a" * 64,
            "loom-llm-gateway": "sha256:" + "5" * 64,
            "loom-rehearsal-postgres": "sha256:" + "9" * 64,
            "loom-service": "sha256:" + "1" * 64,
            "loom-staging-admin-browser-smoke": "sha256:" + "6" * 64,
            "loom-web": "sha256:" + "2" * 64,
            "loom-worker": "sha256:" + "7" * 64,
        },
        image_tag=image_tag,
        image_artifact_sha256="3" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=artifact_root / "artifact.json",
        rendered_manifest_path=rendered_path,
        production_defaults_path=artifact_root / "production-defaults.json",
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        production_defaults_sha256="9" * 64,
        external_supervisor_artifact_sha256="a" * 64,
        external_supervisor_profile_sha256="b" * 64,
        external_supervisor_script_sha256={
            "scripts/ops/worker_pool_autoscaler_external_once.py": "c" * 64,
        },
        external_supervisor_unit_sha256={
            "loom-autoscaler-gb10-staging.service": "d" * 64,
            "loom-autoscaler-gb10-staging.timer": "e" * 64,
        },
        migration_plan_sha256="4" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="5" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://yylx.world/dev",
        ),
        smoke_authority=RehearsalSmokeAuthority(
            represented_username="devansh",
            team_id="11111111-1111-4111-8111-111111111111",
            admin_actor="loom-staging-rollout",
            task_id="loom-smoke/gb10-oracle-hello-world",
            required_worker_pool="gb10",
            agent="oracle",
        ),
        gb10_authority=gb10_rehearsal_authority(),
    )


def test_release_artifact_isolates_exact_candidate_subset(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    artifact = build_rehearsal_release_artifact(plan, service_uid=os.geteuid())
    resources = list(yaml.safe_load_all(artifact.payload))

    assert artifact.resource_count == 10
    assert all(
        resource["metadata"]["namespace"] == plan.resources.namespace for resource in resources
    )
    deployments = {
        resource["metadata"]["name"]: resource
        for resource in resources
        if resource["kind"] == "Deployment"
    }
    assert set(deployments) == {
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-service",
        "loom-web",
    }
    for name, deployment in deployments.items():
        pod = deployment["spec"]["template"]["spec"]
        assert deployment["spec"]["replicas"] == 1
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
        assert pod["containers"][0]["image"].endswith(plan.image_tag)
        assert {item["name"] for item in pod["containers"][0]["env"]} >= {
            "HOME",
            "PYTHONDONTWRITEBYTECODE",
            "TMPDIR",
        }
        assert {item["name"] for item in pod["containers"][0]["volumeMounts"]} >= {
            "loom-rehearsal-tmp"
        }
        assert {item["name"] for item in pod["volumes"]} >= {"loom-rehearsal-tmp"}
        admin_probe_mounts = [
            item
            for item in pod["containers"][0]["volumeMounts"]
            if item.get("subPath") == "secrets.toml"
        ]
        if name == "loom-service":
            assert admin_probe_mounts == [
                {
                    "mountPath": "/var/run/loom/rehearsal-admin/secrets.toml",
                    "name": "loom-admin-secret",
                    "readOnly": True,
                    "subPath": "secrets.toml",
                }
            ]
        else:
            assert admin_probe_mounts == []
        rehearsal_env = {item["name"]: item.get("value") for item in pod["containers"][0]["env"]}
        if name == "loom-web":
            assert rehearsal_env["LOOM_FRONTEND_REHEARSAL_ID"] == "5" * 24
            assert pod["containers"][0]["volumeMounts"][-1] == {
                "mountPath": "/usr/share/nginx/html",
                "name": "loom-rehearsal-webroot",
            }
            assert pod["volumes"][-1] == {
                "emptyDir": {"sizeLimit": "128Mi"},
                "name": "loom-rehearsal-webroot",
            }
            assert pod["initContainers"] == [
                {
                    "args": [
                        "-R",
                        "/usr/share/nginx/html/.",
                        "/var/run/loom-webroot/",
                    ],
                    "command": ["/bin/cp"],
                    "image": "loom-web:" + plan.image_tag,
                    "name": "loom-webroot-copy",
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "25m", "memory": "16Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                        "runAsGroup": 101,
                        "runAsNonRoot": True,
                        "runAsUser": 101,
                    },
                    "volumeMounts": [
                        {
                            "mountPath": "/var/run/loom-webroot",
                            "name": "loom-rehearsal-webroot",
                        }
                    ],
                }
            ]
        else:
            assert "LOOM_FRONTEND_REHEARSAL_ID" not in rehearsal_env
            assert pod.get("initContainers") is None
        assert name in artifact.deployment_images
    postgres = next(
        resource
        for resource in resources
        if resource["kind"] == "Service" and resource["metadata"]["name"] == "loom-postgres"
    )
    assert postgres["spec"]["selector"] == {"loom.openai.dev/component": "rehearsal-database"}
    policy = next(resource for resource in resources if resource["kind"] == "NetworkPolicy")
    assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert policy["spec"]["ingress"][0]["from"][1] == {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/component": "controller"}},
    }


@pytest.mark.parametrize(
    "config_name",
    ("staging.cluster.toml", "staging.multinode.cluster.toml"),
)
def test_release_artifact_accepts_real_staging_render(
    tmp_path: Path,
    config_name: str,
) -> None:
    plan = _plan(tmp_path)
    config = replace(
        load_cluster_config(Path("deploy/environments") / config_name),
        image_tag=plan.image_tag,
        container_registry="",
        container_registry_push="",
    )
    payload = render_manifests(config)
    plan.rendered_manifest_path.write_text(payload)
    plan.rendered_manifest_path.chmod(0o600)
    record = plan.to_record()
    record["rendered_manifest_sha256"] = hashlib.sha256(payload.encode()).hexdigest()

    artifact = build_rehearsal_release_artifact(RehearsalPlan.from_record(record))

    assert artifact.resource_count == 10
    resources = list(yaml.safe_load_all(artifact.payload))
    assert {item["metadata"]["name"] for item in resources} == {
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-postgres",
        "loom-rehearsal-release",
        "loom-service",
        "loom-web",
    }
    gateway = next(
        item
        for item in resources
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "loom-llm-gateway"
    )
    gateway_env = {
        item["name"]: item for item in gateway["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert gateway_env["LOOM_GW_DB_URL"]["valueFrom"]["secretKeyRef"] == {
        "key": "gw-db-url",
        "name": "loom-secrets",
    }
    assert gateway_env["LOOM_GW_DB_URL_POOL"]["valueFrom"]["secretKeyRef"] == {
        "key": "gw-db-url-pool",
        "name": "loom-secrets",
        "optional": True,
    }
    assert not _contains_null_mapping_field(resources)
    for resource in resources:
        if resource["kind"] != "Deployment":
            continue
        container = resource["spec"]["template"]["spec"]["containers"][0]
        assert all(item.get("value") != "" for item in container.get("env", []))
        for quantities in container.get("resources", {}).values():
            assert all(isinstance(value, str) for value in quantities.values())


def _contains_null_mapping_field(value: object) -> bool:
    if isinstance(value, dict):
        return any(item is None or _contains_null_mapping_field(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_null_mapping_field(item) for item in value)
    return False


def test_release_artifact_rejects_host_authority_or_image_drift(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    resources = list(yaml.safe_load_all(plan.rendered_manifest_path.read_text()))
    deployment = next(resource for resource in resources if resource["kind"] == "Deployment")
    deployment["spec"]["template"]["spec"]["containers"][0]["ports"] = [
        {"containerPort": 8080, "hostPort": 8080}
    ]
    payload = yaml.safe_dump_all(resources, sort_keys=True)
    plan.rendered_manifest_path.write_text(payload)
    plan.rendered_manifest_path.chmod(0o600)
    record = plan.to_record()
    record["rendered_manifest_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
    plan = RehearsalPlan.from_record(record)

    with pytest.raises(ValueError, match="forbidden host authority"):
        build_rehearsal_release_artifact(plan)

    resources = list(yaml.safe_load_all(payload))
    deployment = next(resource for resource in resources if resource["kind"] == "Deployment")
    deployment["spec"]["template"]["spec"]["containers"][0].pop("ports")
    deployment["spec"]["template"]["spec"]["containers"][0]["image"] = "stale:tag"
    payload = yaml.safe_dump_all(resources, sort_keys=True)
    plan.rendered_manifest_path.write_text(payload)
    plan.rendered_manifest_path.chmod(0o600)
    record = plan.to_record()
    record["rendered_manifest_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
    plan = RehearsalPlan.from_record(record)
    with pytest.raises(ValueError, match="image binding drifted"):
        build_rehearsal_release_artifact(plan)


def test_release_artifact_rejects_unexpected_postgres_external_name(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    resources = list(yaml.safe_load_all(plan.rendered_manifest_path.read_text()))
    postgres = next(
        resource
        for resource in resources
        if resource["kind"] == "Service" and resource["metadata"]["name"] == "loom-postgres"
    )
    postgres["spec"] = {
        "type": "ExternalName",
        "externalName": "database.example.invalid",
        "ports": [{"port": 5432}],
    }
    payload = yaml.safe_dump_all(resources, sort_keys=True)
    plan.rendered_manifest_path.write_text(payload)
    plan.rendered_manifest_path.chmod(0o600)
    record = plan.to_record()
    record["rendered_manifest_sha256"] = hashlib.sha256(payload.encode()).hexdigest()

    with pytest.raises(ValueError, match="postgres service external authority drifted"):
        build_rehearsal_release_artifact(RehearsalPlan.from_record(record))


@pytest.mark.parametrize(
    ("kind", "field", "expected"),
    (
        ("Deployment", "initContainers", "auxiliary containers are forbidden"),
        ("Service", "externalIPs", "external authority"),
    ),
)
def test_release_artifact_rejects_auxiliary_or_external_authority(
    tmp_path: Path,
    kind: str,
    field: str,
    expected: str,
) -> None:
    plan = _plan(tmp_path)
    resources = list(yaml.safe_load_all(plan.rendered_manifest_path.read_text()))
    resource = next(item for item in resources if item["kind"] == kind)
    if kind == "Deployment":
        resource["spec"]["template"]["spec"][field] = [
            {"image": "busybox:latest", "name": "unexpected"}
        ]
    else:
        resource["spec"][field] = ["192.0.2.1"]
    payload = yaml.safe_dump_all(resources, sort_keys=True)
    plan.rendered_manifest_path.write_text(payload)
    plan.rendered_manifest_path.chmod(0o600)
    record = plan.to_record()
    record["rendered_manifest_sha256"] = hashlib.sha256(payload.encode()).hexdigest()

    with pytest.raises(ValueError, match=expected):
        build_rehearsal_release_artifact(RehearsalPlan.from_record(record))
