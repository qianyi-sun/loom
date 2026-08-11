from __future__ import annotations

from uuid import UUID

import pytest
import yaml

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    PersonalDevManifestBinding,
    personal_dev_activation_manifest_documents,
    personal_dev_preparation_manifest_documents,
    render_dev_instance_manifests,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS


def _config() -> DevInstanceManifestConfig:
    return DevInstanceManifestConfig(
        image_tag="dev-a1b2c3d",
        candidate_sha="a1b2c3d" + "0" * 33,
        deployment_generation=7,
        container_registry="registry.internal.example/loom",
        minio_endpoint="https://dev-minio.internal.example",
    )


def test_manifest_is_external_storage_isolated_and_secret_free() -> None:
    identity = derive_identity("alice")
    rendered = render_dev_instance_manifests(identity, _config())
    documents = [doc for doc in yaml.safe_load_all(rendered) if doc]

    assert {(doc["kind"], doc["metadata"]["name"]) for doc in documents} >= {
        ("Namespace", "loom-dev-alice"),
        ("Job", "loom-migrate-a1b2c3d-g7"),
        ("Deployment", "loom-control-plane"),
        ("Deployment", "loom-llm-gateway"),
        ("Deployment", "loom-service"),
        ("Ingress", "loom-dev"),
    }
    assert not any(doc["kind"] in {"StatefulSet", "PersistentVolumeClaim"} for doc in documents)
    assert "loom-postgres" not in rendered
    assert "loom-minio:9000" not in rendered
    assert "https://dev-minio.internal.example" in rendered
    assert "cp-db-url" in rendered
    assert "minio-access-key" in rendered
    assert "password" not in rendered.lower()
    assert "registry.internal.example/loom/loom-service:dev-a1b2c3d" in rendered

    ingress = next(doc for doc in documents if doc["kind"] == "Ingress")
    assert ingress["spec"]["rules"][0]["host"] == "alice.dev.yylx.world"
    assert ingress["spec"]["rules"][1]["host"] == "cp-alice.dev.yylx.world"
    assert ingress["spec"]["rules"][2]["host"] == "gw-alice.dev.yylx.world"
    assert ingress["spec"]["tls"][0]["secretName"] == "loom-dev-tls"
    for doc in documents:
        metadata = doc.get("metadata", {})
        if doc["kind"] != "Namespace":
            assert metadata["namespace"] == "loom-dev-alice"
        if doc["kind"] in {"Deployment", "Job"}:
            template = doc["spec"]["template"]["spec"]
            assert template["automountServiceAccountToken"] is False
            assert template["securityContext"]["runAsNonRoot"] is True
            assert template["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
            for container in template["containers"]:
                assert container["securityContext"]["allowPrivilegeEscalation"] is False
                assert container["securityContext"]["capabilities"] == {"drop": ["ALL"]}


def test_manifest_requires_candidate_bound_immutable_tag() -> None:
    try:
        DevInstanceManifestConfig(
            image_tag="latest",
            candidate_sha="a" * 40,
            deployment_generation=1,
            container_registry="registry.example/loom",
            minio_endpoint="https://minio.example",
        )
    except ValueError as exc:
        assert "candidate SHA prefix" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("unbound image tag was accepted")


def _immutable_config() -> DevInstanceManifestConfig:
    return DevInstanceManifestConfig(
        image_tag="",
        candidate_sha="b" * 64,
        deployment_generation=8,
        container_registry="",
        minio_endpoint="https://dev-minio.internal.example",
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


def test_personal_manifest_uses_complete_immutable_candidate_image_set() -> None:
    rendered = render_dev_instance_manifests(derive_identity("alice"), _immutable_config())
    documents = [doc for doc in yaml.safe_load_all(rendered) if doc]
    workload_images = {
        container["image"]
        for document in documents
        if document["kind"] in {"Deployment", "Job"}
        for container in document["spec"]["template"]["spec"]["containers"]
    }

    assert workload_images == {
        _immutable_config().image_references[component]
        for component in ("control-plane", "llm-gateway", "service", "web")
    }
    assert all("@sha256:" in image and ":dev" not in image for image in workload_images)
    assert ("Deployment", "loom-web-g8") in {
        (document["kind"], document["metadata"]["name"]) for document in documents
    }

    ingress = next(document for document in documents if document["kind"] == "Ingress")
    public_paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert [path["path"] for path in public_paths] == ["/api/v1", "/"]
    assert public_paths[0]["backend"]["service"]["name"] == "loom-service"
    assert public_paths[1]["backend"]["service"]["name"] == "loom-web"


def test_personal_manifest_stamps_exact_lifecycle_binding_on_every_object() -> None:
    documents = personal_dev_preparation_manifest_documents(
        derive_identity("alice"),
        _immutable_config(),
    )
    expected = {
        "loom.dev/subject": "00000000-0000-0000-0000-000000000001",
        "loom.dev/incarnation": "00000000-0000-0000-0000-000000000002",
        "loom.dev/operation": "00000000-0000-0000-0000-000000000003",
        "loom.dev/attempt": "00000000-0000-0000-0000-000000000004",
        "loom.dev/operation-epoch": "5",
        "loom.dev/generation": "8",
    }

    for document in documents:
        assert document["metadata"]["labels"] | expected == document["metadata"]["labels"]
        if document["kind"] in {"Deployment", "Job"}:
            template_labels = document["spec"]["template"]["metadata"]["labels"]
            assert template_labels | expected == template_labels


def test_personal_generation_prepares_without_switching_stable_routes() -> None:
    identity = derive_identity("alice")
    preparation = personal_dev_preparation_manifest_documents(identity, _immutable_config())
    activation = personal_dev_activation_manifest_documents(identity, _immutable_config())

    assert not any(document["kind"] == "Ingress" for document in preparation)
    assert {
        document["metadata"]["name"] for document in preparation if document["kind"] == "Deployment"
    } == {
        "loom-control-plane-g8",
        "loom-llm-gateway-g8",
        "loom-service-g8",
        "loom-web-g8",
    }
    assert {
        document["metadata"]["name"] for document in preparation if document["kind"] == "Service"
    } == {
        "loom-control-plane-g8",
        "loom-llm-gateway-g8",
        "loom-service-g8",
        "loom-web-g8",
    }

    assert [document["kind"] for document in activation].count("Ingress") == 1
    for service in (document for document in activation if document["kind"] == "Service"):
        assert service["metadata"]["name"] in {
            "loom-control-plane",
            "loom-llm-gateway",
            "loom-service",
            "loom-web",
        }
        assert service["spec"]["selector"]["loom.dev/generation"] == "8"
        assert service["metadata"]["labels"]["loom.dev/operation-epoch"] == "5"


def test_personal_manifest_rejects_incomplete_or_mutable_image_sets() -> None:
    values = dict(_immutable_config().image_references)
    values.pop("worker")
    with pytest.raises(ValueError, match="complete personal-dev component set"):
        DevInstanceManifestConfig(
            image_tag="",
            candidate_sha="b" * 64,
            deployment_generation=8,
            container_registry="",
            minio_endpoint="https://minio.example",
            image_references=values,
        )

    values["worker"] = "registry.example/loom-worker:latest"
    with pytest.raises(ValueError, match="immutable OCI reference"):
        DevInstanceManifestConfig(
            image_tag="",
            candidate_sha="b" * 64,
            deployment_generation=8,
            container_registry="",
            minio_endpoint="https://minio.example",
            image_references=values,
        )
