from __future__ import annotations

import yaml

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    render_dev_instance_manifests,
)


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
