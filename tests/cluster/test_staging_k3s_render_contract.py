"""Mutation-free render contract for the live multi-node staging topology."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config

REPO_ROOT = Path(__file__).resolve().parents[2]
STAGING_CONFIG = REPO_ROOT / "deploy/environments/staging.multinode.cluster.toml"


def _rendered_documents() -> tuple[object, list[dict[str, object]]]:
    config = load_cluster_config(STAGING_CONFIG)
    documents = [
        document
        for document in yaml.safe_load_all(render_manifests(config))
        if isinstance(document, dict)
    ]
    return config, documents


def _resource(
    documents: list[dict[str, object]],
    *,
    kind: str,
    name: str,
) -> dict[str, object]:
    matches = [
        document
        for document in documents
        if document.get("kind") == kind
        and isinstance(document.get("metadata"), dict)
        and document["metadata"].get("name") == name
    ]
    assert len(matches) == 1, f"expected one {kind}/{name}, got {len(matches)}"
    return matches[0]


@pytest.mark.cluster_smoke
def test_live_staging_k3s_render_contract() -> None:
    """The checked-in live profile must retain its accepted k3s topology."""
    config, documents = _rendered_documents()

    assert config.namespace == "loom-staging"
    assert config.runtime_environment == "staging"
    assert config.frontend_route_path == "/staging"
    assert config.persistent_storage_backend == "dynamic"
    assert config.topology.multi_node is True
    assert config.topology.storage_backend == "longhorn"
    assert config.topology.postgres_replicas == 3
    assert config.topology.minio_replicas == 4
    assert config.topology.anti_affinity == "required"
    assert config.topology.min_available == 3

    postgres = _resource(documents, kind="Cluster", name="loom-postgres")
    assert postgres["spec"]["instances"] == 3
    assert postgres["spec"]["storage"]["storageClass"] == "longhorn"

    minio = _resource(documents, kind="StatefulSet", name="loom-minio")
    assert minio["spec"]["replicas"] == 4
    claims = minio["spec"]["volumeClaimTemplates"]
    assert claims[0]["spec"]["storageClassName"] == "longhorn"

    minio_pdb = _resource(documents, kind="PodDisruptionBudget", name="loom-minio")
    assert minio_pdb["spec"]["minAvailable"] == 3

    pgbouncer = _resource(documents, kind="Deployment", name="loom-pgbouncer")
    assert pgbouncer["spec"]["replicas"] == 2

    pgbouncer_service = _resource(documents, kind="Service", name="loom-pgbouncer")
    service_ports = {
        port["name"]: port["port"] for port in pgbouncer_service["spec"]["ports"]
    }
    assert service_ports == {"sql": 6432, "metrics": 9127}

    pgbouncer_pdb = _resource(
        documents,
        kind="PodDisruptionBudget",
        name="loom-pgbouncer",
    )
    assert pgbouncer_pdb["spec"]["minAvailable"] == 1
    _resource(documents, kind="NetworkPolicy", name="loom-pgbouncer")

    host_path_volumes = [
        document
        for document in documents
        if document.get("kind") == "PersistentVolume"
        and "hostPath" in document.get("spec", {})
    ]
    assert host_path_volumes == []
