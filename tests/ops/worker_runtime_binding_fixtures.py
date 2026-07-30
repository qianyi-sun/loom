from __future__ import annotations

import hashlib
from typing import Any

from scripts.ops import developer_environment_registry as registry

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"


def _digest(seed: str) -> str:
    return hashlib.sha256(seed.encode("ascii")).hexdigest()


def rich_image_archives(
    *,
    amd64_config: str,
    arm64_config: str,
    seed: str,
) -> dict[str, dict[str, Any]]:
    bindings: dict[str, dict[str, Any]] = {}
    for architecture, config_digest, size in (
        ("amd64", amd64_config, 2048),
        ("arm64", arm64_config, 4096),
    ):
        prefix = f"{seed}:{architecture}"
        bindings[architecture] = {
            "sha256": _digest(f"{prefix}:archive"),
            "size": size,
            "config_digest": config_digest,
            "index_digest": f"sha256:{_digest(f'{prefix}:index')}",
            "manifest_digest": f"sha256:{_digest(f'{prefix}:manifest')}",
            "manifest_media_type": OCI_MANIFEST,
            "load_descriptor_digest": f"sha256:{_digest(f'{prefix}:manifest')}",
            "load_descriptor_media_type": OCI_MANIFEST,
        }
    return bindings


def worker_runtime_bindings(
    candidate: registry.CandidateRecord,
) -> dict[str, dict[str, dict[str, Any]]]:
    return worker_runtime_bindings_from_archives(
        candidate_id=candidate.candidate_id,
        image_archives=candidate.image_archives,
    )


def worker_runtime_bindings_from_archives(
    *,
    candidate_id: str,
    image_archives: dict[str, dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    nodes: dict[str, dict[str, Any]] = {}
    domains: dict[str, dict[str, Any]] = {}
    for domain, architecture in registry.WORKER_RUNTIME_BINDING_DOMAINS.items():
        archive = image_archives[architecture]
        backend = "containerd-snapshotter-v1" if domain == "oldlab" else "classic-overlay2"
        runtime_image_id = (
            archive["load_descriptor_digest"]
            if domain == "oldlab"
            else archive["config_digest"]
        )
        domain_binding = {
            "architecture": architecture,
            "docker_driver": registry.WORKER_RUNTIME_BACKENDS[backend],
            "docker_backend": backend,
            "config_digest": archive["config_digest"],
            "load_descriptor_digest": archive["load_descriptor_digest"],
            "load_descriptor_media_type": archive["load_descriptor_media_type"],
            "runtime_image_id": runtime_image_id,
        }
        domains[domain] = domain_binding
        for node in registry.FLEET_NODES:
            if ("oldlab" if node.startswith("oldlab-") else "gb10") != domain:
                continue
            nodes[node] = {
                "domain": domain,
                **domain_binding,
                "docker_descriptor_digest": (
                    archive["load_descriptor_digest"] if domain == "oldlab" else None
                ),
                "docker_descriptor_media_type": (
                    archive["load_descriptor_media_type"] if domain == "oldlab" else None
                ),
                "receipt_sha256": _digest(f"{candidate_id}:{node}"),
            }
    return {"nodes": nodes, "domains": domains}
