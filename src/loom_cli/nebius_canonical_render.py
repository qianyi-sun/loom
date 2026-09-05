"""Opt-in canonical-side configuration for a separate Nebius staging target."""

from __future__ import annotations

import ipaddress
import re
from typing import Any

from loom_cli.cluster_config import ClusterConfig

_SOURCE_FIELDS = {
    "ENDPOINT": "endpoint",
    "REGION": "region",
    "BUCKET": "bucket",
    "ACCESS_KEY": "access-key",
    "SECRET_KEY": "secret-key",
}


def nebius_canonical_context(config: ClusterConfig) -> dict[str, Any]:
    """Validate non-secret attachment inputs before generating any resources."""
    binding = config.nebius_execution
    overrides: dict[str, Any] = {
        "slurm_worker_controller_environment": config.runtime_environment,
        "artifacts_bucket": config.artifacts_bucket,
        "trajectories_bucket": config.trajectories_bucket,
    }
    context: dict[str, Any] = {
        "nebius_control_plane_skip_names": (),
        "nebius_control_plane_overrides": overrides,
        "nebius_service_skip_names": (),
        "nebius_source_secret_fields": _SOURCE_FIELDS,
        "nebius_source_egress_rules": [],
        "nebius_execution_ingress_cidrs": [],
    }
    if not binding.enabled:
        return context
    if (
        config.runtime_environment != "staging"
        or config.artifacts_bucket != "loom-staging-artifacts"
        or config.trajectories_bucket != "loom-staging-trajectories"
    ):
        raise ValueError("nebius_execution requires the canonical staging environment and buckets")
    for field in (
        "source_secret_name",
        "runtime_profile_secret_name",
        "image_admission_secret_name",
    ):
        name = getattr(binding, field)
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name):
            raise ValueError(f"nebius_execution.{field} must name an existing Kubernetes Secret")
    if not re.fullmatch(r"[a-f0-9]{64}", binding.configuration_revision):
        raise ValueError(
            "nebius_execution.configuration_revision must be a SHA-256 deployment revision"
        )
    if not binding.source_egress_allowlist or not binding.execution_ingress_cidrs:
        raise ValueError("nebius_execution requires source egress and execution ingress routes")

    # Reuse the normal renderer's bounded IP:port parser; no DNS or cloud lookup.
    from loom_cli.cluster_cmd import _parse_provider_egress_target

    try:
        egress = [_parse_provider_egress_target(value) for value in binding.source_egress_allowlist]
        ingress = [
            ipaddress.ip_network(value, strict=True) for value in binding.execution_ingress_cidrs
        ]
    except ValueError as exc:
        raise ValueError("nebius_execution has an invalid network route") from exc
    if any(
        network.prefixlen == 0
        or network.is_loopback
        or network.is_link_local
        or network.is_multicast
        for network in ingress
    ):
        raise ValueError("nebius_execution requires scoped non-loopback ingress CIDRs")
    overrides.update(
        {
            "service_execution_scheduler_enabled": "true",
            "service_execution_materializer_enabled": "true",
            "service_execution_scheduler_environment": "staging",
            "service_execution_scheduler_pool_id": "nebius-cpu",
        }
    )
    context.update(
        {
            "nebius_control_plane_skip_names": (
                *(f"service_execution_source_{name.lower()}" for name in _SOURCE_FIELDS),
                "service_execution_source_access_key_file",
                "service_execution_source_secret_key_file",
                "execution_image_admission_public_keys_json",
            ),
            "nebius_service_skip_names": ("service_execution_runtime_profile_json",),
            "nebius_source_egress_rules": [{"cidr": row.cidr, "port": row.port} for row in egress],
            "nebius_execution_ingress_cidrs": [str(network) for network in ingress],
        }
    )
    return context
