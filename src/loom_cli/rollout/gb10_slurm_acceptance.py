"""Strict validation for root-issued GB10 Slurm acceptance evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from loom_cli.rollout.gb10_readiness import FULL_GB10_HOSTS

_EXCLUSIVE_GB10_BUILDER_HOST = "trt-gb10-2"
GB10_SLURM_WORKER_HOSTS = tuple(
    host for host in FULL_GB10_HOSTS if host != _EXCLUSIVE_GB10_BUILDER_HOST
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_FIELDS = {
    "candidate_sha",
    "candidate_tree",
    "cluster_name",
    "controller_host",
    "deferred_busy_nodes",
    "expires_at",
    "generated_at",
    "kind",
    "node_count",
    "nodes",
    "probed_node_count",
    "probed_nodes",
    "profile_sha256",
    "result",
    "schema_version",
    "service_identity",
    "trial_cache_registry",
}
_SERVICE_IDENTITY = {
    "user": "loom-rollout",
    "uid": 995,
    "gid": 2007,
    "account": "loom-staging",
    "qos": "loom-staging",
}
_TRIAL_CACHE_REGISTRY = {
    "ca_sha256": "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3",
    "canary_digest": "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e",
    "repository": "192.168.50.103:5443/loom-trial-cache",
}


@dataclass(frozen=True, slots=True)
class GB10SlurmAcceptanceEvidence:
    """Immutable canonical payload plus its content digest."""

    _payload_json: str
    evidence_digest: str

    @property
    def payload(self) -> Mapping[str, object]:
        value = json.loads(self._payload_json)
        if not isinstance(value, dict):  # pragma: no cover - constructor owns canonical JSON
            raise RuntimeError("GB10 Slurm acceptance payload is invalid")
        return MappingProxyType(value)


def validate_gb10_slurm_acceptance(
    value: Mapping[str, object],
    *,
    candidate_sha: str,
    candidate_tree: str | None,
    profile_sha256: str,
    nodes: Sequence[str],
    now: datetime | None = None,
) -> GB10SlurmAcceptanceEvidence:
    """Validate exact candidate, fleet, identity, registry, and freshness bindings."""

    expected_nodes = tuple(nodes)
    if (
        expected_nodes != GB10_SLURM_WORKER_HOSTS
        or any(type(node) is not str for node in expected_nodes)
        or type(candidate_sha) is not str
        or _SHA_RE.fullmatch(candidate_sha) is None
        or type(profile_sha256) is not str
        or _SHA256_RE.fullmatch(profile_sha256) is None
        or (now is not None and not isinstance(now, datetime))
    ):
        raise ValueError("GB10 Slurm acceptance evidence is invalid")
    observed_nodes = value.get("nodes")
    probed_nodes = value.get("probed_nodes")
    deferred_nodes = value.get("deferred_busy_nodes")
    observed_tree = value.get("candidate_tree")
    probed_node_count = len(probed_nodes) if isinstance(probed_nodes, list) else -1
    current_time = datetime.now(UTC) if now is None else now
    try:
        generated_at = datetime.fromisoformat(str(value["generated_at"]))
        expires_at = datetime.fromisoformat(str(value["expires_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("GB10 Slurm acceptance evidence is invalid") from exc
    coverage_valid = bool(
        isinstance(observed_nodes, list)
        and tuple(observed_nodes) == expected_nodes
        and isinstance(probed_nodes, list)
        and probed_nodes
        and all(type(node) is str for node in probed_nodes)
        and len(set(probed_nodes)) == len(probed_nodes)
        and isinstance(deferred_nodes, list)
        and all(type(node) is str for node in deferred_nodes)
        and len(set(deferred_nodes)) == len(deferred_nodes)
        and not (set(probed_nodes) & set(deferred_nodes))
        and set(probed_nodes) | set(deferred_nodes) == set(expected_nodes)
        and probed_nodes == [node for node in expected_nodes if node in set(probed_nodes)]
        and deferred_nodes == [node for node in expected_nodes if node in set(deferred_nodes)]
    )
    times_valid = bool(
        current_time.tzinfo is not None
        and current_time.utcoffset() is not None
        and generated_at.tzinfo is not None
        and generated_at.utcoffset() is not None
        and expires_at.tzinfo is not None
        and expires_at.utcoffset() is not None
        and generated_at <= current_time < expires_at
        and 0 < (expires_at - generated_at).total_seconds() <= 1800
    )
    if (
        set(value) != _EXPECTED_FIELDS
        or value.get("schema_version") != 1
        or type(value.get("schema_version")) is not int
        or value.get("kind") != "loom_gb10_slurm_acceptance"
        or value.get("result") != "pass"
        or value.get("candidate_sha") != candidate_sha
        or type(observed_tree) is not str
        or _SHA_RE.fullmatch(observed_tree) is None
        or (candidate_tree is not None and observed_tree != candidate_tree)
        or value.get("profile_sha256") != profile_sha256
        or value.get("cluster_name") != "trt-gb10"
        or value.get("controller_host") != "gx10-01c7"
        or value.get("service_identity") != _SERVICE_IDENTITY
        or type(value.get("node_count")) is not int
        or value.get("node_count") != len(expected_nodes)
        or type(value.get("probed_node_count")) is not int
        or value.get("probed_node_count") != probed_node_count
        or value.get("trial_cache_registry") != _TRIAL_CACHE_REGISTRY
        or not coverage_valid
        or not times_valid
    ):
        raise ValueError("GB10 Slurm acceptance evidence is invalid")
    payload_json = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return GB10SlurmAcceptanceEvidence(
        _payload_json=payload_json,
        evidence_digest=hashlib.sha256(payload_json.encode()).hexdigest(),
    )


__all__ = [
    "GB10_SLURM_WORKER_HOSTS",
    "GB10SlurmAcceptanceEvidence",
    "validate_gb10_slurm_acceptance",
]
