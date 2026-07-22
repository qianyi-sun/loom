"""Fail-closed capability evidence for Tier 2 readonly staging probes."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

_CAPABILITY_RE = re.compile(r"^[a-z][a-z0-9./:-]{0,95}$")
_READONLY_KUBERNETES_VERBS = frozenset({"get", "list", "watch"})
_READONLY_HTTP_METHODS = frozenset({"GET", "HEAD"})
_FORBIDDEN_RESOURCES = frozenset({"secrets", "serviceaccounts/token"})


def _forbidden_resource(resource: str) -> bool:
    return resource in _FORBIDDEN_RESOURCES or resource.startswith("secrets/")


def readonly_authority_policy_digest() -> str:
    payload = {
        "environment": "staging",
        "forbidden_resources": sorted(_FORBIDDEN_RESOURCES),
        "http_methods": sorted(_READONLY_HTTP_METHODS),
        "kubernetes_verbs": sorted(_READONLY_KUBERNETES_VERBS),
        "namespace": "loom-staging",
        "principal": "loom-rollout-readonly",
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class ReadonlyAuthorityEvidence:
    """Server-observed capabilities for the dedicated baseline principal."""

    principal: str
    environment: str
    namespace: str
    kubernetes_verbs: tuple[str, ...]
    kubernetes_resources: tuple[str, ...]
    http_methods: tuple[str, ...]
    capability_source_digest: str

    def __post_init__(self) -> None:
        values = (*self.kubernetes_verbs, *self.kubernetes_resources)
        if (
            self.principal != "loom-rollout-readonly"
            or self.environment != "staging"
            or self.namespace != "loom-staging"
            or not values
            or any(value != "*" and _CAPABILITY_RE.fullmatch(value) is None for value in values)
            or len(set(self.kubernetes_verbs)) != len(self.kubernetes_verbs)
            or len(set(self.kubernetes_resources)) != len(self.kubernetes_resources)
            or len(set(self.http_methods)) != len(self.http_methods)
            or any(
                method not in {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}
                for method in self.http_methods
            )
            or len(self.capability_source_digest) != 64
            or any(
                character not in "0123456789abcdef" for character in self.capability_source_digest
            )
        ):
            raise ValueError("readonly authority evidence is invalid")

    @property
    def ready(self) -> bool:
        return bool(
            set(self.kubernetes_verbs) <= _READONLY_KUBERNETES_VERBS
            and set(self.http_methods) <= _READONLY_HTTP_METHODS
            and not any(_forbidden_resource(item) for item in self.kubernetes_resources)
            and "*" not in self.kubernetes_verbs
            and "*" not in self.kubernetes_resources
        )

    @property
    def evidence_digest(self) -> str:
        payload = {
            "capability_source_digest": self.capability_source_digest,
            "environment": self.environment,
            "http_methods": sorted(self.http_methods),
            "kubernetes_resources": sorted(self.kubernetes_resources),
            "kubernetes_verbs": sorted(self.kubernetes_verbs),
            "namespace": self.namespace,
            "policy_digest": readonly_authority_policy_digest(),
            "principal": self.principal,
            "ready": self.ready,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


__all__ = ["ReadonlyAuthorityEvidence", "readonly_authority_policy_digest"]
