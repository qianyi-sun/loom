"""Exact one-time adoption plan for recognized staging manifest field owners.

The normal protected apply contract never forces conflicts.  This module owns
the narrower maintenance exception needed to migrate rendered resources from
recognized legacy managers without changing their live specification.  It
projects only fields already present in the live object and copies their live
values, so a force-conflicts dry-run must prove a semantic no-op before any
managed-field adoption can be authorized.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.manifest_readiness import ManifestArtifact
from loom_cli.rollout.operator.manifest_apply_contract import (
    MANIFEST_FIELD_MANAGER,
    MANIFEST_REQUEST_TIMEOUT,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_LAST_APPLIED_ANNOTATION = "kubectl.kubernetes.io/last-applied-configuration"
_ALLOWED_MANAGERS = frozenset(
    {
        MANIFEST_FIELD_MANAGER,
        "kube-controller-manager",
        "kubectl-client-side-apply",
        "kubectl-patch",
        "kubectl-rollout",
        "loom-lifecycle-bootstrap",
        "nginx-ingress-controller",
    }
)
NETWORK_POLICY_CONVERGENCE_TARGETS = (
    "networking.k8s.io/v1|NetworkPolicy|loom-staging|loom-minio",
    "networking.k8s.io/v1|NetworkPolicy|loom-staging|loom-postgres",
    "networking.k8s.io/v1|NetworkPolicy|loom-staging|loom-staging-data-lifecycle",
)
_CONTROLLER_ONLY_MANAGERS = frozenset({"kube-controller-manager", "nginx-ingress-controller"})


class ManifestOwnershipAdoptionError(RuntimeError):
    """Raised when legacy ownership cannot be adopted without live drift."""


@dataclass(frozen=True, slots=True)
class AdoptionResource:
    identity: str
    uid: str
    resource_version: str
    generation: int | None
    live_sha256: str
    managed_fields_sha256: str
    desired_sha256: str
    overlay_sha256: str
    desired_json: str
    overlay_json: str

    def __post_init__(self) -> None:
        try:
            desired = json.loads(self.desired_json)
            overlay = json.loads(self.overlay_json)
        except json.JSONDecodeError as exc:
            raise ValueError("manifest ownership adoption resource is invalid") from exc
        if (
            not isinstance(desired, dict)
            or not isinstance(overlay, dict)
            or not self.uid
            or not self.resource_version.isdigit()
            or (
                self.generation is not None
                and (type(self.generation) is not int or self.generation < 1)
            )
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.live_sha256,
                    self.managed_fields_sha256,
                    self.desired_sha256,
                    self.overlay_sha256,
                )
            )
            or _hash_json(desired) != self.desired_sha256
            or _hash_json(overlay) != self.overlay_sha256
            or self.desired_json != json.dumps(desired, sort_keys=True, separators=(",", ":"))
            or self.overlay_json != json.dumps(overlay, sort_keys=True, separators=(",", ":"))
            or _resource_identity(desired) != self.identity
            or _resource_identity(overlay) != self.identity
        ):
            raise ValueError("manifest ownership adoption resource is invalid")

    @property
    def overlay(self) -> Mapping[str, object]:
        value = json.loads(self.overlay_json)
        if not isinstance(value, dict):  # guarded by __post_init__
            raise RuntimeError("manifest ownership adoption overlay drifted")
        return value

    @property
    def desired(self) -> Mapping[str, object]:
        value = json.loads(self.desired_json)
        if not isinstance(value, dict):  # guarded by __post_init__
            raise RuntimeError("manifest ownership desired resource drifted")
        return value


@dataclass(frozen=True, slots=True)
class ManifestOwnershipAdoptionPlan:
    candidate_sha: str
    candidate_tree: str
    rendered_manifest_sha256: str
    mutation_epoch: int
    resources: tuple[AdoptionResource, ...]
    plan_sha256: str

    def __post_init__(self) -> None:
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _SHA256_RE.fullmatch(self.rendered_manifest_sha256) is None
            or self.mutation_epoch < 0
            or not 1 <= len(self.resources) <= 512
            or tuple(item.identity for item in self.resources)
            != tuple(sorted({item.identity for item in self.resources}))
            or _SHA256_RE.fullmatch(self.plan_sha256) is None
            or self.plan_sha256 != _plan_digest(self)
        ):
            raise ValueError("manifest ownership adoption plan is invalid")

    @property
    def overlay_yaml(self) -> str:
        documents = [dict(item.overlay) for item in self.resources]
        return cast(str, yaml.safe_dump_all(documents, sort_keys=True, explicit_start=True))


def build_manifest_ownership_adoption_plan(
    *,
    artifact: ManifestArtifact,
    live_resources: Sequence[Mapping[str, object]],
    candidate_sha: str,
    candidate_tree: str,
    mutation_epoch: int,
) -> ManifestOwnershipAdoptionPlan:
    """Build one exact semantic-no-op adoption plan for existing rendered resources."""
    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or _SHA_RE.fullmatch(candidate_tree) is None
        or mutation_epoch < 0
    ):
        raise ManifestOwnershipAdoptionError("ownership adoption identity is invalid")
    desired = _documents_by_identity(artifact.rendered_yaml, namespace="loom-staging")
    live = _resources_by_identity(live_resources, namespace="loom-staging")
    if not live or not set(live) <= set(desired):
        raise ManifestOwnershipAdoptionError("ownership adoption resource set is incomplete")

    resources: list[AdoptionResource] = []
    for identity in sorted(live):
        desired_resource = desired[identity]
        live_resource = live[identity]
        metadata = _mapping(live_resource.get("metadata"), label="live metadata")
        uid = metadata.get("uid")
        resource_version = metadata.get("resourceVersion")
        generation = metadata.get("generation")
        managed_fields = metadata.get("managedFields")
        if (
            not isinstance(uid, str)
            or not uid
            or not isinstance(resource_version, str)
            or not resource_version.isdigit()
            or (generation is not None and (type(generation) is not int or generation < 1))
            or not isinstance(managed_fields, list)
            or not managed_fields
        ):
            raise ManifestOwnershipAdoptionError("live ownership identity is incomplete")
        _validate_managed_fields(managed_fields)
        overlay = _build_overlay(desired_resource, live_resource, identity=identity)
        resources.append(
            AdoptionResource(
                identity=identity,
                uid=uid,
                resource_version=resource_version,
                generation=generation,
                live_sha256=_hash_json(_canonical_live(live_resource)),
                managed_fields_sha256=_hash_json(managed_fields),
                desired_sha256=_hash_json(desired_resource),
                overlay_sha256=_hash_json(overlay),
                desired_json=json.dumps(desired_resource, sort_keys=True, separators=(",", ":")),
                overlay_json=json.dumps(overlay, sort_keys=True, separators=(",", ":")),
            )
        )

    provisional = ManifestOwnershipAdoptionPlan.__new__(ManifestOwnershipAdoptionPlan)
    object.__setattr__(provisional, "candidate_sha", candidate_sha)
    object.__setattr__(provisional, "candidate_tree", candidate_tree)
    object.__setattr__(provisional, "rendered_manifest_sha256", artifact.rendered_sha256)
    object.__setattr__(provisional, "mutation_epoch", mutation_epoch)
    object.__setattr__(provisional, "resources", tuple(resources))
    object.__setattr__(provisional, "plan_sha256", "0" * 64)
    return ManifestOwnershipAdoptionPlan(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        rendered_manifest_sha256=artifact.rendered_sha256,
        mutation_epoch=mutation_epoch,
        resources=tuple(resources),
        plan_sha256=_plan_digest(provisional),
    )


def verify_ownership_adoption_dry_run(
    plan: ManifestOwnershipAdoptionPlan,
    *,
    live_resources: Sequence[Mapping[str, object]],
    dry_run_resources: Sequence[Mapping[str, object]],
) -> str:
    """Prove the force-conflicts adoption changes no protected live spec."""
    live = _resources_by_identity(live_resources, namespace="loom-staging")
    dry_run = _resources_by_identity(dry_run_resources, namespace="loom-staging")
    targets = {resource.identity for resource in plan.resources}
    if set(live) != targets or set(dry_run) != targets:
        raise ManifestOwnershipAdoptionError("ownership dry-run resource set drifted")
    for resource in plan.resources:
        observed = live[resource.identity]
        metadata = _mapping(observed.get("metadata"), label="live metadata")
        if (
            metadata.get("uid") != resource.uid
            or metadata.get("resourceVersion") != resource.resource_version
            or metadata.get("generation") != resource.generation
            or _hash_json(_canonical_live(observed)) != resource.live_sha256
            or _hash_json(metadata.get("managedFields")) != resource.managed_fields_sha256
        ):
            raise ManifestOwnershipAdoptionError("live ownership prestate drifted")
        if ownership_semantic_state(dry_run[resource.identity]) != (
            ownership_semantic_state(observed)
        ):
            raise ManifestOwnershipAdoptionError("ownership adoption would change live state")
    return _hash_json(
        {
            "dry_run": {
                identity: ownership_semantic_state(dry_run[identity])
                for identity in sorted(targets)
            },
            "plan_sha256": plan.plan_sha256,
            "version": "v1",
        }
    )


def ownership_adoption_argv(
    *,
    kubeconfig: Path,
    dry_run: bool,
    output_json: bool = False,
) -> tuple[str, ...]:
    """Return the fixed maintenance-only SSA adoption command."""
    if not kubeconfig.is_absolute() or ".." in kubeconfig.parts:
        raise ValueError("ownership adoption kubeconfig is invalid")
    argv = [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "--namespace",
        "loom-staging",
        "apply",
        "--server-side=true",
        f"--field-manager={MANIFEST_FIELD_MANAGER}",
        "--force-conflicts",
    ]
    if dry_run:
        argv.append("--dry-run=server")
    if output_json:
        argv.extend(("--output", "json"))
    argv.extend(("--validate=strict", f"--request-timeout={MANIFEST_REQUEST_TIMEOUT}", "-f", "-"))
    return tuple(argv)


def _build_overlay(
    desired: Mapping[str, object],
    live: Mapping[str, object],
    *,
    identity: str,
) -> dict[str, object]:
    api_version, kind, namespace, name = identity.split("|", 3)
    desired_metadata = _mapping(desired.get("metadata"), label="desired metadata")
    live_metadata = _mapping(live.get("metadata"), label="live metadata")
    live_uid = live_metadata.get("uid")
    live_resource_version = live_metadata.get("resourceVersion")
    if (
        not isinstance(live_uid, str)
        or not live_uid
        or not isinstance(live_resource_version, str)
        or not live_resource_version.isdigit()
    ):
        raise ManifestOwnershipAdoptionError("live ownership precondition is incomplete")
    metadata: dict[str, object] = {
        "name": name,
        "resourceVersion": live_resource_version,
        "uid": live_uid,
    }
    if namespace:
        metadata["namespace"] = namespace
    for key in ("labels", "annotations"):
        if key in desired_metadata and key in live_metadata:
            projected = _project(desired_metadata[key], live_metadata[key])
            if projected is not _MISSING:
                metadata[key] = projected
    projected_body: dict[str, object] = {}
    for key in sorted(desired):
        if key in {"apiVersion", "kind", "metadata", "status"} or key not in live:
            continue
        projected = _project(desired[key], live[key])
        if projected is not _MISSING:
            projected_body[key] = projected
    if not projected_body:
        raise ManifestOwnershipAdoptionError("ownership adoption overlay is empty")
    if kind == "NetworkPolicy":
        desired_spec = _mapping(desired.get("spec"), label="desired spec")
        live_spec = _mapping(live.get("spec"), label="live spec")
        projected_spec = projected_body.get("spec")
        if not isinstance(projected_spec, dict):
            raise ManifestOwnershipAdoptionError("ownership adoption overlay is empty")
        for field in ("egress", "ingress"):
            if (
                desired_spec.get(field) == []
                and field not in live_spec
                and _legacy_manager_owns_spec_field(live_metadata, field)
            ):
                projected_spec[field] = []
    overlay: dict[str, object] = {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": metadata,
        **projected_body,
    }
    if desired.get("apiVersion") != api_version or desired.get("kind") != kind:
        raise ManifestOwnershipAdoptionError("desired ownership identity drifted")
    return overlay


def _legacy_manager_owns_spec_field(metadata: Mapping[str, object], field: str) -> bool:
    """Recognize an API-normalized empty NetworkPolicy field still owned by legacy SSA."""

    if field not in {"egress", "ingress"}:
        raise ValueError("network policy ownership field is invalid")
    managed_fields = metadata.get("managedFields")
    if not isinstance(managed_fields, list):
        return False
    for entry in managed_fields:
        if not isinstance(entry, dict):
            continue
        manager = entry.get("manager")
        fields_v1 = entry.get("fieldsV1")
        if (
            not isinstance(manager, str)
            or manager == MANIFEST_FIELD_MANAGER
            or manager not in _ALLOWED_MANAGERS
            or not isinstance(fields_v1, dict)
        ):
            continue
        spec = fields_v1.get("f:spec")
        if isinstance(spec, dict) and f"f:{field}" in spec:
            return True
    return False


class _Missing:
    pass


_MISSING = _Missing()


def _project(desired: object, live: object) -> object:
    if isinstance(desired, dict):
        if not isinstance(live, dict):
            return _MISSING
        result: dict[str, object] = {}
        for key in sorted(desired):
            if key not in live:
                continue
            value = _project(desired[key], live[key])
            if value is not _MISSING:
                result[key] = value
        return result if result else _MISSING
    if isinstance(desired, list):
        if not isinstance(live, list):
            return _MISSING
        if desired and all(
            isinstance(item, dict) and isinstance(item.get("name"), str) for item in desired
        ):
            live_by_name = {
                item["name"]: item
                for item in live
                if isinstance(item, dict) and isinstance(item.get("name"), str)
            }
            projected = []
            for item in desired:
                live_item = live_by_name.get(item["name"])
                if live_item is None:
                    continue
                value = _project(item, live_item)
                if value is not _MISSING:
                    projected.append(value)
            return projected if projected else _MISSING
        return copy.deepcopy(live)
    return copy.deepcopy(live)


def _validate_managed_fields(fields: list[object]) -> None:
    managers: set[str] = set()
    for entry in fields:
        if not isinstance(entry, dict):
            raise ManifestOwnershipAdoptionError("managed-field evidence is invalid")
        manager = entry.get("manager")
        if (
            not isinstance(manager, str)
            or manager not in _ALLOWED_MANAGERS
            or entry.get("operation") not in {"Apply", "Update"}
            or not isinstance(entry.get("fieldsV1"), dict)
        ):
            raise ManifestOwnershipAdoptionError("managed-field authority is unrecognized")
        managers.add(manager)
    if not managers - _CONTROLLER_ONLY_MANAGERS:
        raise ManifestOwnershipAdoptionError("recognized managed-field authority is absent")


def ownership_manifest_identities(rendered_yaml: str, *, namespace: str) -> tuple[str, ...]:
    """Return exact sorted rendered identities for maintenance live reads."""

    return tuple(sorted(_documents_by_identity(rendered_yaml, namespace=namespace)))


def _documents_by_identity(rendered_yaml: str, *, namespace: str) -> dict[str, dict[str, object]]:
    try:
        documents = tuple(yaml.safe_load_all(rendered_yaml))
    except yaml.YAMLError as exc:
        raise ManifestOwnershipAdoptionError("ownership manifest YAML is invalid") from exc
    resources = [item for item in documents if isinstance(item, dict)]
    return _resources_by_identity(resources, namespace=namespace)


def _resources_by_identity(
    resources: Sequence[Mapping[str, object]],
    *,
    namespace: str,
) -> dict[str, dict[str, object]]:
    if _DNS_RE.fullmatch(namespace) is None or len(resources) > 512:
        raise ManifestOwnershipAdoptionError("ownership resource input is invalid")
    indexed: dict[str, dict[str, object]] = {}
    for source in resources:
        resource = copy.deepcopy(dict(source))
        metadata = resource.get("metadata")
        if not isinstance(metadata, dict):
            raise ManifestOwnershipAdoptionError("ownership resource metadata is invalid")
        api_version = resource.get("apiVersion")
        kind = resource.get("kind")
        name = metadata.get("name")
        declared_namespace = metadata.get("namespace")
        if declared_namespace is None:
            resource_namespace: str | None = ""
        elif declared_namespace == namespace:
            resource_namespace = namespace
        else:
            resource_namespace = None
        if (
            not isinstance(api_version, str)
            or not api_version
            or not isinstance(kind, str)
            or not kind
            or not isinstance(name, str)
            or _DNS_RE.fullmatch(name) is None
            or resource_namespace is None
        ):
            raise ManifestOwnershipAdoptionError("ownership resource identity is invalid")
        identity = f"{api_version}|{kind}|{resource_namespace}|{name}"
        if identity in indexed:
            raise ManifestOwnershipAdoptionError("ownership resource identity is duplicated")
        indexed[identity] = resource
    return indexed


def _resource_identity(resource: Mapping[str, object]) -> str:
    metadata = resource.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    return (
        f"{resource.get('apiVersion')}|{resource.get('kind')}|"
        f"{metadata.get('namespace') or ''}|{metadata.get('name')}"
    )


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ManifestOwnershipAdoptionError(f"{label} is invalid")
    return value


def _canonical_live(resource: Mapping[str, object]) -> dict[str, object]:
    value = copy.deepcopy(dict(resource))
    value.pop("status", None)
    return value


def ownership_semantic_state(resource: Mapping[str, object]) -> dict[str, object]:
    """Return the single canonical semantic state for ownership convergence."""
    metadata = _mapping(resource.get("metadata"), label="semantic metadata")
    body = copy.deepcopy(dict(resource))
    for key in ("apiVersion", "kind", "metadata", "status"):
        body.pop(key, None)
    spec = body.get("spec")
    if resource.get("kind") == "NetworkPolicy" and isinstance(spec, dict):
        policy_types = spec.get("policyTypes")
        if isinstance(policy_types, list):
            if "Ingress" in policy_types:
                spec.setdefault("ingress", [])
            if "Egress" in policy_types:
                spec.setdefault("egress", [])
    return {
        "apiVersion": resource.get("apiVersion"),
        "kind": resource.get("kind"),
        "metadata": {
            "annotations": _semantic_annotations(metadata),
            "labels": metadata.get("labels", {}),
            "name": metadata.get("name"),
            "namespace": metadata.get("namespace"),
        },
        "body": body,
    }


def _semantic_annotations(metadata: Mapping[str, object]) -> dict[str, object]:
    value = metadata.get("annotations", {})
    if not isinstance(value, dict):
        raise ManifestOwnershipAdoptionError("semantic annotations are invalid")
    annotations = copy.deepcopy(value)
    annotations.pop(_LAST_APPLIED_ANNOTATION, None)
    return annotations


def _plan_digest(plan: ManifestOwnershipAdoptionPlan) -> str:
    return _hash_json(
        {
            "candidate_sha": plan.candidate_sha,
            "candidate_tree": plan.candidate_tree,
            "mutation_epoch": plan.mutation_epoch,
            "rendered_manifest_sha256": plan.rendered_manifest_sha256,
            "resources": [
                {
                    "generation": item.generation,
                    "identity": item.identity,
                    "desired_sha256": item.desired_sha256,
                    "live_sha256": item.live_sha256,
                    "managed_fields_sha256": item.managed_fields_sha256,
                    "overlay_sha256": item.overlay_sha256,
                    "resource_version": item.resource_version,
                    "uid": item.uid,
                }
                for item in plan.resources
            ],
            "version": "v1",
        }
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "NETWORK_POLICY_CONVERGENCE_TARGETS",
    "AdoptionResource",
    "ManifestOwnershipAdoptionError",
    "ManifestOwnershipAdoptionPlan",
    "build_manifest_ownership_adoption_plan",
    "ownership_adoption_argv",
    "ownership_manifest_identities",
    "ownership_semantic_state",
    "verify_ownership_adoption_dry_run",
]
