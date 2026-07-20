"""Journaled maintenance orchestration for exact manifest ownership adoption."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, cast

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.manifest_ownership_adoption import (
    NETWORK_POLICY_CONVERGENCE_TARGETS,
    ManifestOwnershipAdoptionError,
    ManifestOwnershipAdoptionPlan,
    build_manifest_ownership_adoption_plan,
    ownership_semantic_state,
    verify_ownership_adoption_dry_run,
)
from loom_cli.rollout.manifest_readiness import ManifestArtifact

_REQUEST_RE = re.compile(r"^req-manifest-ownership-[a-z0-9]{8,32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_POST_APPLY_READ_DELAYS = (0.25, 0.75)

Resource = Mapping[str, object]
ResourceSet = Sequence[Resource]
LiveLoader = Callable[[], ResourceSet]
ManifestAction = Callable[[str], ResourceSet]
EpochClaim = Callable[[int, str, str], int]


class OwnershipJournal(Protocol):
    def publish_inventory(
        self,
        request_id: str,
        inventory: Mapping[str, object],
    ) -> None: ...

    def append(self, request_id: str, event: Mapping[str, object]) -> None: ...


@dataclass(frozen=True, slots=True)
class OwnershipInventory:
    plan: ManifestOwnershipAdoptionPlan
    dry_run_sha256: str
    inventory_sha256: str
    live_json: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.dry_run_sha256) is None
            or _SHA256_RE.fullmatch(self.inventory_sha256) is None
            or self.inventory_sha256
            != _hash_json(
                {
                    "dry_run_sha256": self.dry_run_sha256,
                    "plan_sha256": self.plan.plan_sha256,
                    "version": "v1",
                }
            )
            or len(self.live_json) != len(self.plan.resources)
        ):
            raise ValueError("ownership inventory identity is invalid")
        observed: dict[str, dict[str, object]] = {}
        for payload in self.live_json:
            try:
                resource = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise ValueError("ownership inventory live state is invalid") from exc
            if not isinstance(resource, dict) or payload != json.dumps(
                resource, sort_keys=True, separators=(",", ":")
            ):
                raise ValueError("ownership inventory live state is invalid")
            identity = _identity(resource)
            if identity in observed:
                raise ValueError("ownership inventory live state is duplicated")
            observed[identity] = resource
        if set(observed) != {item.identity for item in self.plan.resources}:
            raise ValueError("ownership inventory live state is incomplete")
        for item in self.plan.resources:
            live = dict(observed[item.identity])
            live.pop("status", None)
            if _hash_json(live) != item.live_sha256:
                raise ValueError("ownership inventory live state drifted")

    @property
    def live_resources(self) -> tuple[Resource, ...]:
        resources: list[Resource] = []
        for payload in self.live_json:
            resource = json.loads(payload)
            if not isinstance(resource, dict):  # guarded by __post_init__
                raise RuntimeError("ownership inventory live state drifted")
            resources.append(resource)
        return tuple(resources)

    def to_document(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "action": "inventory",
            "candidate_sha": self.plan.candidate_sha,
            "candidate_tree": self.plan.candidate_tree,
            "rendered_manifest_sha256": self.plan.rendered_manifest_sha256,
            "mutation_epoch": self.plan.mutation_epoch,
            "plan_sha256": self.plan.plan_sha256,
            "dry_run_sha256": self.dry_run_sha256,
            "inventory_sha256": self.inventory_sha256,
            "resources": [
                {
                    "identity": item.identity,
                    "uid": item.uid,
                    "resource_version": item.resource_version,
                    "generation": item.generation,
                    "live_sha256": item.live_sha256,
                    "managed_fields_sha256": item.managed_fields_sha256,
                    "desired_sha256": item.desired_sha256,
                    "overlay_sha256": item.overlay_sha256,
                }
                for item in self.plan.resources
            ],
        }


@dataclass(slots=True)
class ManifestOwnershipOperator:
    artifact: ManifestArtifact
    candidate_sha: str
    candidate_tree: str
    read_mutation_epoch: Callable[[], int]
    load_live: LiveLoader
    force_dry_run: ManifestAction
    force_apply: ManifestAction
    no_force_dry_run: ManifestAction
    no_force_apply: ManifestAction
    claim_epoch: EpochClaim
    journal: OwnershipJournal
    now: Callable[[], datetime]
    settle: Callable[[float], None] = time.sleep

    def inventory(self) -> OwnershipInventory:
        epoch = self.read_mutation_epoch()
        live = tuple(self.load_live())
        plan = build_manifest_ownership_adoption_plan(
            artifact=self.artifact,
            live_resources=live,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            mutation_epoch=epoch,
        )
        dry_run = tuple(self.force_dry_run(plan.overlay_yaml))
        dry_run_sha256 = verify_ownership_adoption_dry_run(
            plan,
            live_resources=live,
            dry_run_resources=dry_run,
        )
        return OwnershipInventory(
            plan=plan,
            dry_run_sha256=dry_run_sha256,
            inventory_sha256=_hash_json(
                {
                    "dry_run_sha256": dry_run_sha256,
                    "plan_sha256": plan.plan_sha256,
                    "version": "v1",
                }
            ),
            live_json=tuple(
                json.dumps(item, sort_keys=True, separators=(",", ":")) for item in live
            ),
        )

    def apply(self, *, request_id: str, approved_inventory_sha256: str) -> dict[str, object]:
        if (
            _REQUEST_RE.fullmatch(request_id) is None
            or _SHA256_RE.fullmatch(approved_inventory_sha256) is None
        ):
            raise ManifestOwnershipAdoptionError("ownership apply authority is invalid")
        inventory = self.inventory()
        if inventory.inventory_sha256 != approved_inventory_sha256:
            raise ManifestOwnershipAdoptionError("approved ownership inventory drifted")
        self.journal.publish_inventory(request_id, inventory.to_document())
        self._record(
            request_id,
            "inventory-approved",
            {
                "inventory_sha256": inventory.inventory_sha256,
                "plan_sha256": inventory.plan.plan_sha256,
                "starting_epoch": inventory.plan.mutation_epoch,
            },
        )
        stage = "epoch-claim"
        try:
            observed_epoch = self.claim_epoch(
                inventory.plan.mutation_epoch,
                request_id,
                inventory.inventory_sha256,
            )
            if observed_epoch != inventory.plan.mutation_epoch + 1:
                raise ManifestOwnershipAdoptionError("ownership mutation epoch claim drifted")
            self._record(request_id, "epoch-claimed", {"observed_epoch": observed_epoch})

            stage = "ownership-adoption"
            adopted = tuple(self.force_apply(inventory.plan.overlay_yaml))
            adoption_sha256 = verify_ownership_adoption_dry_run(
                inventory.plan,
                live_resources=inventory.live_resources,
                dry_run_resources=adopted,
            )
            self._record(
                request_id,
                "ownership-adopted",
                {"adoption_sha256": adoption_sha256},
            )

            stage = "network-policy-convergence"
            network_yaml = _network_policy_yaml(inventory.plan)
            expected_network = tuple(self.no_force_dry_run(network_yaml))
            applied_network = tuple(self.no_force_apply(network_yaml))
            network_sha256 = _verify_network_convergence(
                expected_network=expected_network,
                applied_network=applied_network,
            )
            self._record(
                request_id,
                "network-policies-converged",
                {"network_sha256": network_sha256},
            )

            stage = "live-state-verification"
            post_apply_sha256, post_apply_attempts = self._verify_live_convergence(inventory)
            self._record(
                request_id,
                "live-state-verified",
                {
                    "attempts": post_apply_attempts,
                    "post_apply_sha256": post_apply_sha256,
                },
            )

            stage = "final-no-force-dry-run"
            final_dry_run = tuple(self.no_force_dry_run(self.artifact.rendered_yaml))
            if len(final_dry_run) != self.artifact.resource_count:
                raise ManifestOwnershipAdoptionError(
                    "final ownership dry-run resource count drifted"
                )
            final_sha256 = _hash_json([ownership_semantic_state(item) for item in final_dry_run])
            self._record(
                request_id,
                "completed",
                {
                    "final_dry_run_sha256": final_sha256,
                    "observed_epoch": observed_epoch,
                },
            )
        except Exception as exc:
            self._record(
                request_id,
                "failed",
                {
                    "failure_class": type(exc).__name__,
                    "failure_code": f"manifest_ownership.{stage}.failed",
                },
            )
            raise
        return {
            "schema_version": 1,
            "action": "apply",
            "request_id": request_id,
            "candidate_sha": inventory.plan.candidate_sha,
            "candidate_tree": inventory.plan.candidate_tree,
            "inventory_sha256": inventory.inventory_sha256,
            "mutation_epoch_before": inventory.plan.mutation_epoch,
            "mutation_epoch_after": observed_epoch,
            "adoption_sha256": adoption_sha256,
            "network_sha256": network_sha256,
            "post_apply_sha256": post_apply_sha256,
            "final_dry_run_sha256": final_sha256,
        }

    def _verify_live_convergence(self, inventory: OwnershipInventory) -> tuple[str, int]:
        for attempt in range(1, len(_POST_APPLY_READ_DELAYS) + 2):
            try:
                return (
                    _verify_post_apply_live(
                        inventory=inventory,
                        live_resources=tuple(self.load_live()),
                    ),
                    attempt,
                )
            except ManifestOwnershipAdoptionError:
                if attempt > len(_POST_APPLY_READ_DELAYS):
                    raise
                self.settle(_POST_APPLY_READ_DELAYS[attempt - 1])
        raise RuntimeError("ownership live convergence attempt accounting drifted")

    def _record(self, request_id: str, name: str, evidence: Mapping[str, object]) -> None:
        observed_at = self.now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("ownership journal clock is invalid")
        self.journal.append(
            request_id,
            {
                "event": name,
                "observed_at": observed_at.isoformat(),
                "evidence": dict(evidence),
            },
        )


def _network_policy_yaml(plan: ManifestOwnershipAdoptionPlan) -> str:
    documents = [
        dict(item.desired)
        for item in plan.resources
        if item.identity in NETWORK_POLICY_CONVERGENCE_TARGETS
    ]
    if len(documents) != 3:
        raise ManifestOwnershipAdoptionError("network policy convergence set is invalid")
    return cast(str, yaml.safe_dump_all(documents, sort_keys=True, explicit_start=True))


def _verify_post_apply_live(
    *,
    inventory: OwnershipInventory,
    live_resources: ResourceSet,
) -> str:
    observed = {_identity(item): ownership_semantic_state(item) for item in live_resources}
    expected: dict[str, dict[str, object]] = {}
    for resource in inventory.plan.resources:
        if resource.identity in NETWORK_POLICY_CONVERGENCE_TARGETS:
            expected[resource.identity] = ownership_semantic_state(resource.desired)
        else:
            source = next(
                (item for item in inventory.live_resources if _identity(item) == resource.identity),
                None,
            )
            if source is None:
                raise ManifestOwnershipAdoptionError(
                    "ownership post-apply CronJob prestate is absent"
                )
            expected[resource.identity] = ownership_semantic_state(source)
    if set(observed) != set(expected) or observed != expected:
        raise ManifestOwnershipAdoptionError("ownership post-apply live state drifted")
    return _hash_json(observed)


def _verify_network_convergence(
    *,
    expected_network: ResourceSet,
    applied_network: ResourceSet,
) -> str:
    expected = sorted((ownership_semantic_state(item) for item in expected_network), key=_identity)
    applied = sorted((ownership_semantic_state(item) for item in applied_network), key=_identity)
    if len(expected) != 3 or expected != applied:
        raise ManifestOwnershipAdoptionError("network policy convergence drifted")
    return _hash_json(expected)


def _identity(resource: Mapping[str, object]) -> str:
    metadata = resource.get("metadata")
    if not isinstance(metadata, dict):
        raise ManifestOwnershipAdoptionError("ownership operation identity is invalid")
    return (
        f"{resource.get('apiVersion')}|{resource.get('kind')}|"
        f"{metadata.get('namespace') or ''}|{metadata.get('name')}"
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "ManifestOwnershipOperator",
    "OwnershipInventory",
    "OwnershipJournal",
]
