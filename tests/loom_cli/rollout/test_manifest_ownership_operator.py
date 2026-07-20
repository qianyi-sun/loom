from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.manifest_ownership_adoption import ManifestOwnershipAdoptionError
from loom_cli.rollout.manifest_ownership_operator import ManifestOwnershipOperator
from loom_cli.rollout.manifest_readiness import ManifestArtifact

_SHA = "1" * 40
_TREE = "2" * 40


def _desired() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "loom-staging-data-lifecycle",
                "namespace": "loom-staging",
                "labels": {"app": "loom-staging-data-lifecycle"},
            },
            "spec": {"schedule": "*/5 * * * *", "suspend": False},
        },
        *[
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": name, "namespace": "loom-staging"},
                "spec": {
                    "podSelector": {"matchLabels": {"app": name.removeprefix("loom-")}},
                    "policyTypes": ["Ingress", "Egress"],
                    "ingress": [{"from": [{"podSelector": {"matchLabels": {"app": "lifecycle"}}}]}],
                },
            }
            for name in (
                "loom-minio",
                "loom-postgres",
                "loom-staging-data-lifecycle",
            )
        ],
    ]


def _artifact() -> ManifestArtifact:
    rendered = yaml.safe_dump_all(_desired(), sort_keys=True)
    return ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        resource_count=4,
        resource_set_digest="3" * 64,
        image_identities={"loom-control-plane": "sha256:" + "4" * 64},
        artifact_digest="5" * 64,
    )


def _live() -> list[dict[str, object]]:
    live = copy.deepcopy(_desired())
    for index, item in enumerate(live, start=1):
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        metadata.update(
            {
                "generation": index,
                "managedFields": [
                    {
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:spec": {}},
                        "manager": (
                            "loom-lifecycle-bootstrap"
                            if index in {1, 4}
                            else "kubectl-client-side-apply"
                        ),
                        "operation": "Apply",
                    }
                ],
                "resourceVersion": str(100 + index),
                "uid": f"uid-{index}",
            }
        )
    cron = live[0]["spec"]
    assert isinstance(cron, dict)
    cron["suspend"] = True
    for item in live[1:]:
        spec = item["spec"]
        assert isinstance(spec, dict)
        spec.pop("ingress")
    return live


def _identity(resource: dict[str, object]) -> str:
    metadata = resource["metadata"]
    assert isinstance(metadata, dict)
    return f"{resource['apiVersion']}|{resource['kind']}|{metadata['namespace']}|{metadata['name']}"


@dataclass
class _Journal:
    events: list[dict[str, object]] = field(default_factory=list)
    inventories: list[dict[str, object]] = field(default_factory=list)

    def publish_inventory(self, request_id: str, inventory: dict[str, object]) -> None:
        self.inventories.append({"request_id": request_id, **copy.deepcopy(inventory)})

    def append(self, request_id: str, event: dict[str, object]) -> None:
        self.events.append({"request_id": request_id, **copy.deepcopy(event)})


@dataclass
class _Harness:
    live: list[dict[str, object]] = field(default_factory=_live)
    journal: _Journal = field(default_factory=_Journal)
    epoch_claims: list[tuple[int, str, str]] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)
    fail_force_apply: bool = False
    claimed_epoch: int | None = None
    post_apply_stale_reads: int = 0
    stale_live: list[dict[str, object]] | None = None
    settle_calls: list[float] = field(default_factory=list)

    def load_live(self):
        self.calls.append("load-live")
        if self.stale_live is not None and self.post_apply_stale_reads > 0:
            self.post_apply_stale_reads -= 1
            return copy.deepcopy(self.stale_live)
        return copy.deepcopy(self.live)

    def force_dry_run(self, payload: str):
        self.calls.append("force-dry-run")
        assert len(tuple(yaml.safe_load_all(payload))) == 4
        return copy.deepcopy(self.live)

    def force_apply(self, payload: str):
        self.calls.append("force-apply")
        if self.fail_force_apply:
            raise RuntimeError("injected force apply failure")
        assert all(
            document["metadata"]["resourceVersion"] for document in yaml.safe_load_all(payload)
        )
        return copy.deepcopy(self.live)

    def no_force_dry_run(self, payload: str):
        documents = [item for item in yaml.safe_load_all(payload) if isinstance(item, dict)]
        self.calls.append(f"no-force-dry-run:{len(documents)}")
        return copy.deepcopy(documents)

    def no_force_apply(self, payload: str):
        documents = [item for item in yaml.safe_load_all(payload) if isinstance(item, dict)]
        self.calls.append(f"no-force-apply:{len(documents)}")
        self.stale_live = copy.deepcopy(self.live)
        desired_by_identity = {_identity(item): item for item in documents}
        self.live = [
            copy.deepcopy(desired_by_identity.get(_identity(item), item)) for item in self.live
        ]
        return copy.deepcopy(documents)

    def claim_epoch(self, epoch: int, request_id: str, inventory_sha256: str) -> int:
        self.calls.append("claim-epoch")
        self.epoch_claims.append((epoch, request_id, inventory_sha256))
        return epoch + 1 if self.claimed_epoch is None else self.claimed_epoch

    def operator(self) -> ManifestOwnershipOperator:
        return ManifestOwnershipOperator(
            artifact=_artifact(),
            candidate_sha=_SHA,
            candidate_tree=_TREE,
            read_mutation_epoch=lambda: 2,
            load_live=self.load_live,
            force_dry_run=self.force_dry_run,
            force_apply=self.force_apply,
            no_force_dry_run=self.no_force_dry_run,
            no_force_apply=self.no_force_apply,
            claim_epoch=self.claim_epoch,
            journal=self.journal,
            now=lambda: datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
            settle=self.settle_calls.append,
        )


def test_inventory_is_immutable_and_apply_is_journaled() -> None:
    harness = _Harness()
    inventory = harness.operator().inventory()
    original_live = inventory.live_resources
    harness.live[0]["spec"]["suspend"] = False  # type: ignore[index]
    assert inventory.live_resources == original_live

    harness = _Harness()
    operator = harness.operator()
    approved = operator.inventory().inventory_sha256
    result = operator.apply(
        request_id="req-manifest-ownership-12345678",
        approved_inventory_sha256=approved,
    )

    assert result["mutation_epoch_before"] == 2
    assert result["mutation_epoch_after"] == 3
    assert harness.journal.inventories[0]["inventory_sha256"] == approved
    assert [event["event"] for event in harness.journal.events] == [
        "inventory-approved",
        "epoch-claimed",
        "ownership-adopted",
        "network-policies-converged",
        "live-state-verified",
        "completed",
    ]
    cron = harness.live[0]["spec"]
    assert isinstance(cron, dict)
    assert cron["suspend"] is True
    assert "no-force-apply:3" in harness.calls
    assert "no-force-dry-run:4" in harness.calls


def test_inventory_digest_drift_blocks_before_epoch_or_mutation() -> None:
    harness = _Harness()
    with pytest.raises(ManifestOwnershipAdoptionError, match="inventory drifted"):
        harness.operator().apply(
            request_id="req-manifest-ownership-12345678",
            approved_inventory_sha256="f" * 64,
        )
    assert harness.epoch_claims == []
    assert "force-apply" not in harness.calls
    assert harness.journal.events == []
    assert harness.journal.inventories == []


def test_epoch_drift_and_force_failure_are_terminally_journaled() -> None:
    harness = _Harness(claimed_epoch=9)
    operator = harness.operator()
    approved = operator.inventory().inventory_sha256
    with pytest.raises(ManifestOwnershipAdoptionError, match="epoch claim drifted"):
        operator.apply(
            request_id="req-manifest-ownership-12345678",
            approved_inventory_sha256=approved,
        )
    assert [event["event"] for event in harness.journal.events] == [
        "inventory-approved",
        "failed",
    ]
    assert harness.journal.events[-1]["evidence"]["failure_code"] == (
        "manifest_ownership.epoch-claim.failed"
    )
    assert "force-apply" not in harness.calls

    harness = _Harness(fail_force_apply=True)
    operator = harness.operator()
    approved = operator.inventory().inventory_sha256
    with pytest.raises(RuntimeError, match="injected"):
        operator.apply(
            request_id="req-manifest-ownership-abcdefgh",
            approved_inventory_sha256=approved,
        )
    assert [event["event"] for event in harness.journal.events] == [
        "inventory-approved",
        "epoch-claimed",
        "failed",
    ]
    assert harness.journal.events[-1]["evidence"]["failure_code"] == (
        "manifest_ownership.ownership-adoption.failed"
    )
    assert not any(call.startswith("no-force") for call in harness.calls)


def test_post_apply_live_drift_fails_closed() -> None:
    harness = _Harness()

    def drift_after_apply(payload: str):
        result = harness.no_force_apply(payload)
        cron = harness.live[0]["spec"]
        assert isinstance(cron, dict)
        cron["suspend"] = False
        return result

    operator = harness.operator()
    operator.no_force_apply = drift_after_apply
    approved = operator.inventory().inventory_sha256
    with pytest.raises(ManifestOwnershipAdoptionError, match="live state drifted"):
        operator.apply(
            request_id="req-manifest-ownership-12345678",
            approved_inventory_sha256=approved,
        )
    assert harness.journal.events[-1]["event"] == "failed"
    assert harness.journal.events[-1]["evidence"]["failure_code"] == (
        "manifest_ownership.live-state-verification.failed"
    )


def test_post_apply_readback_retries_only_within_bounded_readonly_window() -> None:
    harness = _Harness(post_apply_stale_reads=1)
    operator = harness.operator()
    approved = operator.inventory().inventory_sha256

    result = operator.apply(
        request_id="req-manifest-ownership-12345678",
        approved_inventory_sha256=approved,
    )

    assert result["mutation_epoch_after"] == 3
    verified = next(
        event for event in harness.journal.events if event["event"] == "live-state-verified"
    )
    evidence = verified["evidence"]
    assert isinstance(evidence, dict)
    assert evidence["attempts"] == 2
    assert harness.settle_calls == [0.25]


def test_post_apply_readback_fails_closed_after_bounded_stale_views() -> None:
    harness = _Harness(post_apply_stale_reads=3)
    operator = harness.operator()
    approved = operator.inventory().inventory_sha256

    with pytest.raises(ManifestOwnershipAdoptionError, match="post-apply"):
        operator.apply(
            request_id="req-manifest-ownership-12345678",
            approved_inventory_sha256=approved,
        )

    assert harness.settle_calls == [0.25, 0.75]
    assert harness.journal.events[-1]["event"] == "failed"
    assert harness.journal.events[-1]["evidence"]["failure_code"] == (
        "manifest_ownership.live-state-verification.failed"
    )
