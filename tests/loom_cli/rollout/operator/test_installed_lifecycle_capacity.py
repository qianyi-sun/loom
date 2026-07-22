from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import ClusterConfig
from loom_cli.rollout.operator.installed_lifecycle_capacity import (
    InstalledLifecycleCapacityError,
    InstalledLifecycleCapacityService,
)
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config

_SHA = "a" * 40
_TREE = "b" * 40
_BASE = "c" * 40
_BUNDLE = "d" * 64
_RENDERED = "e" * 64
_IMAGE_ID = "sha256:" + "f" * 64
_TAG = "staging-aaaaaaa"
_UID = "12345678-1234-1234-1234-123456789abc"


class _Store:
    def __init__(self, active: object | None = None) -> None:
        self.active = active

    def read_active(self) -> object | None:
        return self.active


class _Commands:
    def __init__(self, capacity: dict[str, object]) -> None:
        self.capacity = capacity
        self.calls: list[tuple[str, ...]] = []
        self.job_metadata: dict[str, object] = {}

    def simple(self, argv):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        self.calls.append(command)
        if "logs" in command:
            stdout = json.dumps(
                {
                    "action": "capacity",
                    "capacity": self.capacity,
                    "gc": None,
                    "schema_version": 1,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        elif "get" in command:
            stdout = json.dumps(
                {
                    "metadata": self.job_metadata,
                    "status": {
                        "conditions": [{"status": "True", "type": "Complete"}],
                        "succeeded": 1,
                    },
                }
            )
        else:
            stdout = ""
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    def manifest_server_apply(self, rendered: str):  # type: ignore[no-untyped-def]
        document = json.loads(json.dumps(__import__("yaml").safe_load(rendered)))
        self.calls.append(("apply", document["metadata"]["name"]))
        self.job_metadata = {**document["metadata"], "uid": _UID}
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"metadata": self.job_metadata}),
            stderr="",
        )

    def lifecycle_capacity_wait(self, job_name: str):  # type: ignore[no-untyped-def]
        self.calls.append(("wait", job_name))
        return SimpleNamespace(returncode=0, stdout="", stderr="")


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha=_SHA,
        image_tag=_TAG,
        fetched_at="2026-07-20T00:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree=_TREE,
        approved_base_sha=_BASE,
    )


def _capacity() -> tuple[dict[str, object], ReadonlyDatabaseEvidence]:
    model = StagingCapacity(
        object_count=1939,
        bytes_used=2_074_200,
        disk_free_percent=26,
        inode_free_percent=75,
    )
    capacity: dict[str, object] = {
        "admission_allowed": True,
        "bytes_used": model.bytes_used,
        "disk_free_percent": model.disk_free_percent,
        "evidence_sha256": model.evidence_digest,
        "gc_required": False,
        "inode_free_percent": model.inode_free_percent,
        "object_count": model.object_count,
        "observed_at": "2026-07-20T00:00:00.640068+00:00",
        "policy_sha256": staging_capacity_policy_digest(),
    }
    database_capacity = {
        "environment": "staging",
        "namespace": "loom-staging",
        "object_count": model.object_count,
        "bytes_used": model.bytes_used,
        "disk_free_percent": model.disk_free_percent,
        "inode_free_percent": model.inode_free_percent,
        "policy_sha256": staging_capacity_policy_digest(),
        "evidence_sha256": model.evidence_digest,
        "source": "exact-object-inventory-v1",
        "observed_at_epoch": int(datetime(2026, 7, 20, 0, 0, 0, 640068, tzinfo=UTC).timestamp()),
    }
    database = ReadonlyDatabaseEvidence(
        schema_revision="0070",
        mutation_epoch=8,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 1,
            "provider_models": 1,
            "tasks": 1,
            "teams": 1,
            "users": 1,
        },
        capacity=database_capacity,
        evidence_sha256="1" * 64,
    )
    return capacity, database


def _service(
    tmp_path: Path,
    *,
    store: _Store | None = None,
) -> tuple[InstalledLifecycleCapacityService, _Commands]:
    config = replace(
        _config(tmp_path),
        source_mode="sealed-cumulative",
        source_commit_sha=_SHA,
        source_tree_sha=_TREE,
        source_base_sha=_BASE,
    )
    config.state_root.mkdir(mode=0o700)
    capacity, database = _capacity()
    commands = _Commands(capacity)
    rendered = render_manifests(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            image_tag=_TAG,
        )
    )
    loaded = SimpleNamespace(
        publication=SimpleNamespace(
            candidate_sha=_SHA,
            candidate_tree=_TREE,
            mutation_epoch=8,
            bundle_digest=_BUNDLE,
        ),
        images=SimpleNamespace(image_digests={"loom-control-plane": _IMAGE_ID}),
        manifests=SimpleNamespace(
            rendered_sha256=_RENDERED,
            rendered_yaml=rendered,
        ),
    )
    service = InstalledLifecycleCapacityService(
        config=config,
        service_uid=os.geteuid(),
        store=store or _Store(),
        bind_candidate=_candidate,
        read_mutation_epoch=lambda: 8,
        load_artifacts=lambda _candidate, _epoch: loaded,  # type: ignore[arg-type]
        commands=commands,
        read_database=lambda: database,
        now=lambda: datetime(2026, 7, 20, 0, 0, 1, tzinfo=UTC),
        expected_buckets=("trajectories", "artifacts"),
        expected_filesystem_paths=("/var/lib/loom-minio-capacity/0",),
    )
    return service, commands


def test_inventory_claim_and_execute_publish_exact_evidence(tmp_path: Path) -> None:
    service, commands = _service(tmp_path)
    plan = service.inventory()

    claimed = service.prepare_apply(approved_plan_digest=plan.plan_digest)
    assert claimed == plan
    evidence = service.execute_claimed(plan)

    assert evidence["plan_digest"] == plan.plan_digest
    assert evidence["mutation_epoch"] == 8
    assert evidence["job_uid"] == _UID
    assert (service.evidence_root / f"{plan.plan_digest}.claim.json").is_file()
    assert (service.evidence_root / f"{plan.plan_digest}.result.json").is_file()
    assert commands.calls[0] == (
        "kind",
        "load",
        "docker-image",
        "--name",
        "loom-staging",
        f"loom-control-plane:{_TAG}",
    )


def test_prepare_rejects_active_rollout_or_digest_drift_without_claim(tmp_path: Path) -> None:
    service, _commands = _service(tmp_path, store=_Store(active=object()))
    plan = service.inventory()

    with pytest.raises(InstalledLifecycleCapacityError, match="active rollout"):
        service.prepare_apply(approved_plan_digest=plan.plan_digest)
    assert not service.evidence_root.exists()

    inactive, _commands = _service(tmp_path / "other")
    with pytest.raises(InstalledLifecycleCapacityError, match="digest drifted"):
        inactive.prepare_apply(approved_plan_digest="0" * 64)
    assert not inactive.evidence_root.exists()


def test_claim_is_single_use_and_failed_execution_preserves_claim(tmp_path: Path) -> None:
    service, commands = _service(tmp_path)
    plan = service.inventory()
    service.prepare_apply(approved_plan_digest=plan.plan_digest)

    with pytest.raises(InstalledLifecycleCapacityError, match="already exists"):
        service.prepare_apply(approved_plan_digest=plan.plan_digest)
    commands.simple = lambda _argv: SimpleNamespace(  # type: ignore[method-assign]
        returncode=1,
        stdout="",
        stderr="sensitive details are not surfaced",
    )
    with pytest.raises(InstalledLifecycleCapacityError, match="image load failed"):
        service.execute_claimed(plan)
    assert (service.evidence_root / f"{plan.plan_digest}.claim.json").is_file()
    assert not (service.evidence_root / f"{plan.plan_digest}.result.json").exists()
