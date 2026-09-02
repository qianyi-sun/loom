from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import loom_cli.rollout.operator.installed_rollout_capacity_refresh as refresh_module
from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import ClusterConfig
from loom_cli.rollout.operator.installed_rollout_capacity_refresh import (
    InstalledRolloutCapacityRefresh,
    InstalledRolloutCapacityRefreshError,
    build_installed_rollout_capacity_job_plan,
)
from loom_cli.rollout.operator.lifecycle_capacity_job import (
    LifecycleCapacityJobPlan,
    build_rollout_capacity_job_plan,
)
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config

_SHA = "a" * 40
_TREE = "b" * 40
_UID = "12345678-1234-1234-1234-123456789abc"
_NOW = datetime(2026, 7, 20, 0, 0, 1, tzinfo=UTC)


def _guard(
    *,
    state: str = "ready",
    mutation_epoch: int = 8,
) -> MutationGuardEvidence:
    return MutationGuardEvidence.build(
        request_id="req-1111111111111111",
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        generation="1" * 32,
        mutation_epoch=mutation_epoch,
        guard_pid=1234,
        database_backend_pid=4321,
        deadline_unix_seconds=2_000_000_000,
        cronjob_uid="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        suspended_resource_version="12345",
        state=state,  # type: ignore[arg-type]
    )


def _final_plan() -> SimpleNamespace:
    return SimpleNamespace(
        request_id="req-1111111111111111",
        attempt_number=2,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        attestation_digest="3" * 64,
        starting_mutation_epoch=8,
        plan_digest="f" * 64,
        environment="staging",
        namespace="loom-staging",
    )


def _job_plan() -> LifecycleCapacityJobPlan:
    rendered = render_manifests(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            image_tag="staging-aaaaaaa",
        )
    )
    return build_rollout_capacity_job_plan(
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=9,
        artifact_bundle_sha256="c" * 64,
        rendered_manifest_sha256="d" * 64,
        control_plane_image_id="sha256:" + "e" * 64,
        image_tag="staging-aaaaaaa",
        rendered_yaml=rendered,
        expected_buckets=("trajectories", "artifacts"),
        expected_filesystem_paths=("/var/lib/loom-minio-capacity/0",),
        request_id="req-1111111111111111",
        attempt_number=2,
        rollout_plan_digest="f" * 64,
        guard_generation="1" * 32,
        guard_backend_pid=4321,
    )


def _capacity(
    *, observed_at: datetime = _NOW - timedelta(seconds=1)
) -> tuple[dict[str, object], ReadonlyDatabaseEvidence]:
    model = StagingCapacity(
        object_count=79,
        bytes_used=546_856,
        disk_free_percent=99,
        inode_free_percent=99,
    )
    capacity: dict[str, object] = {
        "admission_allowed": True,
        "bytes_used": model.bytes_used,
        "disk_free_percent": model.disk_free_percent,
        "evidence_sha256": model.evidence_digest,
        "gc_required": False,
        "inode_free_percent": model.inode_free_percent,
        "object_count": model.object_count,
        "observed_at": observed_at.isoformat(),
        "policy_sha256": staging_capacity_policy_digest(),
    }
    database = ReadonlyDatabaseEvidence(
        schema_revision="0070",
        mutation_epoch=9,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={
            "agents": 1,
            "provider_models": 1,
            "tasks": 1,
            "teams": 1,
            "users": 1,
        },
        capacity={
            "environment": "staging",
            "namespace": "loom-staging",
            "object_count": model.object_count,
            "bytes_used": model.bytes_used,
            "disk_free_percent": model.disk_free_percent,
            "inode_free_percent": model.inode_free_percent,
            "policy_sha256": staging_capacity_policy_digest(),
            "evidence_sha256": model.evidence_digest,
            "source": "exact-object-inventory-v1",
            "observed_at_epoch": int(observed_at.timestamp()),
        },
        evidence_sha256="2" * 64,
    )
    return capacity, database


def _materialize_kubernetes_job_defaults(job: dict[str, object]) -> None:
    metadata = job["metadata"]
    spec = job["spec"]
    assert isinstance(metadata, dict)
    assert isinstance(spec, dict)
    job_name = metadata["name"]
    job_uid = metadata["uid"]
    controller_labels = {
        "batch.kubernetes.io/controller-uid": job_uid,
        "batch.kubernetes.io/job-name": job_name,
        "controller-uid": job_uid,
        "job-name": job_name,
    }
    metadata.update(
        {
            "creationTimestamp": "2026-07-20T00:00:00Z",
            "generation": 1,
            "resourceVersion": "12345",
        }
    )
    spec.update(
        {
            "completionMode": "NonIndexed",
            "completions": 1,
            "manualSelector": False,
            "parallelism": 1,
            "podReplacementPolicy": "TerminatingOrFailed",
            "selector": {
                "matchLabels": {"batch.kubernetes.io/controller-uid": job_uid}
            },
            "suspend": False,
        }
    )
    template = spec["template"]
    assert isinstance(template, dict)
    template_metadata = template["metadata"]
    template_spec = template["spec"]
    assert isinstance(template_metadata, dict)
    assert isinstance(template_spec, dict)
    template_labels = template_metadata["labels"]
    assert isinstance(template_labels, dict)
    template_labels.update(controller_labels)
    template_spec.update(
        {
            "dnsPolicy": "ClusterFirst",
            "schedulerName": "default-scheduler",
            "terminationGracePeriodSeconds": 30,
        }
    )
    containers = template_spec["containers"]
    assert isinstance(containers, list)
    container = containers[0]
    assert isinstance(container, dict)
    container.update(
        {
            "imagePullPolicy": "IfNotPresent",
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
        }
    )
    resources = container["resources"]
    assert isinstance(resources, dict)
    limits = resources["limits"]
    assert isinstance(limits, dict)
    limits["cpu"] = "1"


class _Commands:
    def __init__(
        self,
        plan: LifecycleCapacityJobPlan,
        capacity: dict[str, object],
        *,
        existing: bool,
    ) -> None:
        self.plan = plan
        self.capacity = capacity
        self.calls: list[tuple[str, ...]] = []
        self.applies = 0
        document = yaml.safe_load(plan.job_manifest)
        self.live_job = {
            **document,
            "metadata": {**document["metadata"], "uid": _UID},
            "status": {
                "conditions": [{"status": "True", "type": "Complete"}],
                "succeeded": 1,
            },
        }
        _materialize_kubernetes_job_defaults(self.live_job)
        self.existing = existing

    def simple(self, argv):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        self.calls.append(command)
        if "logs" in command:
            output = {
                "action": "rollout-capacity",
                "capacity": self.capacity,
                "gc": None,
                "schema_version": 1,
            }
            stdout = json.dumps(output, sort_keys=True, separators=(",", ":"))
        elif "get" in command and "--ignore-not-found" in command and not self.existing:
            stdout = ""
        elif "get" in command:
            stdout = json.dumps(self.live_job)
        else:
            stdout = ""
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    def manifest_server_apply(self, rendered: str):  # type: ignore[no-untyped-def]
        assert rendered == self.plan.job_manifest
        self.calls.append(("apply", self.plan.job_name))
        self.applies += 1
        self.existing = True
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"metadata": self.live_job["metadata"]}),
            stderr="",
        )

    def lifecycle_capacity_wait(self, job_name: str):
        self.calls.append(("wait", job_name))
        return SimpleNamespace(returncode=0, stdout="", stderr="")


def _service(
    tmp_path: Path,
    *,
    existing: bool = False,
    observed_at: datetime = _NOW - timedelta(seconds=1),
    guards: list[MutationGuardEvidence] | None = None,
) -> tuple[InstalledRolloutCapacityRefresh, _Commands]:
    config = replace(_config(tmp_path), kubeconfig_path=Path("/etc/loom/kubeconfig-protected"))
    plan = _job_plan()
    capacity, database = _capacity(observed_at=observed_at)
    commands = _Commands(plan, capacity, existing=existing)
    evidence = iter(guards or [_guard(), _guard()])
    service = InstalledRolloutCapacityRefresh(
        config=config,
        service_uid=1000,
        commands=commands,
        read_guard=lambda _plan: next(evidence),
        build_job_plan=lambda _plan, _guard: plan,
        read_database=lambda: database,
        now=lambda: _NOW,
    )
    return service, commands


def test_refresh_applies_waits_and_verifies_fresh_database_publication(tmp_path: Path) -> None:
    service, commands = _service(tmp_path)

    digest = service(_final_plan())  # type: ignore[arg-type]

    assert len(digest) == 64
    assert commands.applies == 1
    assert ("wait", _job_plan().job_name) in commands.calls


def test_refresh_accepts_advanced_guard_with_exact_recovery_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery_calls: list[dict[str, object]] = []

    def find_recovery(state_root: Path, **bindings: object) -> int:
        recovery_calls.append({"state_root": state_root, **bindings})
        return 1

    monkeypatch.setattr(
        refresh_module,
        "find_advanced_epoch_attempt",
        find_recovery,
        raising=False,
    )
    advanced_guard = _guard(mutation_epoch=9)
    service, commands = _service(
        tmp_path,
        guards=[advanced_guard, advanced_guard],
    )

    digest = service(_final_plan())  # type: ignore[arg-type]

    assert len(digest) == 64
    assert commands.applies == 1
    assert recovery_calls == [
        {
            "state_root": service.config.state_root,
            "request_id": "req-1111111111111111",
            "through_attempt": 1,
            "candidate_sha": _SHA,
            "attestation_digest": "3" * 64,
            "starting_mutation_epoch": 8,
            "service_uid": 1000,
        }
    ] * 2


def test_refresh_rejects_unproven_advanced_guard_before_job_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        refresh_module,
        "find_advanced_epoch_attempt",
        lambda *_args, **_kwargs: None,
        raising=False,
    )
    advanced_guard = _guard(mutation_epoch=9)
    service, commands = _service(
        tmp_path,
        guards=[advanced_guard, advanced_guard],
    )

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="guard identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_recovers_exact_existing_same_attempt_job_without_reapplying(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)

    digest = service(_final_plan())  # type: ignore[arg-type]

    assert len(digest) == 64
    assert commands.applies == 0


def test_refresh_rejects_drifted_existing_job_before_apply(tmp_path: Path) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["metadata"]["annotations"]["loom.carin.dev/rollout-plan"] = "0" * 64

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_accepts_kubernetes_job_metadata_without_controller_labels(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)

    digest = service(_final_plan())  # type: ignore[arg-type]

    assert len(digest) == 64
    assert commands.applies == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("completions", 2),
        ("parallelism", 2),
        ("completionMode", "Indexed"),
        ("manualSelector", True),
        ("suspend", True),
    ],
)
def test_refresh_rejects_job_execution_default_drift_before_recovery(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"][field] = value

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_job_selector_identity_drift_before_recovery(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"]["selector"]["matchLabels"][
        "batch.kubernetes.io/controller-uid"
    ] = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_unplanned_job_controller_before_recovery(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"]["managedBy"] = "example.com/unplanned-controller"

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_controller_label_identity_drift_before_recovery(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"]["template"]["metadata"]["labels"][
        "controller-uid"
    ] = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_unplanned_dns_configuration_before_recovery(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"]["template"]["spec"]["dnsConfig"] = {
        "nameservers": ["192.0.2.53"]
    }

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_nested_volume_mount_drift_before_recovery(
    tmp_path: Path,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    mounts = commands.live_job["spec"]["template"]["spec"]["containers"][0][
        "volumeMounts"
    ]
    commands.live_job["spec"]["template"]["spec"]["containers"][0][
        "volumeMounts"
    ] = [*copy.deepcopy(mounts)]
    commands.live_job["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][
        0
    ]["mountPropagation"] = "Bidirectional"

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "initContainers",
            [
                {
                    "name": "injected",
                    "image": "busybox:latest",
                    "command": ["sh", "-c", "true"],
                }
            ],
        ),
        ("hostNetwork", True),
    ],
)
def test_refresh_rejects_unplanned_pod_authority_before_recovery(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    service, commands = _service(tmp_path, existing=True)
    commands.live_job["spec"]["template"]["spec"][field] = value

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_rejects_stale_existing_job_instead_of_reusing_it(tmp_path: Path) -> None:
    service, commands = _service(
        tmp_path,
        existing=True,
        observed_at=_NOW - timedelta(minutes=6),
    )

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="evidence is stale"):
        service(_final_plan())  # type: ignore[arg-type]

    assert commands.applies == 0


def test_refresh_requires_same_guard_to_remain_ready_after_publication(tmp_path: Path) -> None:
    service, _commands = _service(tmp_path, guards=[_guard(), _guard(state="released")])

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="guard identity drifted"):
        service(_final_plan())  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("guard_epoch", "recovery_attempt"),
    [(8, None), (9, 1)],
    ids=("original-epoch", "advanced-epoch-recovery"),
)
def test_installed_job_plan_reads_exact_final_artifacts_and_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_epoch: int,
    recovery_attempt: int | None,
) -> None:
    rendered = render_manifests(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            image_tag="staging-aaaaaaa",
        )
    )
    artifact_root = tmp_path / "artifact"
    artifact_root.mkdir(mode=0o700)
    descriptor_path = artifact_root / "artifact.json"
    rendered_path = artifact_root / "rendered.yaml"
    descriptor = {
        "candidate_sha": _SHA,
        "candidate_tree": _TREE,
        "container_registry": "",
        "image_digests": {"loom-control-plane": "sha256:" + "e" * 64},
        "mutation_epoch": 8,
        "registry_digests": {},
    }
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
    rendered_path.write_text(rendered, encoding="utf-8")
    descriptor_path.chmod(0o600)
    rendered_path.chmod(0o600)
    publication = SimpleNamespace(
        bundle_digest="c" * 64,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        container_registry="",
        descriptor_path=descriptor_path,
        mutation_epoch=8,
        rendered_manifest_path=rendered_path,
        rendered_manifest_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
    )
    artifact_store = SimpleNamespace(read=lambda _digest: publication)
    monkeypatch.setattr(
        refresh_module,
        "PreflightArtifactStore",
        lambda *_args, **_kwargs: artifact_store,
        raising=False,
    )
    monkeypatch.setattr(
        refresh_module,
        "find_advanced_epoch_attempt",
        lambda *_args, **_kwargs: recovery_attempt,
        raising=False,
    )
    final_plan = SimpleNamespace(
        **vars(_final_plan()),
        artifact_bundle_digest="c" * 64,
        artifact_descriptor_path=str(descriptor_path),
        rendered_manifest_path=str(rendered_path),
        rendered_manifest_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        image_digests={"loom-control-plane": "sha256:" + "e" * 64},
    )

    job_plan = build_installed_rollout_capacity_job_plan(
        config=_config(tmp_path),
        service_uid=os.geteuid(),
        final_plan=final_plan,  # type: ignore[arg-type]
        guard=_guard(mutation_epoch=guard_epoch),
        expected_buckets=("trajectories", "artifacts"),
        expected_filesystem_paths=("/var/lib/loom-minio-capacity/0",),
        capacity_source="filesystem",
        expected_drive_count=None,
        container_registry="",
    )

    assert job_plan.mutation_epoch == 9
    assert job_plan.job_name.endswith("-2")
    assert yaml.safe_load(job_plan.job_manifest)["metadata"]["annotations"][
        "loom.carin.dev/rollout-plan"
    ] == "f" * 64


def test_installed_job_plan_rejects_rendered_artifact_drift_before_job_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor_path = tmp_path / "artifact.json"
    rendered_path = tmp_path / "rendered.yaml"
    descriptor_path.write_text(
        json.dumps(
            {
                "candidate_sha": _SHA,
                "candidate_tree": _TREE,
                "container_registry": "",
                "image_digests": {"loom-control-plane": "sha256:" + "e" * 64},
                "mutation_epoch": 8,
                "registry_digests": {},
            }
        ),
        encoding="utf-8",
    )
    rendered_path.write_text("drifted\n", encoding="utf-8")
    descriptor_path.chmod(0o600)
    rendered_path.chmod(0o600)
    publication = SimpleNamespace(
        bundle_digest="c" * 64,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        container_registry="",
        descriptor_path=descriptor_path,
        mutation_epoch=8,
        rendered_manifest_path=rendered_path,
        rendered_manifest_sha256="d" * 64,
    )
    monkeypatch.setattr(
        refresh_module,
        "PreflightArtifactStore",
        lambda *_args, **_kwargs: SimpleNamespace(read=lambda _digest: publication),
        raising=False,
    )
    final_plan = SimpleNamespace(
        **vars(_final_plan()),
        artifact_bundle_digest="c" * 64,
        artifact_descriptor_path=str(descriptor_path),
        rendered_manifest_path=str(rendered_path),
        rendered_manifest_sha256="d" * 64,
        image_digests={"loom-control-plane": "sha256:" + "e" * 64},
    )

    with pytest.raises(InstalledRolloutCapacityRefreshError, match="artifact identity drifted"):
        build_installed_rollout_capacity_job_plan(
            config=_config(tmp_path),
            service_uid=os.geteuid(),
            final_plan=final_plan,  # type: ignore[arg-type]
            guard=_guard(),
            expected_buckets=("trajectories", "artifacts"),
            expected_filesystem_paths=("/var/lib/loom-minio-capacity/0",),
            capacity_source="filesystem",
            expected_drive_count=None,
            container_registry="",
        )
