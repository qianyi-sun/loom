from __future__ import annotations

import hashlib
import json
import os
import traceback
from copy import copy, deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_digest, canonical_digest_excluding
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    LegacyWriterFenceV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_pool_executor.config import SlurmInventoryNodeDocument
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.protected_execution_prerequisite_source import (
    ProtectedExecutionPrerequisiteAuthority,
    ProtectedExecutionPrerequisiteRuntimeSource,
    ProtectedExecutionPrerequisiteSourceError,
)
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component import (
    derive_protected_staging_capacity_configuration,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_configuration_component import (
    _active_document,
    _live_fleet,
    _seed,
)


def _executor_seed(*, desired, executor_image: str):
    template = execution_prerequisite_artifact().executor_profile_seed
    desired_pools = {pool.pool_id: pool for pool in desired.fleet.pools}
    bindings = []
    for binding in template.pools:
        pool = desired_pools[binding.pool_id]
        nodes = tuple(
            SlurmInventoryNodeDocument(
                pool_id=binding.pool_id,
                node_id=node.node_id,
                allocatable=node.allocatable,
                features=(domain.architecture,),
            )
            for domain in pool.resource_domains
            for node in domain.nodes
        )
        inventory = binding.inventory.model_copy(
            update={
                "nodes": nodes,
                "pool_generation": pool.pool_generation,
                "reporter_incarnation": str(pool.pool_reporter_incarnation),
                "relevant_partitions": (pool.partition,),
            }
        )
        bindings.append(
            binding.model_copy(
                update={
                    "association": pool.association,
                    "inventory": inventory,
                    "partition": pool.partition,
                    "pool_generation": pool.pool_generation,
                }
            )
        )
    return replace(
        template,
        authority_incarnation=str(desired.fleet.authority_incarnation),
        executor_image=executor_image,
        pools=tuple(bindings),
    )


def _execution_fleet():
    fleet = _live_fleet()
    executor_pools = {
        pool.pool_id: pool for pool in execution_prerequisite_artifact().executor_profile_seed.pools
    }
    pools = []
    for pool in fleet.pools:
        executor_pool = executor_pools[pool.pool_id]
        changed = pool.model_copy(
            update={
                "association": executor_pool.association,
                "partition": executor_pool.partition,
                "pool_digest": "0" * 64,
                "resource_domains": tuple(
                    domain.model_copy(update={"partition": executor_pool.partition})
                    for domain in pool.resource_domains
                ),
            }
        )
        pools.append(
            changed.model_copy(
                update={"pool_digest": canonical_digest_excluding(changed, "pool_digest")}
            )
        )
    changed_fleet = fleet.model_copy(
        update={
            "development_subject_template": (
                fleet.development_subject_template.model_copy(
                    update={
                        "profiles": tuple(
                            (
                                profile.model_copy(
                                    update={
                                        "pool_digest": next(
                                            pool.pool_digest
                                            for pool in pools
                                            if pool.pool_id == profile.pool_id
                                        ),
                                        "profile_digest": "0" * 64,
                                    }
                                )
                            )
                            for profile in fleet.development_subject_template.profiles
                        )
                    }
                )
                if fleet.development_subject_template is not None
                else None
            ),
            "fleet_digest": "0" * 64,
            "pools": tuple(pools),
        }
    )
    if changed_fleet.development_subject_template is not None:
        changed_fleet = changed_fleet.model_copy(
            update={
                "development_subject_template": (
                    changed_fleet.development_subject_template.model_copy(
                        update={
                            "profiles": tuple(
                                profile.model_copy(
                                    update={
                                        "profile_digest": canonical_digest_excluding(
                                            profile,
                                            "profile_digest",
                                        )
                                    }
                                )
                                for profile in changed_fleet.development_subject_template.profiles
                            )
                        }
                    )
                )
            }
        )
    return changed_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed_fleet, "fleet_digest")}
    )


def _lease(*, plan, desired) -> BackupLease:
    configuration_digest = canonical_digest(desired.original.snapshot)
    authority = DatabaseAuthorityEvidence(
        public_schema_revision="0066",
        capacity_guard_schema_revision="guard_0028",
        configuration_epoch=desired.original.snapshot.configuration_epoch,
        configuration_digest=configuration_digest,
        authority_incarnation=desired.fleet.authority_incarnation,
        writer_epoch=4,
        execution_state="shadow",
        execution_epoch=0,
        execution_manifest_sha256=None,
        executable_new_capacity_ceiling=0,
        increase_freeze=True,
    )
    now = datetime(2026, 9, 3, 12, tzinfo=UTC)
    return BackupLease(
        lease_id="lease-execution-prerequisites",
        source_request_id="req-execution-prerequisites",
        manifest_sha256="a" * 64,
        component_sha256={
            "database_authority": authority.digest,
            "k8s_secrets": "b" * 64,
            "object_inventory": "c" * 64,
            "postgres": "d" * 64,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=plan.starting_mutation_epoch,
        db_snapshot_identity="pgdump-sha256:" + "d" * 64,
        schema_revision="0066",
        object_inventory_root="e" * 64,
        created_at=now - timedelta(minutes=20),
        restore_verified_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(hours=2),
        checkpoint_schema_version=3,
        database_authority_digest=authority.digest,
        public_schema_revision="0066",
        capacity_guard_schema_revision="guard_0028",
        manager_configuration_epoch=desired.original.snapshot.configuration_epoch,
        manager_configuration_digest=configuration_digest,
        manager_authority_incarnation=desired.fleet.authority_incarnation,
        manager_writer_epoch=4,
        manager_execution_state="shadow",
        manager_execution_epoch=0,
        manager_execution_manifest_sha256=None,
        manager_executable_new_capacity_ceiling=0,
        manager_increase_freeze=True,
        restore_report_sha256="f" * 64,
    )


@dataclass(frozen=True)
class _SourceFixture:
    plan: Any
    active: dict[str, object]
    seed_values: dict[str, object]
    desired: Any
    protected_admission: str
    authority: ProtectedExecutionPrerequisiteAuthority
    lease: BackupLease
    store: ProtectedExecutionPrerequisiteStore
    source: ProtectedExecutionPrerequisiteRuntimeSource


def _source_fixture(tmp_path: Path) -> _SourceFixture:
    plan = _plan(tmp_path)
    active = _active_document(_execution_fleet(), ())
    seed_values = _seed()
    desired = derive_protected_staging_capacity_configuration(
        active_document=active,
        seed_values=seed_values,
        target_generation=plan.starting_mutation_epoch + 1,
    )
    executor_digest = "1" * 64
    executor_image = f"registry.example/loom-capacity-executor@sha256:{executor_digest}"
    executor_seed = _executor_seed(desired=desired, executor_image=executor_image)
    protected_admission = "2" * 64
    staging = desired.staging_subject
    acknowledgement = SubjectExecutionAcknowledgementV2(
        subject_id=staging.subject_id,
        subject_incarnation=staging.subject_incarnation,
        configuration_generation=staging.configuration_generation,
        deployment_generation=staging.deployment_generation,
        candidate=CandidateBindingV2(
            algorithm="git-sha1",
            identity=plan.candidate_sha,
            publication_sha256=plan.artifact_bundle_digest,
        ),
        reporter_incarnation=staging.demand_reporter_incarnation,
        protected_admission_sha256=protected_admission,
        legacy_writer_high_water=17,
        acknowledgement_sha256="3" * 64,
    )
    legacy_fence = LegacyWriterFenceV2(
        writer_id="global-dev-supervisor",
        writer_kind="allocation",
        scope_kind="global",
        scope_id="development",
        high_water=17,
        freeze_evidence_sha256="4" * 64,
        state="frozen",
    )
    authority = ProtectedExecutionPrerequisiteAuthority(
        executor_profile_seed=executor_seed,
        subject_acknowledgements=(acknowledgement,),
        manager_client_cidrs={
            "gb10": "192.168.60.11/32",
            "oldlab": "192.168.50.103/32",
            "operator": "192.168.50.103/32",
        },
        credential_metadata_sha256=(execution_prerequisite_artifact().credential_metadata_sha256),
        coexistence_witness_sha256={"gb10": "5" * 64, "oldlab": "6" * 64},
        legacy_writer_fences=(legacy_fence,),
    )
    lease = _lease(plan=plan, desired=desired)
    store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "state",
        service_uid=os.geteuid(),
    )
    source = ProtectedExecutionPrerequisiteRuntimeSource(
        store=store,
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        core_artifact_bundle_sha256=plan.artifact_bundle_digest,
        mutation_epoch=plan.starting_mutation_epoch,
        executor_image_sha256=executor_digest,
        container_registry="registry.example",
        manager_configuration_source=lambda: deepcopy(active),
        configuration_seed_source=lambda: deepcopy(seed_values),
        staging_protected_admission_source=lambda _seed: protected_admission,
        authority_source=lambda _desired: authority,
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=UTC),
    )
    return _SourceFixture(
        plan=plan,
        active=active,
        seed_values=seed_values,
        desired=desired,
        protected_admission=protected_admission,
        authority=authority,
        lease=lease,
        store=store,
        source=source,
    )


def test_source_publishes_prerequisite_only_from_exact_typed_authorities(
    tmp_path: Path,
) -> None:
    """Catch omitted source validation or synthetic prerequisite defaults."""
    fixture = _source_fixture(tmp_path)
    plan = fixture.plan
    desired = fixture.desired
    staging = desired.staging_subject
    lease = fixture.lease

    publication = fixture.source.publish(lease)
    artifact = fixture.store.read(publication)
    expected_rollback = hashlib.sha256(
        json.dumps(
            {
                "backup_component_set_digest": component_set_digest(lease.component_sha256),
                "backup_lease_digest": lease.evidence_digest,
                "backup_lease_id": lease.lease_id,
                "backup_source_request_id": lease.source_request_id,
                "checkpoint_schema_version": 3,
                "predecessor_configuration_digest": canonical_digest(desired.original.snapshot),
                "predecessor_configuration_epoch": (desired.original.snapshot.configuration_epoch),
                "restore_report_sha256": lease.restore_report_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()

    assert artifact.candidate_sha == plan.candidate_sha
    assert artifact.core_artifact_bundle_sha256 == plan.artifact_bundle_digest
    assert artifact.source_configuration_sha256 == canonical_digest(desired.original.snapshot)
    assert artifact.desired_fleet_sha256 == canonical_digest(desired.fleet)
    assert artifact.desired_fleet_generation == desired.fleet.fleet_generation
    assert artifact.desired_subject_sha256 == {str(staging.subject_id): canonical_digest(staging)}
    assert artifact.subject_protected_admission_sha256 == {
        str(staging.subject_id): fixture.protected_admission
    }
    assert artifact.backup_lease_sha256 == lease.evidence_digest
    assert artifact.rollback_evidence_sha256 == expected_rollback
    assert artifact.executor_profile_seed.executor_image == fixture.source.executor_image
    assert artifact.execution_policy.executable_new_capacity_ceiling == 158
    assert artifact.execution_policy.subject_acknowledgements == (
        fixture.authority.subject_acknowledgements
    )
    assert artifact.execution_policy.legacy_writer_fences == (
        fixture.authority.legacy_writer_fences
    )


def test_source_rejects_zero_filled_legacy_high_water(tmp_path: Path) -> None:
    """Catch replacing observed writer progress with a synthetic zero."""
    fixture = _source_fixture(tmp_path)
    acknowledgement = fixture.authority.subject_acknowledgements[0].model_copy(
        update={"legacy_writer_high_water": 0}
    )
    fence = fixture.authority.legacy_writer_fences[0].model_copy(update={"high_water": 0})
    authority = replace(
        fixture.authority,
        subject_acknowledgements=(acknowledgement,),
        legacy_writer_fences=(fence,),
    )
    source = replace(
        fixture.source,
        authority_source=lambda _desired: authority,
    )

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


def test_source_keeps_subject_and_external_writer_high_waters_independent(
    tmp_path: Path,
) -> None:
    """Catch requiring unrelated subject and external writer counters to coincide."""
    fixture = _source_fixture(tmp_path)
    fence = fixture.authority.legacy_writer_fences[0].model_copy(update={"high_water": 23})
    authority = replace(fixture.authority, legacy_writer_fences=(fence,))
    source = replace(
        fixture.source,
        authority_source=lambda _desired: authority,
    )

    publication = source.publish(fixture.lease)

    artifact = fixture.store.read(publication)
    assert artifact.execution_policy.subject_acknowledgements[0].legacy_writer_high_water == 17
    assert artifact.execution_policy.legacy_writer_fences[0].high_water == 23


def test_source_rejects_zero_filled_freeze_evidence(tmp_path: Path) -> None:
    """Catch treating a placeholder digest as a real freeze observation."""
    fixture = _source_fixture(tmp_path)
    fence = fixture.authority.legacy_writer_fences[0].model_copy(
        update={"freeze_evidence_sha256": "0" * 64}
    )
    authority = replace(fixture.authority, legacy_writer_fences=(fence,))
    source = replace(
        fixture.source,
        authority_source=lambda _desired: authority,
    )

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


def test_source_discards_secret_bearing_source_failures(tmp_path: Path) -> None:
    """Catch leaking private source values through exception chaining."""
    fixture = _source_fixture(tmp_path)
    sentinel = "reporter-token-secret-value"

    def fail() -> dict[str, object]:
        raise RuntimeError(sentinel)

    source = replace(fixture.source, manager_configuration_source=fail)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError) as exc_info:
        source.publish(fixture.lease)

    rendered = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    assert sentinel not in rendered
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert not fixture.store.state_root.exists()


def test_source_rejects_zero_filled_subject_acknowledgement(tmp_path: Path) -> None:
    """Catch a placeholder acknowledgement for real legacy-writer state."""
    fixture = _source_fixture(tmp_path)
    acknowledgement = fixture.authority.subject_acknowledgements[0].model_copy(
        update={"acknowledgement_sha256": "0" * 64}
    )
    authority = replace(
        fixture.authority,
        subject_acknowledgements=(acknowledgement,),
    )
    source = replace(
        fixture.source,
        authority_source=lambda _desired: authority,
    )

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize(
    "field",
    ("credential_metadata_sha256", "coexistence_witness_sha256"),
)
def test_source_rejects_zero_filled_access_authority(
    tmp_path: Path,
    field: str,
) -> None:
    """Catch accepting zero placeholders for credentials or reciprocal witnesses."""
    fixture = _source_fixture(tmp_path)
    values = {key: "0" * 64 for key in getattr(fixture.authority, field)}
    authority = replace(fixture.authority, **{field: values})
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize("drifting_source", ("manager", "seed", "authority"))
def test_source_rejects_authority_drift_across_capture(
    tmp_path: Path,
    drifting_source: str,
) -> None:
    """Catch accepting live authority that changes during one publication."""
    fixture = _source_fixture(tmp_path)
    calls = 0

    if drifting_source == "manager":
        changed = _active_document(fixture.desired.fleet, fixture.desired.subjects)

        def manager_source():
            nonlocal calls
            calls += 1
            return deepcopy(fixture.active if calls == 1 else changed)

        source = replace(fixture.source, manager_configuration_source=manager_source)
    elif drifting_source == "seed":
        changed = deepcopy(fixture.seed_values)
        changed["reporter_incarnation"] = "00000000-0000-4000-8000-000000000099"

        def seed_source():
            nonlocal calls
            calls += 1
            return deepcopy(fixture.seed_values if calls == 1 else changed)

        source = replace(fixture.source, configuration_seed_source=seed_source)
    else:
        changed_fence = fixture.authority.legacy_writer_fences[0].model_copy(
            update={"freeze_evidence_sha256": "7" * 64}
        )
        changed_authority = replace(
            fixture.authority,
            legacy_writer_fences=(changed_fence,),
        )

        def authority_source(_desired):
            nonlocal calls
            calls += 1
            return fixture.authority if calls == 1 else changed_authority

        source = replace(fixture.source, authority_source=authority_source)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize(
    "changes",
    (
        {"manager_increase_freeze": False},
        {"manager_executable_new_capacity_ceiling": 1},
        {"manager_execution_epoch": 1},
        {"manager_execution_state": "prepared"},
        {"mutation_epoch": 999},
    ),
)
def test_source_rejects_nonfrozen_or_wrong_epoch_lease(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    """Catch using a lease outside the exact frozen shadow authority."""
    fixture = _source_fixture(tmp_path)
    lease = copy(fixture.lease)
    for field, value in changes.items():
        object.__setattr__(lease, field, value)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        fixture.source.publish(lease)

    assert not fixture.store.state_root.exists()


def test_source_rejects_expired_and_historical_leases(tmp_path: Path) -> None:
    """Catch using an expired lease or a pre-schema-3 checkpoint as rollback authority."""
    fixture = _source_fixture(tmp_path)
    expired = replace(
        fixture.lease,
        expires_at=datetime(2026, 9, 3, 11, 59, tzinfo=UTC),
    )
    historical = replace(
        fixture.lease,
        checkpoint_schema_version=None,
        database_authority_digest=None,
        public_schema_revision=None,
        capacity_guard_schema_revision=None,
        manager_configuration_epoch=None,
        manager_configuration_digest=None,
        manager_authority_incarnation=None,
        manager_writer_epoch=None,
        manager_execution_state=None,
        manager_execution_epoch=None,
        manager_execution_manifest_sha256=None,
        manager_executable_new_capacity_ceiling=None,
        manager_increase_freeze=None,
        restore_report_sha256=None,
    )

    for lease in (expired, historical):
        with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
            fixture.source.publish(lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize(
    ("pool_id", "forbidden_node"),
    (
        ("gb10", "trt-gb10-2"),
        ("gb10", "trt-gb10-16"),
        ("oldlab", "trt-eai-oldlab-1"),
        ("oldlab", "trt-eai-oldlab-2"),
    ),
)
def test_source_rejects_forbidden_controller_inventory_nodes(
    tmp_path: Path,
    pool_id: str,
    forbidden_node: str,
) -> None:
    """Catch admitting reserved or excluded nodes into executable inventory."""
    fixture = _source_fixture(tmp_path)
    bindings = list(fixture.authority.executor_profile_seed.pools)
    index = next(i for i, binding in enumerate(bindings) if binding.pool_id == pool_id)
    binding = bindings[index]
    exemplar = binding.inventory.nodes[0]
    forbidden = exemplar.model_copy(update={"node_id": forbidden_node})
    inventory = binding.inventory.model_copy(
        update={"nodes": (*binding.inventory.nodes, forbidden)}
    )
    bindings[index] = binding.model_copy(update={"inventory": inventory})
    seed = replace(fixture.authority.executor_profile_seed, pools=tuple(bindings))
    authority = replace(fixture.authority, executor_profile_seed=seed)
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


def test_source_rejects_required_controller_inventory_node_omission(
    tmp_path: Path,
) -> None:
    """Catch publishing a partial controller inventory."""
    fixture = _source_fixture(tmp_path)
    bindings = list(fixture.authority.executor_profile_seed.pools)
    binding = bindings[0]
    inventory = binding.inventory.model_copy(update={"nodes": binding.inventory.nodes[:-1]})
    bindings[0] = binding.model_copy(update={"inventory": inventory})
    seed = replace(fixture.authority.executor_profile_seed, pools=tuple(bindings))
    authority = replace(fixture.authority, executor_profile_seed=seed)
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


def test_source_rejects_candidate_executor_image_mismatch(tmp_path: Path) -> None:
    """Catch binding controller authority to a different immutable executor image."""
    fixture = _source_fixture(tmp_path)
    seed = replace(
        fixture.authority.executor_profile_seed,
        executor_image="registry.example/loom-capacity-executor@sha256:" + "8" * 64,
    )
    authority = replace(fixture.authority, executor_profile_seed=seed)
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize("kind", ("duplicate", "foreign"))
def test_source_rejects_duplicate_or_foreign_subject_acknowledgements(
    tmp_path: Path,
    kind: str,
) -> None:
    """Catch ambiguous or non-fleet subject acknowledgement authority."""
    fixture = _source_fixture(tmp_path)
    acknowledgement = fixture.authority.subject_acknowledgements[0]
    acknowledgements = (
        (acknowledgement, acknowledgement)
        if kind == "duplicate"
        else (
            acknowledgement.model_copy(
                update={"subject_id": UUID("00000000-0000-4000-8000-000000000099")}
            ),
        )
    )
    authority = replace(
        fixture.authority,
        subject_acknowledgements=acknowledgements,
    )
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


@pytest.mark.parametrize(
    "field", ("manager_client_cidrs", "credential_metadata_sha256", "coexistence_witness_sha256")
)
def test_source_rejects_route_credential_or_witness_inventory_drift(
    tmp_path: Path,
    field: str,
) -> None:
    """Catch incomplete route, credential, or reciprocal-witness inventories."""
    fixture = _source_fixture(tmp_path)
    values = dict(getattr(fixture.authority, field))
    values.pop(next(iter(values)))
    authority = replace(fixture.authority, **{field: values})
    source = replace(fixture.source, authority_source=lambda _desired: authority)

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        source.publish(fixture.lease)

    assert not fixture.store.state_root.exists()


def test_source_discards_secret_bearing_store_failure(tmp_path: Path) -> None:
    """Catch leaking a private store error through the registered check boundary."""
    fixture = _source_fixture(tmp_path)
    sentinel = "private-key-store-sentinel"

    class FailingStore(ProtectedExecutionPrerequisiteStore):
        def publish(self, _artifact):
            raise RuntimeError(sentinel)

    source = replace(
        fixture.source,
        store=FailingStore(tmp_path / "state", service_uid=os.geteuid()),
    )

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError) as exc_info:
        source.publish(fixture.lease)

    rendered = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    assert sentinel not in rendered
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_source_rejects_registry_with_impossible_port(tmp_path: Path) -> None:
    """Catch constructing a candidate image from a non-network registry origin."""
    fixture = _source_fixture(tmp_path)

    with pytest.raises(ValueError, match="runtime source is invalid"):
        replace(fixture.source, container_registry="registry.example:70000")
