from __future__ import annotations

import hashlib
import importlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import ClassVar
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
    LegacyCompatibilityFreezeV1,
    LegacyWriterFreezeCursorV1,
)
from loom_capacity_guard.contracts import canonical_digest as canonical_guard_digest
from loom_capacity_manager.contracts import (
    canonical_digest as canonical_manager_digest,
)
from loom_capacity_manager.contracts import (
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.global_execution_witness import (
    build_global_execution_witness_export,
)
from loom_capacity_manager.ownership import public_key_fingerprint
from loom_capacity_pool_executor.config import SlurmInventoryNodeDocument
from loom_cli.rollout.operator.protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    controller_job_visibility_evidence_sha256,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    controller_local_authority_sha256,
)
from loom_cli.rollout.operator.protected_staging_capacity_execution_credentials import (
    load_execution_credential_bundle,
)
from tests.loom_cli.rollout.operator.test_protected_execution_prerequisite_source import (
    _source_fixture,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_execution_credentials import (
    _credentials,
)


def _authority_module() -> ModuleType:
    try:
        return importlib.import_module("loom_cli.rollout.operator.installed_execution_authority")
    except ModuleNotFoundError:
        pytest.fail("installed execution authority module is unavailable")


def _freeze(fixture) -> LegacyCompatibilityFreezeV1:
    subject = fixture.desired.staging_subject
    return LegacyCompatibilityFreezeV1(
        environment_id="staging",
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        authority_incarnation=fixture.desired.fleet.authority_incarnation,
        agent_incarnation=UUID(int=101),
        reporter_incarnation=subject.demand_reporter_incarnation,
        candidate_digest=hashlib.sha256(fixture.plan.candidate_sha.encode("ascii")).hexdigest(),
        candidate_identity_algorithm="git-sha1",
        candidate_identity=fixture.plan.candidate_sha,
        candidate_publication_sha256=fixture.plan.artifact_bundle_digest,
        deployment_generation=subject.deployment_generation,
        configuration_generation=subject.configuration_generation,
        mutation_inventory_digest=LEGACY_MUTATION_INVENTORY_DIGEST,
        freeze_id=UUID(int=102),
        preparation_id=UUID(int=103),
        compatibility_incarnation=UUID(int=104),
        fleet_migration_epoch=fixture.desired.fleet.fleet_generation,
        preparation_digest="7" * 64,
        writer_cursors=tuple(
            LegacyWriterFreezeCursorV1(
                mutation_path_id=path_id,
                writer_domain="environment-local",
                writer_incarnation=UUID(int=200 + index),
                writer_epoch=3,
                high_water=17,
                authority_digest="8" * 64,
                freeze_acknowledgement_digest=f"{300 + index:064x}",
            )
            for index, path_id in enumerate(sorted(LEGACY_MUTATION_PATH_IDS))
        ),
    )


def _acknowledgement_digest(
    freeze: LegacyCompatibilityFreezeV1,
    *,
    candidate: CandidateBindingV2,
    protected_admission_sha256: str,
) -> str:
    value = {
        "candidate": candidate.model_dump(mode="json"),
        "freeze_sha256": canonical_guard_digest(freeze),
        "legacy_writer_high_water": max(cursor.high_water for cursor in freeze.writer_cursors),
        "protected_admission_sha256": protected_admission_sha256,
        "schema_version": 1,
        "subject_id": str(freeze.subject_id),
    }
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _publication(tmp_path: Path):
    module = _authority_module()
    fixture = _source_fixture(tmp_path)
    freeze = _freeze(fixture)
    protected_admission = fixture.protected_admission
    candidate = CandidateBindingV2(
        algorithm=freeze.candidate_identity_algorithm,
        identity=freeze.candidate_identity,
        publication_sha256=freeze.candidate_publication_sha256,
    )
    acknowledgement = SubjectExecutionAcknowledgementV2(
        subject_id=freeze.subject_id,
        subject_incarnation=freeze.subject_incarnation,
        configuration_generation=freeze.configuration_generation,
        deployment_generation=freeze.deployment_generation,
        candidate=candidate,
        reporter_incarnation=freeze.reporter_incarnation,
        protected_admission_sha256=protected_admission,
        legacy_writer_high_water=max(cursor.high_water for cursor in freeze.writer_cursors),
        acknowledgement_sha256=_acknowledgement_digest(
            freeze,
            candidate=candidate,
            protected_admission_sha256=protected_admission,
        ),
    )
    return module.InstalledExecutionAuthorityPublication(
        schema_version=1,
        authority_issue=906,
        candidate_sha=fixture.plan.candidate_sha,
        core_artifact_bundle_sha256=fixture.plan.artifact_bundle_digest,
        desired_fleet_sha256=fixture.source._artifact(
            fixture.source._capture(), lease=fixture.lease
        ).desired_fleet_sha256,
        desired_subject_sha256={
            str(subject.subject_id): canonical_manager_digest(subject)
            for subject in fixture.desired.subjects
        },
        subject_protected_admission_sha256={
            str(freeze.subject_id): protected_admission,
        },
        staging_subject_id=freeze.subject_id,
        executor_profile_seed=fixture.authority.executor_profile_seed,
        manager_client_cidrs=fixture.authority.manager_client_cidrs,
        credential_metadata_sha256=fixture.authority.credential_metadata_sha256,
        controller_transport_authority_sha256={
            "gb10": "a" * 64,
            "oldlab": "b" * 64,
        },
        manager_public_key_sha256="c" * 64,
        manager_signing_key_id="global-capacity-manager-2026-08",
        subject_acknowledgements=(acknowledgement,),
        subject_freezes=(freeze,),
        legacy_writer_fences=fixture.authority.legacy_writer_fences,
    )


def _production_desired(fixture):
    pools = []
    for pool in fixture.desired.fleet.pools:
        domains = tuple(
            domain.model_copy(update={"partition": "loom-staging"})
            for domain in pool.resource_domains
        )
        changed = pool.model_copy(
            update={
                "association": "loom-staging",
                "partition": "loom-staging",
                "pool_digest": "0" * 64,
                "resource_domains": domains,
            }
        )
        pools.append(
            changed.model_copy(
                update={"pool_digest": canonical_digest_excluding(changed, "pool_digest")}
            )
        )
    by_pool = {pool.pool_id: pool for pool in pools}

    def update_profiles(profiles):
        updated = []
        for profile in profiles:
            changed = profile.model_copy(
                update={
                    "pool_digest": by_pool[profile.pool_id].pool_digest,
                    "profile_digest": "0" * 64,
                }
            )
            updated.append(
                changed.model_copy(
                    update={
                        "profile_digest": canonical_digest_excluding(
                            changed,
                            "profile_digest",
                        )
                    }
                )
            )
        return tuple(updated)

    template = fixture.desired.fleet.development_subject_template
    assert template is not None
    changed_fleet = fixture.desired.fleet.model_copy(
        update={
            "development_subject_template": template.model_copy(
                update={"profiles": update_profiles(template.profiles)}
            ),
            "fleet_digest": "0" * 64,
            "pools": tuple(pools),
        }
    )
    fleet = changed_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed_fleet, "fleet_digest")}
    )
    subjects = tuple(
        subject.model_copy(update={"profiles": update_profiles(subject.profiles)})
        for subject in fixture.desired.subjects
    )
    staging = next(
        subject
        for subject in subjects
        if subject.subject_id == fixture.desired.staging_subject.subject_id
    )
    return replace(
        fixture.desired,
        fleet=fleet,
        subjects=subjects,
        staging_subject=staging,
    )


def _installed_source_inputs(tmp_path: Path):
    fixture = _source_fixture(tmp_path)
    desired = _production_desired(fixture)
    credential_fixture_root = tmp_path / "execution-credentials"
    credential_fixture_root.mkdir()
    credentials_root = _credentials(credential_fixture_root)
    bundle = load_execution_credential_bundle(
        credentials_root,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    transport_authorities = {"gb10": "a" * 64, "oldlab": "b" * 64}
    routes = {"gb10": "192.168.60.11/32", "oldlab": "192.168.50.103/32"}
    controller_hosts = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
    architectures = {"gb10": "arm64", "oldlab": "amd64"}
    slurm_clusters = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
    service_ids = {"gb10": (1201, 1201), "oldlab": (1202, 1202)}
    discoveries = {}
    bindings = []
    template_bindings = {
        binding.pool_id: binding for binding in fixture.authority.executor_profile_seed.pools
    }
    for pool_id in ("gb10", "oldlab"):
        pool = next(pool for pool in desired.fleet.pools if pool.pool_id == pool_id)
        profile = next(
            profile for profile in desired.staging_subject.profiles if profile.pool_id == pool_id
        )
        shape = profile.worker_shapes[0]
        uid, gid = service_ids[pool_id]
        executable_sha256 = {
            name: hashlib.sha256(f"{pool_id}:{name}".encode("ascii")).hexdigest()
            for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
        }
        configuration_sha256 = {
            "slurm.conf": hashlib.sha256(f"{pool_id}:slurm.conf".encode("ascii")).hexdigest()
        }
        partition_fields = (
            {"AllowAccounts": "loom-staging", "AllowQos": "loom-staging"}
            if pool_id == "gb10"
            else {"AllowGroups": "loom-rollout"}
        )
        association_fields = (
            (
                "trt-gb10",
                "loom-staging",
                "loom_capacity_executor",
                "loom-staging",
                "loom-staging",
                "loom-staging",
            )
            if pool_id == "gb10"
            else ()
        )
        target_nodes = (
            tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16)))
            if pool_id == "gb10"
            else tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6))
        )
        visibility = controller_job_visibility_evidence_sha256(
            pool_id=pool_id,
            partition_fields=partition_fields,
            association_fields=association_fields,
        )
        local_authority = controller_local_authority_sha256(
            pool_id=pool_id,
            architecture=architectures[pool_id],
            controller_hostname=controller_hosts[pool_id],
            service_uid=uid,
            service_gid=gid,
            slurm_cluster=slurm_clusters[pool_id],
            partition="loom-staging",
            target_nodes=target_nodes,
            executable_sha256=executable_sha256,
            configuration_sha256=configuration_sha256,
            job_visibility_evidence_sha256=visibility,
        )
        discovery = ControllerDiscoveryEvidence(
            schema_version=1,
            pool_id=pool_id,
            transport_authority_sha256=transport_authorities[pool_id],
            controller_hostname=controller_hosts[pool_id],
            architecture=architectures[pool_id],
            service_user="loom_capacity_executor",
            service_uid=uid,
            service_gid=gid,
            slurm_cluster=slurm_clusters[pool_id],
            partition="loom-staging",
            target_nodes=target_nodes,
            slurm_version=(23, 11, 4),
            data_parser="data_parser/v0.0.40",
            query_principal="loom_capacity_executor",
            manager_client_cidr=routes[pool_id],
            executable_sha256=executable_sha256,
            configuration_sha256=configuration_sha256,
            job_visibility_evidence_sha256=visibility,
            local_authority_sha256=local_authority,
        )
        discoveries[pool_id] = discovery
        template_binding = template_bindings[pool_id]
        inventory = template_binding.inventory.model_copy(
            update={
                "controller_cluster": discovery.slurm_cluster,
                "data_parser": discovery.data_parser,
                "job_visibility_evidence_sha256": discovery.job_visibility_evidence_sha256,
                "nodes": tuple(
                    SlurmInventoryNodeDocument(
                        pool_id=pool_id,
                        node_id=node.node_id,
                        allocatable=node.allocatable,
                        features=(domain.architecture,),
                    )
                    for domain in pool.resource_domains
                    for node in domain.nodes
                ),
                "pool_generation": pool.pool_generation,
                "query_principal": discovery.query_principal,
                "query_uid": discovery.service_uid,
                "relevant_partitions": (discovery.partition,),
                "reporter_incarnation": str(pool.pool_reporter_incarnation),
                "scontrol_sha256": discovery.executable_sha256["scontrol"],
                "slurm_conf_sha256": discovery.configuration_sha256["slurm.conf"],
                "slurm_version": discovery.slurm_version,
                "slot_resources": shape.total_resources,
                "squeue_sha256": discovery.executable_sha256["squeue"],
            }
        )
        signing_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            bundle.ownership_private_keys[pool_id]
        )
        bindings.append(
            template_binding.model_copy(
                update={
                    "association": pool.association,
                    "controller_authority_sha256": hashlib.sha256(
                        f"{pool_id}:controller".encode("ascii")
                    ).hexdigest(),
                    "controller_host": discovery.controller_hostname,
                    "executor_id": f"{pool_id}-executor-1",
                    "executor_incarnation": str(UUID(int=401 if pool_id == "gb10" else 402)),
                    "inventory": inventory,
                    "local_authority_sha256": discovery.local_authority_sha256,
                    "local_uid": discovery.service_uid,
                    "partition": pool.partition,
                    "profile_digest": profile.profile_digest,
                    "profile_generation": profile.profile_generation,
                    "profile_id": shape.shape_id,
                    "qos": "loom-staging",
                    "signing_key_id": f"{pool_id}-key-1",
                    "signing_key_sha256": public_key_fingerprint(signing_key.public_key()),
                    "slurm_cluster": discovery.slurm_cluster,
                    "submitter": "loom_capacity_executor",
                }
            )
        )
    seed = replace(
        fixture.authority.executor_profile_seed,
        authority_incarnation=str(desired.fleet.authority_incarnation),
        pools=tuple(bindings),
    )
    base = _publication(tmp_path / "owner-publication")
    publication = replace(
        base,
        desired_fleet_sha256=canonical_manager_digest(desired.fleet),
        desired_subject_sha256={
            str(subject.subject_id): canonical_manager_digest(subject)
            for subject in desired.subjects
        },
        executor_profile_seed=seed,
        manager_client_cidrs={**routes, "operator": "192.168.50.103/32"},
        credential_metadata_sha256=bundle.metadata_sha256,
        controller_transport_authority_sha256=transport_authorities,
    )
    now = datetime(2026, 9, 4, 12, tzinfo=UTC)
    manager_key = ed25519.Ed25519PrivateKey.from_private_bytes(b"m" * 32)
    manager_public = manager_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    publication = replace(
        publication,
        manager_public_key_sha256=hashlib.sha256(manager_public).hexdigest(),
    )
    witness_exports = {
        pool_id: build_global_execution_witness_export(
            private_key=manager_key,
            signing_key_id=publication.manager_signing_key_id,
            pool_id=pool_id,
            execution_epoch=0,
            execution_state="shadow",
            executable_new_capacity_ceiling=0,
            expires_at=now + timedelta(seconds=20),
        )
        for pool_id in ("gb10", "oldlab")
    }
    return desired, publication, bundle, discoveries, witness_exports, now


class _DiscoveryTransport:
    def __init__(self, evidence: ControllerDiscoveryEvidence) -> None:
        self.authority_sha256 = evidence.transport_authority_sha256
        self.evidence = evidence

    def discover(self, _request):
        return self.evidence


class _WitnessRunner:
    environment: ClassVar[dict[str, str]] = {
        "HOME": "/var/lib/loom-staging-rollout",
        "KUBECONFIG": "/var/lib/loom-staging-rollout/kubeconfig",
    }

    def __init__(self, value: object) -> None:
        self.payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        self.calls: list[tuple[tuple[str, ...], object, float]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        self.calls.append((tuple(argv), dict(env), timeout_seconds))
        return self.payload


def test_owner_publication_round_trips_canonically_and_binds_real_subject_freeze(
    tmp_path: Path,
) -> None:
    """Catch accepting a subject acknowledgement not derived from its freeze evidence."""
    module = _authority_module()
    publication = _publication(tmp_path)

    payload = module.canonical_installed_execution_authority_bytes(publication)

    assert payload.endswith(b"\n")
    assert module.parse_installed_execution_authority_bytes(payload) == publication

    value = publication.to_dict()
    value["subject_acknowledgements"][0]["acknowledgement_sha256"] = "f" * 64
    tampered = (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")
    with pytest.raises(ValueError):
        module.parse_installed_execution_authority_bytes(tampered)


def test_reader_accepts_only_the_exact_owner_only_canonical_publication(
    tmp_path: Path,
) -> None:
    """Catch bypassing the owner-only file boundary with an ordinary JSON read."""
    module = _authority_module()
    publication = _publication(tmp_path)
    authority_root = tmp_path / "owner-authority"
    authority_root.mkdir(mode=0o700)
    authority_root.chmod(0o700)
    authority_path = authority_root / "issue-906.json"
    authority_path.write_bytes(module.canonical_installed_execution_authority_bytes(publication))
    authority_path.chmod(0o600)

    reader_type = getattr(module, "InstalledExecutionAuthorityReader", None)
    assert reader_type is not None
    reader = reader_type(
        path=authority_path,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )

    assert reader() == publication


def test_reader_rejects_a_symlink_without_exposing_filesystem_errors(tmp_path: Path) -> None:
    """Catch following or surfacing an attacker-replaced authority path."""
    module = _authority_module()
    publication = _publication(tmp_path)
    authority_root = tmp_path / "owner-authority"
    authority_root.mkdir(mode=0o700)
    authority_root.chmod(0o700)
    real_path = authority_root / "real.json"
    real_path.write_bytes(module.canonical_installed_execution_authority_bytes(publication))
    real_path.chmod(0o600)
    authority_path = authority_root / "issue-906.json"
    authority_path.symlink_to(real_path)
    reader = module.InstalledExecutionAuthorityReader(
        path=authority_path,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )

    with pytest.raises(ValueError, match="authority file is unavailable"):
        reader()


def test_installed_source_combines_owner_controller_credential_and_signed_witness_authority(
    tmp_path: Path,
) -> None:
    """Catch returning owner claims without independent live boundary verification."""
    module = _authority_module()
    desired, publication, bundle, discoveries, witness_exports, now = _installed_source_inputs(
        tmp_path
    )
    source_type = getattr(module, "InstalledExecutionAuthoritySource", None)
    assert source_type is not None
    source = source_type(
        publication_reader=lambda: publication,
        controller_transports={
            pool_id: _DiscoveryTransport(discovery) for pool_id, discovery in discoveries.items()
        },
        credential_bundle_reader=lambda: bundle,
        witness_exports_source=lambda: witness_exports,
        now=lambda: now,
    )

    authority = source(desired)

    assert authority.executor_profile_seed == publication.executor_profile_seed
    assert authority.subject_acknowledgements == publication.subject_acknowledgements
    assert authority.manager_client_cidrs == publication.manager_client_cidrs
    assert authority.credential_metadata_sha256 == bundle.metadata_sha256
    assert set(authority.coexistence_witness_sha256) == {"gb10", "oldlab"}
    assert all(
        digest != hashlib.sha256(witness_exports[pool_id]).hexdigest()
        for pool_id, digest in authority.coexistence_witness_sha256.items()
    )
    assert authority.legacy_writer_fences == publication.legacy_writer_fences


def test_installed_source_rejects_a_substituted_operator_route(tmp_path: Path) -> None:
    """Catch routing manager mutation credentials from an unbound private host."""
    module = _authority_module()
    desired, publication, bundle, discoveries, witness_exports, now = _installed_source_inputs(
        tmp_path
    )
    publication = replace(
        publication,
        manager_client_cidrs={
            **publication.manager_client_cidrs,
            "operator": "192.168.50.104/32",
        },
    )
    source = module.InstalledExecutionAuthoritySource(
        publication_reader=lambda: publication,
        controller_transports={
            pool_id: _DiscoveryTransport(discovery) for pool_id, discovery in discoveries.items()
        },
        credential_bundle_reader=lambda: bundle,
        witness_exports_source=lambda: witness_exports,
        now=lambda: now,
    )

    with pytest.raises(ValueError, match="operator route"):
        source(desired)


def test_installed_source_rejects_credential_metadata_not_approved_by_owner(
    tmp_path: Path,
) -> None:
    """Catch silently adopting a rotated execution principal after owner publication."""
    module = _authority_module()
    desired, publication, bundle, discoveries, witness_exports, now = _installed_source_inputs(
        tmp_path
    )
    publication = replace(
        publication,
        credential_metadata_sha256={
            **publication.credential_metadata_sha256,
            "manager-read": "f" * 64,
        },
    )
    source = module.InstalledExecutionAuthoritySource(
        publication_reader=lambda: publication,
        controller_transports={
            pool_id: _DiscoveryTransport(discovery) for pool_id, discovery in discoveries.items()
        },
        credential_bundle_reader=lambda: bundle,
        witness_exports_source=lambda: witness_exports,
        now=lambda: now,
    )

    with pytest.raises(ValueError, match="credential metadata"):
        source(desired)


def test_kubernetes_witness_source_returns_only_the_two_fixed_signed_exports(
    tmp_path: Path,
) -> None:
    """Catch reading witness data from an unbound namespace, object, or data key."""
    module = _authority_module()
    _desired, _publication, _bundle, _discoveries, exports, _now = _installed_source_inputs(
        tmp_path
    )
    config_map = {
        "apiVersion": "v1",
        "data": {
            f"{pool_id}.json": payload.decode("ascii") for pool_id, payload in exports.items()
        },
        "kind": "ConfigMap",
        "metadata": {
            "name": "loom-global-execution-witness-v1",
            "namespace": "loom-dev",
            "resourceVersion": "123",
            "uid": "00000000-0000-4000-8000-000000000906",
        },
    }
    runner = _WitnessRunner(config_map)
    source_type = getattr(module, "KubernetesExecutionWitnessExportsSource", None)
    assert source_type is not None
    source = source_type(runner=runner)

    assert source() == exports
    assert runner.calls == [
        (
            (
                "kubectl",
                "--namespace",
                "loom-dev",
                "get",
                "configmap",
                "loom-global-execution-witness-v1",
                "--output=json",
            ),
            runner.environment,
            10.0,
        )
    ]
