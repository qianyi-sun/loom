from __future__ import annotations

import tomllib
from pathlib import Path
from uuid import UUID

from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionPreparationPolicyV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_cli.capacity_control_plane import CapacityPoolExecutorProfile
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    CapacityPoolExecutorProfileSeed,
    ProtectedExecutionPrerequisiteArtifact,
)

_PROFILE = Path("deploy/dev-fleet/capacity-pool-executor.toml.example")
_CANDIDATE_SHA = "a" * 40
_CANDIDATE_TREE = "b" * 40
_CORE_BUNDLE = "c" * 64
_SUBJECT_ID = UUID("00000000-0000-4000-8000-000000000081")
_SUBJECT_INCARNATION = UUID("00000000-0000-4000-8000-000000000082")
_REPORTER_INCARNATION = UUID("00000000-0000-4000-8000-000000000083")
_PROTECTED_ADMISSION = "3" * 64
_ROLLBACK_EVIDENCE = "6" * 64
_CREDENTIAL_NAMES = {
    "manager-abort",
    "manager-activate",
    "manager-drain",
    "manager-prepare",
    "manager-read",
    "manager-retire",
    "pool-executor-gb10",
    "pool-executor-oldlab",
    "pool-ownership-gb10",
    "pool-ownership-oldlab",
}


def _seed() -> CapacityPoolExecutorProfileSeed:
    profile = CapacityPoolExecutorProfile.model_validate(
        tomllib.loads(_PROFILE.read_text(encoding="utf-8"))
    )
    return CapacityPoolExecutorProfileSeed.from_profile(profile)


def _policy(
    seed: CapacityPoolExecutorProfileSeed,
    *,
    core_bundle_sha256: str = _CORE_BUNDLE,
) -> ExecutionPreparationPolicyV2:
    legacy_fence = LegacyWriterFenceV2(
        writer_id="global-dev-supervisor",
        writer_kind="allocation",
        scope_kind="global",
        scope_id="development",
        high_water=9,
        freeze_evidence_sha256="5" * 64,
        state="frozen",
    )
    return ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256=seed.trusted_fleet_release_sha256,
        executable_new_capacity_ceiling=158,
        executable_new_capacity_rate_per_minute=1,
        executors=tuple(
            PreparedExecutorBindingV2(
                pool_id=pool.pool_id,
                pool_generation=pool.pool_generation,
                executor_id=pool.executor_id,
                executor_incarnation=UUID(pool.executor_incarnation),
                signing_key_sha256=pool.signing_key_sha256,
                local_authority_sha256=pool.local_authority_sha256,
                controller_authority_sha256=pool.controller_authority_sha256,
            )
            for pool in seed.pools
        ),
        subject_acknowledgements=(
            SubjectExecutionAcknowledgementV2(
                subject_id=_SUBJECT_ID,
                subject_incarnation=_SUBJECT_INCARNATION,
                configuration_generation=2,
                deployment_generation=1,
                candidate=CandidateBindingV2(
                    algorithm="git-sha1",
                    identity=_CANDIDATE_SHA,
                    publication_sha256=core_bundle_sha256,
                ),
                reporter_incarnation=_REPORTER_INCARNATION,
                protected_admission_sha256=_PROTECTED_ADMISSION,
                legacy_writer_high_water=9,
                acknowledgement_sha256="4" * 64,
            ),
        ),
        rollback_evidence_sha256=_ROLLBACK_EVIDENCE,
        controller_authorities=tuple(
            PoolControllerAuthorityV2(
                pool_id=pool.pool_id,
                controller_authority_sha256=pool.controller_authority_sha256,
            )
            for pool in seed.pools
        ),
        legacy_writer_fences=(legacy_fence,),
    )


def execution_prerequisite_artifact(
    *,
    core_bundle_sha256: str = _CORE_BUNDLE,
    backup_lease_sha256: str = "d" * 64,
) -> ProtectedExecutionPrerequisiteArtifact:
    seed = _seed()
    return ProtectedExecutionPrerequisiteArtifact(
        schema_version=1,
        candidate_sha=_CANDIDATE_SHA,
        candidate_tree=_CANDIDATE_TREE,
        core_artifact_bundle_sha256=core_bundle_sha256,
        source_configuration_epoch=9,
        source_configuration_sha256="7" * 64,
        desired_fleet_generation=2,
        desired_fleet_sha256="8" * 64,
        desired_subject_sha256={str(_SUBJECT_ID): "9" * 64},
        subject_protected_admission_sha256={
            str(_SUBJECT_ID): _PROTECTED_ADMISSION,
        },
        staging_subject_id=_SUBJECT_ID,
        backup_lease_sha256=backup_lease_sha256,
        rollback_evidence_sha256=_ROLLBACK_EVIDENCE,
        manager_external_origin="https://192.168.50.103:31443",
        manager_client_cidrs={
            "gb10": "192.168.60.11/32",
            "oldlab": "192.168.50.103/32",
            "operator": "192.168.50.103/32",
        },
        credential_metadata_sha256={
            name: f"{index + 10:064x}" for index, name in enumerate(sorted(_CREDENTIAL_NAMES))
        },
        coexistence_witness_sha256={
            "gb10": "e" * 64,
            "oldlab": "f" * 64,
        },
        legacy_writer_evidence_sha256={
            "global/development/allocation/global-dev-supervisor": "5" * 64,
        },
        execution_policy=_policy(seed, core_bundle_sha256=core_bundle_sha256),
        executor_profile_seed=seed,
    )


__all__ = ["execution_prerequisite_artifact"]
