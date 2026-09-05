from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    ImageArtifactSet,
    ImageDescriptor,
    image_plan_digest,
)
from loom_cli.rollout.operator.deep_preflight_authority import RuntimePurpose
from loom_cli.rollout.operator.installed_execution_prerequisite import (
    InstalledExecutionPrerequisitePublisherFactory,
)
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.operator.protected_execution_prerequisite_source import (
    ProtectedExecutionPrerequisiteSourceError,
)
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisitePublication,
)
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    staging_database_protected_admission_digest,
    staging_database_protected_admission_digest_for_candidate,
)
from tests.loom_cli.rollout.operator.test_protected_execution_prerequisite_source import (
    _source_fixture,
)


def _images(*, candidate_sha: str) -> ImageArtifactSet:
    descriptors = {
        name: ImageDescriptor(
            image_id=f"sha256:{hashlib.sha256(name.encode()).hexdigest()}",
            revision=candidate_sha,
            os="linux",
            architecture="amd64",
            entrypoint=(),
        )
        for name, _path in ALL_BUILD_IMAGES
    }
    registry_digests = {
        name: (
            "sha256:" + "1" * 64
            if name == "loom-capacity-executor"
            else f"sha256:{hashlib.sha256((name + '-registry').encode()).hexdigest()}"
        )
        for name, _path in ALL_BUILD_IMAGES
    }
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest=image_plan_digest(),
        artifact_digest="9" * 64,
        registry_digests=registry_digests,
    )


def _candidate(plan) -> CandidateBinding:
    return CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha=plan.candidate_sha,
        image_tag=f"staging-{plan.candidate_sha[:7]}",
        fetched_at="2026-09-03T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree=plan.candidate_tree,
        approved_base_sha="a" * 40,
    )


def test_detached_publisher_binds_loaded_bundle_and_executor_registry_digest(
    tmp_path: Path,
) -> None:
    """Catch rebuilding authority from a different bundle or local-only image ID."""
    fixture = _source_fixture(tmp_path)
    plan = fixture.plan
    images = _images(candidate_sha=plan.candidate_sha)
    candidate = _candidate(plan)
    loaded = SimpleNamespace(
        images=images,
        publication=SimpleNamespace(
            bundle_digest=plan.artifact_bundle_digest,
            candidate_sha=plan.candidate_sha,
            candidate_tree=plan.candidate_tree,
            mutation_epoch=plan.starting_mutation_epoch,
            container_registry="registry.example",
        ),
    )
    authority_source_factory_calls: list[tuple[CandidateBinding, str]] = []

    def authority_source_factory(found_candidate: CandidateBinding, executor_image: str):
        authority_source_factory_calls.append((found_candidate, executor_image))
        return lambda _desired: fixture.authority

    factory = InstalledExecutionPrerequisitePublisherFactory(
        store=fixture.store,
        container_registry="registry.example",
        manager_configuration_source=lambda: deepcopy(fixture.active),
        configuration_seed_source=lambda: deepcopy(fixture.seed_values),
        staging_protected_admission_source=(
            lambda _candidate, _bundle, _epoch, _seed: fixture.protected_admission
        ),
        authority_source_factory=authority_source_factory,
        now=fixture.source.now,
    )

    publisher = factory(
        candidate,
        plan.starting_mutation_epoch,
        RuntimePurpose.DETACHED_REHEARSAL,
        loaded,  # type: ignore[arg-type]
    )
    evidence = publisher(fixture.lease, images)

    executor_digest = images.registry_digests["loom-capacity-executor"].removeprefix("sha256:")
    assert evidence["artifact-path"] == str(
        fixture.store.root / f"{evidence['artifact-sha256']}.json"
    )
    publication = ProtectedExecutionPrerequisitePublication(
        path=fixture.store.root / f"{evidence['artifact-sha256']}.json",
        artifact_sha256=evidence["artifact-sha256"],
    )
    artifact = fixture.store.read(publication)
    assert artifact.core_artifact_bundle_sha256 == plan.artifact_bundle_digest
    assert artifact.executor_profile_seed.executor_image == (
        f"registry.example/loom-capacity-executor@sha256:{executor_digest}"
    )
    assert authority_source_factory_calls == [
        (candidate, f"registry.example/loom-capacity-executor@sha256:{executor_digest}")
    ]


def test_publisher_fails_closed_before_detached_artifacts(tmp_path: Path) -> None:
    """Catch admission-time publication before the immutable core bundle exists."""
    fixture = _source_fixture(tmp_path)
    factory = InstalledExecutionPrerequisitePublisherFactory(
        store=fixture.store,
        container_registry="registry.example",
        manager_configuration_source=lambda: deepcopy(fixture.active),
        configuration_seed_source=lambda: deepcopy(fixture.seed_values),
        staging_protected_admission_source=(
            lambda _candidate, _bundle, _epoch, _seed: fixture.protected_admission
        ),
        authority_source_factory=lambda _candidate, _image: lambda _desired: fixture.authority,
        now=fixture.source.now,
    )

    publisher = factory(
        _candidate(fixture.plan),
        fixture.plan.starting_mutation_epoch,
        RuntimePurpose.ADMISSION,
        None,
    )

    with pytest.raises(
        ProtectedExecutionPrerequisiteSourceError,
        match="unavailable before detached artifacts",
    ):
        publisher(fixture.lease, _images(candidate_sha=fixture.plan.candidate_sha))


def test_detached_publisher_rejects_image_artifact_drift(tmp_path: Path) -> None:
    """Catch substituting a second image result after the bundle was loaded."""
    fixture = _source_fixture(tmp_path)
    plan = fixture.plan
    images = _images(candidate_sha=plan.candidate_sha)
    changed = ImageArtifactSet(
        descriptors=images.descriptors,
        plan_digest=images.plan_digest,
        artifact_digest="8" * 64,
        registry_digests=images.registry_digests,
    )
    loaded = SimpleNamespace(
        images=images,
        publication=SimpleNamespace(
            bundle_digest=plan.artifact_bundle_digest,
            candidate_sha=plan.candidate_sha,
            candidate_tree=plan.candidate_tree,
            mutation_epoch=plan.starting_mutation_epoch,
            container_registry="registry.example",
        ),
    )
    factory = InstalledExecutionPrerequisitePublisherFactory(
        store=fixture.store,
        container_registry="registry.example",
        manager_configuration_source=lambda: deepcopy(fixture.active),
        configuration_seed_source=lambda: deepcopy(fixture.seed_values),
        staging_protected_admission_source=(
            lambda _candidate, _bundle, _epoch, _seed: fixture.protected_admission
        ),
        authority_source_factory=lambda _candidate, _image: lambda _desired: fixture.authority,
        now=fixture.source.now,
    )
    publisher = factory(
        _candidate(plan),
        plan.starting_mutation_epoch,
        RuntimePurpose.DETACHED_REHEARSAL,
        loaded,  # type: ignore[arg-type]
    )

    with pytest.raises(ProtectedExecutionPrerequisiteSourceError):
        publisher(fixture.lease, changed)

    assert not fixture.store.state_root.exists()


def test_detached_publisher_selects_attested_zero_ceiling_bootstrap_when_seed_is_absent(
    tmp_path: Path,
) -> None:
    """Catch deadlocking a safe legacy manager before protected apply can upgrade it."""
    fixture = _source_fixture(tmp_path)
    plan = fixture.plan
    images = _images(candidate_sha=plan.candidate_sha)
    loaded = SimpleNamespace(
        images=images,
        publication=SimpleNamespace(
            bundle_digest=plan.artifact_bundle_digest,
            candidate_sha=plan.candidate_sha,
            candidate_tree=plan.candidate_tree,
            mutation_epoch=plan.starting_mutation_epoch,
            container_registry="registry.example",
        ),
    )
    bootstrap_calls: list[object] = []

    def seed_absent() -> dict[str, object]:
        raise FileNotFoundError("protected seed is absent")

    def bootstrap_authority(lease):  # type: ignore[no-untyped-def]
        bootstrap_calls.append(lease)
        return "b" * 64

    factory = InstalledExecutionPrerequisitePublisherFactory(
        store=fixture.store,
        container_registry="registry.example",
        manager_configuration_source=lambda: (_ for _ in ()).throw(
            RuntimeError("legacy manager has no configuration endpoint")
        ),
        configuration_seed_source=seed_absent,
        staging_protected_admission_source=(
            lambda _candidate, _bundle, _epoch, _seed: fixture.protected_admission
        ),
        authority_source_factory=lambda _candidate, _image: lambda _desired: fixture.authority,
        now=fixture.source.now,
        zero_ceiling_bootstrap_authority_source=bootstrap_authority,
    )
    publisher = factory(
        _candidate(plan),
        plan.starting_mutation_epoch,
        RuntimePurpose.DETACHED_REHEARSAL,
        loaded,  # type: ignore[arg-type]
    )

    evidence = publisher(fixture.lease, images)

    assert evidence == {
        "mode": "zero-ceiling-bootstrap",
        "schema-version": 0,
        "bootstrap-authority-sha256": "b" * 64,
        "artifact-path": "/",
        "artifact-sha256": "0" * 64,
        "core-artifact-bundle-sha256": "0" * 64,
        "execution-policy-sha256": "0" * 64,
        "executor-profile-seed-sha256": "0" * 64,
        "manager-route-sha256": "0" * 64,
        "access-metadata-sha256": "0" * 64,
        "coexistence-witness-sha256": "0" * 64,
        "legacy-writer-sha256": "0" * 64,
        "rollback-evidence-sha256": "0" * 64,
    }
    assert bootstrap_calls == [fixture.lease]
    assert not fixture.store.state_root.exists()


def test_preflight_protected_admission_derivation_matches_final_apply(tmp_path: Path) -> None:
    """Catch preflight and protected apply deriving different reporter authority."""
    fixture = _source_fixture(tmp_path)
    plan = fixture.plan
    seed = {
        **fixture.seed_values,
        "agent_incarnation": "00000000-0000-4000-8000-000000000098",
        "runtime_database_password": "runtime-password-" + "x" * 48,
    }

    assert staging_database_protected_admission_digest_for_candidate(
        candidate_sha=plan.candidate_sha,
        artifact_bundle_digest=plan.artifact_bundle_digest,
        mutation_epoch=plan.starting_mutation_epoch,
        seed=seed,
    ) == staging_database_protected_admission_digest(plan, seed)
