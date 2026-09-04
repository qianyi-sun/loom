from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator.protected_execution_preparation_dependency import (
    ProtectedExecutionPreparationDependencyGuard,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component import (
    derive_protected_staging_capacity_configuration,
)
from tests.loom_cli.rollout.operator.test_protected_execution_prerequisite_source import (
    _source_fixture,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_configuration_component import (
    _active_document,
)


def test_dependency_guard_accepts_only_exact_live_configuration_and_execution_authority(
    tmp_path: Path,
) -> None:
    fixture = _source_fixture(tmp_path)
    publication = fixture.source.publish(fixture.lease)
    artifact = fixture.store.read(publication)
    active = _active_document(fixture.desired.fleet, fixture.desired.subjects)
    desired = derive_protected_staging_capacity_configuration(
        active_document=active,
        seed_values=fixture.seed_values,
        target_generation=fixture.plan.starting_mutation_epoch + 1,
    )
    authority_calls: list[object] = []

    def authority_source(value):
        authority_calls.append(value)
        return fixture.authority

    guard = ProtectedExecutionPreparationDependencyGuard(
        desired_configuration_source=lambda _plan: desired,
        authority_source=authority_source,
    )

    digest = guard(fixture.plan, artifact)

    assert len(digest) == 64
    assert digest != "0" * 64
    assert authority_calls == [desired]


def test_dependency_guard_rejects_configuration_or_signed_witness_drift(
    tmp_path: Path,
) -> None:
    fixture = _source_fixture(tmp_path)
    publication = fixture.source.publish(fixture.lease)
    artifact = fixture.store.read(publication)
    active = _active_document(fixture.desired.fleet, fixture.desired.subjects)
    desired = derive_protected_staging_capacity_configuration(
        active_document=active,
        seed_values=fixture.seed_values,
        target_generation=fixture.plan.starting_mutation_epoch + 1,
    )
    stale_witness = replace(
        fixture.authority,
        coexistence_witness_sha256={"gb10": "9" * 64, "oldlab": "6" * 64},
    )

    with pytest.raises(ValueError, match="configuration is not exact"):
        ProtectedExecutionPreparationDependencyGuard(
            desired_configuration_source=lambda _plan: fixture.desired,
            authority_source=lambda _desired: fixture.authority,
        )(fixture.plan, artifact)

    with pytest.raises(ValueError, match="authority drifted"):
        ProtectedExecutionPreparationDependencyGuard(
            desired_configuration_source=lambda _plan: desired,
            authority_source=lambda _desired: stale_witness,
        )(fixture.plan, artifact)
