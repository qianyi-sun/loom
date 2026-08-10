"""Fleet-state authority and legacy drift tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from loom_capacity_manager.contracts import ProfileReferenceV1
from loom_capacity_manager.fleet_state import (
    FleetStateError,
    inventory_legacy_topology,
    load_fleet_manifest,
    load_subject_configuration,
    validate_profile_narrowing,
)
from tests.capacity_fixtures import profile_reference, shape, valid_profile_payload

FIXTURES = Path("tests/fixtures/capacity")
LEGACY = (
    Path("deploy/environment-state/development.toml"),
    Path("deploy/environment-state/staging.toml"),
    Path("deploy/environment-state/production.toml"),
)


def test_environment_profile_can_narrow_but_not_redefine_pool() -> None:
    manifest = load_fleet_manifest(FIXTURES / "fleet-v1.toml")
    validate_profile_narrowing(
        manifest,
        profile_reference(manifest, eligible_resource_domains=("gb10-arm",)),
    )
    with pytest.raises(ValidationError, match="controller"):
        ProfileReferenceV1.model_validate(
            valid_profile_payload(manifest) | {"controller": "different-controller"}
        )


def test_profile_cannot_add_domain_or_change_generation() -> None:
    manifest = load_fleet_manifest(FIXTURES / "fleet-v1.toml")
    with pytest.raises(FleetStateError, match="unknown resource domain"):
        validate_profile_narrowing(
            manifest,
            profile_reference(
                manifest,
                eligible_resource_domains=("not-a-domain",),
                worker_shapes=(
                    shape(compatible_domain_ids=("not-a-domain",)),
                ),
            ),
        )
    with pytest.raises(FleetStateError, match="pool generation"):
        validate_profile_narrowing(
            manifest,
            profile_reference(manifest, pool_generation=2),
        )


def test_current_legacy_replacement_node_drift_is_reported() -> None:
    report = inventory_legacy_topology(LEGACY)
    assert not report.clean
    assert {conflict.pool_id for conflict in report.conflicts} == {"gb10", "oldlab"}
    assert "allowed_nodes" in {field for conflict in report.conflicts for field in conflict.fields}


def test_inventory_is_deterministic_bounded_json_without_paths_or_credentials() -> None:
    first = inventory_legacy_topology(tuple(reversed(LEGACY)))
    second = inventory_legacy_topology(LEGACY)
    assert first.to_json() == second.to_json()
    payload = json.loads(first.to_json())
    assert payload["schema_version"] == 1
    assert len(first.to_json()) < 16_384
    assert "token" not in first.to_json().lower()
    assert "/home/" not in first.to_json()


def test_fleet_loader_rejects_supplied_digest_mismatch(tmp_path: Path) -> None:
    source = (FIXTURES / "fleet-v1.toml").read_text(encoding="utf-8")
    target = tmp_path / "fleet.toml"
    target.write_text(
        source.replace(
            "de41e44f7648841ece3861fa91f3f3c92fb1152c1cb6b30218e7a5e8d0ac0804",
            "0" * 64,
            1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(FleetStateError, match="pool digest"):
        load_fleet_manifest(target)


def test_subject_file_has_dual_pool_profiles_and_default_zero_minimum() -> None:
    subjects = load_subject_configuration(FIXTURES / "subjects-v1.toml")
    assert len(subjects) == 1
    assert subjects[0].min_slots == 0
    assert tuple(profile.pool_id for profile in subjects[0].profiles) == ("gb10", "oldlab")


def test_checked_in_fleet_files_are_explicitly_synthetic() -> None:
    example_path = Path("deploy/fleet-state/schema-v1.example.toml")
    example = example_path.read_text(encoding="utf-8")
    readme = Path("deploy/fleet-state/README.md").read_text(encoding="utf-8")
    assert "synthetic" in example.lower()
    assert "not a live fleet manifest" in readme.lower()
    assert "reviewed operator reconciliation" in readme.lower()
    manifest = load_fleet_manifest(example_path)
    assert all(
        node.node_id.startswith("synthetic-")
        for pool in manifest.pools
        for domain in pool.resource_domains
        for node in domain.nodes
    )
