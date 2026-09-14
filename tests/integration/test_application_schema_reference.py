"""Generate the bundled reference through actual isolated provisioning."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
import scripts.build_application_schema_reference as builder
from alembic.config import Config
from alembic.script import ScriptDirectory
from scripts.build_application_schema_reference import build_application_schema_reference

from loom.application_schema_reference import (
    ApplicationSchemaReferenceError,
    application_schema_reference,
    require_application_schema_reference,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", ["0142/guard_0033", "0134/guard_0030"])
@pytest.mark.parametrize("postgres_major", [16, 17])
@pytest.mark.parametrize("profile", ["legacy-owner", "sealed-owner", "staging-readonly-legacy-owner", "staging-readonly-sealed-owner", "cnpg-staging-legacy-owner", "cnpg-staging-sealed-owner"])
async def test_bundled_reference_matches_independent_actual_provisioning(
    monkeypatch: pytest.MonkeyPatch,
    profile: str,
    postgres_major: int,
    revision: str,
) -> None:
    # Neither inherited migration setting is authority for reference generation.
    monkeypatch.setenv("LOOM_DB_URL", "postgresql://invalid.invalid:1/not_a_reference")
    monkeypatch.setenv("LOOM_DB_OWNER_ROLE", "not_the_reference_owner")
    observations = []
    original = builder._observe_fresh_database

    async def capture(admin_url, identity, **kwargs):
        observed = await original(admin_url, identity, **kwargs)
        observations.append(observed)
        return observed

    monkeypatch.setattr(builder, "_observe_fresh_database", capture)
    generated = await build_application_schema_reference(profile=profile, postgres_major=postgres_major, revision=revision)
    expected = application_schema_reference(profile=profile, postgres_major=postgres_major, revision=revision)
    assert generated == expected
    assert generated.object_count > 200
    assert generated.postgres_major == postgres_major
    assert generated.inventory_sha256 != "0" * 64
    assert len(observations) == 2
    for observed in observations:
        require_application_schema_reference(observed, profile=profile, revision=revision)
        with pytest.raises(ApplicationSchemaReferenceError, match="trusted reference"):
            require_application_schema_reference(
                replace(observed, postgres_major=17 if postgres_major == 16 else 16), profile=profile, revision=revision
            )
        with pytest.raises(ApplicationSchemaReferenceError, match="trusted reference"):
            require_application_schema_reference(
                observed, profile="sealed-owner" if profile == "legacy-owner" else "legacy-owner", revision=revision
            )
    observed = observations[0]
    changed = replace(
        observed,
        objects=(replace(observed.objects[0], definition_sha256="f" * 64), *observed.objects[1:]),
    )
    with pytest.raises(ApplicationSchemaReferenceError, match="trusted reference"):
        require_application_schema_reference(changed, profile=profile, revision=revision)


def test_sealed_reference_is_distinct_from_legacy_reference() -> None:
    legacy = application_schema_reference(profile="legacy-owner")
    sealed = application_schema_reference(profile="sealed-owner")
    assert legacy.inventory_sha256 != sealed.inventory_sha256
    assert legacy.profile == "legacy-owner"
    assert sealed.profile == "sealed-owner"


def test_bundled_reference_pins_actual_release_image_and_migration_heads() -> None:
    root = Path(__file__).resolve().parents[2]
    expected = application_schema_reference()
    external = json.loads((root / "deploy/dev-fleet/personal-dev-external-images.json").read_text())
    assert expected.postgres_image == external["images"]["postgres"]["reference"]
    for directory, head in (
        ("migrations", expected.application_head),
        ("capacity_guard_migrations", expected.guard_head),
    ):
        config = Config(str(root / directory / "alembic.ini"))
        config.set_main_option("script_location", str(root / directory))
        assert ScriptDirectory.from_config(config).get_heads() == [head]
