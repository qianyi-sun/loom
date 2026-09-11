"""Only a current source's committed accepted claim selects native output."""

from dataclasses import replace
from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registered_release import (
    registered_release_input,
)
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "failed", "cancelled", "missing", "source", "platform", "expired"])
async def test_native_artifact_resolver_joins_accepted_claim_to_current_source(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, _request = prepared_input
    _release, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch,
        result="failed" if boundary == "failed" else "live" if boundary == "missing" else "artifact-ready")
    platform = "linux/arm64" if claim.binding.pool_id == "gb10" else "linux/amd64"
    if boundary == "source":
        source = replace(source, candidate=replace(source.candidate, archive_sha256="f" * 64))
    elif boundary == "platform":
        platform = "linux/amd64" if platform == "linux/arm64" else "linux/arm64"
    elif boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": claim.request_id})
    elif boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": source.build_attempt.id})
    resolver = import_module("loom_capacity_build_guard.artifact_resolver").BuildAcceptedArtifactResolver(
        session_factory=factory, installation=installation)
    if boundary == "exact":
        result = await resolver.resolve(source, platform=platform)
        assert result.request.result == "artifact-ready" and result.request.claim == claim
        assert result.request.artifact is not None
    else:
        from sqlalchemy.exc import DBAPIError

        with pytest.raises((ValueError, RuntimeError, DBAPIError)):
            await resolver.resolve(source, platform=platform)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
