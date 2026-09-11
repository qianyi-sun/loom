"""Native eligibility is approved release policy, not a worker capability string."""

from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    TrustedLaunchContextV2,
    TrustedLaunchRenderError,
    canonical_launch_policy_digest,
    render_launch_request,
)
from loom_capacity_executor.runtime import canonical_approved_profiles_digest
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.unit.test_capacity_executor_launch_renderer import (
    launch_context_fixture,
    operator_profile_fixture,
)


def native_contract_fixture() -> dict[str, object]:
    return {
        "schema_version": 2,
        "protocol": "loom.task-image-native-execution/v2",
        "launch_protocol": "immutable-container-stdin/v1",
        "platform": "linux/amd64",
        "root_key_id": "execution-root-1",
        "environment": "development",
        "root_public_key": base64.urlsafe_b64encode(bytes(range(32))).rstrip(b"=").decode(),
        "root_activated_at": "2026-09-01T00:00:00Z",
        "root_expires_at": "2026-10-01T00:00:00Z",
    }


def native_profile_fixture() -> OperatorLaunchProfileV2:
    raw = operator_profile_fixture().model_dump(mode="json")
    raw["native_execution"] = native_contract_fixture()
    return OperatorLaunchProfileV2.model_validate(raw)


def test_legacy_profile_has_no_native_eligibility_and_preserves_pinned_bytes() -> None:
    profile = operator_profile_fixture()
    assert profile.native_execution is None
    # Captured on the unchanged predecessor, not recomputed expectations.
    assert canonical_launch_policy_digest(profile) == (
        "b57dcc6c69c1669d15bfb93dffc867e1a328fa0a21fbbe3dc78f437b0a991d27"
    )
    assert canonical_approved_profiles_digest((profile,)) == (
        "aa44c06f665844d2b6a6f6b5f96dc232affeb29a98a00b2a2ed87898a534e3b4"
    )
    assert hashlib.sha256(canonical_executable_bytes(profile)).hexdigest() == (
        "ebb98585ad60f65b544e109e84d5bd51061a95b8bba6aa86c79676fadbad45ee"
    )
    assert "native_execution" not in profile.model_dump(mode="json")


def test_approved_native_profile_resolves_the_release_pinned_public_root() -> None:
    profile = native_profile_fixture()
    assert profile.native_execution is not None
    root = profile.native_execution.trust_root()
    assert root.key_id == "execution-root-1"
    assert root.environment == "development"
    assert root.public_key == bytes(range(32))
    assert root.activated_at == datetime(2026, 9, 1, tzinfo=UTC)
    assert root.expires_at == datetime(2026, 10, 1, tzinfo=UTC)
    wire = canonical_executable_bytes(profile)
    assert b'"native_execution":{' in wire
    assert canonical_executable_bytes(OperatorLaunchProfileV2.model_validate_json(wire)) == wire


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("platform", "linux/arm64"),
        ("root_key_id", "execution-root-2"),
        ("environment", "staging"),
        ("root_public_key", base64.urlsafe_b64encode(b"b" * 32).rstrip(b"=").decode()),
        ("root_activated_at", "2026-09-02T00:00:00Z"),
        ("root_expires_at", "2026-10-02T00:00:00Z"),
    ],
)
def test_every_native_trust_pin_changes_both_approved_policy_digests(
    field: str, value: str,
) -> None:
    profile = native_profile_fixture()
    native = native_contract_fixture() | {field: value}
    changed = OperatorLaunchProfileV2.model_validate(
        profile.model_dump(mode="json") | {"native_execution": native}
    )
    assert canonical_launch_policy_digest(changed) != canonical_launch_policy_digest(profile)
    assert canonical_approved_profiles_digest((changed,)) != canonical_approved_profiles_digest((profile,))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protocol", "loom.task-image-native-execution/v1"),
        ("launch_protocol", "compose"),
        ("platform", "linux/any"),
        ("root_public_key", "A" * 42),
        ("root_public_key", "A" * 42 + "B"),  # Noncanonical base64 trailing bits.
        ("root_activated_at", "2026-09-01T00:00:00+00:00"),
        ("root_expires_at", "2026-09-01T00:00:00Z"),
        ("root_expires_at", "2026-08-31T00:00:00Z"),
        ("root_expires_at", "2026-10-01T00:00:00.1Z"),
        ("environment", ""),
        ("root_key_id", ""),
        ("restart", "always"),
    ],
)
def test_invalid_or_ambiguous_native_release_contract_is_rejected(
    field: str, value: str,
) -> None:
    raw = operator_profile_fixture().model_dump(mode="json")
    raw["native_execution"] = native_contract_fixture() | {field: value}
    with pytest.raises(ValidationError):
        OperatorLaunchProfileV2.model_validate(raw)


def _native_context() -> TrustedLaunchContextV2:
    context = launch_context_fixture()
    profile = native_profile_fixture()
    digest = canonical_launch_policy_digest(profile)
    return replace(
        context,
        profile=profile.model_copy(update={"controller_authority_sha256": digest}),
        controller_authority=context.controller_authority.model_copy(
            update={"controller_authority_sha256": digest}
        ),
        submitted_at=datetime(2026, 9, 11, tzinfo=UTC),
    )


def test_native_launch_retains_exact_approved_worker_image_and_config() -> None:
    context = _native_context()
    request = render_launch_request(context)
    assert request.image_digest == context.profile.image_digest
    assert request.trusted_launcher_config == context.profile.trusted_launcher_config


def test_native_root_substitution_after_approval_rejects_scheduler_launch() -> None:
    context = _native_context()
    assert context.profile.native_execution is not None
    changed = context.profile.native_execution.model_copy(update={"root_key_id": "other-root"})
    context = replace(context, profile=context.profile.model_copy(update={"native_execution": changed}))
    with pytest.raises(TrustedLaunchRenderError, match="policy digest"):
        render_launch_request(context)


@pytest.mark.parametrize(
    "submitted_at",
    [datetime(2026, 8, 31, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)],
)
def test_native_launch_rejects_root_outside_its_validity(submitted_at: datetime) -> None:
    context = replace(_native_context(), submitted_at=submitted_at)
    with pytest.raises(TrustedLaunchRenderError, match="native execution root"):
        render_launch_request(context)
