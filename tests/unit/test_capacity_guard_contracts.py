"""Strict canonical contracts for protected environment admission."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from loom.models.capabilities import RequiredCapabilities
from loom_capacity_guard.contracts import (
    MAX_CONTRACT_BYTES,
    CapacityGuardContractError,
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
    StrictGuardModel,
    canonical_bytes,
    canonical_digest,
    seal_requirements,
)


def _fence(**changes: object) -> GuardFenceV1:
    values: dict[str, object] = {
        "environment_id": "dev-alice",
        "subject_id": uuid4(),
        "subject_incarnation": uuid4(),
        "authority_incarnation": uuid4(),
        "reporter_incarnation": uuid4(),
        "deployment_generation": 1,
        "configuration_generation": 1,
        "candidate_digest": "a" * 64,
    }
    values.update(changes)
    return GuardFenceV1.model_validate(values)


def _attempt(**changes: object) -> ProtectedAttemptV1:
    values: dict[str, object] = {
        "trial_id": uuid4(),
        "protected_attempt_id": uuid4(),
        "execution_generation": 1,
        "requirements_digest": "b" * 64,
    }
    values.update(changes)
    return ProtectedAttemptV1.model_validate(values)


def test_sealed_requirements_are_canonical_across_input_order() -> None:
    left = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("public", "allowlist"),
    )
    right = SealedRequirementsV1(
        os="linux",
        cpu_arch="any",
        gpu_vendor="none",
        network_policies=("allowlist", "public"),
    )
    assert left.network_policies == ("allowlist", "public")
    assert left == right
    assert canonical_bytes(left) == canonical_bytes(right)
    assert canonical_digest(left) == canonical_digest(right)


def test_seal_requirements_adapts_normalized_runtime_contract() -> None:
    requirements = RequiredCapabilities(
        os="windows",
        cpu_arch="arm64",
        gpu_vendor="nvidia",
        network_policies=frozenset({"allowlist", "no-network"}),
    )
    sealed = seal_requirements(requirements, required_pool="gb10")
    assert sealed == SealedRequirementsV1(
        os="windows",
        cpu_arch="arm64",
        gpu_vendor="nvidia",
        network_policies=("allowlist", "no-network"),
        required_pool="gb10",
    )
    assert SealedRequirementsV1.model_validate_json(canonical_bytes(sealed)) == sealed


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("os", "darwin"),
        ("cpu_arch", "amd64"),
        ("gpu_vendor", "amd"),
        ("required_pool", "dev-alice"),
        ("network_policies", ("public", "public")),
        ("network_policies", ("none",)),
    ],
)
def test_sealed_requirements_reject_unknown_or_duplicate_values(
    field: str,
    value: object,
) -> None:
    values: dict[str, object] = {
        "os": "linux",
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "network_policies": ("public",),
    }
    values[field] = value
    with pytest.raises(ValidationError):
        SealedRequirementsV1.model_validate(values)


def test_contracts_reject_unknown_fields_versions_and_coercion() -> None:
    with pytest.raises(ValidationError):
        _fence(untrusted=True)
    with pytest.raises(ValidationError):
        _fence(schema_version=2)
    with pytest.raises(ValidationError):
        _fence(deployment_generation="1")
    with pytest.raises(ValidationError):
        _fence(deployment_generation=True)
    with pytest.raises(ValidationError):
        _attempt(execution_generation="1")


@pytest.mark.parametrize("environment_id", ["Dev-alice", "dev/alice", "", "a" * 129])
def test_guard_fence_rejects_noncanonical_environment_ids(environment_id: str) -> None:
    with pytest.raises(ValidationError):
        _fence(environment_id=environment_id)


def test_guard_fence_is_disabled_and_zero_epoch_only() -> None:
    fence = _fence()
    assert fence.authority_mode == "disabled"
    assert fence.allocation_epoch == 0
    assert fence.reporter_high_water == 0
    with pytest.raises(ValidationError):
        _fence(authority_mode="global")
    with pytest.raises(ValidationError):
        _fence(allocation_epoch=1)
    with pytest.raises(ValidationError):
        _fence(reporter_high_water=-1)
    with pytest.raises(ValidationError):
        _fence(configuration_generation=0)
    with pytest.raises(ValidationError):
        _fence(deployment_generation=2**63)


@pytest.mark.parametrize("digest", ["a" * 63, "A" * 64, "g" * 64, "sha256:" + "a" * 64])
def test_contracts_reject_noncanonical_digests(digest: str) -> None:
    with pytest.raises(ValidationError):
        _fence(candidate_digest=digest)
    with pytest.raises(ValidationError):
        _attempt(requirements_digest=digest)


def test_protected_attempt_is_queued_and_unassigned_only() -> None:
    attempt = _attempt()
    assert attempt.claim_state == "queued"
    assert attempt.assigned_pool is None
    assert attempt.assignment_epoch is None
    assert attempt.worker_id is None
    assert attempt.claim_epoch is None
    for field, value in (
        ("claim_state", "claimed"),
        ("assigned_pool", "oldlab"),
        ("assignment_epoch", 1),
        ("worker_id", uuid4()),
        ("claim_epoch", 1),
    ):
        with pytest.raises(ValidationError):
            _attempt(**{field: value})


def test_protected_attempt_rejects_reused_trial_identity() -> None:
    identity = uuid4()
    with pytest.raises(ValidationError, match="distinct"):
        _attempt(trial_id=identity, protected_attempt_id=identity)


def test_canonical_encoding_is_compact_ascii_and_hashes_exact_bytes() -> None:
    class UnicodeV1(StrictGuardModel):
        value: str

    fence = _fence()
    encoded = canonical_bytes(fence)
    assert encoded.decode("ascii")
    assert b" " not in encoded
    assert json.loads(encoded)["candidate_digest"] == "a" * 64
    assert canonical_digest(fence) == hashlib.sha256(encoded).hexdigest()
    assert b"\\u00e9" in canonical_bytes(UnicodeV1(value="é"))


def test_canonical_encoding_rejects_wrong_model_and_oversize() -> None:
    class OversizedV1(StrictGuardModel):
        value: str

    with pytest.raises(CapacityGuardContractError, match="schema-v1"):
        canonical_bytes(object())  # type: ignore[arg-type]
    oversized = OversizedV1(value="x" * MAX_CONTRACT_BYTES)
    with pytest.raises(CapacityGuardContractError, match="maximum"):
        canonical_bytes(oversized)


def test_json_round_trip_accepts_arrays_without_relaxing_numeric_strictness() -> None:
    value = SealedRequirementsV1.model_validate_json(
        b'{"schema_version":1,"os":"linux","cpu_arch":"x86_64",'
        b'"gpu_vendor":"none","network_policies":["public"],"required_pool":null}'
    )
    assert value.network_policies == ("public",)
    with pytest.raises(ValidationError):
        GuardFenceV1.model_validate_json(
            canonical_bytes(_fence()).replace(
                b'"deployment_generation":1', b'"deployment_generation":"1"'
            )
        )


def test_uuid_fields_remain_uuid_objects() -> None:
    trial_id = uuid4()
    attempt = _attempt(trial_id=trial_id)
    assert attempt.trial_id == trial_id
    assert isinstance(attempt.trial_id, UUID)
