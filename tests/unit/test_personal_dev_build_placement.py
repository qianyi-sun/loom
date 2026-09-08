from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

try:
    from loom.personal_dev_build_placement import (
        NativeBuildAllocationBinding,
        NativeBuildNodeObservation,
        NativeBuildPlacementPolicy,
        eligible_native_build_nodes,
    )
except ModuleNotFoundError as exc:
    _IMPORT_ERROR: ModuleNotFoundError | None = exc
else:
    _IMPORT_ERROR = None


_MISSING_FEATURE = pytest.mark.skipif(
    _IMPORT_ERROR is not None,
    reason="personal-dev build placement contracts are not implemented",
)
_NOW = datetime(2026, 9, 8, 16, 0, tzinfo=UTC)
_PROFILE_SHA256 = "a" * 64
_OTHER_PROFILE_SHA256 = "b" * 64
_BOOT_ID = UUID("10000000-0000-0000-0000-000000000001")
_SECOND_BOOT_ID = UUID("10000000-0000-0000-0000-000000000002")
_MANAGER_RESERVATION_ID = UUID("20000000-0000-0000-0000-000000000001")
_CANDIDATE_ID = UUID("20000000-0000-0000-0000-000000000002")
_ATTEMPT_ID = UUID("20000000-0000-0000-0000-000000000003")
_SECOND_ATTEMPT_ID = UUID("20000000-0000-0000-0000-000000000004")
_OWNER_ID = UUID("20000000-0000-0000-0000-000000000005")
_SECOND_OWNER_ID = UUID("20000000-0000-0000-0000-000000000006")


def test_personal_dev_build_placement_api_is_available() -> None:
    """The missing placement module must produce an explicit RED failure."""
    assert _IMPORT_ERROR is None, "loom.personal_dev_build_placement has not been implemented"


def _policy(**changes: object) -> NativeBuildPlacementPolicy:
    values: dict[str, object] = {
        "allowed_node_ids": ("trt-gb10-9", "trt-gb10-3"),
        "runtime_profile_sha256": _PROFILE_SHA256,
        "cpu_millicores": 4000,
        "memory_bytes": 34359738368,
        "minimum_disk_free_bytes": 21474836480,
        "minimum_free_inodes": 100000,
        "max_observation_age_seconds": 60,
    }
    values.update(changes)
    return NativeBuildPlacementPolicy(**values)  # type: ignore[arg-type]


def _observation(node_id: str = "trt-gb10-3", **changes: object) -> NativeBuildNodeObservation:
    values: dict[str, object] = {
        "node_id": node_id,
        "boot_id": _BOOT_ID,
        "observed_at": _NOW,
        "architecture": "aarch64",
        "slurm_state": "IDLE",
        "reserved": False,
        "kvm_available": True,
        "available_cpu_millicores": 4000,
        "available_memory_bytes": 34359738368,
        "available_disk_bytes": 21474836480,
        "available_inodes": 100000,
        "certified_runtime_profile_sha256": _PROFILE_SHA256,
    }
    values.update(changes)
    return NativeBuildNodeObservation(**values)  # type: ignore[arg-type]


def _binding(**changes: object) -> NativeBuildAllocationBinding:
    values: dict[str, object] = {
        "manager_reservation_id": _MANAGER_RESERVATION_ID,
        "candidate_id": _CANDIDATE_ID,
        "candidate_sha256": "c" * 64,
        "attempt_id": _ATTEMPT_ID,
        "attempt_lease_epoch": 11,
        "owner_user_id": _OWNER_ID,
        "runtime_profile_sha256": _PROFILE_SHA256,
        "slurm_cluster": "trt-gb10",
        "slurm_job_id": "9223372036854775807",
        "node_id": "trt-gb10-3",
        "node_boot_id": _BOOT_ID,
    }
    values.update(changes)
    return NativeBuildAllocationBinding(**values)  # type: ignore[arg-type]


@_MISSING_FEATURE
def test_policy_normalizes_node_ids_and_contracts_are_frozen_and_slotted() -> None:
    """Caller order and later assignment must not destabilize placement identity."""
    policy = _policy()

    assert policy.allowed_node_ids == ("trt-gb10-3", "trt-gb10-9")
    assert not hasattr(policy, "__dict__")
    with pytest.raises(FrozenInstanceError):
        policy.cpu_millicores = 8000  # type: ignore[misc]


@_MISSING_FEATURE
def test_eligible_nodes_accept_idle_and_mixed_in_lexical_order() -> None:
    """Returning input order or rejecting exact MIXED would change selection."""
    node9 = _observation("trt-gb10-9", boot_id=_SECOND_BOOT_ID, slurm_state="MIXED")
    node3 = _observation("trt-gb10-3")

    eligible = eligible_native_build_nodes(_policy(), (node9, node3), now=_NOW)

    assert eligible == (node3, node9)


@_MISSING_FEATURE
@pytest.mark.parametrize(
    ("node_id", "changes"),
    (
        ("trt-gb10-1", {}),
        ("trt-gb10-2", {}),
        ("trt-gb10-4", {}),
        ("trt-gb10-3", {"reserved": True}),
        ("trt-gb10-3", {"slurm_state": "DRAIN"}),
        ("trt-gb10-3", {"slurm_state": "DOWN"}),
        ("trt-gb10-3", {"slurm_state": "MIXED+DRAIN"}),
        ("trt-gb10-3", {"architecture": "x86_64"}),
        ("trt-gb10-3", {"kvm_available": False}),
        ("trt-gb10-3", {"certified_runtime_profile_sha256": None}),
        (
            "trt-gb10-3",
            {"certified_runtime_profile_sha256": _OTHER_PROFILE_SHA256},
        ),
    ),
)
def test_eligible_nodes_apply_every_identity_and_readiness_filter(
    node_id: str,
    changes: dict[str, object],
) -> None:
    """Omitting any single identity/readiness filter must admit a named bad node."""
    observation = _observation(node_id, **changes)

    assert eligible_native_build_nodes(_policy(), (observation,), now=_NOW) == ()


@_MISSING_FEATURE
@pytest.mark.parametrize(
    "resource_field",
    (
        "available_cpu_millicores",
        "available_memory_bytes",
        "available_disk_bytes",
        "available_inodes",
    ),
)
def test_eligible_nodes_reject_each_one_unit_resource_shortage(resource_field: str) -> None:
    """Changing any resource comparison from >= to unchecked must fail."""
    required = {
        "available_cpu_millicores": 4000,
        "available_memory_bytes": 34359738368,
        "available_disk_bytes": 21474836480,
        "available_inodes": 100000,
    }
    observation = _observation(**{resource_field: required[resource_field] - 1})

    assert eligible_native_build_nodes(_policy(), (observation,), now=_NOW) == ()


@_MISSING_FEATURE
def test_eligible_nodes_accept_exact_resource_equality() -> None:
    """Using strict greater-than would reject a node meeting the exact request."""
    observation = _observation()

    assert eligible_native_build_nodes(_policy(), (observation,), now=_NOW) == (observation,)


@_MISSING_FEATURE
@pytest.mark.parametrize(
    ("age_seconds", "eligible"),
    ((60, True), (61, False), (-1, False)),
)
def test_eligible_nodes_enforce_exact_freshness_boundary(
    age_seconds: int,
    eligible: bool,
) -> None:
    """Off-by-one age checks and future-report acceptance must fail."""
    observation = _observation(observed_at=_NOW - timedelta(seconds=age_seconds))

    result = eligible_native_build_nodes(_policy(), (observation,), now=_NOW)

    assert result == ((observation,) if eligible else ())


@_MISSING_FEATURE
def test_policy_narrowing_removes_an_otherwise_eligible_node() -> None:
    """Ignoring the policy allowlist would schedule onto removed capacity."""
    node3 = _observation("trt-gb10-3")
    node9 = _observation("trt-gb10-9", boot_id=_SECOND_BOOT_ID)

    assert eligible_native_build_nodes(
        _policy(allowed_node_ids=("trt-gb10-3",)),
        (node9, node3),
        now=_NOW,
    ) == (node3,)


@_MISSING_FEATURE
def test_empty_observation_tuple_returns_empty() -> None:
    """An empty inventory has no node that can be selected."""
    assert eligible_native_build_nodes(_policy(), (), now=_NOW) == ()


@_MISSING_FEATURE
@pytest.mark.parametrize("inventory_kind", ("mutable", "oversized"))
def test_eligible_nodes_reject_mutable_or_oversized_inventory(
    inventory_kind: str,
) -> None:
    """A mutable or over-15 inventory must not become an unstable selection input."""
    observations: object
    if inventory_kind == "mutable":
        observations = [_observation()]
    else:
        observations = (
            *(_observation(f"trt-gb10-{node}") for node in range(1, 16)),
            _observation("trt-gb10-15"),
        )
    with pytest.raises(ValueError, match="observations"):
        eligible_native_build_nodes(_policy(), observations, now=_NOW)  # type: ignore[arg-type]


@_MISSING_FEATURE
def test_eligible_nodes_reject_duplicate_inventory_identity() -> None:
    """Two reports for one node must not be silently merged or selected twice."""
    with pytest.raises(ValueError, match="duplicate"):
        eligible_native_build_nodes(
            _policy(),
            (
                _observation("trt-gb10-3"),
                _observation("trt-gb10-3", boot_id=_SECOND_BOOT_ID),
            ),
            now=_NOW,
        )


@_MISSING_FEATURE
@pytest.mark.parametrize("now", (datetime(2026, 9, 8, 16, 0), "2026-09-08T16:00:00Z"))
def test_eligible_nodes_reject_malformed_now(now: object) -> None:
    """Local or non-datetime clocks must never enter freshness comparison."""
    with pytest.raises(ValueError, match="now"):
        eligible_native_build_nodes(_policy(), (_observation(),), now=now)  # type: ignore[arg-type]


@_MISSING_FEATURE
def test_aware_datetimes_are_normalized_to_utc() -> None:
    """Equivalent offsets must produce one stable observation value."""
    eastern = timezone(timedelta(hours=-4))
    observation = _observation(observed_at=datetime(2026, 9, 8, 12, 0, tzinfo=eastern))

    assert observation.observed_at == _NOW
    assert observation.observed_at.tzinfo is UTC
    assert eligible_native_build_nodes(
        _policy(),
        (observation,),
        now=datetime(2026, 9, 8, 12, 0, tzinfo=eastern),
    ) == (observation,)


@_MISSING_FEATURE
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("allowed_node_ids", ["trt-gb10-3"]),
        ("allowed_node_ids", ()),
        ("allowed_node_ids", ("trt-gb10-3", "trt-gb10-3")),
        ("allowed_node_ids", ("trt-gb10-2",)),
        ("allowed_node_ids", ("trt-gb10-16",)),
        ("allowed_node_ids", ("trt-gb10-03",)),
        ("allowed_node_ids", ("3",)),
        ("allowed_node_ids", (3,)),
        ("runtime_profile_sha256", "0" * 64),
        ("runtime_profile_sha256", "A" * 64),
        ("runtime_profile_sha256", "a" * 63),
    ),
)
def test_policy_rejects_invalid_node_and_digest_contracts(field: str, value: object) -> None:
    """Malformed allowlists or release identities must not define placement policy."""
    with pytest.raises(ValueError):
        _policy(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize(
    "field",
    (
        "cpu_millicores",
        "memory_bytes",
        "minimum_disk_free_bytes",
        "minimum_free_inodes",
        "max_observation_age_seconds",
    ),
)
@pytest.mark.parametrize("value", (0, True, "1", 2**63))
def test_policy_requires_positive_signed_64_bit_integer_quantities(
    field: str,
    value: object,
) -> None:
    """Coercion, zero, or overflow must not relax resource requirements."""
    with pytest.raises(ValueError, match="integer"):
        _policy(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize("node_id", ("trt-gb10-0", "trt-gb10-01", "trt-gb10-16", "3", 3))
def test_observation_rejects_unknown_or_noncanonical_inventory_node(node_id: object) -> None:
    """Only canonical inventory identities 1 through 15 can be observed."""
    with pytest.raises(ValueError, match="node"):
        _observation(node_id)  # type: ignore[arg-type]


@_MISSING_FEATURE
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("boot_id", UUID(int=0)),
        ("boot_id", str(_BOOT_ID)),
        ("observed_at", datetime(2026, 9, 8, 16, 0)),
        ("observed_at", "2026-09-08T16:00:00Z"),
        ("architecture", ""),
        ("architecture", "a" * 65),
        ("architecture", 1),
        ("slurm_state", ""),
        ("slurm_state", "S" * 65),
        ("slurm_state", 1),
        ("reserved", 0),
        ("reserved", "false"),
        ("kvm_available", 1),
        ("kvm_available", "true"),
        ("certified_runtime_profile_sha256", "0" * 64),
        ("certified_runtime_profile_sha256", "A" * 64),
        ("certified_runtime_profile_sha256", "a" * 63),
    ),
)
def test_observation_rejects_malformed_identity_and_status_fields(
    field: str,
    value: object,
) -> None:
    """Malformed observation identity/status must fail before filtering."""
    with pytest.raises(ValueError):
        _observation(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize(
    "field",
    (
        "available_cpu_millicores",
        "available_memory_bytes",
        "available_disk_bytes",
        "available_inodes",
    ),
)
@pytest.mark.parametrize("value", (-1, True, "0", 2**63))
def test_observation_requires_nonnegative_signed_64_bit_resources(
    field: str,
    value: object,
) -> None:
    """Negative, coerced, or overflowing availability must not enter admission."""
    with pytest.raises(ValueError, match="integer"):
        _observation(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize(
    "field",
    (
        "manager_reservation_id",
        "candidate_id",
        "attempt_id",
        "owner_user_id",
        "node_boot_id",
    ),
)
@pytest.mark.parametrize("value", (UUID(int=0), "20000000-0000-0000-0000-000000000001"))
def test_allocation_binding_requires_nonzero_uuid_identities(
    field: str,
    value: object,
) -> None:
    """A zero or string-coerced identity must not bind an allocation."""
    with pytest.raises(ValueError, match="UUID"):
        _binding(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize("field", ("candidate_sha256", "runtime_profile_sha256"))
@pytest.mark.parametrize("value", ("0" * 64, "A" * 64, "a" * 63))
def test_allocation_binding_requires_canonical_nonzero_digests(
    field: str,
    value: object,
) -> None:
    """Malformed source or runtime identity must not be bound to a job."""
    with pytest.raises(ValueError, match="digest"):
        _binding(**{field: value})


@_MISSING_FEATURE
@pytest.mark.parametrize("value", (0, True, "1", 2**63))
def test_allocation_binding_requires_positive_signed_64_bit_lease_epoch(
    value: object,
) -> None:
    """A coerced, zero, or overflowing lease epoch must not bind stale work."""
    with pytest.raises(ValueError, match="integer"):
        _binding(attempt_lease_epoch=value)


@_MISSING_FEATURE
@pytest.mark.parametrize("slurm_job_id", ("0", "01", "1_2", "1.batch", "-1", 1, str(2**63)))
def test_allocation_binding_rejects_zero_array_step_or_malformed_job_ids(
    slurm_job_id: object,
) -> None:
    """Array/step aliases and invalid decimal jobs must not bind as base jobs."""
    with pytest.raises(ValueError, match="job"):
        _binding(slurm_job_id=slurm_job_id)


@_MISSING_FEATURE
@pytest.mark.parametrize(
    "node_id", ("trt-gb10-1", "trt-gb10-2", "trt-gb10-03", "trt-gb10-16", "3", 3)
)
def test_allocation_binding_permits_only_canonical_worker_node_ids(node_id: object) -> None:
    """Controllers and malformed node aliases must not receive build allocations."""
    with pytest.raises(ValueError, match="node"):
        _binding(node_id=node_id)


@_MISSING_FEATURE
@pytest.mark.parametrize("slurm_cluster", ("", "gb10-personal-dev", "trt-gb10-copy", 1))
def test_allocation_binding_requires_exact_cluster_identity(slurm_cluster: object) -> None:
    """An absent or invented cluster cannot identify this inventory's Slurm job."""
    with pytest.raises(ValueError, match="cluster"):
        _binding(slurm_cluster=slurm_cluster)


@_MISSING_FEATURE
def test_allocation_bindings_preserve_distinct_owner_attempt_and_node_identity() -> None:
    """Concurrent owners must not collapse onto one attempt or node binding."""
    first = _binding()
    second = _binding(
        manager_reservation_id=UUID("20000000-0000-0000-0000-000000000007"),
        owner_user_id=_SECOND_OWNER_ID,
        attempt_id=_SECOND_ATTEMPT_ID,
        attempt_lease_epoch=12,
        slurm_job_id="42",
        node_id="trt-gb10-9",
        node_boot_id=_SECOND_BOOT_ID,
    )

    assert (
        first.owner_user_id,
        first.attempt_id,
        first.attempt_lease_epoch,
        first.slurm_cluster,
        first.slurm_job_id,
        first.node_id,
        first.node_boot_id,
    ) == (
        _OWNER_ID,
        _ATTEMPT_ID,
        11,
        "trt-gb10",
        "9223372036854775807",
        "trt-gb10-3",
        _BOOT_ID,
    )
    assert (
        second.owner_user_id,
        second.attempt_id,
        second.attempt_lease_epoch,
        second.slurm_cluster,
        second.slurm_job_id,
        second.node_id,
        second.node_boot_id,
    ) == (
        _SECOND_OWNER_ID,
        _SECOND_ATTEMPT_ID,
        12,
        "trt-gb10",
        "42",
        "trt-gb10-9",
        _SECOND_BOOT_ID,
    )
    assert first != second
