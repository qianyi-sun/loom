from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from loom.personal_dev_capacity import (
    PersonalDevCapacityManagerBinding,
    PersonalDevCapacityProjectionError,
)
from loom.personal_dev_runtime import (
    PersonalDevAcceptanceInterlock,
    PersonalDevAcceptanceInterlockError,
    PersonalDevOperationalInterlock,
    PersonalDevOperationalInterlockError,
)

_PLAN_SHA256 = "a" * 64
_NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)


def _manager_binding() -> PersonalDevCapacityManagerBinding:
    return PersonalDevCapacityManagerBinding(
        authority_incarnation=UUID("00000000-0000-0000-0000-000000000101"),
        observer_principal_id="personal-dev-lifecycle",
        configuration_epoch=7,
        execution_state="shadow",
        execution_epoch=0,
        executable_new_capacity_ceiling=0,
    )


def _binding_json(**manager_overrides: object) -> str:
    manager: dict[str, object] = {
        "authority_incarnation": "00000000-0000-0000-0000-000000000101",
        "configuration_epoch": 7,
        "executable_new_capacity_ceiling": 0,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "observer_principal_id": "personal-dev-lifecycle",
    }
    manager.update(manager_overrides)
    return json.dumps(
        {
            "acceptance_plan_sha256": _PLAN_SHA256,
            "expires_at": "2026-08-18T00:00:00Z",
            "manager": manager,
            "schema_version": 1,
            "started_at": "2026-08-17T00:00:00Z",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


class _Projector:
    def __init__(
        self,
        binding: PersonalDevCapacityManagerBinding | Exception,
    ) -> None:
        self.binding = binding
        self.calls = 0

    async def current_manager_binding(self) -> PersonalDevCapacityManagerBinding:
        self.calls += 1
        if isinstance(self.binding, Exception):
            raise self.binding
        return self.binding


async def test_acceptance_interlock_accepts_the_planned_initial_boundary() -> None:
    projector = _Projector(_manager_binding())
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=projector,  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    await interlock.assert_ready(now=_NOW.astimezone(timezone(timedelta(hours=-4))))

    assert projector.calls == 1
    assert interlock.expected_manager == _manager_binding()


async def test_acceptance_interlock_allows_monotonic_configuration_advancement() -> None:
    projector = _Projector(replace(_manager_binding(), configuration_epoch=9))
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=projector,  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    await interlock.assert_ready(now=_NOW)

    assert projector.calls == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"authority_incarnation": UUID("00000000-0000-0000-0000-000000000102")},
        {"observer_principal_id": "different-lifecycle"},
        {"configuration_epoch": 6},
        {"execution_state": "prepared", "execution_epoch": 1},
        {"execution_state": "drain-only", "execution_epoch": 2},
        {
            "execution_state": "active",
            "execution_epoch": 1,
            "executable_new_capacity_ceiling": 1,
        },
    ],
)
async def test_acceptance_interlock_rejects_any_manager_drift(
    changes: dict[str, object],
) -> None:
    observed = replace(_manager_binding(), **changes)
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=_Projector(observed),  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        await interlock.assert_ready(now=_NOW)

    assert exc.value.code == "capacity-manager-binding-drift"


@pytest.mark.parametrize(
    ("now", "code"),
    [
        (datetime(2026, 8, 16, 23, 59, 59, tzinfo=UTC), "acceptance-window-not-open"),
        (datetime(2026, 8, 18, 0, 0, tzinfo=UTC), "acceptance-window-expired"),
        (datetime(2026, 8, 18, 0, 0, 1, tzinfo=UTC), "acceptance-window-expired"),
    ],
)
async def test_acceptance_interlock_rejects_outside_the_exact_window(
    now: datetime,
    code: str,
) -> None:
    projector = _Projector(_manager_binding())
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=projector,  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        await interlock.assert_ready(now=now)

    assert exc.value.code == code
    assert projector.calls == 0


async def test_acceptance_interlock_maps_manager_transport_failure_to_stable_blocker() -> None:
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=_Projector(PersonalDevCapacityProjectionError("synthetic secret-bearing detail")),  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        await interlock.assert_ready(now=_NOW)

    assert exc.value.code == "capacity-manager-unavailable"
    assert "synthetic" not in str(exc.value)


@pytest.mark.parametrize(
    "binding_json",
    [
        "{}",
        _binding_json(configuration_epoch=True),
        _binding_json(execution_epoch=False),
        _binding_json(executable_new_capacity_ceiling=True),
        _binding_json(authority_incarnation="00000000000000000000000000000101"),
        _binding_json(observer_principal_id="wrong principal"),
        _binding_json(execution_state="active"),
        _binding_json(executable_new_capacity_ceiling=1),
        _binding_json() + "\n",
        _binding_json().replace(
            '"schema_version":1',
            '"schema_version":1,"schema_version":1',
        ),
    ],
)
def test_acceptance_interlock_rejects_ambiguous_or_executable_binding(
    binding_json: str,
) -> None:
    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        PersonalDevAcceptanceInterlock.from_json(
            projector=_Projector(_manager_binding()),  # type: ignore[arg-type]
            binding_json=binding_json,
            expected_plan_sha256=_PLAN_SHA256,
        )

    assert exc.value.code == "acceptance-binding-invalid"


def test_acceptance_interlock_rejects_mismatched_plan_digest() -> None:
    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        PersonalDevAcceptanceInterlock.from_json(
            projector=_Projector(_manager_binding()),  # type: ignore[arg-type]
            binding_json=_binding_json(),
            expected_plan_sha256="b" * 64,
        )

    assert exc.value.code == "acceptance-binding-invalid"


async def test_acceptance_interlock_rejects_naive_observation_time() -> None:
    interlock = PersonalDevAcceptanceInterlock.from_json(
        projector=_Projector(_manager_binding()),  # type: ignore[arg-type]
        binding_json=_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    with pytest.raises(PersonalDevAcceptanceInterlockError) as exc:
        await interlock.assert_ready(now=datetime(2026, 8, 17, 12, 0))

    assert exc.value.code == "acceptance-time-invalid"


def _operational_binding_json(**manager_overrides: object) -> str:
    manager: dict[str, object] = {
        "authority_incarnation": "00000000-0000-0000-0000-000000000101",
        "configuration_epoch": 7,
        "executable_new_capacity_ceiling": 0,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "observer_principal_id": "personal-dev-lifecycle",
    }
    manager.update(manager_overrides)
    return json.dumps(
        {
            "acceptance_result_sha256": "b" * 64,
            "manager": manager,
            "operational_plan_sha256": _PLAN_SHA256,
            "schema_version": 1,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


async def test_operational_interlock_has_no_expiry_but_rechecks_manager() -> None:
    projector = _Projector(replace(_manager_binding(), configuration_epoch=99))
    interlock = PersonalDevOperationalInterlock.from_json(
        projector=projector,  # type: ignore[arg-type]
        binding_json=_operational_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    await interlock.assert_ready(now=datetime(2036, 8, 17, 12, 0, tzinfo=UTC))

    assert projector.calls == 1
    assert interlock.acceptance_result_sha256 == "b" * 64


@pytest.mark.parametrize(
    "binding_json",
    [
        _operational_binding_json() + "\n",
        _operational_binding_json(executable_new_capacity_ceiling=1),
        _operational_binding_json(execution_state="active", execution_epoch=1),
        _operational_binding_json().replace('"acceptance_result_sha256":"' + "b" * 64 + '"', '"acceptance_result_sha256":"' + "0" * 64 + '"'),
    ],
)
def test_operational_interlock_rejects_ambiguous_or_executable_binding(
    binding_json: str,
) -> None:
    with pytest.raises(PersonalDevOperationalInterlockError) as exc:
        PersonalDevOperationalInterlock.from_json(
            projector=_Projector(_manager_binding()),  # type: ignore[arg-type]
            binding_json=binding_json,
            expected_plan_sha256=_PLAN_SHA256,
        )

    assert exc.value.code == "operational-binding-invalid"


async def test_operational_interlock_fails_closed_on_manager_drift() -> None:
    interlock = PersonalDevOperationalInterlock.from_json(
        projector=_Projector(replace(_manager_binding(), execution_state="prepared", execution_epoch=1)),  # type: ignore[arg-type]
        binding_json=_operational_binding_json(),
        expected_plan_sha256=_PLAN_SHA256,
    )

    with pytest.raises(PersonalDevOperationalInterlockError) as exc:
        await interlock.assert_ready(now=_NOW)

    assert exc.value.code == "capacity-manager-binding-drift"
