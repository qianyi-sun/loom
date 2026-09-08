"""Active acceptance pins a V3 authority, without reinterpreting zero-capacity evidence."""

import json
from datetime import timedelta
from importlib import import_module

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from tests.unit.test_capacity_membership import delegated_input_with_new_owner
from tests.unit.test_personal_dev_reconciler import _NOW


def admission_values():
    preparation = delegated_input_with_new_owner().preparation
    execution = ExecutionAuthorityV2(
        authority_incarnation=preparation.authority_incarnation,
        writer_epoch=preparation.expected_writer_epoch,
        configuration_epoch=preparation.configuration_epoch,
        execution_epoch=11,
        execution_manifest_sha256=canonical_executable_digest(preparation),
        execution_state="active",
        executable_new_capacity_ceiling=preparation.requested_ceiling,
        executable_new_capacity_rate_per_minute=preparation.requested_rate_per_minute,
        trusted_fleet_release_sha256=preparation.trusted_fleet_release_sha256,
    )
    return {
        "schema_version": 1,
        "capacity_mode": "membership-v1",
        "purpose": "acceptance",
        "plan_sha256": "a" * 64,
        "preparation": preparation.model_dump(mode="json"),
        "execution": execution.model_dump(mode="json"),
        "started_at": (_NOW - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": (_NOW + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _wire(values):
    return json.dumps(values, sort_keys=True, separators=(",", ":")).encode()


def test_active_acceptance_binding_retains_complete_prepared_delegation():
    module = import_module("loom.personal_dev_membership_admission")
    binding = module.parse_membership_acceptance_binding(_wire(admission_values()), expected_plan_sha256="a" * 64)
    assert binding.preparation.schema_version == 3
    assert binding.namespace_id == binding.preparation.personal_membership.namespace_id
    assert binding.management_principal_id == binding.preparation.personal_membership.management_principal_id
    assert module.parse_membership_acceptance_binding(canonical_bytes(binding), expected_plan_sha256="a" * 64) == binding


@pytest.mark.parametrize("tamper", (
    "plan", "manifest", "delegate", "namespace", "writer", "release", "ceiling",
    "legacy", "purpose", "float_version", "duplicate", "naive", "window",
))
def test_active_acceptance_binding_rejects_inconsistent_or_ambiguous_authority(tamper):
    module = import_module("loom.personal_dev_membership_admission")
    values = admission_values()
    if tamper == "plan":
        values["plan_sha256"] = "b" * 64
    elif tamper == "manifest":
        values["execution"]["execution_manifest_sha256"] = "b" * 64
    elif tamper == "delegate":
        values["preparation"]["personal_membership"]["management_principal_id"] = "another-delegate"
    elif tamper == "namespace":
        values["preparation"]["personal_membership"]["namespace_id"] = "00000000-0000-0000-0000-000000000123"
    elif tamper == "writer":
        values["execution"]["writer_epoch"] += 1
    elif tamper == "release":
        values["execution"]["trusted_fleet_release_sha256"] = "b" * 64
    elif tamper == "ceiling":
        values["execution"]["executable_new_capacity_ceiling"] += 1
    elif tamper == "legacy":
        values["preparation"]["schema_version"] = 2
    elif tamper == "purpose":
        values["purpose"] = "operational"
    elif tamper == "float_version":
        values["preparation"]["schema_version"] = 3.0
    elif tamper == "naive":
        values["started_at"] = _NOW.replace(tzinfo=None).isoformat()
    elif tamper == "window":
        values["expires_at"] = values["started_at"]
    wire = _wire(values)
    if tamper == "duplicate":
        wire = wire.replace(b'"purpose":"acceptance"', b'"purpose":"acceptance","purpose":"acceptance"')
    with pytest.raises(ValueError):
        module.parse_membership_acceptance_binding(wire, expected_plan_sha256="a" * 64)


@pytest.mark.parametrize("revision", (0, 5))
async def test_admission_accepts_exact_execution_while_other_owners_advance_membership(revision):
    module = import_module("loom.personal_dev_membership_admission")
    binding = module.parse_membership_acceptance_binding(_wire(admission_values()), expected_plan_sha256="a" * 64)

    class Manager:
        async def membership_checkpoint(self):
            return PersonalMembershipCheckpointV1(
                execution=binding.execution, namespace_id=binding.namespace_id,
                revision=revision, head_sha256=("0" if revision == 0 else "b") * 64,
            )

    guard = module.PersonalDevMembershipAdmissionInterlock(binding=binding, client=Manager())
    await guard.assert_admission_ready(now=_NOW)


@pytest.mark.parametrize("change", ("expired", "future", "manifest", "namespace", "writer"))
async def test_admission_rejects_expiry_or_drift_before_installation(change):
    module = import_module("loom.personal_dev_membership_admission")
    binding = module.parse_membership_acceptance_binding(_wire(admission_values()), expected_plan_sha256="a" * 64)
    reads = []

    class Manager:
        async def membership_checkpoint(self):
            reads.append(True)
            checkpoint = PersonalMembershipCheckpointV1(
                execution=binding.execution, namespace_id=binding.namespace_id,
                revision=0, head_sha256="0" * 64,
            )
            if change == "namespace":
                return checkpoint.model_copy(update={"namespace_id": binding.execution.authority_incarnation})
            update = {"execution_manifest_sha256": "f" * 64} if change == "manifest" else {"writer_epoch": 999}
            return checkpoint.model_copy(update={"execution": binding.execution.model_copy(update=update)})

    guard = module.PersonalDevMembershipAdmissionInterlock(binding=binding, client=Manager())
    now = binding.expires_at if change == "expired" else binding.started_at - timedelta(seconds=1) if change == "future" else _NOW
    with pytest.raises(module.PersonalDevMembershipAdmissionError):
        await guard.assert_admission_ready(now=now)
    assert bool(reads) == (change not in {"expired", "future"})


async def test_checkpoint_latency_cannot_carry_admission_beyond_acceptance_window(monkeypatch):
    module = import_module("loom.personal_dev_membership_admission")
    binding = module.parse_membership_acceptance_binding(_wire(admission_values()), expected_plan_sha256="a" * 64)
    ticks = iter((0.0, 3601.0))
    monkeypatch.setattr(module, "monotonic", lambda: next(ticks), raising=False)

    class Manager:
        async def membership_checkpoint(self):
            return PersonalMembershipCheckpointV1(
                execution=binding.execution, namespace_id=binding.namespace_id,
                revision=0, head_sha256="0" * 64,
            )

    guard = module.PersonalDevMembershipAdmissionInterlock(binding=binding, client=Manager())
    with pytest.raises(module.PersonalDevMembershipAdmissionError, match="window"):
        await guard.assert_admission_ready(now=_NOW)
