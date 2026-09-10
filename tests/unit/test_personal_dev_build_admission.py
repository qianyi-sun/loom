"""Manager placements join exact native build work without allocating again."""

from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest

from loom.db.schema import PersonalDevBuildPlatformRequest
from loom.personal_dev_build_platform_requests import _installation, _values
from loom.personal_dev_build_runtime_installation import resolve_personal_build_runtime_installation
from loom.personal_dev_candidate import CandidateRegistration
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAllowanceV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableAdmissionShapeV2,
    canonical_executable_digest,
)
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context
from tests.unit.test_personal_dev_build_runtime_installation import installation_input
from tests.unit.test_personal_dev_builder import _NOW, _attempt, _candidate


def admission_input(tmp_path, pool="gb10"):
    publication, preparation, configs = installation_input(tmp_path)
    runtime = resolve_personal_build_runtime_installation(publication,
        preparation=preparation, pool_profiles=configs)
    member = build_membership_input().membership.members[-1]
    member = member.model_copy(update={"acknowledgement": member.acknowledgement.model_copy(
        update={"candidate": publication.candidate})})
    registration = CandidateRegistration(candidate=_candidate(owner_user_id=member.owner_id),
        build_attempt=_attempt(state="running"), created=False)
    platform = "linux/arm64" if pool == "gb10" else "linux/amd64"
    request = PersonalDevBuildPlatformRequest(**_values(registration, member,
        _installation(member, runtime), platform, _NOW), created_at=_NOW)
    reference = next(item for item in member.configuration.profiles if item.pool_id == pool)
    executor = next(item for item in preparation.executors if item.pool_id == pool)
    binding = typed_context(pool=pool).binding
    fence = binding.execution.model_copy(update={
        "execution_manifest_sha256": canonical_executable_digest(preparation)})
    binding = binding.model_copy(update={"execution": fence, "candidate": publication.candidate,
        "executor_id": executor.executor_id, "executor_incarnation": executor.executor_incarnation})
    shape = ExecutableAdmissionShapeV2(binding=binding,
        protocol_generation=reference.protocol_generation, protocol_digest=reference.protocol_digest,
        worker_shape=reference.worker_shapes[0], worker_shape_digest=canonical_digest(reference.worker_shapes[0]),
        bootstrap_registration_epoch=1)
    allowance = ExecutableAdmissionAllowanceV2(allowance_id=uuid4(), protected_attempt_id=request.id,
        shape_instance_id=binding.shape_instance_id, shape_slot_index=0, submission_intent_id=binding.intent_id)
    proposal = ExecutableAdmissionPlanProposalV2(proposal_id=uuid4(), plan_id=uuid4(),
        admission_incarnation=uuid4(), reporter_incarnation=member.configuration.demand_reporter_incarnation,
        protected_admission_sha256=member.acknowledgement.protected_admission_sha256,
        manager_input_digest="2" * 64, manager_allocation_digest="3" * 64,
        lease_not_after=_NOW + timedelta(seconds=30), shapes=(shape,), allowances=(allowance,))
    return dict(member=member, runtime=runtime, execution=fence, proposal=proposal,
        requests=((request, registration),), now=_NOW)


def join(values):
    return import_module("loom.personal_dev_build_admission").bind_personal_build_admission(**values)


@pytest.mark.parametrize("pool", ("gb10", "oldlab"))
def test_both_native_proposals_bind_exact_durable_work(tmp_path, pool):
    values = admission_input(tmp_path, pool)
    result = join(values)
    assert len(result) == 1
    item = result[0]
    assert item.allowance == values["proposal"].allowances[0]
    assert item.shape == values["proposal"].shapes[0]
    assert item.request_id == values["requests"][0][0].id
    assert item.runtime_installation_sha256 == _installation(values["member"], values["runtime"])
    assert item.source_binding_sha256 == values["requests"][0][0].source_binding_sha256
    assert item.lease_not_after == values["proposal"].lease_not_after


@pytest.mark.parametrize("boundary", (
    "empty", "missing-request", "duplicate-request", "cancelled", "source", "runtime", "expired-parent",
    "expired-proposal", "fence", "disabled", "reporter", "protected-admission", "foreign-attempt",
    "account", "subject", "candidate", "generation", "pool-generation", "profile", "executor",
    "node", "protocol", "shape", "manifest", "fleet-release", "zero-capacity", "drain-only",
))
def test_admission_rejects_identity_and_authority_drift(tmp_path, boundary):
    values = admission_input(tmp_path)
    proposal = values["proposal"]
    row, registration = values["requests"][0]
    if boundary == "empty":
        proposal = proposal.model_copy(update={"allowances": ()})
    elif boundary in {"missing-request", "duplicate-request"}:
        values["requests"] = () if boundary == "missing-request" else values["requests"] * 2
    elif boundary == "cancelled":
        row.cancelled_at = _NOW
    elif boundary == "source":
        values["requests"] = ((row, replace(registration, candidate=replace(registration.candidate,
            archive_size_bytes=registration.candidate.archive_size_bytes + 1))),)
    elif boundary == "runtime":
        row.runtime_installation_sha256 = "f" * 64
    elif boundary == "expired-parent":
        values["requests"] = ((row, replace(registration, build_attempt=replace(registration.build_attempt,
            lease_expires_at=_NOW))),)
    elif boundary == "expired-proposal":
        values["now"] = proposal.lease_not_after
    elif boundary == "fence":
        values["execution"] = values["execution"].model_copy(update={"allocation_epoch": 999})
    elif boundary == "drain-only":
        fence = values["execution"].model_copy(update={"execution_state": "drain-only",
            "executable_new_capacity_ceiling": 0, "executable_new_capacity_rate_per_minute": 0})
        values["execution"] = fence
        shape = proposal.shapes[0]
        proposal = proposal.model_copy(update={"shapes": (shape.model_copy(update={
            "binding": shape.binding.model_copy(update={"execution": fence})}),)})
    elif boundary == "zero-capacity":
        values["member"] = values["member"].model_copy(update={"configuration": values["member"].configuration.model_copy(
            update={"max_slots": 0})})
    elif boundary == "disabled":
        values["member"] = values["member"].model_copy(update={"configuration": values["member"].configuration.model_copy(
            update={"lifecycle_state": "disabled", "max_slots": 0})})
    elif boundary in {"reporter", "protected-admission"}:
        proposal = proposal.model_copy(update={
            "reporter_incarnation" if boundary == "reporter" else "protected_admission_sha256":
            uuid4() if boundary == "reporter" else "f" * 64})
    elif boundary == "foreign-attempt":
        proposal = proposal.model_copy(update={"allowances": (proposal.allowances[0].model_copy(
            update={"protected_attempt_id": uuid4()}),)})
    else:
        shape = proposal.shapes[0]
        binding = shape.binding
        fields = {
            "account": {"account_id": "dev-owner-foreign"}, "subject": {"subject_id": uuid4()},
            "candidate": {"candidate": binding.candidate.model_copy(update={"identity": "f" * 40})},
            "generation": {"deployment_generation": binding.deployment_generation + 1},
            "pool-generation": {"pool_generation": binding.pool_generation + 1},
            "profile": {"profile_digest": "f" * 64}, "executor": {"executor_incarnation": uuid4()},
            "node": {"node_ids": ("trt-gb10-2",)},
            "manifest": {"execution": binding.execution.model_copy(update={"execution_manifest_sha256": "f" * 64})},
            "fleet-release": {"execution": binding.execution.model_copy(update={"trusted_fleet_release_sha256": "f" * 64})},
        }
        if boundary == "protocol":
            shape = shape.model_copy(update={"protocol_digest": "f" * 64})
        elif boundary == "shape":
            worker = shape.worker_shape.model_copy(update={"capabilities": ("application-worker", "cpu_arch.arm64")})
            shape = shape.model_copy(update={"worker_shape": worker, "worker_shape_digest": canonical_digest(worker)})
        else:
            shape = shape.model_copy(update={"binding": binding.model_copy(update=fields[boundary])})
            if boundary in {"manifest", "fleet-release"}:
                values["execution"] = shape.binding.execution
        proposal = proposal.model_copy(update={"shapes": (shape,)})
    values["proposal"] = proposal
    with pytest.raises(ValueError):
        join(values)
