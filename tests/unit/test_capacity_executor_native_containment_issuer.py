"""Preparation signing fetches authority, never promotes caller observation JSON."""

import base64
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from loom_capacity_agent.admission import CurrentExecutableBootstrapV2, PhysicalJobBindingV2
from loom_capacity_executor.native_slurm_allocation import parse_native_allocation
from loom_capacity_executor.typed_launch_renderer import render_typed_signed_launch
from loom_capacity_manager.executable_contracts import ExecutableLaunchPermitV2, canonical_executable_digest
from loom_capacity_manager.launch_subject_contracts import CurrentApplicationAllocationV3, ExecutableLaunchSubjectV3
from loom_capacity_manager.typed_ownership_contracts import canonical_typed_ownership_bytes
from tests.unit.test_capacity_executor_native_containment_authority import canonical
from tests.unit.test_capacity_executor_native_launch_profile import _native_typed_context
from tests.unit.test_native_slurm_allocation import _document


@pytest.fixture
def source():
    now = datetime.now(UTC)
    context = replace(_native_typed_context(), submitted_at=now - timedelta(seconds=60))
    rendered = render_typed_signed_launch(context)
    binding, profile = context.binding, context.profiles[0]
    execution = binding.execution
    physical = PhysicalJobBindingV2(operation_id=UUID(int=811), binding=binding,
        bootstrap_registration_epoch=1, slurm_job_id="101",
        ownership_evidence_sha256=hashlib.sha256(canonical_typed_ownership_bytes(rendered.ownership_proof)).hexdigest())
    subject = ExecutableLaunchSubjectV3(binding=binding, configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement, authority=context.subject.authority)
    manager = CurrentApplicationAllocationV3(subject=subject,
        permit=ExecutableLaunchPermitV2(binding=binding, permit_id=UUID(int=812), permit_epoch=1,
            launch_rank=1, expires_at=now - timedelta(seconds=20),
            bootstrap_registration_epoch=1, bootstrap_evidence_sha256="8" * 64),
        permit_consumed_at=now - timedelta(seconds=40), observed_at=now,
        expires_at=now + timedelta(seconds=9), observed_slurm_job_id="101")
    bootstrap = CurrentExecutableBootstrapV2(physical_binding=physical, agent_incarnation=UUID(int=813),
        bootstrap_sha256="7" * 64, observed_at=now, bootstrap_expires_at=now + timedelta(seconds=8),
        request_digest=canonical_executable_digest(physical))
    raw = _document(rendered.request, 1000)
    job = raw["jobs"][0]
    job["submit_time"]["number"] = int(now.timestamp()) - 30
    job["start_time"]["number"] = int(now.timestamp()) - 15
    job["tres_alloc_str"] = f"cpu={profile.cpus},mem={profile.resources.memory_bytes // 1048576}M,node=1,billing={profile.cpus}"
    policy = {
        "schema": "loom.native-worker-cgroup-policy/v1", "environment": profile.native_execution.environment,
        "pool_id": binding.pool_id, "pool_generation": binding.pool_generation,
        "node": binding.node_ids[0], "cluster": profile.slurm_cluster, "submitter": profile.submitter,
        "uid": 1000, "account": profile.association, "partition": profile.partition, "qos": profile.qos,
        "authority_incarnation": str(execution.authority_incarnation), "execution_epoch": execution.execution_epoch,
        "execution_manifest_sha256": execution.execution_manifest_sha256,
        "executor_id": binding.executor_id, "executor_incarnation": str(binding.executor_incarnation),
        "controller_authority_sha256": profile.controller_authority_sha256,
        "trusted_fleet_release_sha256": execution.trusted_fleet_release_sha256,
        "issuer_key_id": context.ownership_key.signing_key_id,
        "issuer_public_key_hex": context.ownership_key.private_key.public_key().public_bytes_raw().hex(),
        "not_before_ms": int(now.timestamp() * 1000) - 60_000,
        "expires_at_ms": int(now.timestamp() * 1000) + 60_000,
        "profiles": [{"profile_digest": profile.profile_digest, "pids_max": 4096}],
    }
    return SimpleNamespace(context=context, physical=physical, rendered=rendered, manager=manager,
        bootstrap=bootstrap, raw=json.dumps(raw), observed=now, policy=policy, calls=[])


def issuer(source):
    module = import_module("loom_capacity_executor.native_containment_issuer")

    class Manager:
        async def current_application_allocation(self, binding):
            source.calls.append("manager")
            assert binding == source.physical.binding
            return source.manager

    class Admission:
        async def observe_current_bootstrap(self, physical):
            source.calls.append("bootstrap")
            assert physical == source.physical
            return source.bootstrap

    class Slurm:
        async def observe_native_allocation(self, request, *, job_id):
            source.calls.append("scheduler")
            assert request.ownership_token == source.rendered.request.ownership_token
            return parse_native_allocation(source.raw, request=request, job_id=job_id,
                expected_uid=1000, observed_at=source.observed)

    return module.NativePreparationIssuer(root_policy=canonical(source.policy),
        ownership_key=source.context.ownership_key, profiles=source.context.profiles,
        launch_policy=source.context.policy, manager=Manager(), admission=Admission(), slurm=Slurm())


async def issue(source):
    return await issuer(source).issue(physical_binding=source.physical,
        ownership_proof=source.rendered.ownership_proof, grant_id=UUID(int=814), grant_generation=1)


async def test_issuer_fetches_current_authority_and_packet_passes_root_verifier(source):
    packet = await issue(source)
    assert source.calls == ["manager", "bootstrap", "scheduler"]
    module = import_module("loom_capacity_executor.native_containment_protocol")
    payload = module.verify_native_preparation(signed_packet=packet, root_policy=canonical(source.policy),
        scheduler_raw=source.raw, scheduler_observed_at=source.observed,
        openssl_path="/usr/bin/openssl", openssl_sha256=hashlib.sha256(Path("/usr/bin/openssl").read_bytes()).hexdigest())
    assert payload["physical_binding_sha256"] == canonical_executable_digest(source.physical)
    assert payload["expires_at_ms"] <= int(source.bootstrap.bootstrap_expires_at.timestamp() * 1000)
    assert payload["grant_id"] == str(UUID(int=814))
    assert payload["grant_generation"] == 1
    assert base64.urlsafe_b64encode(bytes.fromhex(payload["ownership_evidence_sha256"])).rstrip(b"=").decode() == payload["ownership_token"]


@pytest.mark.parametrize("changed", (
    "manager-job", "manager-binding", "manager-expired", "manager-future", "permit-bootstrap",
    "bootstrap-physical", "bootstrap-expired", "bootstrap-old", "bootstrap-future",
    "ownership", "policy-executor", "policy-environment", "policy-release", "policy-profile",
    "scheduler-old", "scheduler-job", "scheduler-resource", "scheduler-requeue",
))
async def test_issuer_refuses_stale_or_mismatched_independent_observations(source, changed):
    if changed == "manager-job":
        source.manager = source.manager.model_copy(update={"observed_slurm_job_id": "102"})
    elif changed == "manager-binding":
        source.manager = source.manager.model_copy(update={"subject": source.manager.subject.model_copy(update={
            "binding": source.physical.binding.model_copy(update={"intent_id": UUID(int=999)})})})
    elif changed in {"manager-expired", "manager-future"}:
        shift = timedelta(seconds=-60 if changed == "manager-expired" else 60)
        source.manager = source.manager.model_copy(update={"observed_at": source.manager.observed_at + shift,
            "expires_at": source.manager.expires_at + shift})
    elif changed == "permit-bootstrap":
        source.manager = source.manager.model_copy(update={"permit": source.manager.permit.model_copy(update={"bootstrap_registration_epoch": 2})})
    elif changed == "bootstrap-physical":
        source.bootstrap = source.bootstrap.model_copy(update={"physical_binding": source.physical.model_copy(update={"slurm_job_id": "102"})})
    elif changed == "bootstrap-expired":
        source.bootstrap = source.bootstrap.model_copy(update={"bootstrap_expires_at": source.observed - timedelta(seconds=1)})
    elif changed in {"bootstrap-old", "bootstrap-future"}:
        source.bootstrap = source.bootstrap.model_copy(update={"observed_at": source.observed + timedelta(seconds=-60 if changed == "bootstrap-old" else 60)})
    elif changed == "ownership":
        source.physical = source.physical.model_copy(update={"ownership_evidence_sha256": "f" * 64})
    elif changed.startswith("policy-"):
        field = {"policy-executor": "executor_id", "policy-environment": "environment", "policy-release": "trusted_fleet_release_sha256"}.get(changed)
        if field is None:
            source.policy["profiles"][0]["profile_digest"] = "f" * 64
        else:
            source.policy[field] = "f" * 64 if changed == "policy-release" else "foreign"
    elif changed == "scheduler-old":
        source.observed -= timedelta(seconds=60)
    else:
        raw = json.loads(source.raw)
        job = raw["jobs"][0]
        if changed == "scheduler-job":
            job["job_id"] = 102
        elif changed == "scheduler-resource":
            job["cpus"]["number"] += 1
        else:
            job["requeue"] = True
        source.raw = json.dumps(raw)
    with pytest.raises(ValueError):
        await issue(source)


async def test_issuer_does_not_refresh_expired_authority_after_slow_scheduler(source, monkeypatch):
    original = issuer(source)
    module = import_module("loom_capacity_executor.native_containment_issuer")
    later = source.bootstrap.bootstrap_expires_at + timedelta(seconds=1)
    monkeypatch.setattr(module.time, "time_ns", lambda: int(later.timestamp() * 1_000_000_000))
    with pytest.raises(ValueError):
        await original.issue(physical_binding=source.physical, ownership_proof=source.rendered.ownership_proof,
            grant_id=UUID(int=814), grant_generation=1)
