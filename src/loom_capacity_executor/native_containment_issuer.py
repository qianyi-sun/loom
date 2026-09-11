"""Controller-only preparation issuer over independently fetched current facts.

Construct only in the protected executor with its pinned manager, application
admission and scheduler adapters and independently installed policy/key/profile
bundle. This is not an HTTP signing API. Callers supply references, not unsigned
authority observations. Installation, durable grant allocation/replay, transport,
root cgroup preparation and trial start are separate lifecycle responsibilities.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from datetime import UTC, datetime
from typing import Protocol, cast
from uuid import UUID

from loom_capacity_agent.admission import CurrentExecutableBootstrapV2, PhysicalJobBindingV2
from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    full_launch_profile_digest,
    resolve_typed_runtime_profile,
    validate_typed_runtime_profiles,
)
from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    _render_slurm_request,
    assert_native_root_at_submission,
)
from loom_capacity_executor.native_containment_protocol import (
    _MAX_SIGNATURE_INPUT_BYTES,
    _POLICY_FIELDS,
    _PREPARATION_DOMAIN,
    _canonical_native,
    _closed_native_document,
    _native_millis,
    _native_quantity,
    _native_uuid,
    _validate_native_policy,
    _validate_native_preparation,
)
from loom_capacity_executor.native_slurm_allocation import NativeSlurmAllocationV1
from loom_capacity_executor.slurm_contracts import SlurmLaunchRequestV2
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_digest,
)
from loom_capacity_manager.launch_subject_contracts import (
    CurrentApplicationAllocationV3,
    canonical_current_application_allocation_bytes,
    parse_current_application_allocation,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.typed_ownership_contracts import (
    SignedExecutableOwnershipProofV3,
    canonical_typed_ownership_bytes,
)


class _Manager(Protocol):
    async def current_application_allocation(self, binding: ExecutableIntentBindingV2) -> CurrentApplicationAllocationV3: ...


class _Admission(Protocol):
    async def observe_current_bootstrap(self, request: PhysicalJobBindingV2) -> CurrentExecutableBootstrapV2: ...


class _Scheduler(Protocol):
    async def observe_native_allocation(self, request: SlurmLaunchRequestV2, *, job_id: str) -> NativeSlurmAllocationV1: ...


class NativePreparationIssuer:
    """Fixed-purpose signing after bounded current protected reads, without locks."""

    def __init__(self, *, root_policy: bytes, ownership_key: ExecutorOwnershipKey,
        profiles: tuple[OperatorLaunchProfileV2, ...], launch_policy: PoolLaunchPolicyV3,
        manager: _Manager, admission: _Admission, slurm: _Scheduler) -> None:
        policy = _closed_native_document(root_policy, _POLICY_FIELDS)
        _validate_native_policy(policy)
        if (policy["issuer_key_id"] != ownership_key.signing_key_id
            or policy["issuer_public_key_hex"] != ownership_key.private_key.public_key().public_bytes_raw().hex()):
            raise ValueError("native preparation issuer key differs from root policy")
        validate_typed_runtime_profiles(profiles, policy=launch_policy,
            controller_authority_sha256=cast(str, policy["controller_authority_sha256"]))
        self._root_policy = root_policy
        self._key = ownership_key
        self._profiles = profiles
        self._launch_policy = launch_policy
        self._manager, self._admission, self._slurm = manager, admission, slurm

    async def issue(self, *, physical_binding: PhysicalJobBindingV2,
        ownership_proof: SignedExecutableOwnershipProofV3, grant_id: UUID,
        grant_generation: int) -> bytes:
        """Issue preparation only; caller must durably allocate the grant identity.

        Exact retry semantics belong to that durable caller and the root guard;
        calling this method again never itself proves replay admission or cleanup.
        No private key or unsigned observation is exposed to the worker.
        """
        async with asyncio.timeout(10):
            return await self._issue(physical_binding=physical_binding, ownership_proof=ownership_proof,
                grant_id=grant_id, grant_generation=grant_generation)

    async def _issue(self, *, physical_binding: PhysicalJobBindingV2,
        ownership_proof: SignedExecutableOwnershipProofV3, grant_id: UUID,
        grant_generation: int) -> bytes:
        initial_ms = time.time_ns() // 1_000_000
        _native_uuid(str(grant_id))
        _native_quantity(grant_generation)
        physical = PhysicalJobBindingV2.model_validate_json(physical_binding.model_dump_json())
        proof = SignedExecutableOwnershipProofV3.model_validate_json(ownership_proof.model_dump_json())
        binding = physical.binding
        policy = _closed_native_document(self._root_policy, _POLICY_FIELDS)
        caps = _validate_native_policy(policy)
        proof_digest = hashlib.sha256(canonical_typed_ownership_bytes(proof)).hexdigest()
        keyring = OwnershipKeyring({self._key.signing_key_id: self._key.private_key.public_key()})
        if (proof.metadata.binding != binding or proof_digest != physical.ownership_evidence_sha256
            or not keyring.verify_typed_executable(proof, expected_public_key_sha256=self._key.public_key_sha256)):
            raise ValueError("native preparation ownership reference is not authenticated")
        execution = binding.execution
        expected_policy = {
            "pool_id": binding.pool_id, "pool_generation": binding.pool_generation,
            "executor_id": binding.executor_id, "executor_incarnation": str(binding.executor_incarnation),
            "authority_incarnation": str(execution.authority_incarnation), "execution_epoch": execution.execution_epoch,
            "execution_manifest_sha256": execution.execution_manifest_sha256,
            "trusted_fleet_release_sha256": execution.trusted_fleet_release_sha256,
        }
        if (execution.execution_state != "active" or len(binding.node_ids) != 1
            or binding.node_ids[0] != policy["node"]
            or any(policy[name] != value for name, value in expected_policy.items())
            or not cast(int, policy["not_before_ms"]) <= initial_ms < cast(int, policy["expires_at_ms"])):
            raise ValueError("native preparation execution differs from current root policy")
        manager = await self._manager.current_application_allocation(binding)
        manager_raw = canonical_current_application_allocation_bytes(manager)
        manager = parse_current_application_allocation(manager_raw)
        if (manager.subject.binding != binding or manager.subject.authority != proof.metadata.subject_authority
            or manager.observed_slurm_job_id not in {None, physical.slurm_job_id}
            or manager.permit.bootstrap_registration_epoch != physical.bootstrap_registration_epoch):
            raise ValueError("native preparation manager identity differs")
        profile = resolve_typed_runtime_profile(binding, self._profiles, policy=self._launch_policy,
            purpose="application-worker", controller_authority_sha256=cast(str, policy["controller_authority_sha256"]))
        if (profile.native_execution is None or profile.native_execution.environment != policy["environment"]
            or profile.profile_digest not in caps or binding.resources.gpu_count or binding.resources.generic
            or proof.metadata.launch_profile_sha256 != full_launch_profile_digest(profile)
            or proof.metadata.controller_authority_sha256 != policy["controller_authority_sha256"]
            or proof.metadata.trusted_launcher_sha256 != policy["trusted_fleet_release_sha256"]
            or proof.metadata.slurm_cluster != policy["cluster"]
            or proof.metadata.submitter_identity != policy["submitter"]
            or proof.metadata.association != policy["account"]):
            raise ValueError("native preparation native profile or ownership scope differs")
        domain = next(domain for domain in profile.resource_domains if set(binding.node_ids) <= set(domain.node_ids))
        token = base64.urlsafe_b64encode(bytes.fromhex(proof_digest)).rstrip(b"=").decode("ascii")
        request = _render_slurm_request(binding=binding, profile=profile, domain=domain,
            ownership_token=token, candidate_diagnostic="", display_diagnostic="")
        if any(policy[name] != value for name, value in {
            "cluster": request.cluster, "submitter": request.submitter, "account": request.account,
            "partition": request.partition, "qos": request.qos,
        }.items()):
            raise ValueError("native preparation scheduler scope differs from policy")
        bootstrap = await self._admission.observe_current_bootstrap(physical)
        bootstrap = CurrentExecutableBootstrapV2.model_validate_json(bootstrap.model_dump_json())
        if bootstrap.physical_binding != physical:
            raise ValueError("native preparation current bootstrap identity differs")
        scheduler = await self._slurm.observe_native_allocation(request, job_id=physical.slurm_job_id)
        scheduler = NativeSlurmAllocationV1.model_validate_json(scheduler.model_dump_json())
        expected_scheduler = {
            "job_id": physical.slurm_job_id, "hostname": policy["node"], "uid": policy["uid"],
            "cluster": request.cluster, "submitter": request.submitter, "account": request.account,
            "partition": request.partition, "qos": request.qos, "cpus": request.cpus,
            "memory_bytes": request.memory_bytes, "ownership_token": token,
        }
        if any(getattr(scheduler, name) != value for name, value in expected_scheduler.items()):
            raise ValueError("native preparation scheduler observation differs")
        issued = time.time_ns() // 1_000_000
        observations = tuple(_native_millis(value) for value in (manager.observed_at, bootstrap.observed_at, scheduler.observed_at))
        expires = min(
            *(observed + 10_000 for observed in observations), _native_millis(manager.expires_at),
            _native_millis(bootstrap.bootstrap_expires_at), cast(int, policy["expires_at_ms"]),
            _native_millis(profile.native_execution.trust_root().expires_at),
        )
        if not initial_ms <= issued < expires or any(observed > issued for observed in observations):
            raise ValueError("native preparation observations are expired or clock changed")
        assert_native_root_at_submission(profile, datetime.fromtimestamp(issued / 1000, UTC))
        payload = {
            "schema": "loom.native-worker-cgroup-preparation/v1", "purpose": "prepare-application-worker-cgroup",
            "policy_sha256": hashlib.sha256(self._root_policy).hexdigest(), "grant_id": str(grant_id),
            "grant_generation": grant_generation, "intent_id": str(binding.intent_id),
            "binding_sha256": canonical_executable_digest(binding), "physical_binding_sha256": canonical_executable_digest(physical),
            "bootstrap_registration_epoch": physical.bootstrap_registration_epoch,
            "bootstrap_sha256": bootstrap.bootstrap_sha256, "agent_incarnation": str(bootstrap.agent_incarnation),
            "ownership_evidence_sha256": proof_digest, "manager_observation_sha256": hashlib.sha256(manager_raw).hexdigest(),
            "bootstrap_observation_sha256": canonical_executable_digest(bootstrap),
            "scheduler_observation_sha256": hashlib.sha256(_canonical_native(scheduler.model_dump(mode="json"))).hexdigest(),
            "profile_digest": profile.profile_digest, "pids_max": caps[profile.profile_digest],
            "job_id": physical.slurm_job_id, "ownership_token": token, "cpus": request.cpus,
            "memory_bytes": request.memory_bytes, "submitted_at_ms": _native_millis(scheduler.submitted_at),
            "started_at_ms": _native_millis(scheduler.started_at), "issued_at_ms": issued, "expires_at_ms": expires,
        }
        _validate_native_preparation(payload, policy, caps, hashlib.sha256(self._root_policy).hexdigest())
        envelope = {"schema": "loom.native-worker-cgroup-envelope/v1", "key_id": self._key.signing_key_id, "payload": payload}
        signature = self._key.private_key.sign(_PREPARATION_DOMAIN + _canonical_native(envelope))
        packet = _canonical_native(envelope | {"signature_hex": signature.hex()})
        if not issued <= time.time_ns() // 1_000_000 < expires or len(packet) > _MAX_SIGNATURE_INPUT_BYTES:
            raise ValueError("native preparation expired during signing or exceeds its bound")
        return packet
