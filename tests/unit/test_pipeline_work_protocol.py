from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.state import RetryClass, StageResultV1
from loom.pipeline.work_protocol import (
    AcceptancePreflightGrantV1,
    ExecutionAttemptClaimV1,
    ExecutionCancelAckV1,
    ExecutionCompleteV1,
    ExecutionControlResponseV1,
    ExecutionEventsV1,
    ExecutionFailedV1,
    ExecutionHeartbeatV1,
    ExecutionStartedV1,
    PipelineInputMaterializationEvidenceReportV1,
    Stage1SmokeGrantV1,
    StageRequestGrantV1,
    TerminalGenAuthoringGrantV1,
    TerminalTaskValidationGrantV1,
    WorkClaimRequestV1,
    WorkClaimV1,
    WorkerCleanupProofV1,
    WorkerLostCleanupAckV1,
)

D0 = "sha256:" + "0" * 64
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "4" * 64
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
STAGE_ID = UUID("20000000-0000-0000-0000-000000000002")
ATTEMPT_ID = UUID("30000000-0000-0000-0000-000000000003")
TEAM_ID = UUID("40000000-0000-0000-0000-000000000004")
WORKER_ID = UUID("50000000-0000-0000-0000-000000000005")
CLAIM_ID = UUID("60000000-0000-0000-0000-000000000006")
SESSION_ID = UUID("70000000-0000-0000-0000-000000000007")
NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


def resource_profile() -> dict[str, Any]:
    return {
        "name": "cpu_small",
        "version": 1,
        "execution_variants": [
            {
                "variant_id": "cpu-data-x86_64",
                "cpu_arch": "x86_64",
                "gpu_count_exact": 0,
                "gpu_vendor": None,
                "allowed_gpu_models": [],
                "gpu_memory_kind": None,
                "gpu_memory_mb_min": None,
                "gpu_unified_memory_mb_min": None,
                "memory_accounting_kind": "separate",
                "container_memory_bytes_override": None,
                "same_gpu_model_required": False,
                "pool_class": "behavior-cpu-data",
                "device_roles": None,
            }
        ],
        "cpu_cores": 2,
        "memory_bytes": 1_073_741_824,
        "scratch_bytes": 1_073_741_824,
        "timeout_seconds_max": 120,
        "required_host_runtime_features": [],
        "required_image_features": ["behavior-cpu-data"],
        "network_profile": "none",
        "input_cache_capacity_bytes_min": 0,
    }


def image_contract() -> dict[str, Any]:
    return {
        "image_index_digest": IMAGE,
        "platform": "linux/amd64",
        "platform_manifest_digest": D3,
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "cuda_userspace_version": None,
        "min_nvidia_driver_version": None,
        "application_features": ["behavior-cpu-data"],
        "provider_assets": [],
        "preflight_argv": ["/opt/loom/preflight"],
        "preflight_digest": D0,
        "sbom_digest": D1,
        "attestation_digest": D2,
    }


def binding() -> dict[str, Any]:
    return {
        "binding_name": "dataset",
        "artifact_type": "behavior.dataset.v1",
        "cardinality": "one",
        "items": [
            {
                "artifact_id": UUID("80000000-0000-0000-0000-000000000008"),
                "content_sha256": D0,
                "file_count": 1,
                "item_key": "singleton",
                "manifest_sha256": D1,
                "stored_size_bytes": 10,
                "unpacked_size_bytes": 10,
            }
        ],
    }


def worker_capability() -> dict[str, Any]:
    return {
        "schema_version": "loom.worker-capabilities.v1",
        "cpu_arch": "x86_64",
        "cpu_cores": 2,
        "memory_bytes": 2_147_483_648,
        "scratch_bytes": 2_147_483_648,
        "network_profiles": ["none"],
        "container_runtime_features": [],
        "gpu_devices": [],
        "input_cache_capacity_bytes": 0,
        "input_cache_reserved_bytes": 0,
        "input_cache_ready_bytes": 0,
    }


def execution_spec() -> dict[str, Any]:
    profile_digest = canonical_digest(resource_profile())
    runtime_digest = canonical_digest(image_contract())
    bindings_digest = canonical_digest([binding()])
    return {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": D0,
        "run_graph_digest": D1,
        "node_key": "prepare",
        "shard_key": "singleton",
        "container_node": {
            "node_kind": "container",
            "node_key": "prepare",
            "image": IMAGE,
            "argv": ["python", "-m", "pipeline.prepare"],
            "workdir": "/workspace",
            "resource_profile": "cpu_small@1",
            "network_profile": "none",
            "needs": [],
            "inputs": [],
            "outputs": [
                {
                    "name": "result",
                    "artifact_type": "behavior.result.v1",
                    "required": True,
                    "role": "artifact",
                    "producer": "container",
                    "max_bytes": 4096,
                }
            ],
            "request_renderer": None,
            "checkpoint": None,
            "fanout": None,
            "fanout_commit": None,
            "timeout_seconds": 120,
            "max_attempts": 2,
            "failure_policy": "fail_run",
        },
        "image_runtime_contract_digest": runtime_digest,
        "resource_profile_digest": profile_digest,
        "execution_variant_id": "cpu-data-x86_64",
        "gpu_backend_selection_sha256": None,
        "resolved_image_manifest_digest": D3,
        "network_profile": "none",
        "resolved_input_bindings_digest": bindings_digest,
        "fanout_source_manifest_digest": None,
        "fanout_item_digest": None,
        "fanout_parameters_digest": None,
        "request_renderer_lock_digest": None,
        "control_binding_snapshots": [],
    }


def attempt_claim() -> dict[str, Any]:
    spec = execution_spec()
    return {
        "execution_attempt_id": ATTEMPT_ID,
        "pipeline_run_id": RUN_ID,
        "stage_run_id": STAGE_ID,
        "team_id": TEAM_ID,
        "node_key": "prepare",
        "shard_key": "singleton",
        "attempt_number": 1,
        "claim_id": CLAIM_ID,
        "lease_epoch": 1,
        "lease_token": "x" * 48,
        "lease_expires_at": NOW + timedelta(seconds=60),
        "recipe_digest": D0,
        "run_graph_digest": D1,
        "execution_spec_snapshot": spec,
        "execution_spec_digest": canonical_digest(spec),
        "image": IMAGE,
        "argv": ["python", "-m", "pipeline.prepare"],
        "workdir": "/workspace",
        "resource_profile_snapshot": resource_profile(),
        "resource_profile_digest": spec["resource_profile_digest"],
        "network_profile": "none",
        "image_runtime_contract_snapshot": image_contract(),
        "image_runtime_contract_digest": spec["image_runtime_contract_digest"],
        "worker_capability_snapshot": worker_capability(),
        "worker_capability_snapshot_digest": canonical_digest(worker_capability()),
        "slurm_gpu_allocation_evidence": None,
        "slurm_gpu_allocation_evidence_digest": None,
        "input_bindings": [binding()],
        "outputs": spec["container_node"]["outputs"],
        "checkpoint": None,
        "fanout_commit": None,
        "stage_request": None,
        "acceptance_preflight": None,
        "provider_connection_ref": None,
        "secret_refs": [],
        "resume_checkpoint": None,
        "timeout_seconds": 120,
        "cancellation_poll_seconds": 5,
        "cancellation_grace_seconds": 30,
    }


def terminalgen_validation_claim() -> dict[str, Any]:
    value = attempt_claim()
    task_binding = binding()
    task_binding.update(
        binding_name="task_bundle",
        artifact_type="terminalgen_task_bundle.v1",
    )
    profile = resource_profile()
    profile.update(
        name="terminalgen-validate-none",
        cpu_cores=4,
        memory_bytes=16 << 30,
        scratch_bytes=320 << 30,
        pids_limit=2_048,
        timeout_seconds_max=7_200,
        required_host_runtime_features=[
            "loom-terminal-task-validator-v1",
            "loom-terminalgen-authoring-worker-v1",
        ],
        required_image_features=["terminalgen-validator"],
        input_cache_capacity_bytes_min=0,
    )
    profile["execution_variants"][0].update(
        variant_id="terminalgen-cpu-x86_64",
        pool_class="terminalgen-validate-none",
    )
    runtime = image_contract()
    runtime["application_features"] = ["terminalgen-validator"]
    capability = worker_capability()
    capability.update(
        cpu_cores=4,
        memory_bytes=16 << 30,
        scratch_bytes=320 << 30,
        container_runtime_features=[
            "loom-terminal-task-validator-v1",
            "loom-terminalgen-authoring-worker-v1",
        ],
    )
    spec = execution_spec()
    spec.update(
        node_key="validate_card_00",
        image_runtime_contract_digest=canonical_digest(runtime),
        resource_profile_digest=canonical_digest(profile),
        execution_variant_id="terminalgen-cpu-x86_64",
        resolved_input_bindings_digest=canonical_digest([task_binding]),
    )
    spec["container_node"].update(
        node_key="validate_card_00",
        image=IMAGE,
        argv=["python", "-m", "loom.integrations.terminalgen.cli", "run", "validate_card_00"],
        resource_profile="terminalgen-validate-none@1",
        timeout_seconds=7_200,
    )
    value.update(
        node_key="validate_card_00",
        execution_spec_snapshot=spec,
        execution_spec_digest=canonical_digest(spec),
        argv=spec["container_node"]["argv"],
        resource_profile_snapshot=profile,
        resource_profile_digest=spec["resource_profile_digest"],
        image_runtime_contract_snapshot=runtime,
        image_runtime_contract_digest=spec["image_runtime_contract_digest"],
        worker_capability_snapshot=capability,
        worker_capability_snapshot_digest=canonical_digest(capability),
        input_bindings=[task_binding],
        timeout_seconds=7_200,
    )
    value["terminalgen_authoring"] = {
        "authorization_id": UUID(int=40),
        "pipeline_run_id": RUN_ID,
        "stage_run_id": STAGE_ID,
        "execution_attempt_id": ATTEMPT_ID,
        "recipe_digest": D0,
        "node_key": "validate_card_00",
        "resource_profile_digest": spec["resource_profile_digest"],
        "image_runtime_contract_digest": spec["image_runtime_contract_digest"],
        "resolved_input_bindings_digest": spec["resolved_input_bindings_digest"],
        "runtime_policy_digest": D2,
        "validation": {
            "authorization_id": UUID(int=41),
            "pipeline_run_id": RUN_ID,
            "stage_run_id": STAGE_ID,
            "execution_attempt_id": ATTEMPT_ID,
            "task_bundle_content_sha256": D0,
            "validator_image": IMAGE,
            "task_base_image": "registry.example.com/loom/task-base@sha256:" + "5" * 64,
            "dependency_resolver_image": (
                "registry.example.com/loom/dependency-resolver@sha256:" + "6" * 64
            ),
            "policy_digest": D3,
            "dependency_allowlist_digest": D1,
            "repeat_count": 2,
            "cpu_cores": 4,
            "memory_bytes": 16 << 30,
            "pids_limit": 2_048,
            "timeout_seconds": 7_200,
            "network_profile": "none",
        },
    }
    return value


def stage_result() -> StageResultV1:
    return StageResultV1.model_validate(
        {
            "schema_version": "loom.stage-result.v1",
            "domain_outcome": "complete",
            "reason_code": "completed",
            "retry_class": RetryClass.NONE,
            "inputs": [],
            "outputs": [{"name": "result", "artifact_type": "behavior.result.v1"}],
            "metrics": {},
            "provenance": {
                "pipeline_run_id": RUN_ID,
                "stage_run_id": STAGE_ID,
                "execution_attempt_id": ATTEMPT_ID,
                "recipe_digest": D0,
                "execution_spec_digest": canonical_digest(execution_spec()),
                "image_digest": D2,
            },
            "error": None,
        }
    )


def cleanup_proof() -> dict[str, Any]:
    return {
        "container_absent": True,
        "cgroup_empty": True,
        "network_absent": True,
        "step_jwt_revoked": True,
        "runtime_secret_mount_absent": True,
        "scratch_absent": True,
        "outputs_absent": True,
        "input_views_absent": True,
        "active_upload_session_ids": [],
    }


def test_work_claim_request_and_union_are_closed_and_discriminated() -> None:
    request = WorkClaimRequestV1.model_validate(
        {
            "schema_version": "loom.work-claim-request.v1",
            "worker_id": WORKER_ID,
            "capability_snapshot_digest": D0,
            "free_slots": 1,
            "supported_work_kinds": ["trial", "execution_attempt"],
        }
    )
    assert request.free_slots == 1

    claim = WorkClaimV1.model_validate(
        {
            "schema_version": "loom.work-claim.v1",
            "work_kind": "execution_attempt",
            "payload": attempt_claim(),
        }
    )
    assert isinstance(claim.payload, ExecutionAttemptClaimV1)
    transported = WorkClaimV1.model_validate_json(
        canonical_document(claim.model_dump(mode="json", exclude_none=False))
    )
    assert transported == claim

    with pytest.raises(ValidationError, match="work_kind does not match"):
        WorkClaimV1.model_validate(
            {
                "schema_version": "loom.work-claim.v1",
                "work_kind": "trial",
                "payload": attempt_claim(),
            }
        )
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        WorkClaimRequestV1.model_validate({**request.model_dump(), "unknown": True})


def test_execution_attempt_claim_cross_checks_all_frozen_duplicates() -> None:
    parsed = ExecutionAttemptClaimV1.model_validate(attempt_claim())
    assert parsed.execution_spec_snapshot.node_key == parsed.node_key

    drift = attempt_claim()
    drift["argv"] = ["sh", "-c", "unsafe"]
    with pytest.raises(ValidationError, match="claim fields drift"):
        ExecutionAttemptClaimV1.model_validate(drift)

    digest_drift = attempt_claim()
    digest_drift["input_bindings"][0]["items"][0]["stored_size_bytes"] = 11
    with pytest.raises(ValidationError, match="input bindings digest drift"):
        ExecutionAttemptClaimV1.model_validate(digest_drift)

    secret = attempt_claim()
    secret["secret_refs"] = ["plain-secret"]
    with pytest.raises(ValidationError):
        ExecutionAttemptClaimV1.model_validate(secret)


def test_stage_request_grant_requires_exact_canonical_bytes_size_and_digest() -> None:
    raw = canonical_document({"schema_version": "behavior.stage-request.v1"})
    value = {
        "renderer_name": "behavior_stage_request",
        "renderer_version": 1,
        "renderer_digest": D0,
        "canonical_jcs_lf": raw.decode(),
        "stage_request_sha256": "sha256:" + __import__("hashlib").sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }
    assert StageRequestGrantV1.model_validate(value).size_bytes == len(raw)

    noncanonical = {**value, "canonical_jcs_lf": '{"z":1, "a":2}\n'}
    noncanonical["size_bytes"] = len(noncanonical["canonical_jcs_lf"].encode())
    noncanonical["stage_request_sha256"] = (
        "sha256:"
        + __import__("hashlib").sha256(noncanonical["canonical_jcs_lf"].encode()).hexdigest()
    )
    with pytest.raises(ValidationError, match="canonical JCS"):
        StageRequestGrantV1.model_validate(noncanonical)


def test_acceptance_preflight_pins_variant_policy_cluster_node_and_cache_phase() -> None:
    value = {
        "authorization_id": UUID(int=20),
        "authorization_snapshot_sha256": D0,
        "action": "matrix",
        "candidate_sha256": D1,
        "preflight_input_set_id": "S02",
        "prerequisite_pipeline_run_id": RUN_ID,
        "exclusive_fence_id": UUID(int=21),
        "node_key": "oldlab-rtx5080-2gpu_acceptance_preflight_cold",
        "backend_variant_id": "oldlab-rtx5080-2gpu",
        "cache_expectation": "cold_after_eviction",
        "sealed_input_descriptor_set_sha256": D2,
        "policy_id": "behavior-gpu-oldlab",
        "policy_config_sha256": D3,
        "policy_activation_epoch": 3,
        "slurm_cluster_id": "oldlab",
        "slurm_cluster_config_sha256": D0,
        "slurm_allocation_id": "oldlab:123",
        "image_runtime_contract_digest": D1,
        "resource_profile_digest": D2,
        "network_profile": "none",
        "renderer_digest": D3,
    }
    assert AcceptancePreflightGrantV1.model_validate(value).policy_id == "behavior-gpu-oldlab"
    with pytest.raises(ValidationError, match="variant/policy/cluster drift"):
        AcceptancePreflightGrantV1.model_validate({**value, "slurm_cluster_id": "gb10"})
    with pytest.raises(ValidationError, match="node/cache phase drift"):
        AcceptancePreflightGrantV1.model_validate({**value, "cache_expectation": "warm_reuse_only"})


def test_stage1_smoke_grant_is_closed_and_binds_the_selected_child() -> None:
    value = {
        "authorization_id": UUID(int=30),
        "pipeline_run_id": RUN_ID,
        "candidate_sha256": D0,
        "authorization_sha256": D1,
        "preflight_sha256": D2,
        "policy_activation_epoch": 7,
        "recipe_digest": D3,
        "platform_child_digest": D0,
        "image_runtime_contract_digest": D1,
        "resolved_input_bindings_digest": D2,
        "renderer_digest": D3,
    }
    assert Stage1SmokeGrantV1.model_validate(value).policy_activation_epoch == 7
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Stage1SmokeGrantV1.model_validate({**value, "raw_image_override": IMAGE})


def test_terminalgen_authoring_and_validation_grants_are_exact_and_fail_closed() -> None:
    value = terminalgen_validation_claim()
    parsed = ExecutionAttemptClaimV1.model_validate(value)
    assert parsed.terminalgen_authoring is not None
    assert parsed.terminalgen_authoring.validation is not None
    assert parsed.terminalgen_authoring.validation.repeat_count == 2

    missing = deepcopy(value)
    missing["terminalgen_authoring"] = None
    with pytest.raises(ValidationError, match="require their authoring grant"):
        ExecutionAttemptClaimV1.model_validate(missing)

    wrong_bundle = deepcopy(value)
    wrong_bundle["terminalgen_authoring"]["validation"]["task_bundle_content_sha256"] = D3
    with pytest.raises(ValidationError, match="validation authority drift"):
        ExecutionAttemptClaimV1.model_validate(wrong_bundle)

    excessive_pids = deepcopy(value)
    excessive_pids["terminalgen_authoring"]["validation"]["pids_limit"] = 2_049
    with pytest.raises(ValidationError, match="validation authority drift"):
        ExecutionAttemptClaimV1.model_validate(excessive_pids)

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TerminalTaskValidationGrantV1.model_validate(
            {
                **value["terminalgen_authoring"]["validation"],
                "docker_socket": "/var/run/docker.sock",
            }
        )
    with pytest.raises(ValidationError, match="must match a validation node"):
        TerminalGenAuthoringGrantV1.model_validate(
            {**value["terminalgen_authoring"], "node_key": "plan_batch"}
        )


def test_heartbeat_control_and_events_enforce_order_bounds_and_closed_fields() -> None:
    heartbeat = ExecutionHeartbeatV1.model_validate(
        {
            "schema_version": "loom.execution-heartbeat.v1",
            "phase": "running",
            "monotonic_runtime_seconds": 10,
            "active_upload_session_ids": [SESSION_ID],
        }
    )
    assert heartbeat.phase == "running"

    control = ExecutionControlResponseV1.model_validate(
        {
            "commands": [
                {"seq": 1, "command": "rotate_step_jwt"},
                {"seq": 2, "command": "cancel_requested"},
            ],
            "current_seq": 2,
        }
    )
    assert control.current_seq == 2
    with pytest.raises(ValidationError, match="increasing unique"):
        ExecutionControlResponseV1.model_validate(
            {
                "commands": [
                    {"seq": 2, "command": "cancel_requested"},
                    {"seq": 1, "command": "rotate_step_jwt"},
                ],
                "current_seq": 2,
            }
        )

    events = ExecutionEventsV1.model_validate(
        {
            "events": [
                {
                    "local_seq": 0,
                    "timestamp": NOW,
                    "stream": "worker",
                    "level": "info",
                    "message": "started",
                }
            ]
        }
    )
    assert events.events[0].message == "started"
    too_large = deepcopy(events.model_dump())
    too_large["events"][0]["message"] = "x" * 65_537
    with pytest.raises(ValidationError, match="64 KiB"):
        ExecutionEventsV1.model_validate(too_large)


def test_started_complete_failed_and_cancel_reports_are_strict() -> None:
    started = ExecutionStartedV1.model_validate(
        {
            "container_id": "container-1",
            "runtime_started_at": NOW,
            "input_view_digest": D0,
            "step_jwt_id": None,
        }
    )
    assert started.step_jwt_id is None

    result = stage_result()
    complete = ExecutionCompleteV1.model_validate(
        {
            "exit_code": 0,
            "stage_result": result,
            "stage_result_sha256": canonical_digest(result),
            "final_output_upload_session_id": SESSION_ID,
        }
    )
    assert complete.exit_code == 0
    round_tripped = ExecutionCompleteV1.model_validate_json(
        json.dumps(complete.model_dump(mode="json"))
    )
    assert round_tripped.final_output_upload_session_id == SESSION_ID
    assert round_tripped.stage_result.retry_class is RetryClass.NONE
    with pytest.raises(ValidationError, match="StageResult digest drift"):
        ExecutionCompleteV1.model_validate({**complete.model_dump(), "stage_result_sha256": D0})

    failed_value = result.model_dump()
    failed_value.update(
        {
            "domain_outcome": None,
            "reason_code": "provider_timeout",
            "retry_class": RetryClass.PROVIDER_TRANSIENT,
            "error": {"code": "provider_timeout", "message": "retry"},
        }
    )
    failed_result = StageResultV1.model_validate(failed_value)
    failed = ExecutionFailedV1.model_validate(
        {
            "exit_code": 75,
            "retry_class": RetryClass.PROVIDER_TRANSIENT,
            "reason_code": "provider_timeout",
            "redacted_message": "provider request timed out",
            "stage_result": failed_result,
            "stage_result_sha256": canonical_digest(failed_result),
            "teardown_observed": True,
            "resources": {
                "container_absent": True,
                "cgroup_empty": True,
                "network_absent": True,
                "step_jwt_revoked": True,
                "runtime_secret_mount_absent": True,
                "scratch_absent": True,
                "outputs_absent": True,
                "input_views_absent": True,
                "active_upload_session_ids": [],
            },
        }
    )
    assert failed.teardown_observed is True

    cancel = ExecutionCancelAckV1.model_validate(
        {
            "outcome": "forced",
            "observed_at": NOW,
            "last_committed_checkpoint_artifact_id": None,
            "teardown_observed": True,
            "resources": failed.resources,
        }
    )
    assert cancel.outcome == "forced"


def test_input_evidence_and_cleanup_ack_enforce_exact_positive_observations() -> None:
    evidence = PipelineInputMaterializationEvidenceReportV1.model_validate(
        {
            "schema_version": "loom.pipeline-input-materialization-evidence-report.v1",
            "execution_attempt_id": ATTEMPT_ID,
            "worker_id": WORKER_ID,
            "lease_epoch": 1,
            "cache_expectation": "cold_after_eviction",
            "ordered_manifest_sha256s": [D0, D1, D2, D3, "sha256:" + "4" * 64],
            "manifest_open_count": 5,
            "file_open_count": 8,
            "file_bytes": 100,
            "archive_extraction_count": 1,
            "cas_rename_count": 5,
            "input_view_sha256": D0,
        }
    )
    assert len(evidence.ordered_manifest_sha256s) == 5

    proof = WorkerCleanupProofV1.model_validate(cleanup_proof())
    assert proof.active_upload_session_ids == []
    with pytest.raises(ValidationError):
        WorkerCleanupProofV1.model_validate(
            {**cleanup_proof(), "active_upload_session_ids": [SESSION_ID]}
        )

    journal = WorkerLostCleanupAckV1.model_validate(
        {
            "schema_version": "loom.worker-lost-cleanup-ack.v1",
            "observer_kind": "worker_journal",
            "observed_at": NOW,
            "allocation_id": None,
            "allocation_terminal": None,
            "resources": cleanup_proof(),
        }
    )
    assert journal.observer_kind == "worker_journal"
    with pytest.raises(ValidationError, match="terminal allocation identity"):
        WorkerLostCleanupAckV1.model_validate(
            {
                **journal.model_dump(),
                "observer_kind": "slurm_node_reaper",
            }
        )
