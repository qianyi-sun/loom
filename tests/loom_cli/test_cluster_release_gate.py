from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import _load_root_owned_external_slurm_authority
from loom_cli.cluster_release_gate import (
    ReleaseGateCheck,
    ReleaseGateReport,
    _external_slurm_acceptance_check,
    _gb10_worker_check,
    _hf_mirror_boundary_check,
    collect_release_gate_report,
    format_release_gate_markdown,
    query_live_alembic_heads,
    release_gate_report_to_dict,
)


class _Spec:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class _FakeAppsV1:
    def __init__(self, deployments: dict[str, Any]) -> None:
        self.deployments = deployments

    def read_namespaced_deployment(self, *, name: str, namespace: str) -> Any:
        return self.deployments[name]


class _FakeCoreV1:
    def __init__(self, pods: list[Any], events: list[Any] | None = None) -> None:
        self.pods = pods
        self.events = events or []

    def list_namespaced_pod(self, *, namespace: str) -> Any:
        return _Spec(items=self.pods)

    def list_namespaced_event(self, *, namespace: str) -> Any:
        return _Spec(items=self.events)


def _deployment(
    *,
    name: str,
    image: str,
    generation: int = 7,
    observed_generation: int = 7,
    replicas: int = 1,
    workload_contract_env: dict[str, str] | None = None,
) -> Any:
    workload_contract_env = workload_contract_env or {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    return _Spec(
        metadata=_Spec(name=name, generation=generation),
        spec=_Spec(
            replicas=replicas,
            selector=_Spec(match_labels={"app": name}),
            template=_Spec(
                metadata=_Spec(labels={"app": name}),
                spec=_Spec(
                    containers=[
                        _Spec(name="app", image=image),
                        _Spec(
                            name="loom-service",
                            image=image,
                            env=[
                                _Spec(name=key, value=value)
                                for key, value in workload_contract_env.items()
                            ],
                        ),
                    ]
                ),
            ),
        ),
        status=_Spec(
            observed_generation=observed_generation,
            ready_replicas=replicas,
            updated_replicas=replicas,
        ),
    )


def _ready_pod(
    *,
    name: str,
    app: str,
    image: str,
    image_id: str | None,
    status_image: str | None = None,
) -> Any:
    container_status = _Spec(name="app", image=status_image or image)
    if image_id is not None:
        container_status.image_id = image_id
    return _Spec(
        metadata=_Spec(name=name, labels={"app": app}),
        spec=_Spec(containers=[_Spec(name="app", image=image)]),
        status=_Spec(
            conditions=[_Spec(type="Ready", status="True")],
            container_statuses=[container_status],
        ),
    )


def _pod(
    *,
    name: str,
    app: str,
    image: str,
    deletion_timestamp: str | None = None,
) -> Any:
    metadata = _Spec(name=name, labels={"app": app})
    if deletion_timestamp is not None:
        metadata.deletion_timestamp = deletion_timestamp
    return _Spec(
        metadata=metadata,
        spec=_Spec(containers=[_Spec(name="app", image=image)]),
        status=_Spec(phase="Running", container_statuses=[]),
    )


def _event(*, pod: str, reason: str, message: str) -> Any:
    return _Spec(
        involved_object=_Spec(name=pod),
        reason=reason,
        message=message,
    )


def _manifest(
    *,
    expected_digest: str = "sha256:" + "1" * 64,
    alembic_heads: list[str] | None = None,
    external_workers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = {
        "schema_version": 1,
        "release": {
            "environment": "staging",
            "git_sha": "a" * 40,
            "image_tag": "staging-abc123",
            "generated_at": "2026-07-01T00:00:00Z",
        },
        "cluster_config": {"sha256": "config-sha", "namespace": "loom"},
        "rendered_manifest": {
            "sha256": "rendered-sha",
            "deployment_images": {
                "loom-service": {"app": "loom-service:staging-abc123"},
            },
            "deployment_image_identities": {
                "loom-service": {
                    "app": {
                        "image": "loom-service:staging-abc123",
                        "repo_digest": f"loom-service@{expected_digest}",
                        "image_id": "sha256:" + "2" * 64,
                    },
                },
            },
        },
        "alembic": {
            "expected_heads": alembic_heads or ["0050"],
            "compatible_heads": alembic_heads or ["0050"],
        },
        "workload_contract": {
            "workload_trust_mode": "internal_trusted",
            "taskset_transforms_enabled": False,
            "taskset_transform_network_isolated": False,
            "untrusted_workload_isolation": False,
        },
    }
    if external_workers is not None:
        manifest["external_workers"] = external_workers
    return manifest


def _external_gb10_workers(*, enabled: bool) -> dict[str, Any]:
    workers: dict[str, Any] = {
        "slurm_pools": [
            {
                "pool_name": "gb10",
                "actuator": "slurm",
                "enabled": enabled,
                "disabled_reason": None if enabled else "#827 acceptance incomplete",
                "external_runner": True,
                "allowed_nodes": ["trt-gb10-1"],
            }
        ],
        "external_slurm_runner_prerequisites": {
            "pools": ["gb10"],
            "materialize": enabled,
            "require_external_allocation_authority": enabled,
        },
        "external_slurm_autoscaler_supervisors": [
            {"pool_name": "gb10", "enabled": enabled, "active": enabled}
        ],
    }
    if enabled:
        workers["gb10_desired_states"] = [
            {
                "pool_name": "gb10",
                "target_slots": 0,
                "host_intents": {
                    f"trt-gb10-{index}": "stopped" for index in range(1, 16)
                },
            }
        ]
    return workers


def test_release_gate_passes_for_fail_closed_external_gb10() -> None:
    check = _external_slurm_acceptance_check(
        _manifest(external_workers=_external_gb10_workers(enabled=False))
    )

    assert check is not None
    assert check.outcome == "pass"
    assert check.evidence["policy_enabled"] is False


def test_release_gate_rejects_candidate_self_attested_external_gb10_activation() -> None:
    workers = _external_gb10_workers(enabled=True)
    workers["external_slurm_runner_prerequisites"].update(
        {
            "service_identity": {
                "username": "loom-rollout",
                "uid": 995,
                "gid": 982,
                "supplementary_groups": ["docker"],
                "slurm_account": "loom-staging",
                "submit_host": "gb10-submit.example",
            },
            "allocation_attestation": {
                "candidate_sha": "a" * 40,
                "artifact_path": "/candidate-controlled/attestation.json",
                "artifact_sha256": "b" * 64,
                "passed": True,
                "nodes": ["trt-gb10-1"],
            },
        }
    )
    check = _external_slurm_acceptance_check(_manifest(external_workers=workers))

    assert check is not None
    assert check.outcome == "fail"
    assert "candidate_external_slurm_self_attestation_forbidden" in check.evidence["blockers"]
    assert "external_slurm_acceptance_authority_unavailable" in check.evidence["blockers"]


def test_release_gate_rejects_gb10_supervisor_activation_without_policy() -> None:
    workers = _external_gb10_workers(enabled=False)
    workers["slurm_pools"] = []
    workers["external_slurm_autoscaler_supervisors"][0].update({"enabled": True, "active": True})

    check = _external_slurm_acceptance_check(_manifest(external_workers=workers))

    assert check is not None
    assert check.outcome == "fail"
    assert "external_slurm_acceptance_authority_unavailable" in check.evidence["blockers"]
    assert "gb10_node_agent_authority_not_retired" in check.evidence["blockers"]


def test_release_gate_accepts_exact_candidate_fifteen_node_authority() -> None:
    manifest = _manifest(external_workers=_external_gb10_workers(enabled=True))
    manifest["external_workers"]["environment_state_file"] = {"sha256": "b" * 64}
    generated_at = datetime.now(UTC)

    check = _external_slurm_acceptance_check(
        manifest,
        authority_artifact={
            "schema_version": 1,
            "kind": "loom_gb10_slurm_acceptance",
            "result": "pass",
            "candidate_sha": manifest["release"]["git_sha"],
            "candidate_tree": "c" * 40,
            "profile_sha256": "b" * 64,
            "cluster_name": "trt-gb10",
            "controller_host": "gx10-01c7",
            "service_identity": {
                "user": "loom-rollout",
                "uid": 995,
                "gid": 2007,
                "account": "loom-staging",
                "qos": "loom-staging",
            },
            "nodes": [f"trt-gb10-{index}" for index in range(1, 16)],
            "node_count": 15,
            "generated_at": generated_at.isoformat(),
            "expires_at": (generated_at + timedelta(minutes=15)).isoformat(),
        },
    )

    assert check is not None
    assert check.outcome == "pass"
    assert check.evidence["authority_verified"] is True


def test_release_gate_rejects_authority_with_forged_node_inventory() -> None:
    manifest = _manifest(external_workers=_external_gb10_workers(enabled=True))
    manifest["external_workers"]["environment_state_file"] = {"sha256": "b" * 64}
    generated_at = datetime.now(UTC)
    artifact = {
        "schema_version": 1,
        "kind": "loom_gb10_slurm_acceptance",
        "result": "pass",
        "candidate_sha": manifest["release"]["git_sha"],
        "candidate_tree": "c" * 40,
        "profile_sha256": "b" * 64,
        "cluster_name": "trt-gb10",
        "controller_host": "gx10-01c7",
        "service_identity": {
            "user": "loom-rollout",
            "uid": 995,
            "gid": 2007,
            "account": "loom-staging",
            "qos": "loom-staging",
        },
        "nodes": [f"trt-gb10-{index}" for index in range(1, 15)] + ["trt-gb10-16"],
        "node_count": 15,
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=15)).isoformat(),
    }

    check = _external_slurm_acceptance_check(manifest, authority_artifact=artifact)

    assert check is not None
    assert check.outcome == "fail"
    assert "external_slurm_acceptance_authority_mismatch" in check.evidence["blockers"]


@pytest.mark.parametrize(
    "workload_contract",
    [
        None,
        {"workload_trust_mode": "unknown"},
        {
            "workload_trust_mode": "internal_trusted",
            "taskset_transforms_enabled": True,
            "taskset_transform_network_isolated": False,
            "untrusted_workload_isolation": False,
        },
    ],
)
def test_release_gate_rejects_absent_or_invalid_manifest_workload_contract(
    workload_contract: dict[str, Any] | None,
) -> None:
    manifest = _manifest()
    if workload_contract is None:
        manifest.pop("workload_contract")
    else:
        manifest["workload_contract"] = workload_contract
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert not report.all_pass


def test_release_gate_invalid_workload_contract_does_not_echo_raw_candidate_value() -> None:
    raw_mode = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    manifest = _manifest()
    manifest["workload_contract"]["workload_trust_mode"] = raw_mode
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_mode not in json.dumps(check.evidence, sort_keys=True)


def test_release_gate_does_not_echo_unknown_workload_contract_field_name() -> None:
    raw_field = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    manifest = _manifest()
    manifest["workload_contract"][raw_field] = False
    apps = _FakeAppsV1(
        {"loom-service": _deployment(name="loom-service", image="loom-service:staging-abc123")}
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_field not in json.dumps(
        {"detail": check.detail, "evidence": check.evidence},
        sort_keys=True,
    )


@pytest.mark.parametrize(
    "expected_env_name",
    [
        "LOOM_SVC_WORKLOAD_TRUST_MODE",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION",
    ],
)
def test_release_gate_rejects_live_loom_service_workload_contract_mismatch(
    expected_env_name: str,
) -> None:
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    live_env[expected_env_name] = "mismatch"
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert (
        check.evidence["expected"][expected_env_name] != check.evidence["actual"][expected_env_name]
    )
    assert not report.all_pass


def test_release_gate_live_workload_contract_mismatch_redacts_raw_actual_value() -> None:
    raw_mode = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": raw_mode,
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
        "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION": "False",
    }
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert raw_mode not in json.dumps(check.evidence, sort_keys=True)
    assert check.evidence["actual"]["LOOM_SVC_WORKLOAD_TRUST_MODE"] == "[REDACTED]"


def test_release_gate_rejects_missing_live_loom_service_workload_contract_env() -> None:
    live_env = {
        "LOOM_SVC_WORKLOAD_TRUST_MODE": "internal_trusted",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED": "False",
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED": "False",
    }
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                workload_contract_env=live_env,
            )
        }
    )

    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=apps,
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "workload-trust-contract")
    assert check.outcome == "fail"
    assert check.evidence["actual"]["LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION"] is None


def _external_workers_manifest_section() -> dict[str, Any]:
    return {
        "environment_state_file": {
            "path": "deploy/environment-state/staging.toml",
            "sha256": "state-sha",
        },
        "control_plane_environment": "staging",
        "slurm_pools": [
            {
                "pool_name": "oldlab",
                "actuator": "slurm",
                "external_runner": True,
                "env_file": (
                    "/shared_work/qianyi/loom-worker-capacity/"
                    "staging-oldlab-worker-staging-abc123.env"
                ),
                "repo_dir": "/shared_work/qianyi/loom-remote-worker-staging-abc123",
            },
        ],
        "gb10_desired_states": [
            {
                "environment": "staging",
                "pool_name": "gb10",
                "image_tag": "staging-abc123",
                "max_concurrent": 10,
                "env_config_version": "staging-abc123",
                "source_git_commit": "a" * 40,
                "target_slots": 150,
                "host_intents": {
                    f"trt-gb10-{index}": "active" for index in range(1, 16)
                },
            },
        ],
    }


def _catalog_manifest_section() -> dict[str, Any]:
    return {
        "required": True,
        "command": (
            "loom datasets provision-catalog && "
            "loom datasets register skilllearnbench --hf-org PRHW "
            '--revision "$PUBLISHED_SHA" --mirror-to-object-store '
            "--bucket loom-benchmarks && "
            "loom datasets audit --all --verify-bundles"
        ),
        "required_env": [
            "HF_TOKEN",
            "LOOM_SVC_DB_URL",
            "LOOM_SVC_MINIO_ENDPOINT",
            "LOOM_SVC_MINIO_ACCESS_KEY",
            "LOOM_SVC_MINIO_SECRET_KEY",
        ],
    }


_ACTIVE_GB10_HOSTS = [f"trt-gb10-{index}" for index in range(1, 16)]


def _hf_external_workers_manifest_section() -> dict[str, Any]:
    external_workers = _external_workers_manifest_section()
    desired = external_workers["gb10_desired_states"][0]
    desired["target_slots"] = 150
    desired["host_intents"] = {host: "active" for host in _ACTIVE_GB10_HOSTS}
    return external_workers


def _gb10_status_for_external_workers(
    external_workers: dict[str, Any],
) -> dict[str, Any]:
    desired_states = copy.deepcopy(external_workers["gb10_desired_states"])
    desired = desired_states[0]
    nodes = [
        {
            "environment": desired["environment"],
            "pool_name": desired["pool_name"],
            "hostname": host,
            "apply_state": "applied",
            "current_image_tag": desired["image_tag"],
            "current_env_config_version": desired["env_config_version"],
            "current_max_concurrent": desired["max_concurrent"],
            "desired_intent": "active",
            "source_git_commit": desired["source_git_commit"],
            "source_git_dirty": False,
            "worker_id": f"worker-{host}",
            "worker_status": "active",
            "worker_fresh": True,
            "worker_backend_names": ["docker"],
        }
        for host in _ACTIVE_GB10_HOSTS
    ]
    return {
        "desired_states": desired_states,
        "nodes": nodes,
        "unlinked_workers": [],
    }


def _gb10_release_gate_inputs(external_workers: dict[str, Any]) -> dict[str, Any]:
    return {
        "environment_state_check_artifact": {
            "environment": "staging",
            "control_plane_environment": "staging",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        "gb10_workers_status_artifact": _gb10_status_for_external_workers(external_workers),
    }


def _hf_boundary_evidence(
    *,
    gb10_status: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    if gb10_status is None:
        gb10_status = _gb10_status_for_external_workers(_hf_external_workers_manifest_section())
    status_sha256 = hashlib.sha256(
        json.dumps(gb10_status, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    current_active_worker_ids = [
        node["worker_id"]
        for node in gb10_status.get("nodes", [])
        if isinstance(node, dict)
        and node.get("hostname") in _ACTIVE_GB10_HOSTS
        and isinstance(node.get("worker_id"), str)
    ]
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "environment": "staging",
        "benchmark_id": "skilllearnbench",
        "candidate_binding": {
            "environment": "staging",
            "release_image_tag": "staging-abc123",
            "release_git_sha": "a" * 40,
            "gb10_workers_status_sha256": status_sha256,
        },
        "catalog": {
            "runnable_tasks": 100,
            "artifact_contract_classified_tasks": 100,
            "apd5_required_artifact_contract_tasks": 1,
            "requires_caps": {"cpu_arch": "any"},
        },
        "runtime_sources": {
            "total_task_sources": 100,
            "internal_s3_sources": 100,
            "non_internal_sources": [],
            "sample_s3_source": "s3://loom-benchmarks/skilllearnbench/task-000/",
        },
        "hf_provenance": {
            "upstream_kind": "huggingface",
            "upstream_locator": "PRHW/SkillLearnBench",
            "upstream_revision": "abc123def456",
        },
        "worker_boundary": {
            "canary_started": True,
            "terminal_state": "succeeded",
            "canary_task_filter": {
                "task_ids": ["skilllearnbench/example/example-1"],
            },
            "canary_worker_pools": {
                "active": {},
                "terminal": {"gb10": 2},
            },
            "expected_trial_count": 2,
            "succeeded_trials": 2,
            "canary_task_provenance": {
                "trial_count": 2,
                "target_benchmark_trial_count": 2,
                "non_target_trial_count": 0,
                "task_set_trial_count": 0,
                "benchmark_ids": ["skilllearnbench"],
                "worker_ids": current_active_worker_ids[:2],
            },
            "hf_token_present": False,
            "hf_token_isolated": True,
            "direct_hf_egress_required": False,
            "materialized_from_internal_source": True,
            "gb10_hf_token_check_summary": {
                "checked_hosts": 15,
                "checked_host_names": _ACTIVE_GB10_HOSTS,
                "ssh_failed_hosts": [],
                "docker_ps_failed_hosts": [],
                "hosts_without_containers": [],
                "env_file_missing_hosts": [],
                "env_file_hf_token_present_hosts": [],
                "hosts_with_container_hf_token_present": [],
                "containers_checked": 15,
                "inspect_failed": [],
            },
        },
        "secret_scan": {"raw_secret_values_present": False},
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(evidence.get(key), dict):
            evidence[key] = {**evidence[key], **value}
        else:
            evidence[key] = value
    return evidence


def test_release_gate_passes_when_ready_pod_image_id_matches_expected_digest() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
        external_workers=external_workers,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        **_gb10_release_gate_inputs(external_workers),
    )

    assert report.all_pass
    image_check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert image_check.outcome == "pass"
    assert image_check.evidence["pod"] == "loom-service-abc"
    assert image_check.evidence["generation"] == 7


def test_release_gate_fails_when_ready_pod_image_id_does_not_match_manifest() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["expected_digest"] == "sha256:" + "1" * 64
    assert check.evidence["live_image_id"].endswith("sha256:" + "9" * 64)
    assert check.evidence["identity_strategy"] == "runtime-image-id-or-repo-digest"
    assert check.evidence["runtime_identity_kind"] == "runtime"
    assert check.evidence["runtime_identity_mismatch"] is True


def test_release_gate_accepts_kind_import_runtime_identity_when_template_matches() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
        external_workers=external_workers,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-kind",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        **_gb10_release_gate_inputs(external_workers),
    )

    assert report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "pass"
    assert (
        check.detail == "Ready pod uses kind-imported runtime identity for release template image"
    )
    assert check.evidence["identity_strategy"] == "kind-import-template-image"
    assert check.evidence["runtime_identity_kind"] == "kind-import"
    assert check.evidence["runtime_identity_mismatch"] is True


def test_release_gate_rejects_stale_status_image_on_kind_import_pod() -> None:
    """#339 regression — kind-import must not mask an old ReplicaSet pod.

    Deployment template image says `staging-abc123` (the release target),
    but the only Ready pod still has the old image in its pod spec. The pod's
    runtime image ID has the kind-import shape, so the gate must reject it
    before treating a kind-import runtime identity as acceptable.
    """
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-kind",
                app="loom-service",
                image="loom-service:staging-old",
                status_image="loom-service:staging-old",
                image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.remediation is not None
    assert "wait" in check.remediation.lower()


def test_release_gate_accepts_kind_import_status_image_alias_on_target_pod() -> None:
    """kind/containerd can report another tag for the target pod's image.

    The release gate should reject old ReplicaSet pods by checking the pod spec
    against the Deployment template. Once the Ready pod's spec is the release
    template image, a kind-import runtime identity plus a different
    status.containerStatuses[].image tag can be a containerd display alias.
    """
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
        external_workers=external_workers,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-kind",
                app="loom-service",
                image="loom-service:staging-abc123",
                status_image="docker.io/library/loom-service:staging-old",
                image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        **_gb10_release_gate_inputs(external_workers),
    )

    assert report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "pass"
    assert check.evidence["identity_strategy"] == "kind-import-template-image"
    assert check.evidence["status_image_stale"] is True
    assert check.evidence["status_image_matches_template"] is False
    assert check.evidence["live_image"] == "docker.io/library/loom-service:staging-old"


def test_release_gate_does_not_mark_default_docker_prefix_status_image_stale() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
        external_workers=external_workers,
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-kind",
                app="loom-service",
                image="loom-service:staging-abc123",
                status_image="docker.io/library/loom-service:staging-abc123",
                image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        **_gb10_release_gate_inputs(external_workers),
    )

    assert report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.evidence["live_image"] == "docker.io/library/loom-service:staging-abc123"
    assert check.evidence["status_image_matches_template"] is True
    assert check.evidence["status_image_stale"] is False


def test_release_gate_passes_zero_replica_deployment_when_template_matches() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(
        expected_digest="sha256:" + "1" * 64,
        external_workers=external_workers,
    )
    deployment = _deployment(
        name="loom-service",
        image="loom-service:staging-abc123",
        replicas=0,
    )
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        **_gb10_release_gate_inputs(external_workers),
    )

    assert report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "pass"
    assert check.detail == "zero-replica Deployment template image matches release manifest"
    assert check.evidence["desired_replicas"] == 0
    assert check.evidence["identity_strategy"] == "zero-replica-template-image"


def test_release_gate_fails_zero_replica_deployment_when_template_drifts() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:old-tag",
        replicas=0,
    )
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1([])

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Deployment template image does not match release manifest"
    assert check.evidence["desired_replicas"] == 0


def test_release_gate_ignores_ready_pods_not_from_deployment_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:old-tag",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_target_generation_pod_lacks_runtime_image_id() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id=None,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Ready pod is missing runtime image identity"
    assert check.evidence["runtime_identity_kind"] == "missing"


def test_release_gate_rejects_stale_kind_import_pod_from_old_template() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:staging-old",
                image_id="docker.io/library/import-2026-07-02@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "no target-generation Ready pods found for managed Deployment"
    assert check.evidence["pod_template_image"] == "loom-service:staging-abc123"


def test_release_gate_fails_when_deployment_generation_is_not_observed() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
                generation=8,
                observed_generation=7,
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "Deployment rollout is not target-generation converged"
    assert check.evidence["generation"] == 8
    assert check.evidence["observed_generation"] == 7


def test_release_gate_fails_when_deployment_updated_replicas_are_partial() -> None:
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:staging-abc123",
    )
    deployment.spec.replicas = 2
    deployment.status.updated_replicas = 1
    deployment.status.ready_replicas = 1
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.evidence["desired_replicas"] == 2
    assert check.evidence["updated_replicas"] == 1
    assert check.evidence["ready_replicas"] == 1


def test_release_gate_classifies_node_runtime_sandbox_cleanup_failure() -> None:
    """#206 regression: a target pod can be Ready while an old pod is stuck
    terminating because kubelet/containerd cannot kill its pod sandbox.

    That is a node-runtime cleanup failure. The release gate must not pass the
    image identity row just because one target-generation pod is Ready.
    """
    manifest = _manifest(expected_digest="sha256:" + "1" * 64)
    deployment = _deployment(
        name="loom-service",
        image="loom-service:staging-abc123",
    )
    deployment.status.replicas = 2
    deployment.status.updated_replicas = 1
    deployment.status.ready_replicas = 1
    apps = _FakeAppsV1({"loom-service": deployment})
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
            _pod(
                name="loom-service-old",
                app="loom-service",
                image="loom-service:staging-old",
                deletion_timestamp="2026-06-30T16:44:56Z",
            ),
        ],
        events=[
            _event(
                pod="loom-service-old",
                reason="FailedKillPod",
                message=(
                    "KillPodSandboxError: rpc error: code = DeadlineExceeded "
                    "desc = context deadline exceeded"
                ),
            ),
        ],
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(
        check for check in report.checks if check.name == "image-identity:loom-service/app"
    )
    assert check.outcome == "fail"
    assert check.detail == "node runtime sandbox deadline blocked Deployment rollout"
    assert check.remediation is not None
    assert "--recover-sandbox-deadlines" in check.remediation
    assert check.evidence["failure_class"] == "node_runtime_sandbox_deadline"
    assert check.evidence["total_replicas"] == 2
    assert check.evidence["sandbox_deadline_diagnostics"] == [
        {
            "pod": "loom-service-old",
            "reason": "FailedKillPod",
            "operation": "kill",
            "target_generation": False,
        },
    ]


def test_release_gate_fails_on_rendered_manifest_hash_drift() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=_FakeAppsV1({}),
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="different-rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    check = next(check for check in report.checks if check.name == "rendered-manifest-sha256")
    assert check == ReleaseGateCheck(
        name="rendered-manifest-sha256",
        outcome="fail",
        detail="rendered manifest hash drift",
        evidence={
            "expected_sha256": "rendered-sha",
            "live_sha256": "different-rendered-sha",
        },
        remediation="rerender from the release manifest inputs before accepting rollout",
    )


def test_release_gate_fails_when_disabled_k8s_worker_is_still_live() -> None:
    manifest = _manifest()
    manifest["cluster_config"]["k8s_worker_enabled"] = False
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
            "loom-worker": _deployment(
                name="loom-worker",
                image="loom-worker:stale",
                replicas=6,
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-new",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
            _ready_pod(
                name="loom-worker-stale",
                app="loom-worker",
                image="loom-worker:stale",
                image_id="docker-pullable://loom-worker@sha256:" + "9" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom-staging",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "disabled-k8s-worker-pruned")
    assert check.outcome == "fail"
    assert check.detail == "disabled k8s worker remains live"
    assert check.evidence["deployment"] == "loom-worker"
    assert check.evidence["desired_replicas"] == 6
    assert check.evidence["ready_replicas"] == 6
    assert check.evidence["ready_pods"] == ["loom-worker-stale"]
    assert "loom cluster up" in (check.remediation or "")


def test_release_gate_fails_on_live_alembic_revision_mismatch() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(alembic_heads=["0050"]),
        apps_v1=_FakeAppsV1({}),
        core_v1=_FakeCoreV1([]),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0049"],
    )

    check = next(check for check in report.checks if check.name == "alembic-heads")
    assert check.outcome == "fail"
    assert check.evidence == {
        "expected_heads": ["0050"],
        "compatible_heads": ["0050"],
        "live_heads": ["0049"],
        "database_target": "env:LOOM_CP_DB_URL",
    }
    assert "LOOM_CP_DB_URL" in check.detail


def test_release_gate_requires_environment_state_check_when_manifest_records_external_workers() -> (
    None
):
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "environment-state-convergence")
    assert check.outcome == "fail"
    assert check.detail == "environment-state check artifact is required"
    assert check.evidence["expected_profile"] == "deploy/environment-state/staging.toml"
    assert check.evidence["expected_profile_sha256"] == "state-sha"


def test_release_gate_requires_hf_mirror_boundary_evidence_for_staging_catalog_gate() -> None:
    manifest = _manifest()
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "hf-mirror-token-boundary")
    assert check.outcome == "fail"
    assert check.detail == "HF mirror/token boundary evidence artifact is required"
    assert check.evidence["benchmark_id"] == "skilllearnbench"
    assert check.evidence["catalog_provisioning_required"] is True
    assert "release-gate --hf-mirror-boundary-evidence" in (check.remediation or "")


def test_release_gate_accepts_secret_safe_hf_mirror_boundary_evidence() -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact=_gb10_status_for_external_workers(external_workers),
        hf_mirror_boundary_artifact=_hf_boundary_evidence(),
        hf_mirror_boundary_path="hf-mirror-boundary-staging-abc123.json",
    )

    assert report.all_pass
    check = next(check for check in report.checks if check.name == "hf-mirror-token-boundary")
    assert check.outcome == "pass"
    assert check.evidence["internal_s3_sources"] == 100
    assert check.evidence["hf_provenance_retained"] is True
    assert check.evidence["worker_hf_token_present"] is False
    assert check.evidence["direct_hf_egress_required"] is False
    assert check.evidence["canary_workers_match_current_candidate"] is True
    assert check.evidence["canary_trial_worker_ids"] == [
        "worker-trt-gb10-1",
        "worker-trt-gb10-2",
    ]


def test_release_gate_rejects_hf_boundary_when_gb10_checks_do_not_run() -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    evidence = _hf_boundary_evidence(
        worker_boundary={
            "gb10_hf_token_check_summary": {
                "checked_hosts": 15,
                "checked_host_names": [
                    *_ACTIVE_GB10_HOSTS,
                    "trt-gb10-7",
                ],
                "ssh_failed_hosts": [
                    "trt-gb10-1",
                    "trt-gb10-2",
                ],
                "docker_ps_failed_hosts": [],
                "hosts_without_containers": [],
                "env_file_missing_hosts": [],
                "env_file_hf_token_present_hosts": [],
                "hosts_with_container_hf_token_present": [],
                "containers_checked": 0,
                "inspect_failed": [],
            },
        },
    )
    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact=_gb10_status_for_external_workers(external_workers),
        hf_mirror_boundary_artifact=evidence,
        hf_mirror_boundary_path="hf-mirror-boundary-staging-abc123.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "hf-mirror-token-boundary")
    assert check.outcome == "fail"
    assert "exact manifest-active host set" in check.detail
    assert check.evidence["gb10_ssh_failed_hosts"] == [
        "trt-gb10-1",
        "trt-gb10-2",
    ]
    assert check.evidence["gb10_containers_checked"] == 0


@pytest.mark.parametrize(
    "drift",
    [
        "fourteen-hosts",
        "sixteen-hosts",
        "wrong-fifteen-host-set",
        "missing-failure-list",
        "non-list-failure-field",
    ],
)
def test_hf_boundary_rejects_inexact_or_incomplete_gb10_coverage(drift: str) -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    gb10_status = _gb10_status_for_external_workers(external_workers)
    artifact = _hf_boundary_evidence(gb10_status=gb10_status)
    summary = artifact["worker_boundary"]["gb10_hf_token_check_summary"]
    if drift == "fourteen-hosts":
        summary["checked_hosts"] = 14
        summary["checked_host_names"] = _ACTIVE_GB10_HOSTS[:-1]
        summary["containers_checked"] = 14
    elif drift == "sixteen-hosts":
        summary["checked_hosts"] = 16
        summary["checked_host_names"] = [*_ACTIVE_GB10_HOSTS, "trt-gb10-7"]
        summary["containers_checked"] = 16
    elif drift == "wrong-fifteen-host-set":
        summary["checked_host_names"] = [*_ACTIVE_GB10_HOSTS[:-1], "trt-gb10-14"]
    elif drift == "missing-failure-list":
        summary.pop("docker_ps_failed_hosts")
    elif drift == "non-list-failure-field":
        summary["hosts_without_containers"] = "none"

    check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path="hf-mirror-boundary.json",
        artifact_error=None,
        gb10_status_artifact=gb10_status,
    )

    assert check is not None
    assert check.outcome == "fail"
    if drift in {"missing-failure-list", "non-list-failure-field"}:
        assert check.evidence["gb10_summary_schema_valid"] is False
        assert check.detail == "GB10 HF token check summary is incomplete or invalid"
    else:
        assert "exact manifest-active host set" in check.detail


@pytest.mark.parametrize("drift", ["image-tag", "git-sha", "gb10-status"])
def test_hf_boundary_rejects_stale_candidate_binding(drift: str) -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    original_status = _gb10_status_for_external_workers(external_workers)
    artifact = _hf_boundary_evidence(gb10_status=original_status)
    checked_status = copy.deepcopy(original_status)
    if drift == "image-tag":
        manifest["release"]["image_tag"] = "staging-new1234"
    elif drift == "git-sha":
        manifest["release"]["git_sha"] = "b" * 40
    elif drift == "gb10-status":
        checked_status["nodes"][0]["worker_id"] = "replacement-worker"

    check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path="hf-mirror-boundary.json",
        artifact_error=None,
        gb10_status_artifact=checked_status,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == (
        "HF boundary evidence must bind the exact candidate and GB10 status artifact"
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "explicit-old-batch",
        "worker-restart",
        "mixed-current-old",
        "unknown-worker",
        "unlinked-worker",
        "stopped-host-worker",
        "null-worker",
        "missing-worker-ids",
    ],
)
def test_hf_boundary_rejects_canary_from_non_current_worker_registration(
    scenario: str,
) -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    gb10_status = _gb10_status_for_external_workers(external_workers)
    original_worker_ids = [node["worker_id"] for node in gb10_status["nodes"][:2]]

    if scenario == "worker-restart":
        gb10_status["nodes"][0]["worker_id"] = "worker-restarted-1"
        gb10_status["nodes"][1]["worker_id"] = "worker-restarted-2"
    elif scenario == "unlinked-worker":
        gb10_status["unlinked_workers"] = [
            {
                "worker_id": "worker-unlinked",
                "hostname": "trt-gb10-1",
                "pool_name": "gb10",
                "worker_fresh": False,
            },
        ]
    elif scenario == "stopped-host-worker":
        gb10_status["nodes"].append(
            {
                "environment": "staging",
                "pool_name": "gb10",
                "hostname": "trt-gb10-7",
                "desired_intent": "stopped",
                "worker_id": "worker-stopped-7",
                "worker_status": "inactive",
                "worker_fresh": False,
            },
        )

    artifact = _hf_boundary_evidence(gb10_status=gb10_status)
    provenance = artifact["worker_boundary"]["canary_task_provenance"]
    if scenario == "explicit-old-batch":
        artifact["worker_boundary"]["canary_batch_id"] = "11111111-1111-1111-1111-111111111111"
        provenance["worker_ids"] = ["worker-old-1", "worker-old-2"]
    elif scenario == "worker-restart":
        provenance["worker_ids"] = original_worker_ids
    elif scenario == "mixed-current-old":
        provenance["worker_ids"] = [gb10_status["nodes"][0]["worker_id"], "worker-old"]
    elif scenario == "unknown-worker":
        provenance["worker_ids"] = ["worker-unknown-1", "worker-unknown-2"]
    elif scenario == "unlinked-worker":
        provenance["worker_ids"] = [gb10_status["nodes"][0]["worker_id"], "worker-unlinked"]
    elif scenario == "stopped-host-worker":
        provenance["worker_ids"] = [gb10_status["nodes"][0]["worker_id"], "worker-stopped-7"]
    elif scenario == "null-worker":
        provenance["worker_ids"] = [gb10_status["nodes"][0]["worker_id"], None]
    elif scenario == "missing-worker-ids":
        provenance.pop("worker_ids")

    check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path="hf-mirror-boundary.json",
        artifact_error=None,
        gb10_status_artifact=gb10_status,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == "canary trials must use current candidate GB10 worker registrations"
    assert check.evidence["canary_workers_match_current_candidate"] is False
    if scenario in {"null-worker", "missing-worker-ids"}:
        assert check.evidence["canary_worker_ids_schema_valid"] is False
    else:
        assert check.evidence["canary_worker_ids_schema_valid"] is True


@pytest.mark.parametrize(
    "drift",
    [
        "failed",
        "cancelled",
        "running",
        "zero-expected",
        "partial-success",
        "boolean-counts",
        "wrong-task-filter",
        "mixed-benchmark-filter",
        "mixed-task-filter",
        "taskset-filter",
        "missing-task-provenance",
        "non-target-task-provenance",
        "boolean-task-provenance",
        "wrong-worker-pool",
        "extra-terminal-pool",
    ],
)
def test_hf_boundary_requires_fully_succeeded_matching_canary(drift: str) -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    gb10_status = _gb10_status_for_external_workers(external_workers)
    artifact = _hf_boundary_evidence(gb10_status=gb10_status)
    boundary = artifact["worker_boundary"]
    if drift in {"failed", "cancelled", "running"}:
        boundary["terminal_state"] = drift
    elif drift == "zero-expected":
        boundary["expected_trial_count"] = 0
        boundary["succeeded_trials"] = 0
        boundary["canary_worker_pools"]["terminal"]["gb10"] = 0
    elif drift == "partial-success":
        boundary["succeeded_trials"] = 1
    elif drift == "boolean-counts":
        boundary["expected_trial_count"] = 1
        boundary["succeeded_trials"] = True
        boundary["canary_worker_pools"]["terminal"]["gb10"] = True
    elif drift == "wrong-task-filter":
        boundary["canary_task_filter"] = {"benchmark_id": "other"}
    elif drift == "mixed-benchmark-filter":
        boundary["canary_task_filter"] = {
            "benchmark_ids": ["other", "skilllearnbench"],
            "subset_kind": "first_n",
            "n": 1,
        }
    elif drift == "mixed-task-filter":
        boundary["canary_task_filter"] = {
            "task_ids": ["other/task-1", "skilllearnbench/example/example-1"],
        }
    elif drift == "taskset-filter":
        boundary["canary_task_filter"] = {
            "benchmark_id": "skilllearnbench",
            "task_set_id": "other-set",
        }
    elif drift == "missing-task-provenance":
        boundary.pop("canary_task_provenance")
    elif drift == "non-target-task-provenance":
        boundary["canary_task_provenance"] = {
            "trial_count": 2,
            "target_benchmark_trial_count": 1,
            "non_target_trial_count": 1,
            "task_set_trial_count": 0,
            "benchmark_ids": ["other", "skilllearnbench"],
        }
    elif drift == "boolean-task-provenance":
        boundary["canary_task_provenance"]["non_target_trial_count"] = False
    elif drift == "wrong-worker-pool":
        boundary["canary_worker_pools"] = {
            "active": {},
            "terminal": {"oldlab": 2},
        }
    elif drift == "extra-terminal-pool":
        boundary["canary_worker_pools"]["terminal"]["oldlab"] = 1

    check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path="hf-mirror-boundary.json",
        artifact_error=None,
        gb10_status_artifact=gb10_status,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == "canary must be a fully succeeded SkillLearnBench GB10 batch"


@pytest.mark.parametrize(
    ("drift", "expected_detail"),
    [
        ("negative-runnable", "SkillLearnBench catalog must report runnable tasks"),
        (
            "missing-non-internal-list",
            "runtime_sources.non_internal_sources must be a list",
        ),
        (
            "string-non-internal-list",
            "runtime_sources.non_internal_sources must be a list",
        ),
        ("string-source-count", "SkillLearnBench must use internal s3:// runtime sources"),
        (
            "stale-artifact-contract",
            "SkillLearnBench manifest must classify every required-artifact "
            "contract and declare APD-5 required_artifacts",
        ),
        (
            "missing-apd5-contract",
            "SkillLearnBench manifest must classify every required-artifact "
            "contract and declare APD-5 required_artifacts",
        ),
    ],
)
def test_hf_boundary_rejects_malformed_catalog_or_source_coverage(
    drift: str,
    expected_detail: str,
) -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    gb10_status = _gb10_status_for_external_workers(external_workers)
    artifact = _hf_boundary_evidence(gb10_status=gb10_status)
    if drift == "negative-runnable":
        artifact["catalog"]["runnable_tasks"] = -1
    elif drift == "missing-non-internal-list":
        artifact["runtime_sources"].pop("non_internal_sources")
    elif drift == "string-non-internal-list":
        artifact["runtime_sources"]["non_internal_sources"] = "hf://external/task"
    elif drift == "string-source-count":
        artifact["runtime_sources"]["total_task_sources"] = "100"
    elif drift == "stale-artifact-contract":
        artifact["catalog"]["artifact_contract_classified_tasks"] = 0
    elif drift == "missing-apd5-contract":
        artifact["catalog"]["apd5_required_artifact_contract_tasks"] = 0

    check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path="hf-mirror-boundary.json",
        artifact_error=None,
        gb10_status_artifact=gb10_status,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == expected_detail


def test_release_gate_rejects_non_s3_or_secret_leaking_hf_boundary_evidence() -> None:
    external_workers = _hf_external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    evidence = _hf_boundary_evidence(
        runtime_sources={
            "internal_s3_sources": 99,
            "non_internal_sources": ["hf://PRHW/SkillLearnBench/task-099"],
        },
        worker_boundary={"hf_token_present": True},
        operator_note="HF_TOKEN=hf_FAKESECRET12345678901234567890",
    )
    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact=_gb10_status_for_external_workers(external_workers),
        hf_mirror_boundary_artifact=evidence,
        hf_mirror_boundary_path="hf-mirror-boundary-staging-abc123.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "hf-mirror-token-boundary")
    assert check.outcome == "fail"
    assert "must use internal s3:// runtime sources" in check.detail
    assert check.evidence["secret_safe"] is False
    assert check.evidence["secret_leak_paths"] == ["operator_note"]
    assert check.evidence["worker_hf_token_present"] is True


def test_release_gate_fails_when_environment_state_reports_excluded_slurm_node() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": False,
            "drift": [
                {
                    "path": ("slurm_worker_jobs[production/gb10/18186].nodelist"),
                    "desired": ["trt-gb10-1", "trt-gb10-2"],
                    "live": "trt-gb10-7",
                },
            ],
        },
        environment_state_check_path=(
            "/data/loom-staging/rollouts/20260702T055745Z-staging-d46a16c/"
            "environment-state-check-live-secrets.json"
        ),
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "environment-state-convergence")
    assert check.outcome == "fail"
    assert check.detail == "live environment-state check reports drift"
    assert check.evidence["drift_count"] == 1
    assert check.evidence["drift"][0]["live"] == "trt-gb10-7"
    assert "environment-state apply/check" in (check.remediation or "")


def test_release_gate_passes_when_environment_state_check_is_clean() -> None:
    external_workers = _external_workers_manifest_section()
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=external_workers),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
        gb10_workers_status_artifact=_gb10_status_for_external_workers(external_workers),
        gb10_workers_status_path="gb10-workers-status-staging-abc123.json",
    )

    assert report.all_pass
    check = next(check for check in report.checks if check.name == "environment-state-convergence")
    assert check.outcome == "pass"
    assert check.detail == "live environment-state check passed"
    assert check.evidence["drift_count"] == 0
    assert check.evidence["artifact"] == "environment-state-check-live-secrets.json"


def test_release_gate_fails_when_minio_storage_preflight_stops() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        minio_storage_preflight_artifact={
            "outcome": "stop",
            "filesystem": {
                "free_percent": 8.0,
                "free_bytes": 8 * 1024**3,
            },
            "thresholds": {
                "warn_free_percent": 25.0,
                "stop_free_percent": 15.0,
            },
            "checks": [
                {
                    "name": "minio-data-free-space",
                    "outcome": "stop",
                    "detail": "free space 8.0% is below stop threshold 15.0%",
                },
            ],
        },
        minio_storage_preflight_path="minio-storage-preflight.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "minio-storage-pressure")
    assert check.outcome == "fail"
    assert check.detail == "MinIO storage preflight reports stop"
    assert check.evidence["artifact"] == "minio-storage-preflight.json"
    assert check.evidence["free_percent"] == 8.0


def test_release_gate_evidence_includes_autoscaler_blockers() -> None:
    blockers = [
        {
            "environment": "staging",
            "pool_name": "oldlab",
            "actuator": "slurm",
            "last_decision": "blocked",
            "last_decision_reason": "no_safe_slurm_nodes",
            "last_blocked_reason": "no_safe_slurm_nodes",
            "last_blocked_details": {
                "node_exclusions": [
                    {"hostname": "oldlab-1", "reason": "insufficient_memory"},
                    {"hostname": "oldlab-2", "reason": "cpu_load_high"},
                ],
            },
            "last_error": None,
        },
    ]
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": False,
            "drift": [],
            "autoscaler_blockers": blockers,
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "environment-state-convergence")
    assert check.outcome == "fail"
    assert check.detail == "live environment-state check reports autoscaler blockers"
    assert check.evidence["drift_count"] == 0
    assert check.evidence["autoscaler_blocker_count"] == 1
    assert check.evidence["autoscaler_blockers"] == blockers


def test_release_gate_report_includes_component_evidence_rows() -> None:
    report = collect_release_gate_report(
        manifest=_manifest(external_workers=_external_workers_manifest_section()),
        apps_v1=_FakeAppsV1(
            {
                "loom-service": _deployment(
                    name="loom-service",
                    image="loom-service:staging-abc123",
                    generation=9,
                    observed_generation=9,
                ),
            }
        ),
        core_v1=_FakeCoreV1(
            [
                _ready_pod(
                    name="loom-service-new",
                    app="loom-service",
                    image="loom-service:staging-abc123",
                    image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
                ),
            ]
        ),
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "control_plane_environment": "production",
            "profile": "deploy/environment-state/staging.toml",
            "ok": True,
            "drift": [],
        },
        environment_state_check_path="environment-state-check-live-secrets.json",
    )

    data = release_gate_report_to_dict(report)
    rows = data["component_evidence"]

    k8s_row = next(row for row in rows if row["component"] == "loom-service/app")
    assert k8s_row["surface"] == "kubernetes"
    assert k8s_row["expected_release"] == "loom-service:staging-abc123"
    assert k8s_row["live_release"] == "loom-service:staging-abc123"
    assert k8s_row["expected_digest"] == "loom-service@sha256:" + "1" * 64
    assert k8s_row["live_digest"].endswith("sha256:" + "1" * 64)
    assert k8s_row["generation"] == 9
    assert k8s_row["readiness"] == "1/1 ready"
    assert k8s_row["outcome"] == "pass"

    oldlab_row = next(row for row in rows if row["component"] == "oldlab")
    assert oldlab_row["surface"] == "external-worker"
    assert oldlab_row["expected_release"] == "deploy/environment-state/staging.toml"
    assert oldlab_row["live_release"] == "environment-state-check-live-secrets.json"
    assert oldlab_row["readiness"] == "environment-state converged"
    assert oldlab_row["outcome"] == "pass"

    gb10_row = next(row for row in rows if row["component"] == "gb10")
    assert gb10_row["surface"] == "external-worker"
    assert gb10_row["outcome"] == "pass"


def test_release_gate_markdown_formats_pasteable_component_table() -> None:
    report = ReleaseGateReport(
        environment="staging",
        namespace="loom",
        checks=[
            ReleaseGateCheck(
                name="image-identity:loom-service/app",
                outcome="pass",
                detail="Ready pod image identity matches release manifest",
                evidence={
                    "deployment": "loom-service",
                    "container": "app",
                    "expected_image": "loom-service:staging-abc123",
                    "expected_repo_digest": "loom-service@sha256:" + "1" * 64,
                    "generation": 7,
                    "observed_generation": 7,
                    "desired_replicas": 1,
                    "ready_replicas": 1,
                    "live_image": "loom-service:staging-abc123",
                    "live_image_id": "docker-pullable://loom-service@sha256:" + "1" * 64,
                    "pod": "loom-service-new",
                },
            ),
        ],
    )

    markdown = format_release_gate_markdown(report)

    assert (
        "| Surface | Component | Expected | Live | Generation/job | Readiness | Restart/crash | Evidence | Result |"
        in markdown
    )
    assert (
        "| kubernetes | loom-service/app | "
        "`loom-service:staging-abc123 / loom-service@sha256:"
        + "1" * 64
        + "` | `loom-service:staging-abc123 / docker-pullable://loom-service@sha256:"
        + "1" * 64
        + "` | `7` | 1/1 ready |  | `pod=loom-service-new` | PASS |"
    ) in markdown


def test_live_alembic_query_uses_kubectl_exec_without_leaking_db_url() -> None:
    calls: list[list[str]] = []

    def _runner(cmd: list[str]) -> tuple[int, str, str]:
        calls.append(cmd)
        return (
            0,
            json.dumps(
                {
                    "database_target": "env:LOOM_CP_DB_URL",
                    "heads": ["0050"],
                }
            ),
            "ignored stderr with postgresql://loom:secret@postgres/loom",
        )

    result = query_live_alembic_heads(
        namespace="loom",
        context="prod",
        runner=_runner,
    )

    assert result.heads == ["0050"]
    assert result.database_target == "env:LOOM_CP_DB_URL"
    assert calls[0][:5] == ["kubectl", "exec", "-n", "loom", "deploy/loom-control-plane"]
    assert "--context" in calls[0]
    assert "secret" not in json.dumps(result.evidence)


def test_live_alembic_query_timeout_returns_redacted_structured_error() -> None:
    def _runner(cmd: list[str]) -> tuple[int, str, str]:
        raise subprocess.TimeoutExpired(
            cmd=cmd,
            timeout=12,
            output="postgresql://loom:secret@postgres/loom",
            stderr="password=super-secret-token",
        )

    result = query_live_alembic_heads(
        namespace="loom",
        context="prod",
        runner=_runner,
        timeout_sec=12,
    )

    assert result.heads == []
    assert result.error == "kubectl exec timed out after 12s"
    evidence = json.dumps(result.evidence)
    assert "super-secret-token" not in evidence
    assert "postgresql://loom:secret" not in evidence
    assert "<redacted>" in evidence


def test_cluster_release_gate_cli_dry_run_reports_structured_failure(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        lambda **_kwargs: ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="alembic-heads",
                    outcome="fail",
                    detail="live DB revision does not match env:LOOM_CP_DB_URL",
                    evidence={
                        "expected_heads": ["0050"],
                        "live_heads": ["0049"],
                    },
                    remediation="run alembic upgrade head before accepting release",
                ),
            ],
        ),
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["all_pass"] is False
    assert out["checks"][0]["name"] == "alembic-heads"


def test_staging_cluster_release_gate_dry_run_does_not_require_prod_credentials(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    for name in (
        "LOOM_CANDIDATE_SHA",
        "LOOM_IMAGE_TAG",
        "LOOM_RELEASE_GATE_RUN_ID",
        "LOOM_SERVICE_API_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        lambda **_kwargs: ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="image-identity:loom-service/app",
                    outcome="pass",
                    detail="staging release manifest identity matched",
                    evidence={},
                ),
            ],
        ),
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["environment"] == "staging"
    assert out["all_pass"] is True


def test_cluster_release_gate_cli_passes_environment_state_check_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest(external_workers=_external_workers_manifest_section())),
        encoding="utf-8",
    )
    environment_state_check_path = tmp_path / "environment-state-check.json"
    environment_state_check_path.write_text(
        json.dumps(
            {
                "environment": "staging",
                "control_plane_environment": "production",
                "profile": "deploy/environment-state/staging.toml",
                "ok": True,
                "drift": [],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="environment-state-convergence",
                    outcome="pass",
                    detail="live environment-state check passed",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--environment-state-check",
            str(environment_state_check_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["environment_state_check_artifact"]["ok"] is True
    assert captured["environment_state_check_path"] == str(environment_state_check_path.resolve())


def test_cluster_release_gate_cli_passes_external_slurm_authority_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest(external_workers=_external_gb10_workers(enabled=True))),
        encoding="utf-8",
    )
    authority_path = tmp_path / "external-slurm-authority.json"
    authority = {"result": "pass", "candidate_sha": "a" * 40, "node_count": 15}
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(environment="staging", namespace="loom", checks=[])

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_root_owned_external_slurm_authority",
        lambda path: authority if path == authority_path.resolve() else None,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--external-slurm-authority",
            str(authority_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["external_slurm_authority_artifact"] == authority
    assert captured["external_slurm_authority_error"] is None


def test_external_slurm_authority_loader_rejects_operator_owned_json(tmp_path) -> None:
    path = tmp_path / "authority.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="root-owned"):
        _load_root_owned_external_slurm_authority(path)


def test_cluster_release_gate_cli_passes_hf_mirror_boundary_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest = _manifest()
    manifest["catalog_provisioning"] = _catalog_manifest_section()
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    boundary_path = tmp_path / "hf-mirror-boundary.json"
    boundary_path.write_text(json.dumps(_hf_boundary_evidence()), encoding="utf-8")
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="hf-mirror-token-boundary",
                    outcome="pass",
                    detail="SkillLearnBench HF mirror/token boundary evidence passed",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--hf-mirror-boundary-evidence",
            str(boundary_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["hf_mirror_boundary_artifact"]["benchmark_id"] == "skilllearnbench"
    assert captured["hf_mirror_boundary_path"] == str(boundary_path.resolve())


def test_release_gate_requires_gb10_status_artifact_when_manifest_declares_gb10() -> None:
    manifest = _manifest(external_workers=_external_workers_manifest_section())
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "gb10-worker-convergence")
    assert check.outcome == "fail"
    assert check.detail == "GB10 worker status artifact is required"


def test_release_gate_rejects_gb10_status_without_manifest_desired_state() -> None:
    manifest = _manifest(
        external_workers={
            "environment_state_file": None,
            "control_plane_environment": None,
            "slurm_pools": [],
            "gb10_desired_states": [],
        },
    )
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        gb10_workers_status_artifact={
            "desired_states": [],
            "nodes": [],
        },
        gb10_workers_status_path="gb10-workers-status.json",
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "gb10-worker-convergence")
    assert check.outcome == "fail"
    assert check.detail == "release manifest declares an invalid GB10 desired-state policy"
    assert check.evidence["manifest_desired_state_count"] == 0
    assert check.evidence["manifest_policy_mismatches"]


def test_gb10_gate_rejects_staging_manifest_without_external_worker_sections() -> None:
    check = _gb10_worker_check(
        manifest=_manifest(),
        artifact=None,
        artifact_path=None,
        artifact_error=None,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == "release manifest declares an invalid GB10 desired-state policy"
    assert check.evidence["manifest_policy_mismatches"] == [
        "staging release manifest must declare the GB10 desired-state contract"
    ]


@pytest.mark.parametrize(
    "drift",
    [
        "empty-desired-states",
        "missing-host-intents",
        "host-intents-subset",
        "wrong-target-slots",
    ],
)
def test_gb10_gate_rejects_status_not_exactly_bound_to_manifest(drift: str) -> None:
    external_workers = _external_workers_manifest_section()
    artifact_desired = copy.deepcopy(external_workers["gb10_desired_states"])
    if drift == "empty-desired-states":
        artifact_desired = []
    elif drift == "missing-host-intents":
        artifact_desired[0].pop("host_intents")
    elif drift == "host-intents-subset":
        artifact_desired[0]["host_intents"].pop("trt-gb10-7")
    elif drift == "wrong-target-slots":
        artifact_desired[0]["target_slots"] = 20

    check = _gb10_worker_check(
        manifest=_manifest(external_workers=external_workers),
        artifact={
            "desired_states": artifact_desired,
            "nodes": [],
            "unlinked_workers": [],
        },
        artifact_path="gb10-workers-status.json",
        artifact_error=None,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == ("GB10 worker status desired state does not match release manifest")
    assert check.evidence["contract_mismatches"]


@pytest.mark.parametrize(
    "drift",
    [
        "empty-manifest",
        "wrong-environment",
        "unknown-host-intent",
        "stale-source-sha",
    ],
)
def test_gb10_gate_rejects_invalid_candidate_manifest_policy(drift: str) -> None:
    external_workers = _external_workers_manifest_section()
    desired_states = external_workers["gb10_desired_states"]
    if drift == "empty-manifest":
        external_workers["gb10_desired_states"] = []
    elif drift == "wrong-environment":
        desired_states[0]["environment"] = "production"
    elif drift == "unknown-host-intent":
        desired_states[0]["host_intents"]["trt-gb10-6"] = "actve"
    elif drift == "stale-source-sha":
        desired_states[0]["source_git_commit"] = "b" * 40
    manifest = _manifest(external_workers=external_workers)
    artifact = (
        None if drift == "empty-manifest" else _gb10_status_for_external_workers(external_workers)
    )

    check = _gb10_worker_check(
        manifest=manifest,
        artifact=artifact,
        artifact_path=None,
        artifact_error=None,
    )

    assert check is not None
    assert check.outcome == "fail"
    assert check.detail == "release manifest declares an invalid GB10 desired-state policy"


def test_release_gate_fails_when_gb10_status_reports_missing_active_host() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    status = _gb10_status_for_external_workers(external_workers)
    status["nodes"] = []
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact=status,
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "gb10-worker-convergence")
    assert check.outcome == "fail"
    assert "trt-gb10-1" in check.evidence["mismatches"][0]
    assert "missing active node report" in check.evidence["mismatches"][0]


def test_release_gate_fails_when_active_gb10_node_has_no_registered_worker() -> None:
    external_workers = _external_workers_manifest_section()
    manifest = _manifest(external_workers=external_workers)
    status = _gb10_status_for_external_workers(external_workers)
    status["nodes"][0]["worker_id"] = None
    apps = _FakeAppsV1(
        {
            "loom-service": _deployment(
                name="loom-service",
                image="loom-service:staging-abc123",
            ),
        }
    )
    core = _FakeCoreV1(
        [
            _ready_pod(
                name="loom-service-abc",
                app="loom-service",
                image="loom-service:staging-abc123",
                image_id="docker-pullable://loom-service@sha256:" + "1" * 64,
            ),
        ]
    )

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps,
        core_v1=core,
        namespace="loom",
        rendered_manifest_sha256="rendered-sha",
        cluster_config_sha256="config-sha",
        live_alembic_heads=["0050"],
        environment_state_check_artifact={
            "environment": "staging",
            "ok": True,
            "drift": [],
            "autoscaler_blockers": [],
        },
        gb10_workers_status_artifact=status,
    )

    assert not report.all_pass
    check = next(check for check in report.checks if check.name == "gb10-worker-convergence")
    assert check.outcome == "fail"
    assert "trt-gb10-1" in check.evidence["mismatches"][0]
    assert "missing active/fresh docker worker registration" in check.evidence["mismatches"][0]


def test_cluster_release_gate_cli_passes_gb10_status_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(_manifest(external_workers=_external_workers_manifest_section())),
        encoding="utf-8",
    )
    gb10_status_path = tmp_path / "gb10-workers-status.json"
    gb10_status_path.write_text(
        json.dumps({"desired_states": [], "nodes": []}),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="gb10-worker-convergence",
                    outcome="pass",
                    detail="GB10 worker status matches release target",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--gb10-workers-status",
            str(gb10_status_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["gb10_workers_status_artifact"] == {
        "desired_states": [],
        "nodes": [],
    }
    assert captured["gb10_workers_status_path"] == str(gb10_status_path.resolve())


def test_cluster_release_gate_cli_passes_minio_storage_preflight_artifact(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(_manifest()), encoding="utf-8")
    storage_path = tmp_path / "minio-storage-preflight.json"
    storage_path.write_text(
        json.dumps(
            {
                "outcome": "pass",
                "filesystem": {"free_percent": 42.0, "free_bytes": 42 * 1024**3},
                "thresholds": {"warn_free_percent": 25.0, "stop_free_percent": 15.0},
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    def _fake_collect_release_gate_report(**kwargs: Any) -> ReleaseGateReport:
        captured.update(kwargs)
        return ReleaseGateReport(
            environment="staging",
            namespace="loom",
            checks=[
                ReleaseGateCheck(
                    name="minio-storage-pressure",
                    outcome="pass",
                    detail="MinIO storage preflight passed",
                    evidence={},
                ),
            ],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_release_gate_report",
        _fake_collect_release_gate_report,
    )

    rc = main(
        [
            "cluster",
            "release-gate",
            "--manifest",
            str(manifest_path),
            "--namespace",
            "loom",
            "--environment",
            "staging",
            "--minio-storage-preflight",
            str(storage_path),
            "--dry-run",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert captured["minio_storage_preflight_artifact"]["outcome"] == "pass"
    assert captured["minio_storage_preflight_path"] == str(storage_path.resolve())
