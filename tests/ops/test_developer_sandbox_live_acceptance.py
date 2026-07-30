from __future__ import annotations

import base64
import copy
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator
from tests.ops.worker_runtime_binding_fixtures import (
    rich_image_archives,
    worker_runtime_bindings_from_archives,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/developer_sandbox_live_acceptance.py"
SCHEMA = REPO_ROOT / "docs/evidence/developer-sandbox-live-acceptance.schema.json"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("developer_sandbox_live_acceptance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ACCEPTANCE = _load_module()
FIXTURE_SANDBOXES = ("qianyi", "hongjian", "devansh")
FIXTURE_SERVICE_USERS = {
    "qianyi": "loom-sandbox-qianyi",
    "hongjian": "loom-sandbox-hongjian",
    "devansh": "loom-sandbox-devansh",
}
FIXTURE_PHASE_CHECKPOINTS = tuple(
    (phase, sandbox) for phase in ACCEPTANCE.PHASES for sandbox in FIXTURE_SANDBOXES
)


def _source_registry_snapshot(
    sandboxes: tuple[str, ...] = FIXTURE_SANDBOXES,
) -> dict[str, Any]:
    candidates = _candidate_map(sandboxes)
    environments: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    deployment_rows: list[dict[str, Any]] = []
    finalization_rows: list[dict[str, Any]] = []
    for index, sandbox in enumerate(sandboxes, start=1):
        env_id = f"denv-{index:08d}"
        principal_id = f"github:{1000 + index}"
        candidate = candidates[sandbox]
        bundle_sha256 = str(index) * 64
        image_digests = {
            "amd64": f"sha256:{candidate['sha']}{candidate['sha'][:24]}",
            "arm64": f"sha256:{candidate['tree']}{candidate['tree'][:24]}",
        }
        image_archive_bindings = rich_image_archives(
            amd64_config=image_digests["amd64"],
            arm64_config=image_digests["arm64"],
            seed=f"live-acceptance-{sandbox}",
        )
        candidate_identity = {
            "principal_id": principal_id,
            "env_id": env_id,
            "lifecycle_epoch": 1,
            "repository_id": "qianyi-sun/loom",
            "image_binding_kind": (ACCEPTANCE.environment_registry.WORKER_IMAGE_BINDING_KIND),
            "image_binding_version": (ACCEPTANCE.environment_registry.WORKER_IMAGE_BINDING_VERSION),
            "candidate_sha": candidate["sha"],
            "candidate_tree": candidate["tree"],
            "bundle_sha256": bundle_sha256,
            "image_digests": image_digests,
            "image_archives": image_archive_bindings,
        }
        candidate_id = "cand-" + ACCEPTANCE.environment_registry._digest(candidate_identity)[:40]
        legacy = sandbox in FIXTURE_SERVICE_USERS
        if legacy:
            service_user = FIXTURE_SERVICE_USERS[sandbox]
            resources = {
                "service_user": service_user,
                "service_group": service_user,
                "compose_project": f"loom-sandbox-{sandbox}",
                "systemd_instance": sandbox,
                "candidate_root": f"/shared_work/loom/candidates/sandboxes/{sandbox}",
                "runtime_root": f"/shared_work/loom/runtime/sandboxes/{sandbox}",
                "state_root": f"/srv/loom/developer-sandboxes/{sandbox}",
                "evidence_root": f"/srv/loom/developer-sandboxes/{sandbox}/evidence",
                "database_name": f"loom_sandbox_{sandbox}",
                "postgres_volume": f"loom-sandbox-{sandbox}_postgres_data",
                "minio_volume": f"loom-sandbox-{sandbox}_minio_data",
                "task_bucket": f"loom-sandbox-{sandbox}-tasks",
                "trajectories_bucket": f"loom-sandbox-{sandbox}-trajectories",
                "artifacts_bucket": f"loom-sandbox-{sandbox}-artifacts",
                "provider_namespace": f"sandbox-{sandbox}",
                "slurm_user": service_user,
                "slurm_account": f"loom-dev-{sandbox}",
                "slurm_qos": f"loom-dev-{sandbox}",
                "cgroup_slice": f"loom-dev-{sandbox}.slice",
            }
        else:
            resources = (
                ACCEPTANCE.environment_registry.DeveloperEnvironmentRegistry._dynamic_resources(
                    env_id,
                    sandbox,
                )
            )
        environments.append(
            {
                "env_id": env_id,
                "principal_id": principal_id,
                "display_name": sandbox,
                "layout_version": "legacy-v1" if legacy else "dynamic-v1",
                "runtime_id": sandbox,
                "state": "active",
                "resource_generation": 2,
                "lifecycle_epoch": 1,
                **resources,
                "uid": 20_000 + index,
                "gid": 20_000 + index,
                "ports": {
                    name: 30_000 + index * 100 + offset
                    for offset, name in enumerate(
                        ACCEPTANCE.environment_registry.PORT_NAMES,
                    )
                },
                "current_candidate_id": candidate_id,
                "created_at": _iso(0),
            },
        )
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "principal_id": principal_id,
                "env_id": env_id,
                "lifecycle_epoch": 1,
                "repository_id": "qianyi-sun/loom",
                "image_binding_kind": (ACCEPTANCE.environment_registry.WORKER_IMAGE_BINDING_KIND),
                "image_binding_version": (
                    ACCEPTANCE.environment_registry.WORKER_IMAGE_BINDING_VERSION
                ),
                "candidate_sha": candidate["sha"],
                "candidate_tree": candidate["tree"],
                "bundle_sha256": bundle_sha256,
                "bundle_size": 1024,
                "bundle_path": str(
                    ACCEPTANCE.environment_registry.SYSTEM_CANDIDATE_ROOT
                    / candidate_id
                    / "candidate.bundle"
                ),
                "image_digests": image_digests,
                "image_archives": {
                    architecture: {
                        **binding,
                        "path": str(
                            ACCEPTANCE.environment_registry.worker_image_archive_path(
                                ACCEPTANCE.environment_registry.SYSTEM_CANDIDATE_ROOT,
                                candidate_id,
                                architecture,
                            ),
                        ),
                    }
                    for architecture, binding in image_archive_bindings.items()
                },
                "imported_at": _iso(0),
            },
        )
        deployment_id = f"dep-{index:032x}"
        applied_registry_payload_sha256 = f"{index + 5:x}" * 64
        finalization_unsigned = {
            "deployment_id": deployment_id,
            "env_id": env_id,
            "principal_id": principal_id,
            "candidate_id": candidate_id,
            "candidate_sha": candidate["sha"],
            "candidate_tree": candidate["tree"],
            "applied_resource_generation": 2,
            "applied_registry_generation": 41,
            "applied_registry_payload_sha256": applied_registry_payload_sha256,
            "capacity_finalize_receipt_sha256": "a" * 64,
            "capacity_finalize_check_receipt_sha256": "b" * 64,
            "runtime_reconcile_receipt_sha256": "c" * 64,
            "runtime_prepare_check_receipt_sha256": "d" * 64,
            "acceptance_probe_receipt_sha256": "e" * 64,
            "created_at": _iso(1),
        }
        finalization = {
            **finalization_unsigned,
            "payload_sha256": ACCEPTANCE.environment_registry._digest(
                finalization_unsigned,
            ),
        }
        finalization_rows.append(finalization)
        deployment_rows.append(
            {
                "deployment_id": deployment_id,
                "principal_id": principal_id,
                "env_id": env_id,
                "candidate_id": candidate_id,
                "expected_resource_generation": 1,
                "applied_resource_generation": 2,
                "applied_registry_generation": 41,
                "applied_registry_payload_sha256": applied_registry_payload_sha256,
                "finalization_payload_sha256": finalization["payload_sha256"],
                "worker_runtime_bindings": worker_runtime_bindings_from_archives(
                    candidate_id=candidate_id,
                    image_archives=image_archive_bindings,
                ),
                "phase": "committed",
                "previous_candidate_id": None,
                "request_digest": str(index + 4) * 64,
                "created_at": _iso(0),
                "updated_at": _iso(1),
            },
        )
    candidate_rows.sort(key=lambda candidate: candidate["candidate_id"])
    deployment_rows.sort(key=lambda deployment: deployment["deployment_id"])
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.registry-snapshot",
        "generation": 42,
        "environments": environments,
        "candidates": candidate_rows,
        "deployments": deployment_rows,
        "deployment_finalizations": finalization_rows,
    }
    return {
        **unsigned,
        "payload_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_bytes(unsigned),
        ).hexdigest(),
    }


def _registry_snapshot(
    sandboxes: tuple[str, ...] = FIXTURE_SANDBOXES,
) -> dict[str, Any]:
    return ACCEPTANCE._acceptance_registry_snapshot(
        _source_registry_snapshot(sandboxes),
    )


def _reseal_source_registry(source: dict[str, Any]) -> None:
    unsigned = {key: value for key, value in source.items() if key != "payload_sha256"}
    source["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(unsigned),
    ).hexdigest()


def _candidate_map(
    sandboxes: tuple[str, ...] = FIXTURE_SANDBOXES,
) -> dict[str, dict[str, str]]:
    identity_chars = (("a", "b"), ("c", "d"), ("e", "f"), ("1", "2"))
    assert len(sandboxes) <= len(identity_chars)
    return {
        sandbox: {
            "sha": identity_chars[index][0] * 40,
            "tree": identity_chars[index][1] * 40,
        }
        for index, sandbox in enumerate(sandboxes)
    }


def _start_session() -> dict[str, Any]:
    return ACCEPTANCE.start_session(
        _candidate_map(),
        registry_snapshot=_source_registry_snapshot(),
        execute=True,
    )


def _run(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _iso(minutes: int, seconds: int = 0) -> str:
    value = datetime(2026, 7, 28, tzinfo=UTC) + timedelta(
        minutes=minutes,
        seconds=seconds,
    )
    return value.isoformat().replace("+00:00", "Z")


PHASE_BOUNDS = {
    "preflight": (0, 1),
    "baseline": (1, 2),
    "multi_candidate_overlap": (2, 3),
    "large_batch_burst": (3, 5),
    "fairness_contention": (5, 35),
    "mixed_non_loom": (35, 37),
    "cancel_cleanup": (37, 38),
    "ttl_cleanup": (38, 39),
    "submit_host_restart": (39, 40),
    "worker_crash": (40, 41),
    "final_drain": (41, 42),
}


def _phase_observed_at(phase: str, offset_seconds: int = 30) -> str:
    return _iso(PHASE_BOUNDS[phase][0], offset_seconds)


def _request_id(index: int) -> str:
    return str(uuid.UUID(int=index))


def _job_name(sandbox: str, candidate_sha: str, node: str) -> str:
    return f"accept-{sandbox}-{candidate_sha[:12]}-{node}"[:128]


def _runtime_receipts(
    sandbox: str,
    candidate: str,
    tree: str,
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    sandbox_index = int(candidate[0], 16) + 1
    previous: str | None = None
    for generation, minute in enumerate((0, 10, 20, 30, 40), start=1):
        collected_at = _iso(minute)
        expires_at = _iso(minute + 15)
        fleet_nodes = ACCEPTANCE.RUNTIME_FLEET_INFRASTRUCTURE_NODES
        fleet_unsigned = {
            "schema_version": 1,
            "sandbox": sandbox,
            "candidate_sha": candidate,
            "generated_at": collected_at,
            "expires_at": expires_at,
            "eligible_nodes": list(fleet_nodes),
            "bundle_generation": {"candidate_sha": candidate},
            "server": {
                "node": "oldlab-2",
                "unit_active": True,
                "active_candidate_sha": candidate,
            },
            "nodes": {node: {"candidate_sha": candidate} for node in fleet_nodes},
        }
        fleet_proof = dict(fleet_unsigned)
        fleet_proof["payload_sha256"] = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    fleet_unsigned,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode(),
            ).hexdigest()
        )
        domains = {
            domain: {
                "manifest_path": (
                    f"/var/lib/loom-developer-domain-attestations/"
                    f"{sandbox}/{candidate}/{domain}.json"
                ),
                "signature_path": (
                    f"/var/lib/loom-developer-domain-attestations/"
                    f"{sandbox}/{candidate}/{domain}.sig"
                ),
                "payload_sha256": f"{sandbox_index * 100 + generation:064x}",
                "signature_sha256": f"{sandbox_index * 1000 + generation:064x}",
                "key_id": f"{sandbox_index * 10000 + generation:064x}",
                "generation": sandbox_index * 100 + generation,
                "published_at": collected_at,
                "expires_at": expires_at,
            }
            for domain in ACCEPTANCE.POOLS
        }
        combined_unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-combined-activation",
            "sandbox": sandbox,
            "candidate_sha": candidate,
            "candidate_tree": tree,
            "collector": {
                "hostname": ACCEPTANCE.SUBMIT_HOST,
                "collected_at": collected_at,
                "expires_at": expires_at,
            },
            "fleet_attestation": {
                "path": (
                    "/var/lib/loom-developer-sandbox-links/attestations/"
                    f"{sandbox}/{candidate}/fleet.json"
                ),
                "payload_sha256": fleet_proof["payload_sha256"],
                "generated_at": collected_at,
                "expires_at": expires_at,
            },
            "domains": domains,
        }
        combined = dict(combined_unsigned)
        combined["payload_sha256"] = hashlib.sha256(
            json.dumps(
                combined_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
        wrapper_unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-attestation-renewal",
            "sandbox": sandbox,
            "candidate_sha": candidate,
            "candidate_tree": tree,
            "renewal_generation": generation,
            "previous_payload_sha256": previous,
            "collected_at": collected_at,
            "expires_at": expires_at,
            "domain_generations": {
                domain: domains[domain]["generation"] for domain in ACCEPTANCE.POOLS
            },
            "fleet_attestation": fleet_proof,
            "combined_receipt": combined,
        }
        digest = hashlib.sha256(
            json.dumps(
                wrapper_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
        receipts.append(
            {
                "sandbox": sandbox,
                "candidate_sha": candidate,
                "candidate_tree": tree,
                "path": str(
                    ACCEPTANCE.RUNTIME_ATTESTATION_ROOT
                    / sandbox
                    / candidate
                    / "renewals"
                    / f"{generation:020d}-{digest}.json",
                ),
                "renewal_generation": generation,
                "previous_payload_sha256": previous,
                "collected_at": collected_at,
                "expires_at": expires_at,
                "payload_sha256": digest,
                "domain_generations": wrapper_unsigned["domain_generations"],
                "_wrapper": {**wrapper_unsigned, "payload_sha256": digest},
            },
        )
        previous = digest
    return receipts


def _patch_live_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ACCEPTANCE, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(
        ACCEPTANCE,
        "RUNTIME_ATTESTATION_ROOT",
        tmp_path / "runtime-attestations",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "CAPACITY_OBSERVATION_ROOT",
        tmp_path / "authority/capacity",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "SERVICE_STATE_ROOT",
        tmp_path / "authority/services",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "LIVE_AUTHORITY_ROOT",
        tmp_path / "authority/live",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "PROMOTION_AUTHORITY_RECEIPT",
        tmp_path / "authority/promotion/promotion.json",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "PLATFORM_HEALTH_AUTHORITY_ROOT",
        tmp_path / "authority/platform-health",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "STAGING_PRESSURE_PUBLISHED_ROOT",
        tmp_path / "authority/staging-pressure",
    )
    monkeypatch.setattr(
        ACCEPTANCE,
        "STAGING_PRESSURE_PUBLIC_KEY",
        tmp_path / "authority/staging-pressure-key/authority-public.pem",
    )
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_UID", os.getuid())
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_GID", os.getgid())
    monkeypatch.setattr(ACCEPTANCE.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        ACCEPTANCE.socket,
        "gethostname",
        lambda: f"{ACCEPTANCE.SUBMIT_HOST}.internal",
    )


def _write_authority_json(path: Path, root: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    current = path.parent
    while True:
        if current == root or root in current.parents:
            current.chmod(0o700)
        if current == root:
            break
        current = current.parent
    path.write_bytes(ACCEPTANCE._canonical_bytes(payload))
    path.chmod(0o600)


def _write_authority_bytes(path: Path, root: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    current = path.parent
    while True:
        if current == root or root in current.parents:
            current.chmod(0o700)
        if current == root:
            break
        current = current.parent
    path.write_bytes(payload)
    path.chmod(0o600)


def _write_overlap_authority_sources(
    evidence: dict[str, Any],
    pool: str,
    observation: dict[str, Any],
) -> tuple[Path, Path, Path]:
    sandbox = observation["sandbox"]
    sample = next(
        row
        for row in evidence["capacity_samples"]
        if row["phase"] == "multi_candidate_overlap"
        and row["sandbox"] == sandbox
        and row["pool"] == pool
    )
    capacity_unsigned = {
        "sandbox": sandbox,
        "pool_name": pool,
        "candidate_sha": observation["candidate_sha"],
        "request_id": sample["request_id"],
        "lease_epoch": sample["lease_epoch"],
        "capacity_lease_state": "active",
        "observed_at": sample["observed_at"],
        "observation_sequence": sample["observation_sequence"],
        "pending_slots": sample["pending_slots"],
        "active_slots": sample["active_slots"],
        "draining_slots": sample["draining_slots"],
        "terminal_slots": sample["terminal_slots"],
    }
    capacity = {
        **capacity_unsigned,
        "payload_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_digest_bytes(capacity_unsigned),
        ).hexdigest(),
    }
    capacity_document = [capacity]
    sandbox_state = {
        "schema_version": 1,
        "sandbox": sandbox,
        "compose_project": f"loom-sandbox-{sandbox}",
        "candidate_sha": observation["candidate_sha"],
        "candidate_tree": observation["candidate_tree"],
        "source_repo": "/srv/loom/source",
        "updated_at": observation["observed_at"],
    }
    live_observation = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.live-overlap-observation",
        "source_host": ACCEPTANCE.POOL_AUTHORITY_HOSTS[pool],
        "observed_at": observation["observed_at"],
        "sandbox": sandbox,
        "pool": pool,
        "candidate_sha": observation["candidate_sha"],
        "candidate_tree": observation["candidate_tree"],
        "capacity_observation_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_bytes(capacity_document),
        ).hexdigest(),
        "sandbox_state_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_bytes(sandbox_state),
        ).hexdigest(),
        "capacity_sample": sample,
        "job_readback": observation["job_readback"],
        "service_readback": observation["service_readback"],
    }
    paths = ACCEPTANCE._overlap_source_paths(
        sandbox=sandbox,
        pool=pool,
        candidate_sha=observation["candidate_sha"],
        job_id=observation["job_id"],
    )
    _write_authority_json(
        paths[0],
        ACCEPTANCE.CAPACITY_OBSERVATION_ROOT,
        capacity_document,
    )
    _write_authority_json(
        paths[1],
        ACCEPTANCE.SERVICE_STATE_ROOT,
        sandbox_state,
    )
    _write_authority_json(
        paths[2],
        ACCEPTANCE.LIVE_AUTHORITY_ROOT,
        live_observation,
    )
    return paths


def _staging_pressure_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    authority_session_id = "00000000-0000-0000-0000-000000000001"
    acceptance_session_id = evidence["session"]["id"]
    promotion = evidence["promotion_candidate"]
    owned_job = {
        "registry_id": "00000000-0000-0000-0000-000000000006",
        "job_id": "12345",
        "worker_id": "00000000-0000-0000-0000-000000000007",
        "compose_project": "loom-pressure-acceptance",
        "sandbox_identity": "qianyi",
        "candidate_sha": promotion["sha"],
        "state": "running",
        "pending_reason": None,
        "acceptance_owned": True,
    }
    peer = {
        "job_id": "99999",
        "user": "researcher",
        "account": "research",
        "qos": "normal",
        "state": "RUNNING",
        "nodes": "trt-gb10-2",
        "name": "peer",
    }
    owned_slurm = {
        "job_id": "12345",
        "user": "loom-staging-worker",
        "account": "loom-staging",
        "qos": "loom-staging",
        "state": "RUNNING",
        "nodes": "trt-gb10-1",
        "name": "loom-pressure",
    }

    def snapshot(phase: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": 1,
            "kind": "loom.staging-pressure-reclaim.observe-result",
            "submit_host": "trt-gb10-1",
            "environment": "staging",
            "pool": "gb10",
            "partition": "gb10",
            "account": "loom-staging",
            "qos": "loom-staging",
            "phase": phase,
            "session_id": authority_session_id,
            "acceptance_session_id": acceptance_session_id,
            "candidate_sha": promotion["sha"],
            "candidate_tree": promotion["tree"],
            "observed_at": _iso(42, 20 if phase == "before" else 25),
            "jobs": (
                sorted([owned_slurm, peer], key=lambda row: row["job_id"])
                if phase == "before"
                else [peer]
            ),
        }
        payload["snapshot_sha256"] = hashlib.sha256(
            ACCEPTANCE._canonical_bytes(payload),
        ).hexdigest()
        return payload

    authority_receipt = {
        "schema_version": 1,
        "kind": "loom.staging-pressure-reclaim.receipt",
        "environment": "staging",
        "pool": "gb10",
        "partition": "gb10",
        "source_host": ACCEPTANCE.STAGING_PRESSURE_SOURCE_HOST,
        "submit_host": "trt-gb10-1",
        "sequence": 1,
        "session_id": authority_session_id,
        "acceptance_session_id": acceptance_session_id,
        "session_sha256": "2" * 64,
        "candidate_sha": promotion["sha"],
        "candidate_tree": promotion["tree"],
        "issued_at": _iso(42, 30),
        "evidence": {
            "registry_before": [owned_job],
            "interrupted_trial_before": {
                "id": "00000000-0000-0000-0000-000000000005",
                "team_id": "00000000-0000-0000-0000-000000000002",
                "task_id": "pressure-interrupted",
                "state": "running",
                "failure_reason": None,
                "attempt_count": 1,
            },
            "claim_probe_before": {
                "id": "00000000-0000-0000-0000-000000000003",
                "team_id": "00000000-0000-0000-0000-000000000002",
                "task_id": "pressure-claim-probe",
                "state": "queued",
                "failure_reason": None,
                "attempt_count": 0,
            },
            "slurm_before": snapshot("before"),
            "foreign_peer_snapshot": [peer],
            "pressure_on": {
                "action": "draining",
                "actuator": "slurm",
                "environment": "staging",
                "pool_name": "gb10",
                "has_pressure": True,
                "new_staging_claims_allowed": False,
                "drain_intent_active": True,
                "grace_action": "cancel_retryable",
            },
            "claim_fence": {"status": 204, "trial_id": None},
            "registry_terminal": [
                {
                    **owned_job,
                    "state": "cancelled",
                    "pending_reason": "cancelled by prod-pressure reclaim",
                },
            ],
            "interrupted_trial_retryable": {
                "id": "00000000-0000-0000-0000-000000000005",
                "team_id": "00000000-0000-0000-0000-000000000002",
                "task_id": "pressure-interrupted",
                "state": "queued",
                "failure_reason": "prod_capacity_pressure",
                "attempt_count": 1,
            },
            "slurm_during": snapshot("during"),
            "pressure_off": {
                "action": "recovered",
                "actuator": "slurm",
                "environment": "staging",
                "pool_name": "gb10",
                "has_pressure": False,
                "new_staging_claims_allowed": True,
                "drain_intent_active": False,
                "grace_action": "none",
            },
            "claim_recovered": {
                "trial_id": "00000000-0000-0000-0000-000000000003",
                "state": "claimed",
            },
            "claim_probe_requeued": {
                "id": "00000000-0000-0000-0000-000000000003",
                "team_id": "00000000-0000-0000-0000-000000000002",
                "task_id": "pressure-claim-probe",
                "state": "queued",
                "failure_reason": "node_setup_health",
                "attempt_count": 0,
            },
            "slurm_after": snapshot("after"),
            "foreign_peer_zero_impact": True,
        },
    }
    return {
        "authority_evidence": authority_receipt,
        "trusted_receipt": {
            "receipt_sha256": "0" * 64,
            "authority_session_id": authority_session_id,
            "authority_receipt_sha256": hashlib.sha256(
                ACCEPTANCE._canonical_bytes(authority_receipt),
            ).hexdigest(),
            "authority_signature_sha256": "3" * 64,
            "authority_key_id": "4" * 64,
            "sequence": 1,
            "source_host": ACCEPTANCE.STAGING_PRESSURE_SOURCE_HOST,
            "observed_at": authority_receipt["issued_at"],
        },
    }


def _record_trusted_receipts(
    session_id: str,
    evidence: dict[str, Any],
) -> None:
    for window in evidence["overlap_windows"]:
        pool = window["pool"]
        for observation in window["observations"]:
            sandbox = observation["sandbox"]
            _write_overlap_authority_sources(evidence, pool, observation)
            receipt = ACCEPTANCE.record_overlap_receipt(
                session_id,
                sandbox,
                pool,
                observation["job_id"],
                execute=True,
            )
            observation["trusted_receipt"] = {
                "sequence": receipt["sequence"],
                "receipt_sha256": receipt["receipt_sha256"],
            }

    promotion = evidence["promotion_candidate"]
    promotion_reference = promotion["trusted_receipt"]
    authority = {
        "schema_version": 1,
        "kind": "loom.staging-rollout.acceptance",
        "source_host": ACCEPTANCE.PROMOTION_SOURCE_HOST,
        "rollout_id": promotion_reference["rollout_id"],
        "candidate_sha": promotion["sha"],
        "candidate_tree": promotion["tree"],
        "result": "pass",
        "observed_at": promotion_reference["observed_at"],
    }
    _write_authority_json(
        ACCEPTANCE.PROMOTION_AUTHORITY_RECEIPT,
        ACCEPTANCE.PROMOTION_AUTHORITY_RECEIPT.parent,
        authority,
    )
    receipt = ACCEPTANCE.record_promotion_receipt(session_id, execute=True)
    promotion["trusted_receipt"] = {
        "receipt_sha256": receipt["receipt_sha256"],
        "source_host": receipt["source_host"],
        "rollout_id": receipt["rollout_id"],
        "result": receipt["result"],
        "observed_at": receipt["observed_at"],
    }

    pressure = _staging_pressure_evidence(evidence)
    authority_receipt = pressure["authority_evidence"]
    authority_session_id = authority_receipt["session_id"]
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    _write_authority_bytes(
        ACCEPTANCE.STAGING_PRESSURE_PUBLIC_KEY,
        ACCEPTANCE.STAGING_PRESSURE_PUBLIC_KEY.parent,
        public_key,
    )
    signature_bytes = private_key.sign(
        ACCEPTANCE._canonical_bytes(authority_receipt),
    )
    signature = {
        "schema_version": 1,
        "kind": "loom.staging-pressure-reclaim.receipt.signature",
        "session_id": authority_session_id,
        "receipt_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_bytes(authority_receipt),
        ).hexdigest(),
        "key_id": hashlib.sha256(public_key).hexdigest(),
        "signature_base64": base64.b64encode(signature_bytes).decode("ascii"),
        "signature_sha256": hashlib.sha256(signature_bytes).hexdigest(),
    }
    published = {
        "schema_version": 1,
        "kind": "loom.staging-pressure-reclaim.published-receipt",
        "acceptance_session_id": session_id,
        "authority_session_id": authority_session_id,
        "candidate_sha": promotion["sha"],
        "candidate_tree": promotion["tree"],
        "source_host": ACCEPTANCE.STAGING_PRESSURE_SOURCE_HOST,
        "published_at": authority_receipt["issued_at"],
        "receipt": authority_receipt,
        "signature": signature,
    }
    authority_path = ACCEPTANCE._pressure_authority_path(
        session_id,
        authority_session_id,
    )
    _write_authority_json(
        authority_path,
        ACCEPTANCE.STAGING_PRESSURE_PUBLISHED_ROOT,
        published,
    )
    pressure_receipt = ACCEPTANCE.record_staging_pressure_receipt(
        session_id,
        authority_session_id,
        execute=True,
    )
    evidence["staging_pressure_reclaim"] = {
        "authority_evidence": authority_receipt,
        "trusted_receipt": {
            "receipt_sha256": pressure_receipt["receipt_sha256"],
            "authority_session_id": pressure_receipt["authority_session_id"],
            "authority_receipt_sha256": pressure_receipt["authority_receipt_sha256"],
            "authority_signature_sha256": pressure_receipt["authority_signature_sha256"],
            "authority_key_id": pressure_receipt["authority_key_id"],
            "sequence": pressure_receipt["sequence"],
            "source_host": pressure_receipt["source_host"],
            "observed_at": pressure_receipt["observed_at"],
        },
    }

    platform_authority = _platform_health_authority(evidence)
    platform_path = (
        ACCEPTANCE.PLATFORM_HEALTH_AUTHORITY_ROOT / "sessions" / session_id / "evidence.json"
    )
    _write_authority_json(
        platform_path,
        ACCEPTANCE.PLATFORM_HEALTH_AUTHORITY_ROOT,
        platform_authority,
    )
    platform_receipt = ACCEPTANCE.record_platform_health_receipt(
        session_id,
        execute=True,
    )
    evidence["platform_health"] = {
        "authority_evidence": platform_authority,
        "trusted_receipt": {
            "receipt_sha256": platform_receipt["receipt_sha256"],
            "authority_payload_sha256": platform_receipt["authority_payload_sha256"],
            "source_host": platform_receipt["source_host"],
            "observed_at": platform_receipt["observed_at"],
        },
    }


def _platform_health_authority(evidence: Mapping[str, Any]) -> dict[str, Any]:
    registry_environments = ACCEPTANCE._registry_environments(
        evidence["registry_snapshot"],
    )
    policy_contracts = {
        pool: ACCEPTANCE._platform_policy_contract(pool) for pool in ACCEPTANCE.POOLS
    }
    mixed_jobs: list[dict[str, Any]] = []
    for row in evidence["runtime_envelopes"]:
        pool = row["pool"]
        policy = policy_contracts[pool][0]
        job_id = row["job_id"]
        job_path = row["cgroup"]["job_path"]
        environment = registry_environments[row["sandbox"]]
        compose_project = f"loom-{row['sandbox']}-{job_id}"
        compose_networks = [f"{compose_project}_default"]
        containers = [
            {
                "container_id": container["container_id"],
                "role": container["role"],
                "sandbox": row["sandbox"],
                "candidate_sha": row["candidate_sha"],
                "job_id": job_id,
                "compose_project": compose_project,
                "identity_labels": {
                    "loom.sandbox": row["sandbox"],
                    "loom.candidate_sha": row["candidate_sha"],
                    "loom.slurm_job_id": job_id,
                    "loom.compose_project": compose_project,
                    "loom.env_id": environment["env_id"],
                    "loom.resource_generation": str(
                        environment["resource_generation"],
                    ),
                    "loom.candidate_id": environment["candidate_id"],
                    "loom.candidate_tree": row["candidate_tree"],
                    "loom.registry_generation": str(
                        evidence["registry_snapshot"]["generation"],
                    ),
                    "loom.registry_payload_sha256": evidence["registry_snapshot"]["payload_sha256"],
                },
                "compose_networks": compose_networks,
                "pid": 2000 + index,
                "cgroup_parent": job_path,
                "observed_cgroup_path": container["observed_cgroup_path"],
                "limits": {
                    "cpu_cores": policy["container_cpus"],
                    "memory_bytes": policy["container_memory_mib"] * 1024**2,
                    "pids": policy["container_pids"],
                    "gpu_count": 0,
                    "gpu_ids": [],
                },
            }
            for index, container in enumerate(row["containers"], start=1)
        ]
        if policy["gpu_tres"]:
            containers[0]["limits"]["gpu_count"] = 1
            containers[0]["limits"]["gpu_ids"] = ["GPU-0"]
        allocation = {
            "cpu_cores": policy["requested_cpus"],
            "memory_bytes": policy["requested_memory_mib"] * 1024**2,
            "pids": policy["job_pids_max"],
            "gpu_count": 1 if policy["gpu_tres"] else 0,
            "tres": row["allocation"]["tres"],
            "exclusive": False,
        }
        platform_cgroup = copy.deepcopy(row["cgroup"])
        platform_cgroup["cpu_cores_max"] = allocation["cpu_cores"]
        platform_cgroup["memory_bytes_max"] = allocation["memory_bytes"]
        platform_cgroup["pids_max"] = allocation["pids"]
        mixed_jobs.append(
            {
                "job_id": job_id,
                "job_start_time": row["job_start_time"],
                "job_name": (f"loom-{row['sandbox']}-{row['candidate_sha'][:12]}-{row['node']}"),
                "sandbox": row["sandbox"],
                "env_id": environment["env_id"],
                "resource_generation": environment["resource_generation"],
                "candidate_id": environment["candidate_id"],
                "candidate_sha": row["candidate_sha"],
                "candidate_tree": row["candidate_tree"],
                "registry_generation": evidence["registry_snapshot"]["generation"],
                "registry_payload_sha256": evidence["registry_snapshot"]["payload_sha256"],
                "account": row["account"],
                "qos": registry_environments[row["sandbox"]]["slurm_qos"],
                "user": registry_environments[row["sandbox"]]["slurm_user"],
                "node": row["node"],
                "state": "RUNNING",
                "allocation": allocation,
                "compose_project": compose_project,
                "compose_networks": compose_networks,
                "cgroup": platform_cgroup,
                "containers": containers,
                "aggregate_limits": {
                    "cpu_cores": len(containers) * policy["container_cpus"],
                    "memory_bytes": (len(containers) * policy["container_memory_mib"] * 1024**2),
                    "pids": len(containers) * policy["container_pids"],
                    "gpu_count": 1 if policy["gpu_tres"] else 0,
                },
            },
        )
    oldlab_capacity = {
        **policy_contracts["oldlab"][0],
        "minimum_node_cpu_cores": 24,
        "minimum_node_memory_bytes": 120 * 1024**3,
        "reserved_cpu_cores_per_node": 4,
        "reserved_memory_mib_per_node": 16384,
    }
    gb10_capacity = {
        **policy_contracts["gb10"][0],
        "minimum_node_cpu_cores": 20,
        "minimum_node_memory_bytes": 115000 * 1024**2,
        "reserved_cpu_cores_per_node": 4,
        "reserved_memory_mib_per_node": 23000,
    }
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": evidence["session"]["id"],
        "registry_snapshot": evidence["registry_snapshot"],
        "candidates": {
            sandbox: {
                "sha": evidence["candidates"][sandbox]["sha"],
                "tree": evidence["candidates"][sandbox]["tree"],
            }
            for sandbox in evidence["candidates"]
        },
        "collector_host": ACCEPTANCE.SUBMIT_HOST,
        "checkpoints": [
            {
                "sequence": sequence,
                "checkpoint": checkpoint,
                "checkpoint_group": (
                    "baseline"
                    if checkpoint == "baseline"
                    else "after"
                    if checkpoint == "final_drain"
                    else "during"
                ),
                "observed_at": _iso(35 + sequence),
                "payload_sha256": f"{sequence:064x}",
            }
            for sequence, checkpoint in enumerate(
                (
                    "baseline",
                    "mixed_non_loom",
                    "cancel_cleanup",
                    "worker_crash",
                    "final_drain",
                ),
                start=1,
            )
        ],
        "mixed_jobs": mixed_jobs,
        "cancelled_jobs": [
            {
                "job_id": "9001",
                "job_name": "loom-qianyi-cancel",
                "node": "trt-eai-oldlab-1",
                "sandbox": "qianyi",
                "candidate_sha": evidence["candidates"]["qianyi"]["sha"],
                "state": "CANCELLED",
            },
        ],
        "crashed_jobs": [
            {
                "job_id": "9002",
                "job_name": "loom-hongjian-crash",
                "node": "trt-eai-oldlab-2",
                "sandbox": "hongjian",
                "candidate_sha": evidence["candidates"]["hongjian"]["sha"],
                "state": "FAILED",
            },
        ],
        "node_intervals": {
            node: {
                "cpu_busy_ratio": 0.3,
                "minimum_cpu_cores_available": 8.0,
                "minimum_memory_bytes_available": 32_000_000_000,
                "read_bytes": 1024,
                "write_bytes": 2048,
            }
            for node in ACCEPTANCE.PLATFORM_HEALTH_NODE_KEYS
        },
        "policy_capacity": {
            "oldlab": oldlab_capacity,
            "gb10": gb10_capacity,
        },
        "oldlab_capacity_recommendation": {
            "schema_version": 1,
            "pool": "oldlab",
            "source": ACCEPTANCE.PLATFORM_POLICY_SOURCES["oldlab"],
            "source_sha256": policy_contracts["oldlab"][1],
            "values": oldlab_capacity,
            "derivation": {
                "method": "installed-shared-capacity-policy-v1",
                "measured_node_count": 5,
                "minimum_observed_node_cpu_cores": 24,
                "minimum_observed_node_memory_bytes": 120 * 1024**3,
                "minimum_observed_free_cpu_cores": 8.0,
                "minimum_observed_free_memory_bytes": 32 * 1024**3,
                "minimum_required_free_cpu_cores": 4,
                "minimum_required_free_memory_bytes": 16 * 1024**3,
                "maximum_allowed_cpu_busy_ratio": 0.85,
                "all_nodes_passed": True,
            },
        },
        "zero_orphans": True,
        "completed_at": _phase_observed_at("final_drain"),
        "expires_at": _iso(PHASE_BOUNDS["final_drain"][0] + 15, 30),
    }
    payload["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(payload),
    ).hexdigest()
    return payload


@pytest.mark.parametrize("attack", ["missing", "short", "long"])
def test_platform_health_authority_requires_exact_bounded_expiry(attack: str) -> None:
    evidence = _evidence()
    authority = _platform_health_authority(evidence)
    if attack == "missing":
        del authority["expires_at"]
    elif attack == "short":
        authority["expires_at"] = _iso(PHASE_BOUNDS["final_drain"][0] + 14, 30)
    else:
        authority["expires_at"] = _iso(PHASE_BOUNDS["final_drain"][0] + 16, 30)
    unsigned = {key: value for key, value in authority.items() if key != "payload_sha256"}
    authority["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(unsigned),
    ).hexdigest()

    with pytest.raises(ACCEPTANCE.AcceptanceError):
        ACCEPTANCE._validate_platform_health_authority(
            authority,
            session_id=evidence["session"]["id"],
            registry_snapshot=evidence["registry_snapshot"],
            candidates=evidence["candidates"],
        )


@pytest.mark.parametrize(
    "attack",
    [
        "source_digest",
        "capacity_extra",
        "recommendation_extra",
        "slurm_job_id",
        "forged_parent",
        "compose_reuse",
        "network_reuse",
    ],
)
def test_platform_health_authority_rejects_policy_and_isolation_drift(
    attack: str,
) -> None:
    evidence = _evidence()
    platform = _platform_health_authority(evidence)
    if attack == "source_digest":
        platform["oldlab_capacity_recommendation"]["source_sha256"] = "f" * 64
    elif attack == "capacity_extra":
        platform["policy_capacity"]["oldlab"]["unexpected"] = 1
    elif attack == "recommendation_extra":
        platform["oldlab_capacity_recommendation"]["unexpected"] = True
    elif attack == "slurm_job_id":
        platform["mixed_jobs"][0]["cgroup"]["slurm_job_id"] = "999"
    elif attack == "forged_parent":
        platform["mixed_jobs"][0]["cgroup"]["job_path"] = "/system.slice/slurmstepd.scope/job_999"
    elif attack == "compose_reuse":
        platform["mixed_jobs"][1]["compose_project"] = platform["mixed_jobs"][0]["compose_project"]
    else:
        platform["mixed_jobs"][1]["compose_networks"] = platform["mixed_jobs"][0][
            "compose_networks"
        ]
    unsigned = {key: value for key, value in platform.items() if key != "payload_sha256"}
    platform["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(unsigned),
    ).hexdigest()

    with pytest.raises(ACCEPTANCE.AcceptanceError):
        ACCEPTANCE._validate_platform_health_authority(
            platform,
            session_id=evidence["session"]["id"],
            registry_snapshot=evidence["registry_snapshot"],
            candidates=evidence["candidates"],
        )


def test_platform_health_policy_contract_rejects_extra_source_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for relative in (
        "deploy/developer-sandboxes/platform-health-authority.toml",
        *ACCEPTANCE.PLATFORM_POLICY_SOURCES.values(),
    ):
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO_ROOT / relative, destination)
    oldlab = tmp_path / ACCEPTANCE.PLATFORM_POLICY_SOURCES["oldlab"]
    oldlab.write_text(
        oldlab.read_text(encoding="utf-8") + "\nunexpected_policy_drift = true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ACCEPTANCE, "REPO_ROOT", tmp_path)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="invalid"):
        ACCEPTANCE._platform_policy_contract("oldlab")


def _runtime_envelope(
    sandbox: str,
    pool: str,
    index: int,
    candidate: str,
    tree: str,
    account: str,
    qos: str,
) -> dict[str, Any]:
    job_id = str(1000 + index)
    job_start_time = f"2026-07-30T12:{index:02d}:00"
    job_path = f"/system.slice/slurmstepd.scope/job_{job_id}"
    if pool == "gb10":
        allocation = {
            "cpu_cores": 16,
            "memory_bytes": 92_000_000_000,
            "pids": 65_536,
            "gpu_count": 1,
            "tres": "cpu=16,mem=92000M,gres/gpu=1",
            "exclusive": False,
        }
        node = "trt-gb10-1"
    else:
        allocation = {
            "cpu_cores": 8,
            "memory_bytes": 32_000_000_000,
            "pids": 32_768,
            "gpu_count": 0,
            "tres": "cpu=8,mem=32000M",
            "exclusive": False,
        }
        node = "trt-eai-oldlab-1"
    containers = []
    for role_index, role in enumerate(ACCEPTANCE.CONTAINER_ROLES, start=1):
        limits = {
            "cpu_cores": 1,
            "memory_bytes": 1_000_000_000,
            "pids": 128,
        }
        containers.append(
            {
                "role": role,
                "container_id": f"{index * 16 + role_index:012x}",
                "cgroup_parent": job_path,
                "observed_cgroup_path": (f"{job_path}/docker/{index * 16 + role_index:012x}"),
                "limits": limits,
                "observed_limits": limits.copy(),
                "gpu_ids": ["GPU-0"] if pool == "gb10" and role == "trial" else [],
            },
        )
    return {
        "sandbox": sandbox,
        "pool": pool,
        "phase": "mixed_non_loom",
        "candidate_sha": candidate,
        "candidate_tree": tree,
        "observed_at": _phase_observed_at("mixed_non_loom"),
        "job_id": job_id,
        "job_start_time": job_start_time,
        "node": node,
        "account": account,
        "qos": qos,
        "allocation": allocation,
        "cgroup": {
            "layout_version": "cgroupfs-job-v1",
            "job_path": job_path,
            "container_parent": job_path,
            "slurm_job_id": job_id,
            "slurm_pid_cgroup_paths": [f"{job_path}/step_batch"],
            "controllers": ["cpu", "memory", "pids"],
            "delegated_controllers": ["cpu", "memory", "pids"],
            "delegated": True,
            "cpu_cores_max": allocation["cpu_cores"],
            "memory_bytes_max": allocation["memory_bytes"],
            "pids_max": allocation["pids"],
            "pids_current": 16,
            "systemd_slice_receipt": None,
            "systemd_slice_live": None,
        },
        "containers": containers,
    }


def _refresh_systemd_slice_receipt(cgroup: dict[str, Any]) -> None:
    receipt = cgroup["systemd_slice_receipt"]
    identity = {
        "cluster": receipt["cluster"],
        "node": receipt["node_list"].lower(),
        "job_id": receipt["job_id"],
        "job_start_time": receipt["job_start_time"],
        "account": receipt["account"],
        "env_id": receipt["env_id"],
        "resource_generation": receipt["resource_generation"],
        "runtime_id": receipt["runtime_id"],
        "candidate_id": receipt["candidate_id"],
        "candidate_sha": receipt["candidate_sha"],
        "candidate_tree": receipt["candidate_tree"],
    }
    identity_digest = hashlib.sha256(
        ACCEPTANCE._canonical_digest_bytes(identity),
    ).hexdigest()
    unit = f"loom-job-{receipt['job_id']}-{identity_digest[:40]}.slice"
    receipt["systemd_slice"] = unit
    receipt["slice_identity_sha256"] = identity_digest
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    receipt["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_digest_bytes(unsigned),
    ).hexdigest()
    cgroup["container_parent"] = unit
    cgroup["systemd_slice_live"]["path"] = (
        f"/loom.slice/loom-job.slice/loom-job-{receipt['job_id']}.slice/{unit}"
    )


def _use_systemd_mirror(
    job: dict[str, Any],
    environment: Mapping[str, Any],
    *,
    pool: str,
) -> None:
    cgroup = job["cgroup"]
    allocation = job["allocation"]
    cgroup["layout_version"] = "systemd-mirror-v1"
    receipt = {
        "schema_version": 1,
        "kind": "loom.slurm-systemd-slice-receipt",
        "systemd_slice": "",
        "slice_identity_sha256": "",
        "unit_sha256": hashlib.sha256(b"fixture systemd slice unit").hexdigest(),
        "job_id": job["job_id"],
        "job_start_time": job["job_start_time"],
        "cluster": "trt-oldlab" if pool == "oldlab" else "trt-gb10",
        "node_list": job["node"],
        "account": job["account"],
        "env_id": environment["env_id"],
        "resource_generation": environment["resource_generation"],
        "runtime_id": job["sandbox"],
        "candidate_id": environment["candidate_id"],
        "candidate_sha": job["candidate_sha"],
        "candidate_tree": job["candidate_tree"],
        "cpu_max": f"{allocation['cpu_cores'] * 100000} 100000",
        "memory_max": str(allocation["memory_bytes"]),
        "memory_swap_max_source": "max",
        "memory_swap_max_effective": "0",
        "pids_max": str(allocation["pids"]),
        "cpuset_cpus": "0-7" if pool == "oldlab" else "0-15",
        "cpuset_mems": "0",
        "gpu_tres": allocation["tres"] if allocation["gpu_count"] else "not-required",
        "gpu_detail": "gpu(IDX:0)" if allocation["gpu_count"] else "not-required",
        "payload_sha256": "",
    }
    cgroup["systemd_slice_receipt"] = receipt
    cgroup["systemd_slice_live"] = {
        "path": "",
        "cpu_cores_max": allocation["cpu_cores"],
        "memory_bytes_max": allocation["memory_bytes"],
        "memory_swap_bytes_max": 0,
        "pids_max": allocation["pids"],
        "cpuset_cpus": receipt["cpuset_cpus"],
        "cpuset_mems": receipt["cpuset_mems"],
    }
    _refresh_systemd_slice_receipt(cgroup)
    if allocation["gpu_count"]:
        for container in job["containers"]:
            if container.get("gpu_ids"):
                container["gpu_ids"] = ["0"]
            if container.get("limits", {}).get("gpu_ids"):
                container["limits"]["gpu_ids"] = ["0"]
    _rebind_systemd_containers(job)


def _rebind_systemd_containers(job: dict[str, Any]) -> None:
    cgroup = job["cgroup"]
    live_path = cgroup["systemd_slice_live"]["path"]
    for container in job["containers"]:
        container["cgroup_parent"] = cgroup["container_parent"]
        container["observed_cgroup_path"] = f"{live_path}/docker-{container['container_id']}.scope"


def _evidence(
    sandboxes: tuple[str, ...] = FIXTURE_SANDBOXES,
) -> dict[str, Any]:
    candidate_ids = _candidate_map(sandboxes)
    registry_snapshot = _registry_snapshot(sandboxes)
    registry_environments = ACCEPTANCE._registry_environments(registry_snapshot)
    phase_checkpoints = ACCEPTANCE._phase_checkpoints(sandboxes)
    session_id = "1" * 32
    candidates: dict[str, Any] = {}
    for sandbox in sandboxes:
        identity = candidate_ids[sandbox]
        receipts = _runtime_receipts(sandbox, identity["sha"], identity["tree"])
        for receipt in receipts:
            receipt.pop("_wrapper")
        candidates[sandbox] = {**identity, "runtime_receipts": receipts}

    phases = []
    for index, (phase, sandbox) in enumerate(phase_checkpoints):
        phase_start, phase_finish = PHASE_BOUNDS[phase]
        identity = candidate_ids[sandbox]
        phase_row = {
            "phase": phase,
            "sandbox": sandbox,
            "candidate_sha": identity["sha"],
            "candidate_tree": identity["tree"],
            "started_at": _iso(phase_start),
            "finished_at": _iso(phase_finish),
            "deadline_seconds": (phase_finish - phase_start) * 60,
            "status": "pass",
            "checkpoint_sha256": f"{index + 1:064x}",
        }
        if phase == "mixed_non_loom":
            sandbox_index = sandboxes.index(sandbox)
            phase_row["trial_batches"] = {
                "oldlab": _request_id(500 + sandbox_index * 2),
                "gb10": _request_id(501 + sandbox_index * 2),
            }
        phases.append(phase_row)
    capacity_samples = []
    pair_index = 1
    for sandbox in sandboxes:
        identity = candidate_ids[sandbox]
        for pool in ACCEPTANCE.POOLS:
            for phase_index, phase in enumerate(ACCEPTANCE.CAPACITY_PHASES, start=1):
                final = phase == "final_drain"
                job_id = str(6000 + pair_index * 100 + phase_index)
                node = "trt-eai-oldlab-1" if pool == "oldlab" else "trt-gb10-1"
                allocation = (
                    {
                        "cpu_cores": 8,
                        "memory_bytes": 32_000_000_000,
                        "pids": 32_768,
                        "gpu_count": 0,
                        "tres": "cpu=8,mem=32000M",
                        "exclusive": False,
                    }
                    if pool == "oldlab"
                    else {
                        "cpu_cores": 16,
                        "memory_bytes": 92_000_000_000,
                        "pids": 65_536,
                        "gpu_count": 1,
                        "tres": "cpu=16,mem=92000M,gres/gpu=1",
                        "exclusive": False,
                    }
                )
                capacity_samples.append(
                    {
                        "phase": phase,
                        "observed_at": _phase_observed_at(
                            phase,
                            30 if phase == "multi_candidate_overlap" else pair_index,
                        ),
                        "sandbox": sandbox,
                        "pool": pool,
                        "candidate_sha": identity["sha"],
                        "candidate_tree": identity["tree"],
                        "job_id": job_id,
                        "account": registry_environments[sandbox]["slurm_account"],
                        "qos": registry_environments[sandbox]["slurm_qos"],
                        "user": registry_environments[sandbox]["slurm_user"],
                        "job_name": _job_name(
                            sandbox,
                            identity["sha"],
                            node,
                        ),
                        "node": node,
                        "allocation": allocation,
                        "request_id": _request_id(pair_index),
                        "lease_epoch": 1,
                        "observation_sequence": phase_index,
                        "requested_slots": 0 if final else 4,
                        "granted_slots": 0 if final else 2,
                        "pending_slots": 0 if final else 1,
                        "active_slots": 0 if final else 1,
                        "draining_slots": 0,
                        "terminal_slots": 2 if final else 0,
                    },
                )
            pair_index += 1
    runtime_envelopes = []
    index = 1
    for sandbox in sandboxes:
        identity = candidate_ids[sandbox]
        for pool in ACCEPTANCE.POOLS:
            runtime_envelopes.append(
                _runtime_envelope(
                    sandbox,
                    pool,
                    index,
                    identity["sha"],
                    identity["tree"],
                    registry_environments[sandbox]["slurm_account"],
                    registry_environments[sandbox]["slurm_qos"],
                ),
            )
            index += 1
    fairness = []
    for pool in ACCEPTANCE.POOLS:
        fairness.append(
            {
                "pool": pool,
                "phase": "fairness_contention",
                "started_at": _iso(PHASE_BOUNDS["fairness_contention"][0]),
                "finished_at": _iso(PHASE_BOUNDS["fairness_contention"][1]),
                "window_seconds": 1800,
                "max_grant_wait_seconds": 600,
                "max_grant_skew_ratio": 0.2,
                "participants": [
                    {
                        "sandbox": sandbox,
                        "candidate_sha": candidate_ids[sandbox]["sha"],
                        "candidate_tree": candidate_ids[sandbox]["tree"],
                        "requested_slots": 4,
                        "granted_slots_total": 8,
                        "grant_cycles": 2,
                        "first_grant_wait_seconds": 30,
                        "longest_starvation_seconds": 120,
                        "indefinite_starvation": False,
                    }
                    for sandbox in sandboxes
                ],
            },
        )
    peer_workloads = []
    for pool in ACCEPTANCE.POOLS:
        peer_workloads.append(
            {
                "pool": pool,
                "job_id": "9001" if pool == "oldlab" else "9002",
                "account": "research-peer",
                "baseline": {
                    "observed_at": _phase_observed_at("baseline"),
                    "running_jobs": 1,
                    "completed_jobs": 10,
                    "failed_jobs": 0,
                    "throughput_per_second": 10,
                    "p95_latency_seconds": 1,
                },
                "during": {
                    "observed_at": _phase_observed_at("mixed_non_loom"),
                    "running_jobs": 1,
                    "completed_jobs": 20,
                    "failed_jobs": 0,
                    "throughput_per_second": 9,
                    "p95_latency_seconds": 1.1,
                },
                "after": {
                    "observed_at": _phase_observed_at("final_drain"),
                    "running_jobs": 1,
                    "completed_jobs": 30,
                    "failed_jobs": 0,
                    "throughput_per_second": 10,
                    "p95_latency_seconds": 1,
                },
                "max_throughput_regression_ratio": 0.2,
                "disrupted": False,
            },
        )
    storage_io = [
        {
            "domain": pool,
            "baseline_observed_at": _phase_observed_at("baseline"),
            "minimum_observed_at": _phase_observed_at("mixed_non_loom"),
            "after_observed_at": _phase_observed_at("final_drain"),
            "baseline_free_bytes": 1_000_000_000_000,
            "minimum_free_bytes": 900_000_000_000,
            "after_free_bytes": 950_000_000_000,
            "required_free_bytes": 100_000_000_000,
            "cache_peak_bytes": 10_000_000_000,
            "cache_limit_bytes": 20_000_000_000,
            "read_bytes": 100_000_000_000,
            "read_limit_bytes": 200_000_000_000,
            "write_bytes": 50_000_000_000,
            "write_limit_bytes": 100_000_000_000,
            "io_errors": 0,
            "enospc_events": 0,
        }
        for pool in ACCEPTANCE.POOLS
    ]
    fault_recovery = []
    for index, event in enumerate(ACCEPTANCE.FAULTS, start=1):
        phase = ACCEPTANCE.FAULT_PHASES[event]
        sandbox = sandboxes[(index - 1) % len(sandboxes)]
        fault_recovery.append(
            {
                "event": event,
                "phase": phase,
                "candidate_sha": candidate_ids[sandbox]["sha"],
                "candidate_tree": candidate_ids[sandbox]["tree"],
                "sandbox": sandbox,
                "pool": ACCEPTANCE.POOLS[(index - 1) % 2],
                "request_id": _request_id(100 + index),
                "injected_at": _phase_observed_at(phase, 10),
                "recovered_at": _phase_observed_at(phase, 50),
                "recovery_deadline_seconds": 600,
                "orphan_jobs": 0,
                "orphan_containers": 0,
                "orphan_leases": 0,
                "orphan_trials": 0,
                "retry_attribution": {
                    "interrupted_trials": 2,
                    "retryable_trials": 2,
                    "retried_trials": 2,
                    "duplicate_retries": 0,
                    "lost_trials": 0,
                    "unknown_attribution": 0,
                },
            },
        )
    overlap_windows = []
    for pool_index, pool in enumerate(ACCEPTANCE.POOLS, start=1):
        observations = []
        for sandbox_index, sandbox in enumerate(sandboxes, start=1):
            identity = candidate_ids[sandbox]
            capacity_sample = next(
                sample
                for sample in capacity_samples
                if sample["phase"] == "multi_candidate_overlap"
                and sample["sandbox"] == sandbox
                and sample["pool"] == pool
            )
            job_id = capacity_sample["job_id"]
            node = capacity_sample["node"]
            observed_at = _iso(2, 30)
            service_unit = registry_environments[sandbox]["service_unit"]
            job_name = _job_name(sandbox, identity["sha"], node)
            job_readback = {
                "sandbox": sandbox,
                "pool": pool,
                "candidate_sha": identity["sha"],
                "candidate_tree": identity["tree"],
                "job_id": job_id,
                "account": registry_environments[sandbox]["slurm_account"],
                "qos": registry_environments[sandbox]["slurm_qos"],
                "user": registry_environments[sandbox]["slurm_user"],
                "job_name": job_name,
                "node": node,
                "state": "RUNNING",
                "allocation": capacity_sample["allocation"],
                "observed_at": observed_at,
            }
            service_readback = {
                "sandbox": sandbox,
                "candidate_sha": identity["sha"],
                "candidate_tree": identity["tree"],
                "unit": service_unit,
                "active_state": "active",
                "sub_state": "running",
                "observed_at": observed_at,
            }
            observations.append(
                {
                    "sandbox": sandbox,
                    "candidate_sha": identity["sha"],
                    "candidate_tree": identity["tree"],
                    "active_candidate_sha": identity["sha"],
                    "active_candidate_tree": identity["tree"],
                    "service_unit": service_unit,
                    "service_active": True,
                    "service_readback": service_readback,
                    "service_readback_sha256": hashlib.sha256(
                        ACCEPTANCE._canonical_bytes(service_readback),
                    ).hexdigest(),
                    "job_id": job_id,
                    "job_active": True,
                    "slurm_account": registry_environments[sandbox]["slurm_account"],
                    "slurm_qos": registry_environments[sandbox]["slurm_qos"],
                    "slurm_user": registry_environments[sandbox]["slurm_user"],
                    "job_name": job_name,
                    "job_readback": job_readback,
                    "job_readback_sha256": hashlib.sha256(
                        ACCEPTANCE._canonical_bytes(job_readback),
                    ).hexdigest(),
                    "capacity_binding": {
                        "request_id": capacity_sample["request_id"],
                        "lease_epoch": capacity_sample["lease_epoch"],
                        "observation_sequence": capacity_sample["observation_sequence"],
                        "sample_sha256": hashlib.sha256(
                            ACCEPTANCE._canonical_bytes(capacity_sample),
                        ).hexdigest(),
                    },
                    "trusted_receipt": {
                        "sequence": (pool_index - 1) * len(sandboxes) + sandbox_index,
                        "receipt_sha256": "0" * 64,
                    },
                    "node": node,
                    "active_from": _iso(2, 5),
                    "active_until": _iso(2, 55),
                    "observed_at": observed_at,
                },
            )
        overlap_windows.append(
            {
                "phase": "multi_candidate_overlap",
                "pool": pool,
                "started_at": _iso(2, 10),
                "finished_at": _iso(2, 50),
                "observations": observations,
            },
        )

    bursts = []
    for sandbox_index, sandbox in enumerate(sandboxes, start=1):
        identity = candidate_ids[sandbox]
        for pool_index, pool in enumerate(ACCEPTANCE.POOLS, start=1):
            oldlab = pool == "oldlab"
            nodes = (
                ["trt-eai-oldlab-1", "trt-eai-oldlab-3"] if oldlab else ["trt-gb10-1", "trt-gb10-2"]
            )
            budget = ACCEPTANCE.POOL_SLOT_BUDGETS[pool]
            bursts.append(
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "phase": "large_batch_burst",
                    "candidate_sha": identity["sha"],
                    "candidate_tree": identity["tree"],
                    "started_at": _iso(PHASE_BOUNDS["large_batch_burst"][0]),
                    "finished_at": _iso(PHASE_BOUNDS["large_batch_burst"][1]),
                    "batch_id": _request_id(200 + sandbox_index * 10 + pool_index),
                    "trial_count": 100,
                    "completed_trials": 100,
                    "failed_trials": 0,
                    "cancelled_trials": 0,
                    "duplicate_trial_ids": 0,
                    "requested_slots": budget,
                    "granted_slots": budget,
                    "peak_active_slots": budget,
                    "nodes": nodes,
                    "node_trial_counts": {node: 50 for node in nodes},
                },
            )

    promotion_phase = {
        "phase": "promotion_staging_regression",
        "candidate_sha": "7" * 40,
        "candidate_tree": "8" * 40,
        "started_at": _iso(42),
        "finished_at": _iso(43),
        "status": "pass",
    }
    promotion_checkpoint = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(promotion_phase),
    ).hexdigest()

    evidence = {
        "schema_version": ACCEPTANCE.SCHEMA_VERSION,
        "registry_snapshot": registry_snapshot,
        "candidates": candidates,
        "promotion_candidate": {
            "sha": "7" * 40,
            "tree": "8" * 40,
            "staging_regression": {
                **promotion_phase,
                "checkpoint_sha256": promotion_checkpoint,
            },
            "trusted_receipt": {
                "receipt_sha256": "0" * 64,
                "source_host": ACCEPTANCE.PROMOTION_SOURCE_HOST,
                "rollout_id": "rollout-1023",
                "result": "pass",
                "observed_at": _iso(42, 30),
            },
        },
        "session": {
            "id": session_id,
            "submit_host": ACCEPTANCE.SUBMIT_HOST,
            "execute_acknowledged": True,
            "started_at": _iso(0),
            "completed_at": _iso(42),
            "collected_at": _iso(43),
            "max_collection_lag_seconds": 300,
        },
        "topology": {
            "sandboxes": list(sandboxes),
            "pools": list(ACCEPTANCE.POOLS),
            "infrastructure_nodes": list(ACCEPTANCE.INFRASTRUCTURE_NODES),
            "eligible_nodes": list(ACCEPTANCE.EXPECTED_NODES),
            "excluded_nodes": [],
            "slot_budgets": ACCEPTANCE.POOL_SLOT_BUDGETS.copy(),
            "pending_slot_budgets": ACCEPTANCE.POOL_PENDING_BUDGETS.copy(),
        },
        "state_machine": phases,
        "overlap_windows": overlap_windows,
        "cross_sandbox_negative": [
            {
                "phase": "baseline",
                "source": source,
                "target": target,
                "resource": resource,
                "source_candidate_sha": candidate_ids[source]["sha"],
                "source_candidate_tree": candidate_ids[source]["tree"],
                "target_candidate_sha": candidate_ids[target]["sha"],
                "target_candidate_tree": candidate_ids[target]["tree"],
                "observed_at": _phase_observed_at("baseline"),
                "denied": True,
            }
            for source in sandboxes
            for target in sandboxes
            if source != target
            for resource in ACCEPTANCE.CROSS_SANDBOX_RESOURCES
        ],
        "capacity_samples": capacity_samples,
        "large_batch_bursts": bursts,
        "fairness": fairness,
        "runtime_envelopes": runtime_envelopes,
        "peer_workloads": peer_workloads,
        "storage_io": storage_io,
        "fault_recovery": fault_recovery,
        "invariants": {
            "capacity_overshoot_events": 0,
            "duplicate_observations": 0,
            "duplicate_trials": 0,
            "indefinite_starvation_events": 0,
            "exclusive_slurm_jobs": 0,
            "cgroup_escape_events": 0,
            "peer_disruption_events": 0,
            "storage_error_events": 0,
            "orphan_jobs": 0,
            "orphan_containers": 0,
            "orphan_leases": 0,
            "orphan_trials": 0,
            "duplicate_retries": 0,
            "unattributed_retries": 0,
        },
    }
    platform_authority = _platform_health_authority(evidence)
    evidence["platform_health"] = {
        "authority_evidence": platform_authority,
        "trusted_receipt": {
            "receipt_sha256": "0" * 64,
            "authority_payload_sha256": platform_authority["payload_sha256"],
            "source_host": ACCEPTANCE.SUBMIT_HOST,
            "observed_at": platform_authority["completed_at"],
        },
    }
    evidence["staging_pressure_reclaim"] = _staging_pressure_evidence(evidence)
    return evidence


def _failures(evidence: dict[str, Any]) -> list[str]:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return list(ACCEPTANCE.verify_evidence(evidence, schema))


def _journaled_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, dict[str, Any], Path, dict[str, Any]]:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    session_id = state["session_id"]
    evidence = _evidence()
    evidence["session"]["id"] = session_id
    evidence["staging_pressure_reclaim"] = _staging_pressure_evidence(evidence)
    for sandbox in FIXTURE_SANDBOXES:
        candidate = evidence["candidates"][sandbox]
        for receipt in _runtime_receipts(
            sandbox,
            candidate["sha"],
            candidate["tree"],
        ):
            wrapper = receipt.pop("_wrapper")
            assert receipt in candidate["runtime_receipts"]
            path = Path(receipt["path"])
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            path.write_bytes(ACCEPTANCE._canonical_bytes(wrapper))
            path.chmod(0o600)
    _record_trusted_receipts(session_id, evidence)
    phase_dir = tmp_path / "phase-inputs"
    phase_dir.mkdir()
    for index, (phase, sandbox) in enumerate(FIXTURE_PHASE_CHECKPOINTS):
        phase_payload = evidence["state_machine"][index].copy()
        del phase_payload["checkpoint_sha256"]
        digest = hashlib.sha256(ACCEPTANCE._canonical_bytes(phase_payload)).hexdigest()
        evidence["state_machine"][index]["checkpoint_sha256"] = digest
        phase_path = phase_dir / f"{sandbox}-{phase}.json"
        phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
        ACCEPTANCE.checkpoint_session(
            session_id,
            phase,
            sandbox,
            phase_path,
            execute=True,
        )
    evidence_path = tmp_path / "final.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return session_id, evidence, evidence_path, schema


def test_schema_is_valid_and_complete_fixture_passes() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    assert _failures(_evidence()) == []


def test_future_fourth_developer_requires_no_acceptance_code_or_schema_edit() -> None:
    evidence = _evidence((*FIXTURE_SANDBOXES, "future-dev"))
    future = next(
        environment
        for environment in evidence["registry_snapshot"]["environments"]
        if environment["runtime_id"] == "future-dev"
    )

    assert _failures(evidence) == []
    assert set(evidence["candidates"]) == {
        "qianyi",
        "hongjian",
        "devansh",
        "future-dev",
    }
    assert future["slurm_account"] == "lda-future-dev"
    assert future["slurm_qos"] == "ldq-future-dev"
    assert future["slurm_user"] == "loom-e-future-dev"
    assert future["systemd_instance"] == "future-dev"
    assert all(
        sample["account"] == future["slurm_account"]
        and sample["qos"] == future["slurm_qos"]
        and sample["user"] == future["slurm_user"]
        for sample in evidence["capacity_samples"]
        if sample["sandbox"] == "future-dev"
    )


def test_registry_snapshot_digest_tampering_fails_closed() -> None:
    evidence = _evidence()
    evidence["registry_snapshot"]["generation"] += 1

    assert _failures(evidence) == ["acceptance registry snapshot is invalid"]
    with pytest.raises(
        ACCEPTANCE.AcceptanceError,
        match="registry snapshot digest is invalid",
    ):
        ACCEPTANCE._validated_registry_snapshot(evidence["registry_snapshot"])


def test_source_registry_digest_tampering_fails_closed() -> None:
    source = _source_registry_snapshot()
    source["generation"] += 1

    with pytest.raises(
        ACCEPTANCE.AcceptanceError,
        match="source registry snapshot is invalid",
    ):
        ACCEPTANCE._acceptance_registry_snapshot(source)


def test_registry_projection_rejects_noncanonical_order() -> None:
    snapshot = _registry_snapshot()
    snapshot["environments"].reverse()
    unsigned = {key: value for key, value in snapshot.items() if key != "payload_sha256"}
    snapshot["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_digest_bytes(unsigned),
    ).hexdigest()

    with pytest.raises(
        ACCEPTANCE.AcceptanceError,
        match="cohort is not canonical",
    ):
        ACCEPTANCE._validated_registry_snapshot(snapshot)


def test_source_registry_rejects_duplicate_principal_and_resource() -> None:
    source = _source_registry_snapshot()
    source["environments"][1]["principal_id"] = source["environments"][0]["principal_id"]
    source["environments"][1]["slurm_account"] = source["environments"][0]["slurm_account"]
    _reseal_source_registry(source)

    with pytest.raises(
        ACCEPTANCE.AcceptanceError,
        match="source registry snapshot is invalid",
    ):
        ACCEPTANCE._acceptance_registry_snapshot(source)


@pytest.mark.parametrize(
    "attack",
    ["extra_candidate_field", "bundle_path", "same_worker_image_id"],
)
def test_source_registry_candidate_rows_are_closed_and_path_bound(attack: str) -> None:
    source = _source_registry_snapshot()
    if attack == "extra_candidate_field":
        source["candidates"][0]["unexpected"] = True
    elif attack == "bundle_path":
        source["candidates"][0]["bundle_path"] = "/tmp/candidate.bundle"
    else:
        source["candidates"][0]["image_digests"]["arm64"] = source["candidates"][0][
            "image_digests"
        ]["amd64"]
    _reseal_source_registry(source)

    with pytest.raises(
        ACCEPTANCE.AcceptanceError,
        match="source registry snapshot is invalid",
    ):
        ACCEPTANCE._acceptance_registry_snapshot(source)


@pytest.mark.parametrize("state", ["retired", "quarantined"])
def test_nonactive_registry_environment_is_excluded_from_cohort(state: str) -> None:
    source = _source_registry_snapshot()
    source["environments"][-1]["state"] = state
    _reseal_source_registry(source)

    snapshot = ACCEPTANCE._acceptance_registry_snapshot(source)

    assert [row["runtime_id"] for row in snapshot["environments"]] == [
        "qianyi",
        "hongjian",
    ]


def test_schema_node_contract_includes_gb10_node7() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    validator = Draft202012Validator(schema["$defs"]["node"])

    assert validator.is_valid("trt-gb10-7")


@pytest.mark.parametrize(
    "attack",
    ["authority_extra", "missing_expiry", "wrong_cgroup_type", "capacity_extra"],
)
def test_platform_health_schema_is_closed_and_typed(attack: str) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    evidence = _evidence()
    authority = evidence["platform_health"]["authority_evidence"]
    if attack == "authority_extra":
        authority["unexpected"] = True
    elif attack == "missing_expiry":
        del authority["expires_at"]
    elif attack == "wrong_cgroup_type":
        authority["mixed_jobs"][0]["cgroup"]["slurm_pid_cgroup_paths"] = "forged"
    else:
        authority["policy_capacity"]["oldlab"]["unexpected"] = 1

    assert list(Draft202012Validator(schema).iter_errors(evidence))


def test_premerge_candidate_map_requires_three_distinct_sandbox_shas() -> None:
    evidence = _evidence()
    evidence["candidates"]["hongjian"]["sha"] = evidence["candidates"]["qianyi"]["sha"]

    assert "pre-merge sandbox candidate SHAs must be distinct" in _failures(evidence)


def test_legacy_single_candidate_shape_is_rejected() -> None:
    evidence = _evidence()
    evidence["candidate"] = evidence.pop("candidates")["qianyi"]

    assert any("schema violation" in failure for failure in _failures(evidence))


def test_runtime_receipt_cannot_cross_candidate_map_entries() -> None:
    evidence = _evidence()
    receipt = evidence["candidates"]["qianyi"]["runtime_receipts"][0]
    receipt["candidate_sha"] = evidence["candidates"]["hongjian"]["sha"]

    assert "qianyi runtime receipt candidate does not match" in _failures(evidence)


def test_overlap_requires_a_real_common_active_window() -> None:
    evidence = _evidence()
    evidence["overlap_windows"][0]["observations"][0]["active_until"] = _iso(2, 20)

    assert any(
        "does not prove the common active window" in failure for failure in _failures(evidence)
    )

    evidence = _evidence()
    evidence["overlap_windows"][1]["observations"][1]["candidate_sha"] = evidence["candidates"][
        "qianyi"
    ]["sha"]
    assert any("overlap candidate does not match" in failure for failure in _failures(evidence))


def test_overlap_rejects_reused_job_ids_with_self_consistent_readbacks() -> None:
    evidence = _evidence()
    for window in evidence["overlap_windows"]:
        for observation in window["observations"]:
            observation["job_id"] = "123"
            observation["job_readback"]["job_id"] = "123"
            observation["job_readback_sha256"] = hashlib.sha256(
                ACCEPTANCE._canonical_bytes(observation["job_readback"]),
            ).hexdigest()

    failures = _failures(evidence)
    assert all(f"{pool} overlap job IDs are not unique" in failures for pool in ACCEPTANCE.POOLS)
    assert "overlap job ID is reused across pools" in failures


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("slurm_account", "loom-dev-hongjian"),
        ("slurm_user", "loom-sandbox-hongjian"),
        ("job_name", f"loom-sandbox-hongjian-{'c' * 12}-trt-eai-oldlab-1"),
    ],
)
def test_overlap_rejects_wrong_slurm_identity(field: str, replacement: str) -> None:
    evidence = _evidence()
    evidence["overlap_windows"][0]["observations"][0][field] = replacement

    expected = (
        "overlap Slurm readback does not match"
        if field == "job_name"
        else "overlap Slurm identity does not match"
    )
    assert any(expected in failure for failure in _failures(evidence))


def test_overlap_rejects_service_candidate_drift() -> None:
    evidence = _evidence()
    observation = evidence["overlap_windows"][0]["observations"][0]
    observation["active_candidate_sha"] = evidence["candidates"]["hongjian"]["sha"]

    assert any(
        "overlap service candidate does not match" in failure for failure in _failures(evidence)
    )


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("job_readback_sha256", "overlap Slurm readback digest does not match"),
        (
            "service_readback_sha256",
            "overlap service readback digest does not match",
        ),
    ],
)
def test_overlap_rejects_readback_digest_tamper(field: str, expected: str) -> None:
    evidence = _evidence()
    evidence["overlap_windows"][0]["observations"][0][field] = "0" * 64

    assert any(expected in failure for failure in _failures(evidence))


def test_overlap_rejects_capacity_binding_digest_tamper() -> None:
    evidence = _evidence()
    evidence["overlap_windows"][0]["observations"][0]["capacity_binding"]["sample_sha256"] = (
        "0" * 64
    )

    assert any(
        "overlap capacity binding does not match" in failure for failure in _failures(evidence)
    )


def test_global_job_identity_cannot_be_reused_by_runtime_or_peer() -> None:
    evidence = _evidence()
    overlap_job_id = evidence["overlap_windows"][0]["observations"][0]["job_id"]
    evidence["runtime_envelopes"][0]["job_id"] = overlap_job_id

    assert "overlap job ID is reused by a runtime envelope" in _failures(evidence)

    evidence = _evidence()
    runtime_job_id = evidence["runtime_envelopes"][0]["job_id"]
    evidence["peer_workloads"][0]["job_id"] = runtime_job_id

    assert "runtime envelope job ID is reused by a peer workload" in _failures(evidence)


def test_each_phase_binds_its_sandbox_candidate() -> None:
    evidence = _evidence()
    hongjian_phase = next(
        phase for phase in evidence["state_machine"] if phase["sandbox"] == "hongjian"
    )
    hongjian_phase["candidate_sha"] = evidence["candidates"]["qianyi"]["sha"]

    assert any("is not bound to the exact candidate" in failure for failure in _failures(evidence))


def test_duplicate_and_missing_phase_identity_fail_without_keyerror() -> None:
    evidence = _evidence()
    evidence["state_machine"][4] = copy.deepcopy(evidence["state_machine"][0])

    failures = _failures(evidence)
    assert "state-machine phase identity is duplicated" in failures
    assert "state-machine phase identity is missing" in failures

    evidence = _evidence()
    evidence["state_machine"].pop()
    failures = ACCEPTANCE._semantic_failures(evidence)
    assert "state-machine phase identity is missing" in failures


def test_promotion_candidate_is_separate_and_exactly_bound() -> None:
    evidence = _evidence()
    evidence["promotion_candidate"]["sha"] = evidence["candidates"]["qianyi"]["sha"]
    evidence["promotion_candidate"]["staging_regression"]["candidate_sha"] = evidence["candidates"][
        "qianyi"
    ]["sha"]

    failures = _failures(evidence)
    assert "promotion candidate must be distinct from pre-merge sandbox SHAs" in failures
    assert "promotion staging regression checkpoint digest does not match" in failures


def test_default_plan_is_read_only_registry_driven_and_complete() -> None:
    completed = _run()

    assert completed.returncode == 0, completed.stderr
    plan = json.loads(completed.stdout)
    assert plan["mode"] == "plan_read_only"
    assert plan["live_mutations_supported"] is False
    assert plan["submit_host"] == "trt-eai-oldlab-2"
    assert plan["cohort_source"] == "generation-and-digest-bound registry snapshot"
    assert plan["pools"] == ["oldlab", "gb10"]
    assert plan["infrastructure_nodes"] == list(ACCEPTANCE.INFRASTRUCTURE_NODES)
    assert plan["excluded_nodes"] == []
    assert plan["state_machine"] == list(ACCEPTANCE.PHASES)
    assert len(plan["stop_rules"]) >= 8


def test_session_mutation_requires_execute_before_host_checks(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.json"
    candidates_path.write_text(json.dumps(_candidate_map()), encoding="utf-8")
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(_source_registry_snapshot()), encoding="utf-8")
    completed = _run(
        "session-start",
        "--registry-snapshot",
        registry_path,
        "--candidates",
        candidates_path,
    )

    assert completed.returncode == 1
    assert "explicit --execute" in completed.stdout


def test_session_start_derives_fourth_environment_from_full_source_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandboxes = (*FIXTURE_SANDBOXES, "future-dev")
    _patch_live_host(tmp_path, monkeypatch)

    state = ACCEPTANCE.start_session(
        _candidate_map(sandboxes),
        registry_snapshot=_source_registry_snapshot(sandboxes),
        execute=True,
    )

    assert tuple(state["candidates"]) == sandboxes
    assert (
        state["registry_snapshot"]["source_registry"]["payload_sha256"]
        == _source_registry_snapshot(sandboxes)["payload_sha256"]
    )
    assert state["registry_snapshot"]["environments"][-1]["slurm_account"] == ("lda-future-dev")


def test_persistent_state_machine_is_ordered_and_candidate_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    session_id = state["session_id"]

    phase_evidence = {
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "sandbox": "qianyi",
        "phase": "preflight",
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="exact next phase"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "baseline",
            "qianyi",
            phase_path,
            execute=True,
        )

    advanced = ACCEPTANCE.checkpoint_session(
        session_id,
        "preflight",
        "qianyi",
        phase_path,
        execute=True,
    )
    assert advanced["completed_phases"] == ["qianyi:preflight"]
    assert advanced["next_phase_index"] == 1
    persisted = json.loads(
        (
            tmp_path / "state/sessions" / session_id / "checkpoints/00-qianyi-preflight.json"
        ).read_text(encoding="utf-8"),
    )
    digest = hashlib.sha256(ACCEPTANCE._canonical_bytes(phase_evidence)).hexdigest()
    assert persisted["evidence_sha256"] == digest
    assert persisted["recorded_at"] == phase_evidence["finished_at"]


def test_complete_session_seals_verified_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )

    complete = ACCEPTANCE.finalize_session(
        session_id,
        evidence_path,
        schema,
        execute=True,
    )

    assert complete["status"] == "complete"
    assert len(complete["evidence_sha256"]) == 64
    sealed = tmp_path / "state/sessions" / session_id / "evidence.json"
    assert json.loads(sealed.read_text(encoding="utf-8")) == evidence


def test_gate6_seal_is_root_owned_candidate_bound_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    ACCEPTANCE.finalize_session(session_id, evidence_path, schema, execute=True)
    slurm_root = tmp_path / "authority/slurm-policy"
    monkeypatch.setattr(ACCEPTANCE, "SLURM_POLICY_STATE_ROOT", slurm_root)
    matrices: dict[tuple[str, str], dict[str, Any]] = {}
    for sandbox in FIXTURE_SANDBOXES:
        sha = evidence["candidates"][sandbox]["sha"]
        wrapper = json.loads(
            Path(evidence["candidates"][sandbox]["runtime_receipts"][-1]["path"]).read_text(),
        )
        for pool in ACCEPTANCE.POOLS:
            combined = wrapper["combined_receipt"]
            domain = combined["domains"][pool]
            matrix = {
                "sandbox": sandbox,
                "pool": pool,
                "candidate_sha": sha,
                "runtime_attestation": {
                    "receipt_sha256": combined["payload_sha256"],
                    "domain_payload_sha256": domain["payload_sha256"],
                    "domain_signature_sha256": domain["signature_sha256"],
                    "domain_generation": domain["generation"],
                },
            }
            matrices[(sandbox, pool)] = matrix
            _write_authority_json(
                ACCEPTANCE._gate6_matrix_path(sandbox, pool, sha),
                slurm_root,
                matrix,
            )
    bundle = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.gate6-acceptance",
        "session_id": session_id,
        "payload_sha256": "7" * 64,
        "status": "pass",
    }
    artifacts = {
        pair: {"schema_version": 1, "sandbox": pair[0], "pool": pair[1]} for pair in matrices
    }

    def build(
        observed_live: Any,
        observed_platform: Any,
        observed_matrices: Any,
        _nonexclusive_schema: Any,
    ) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
        assert observed_live == evidence
        assert observed_platform == evidence["platform_health"]["authority_evidence"]
        assert observed_matrices == matrices
        return bundle, artifacts

    monkeypatch.setattr(ACCEPTANCE.gate6_verifier, "build_gate6_bundle", build)
    nonexclusive_schema = ACCEPTANCE.gate6_verifier._load_schema(
        ACCEPTANCE.NONEXCLUSIVE_SCHEMA,
    )
    tampered = copy.deepcopy(matrices[("qianyi", "gb10")])
    tampered["runtime_attestation"]["receipt_sha256"] = "8" * 64
    qianyi_sha = evidence["candidates"]["qianyi"]["sha"]
    qianyi_gb10_path = ACCEPTANCE._gate6_matrix_path("qianyi", "gb10", qianyi_sha)
    _write_authority_json(qianyi_gb10_path, slurm_root, tampered)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="trusted runtime receipt"):
        ACCEPTANCE.seal_gate6(
            session_id,
            schema,
            nonexclusive_schema,
            execute=True,
        )
    _write_authority_json(
        qianyi_gb10_path,
        slurm_root,
        matrices[("qianyi", "gb10")],
    )

    first = ACCEPTANCE.seal_gate6(
        session_id,
        schema,
        nonexclusive_schema,
        execute=True,
    )
    second = ACCEPTANCE.seal_gate6(
        session_id,
        schema,
        nonexclusive_schema,
        execute=True,
    )

    assert first == second
    assert first["gate6_sha256"] == "7" * 64
    gate_root = tmp_path / "state/sessions" / session_id / "gate6"
    assert json.loads((gate_root / "acceptance.json").read_text()) == bundle
    assert stat.S_IMODE((gate_root / "acceptance.json").stat().st_mode) == 0o600
    assert sorted(path.name for path in gate_root.glob("*.nonexclusive.json")) == [
        f"{sandbox}-{pool}.nonexclusive.json"
        for sandbox in sorted(FIXTURE_SANDBOXES)
        for pool in sorted(ACCEPTANCE.POOLS)
    ]


def test_staging_pressure_semantics_reject_candidate_and_session_drift() -> None:
    evidence = _evidence()
    evidence["staging_pressure_reclaim"]["authority_evidence"]["candidate_sha"] = "9" * 40
    evidence["staging_pressure_reclaim"]["authority_evidence"]["acceptance_session_id"] = "2" * 32

    failures = _failures(evidence)

    assert "staging pressure reclaim evidence is not exactly bound" in failures


def test_staging_pressure_published_receipt_rejects_bad_signature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, _evidence_path, _schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    authority_session_id = evidence["staging_pressure_reclaim"]["trusted_receipt"][
        "authority_session_id"
    ]
    published_path = ACCEPTANCE._pressure_authority_path(
        session_id,
        authority_session_id,
    )
    published = json.loads(published_path.read_text(encoding="utf-8"))
    signature_bytes = base64.b64decode(published["signature"]["signature_base64"])
    signature_bytes = bytes([signature_bytes[0] ^ 1, *signature_bytes[1:]])
    published["signature"]["signature_base64"] = base64.b64encode(signature_bytes).decode(
        "ascii",
    )
    published["signature"]["signature_sha256"] = hashlib.sha256(signature_bytes).hexdigest()

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="signature is invalid"):
        ACCEPTANCE._validate_pressure_authority(
            published,
            acceptance_session_id=session_id,
            authority_session_id=authority_session_id,
            candidate_sha=evidence["promotion_candidate"]["sha"],
            candidate_tree=evidence["promotion_candidate"]["tree"],
        )


def test_finalize_rejects_tampered_staging_pressure_session_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    state = ACCEPTANCE._session_state(session_id)
    digest = state["staging_pressure_receipt_sha256"]
    receipt_path = (
        tmp_path
        / "state/sessions"
        / session_id
        / "trusted-receipts"
        / f"staging-pressure-{digest}.json"
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["sequence"] += 1
    receipt_path.write_bytes(ACCEPTANCE._canonical_bytes(receipt))
    receipt_path.chmod(0o600)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="receipt digest"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_runtime_receipt_series_covers_longer_than_single_ttl() -> None:
    evidence = _evidence()

    assert _failures(evidence) == []
    for sandbox in FIXTURE_SANDBOXES:
        chain = [receipt for receipt in evidence["candidates"][sandbox]["runtime_receipts"]]
        assert len(chain) == 5
        assert ACCEPTANCE._timestamp(chain[0]["collected_at"]) <= ACCEPTANCE._timestamp(
            evidence["session"]["started_at"],
        )
        assert ACCEPTANCE._timestamp(chain[-1]["expires_at"]) >= ACCEPTANCE._timestamp(
            evidence["session"]["completed_at"],
        )


def test_runtime_receipt_series_rejects_missing_renewal_gap() -> None:
    evidence = _evidence()
    evidence["candidates"]["qianyi"]["runtime_receipts"] = [
        receipt
        for receipt in evidence["candidates"]["qianyi"]["runtime_receipts"]
        if receipt["renewal_generation"] != 2
    ]

    assert "qianyi runtime receipt chain link is invalid" in _failures(evidence)
    assert "qianyi runtime receipt chain has a liveness gap" in _failures(evidence)


def test_finalize_reads_root_owned_receipt_instead_of_trusting_caller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    reference = evidence["candidates"]["qianyi"]["runtime_receipts"][0]
    path = Path(reference["path"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["expires_at"] = _iso(60)
    path.write_bytes(ACCEPTANCE._canonical_bytes(payload))

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="identity or digest"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_missing_root_owned_renewal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    missing = next(
        receipt
        for receipt in evidence["candidates"]["hongjian"]["runtime_receipts"]
        if receipt["renewal_generation"] == 3
    )
    Path(missing["path"]).unlink()

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="root-owned runtime receipt"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_overlap_record_fails_closed_when_fixed_producer_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    observation = _evidence()["overlap_windows"][0]["observations"][0]

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="authority root is unavailable"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )


def test_overlap_record_rejects_foreign_path_and_hardlinked_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    evidence = _evidence()
    observation = evidence["overlap_windows"][0]["observations"][0]
    paths = _write_overlap_authority_sources(evidence, "oldlab", observation)

    foreign = paths[0].with_name("foreign.json")
    paths[0].replace(foreign)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="authority file safely"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )

    foreign.replace(paths[0])
    os.link(paths[0], paths[0].with_name("hardlink.json"))
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="authority file is unsafe"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )


def test_overlap_record_detects_source_swap_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    evidence = _evidence()
    observation = evidence["overlap_windows"][0]["observations"][0]
    paths = _write_overlap_authority_sources(evidence, "oldlab", observation)
    replacement = paths[0].with_name("replacement.json")
    replacement.write_bytes(paths[0].read_bytes())
    replacement.chmod(0o600)
    original_read = ACCEPTANCE.os.read
    swapped = False

    def swap_during_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        if not swapped:
            swapped = True
            os.replace(replacement, paths[0])
        return original_read(descriptor, size)

    monkeypatch.setattr(ACCEPTANCE.os, "read", swap_during_read)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="changed during read"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )


def test_overlap_receipt_replay_and_sequence_regression_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    evidence = _evidence()
    observation = evidence["overlap_windows"][0]["observations"][0]
    _write_overlap_authority_sources(evidence, "oldlab", observation)
    ACCEPTANCE.record_overlap_receipt(
        state["session_id"],
        observation["sandbox"],
        "oldlab",
        observation["job_id"],
        execute=True,
    )
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="already exists"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )

    session_dir = tmp_path / "state/sessions" / state["session_id"]
    persisted = json.loads((session_dir / "state.json").read_text(encoding="utf-8"))
    persisted["next_trusted_sequence"] = 1
    ACCEPTANCE._atomic_write(session_dir / "state.json", persisted)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="progress is invalid"):
        ACCEPTANCE._session_state(state["session_id"])


def test_overlap_record_rejects_capacity_sample_drift_from_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    evidence = _evidence()
    observation = evidence["overlap_windows"][0]["observations"][0]
    paths = _write_overlap_authority_sources(evidence, "oldlab", observation)
    live_observation = json.loads(paths[2].read_text(encoding="utf-8"))
    live_observation["capacity_sample"]["observation_sequence"] += 1
    _write_authority_json(
        paths[2],
        ACCEPTANCE.LIVE_AUTHORITY_ROOT,
        live_observation,
    )

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="authority sources do not agree"):
        ACCEPTANCE.record_overlap_receipt(
            state["session_id"],
            observation["sandbox"],
            "oldlab",
            observation["job_id"],
            execute=True,
        )


def test_finalize_rejects_self_consistent_forged_overlap_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    observation = evidence["overlap_windows"][0]["observations"][0]
    sample = next(
        row
        for row in evidence["capacity_samples"]
        if row["phase"] == "multi_candidate_overlap"
        and row["sandbox"] == observation["sandbox"]
        and row["pool"] == "oldlab"
    )
    observation["node"] = "trt-eai-oldlab-3"
    observation["job_name"] = _job_name(
        observation["sandbox"],
        observation["candidate_sha"],
        observation["node"],
    )
    observation["job_readback"]["node"] = observation["node"]
    observation["job_readback"]["job_name"] = observation["job_name"]
    observation["job_readback_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(observation["job_readback"]),
    ).hexdigest()
    sample["node"] = observation["node"]
    sample["job_name"] = observation["job_name"]
    observation["capacity_binding"]["sample_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(sample),
    ).hexdigest()
    assert _failures(evidence) == []
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="trusted overlap receipt"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_stale_trusted_overlap_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, _evidence_path, _schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    state = ACCEPTANCE._session_state(session_id)
    evidence["overlap_windows"][0]["started_at"] = _iso(2, 40)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="trusted overlap receipt"):
        ACCEPTANCE._verify_overlap_session_receipts(
            session_id,
            evidence,
            state,
        )


def test_mixed_phase_checkpoint_persists_exact_soak_batch_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, _evidence_path, _schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    checkpoint = json.loads(
        (
            tmp_path / "state/sessions" / session_id / "checkpoints/15-qianyi-mixed_non_loom.json"
        ).read_text(encoding="utf-8"),
    )
    phase = next(
        row
        for row in evidence["state_machine"]
        if row["phase"] == "mixed_non_loom" and row["sandbox"] == "qianyi"
    )

    assert checkpoint["trial_batches"] == phase["trial_batches"]
    assert set(checkpoint["trial_batches"]) == {"oldlab", "gb10"}


def test_finalize_rejects_self_consistent_capacity_drift_from_root_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    observation = evidence["overlap_windows"][0]["observations"][0]
    sample = next(
        row
        for row in evidence["capacity_samples"]
        if row["phase"] == "multi_candidate_overlap"
        and row["sandbox"] == observation["sandbox"]
        and row["pool"] == "oldlab"
    )
    sample["requested_slots"] += 1
    observation["capacity_binding"]["sample_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(sample),
    ).hexdigest()
    assert _failures(evidence) == []
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="trusted overlap receipt"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_promotion_self_hash_without_root_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    state = ACCEPTANCE._session_state(session_id)
    digest = state["promotion_receipt_sha256"]
    assert isinstance(digest, str)
    (
        tmp_path / "state/sessions" / session_id / "trusted-receipts" / f"promotion-{digest}.json"
    ).unlink()

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="state file is unavailable"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_hardlinked_session_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    state = ACCEPTANCE._session_state(session_id)
    descriptor = state["trusted_overlap_receipts"][0]
    receipt = (
        tmp_path
        / "state/sessions"
        / session_id
        / "trusted-receipts"
        / (
            f"{descriptor['sequence']:020d}-{descriptor['sandbox']}-"
            f"{descriptor['pool']}-{descriptor['receipt_sha256']}.json"
        )
    )
    os.link(receipt, receipt.with_name("foreign-hardlink.json"))

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_state_tree_is_closed_root_only_and_rejects_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    target = tmp_path / "redirect"
    target.mkdir(mode=0o700)
    (tmp_path / "state").symlink_to(target, target_is_directory=True)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        _start_session()


def test_state_tree_owner_modes_and_fqdn_host_are_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    session_dir = tmp_path / "state/sessions" / state["session_id"]

    for directory in (
        tmp_path / "state",
        tmp_path / "state/sessions",
        session_dir,
        session_dir / "checkpoints",
    ):
        metadata = directory.lstat()
        assert metadata.st_uid == os.getuid()
        assert metadata.st_gid == os.getgid()
        assert metadata.st_mode & 0o777 == 0o700
    for file_path in (session_dir / "state.json", session_dir / "session.lock"):
        metadata = file_path.lstat()
        assert metadata.st_mode & 0o777 == 0o600

    os.chmod(session_dir / "checkpoints", 0o755)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE._session_state(state["session_id"])


def test_state_file_owner_mismatch_fails_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    monkeypatch.setattr(ACCEPTANCE, "REQUIRED_OWNER_UID", os.getuid() + 1)

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="unsafe ownership or mode"):
        ACCEPTANCE._session_state(state["session_id"])


def test_late_checkpoint_create_failure_is_not_mistaken_for_durable_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    destination = (
        tmp_path / "state/sessions" / state["session_id"] / "checkpoints/00-qianyi-preflight.json"
    )
    payload = {"schema_version": 1, "phase": "preflight"}
    original_fsync_directory = ACCEPTANCE._fsync_directory
    destination_fsync_attempts = 0

    def fail_first_destination_fsync(path: Path) -> None:
        nonlocal destination_fsync_attempts
        if path == destination.parent:
            destination_fsync_attempts += 1
            if destination_fsync_attempts == 1:
                raise OSError("simulated directory fsync failure")
        original_fsync_directory(path)

    monkeypatch.setattr(
        ACCEPTANCE,
        "_fsync_directory",
        fail_first_destination_fsync,
    )
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="cannot create"):
        ACCEPTANCE._write_or_verify_secure(destination, payload)
    assert destination.is_file()

    ACCEPTANCE._write_or_verify_secure(destination, payload)
    assert destination_fsync_attempts == 2


def test_checkpoint_is_crash_idempotent_and_rejects_changed_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    session_id = state["session_id"]
    phase_payload = {
        "phase": "preflight",
        "sandbox": "qianyi",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
    original_atomic = ACCEPTANCE._atomic_write
    failed = False

    def fail_state_once(path: Path, payload: dict[str, Any]) -> None:
        nonlocal failed
        if path.name == "state.json" and payload["next_phase_index"] == 1 and not failed:
            failed = True
            raise ACCEPTANCE.AcceptanceError("simulated state crash")
        original_atomic(path, payload)

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", fail_state_once)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="simulated state crash"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "preflight",
            "qianyi",
            phase_path,
            execute=True,
        )
    checkpoint = tmp_path / "state/sessions" / session_id / "checkpoints/00-qianyi-preflight.json"
    assert checkpoint.is_file()
    assert ACCEPTANCE._session_state(session_id)["next_phase_index"] == 0

    changed = phase_payload.copy()
    changed["deadline_seconds"] = 601
    phase_path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="does not match"):
        ACCEPTANCE.checkpoint_session(
            session_id,
            "preflight",
            "qianyi",
            phase_path,
            execute=True,
        )

    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")
    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", original_atomic)
    recovered = ACCEPTANCE.checkpoint_session(
        session_id,
        "preflight",
        "qianyi",
        phase_path,
        execute=True,
    )
    assert recovered["next_phase_index"] == 1


def test_concurrent_same_phase_checkpoint_is_serialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_host(tmp_path, monkeypatch)
    state = _start_session()
    phase_payload = {
        "phase": "preflight",
        "sandbox": "qianyi",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "started_at": _iso(0),
        "finished_at": _iso(0, 30),
        "deadline_seconds": 600,
        "status": "pass",
    }
    phase_path = tmp_path / "phase.json"
    phase_path.write_text(json.dumps(phase_payload), encoding="utf-8")

    def checkpoint() -> dict[str, Any]:
        return dict(
            ACCEPTANCE.checkpoint_session(
                state["session_id"],
                "preflight",
                "qianyi",
                phase_path,
                execute=True,
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: checkpoint(), range(2)))

    assert [result["next_phase_index"] for result in results] == [1, 1]
    assert ACCEPTANCE._session_state(state["session_id"])["completed_phases"] == [
        "qianyi:preflight",
    ]


def test_finalize_is_crash_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence_payload, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    original_atomic = ACCEPTANCE._atomic_write
    failed = False

    def fail_complete_once(path: Path, payload: dict[str, Any]) -> None:
        nonlocal failed
        if path.name == "state.json" and payload["status"] == "complete" and not failed:
            failed = True
            raise ACCEPTANCE.AcceptanceError("simulated finalize crash")
        original_atomic(path, payload)

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", fail_complete_once)
    with pytest.raises(ACCEPTANCE.AcceptanceError, match="simulated finalize crash"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )
    session_dir = tmp_path / "state/sessions" / session_id
    assert (session_dir / "evidence.json").is_file()
    assert ACCEPTANCE._session_state(session_id)["status"] == "running"

    monkeypatch.setattr(ACCEPTANCE, "_atomic_write", original_atomic)
    recovered = ACCEPTANCE.finalize_session(
        session_id,
        evidence_path,
        schema,
        execute=True,
    )
    assert recovered["status"] == "complete"


def test_finalize_recomputes_phase_digest_and_rejects_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, evidence, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    evidence["state_machine"][0]["deadline_seconds"] = 601
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="checkpoint journal"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_finalize_rejects_checkpoint_metadata_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id, _evidence_payload, evidence_path, schema = _journaled_evidence(
        tmp_path,
        monkeypatch,
    )
    checkpoint_path = (
        tmp_path / "state/sessions" / session_id / "checkpoints/00-qianyi-preflight.json"
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["candidate_sha"] = "e" * 40
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="checkpoint journal"):
        ACCEPTANCE.finalize_session(
            session_id,
            evidence_path,
            schema,
            execute=True,
        )


def test_cross_sandbox_negative_matrix_is_exact_and_candidate_bound() -> None:
    evidence = _evidence()
    evidence["cross_sandbox_negative"].pop()
    assert any("negative matrix is incomplete" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["cross_sandbox_negative"][-1] = copy.deepcopy(
        evidence["cross_sandbox_negative"][0],
    )
    assert any("negative matrix is incomplete" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["cross_sandbox_negative"][0]["source_candidate_sha"] = "e" * 40
    assert any(
        "negative probe candidate pair does not match" in failure for failure in _failures(evidence)
    )


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["capacity_samples"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "capacity sample candidate does not match",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "candidate_sha",
                "e" * 40,
            ),
            "large-batch burst candidate does not match",
        ),
        (
            lambda item: item["fairness"][0]["participants"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "fairness candidate does not match",
        ),
        (
            lambda item: item["runtime_envelopes"][0].__setitem__(
                "candidate_sha",
                "e" * 40,
            ),
            "runtime envelope candidate does not match",
        ),
        (
            lambda item: item["fault_recovery"][0].__setitem__(
                "candidate_tree",
                "e" * 40,
            ),
            "is not candidate-bound",
        ),
    ],
)
def test_all_evidence_domains_reject_an_old_candidate(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["cross_sandbox_negative"][0].__setitem__(
                "observed_at",
                _phase_observed_at("mixed_non_loom"),
            ),
            "negative probe is outside",
        ),
        (
            lambda item: item["capacity_samples"][0].__setitem__(
                "observed_at",
                _phase_observed_at("baseline"),
            ),
            "capacity sample is outside",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "started_at",
                _phase_observed_at("baseline"),
            ),
            "large batch is outside",
        ),
        (
            lambda item: item["fairness"][0].__setitem__(
                "started_at",
                _phase_observed_at("baseline"),
            ),
            "fairness is outside",
        ),
        (
            lambda item: item["runtime_envelopes"][0].__setitem__(
                "observed_at",
                _phase_observed_at("baseline"),
            ),
            "runtime envelope is outside",
        ),
        (
            lambda item: item["peer_workloads"][0]["baseline"].__setitem__(
                "observed_at",
                _phase_observed_at("preflight"),
            ),
            "peer checkpoints are outside",
        ),
        (
            lambda item: item["storage_io"][0].__setitem__(
                "baseline_observed_at",
                _phase_observed_at("preflight"),
            ),
            "storage observations are outside",
        ),
        (
            lambda item: item["fault_recovery"][0].__setitem__(
                "phase",
                "ttl_cleanup",
            ),
            "bound to the wrong phase",
        ),
    ],
)
def test_evidence_timestamps_must_land_in_the_exact_phase(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["session"].__setitem__("collected_at", _iso(50)),
            "freshness",
        ),
        (
            lambda item: item["capacity_samples"][0].__setitem__("active_slots", 30),
            "committed bounds",
        ),
        (
            lambda item: item["capacity_samples"].__setitem__(
                1,
                copy.deepcopy(item["capacity_samples"][0]),
            ),
            "capacity samples",
        ),
        (
            lambda item: item["large_batch_bursts"][0].__setitem__(
                "nodes",
                ["trt-eai-oldlab-1"],
            ),
            "schema violation",
        ),
        (
            lambda item: item["runtime_envelopes"][0]["allocation"].__setitem__(
                "exclusive",
                True,
            ),
            "schema violation",
        ),
        (
            lambda item: item["runtime_envelopes"][0]["containers"][0].__setitem__(
                "observed_cgroup_path",
                "/system.slice/slurmstepd.scope/job_999/docker/escape",
            ),
            "escaped",
        ),
        (
            lambda item: item["peer_workloads"][0]["during"].__setitem__(
                "throughput_per_second",
                1,
            ),
            "throughput regression",
        ),
        (
            lambda item: item["storage_io"][0].__setitem__("io_errors", 1),
            "schema violation",
        ),
        (
            lambda item: item["fault_recovery"][0]["retry_attribution"].__setitem__(
                "retryable_trials",
                1,
            ),
            "fully retryable",
        ),
    ],
)
def test_acceptance_failures_are_closed(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


def test_capacity_overshoot_and_duplicate_observation_fail() -> None:
    evidence = _evidence()
    for sample in evidence["capacity_samples"]:
        if sample["pool"] == "oldlab":
            sample["requested_slots"] = 20
            sample["granted_slots"] = 20
            sample["pending_slots"] = 0
            sample["active_slots"] = 20
    failures = _failures(evidence)
    assert any("overshoots the slot budget" in failure for failure in failures)

    evidence = _evidence()
    evidence["capacity_samples"][1]["observation_sequence"] = evidence["capacity_samples"][0][
        "observation_sequence"
    ]
    failures = _failures(evidence)
    assert any("observation identity is duplicated" in failure for failure in failures)


def test_capacity_requires_every_phase_pair_and_zero_final_drain() -> None:
    evidence = _evidence()
    evidence["capacity_samples"].pop()
    assert any("every required phase/pair" in failure for failure in _failures(evidence))

    evidence = _evidence()
    final = next(
        sample for sample in evidence["capacity_samples"] if sample["phase"] == "final_drain"
    )
    final["requested_slots"] = 1
    final["granted_slots"] = 1
    final["active_slots"] = 1
    assert any("final drain retains" in failure for failure in _failures(evidence))


def test_runtime_limits_and_candidate_binding_fail() -> None:
    evidence = _evidence()
    envelope = evidence["runtime_envelopes"][0]
    envelope["containers"][0]["observed_limits"]["pids"] = 999
    envelope["candidate_sha"] = "e" * 40

    failures = _failures(evidence)

    assert any("configured/observed limits differ" in failure for failure in failures)
    assert any("runtime envelope candidate does not match" in failure for failure in failures)


def test_runtime_account_node_and_gpu_envelopes_are_bound() -> None:
    evidence = _evidence()
    envelope = evidence["runtime_envelopes"][0]
    envelope["account"] = "loom-dev-devansh"
    envelope["node"] = "trt-gb10-1"
    envelope["containers"][0]["gpu_ids"] = ["GPU-0"]

    failures = _failures(evidence)

    assert any("Slurm identity does not match" in failure for failure in failures)
    assert any("node does not match" in failure for failure in failures)
    assert any("OLDLAB runtime envelope" in failure for failure in failures)


def _validate_fixture_cgroup(
    evidence: Mapping[str, Any],
    job: Mapping[str, Any],
    *,
    pool: str,
) -> None:
    environments = ACCEPTANCE._registry_environments(evidence["registry_snapshot"])
    ACCEPTANCE._validate_cgroup_evidence(
        job["cgroup"],
        job["containers"],
        job_id=job["job_id"],
        job_start_time=job["job_start_time"],
        node=job["node"],
        pool=pool,
        account=job["account"],
        sandbox=job["sandbox"],
        environment=environments[job["sandbox"]],
        candidate_sha=job["candidate_sha"],
        candidate_tree=job["candidate_tree"],
        allocation=job["allocation"],
    )


@pytest.mark.parametrize("pool", ACCEPTANCE.POOLS)
def test_systemd_mirror_runtime_cgroup_is_closed_and_candidate_bound(pool: str) -> None:
    evidence = _evidence()
    envelope = next(row for row in evidence["runtime_envelopes"] if row["pool"] == pool)
    environment = ACCEPTANCE._registry_environments(evidence["registry_snapshot"])[
        envelope["sandbox"]
    ]
    _use_systemd_mirror(envelope, environment, pool=pool)

    _validate_fixture_cgroup(evidence, envelope, pool=pool)


@pytest.mark.parametrize(
    ("attack", "expected"),
    [
        ("receipt-env", "receipt binding"),
        ("job-reuse", "receipt binding"),
        ("limit-drift", "receipt binding"),
        ("live-limit-drift", "live path or limits"),
        ("slice-escape", "live path or limits"),
        ("same-slice-wrong-root", "live path or limits"),
        ("coordinated-path-rewrite", "live path or limits"),
        ("receipt-swap", "receipt binding"),
        ("live-swap", "live path or limits"),
        ("gpu-wrong-id", "receipt binding"),
        ("gpu-multi-container", "GPU assignment"),
        ("start-format", "start identity"),
    ],
)
def test_systemd_mirror_rejects_receipt_reuse_limit_drift_and_escape(
    attack: str,
    expected: str,
) -> None:
    evidence = _evidence()
    envelope = (
        next(row for row in evidence["runtime_envelopes"] if row["pool"] == "gb10")
        if attack in {"gpu-wrong-id", "gpu-multi-container"}
        else evidence["runtime_envelopes"][0]
    )
    environment = ACCEPTANCE._registry_environments(evidence["registry_snapshot"])[
        envelope["sandbox"]
    ]
    _use_systemd_mirror(envelope, environment, pool=envelope["pool"])
    cgroup = envelope["cgroup"]
    receipt = cgroup["systemd_slice_receipt"]
    if attack == "receipt-env":
        receipt["env_id"] = "denv-foreign001"
        _refresh_systemd_slice_receipt(cgroup)
        _rebind_systemd_containers(envelope)
    elif attack == "job-reuse":
        receipt["job_start_time"] = "2026-07-29T12:00:00"
        _refresh_systemd_slice_receipt(cgroup)
        _rebind_systemd_containers(envelope)
    elif attack == "limit-drift":
        receipt["memory_max"] = str(envelope["allocation"]["memory_bytes"] + 1)
        _refresh_systemd_slice_receipt(cgroup)
    elif attack == "live-limit-drift":
        cgroup["systemd_slice_live"]["pids_max"] -= 1
    elif attack == "slice-escape":
        envelope["containers"][0]["observed_cgroup_path"] = (
            "/loom.slice/loom-job.slice/loom-job-999-" + "f" * 40 + ".slice/docker.scope"
        )
    elif attack == "same-slice-wrong-root":
        envelope["containers"][0]["observed_cgroup_path"] = (
            f"/foreign.slice/{cgroup['container_parent']}/docker.scope"
        )
    elif attack == "coordinated-path-rewrite":
        rewritten = f"/foreign.slice/{cgroup['container_parent']}"
        cgroup["systemd_slice_live"]["path"] = rewritten
        for container in envelope["containers"]:
            container["observed_cgroup_path"] = (
                f"{rewritten}/docker-{container['container_id']}.scope"
            )
    elif attack == "receipt-swap":
        receipt["memory_swap_max_source"] = "1"
        receipt["memory_swap_max_effective"] = "1"
        cgroup["systemd_slice_live"]["memory_swap_bytes_max"] = 1
        _refresh_systemd_slice_receipt(cgroup)
    elif attack == "live-swap":
        cgroup["systemd_slice_live"]["memory_swap_bytes_max"] = 1
    elif attack == "gpu-wrong-id":
        allocated = next(item for item in envelope["containers"] if item["gpu_ids"])
        allocated["gpu_ids"] = ["1"]
    elif attack == "gpu-multi-container":
        allocated = next(item for item in envelope["containers"] if item["gpu_ids"])
        denied = next(item for item in envelope["containers"] if not item["gpu_ids"])
        denied["gpu_ids"] = list(allocated["gpu_ids"])
    else:
        envelope["job_start_time"] = "2026-07-30T12:00:00-04:00"

    with pytest.raises(ACCEPTANCE.AcceptanceError, match=expected):
        _validate_fixture_cgroup(evidence, envelope, pool=envelope["pool"])


def test_cgroupfs_gpu_assignment_requires_one_exact_allocated_container() -> None:
    evidence = _evidence()
    envelope = next(row for row in evidence["runtime_envelopes"] if row["pool"] == "gb10")
    allocated = next(item for item in envelope["containers"] if item["gpu_ids"])
    denied = next(item for item in envelope["containers"] if not item["gpu_ids"])
    denied["gpu_ids"] = list(allocated["gpu_ids"])

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="GPU assignment"):
        _validate_fixture_cgroup(evidence, envelope, pool="gb10")


@pytest.mark.parametrize(
    "attack",
    ("receipt-swap", "live-swap", "control-group-path", "start-time"),
)
def test_systemd_runtime_schema_rejects_noncanonical_authority(
    attack: str,
) -> None:
    evidence = _evidence()
    envelope = next(row for row in evidence["runtime_envelopes"] if row["pool"] == "gb10")
    environment = ACCEPTANCE._registry_environments(evidence["registry_snapshot"])[
        envelope["sandbox"]
    ]
    _use_systemd_mirror(envelope, environment, pool="gb10")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    assert list(validator.iter_errors(evidence)) == []

    if attack == "receipt-swap":
        envelope["cgroup"]["systemd_slice_receipt"]["memory_swap_max_effective"] = "1"
    elif attack == "live-swap":
        envelope["cgroup"]["systemd_slice_live"]["memory_swap_bytes_max"] = 1
    elif attack == "control-group-path":
        envelope["cgroup"]["systemd_slice_live"]["path"] = (
            f"/foreign.slice/{envelope['cgroup']['container_parent']}"
        )
    else:
        envelope["job_start_time"] = "2026-07-30T12:00:00Z"

    assert list(validator.iter_errors(evidence))


def test_platform_health_accepts_systemd_mirror_and_keeps_peer_checks() -> None:
    evidence = _evidence()
    authority = _platform_health_authority(evidence)
    mixed_job = authority["mixed_jobs"][0]
    environment = ACCEPTANCE._registry_environments(evidence["registry_snapshot"])[
        mixed_job["sandbox"]
    ]
    pool = "oldlab" if mixed_job["node"] in ACCEPTANCE.EXPECTED_NODES[:5] else "gb10"
    _use_systemd_mirror(mixed_job, environment, pool=pool)
    authority["payload_sha256"] = hashlib.sha256(
        ACCEPTANCE._canonical_bytes(
            {key: value for key, value in authority.items() if key != "payload_sha256"},
        ),
    ).hexdigest()

    ACCEPTANCE._validate_platform_health_authority(
        authority,
        session_id=evidence["session"]["id"],
        registry_snapshot=evidence["registry_snapshot"],
        candidates={
            sandbox: {
                "sha": candidate["sha"],
                "tree": candidate["tree"],
            }
            for sandbox, candidate in evidence["candidates"].items()
        },
    )

    peer_before = copy.deepcopy(evidence["peer_workloads"])
    _validate_fixture_cgroup(evidence, mixed_job, pool=pool)
    assert evidence["peer_workloads"] == peer_before


def test_faults_require_exact_set_zero_orphans_and_bounded_recovery() -> None:
    evidence = _evidence()
    evidence["fault_recovery"][-1]["event"] = "cancel"

    assert any("fault recovery evidence" in failure for failure in _failures(evidence))

    evidence = _evidence()
    evidence["fault_recovery"][0]["recovered_at"] = _iso(50)
    assert any("recovery exceeded" in failure for failure in _failures(evidence))


def test_secret_like_input_is_rejected_without_echoing_value(tmp_path: Path) -> None:
    evidence = _evidence()
    evidence["api_token"] = "loom_api_DO_NOT_ECHO_123456"
    source = tmp_path / "unsafe.json"
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("verify", "--evidence", source)

    assert completed.returncode == 1
    assert "secret-like field" in completed.stdout
    assert "DO_NOT_ECHO" not in completed.stdout


def test_collect_canonicalizes_valid_evidence_without_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(_evidence()), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 0, completed.stdout
    assert json.loads(output.read_text(encoding="utf-8")) == _evidence()

    completed = _run("collect", "--input", source, "--output", output)
    assert completed.returncode == 1
    assert "cannot create acceptance artifact" in completed.stdout


def test_incomplete_evidence_is_not_collected(tmp_path: Path) -> None:
    evidence = _evidence()
    del evidence["fault_recovery"]
    source = tmp_path / "input.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 1
    assert not output.exists()
