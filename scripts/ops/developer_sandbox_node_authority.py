#!/usr/bin/python3 -I
"""Fixed host-root authority for one exact developer-sandbox node candidate.

A persistent root channel bootstraps this program from a clean, root-owned
exact checkout.  The channel may be host root or the repository's one-shot
Docker/chroot bootstrap.  The installed runtime exposes only ``transact`` and
``check`` through two fixed sudoers commands.  Requests arrive on stdin as a
closed canonical envelope; no path, program, user, or secret is accepted in
argv or the inherited environment.  A different SHA or tree is admitted only
through the persistent-root ``upgrade`` transaction, which snapshots and
restores the installed authority without reinitializing runtime receipts or
journals.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import fcntl
import grp
import hashlib
import importlib.util
import io
import json
import os
import pwd
import re
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, cast

REPO_ROOT: Final = Path(__file__).absolute().parents[2]
OPERATOR: Final = "qianyi"
LIBEXEC: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-authority")
SOURCE_ROOT: Final = Path("/opt/loom-developer-sandbox-node-authority/source")
POLICY: Final = Path("/etc/loom/developer-sandbox-node-authority.json")
SUDOERS: Final = Path("/etc/sudoers.d/loom-developer-sandbox-node-authority")
NODE_AUTHORITY_SUDOERS_PAYLOAD: Final = (
    b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
    b"/usr/local/libexec/loom-developer-sandbox-node-authority transact\n"
    b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
    b"/usr/local/libexec/loom-developer-sandbox-node-authority check\n"
)
STATE_ROOT: Final = Path("/var/lib/loom-developer-sandbox-node-authority")
LOCK: Final = STATE_ROOT / "authority.lock"
JOURNAL: Final = STATE_ROOT / "journal.jsonl"
RECEIPT_ROOT: Final = STATE_ROOT / "receipts"
IDENTITY_TRANSACTION_ROOT: Final = STATE_ROOT / "identity-transactions"
STAGING_BROKER_ROOT: Final = STATE_ROOT / "staging-broker"
STAGING_ACCOUNTING_ROOT: Final = STATE_ROOT / "staging-accounting"
STAGING_ACCOUNTING_JOURNAL: Final = STAGING_ACCOUNTING_ROOT / "journal.json"
STAGING_INFRASTRUCTURE_PRODUCER_ROOT: Final = STATE_ROOT / "staging-infrastructure-producer"
STAGING_INFRASTRUCTURE_PRODUCER_LOCK: Final = STAGING_INFRASTRUCTURE_PRODUCER_ROOT / "producer.lock"
STAGING_INFRASTRUCTURE_PRODUCER_JOURNAL: Final = (
    STAGING_INFRASTRUCTURE_PRODUCER_ROOT / "journal.json"
)
STAGING_INFRASTRUCTURE_PRODUCER_HIGH_WATER: Final = (
    STAGING_INFRASTRUCTURE_PRODUCER_ROOT / "high-water.json"
)
STAGING_INFRASTRUCTURE_PRODUCER_RECEIPTS: Final = STAGING_INFRASTRUCTURE_PRODUCER_ROOT / "receipts"
STAGING_INFRASTRUCTURE_RECEIPT_ROOT: Final = STATE_ROOT / "staging-infrastructure"
STAGING_INFRASTRUCTURE_INSTALL_LOCK: Final = STAGING_INFRASTRUCTURE_RECEIPT_ROOT / "install.lock"
STAGING_INFRASTRUCTURE_INSTALL_JOURNAL: Final = (
    STAGING_INFRASTRUCTURE_RECEIPT_ROOT / "install-journal.json"
)
STAGING_INFRASTRUCTURE_INSTALL_HIGH_WATER: Final = (
    STAGING_INFRASTRUCTURE_RECEIPT_ROOT / "high-water.json"
)
STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS: Final = (
    STAGING_INFRASTRUCTURE_RECEIPT_ROOT / "generations"
)
STAGING_GUARD_BINDING_PATH: Final = (
    STATE_ROOT.parent / "loom-developer-sandbox-slurm-policy/staging-binding-trt-gb10.json"
)
NODE_TRANSPORT: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-transport",
)
STAGE_ROOT: Final = Path("/run/loom-developer-sandbox-node-authority")
NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-developer-sandbox-node-authority.tmpfiles.conf",
)
NODE_AUTHORITY_TMPFILES: Final = Path(
    "/etc/tmpfiles.d/loom-developer-sandbox-node-authority.conf",
)
NODE_AUTHORITY_TMPFILES_DIRECTORIES: Final = (
    (Path("run/loom-developer-sandbox-node-authority"), 0o700),
)
STAGING_SHARED_TMPFILES_DIRECTORIES: Final = (
    (Path("srv/loom"), 0o755),
    (Path("srv/loom/staging-shared"), 0o755),
)
UPGRADE_ROOT: Final = STATE_ROOT / "upgrades"
UPGRADE_ACTIVE: Final = STATE_ROOT / "upgrade-active.json"
UPGRADE_JOURNAL: Final = STATE_ROOT / "upgrade-journal.jsonl"
DOMAIN_RUNTIME_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_domain_runtime.py",
)
REMOTE_LINK_HOST_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_remote_link_host.py",
)
LIVE_AUTHORITY_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_live_authority.py",
)
PLATFORM_HEALTH_AUTHORITY_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_platform_health_authority.py",
)
CAPACITY_CONTRACT_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_capacity_contract.py",
)
OLDLAB_CAPACITY_POLICY_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml",
)
GB10_CAPACITY_POLICY_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/shared-capacity-policies/gb10.toml",
)
PLATFORM_HEALTH_CONFIG_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/platform-health-authority.toml",
)
PLATFORM_HEALTH_SERVICE_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.service",
)
PLATFORM_HEALTH_SUDOERS_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.sudoers",
)
SLURM_RECOVERY_SERVICE_RELATIVE: Final = Path(
    "deploy/slurm/loom-developer-sandbox-slurm-recovery.service",
)
SLURM_RECOVERY_TIMER_RELATIVE: Final = Path(
    "deploy/slurm/loom-developer-sandbox-slurm-recovery.timer",
)
STAGING_PRESSURE_AUTHORITY_RELATIVE: Final = Path(
    "scripts/ops/staging_pressure_reclaim_authority.py",
)
STAGING_PRESSURE_CONFIG_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/staging-pressure-reclaim-authority.toml",
)
STAGING_PRESSURE_SERVICE_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-pressure-reclaim-authority.service",
)
STAGING_PRESSURE_SUDOERS_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-pressure-reclaim-authority.sudoers",
)
STAGING_EXTERNAL_AUTHORITY_RELATIVE: Final = Path(
    "scripts/ops/staging_external_slurm_acceptance_authority.py",
)
STAGING_EXTERNAL_CONSUMER_RELATIVE: Final = Path(
    "src/loom_cli/external_slurm_acceptance.py",
)
STAGING_EXTERNAL_CONFIG_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/staging-external-slurm-authority.toml",
)
STAGING_EXTERNAL_SERVICE_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.service",
)
STAGING_EXTERNAL_SUDOERS_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.sudoers",
)
STAGING_EXTERNAL_WRAPPER_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.wrapper",
)
HOST_AUTHORITY_RELATIVE: Final = Path(
    "scripts/ops/developer_sandbox_host.py",
)
RUNTIME_CONFIG_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/runtime-domains.toml",
)
SUDOERS_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-developer-sandbox-node-authority.sudoers",
)
SCHEMA_VERSION: Final = 1
MAX_REQUEST_BYTES: Final = 96 * 1024 * 1024
MAX_PAYLOAD_BYTES: Final = 64 * 1024 * 1024
SHA_RE: Final = frozenset("0123456789abcdef")
SAFE_RUNTIME_RE: Final = re.compile(r"[a-z][a-z0-9-]{1,31}\Z")
REGISTRY_SNAPSHOT: Final = Path(
    "/var/lib/loom-developer-environment-registry/current-snapshot.json",
)
REGISTRY_SNAPSHOT_ARCHIVE: Final = REGISTRY_SNAPSHOT.parent / "snapshots"
REGISTRY_SNAPSHOT_SYNC_ACTION: Final = "registry-snapshot-sync"
REGISTRY_SNAPSHOT_SYNC_KIND: Final = "developer-environment-registry-snapshot-json"
REGISTRY_SNAPSHOT_ARCHIVE_RE: Final = re.compile(
    r"registry-([1-9][0-9]*)-([0-9a-f]{64})\.json\Z",
)
REGISTRY_MODULE_RELATIVE: Final = Path(
    "scripts/ops/developer_environment_registry.py",
)
STAGING_SCOPE: Final = "staging"
STAGING_BOOT_ID_PATH: Final = Path("/proc/sys/kernel/random/boot_id")
STAGING_BOOT_ID_RE: Final = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z",
)
DOMAINS: Final = frozenset({"oldlab", "gb10"})
TRANSACT_ACTIONS: Final = frozenset(
    {
        "host-converge",
        "materialize",
        "install-client",
        "attest",
        "rollback",
        "persist-fleet-attestation",
        "slurm-node-converge",
        "slurm-controller-converge",
        "slurm-identity-converge",
        "slurm-identity-retire",
        "developer-environment-acceptance-probe",
        "developer-environment-runtime-retire",
        REGISTRY_SNAPSHOT_SYNC_ACTION,
        "slurm-rollback",
        "collect-live-overlap",
        "staging-allocation-bootstrap",
        "staging-allocation-probe",
        "staging-allocation-submit",
        "staging-allocation-cancel",
        "staging-shared-source-bootstrap",
        "staging-slurm-accounting-converge",
        "staging-infrastructure-converge",
        "staging-infrastructure-install",
    },
)
CHECK_ACTIONS: Final = frozenset(
    {
        "inspect-candidate",
        "inspect-local",
        "inspect-link-client",
        "inspect-link-server",
        "export-domain-attestation",
        "export-runtime-proof-artifact",
        "slurm-check",
        "slurm-identity-preflight",
        "slurm-identity-inventory",
        "observe-live-overlap-job",
        "observe-platform-health-node",
        "staging-pressure-reclaim-observe",
    },
)
STAGING_ACTIONS: Final = frozenset(
    {
        "staging-allocation-bootstrap",
        "staging-allocation-probe",
        "staging-allocation-submit",
        "staging-allocation-cancel",
        "staging-shared-source-bootstrap",
        "staging-slurm-accounting-converge",
        "staging-infrastructure-converge",
        "staging-infrastructure-install",
        "staging-pressure-reclaim-observe",
    },
)
SLURM_ACTIONS: Final = frozenset(
    {
        "slurm-node-converge",
        "slurm-controller-converge",
        "slurm-rollback",
        "slurm-check",
        "slurm-identity-converge",
        "slurm-identity-retire",
        "slurm-identity-inventory",
        "slurm-identity-preflight",
    },
)
UPGRADE_PHASES: Final = frozenset(
    {
        "prepared",
        "admission-disabled",
        "assets-replaced",
        "committed",
        "rolled-back",
        "recovered-committed",
        "recovered-rolled-back",
    },
)
PAYLOAD_KIND: Final = {
    "host-converge": "none",
    "materialize": "git-bundle",
    "install-client": "client-credentials",
    "attest": "attestation-seed",
    "rollback": "none",
    "persist-fleet-attestation": "fleet-attestation-json",
    "slurm-node-converge": "slurm-candidate-set-json",
    "slurm-controller-converge": "slurm-candidate-set-json",
    "slurm-identity-converge": "developer-environment-identity-preflight-json",
    "slurm-identity-retire": "developer-environment-identity-preflight-json",
    "slurm-rollback": "slurm-candidate-set-json",
    "collect-live-overlap": "live-overlap-collection-json",
    "staging-allocation-bootstrap": "staging-infrastructure-operation-request",
    "staging-allocation-probe": "staging-allocation-probe-request",
    "staging-allocation-submit": "staging-allocation-submit-request",
    "staging-allocation-cancel": "staging-allocation-cancel-request",
    "staging-shared-source-bootstrap": "staging-infrastructure-operation-request",
    "staging-slurm-accounting-converge": "staging-infrastructure-operation-request",
    "staging-infrastructure-converge": "staging-infrastructure-converge-request",
    "staging-infrastructure-install": "staging-infrastructure-receipt-json",
    "inspect-candidate": "none",
    "inspect-local": "none",
    "inspect-link-client": "none",
    "inspect-link-server": "none",
    "export-domain-attestation": "none",
    "export-runtime-proof-artifact": "runtime-proof-artifact-id",
    "slurm-check": "slurm-candidate-set-json",
    "slurm-identity-preflight": "developer-environment-identity-preflight-json",
    "slurm-identity-inventory": "developer-environment-identity-inventory-json",
    "developer-environment-acceptance-probe": ("developer-environment-acceptance-probe-json"),
    "developer-environment-runtime-retire": ("developer-environment-runtime-retire-json"),
    REGISTRY_SNAPSHOT_SYNC_ACTION: REGISTRY_SNAPSHOT_SYNC_KIND,
    "observe-live-overlap-job": "live-overlap-job-json",
    "observe-platform-health-node": "platform-health-node-json",
    "staging-pressure-reclaim-observe": "staging-pressure-reclaim-observe-request",
}
DYNAMIC_TARGET_ACTIONS: Final = frozenset(
    {
        "host-converge",
        "materialize",
        "install-client",
        "attest",
        "rollback",
        "persist-fleet-attestation",
        "inspect-candidate",
        "inspect-local",
        "inspect-link-client",
        "inspect-link-server",
        "export-domain-attestation",
        "export-runtime-proof-artifact",
        "collect-live-overlap",
        "observe-live-overlap-job",
        "observe-platform-health-node",
        "slurm-identity-preflight",
        "slurm-identity-converge",
        "slurm-identity-retire",
    },
)
REQUEST_FIELDS: Final = {
    "schema_version",
    "request_id",
    "action",
    "node",
    "domain",
    "sandbox",
    "candidate_sha",
    "candidate_tree",
    "payload_kind",
    "payload_sha256",
    "payload_base64",
    "prior_request_id",
}
DYNAMIC_TARGET_BINDING_FIELDS: Final = {
    "env_id",
    "resource_generation",
    "candidate_id",
    "registry_generation",
    "registry_payload_sha256",
}
DEPLOYMENT_TARGET_ACTIONS: Final = frozenset(
    {
        "slurm-identity-preflight",
        "slurm-identity-converge",
        "slurm-identity-retire",
    },
)
DEPLOYMENT_TARGET_BINDING_FIELDS: Final = {"deployment_id"}
DYNAMIC_REQUEST_FIELDS: Final = REQUEST_FIELDS | DYNAMIC_TARGET_BINDING_FIELDS
REGISTRY_SNAPSHOT_SYNC_FIELDS: Final = REQUEST_FIELDS | {
    "registry_generation",
    "registry_payload_sha256",
}
RECEIPT_FIELDS: Final = {
    "schema_version",
    "request_id",
    "action",
    "node",
    "domain",
    "sandbox",
    "candidate_sha",
    "candidate_tree",
    "payload_sha256",
    "result_sha256",
    "inner_receipt",
    "completed_at",
    "status",
}
DYNAMIC_RECEIPT_FIELDS: Final = RECEIPT_FIELDS | DYNAMIC_TARGET_BINDING_FIELDS
REGISTRY_SNAPSHOT_SYNC_RECEIPT_FIELDS: Final = RECEIPT_FIELDS | {
    "registry_generation",
    "registry_payload_sha256",
    "source_sha",
    "source_tree",
}
JOURNAL_FIELDS: Final = {
    "schema_version",
    "request_id",
    "action",
    "candidate_sha",
    "candidate_tree",
    "result_sha256",
    "completed_at",
    "status",
}
REGISTRY_SNAPSHOT_SYNC_JOURNAL_FIELDS: Final = JOURNAL_FIELDS | {
    "registry_generation",
    "registry_payload_sha256",
}
LIVE_COLLECTION_FIELDS: Final = {
    "schema_version",
    "kind",
    "collection_id",
    "candidate_tree",
    "job_id",
}
LIVE_SLURM_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "source_host",
    "sandbox",
    "pool",
    "candidate_sha",
    "candidate_tree",
    "job_id",
    "account",
    "user",
    "job_name",
    "node",
    "requested_cpus",
    "requested_memory_mib",
    "job_pids_max",
    "requested_gpus",
    "requested_gpu_tres",
}
PLATFORM_HEALTH_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "session_id",
    "checkpoint",
    "checkpoint_group",
    "expected_node",
    "expected_host",
    "since_at",
    "candidates",
}
IDENTITY_PREFLIGHT_FIELDS: Final = {
    "schema_version",
    "kind",
    "env_id",
    "principal_id",
    "resource_generation",
    "service_user",
    "service_group",
    "uid",
    "gid",
    "slurm_account",
    "slurm_qos",
    "registry_generation",
    "registry_payload_sha256",
    "candidate_set_sha256",
    "revive_journal_sha256",
}
IDENTITY_PREFLIGHT_KIND: Final = "loom.developer-environment.identity-preflight"
IDENTITY_PREFLIGHT_RESULT_KIND: Final = "loom.developer-environment.identity-preflight-result"
IDENTITY_CONVERGENCE_RESULT_KIND: Final = "loom.developer-environment.identity-convergence-result"
IDENTITY_INVENTORY_KIND: Final = "loom.developer-environment.identity-inventory-request"
IDENTITY_INVENTORY_RESULT_KIND: Final = "loom.developer-environment.identity-inventory-result"
FLEET_BOOTSTRAP_SCOPE: Final = "fleet-bootstrap"
IDENTITY_INVENTORY_FIELDS: Final = {
    "schema_version",
    "kind",
    "uid_start",
    "uid_end",
    "registry_generation",
    "registry_payload_sha256",
}
IDENTITY_UID_START: Final = 32_000
IDENTITY_UID_END: Final = 60_000
ACCEPTANCE_PROBE_ACTION: Final = "developer-environment-acceptance-probe"
ACCEPTANCE_PROBE_REQUEST_KIND: Final = "loom.developer-environment.acceptance-probe-domain-request"
ACCEPTANCE_PROBE_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "action",
    "domain",
    "cluster",
    "submit_host",
    "controller",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "applied_resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "service_user",
    "slurm_account",
    "slurm_qos",
    "job_name",
    "time_limit_seconds",
    "health_services",
    "general_admission_authorized",
    "foreign_job_action",
    "idempotency_key",
    "payload_sha256",
}
RUNTIME_RETIRE_ACTION: Final = "developer-environment-runtime-retire"
RUNTIME_RETIRE_REQUEST_KIND: Final = "loom.developer-environment.runtime-retire-node-request"
RUNTIME_RETIRE_RECEIPT_KIND: Final = "loom.developer-environment.runtime-retire-node-receipt"
RUNTIME_RETIRE_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "action",
    "node",
    "domain",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "retire_operation_sha256",
    "current_candidate_id",
    "candidate_bindings",
    "foreign_path_action",
    "audit_action",
    "payload_sha256",
}
RUNTIME_RETIRE_ABSENCE_FIELDS: Final = {
    "link_client_credentials",
    "tls_private_keys",
    "token_files",
    "domain_environment",
    "domain_config",
    "candidate_material",
    "active_attestation_pointers",
}
RUNTIME_RETIRE_ROOT: Final = Path(
    "/var/lib/loom-developer-environment-runtime-retire",
)
RUNTIME_RETIRE_WAL_ROOT: Final = Path(
    "/var/lib/loom-developer-environment-runtime/lifecycle/retire",
)
RUNTIME_RETIRE_WAL_FIELDS: Final = {
    "schema_version",
    "kind",
    "phase",
    "env_id",
    "principal_id",
    "runtime_id",
    "uid",
    "gid",
    "service_user",
    "service_group",
    "slurm_user",
    "slurm_account",
    "slurm_qos",
    "expected_resource_generation",
    "current_candidate_id",
    "idempotency_key",
    "evidence",
    "object_checkpoints",
    "created_at",
    "updated_at",
    "payload_sha256",
}
ACCEPTANCE_PROBE_ROUTE: Final = {
    "oldlab": {
        "cluster": "trt-oldlab",
        "node": "oldlab-2",
        "submit_host": "trt-EAI-OLDLAB-2",
        "controller": "TRT-EAI-OLDLAB-1",
    },
    "gb10": {
        "cluster": "trt-gb10",
        "node": "trt-gb10-1",
        "submit_host": "trt-gb10-1",
        "controller": "trt-gb10-1",
    },
}
ACCEPTANCE_PROBE_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
    "action",
    "domain",
    "cluster",
    "submit_host",
    "controller",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "applied_resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "probe_request_sha256",
    "transport_request_id",
    "submission_count",
    "job",
    "health",
    "terminal",
    "job_output_sha256",
    "authority_receipt_sha256",
    "completed_at",
    "payload_sha256",
}
ACCEPTANCE_PROBE_JOB_FIELDS: Final = {
    "job_id",
    "job_name",
    "user",
    "account",
    "qos",
    "submit_host",
    "controller",
    "allocation_nodes",
    "time_limit_seconds",
}
ACCEPTANCE_PROBE_HEALTH_FIELDS: Final = {
    "service",
    "status",
    "http_status",
    "candidate_binding_sha256",
    "response_sha256",
}
IDENTITY_TRANSACTION_FIELDS: Final = {
    "schema_version",
    "kind",
    "request_id",
    "payload_sha256",
    "node",
    "domain",
    "env_id",
    "service_user",
    "service_group",
    "uid",
    "gid",
    "phase",
    "created_at",
    "updated_at",
}
STAGING_ALLOCATION_PROBE_FIELDS: Final = {
    "schema_version",
    "kind",
    "request_id",
    "candidate_sha",
    "candidate_tree",
}
STAGING_ALLOCATION_SUBMIT_FIELDS: Final = {
    *STAGING_ALLOCATION_PROBE_FIELDS,
    "requested_node",
}
STAGING_ALLOCATION_CANCEL_FIELDS: Final = {
    *STAGING_ALLOCATION_SUBMIT_FIELDS,
    "job_id",
    "submit_request_id",
}
STAGING_INFRASTRUCTURE_OPERATION_FIELDS: Final = {
    "schema_version",
    "kind",
    "request_id",
    "action",
    "node",
    "candidate_sha",
    "candidate_tree",
    "generation",
    "convergence_id",
    "requested_at",
}
STAGING_INFRASTRUCTURE_CONVERGE_FIELDS: Final = {
    "schema_version",
    "kind",
    "candidate_sha",
    "candidate_tree",
    "convergence_id",
    "requested_at",
}
STAGING_INFRASTRUCTURE_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "candidate_sha",
    "candidate_tree",
    "generation",
    "convergence_id",
    "requested_at",
    "request_sha256",
    "source_controller",
    "source_controller_host",
    "created_at",
    "expires_at",
    "source_bootstrap",
    "accounting",
    "node_bootstraps",
    "mount_contract",
    "result",
}
STAGING_INFRASTRUCTURE_NODES: Final = tuple(f"trt-gb10-{index}" for index in range(1, 16))
STAGING_INFRASTRUCTURE_MAX_TRANSACTION_SECONDS: Final = 3660
STAGING_PRESSURE_OBSERVE_FIELDS: Final = {
    "schema_version",
    "kind",
    "source_host",
    "submit_host",
    "environment",
    "pool",
    "partition",
    "account",
    "qos",
    "phase",
    "session_id",
    "acceptance_session_id",
    "candidate_sha",
    "candidate_tree",
    "owned_jobs",
}
SOURCE_ASSETS: Final = (
    Path("scripts/ops/developer_sandbox_node_authority.py"),
    Path("scripts/ops/developer_sandbox_node_docker_request.py"),
    Path("scripts/ops/developer_sandbox_node_transport.py"),
    REGISTRY_MODULE_RELATIVE,
    LIVE_AUTHORITY_RELATIVE,
    PLATFORM_HEALTH_AUTHORITY_RELATIVE,
    CAPACITY_CONTRACT_RELATIVE,
    STAGING_PRESSURE_AUTHORITY_RELATIVE,
    HOST_AUTHORITY_RELATIVE,
    DOMAIN_RUNTIME_RELATIVE,
    REMOTE_LINK_HOST_RELATIVE,
    Path("scripts/ops/developer_sandbox_remote_link.py"),
    Path("scripts/ops/developer_sandbox_slurm_policy.py"),
    Path("scripts/ops/developer_environment_acceptance_probe_container.py"),
    Path("scripts/ops/developer_environment_runtime_retire.py"),
    Path("scripts/ops/slurm_job_cgroup_guard.py"),
    Path("src/loom_control_plane/slurm_job_cgroup.py"),
    RUNTIME_CONFIG_RELATIVE,
    Path("deploy/docker-compose.remote-worker.yml"),
    Path("deploy/docker-compose.remote-worker.sandbox-link.yml"),
    Path("deploy/docker-compose.remote-worker.cgroup-parent.yml"),
    Path("deploy/docker-compose.remote-worker.acceptance-probe.yml"),
    Path("deploy/developer-sandboxes/node-authority-transport.toml"),
    PLATFORM_HEALTH_CONFIG_RELATIVE,
    OLDLAB_CAPACITY_POLICY_RELATIVE,
    GB10_CAPACITY_POLICY_RELATIVE,
    STAGING_PRESSURE_CONFIG_RELATIVE,
    STAGING_PRESSURE_SERVICE_RELATIVE,
    STAGING_PRESSURE_SUDOERS_RELATIVE,
    STAGING_EXTERNAL_AUTHORITY_RELATIVE,
    STAGING_EXTERNAL_CONSUMER_RELATIVE,
    STAGING_EXTERNAL_CONFIG_RELATIVE,
    STAGING_EXTERNAL_SERVICE_RELATIVE,
    STAGING_EXTERNAL_SUDOERS_RELATIVE,
    STAGING_EXTERNAL_WRAPPER_RELATIVE,
    NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
    Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
    Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
    PLATFORM_HEALTH_SERVICE_RELATIVE,
    PLATFORM_HEALTH_SUDOERS_RELATIVE,
    Path("deploy/developer-sandboxes/loom-developer-sandbox-link@.service"),
    Path("deploy/slurm/developer-sandboxes/oldlab.toml"),
    Path("deploy/slurm/developer-sandboxes/gb10.toml"),
    Path("deploy/slurm/loom-slurm-job-cgroup-guard.service"),
    SLURM_RECOVERY_SERVICE_RELATIVE,
    SLURM_RECOVERY_TIMER_RELATIVE,
    SUDOERS_RELATIVE,
)
SOURCE_ASSET_PARENT_PATHS: Final = tuple(
    sorted(
        {parent for asset in SOURCE_ASSETS for parent in asset.parents if parent != Path(".")},
        key=lambda path: (len(path.parts), path.as_posix()),
    ),
)
MIGRATABLE_EXTERNAL_SOURCE_ASSETS: Final = frozenset(
    {
        Path("scripts/ops/developer_sandbox_node_docker_request.py"),
        SLURM_RECOVERY_SERVICE_RELATIVE,
        SLURM_RECOVERY_TIMER_RELATIVE,
        STAGING_EXTERNAL_AUTHORITY_RELATIVE,
        STAGING_EXTERNAL_CONSUMER_RELATIVE,
        STAGING_EXTERNAL_CONFIG_RELATIVE,
        STAGING_EXTERNAL_SERVICE_RELATIVE,
        STAGING_EXTERNAL_SUDOERS_RELATIVE,
        STAGING_EXTERNAL_WRAPPER_RELATIVE,
        NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
        Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
        Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
    },
)
LEGACY_V1_POLICY_SOURCE_ASSETS: Final = (
    Path("deploy/developer-sandboxes/loom-developer-sandbox-link@.service"),
    Path("deploy/developer-sandboxes/loom-developer-sandbox-node-authority.sudoers"),
    Path("deploy/developer-sandboxes/loom-developer-sandbox-node-authority.tmpfiles.conf"),
    Path(
        "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.service",
    ),
    Path(
        "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.sudoers",
    ),
    Path("deploy/developer-sandboxes/loom-staging-external-slurm-authority.service"),
    Path("deploy/developer-sandboxes/loom-staging-external-slurm-authority.sudoers"),
    Path("deploy/developer-sandboxes/loom-staging-external-slurm-authority.wrapper"),
    Path("deploy/developer-sandboxes/loom-staging-pressure-reclaim-authority.service"),
    Path("deploy/developer-sandboxes/loom-staging-pressure-reclaim-authority.sudoers"),
    Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
    Path("deploy/developer-sandboxes/node-authority-transport.toml"),
    Path("deploy/developer-sandboxes/platform-health-authority.toml"),
    Path("deploy/developer-sandboxes/remote-links/devansh.toml"),
    Path("deploy/developer-sandboxes/remote-links/hongjian.toml"),
    Path("deploy/developer-sandboxes/remote-links/qianyi.toml"),
    Path("deploy/developer-sandboxes/runtime-domains.toml"),
    Path("deploy/developer-sandboxes/shared-capacity-policies/gb10.toml"),
    Path("deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml"),
    Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
    Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    Path("deploy/developer-sandboxes/staging-pressure-reclaim-authority.toml"),
    Path("deploy/slurm/developer-sandboxes/gb10.toml"),
    Path("deploy/slurm/developer-sandboxes/oldlab.toml"),
    Path("deploy/slurm/loom-developer-sandbox-slurm-recovery.service"),
    Path("deploy/slurm/loom-developer-sandbox-slurm-recovery.timer"),
    Path("deploy/slurm/loom-slurm-job-cgroup-guard.service"),
    Path("scripts/ops/developer_sandbox_capacity_contract.py"),
    Path("scripts/ops/developer_sandbox_domain_runtime.py"),
    Path("scripts/ops/developer_sandbox_host.py"),
    Path("scripts/ops/developer_sandbox_live_authority.py"),
    Path("scripts/ops/developer_sandbox_node_authority.py"),
    Path("scripts/ops/developer_sandbox_node_docker_request.py"),
    Path("scripts/ops/developer_sandbox_node_transport.py"),
    Path("scripts/ops/developer_sandbox_platform_health_authority.py"),
    Path("scripts/ops/developer_sandbox_remote_link.py"),
    Path("scripts/ops/developer_sandbox_remote_link_host.py"),
    Path("scripts/ops/developer_sandbox_slurm_policy.py"),
    Path("scripts/ops/slurm_job_cgroup_guard.py"),
    Path("scripts/ops/staging_external_slurm_acceptance_authority.py"),
    Path("scripts/ops/staging_pressure_reclaim_authority.py"),
    Path("src/loom_cli/external_slurm_acceptance.py"),
)
LEGACY_V1_POLICY_ASSET_KEYS: Final = frozenset(
    str(relative) for relative in LEGACY_V1_POLICY_SOURCE_ASSETS
)
CURRENT_POLICY_ASSET_KEYS: Final = frozenset(str(relative) for relative in SOURCE_ASSETS)
LEGACY_V1_MIGRATABLE_SOURCE_ASSETS: Final = frozenset(
    {
        Path("deploy/docker-compose.remote-worker.acceptance-probe.yml"),
        Path("deploy/docker-compose.remote-worker.cgroup-parent.yml"),
        Path("deploy/docker-compose.remote-worker.sandbox-link.yml"),
        Path("deploy/docker-compose.remote-worker.yml"),
        Path("scripts/ops/developer_environment_acceptance_probe_container.py"),
        Path("scripts/ops/developer_environment_registry.py"),
        Path("scripts/ops/developer_environment_runtime_retire.py"),
        Path("src/loom_control_plane/slurm_job_cgroup.py"),
    },
)
RETIRED_LEGACY_SOURCE_ASSETS: Final = tuple(
    relative
    for relative in LEGACY_V1_POLICY_SOURCE_ASSETS
    if str(relative) not in CURRENT_POLICY_ASSET_KEYS
)
PLATFORM_HEALTH_LIBEXEC: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-platform-health-authority",
)
CAPACITY_CONTRACT_LIBEXEC: Final = Path(
    "/usr/local/libexec/scripts/ops/developer_sandbox_capacity_contract.py",
)
PLATFORM_HEALTH_SERVICE: Final = Path(
    "/etc/systemd/system/loom-developer-sandbox-platform-health-authority.service",
)
PLATFORM_HEALTH_SUDOERS: Final = Path(
    "/etc/sudoers.d/loom-developer-sandbox-platform-health-authority",
)
SLURM_RECOVERY_LIBEXEC: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-slurm-recovery",
)
SLURM_REGISTRY_CONTRACT_LIBEXEC: Final = Path(
    "/usr/local/libexec/developer_environment_registry.py",
)
SLURM_RECOVERY_SERVICE: Final = Path(
    "/etc/systemd/system/loom-developer-sandbox-slurm-recovery.service",
)
SLURM_RECOVERY_TIMER: Final = Path(
    "/etc/systemd/system/loom-developer-sandbox-slurm-recovery.timer",
)
SLURM_RECOVERY_TIMER_UNIT: Final = "loom-developer-sandbox-slurm-recovery.timer"
STAGING_PRESSURE_LIBEXEC: Final = Path(
    "/usr/local/libexec/loom-staging-pressure-reclaim-authority",
)
STAGING_PRESSURE_CONFIG: Final = Path(
    "/etc/loom/staging-pressure-reclaim-authority.toml",
)
STAGING_PRESSURE_SERVICE: Final = Path(
    "/etc/systemd/system/loom-staging-pressure-reclaim-authority.service",
)
STAGING_PRESSURE_SUDOERS: Final = Path(
    "/etc/sudoers.d/loom-staging-pressure-reclaim-authority",
)
STAGING_EXTERNAL_INSTALL_ROOT: Final = Path(
    "/usr/local/lib/loom-staging-external-slurm-authority",
)
STAGING_EXTERNAL_SOURCE: Final = (
    STAGING_EXTERNAL_INSTALL_ROOT / "staging_external_slurm_acceptance_authority.py"
)
STAGING_EXTERNAL_CONSUMER: Final = (
    STAGING_EXTERNAL_INSTALL_ROOT / "loom_cli/external_slurm_acceptance.py"
)
STAGING_EXTERNAL_WRAPPER: Final = Path(
    "/usr/local/libexec/loom-staging-external-slurm-authority",
)
STAGING_EXTERNAL_CONFIG: Final = Path(
    "/etc/loom/staging-external-slurm-authority/authority.toml",
)
STAGING_EXTERNAL_SERVICE: Final = Path(
    "/etc/systemd/system/loom-staging-external-slurm-authority.service",
)
STAGING_EXTERNAL_SUDOERS: Final = Path(
    "/etc/sudoers.d/loom-staging-external-slurm-authority",
)
STAGING_EXTERNAL_MOUNT: Final = Path(
    r"/etc/systemd/system/srv-loom-staging\x2dshared.mount",
)
STAGING_EXTERNAL_TMPFILES: Final = Path(
    "/etc/tmpfiles.d/loom-staging-shared.conf",
)
SYSTEM_INSTALL_ASSETS: Final = (
    (
        Path("scripts/ops/developer_sandbox_slurm_policy.py"),
        SLURM_RECOVERY_LIBEXEC,
        0o755,
        0o755,
    ),
    (
        REGISTRY_MODULE_RELATIVE,
        SLURM_REGISTRY_CONTRACT_LIBEXEC,
        0o644,
        0o755,
    ),
    (
        SLURM_RECOVERY_SERVICE_RELATIVE,
        SLURM_RECOVERY_SERVICE,
        0o644,
        0o755,
    ),
    (
        SLURM_RECOVERY_TIMER_RELATIVE,
        SLURM_RECOVERY_TIMER,
        0o644,
        0o755,
    ),
    (
        PLATFORM_HEALTH_AUTHORITY_RELATIVE,
        PLATFORM_HEALTH_LIBEXEC,
        0o755,
        0o755,
    ),
    (
        CAPACITY_CONTRACT_RELATIVE,
        CAPACITY_CONTRACT_LIBEXEC,
        0o644,
        0o755,
    ),
    (
        PLATFORM_HEALTH_SERVICE_RELATIVE,
        PLATFORM_HEALTH_SERVICE,
        0o644,
        0o755,
    ),
    (
        PLATFORM_HEALTH_SUDOERS_RELATIVE,
        PLATFORM_HEALTH_SUDOERS,
        0o440,
        0o755,
    ),
    (
        STAGING_PRESSURE_AUTHORITY_RELATIVE,
        STAGING_PRESSURE_LIBEXEC,
        0o755,
        0o755,
    ),
    (
        STAGING_PRESSURE_CONFIG_RELATIVE,
        STAGING_PRESSURE_CONFIG,
        0o600,
        0o755,
    ),
    (
        STAGING_PRESSURE_SERVICE_RELATIVE,
        STAGING_PRESSURE_SERVICE,
        0o644,
        0o755,
    ),
    (
        STAGING_PRESSURE_SUDOERS_RELATIVE,
        STAGING_PRESSURE_SUDOERS,
        0o440,
        0o755,
    ),
    (
        STAGING_EXTERNAL_AUTHORITY_RELATIVE,
        STAGING_EXTERNAL_SOURCE,
        0o644,
        0o755,
    ),
    (
        STAGING_EXTERNAL_CONSUMER_RELATIVE,
        STAGING_EXTERNAL_CONSUMER,
        0o644,
        0o755,
    ),
    (
        STAGING_EXTERNAL_WRAPPER_RELATIVE,
        STAGING_EXTERNAL_WRAPPER,
        0o755,
        0o755,
    ),
    (
        STAGING_EXTERNAL_CONFIG_RELATIVE,
        STAGING_EXTERNAL_CONFIG,
        0o600,
        0o700,
    ),
    (
        STAGING_EXTERNAL_SERVICE_RELATIVE,
        STAGING_EXTERNAL_SERVICE,
        0o644,
        0o755,
    ),
    (
        STAGING_EXTERNAL_SUDOERS_RELATIVE,
        STAGING_EXTERNAL_SUDOERS,
        0o440,
        0o755,
    ),
    (
        Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
        STAGING_EXTERNAL_MOUNT,
        0o644,
        0o755,
    ),
    (
        Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
        STAGING_EXTERNAL_TMPFILES,
        0o644,
        0o755,
    ),
    (
        NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
        NODE_AUTHORITY_TMPFILES,
        0o644,
        0o755,
    ),
)
NODE_HOSTNAMES: Final = {
    **{f"oldlab-{index}": f"trt-eai-oldlab-{index}" for index in range(1, 6)},
    "trt-gb10-1": "gx10-01c7",
    "trt-gb10-2": "gx10-0fca",
    "trt-gb10-3": "gx10-0f0d",
    "trt-gb10-4": "gx10-0d93",
    "trt-gb10-5": "gx10-1036",
    "trt-gb10-6": "gx10-1000",
    "trt-gb10-7": "gx10-0faf",
    "trt-gb10-8": "gx10-db22",
    "trt-gb10-9": "gx10-16f6",
    "trt-gb10-10": "gx10-0f82",
    "trt-gb10-11": "gx10-c38b",
    "trt-gb10-12": "gx10-e45f",
    "trt-gb10-13": "gx10-fc5d",
    "trt-gb10-14": "gx10-0a49",
    "trt-gb10-15": "gx10-0152",
}
BOOTSTRAP_DIRECTORIES: Final = (
    (SOURCE_ROOT.parent, 0o755, 0o755),
    (SOURCE_ROOT, 0o755, 0o755),
    *((SOURCE_ROOT / parent, 0o755, 0o755) for parent in SOURCE_ASSET_PARENT_PATHS),
    (LIBEXEC.parent, 0o755, 0o755),
    (POLICY.parent, 0o755, 0o755),
    (SUDOERS.parent, 0o755, 0o755),
    (PLATFORM_HEALTH_SERVICE.parent, 0o755, 0o755),
    (STAGING_EXTERNAL_INSTALL_ROOT, 0o755, 0o755),
    (STAGING_EXTERNAL_CONSUMER.parent, 0o755, 0o755),
    (STAGING_EXTERNAL_CONFIG.parent, 0o700, 0o755),
    (STATE_ROOT, 0o700, 0o755),
    (RECEIPT_ROOT, 0o700, 0o700),
    (IDENTITY_TRANSACTION_ROOT, 0o700, 0o700),
    (STAGING_BROKER_ROOT, 0o700, 0o700),
    (STAGING_ACCOUNTING_ROOT, 0o700, 0o700),
    (STAGING_INFRASTRUCTURE_PRODUCER_ROOT, 0o700, 0o700),
    (STAGING_INFRASTRUCTURE_PRODUCER_RECEIPTS, 0o700, 0o700),
    (STAGING_INFRASTRUCTURE_RECEIPT_ROOT, 0o700, 0o700),
    (STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS, 0o700, 0o700),
    (UPGRADE_ROOT, 0o700, 0o700),
    (STAGE_ROOT, 0o700, 0o755),
)
SLURM_CLUSTER: Final = {"oldlab": "trt-oldlab", "gb10": "trt-gb10"}
SLURM_CONTROLLER: Final = {"oldlab": "oldlab-1", "gb10": "trt-gb10-1"}
SLURM_PROFILE_NAME: Final = {"oldlab": "oldlab.toml", "gb10": "gb10.toml"}
SLURM_POLICY_RELATIVE: Final = Path("scripts/ops/developer_sandbox_slurm_policy.py")
SLURM_STATE_ROOT: Final = Path("/var/lib/loom-developer-sandbox-slurm-policy")
SLURM_TRANSACTION_ROOT: Final = SLURM_STATE_ROOT / "transactions"
STAGING_SERVICE_USER: Final = "loom-staging-worker"
STAGING_SERVICE_GROUP: Final = "loom-staging-worker"
STAGING_SERVICE_UID: Final = 31024
STAGING_SERVICE_GID: Final = 31024
STAGING_SERVICE_HOME: Final = Path("/nonexistent")
STAGING_SERVICE_SHELL: Final = "/usr/sbin/nologin"
STAGING_SUPPLEMENTARY_GROUPS: Final = ("docker",)
STAGING_SHARED_ROOT: Final = Path("/srv/loom/staging-shared")
STAGING_SHARED_PATHS: Final = tuple(
    STAGING_SHARED_ROOT / name for name in ("candidates", "generated", "results")
)
STAGING_MOUNT_UNIT: Final = r"srv-loom-staging\x2dshared.mount"
STAGING_MOUNT_SOURCE_RELATIVE: Final = Path(
    r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount",
)
STAGING_TMPFILES_SOURCE_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf",
)
STAGING_MOUNT_UNIT_PATH: Final = Path("/etc/systemd/system") / STAGING_MOUNT_UNIT
STAGING_TMPFILES_PATH: Final = Path("/usr/lib/tmpfiles.d/loom-staging-shared.conf")
STAGING_RAW_SOURCE_ROOT: Final = Path("/shared_work2/loom/staging")
SLURM_SNAPSHOT_ROOT: Final = SLURM_STATE_ROOT / "snapshots"
SLURM_SNAPSHOT_RELATIVE_PATHS: Final = (
    "etc/slurm/slurm.conf",
    "etc/slurm/cgroup.conf",
    "etc/docker/daemon.json",
    "usr/libexec/loom-slurm-job-cgroup-guard",
    "etc/loom/slurm-job-cgroup-guard.json",
    "etc/systemd/system/loom-slurm-job-cgroup-guard.service",
)
SLURM_SNAPSHOT_ROW_FIELDS: Final = {
    "path",
    "present",
    "mode",
    "uid",
    "gid",
    "nlink",
    "size",
    "sha256",
}
SLURM_POLICY_JOURNAL_COMMON_FIELDS: Final = {
    "schema_version",
    "operation",
    "cluster",
    "host",
    "slurm_node",
    "candidate_sha",
    "candidate_set_sha256",
    "candidate_bindings",
    "transaction_id",
    "candidate_set_generation",
    "candidate_set_convergence_id",
    "candidate_set_payload_sha256",
    "snapshot",
    "accounting_snapshot",
    "restart",
    "apply_accounting",
    "phase",
    "created_at",
    "updated_at",
}
SLURM_BINDING_RE: Final = re.compile(
    r"^slurm-policy-v1:(trt-oldlab|trt-gb10):([0-9a-f]{64}):([0-9a-f]{64})$",
)
CLIENT_ARCHIVE_FILES: Final = {
    "ca.pem",
    "client.pem",
    "client-key.pem",
    "worker-token",
    "minio-access-key",
    "minio-secret-key",
}
ATTESTATION_ARCHIVE_FILES: Final = {"worker.env", "fleet.json"}
RUNTIME_PROOF_ARTIFACT_NAMES: Final = frozenset(
    {
        "combined.json",
        "fleet.json",
        "oldlab.json",
        "oldlab.sig",
        "oldlab.pub",
        "gb10.json",
        "gb10.sig",
        "gb10.pub",
    },
)
RUNTIME_PROOF_ARTIFACT_SOURCES: Final = {
    "combined.json": ("oldlab", "oldlab-2"),
    "fleet.json": ("oldlab", "oldlab-2"),
    "oldlab.json": ("oldlab", "oldlab-1"),
    "oldlab.sig": ("oldlab", "oldlab-1"),
    "oldlab.pub": ("oldlab", "oldlab-1"),
    "gb10.json": ("gb10", "trt-gb10-1"),
    "gb10.sig": ("gb10", "trt-gb10-1"),
    "gb10.pub": ("gb10", "trt-gb10-1"),
}
MAX_HELPER_STDOUT_BYTES: Final = 1536 * 1024
MAX_FLEET_ATTESTATION_BYTES: Final = 1 << 20
MAX_SLURM_POLICY_SURFACE_BYTES: Final = 1 << 20


class NodeAuthorityError(RuntimeError):
    """A bounded, secret-safe node-authority failure."""


@dataclass(frozen=True, slots=True)
class AuthorityPolicy:
    source_sha: str
    source_tree: str
    node: str
    asset_sha256: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class Request:
    payload: Mapping[str, Any]
    payload_bytes: bytes

    @property
    def request_id(self) -> str:
        return str(self.payload["request_id"])

    @property
    def action(self) -> str:
        return str(self.payload["action"])


@dataclass(frozen=True, slots=True)
class UpgradeSnapshot:
    upgrade_id: str
    root: Path
    manifest: Path
    entries: tuple[Mapping[str, Any], ...]
    old_source_sha: str
    old_source_tree: str
    new_source_sha: str
    new_source_tree: str
    high_value_state: Mapping[str, Any]


def _is_sha(value: object, *, length: int = 40) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in SHA_RE for character in value)
    )


def _canonical_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00").astimezone(UTC)
    except ValueError:
        return None
    if parsed.isoformat().replace("+00:00", "Z") != value:
        return None
    return parsed


def _timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _request_digest(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("request_id", None)
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _clean_env() -> dict[str, str]:
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _hostname() -> str:
    return socket.gethostname().rstrip(".").lower()


def _node_for_hostname(hostname: str) -> str:
    matches = [node for node, canonical in NODE_HOSTNAMES.items() if canonical == hostname]
    if len(matches) != 1:
        raise NodeAuthorityError("node authority host is outside the closed inventory")
    return matches[0]


def _read_all_stdin() -> bytes:
    raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        raise NodeAuthorityError("node authority request exceeds its size bound")
    return raw


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_fd_twice(
    descriptor: int,
    *,
    limit: int,
    error: str,
) -> bytes:
    payloads: list[bytes] = []
    identities: list[tuple[int, ...]] = []
    for _ in range(2):
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise NodeAuthorityError(error)
            chunks.append(chunk)
        payloads.append(b"".join(chunks))
        identities.append(_metadata_identity(os.fstat(descriptor)))
    if (
        identities[0] != identities[1]
        or payloads[0] != payloads[1]
        or hashlib.sha256(payloads[0]).digest() != hashlib.sha256(payloads[1]).digest()
    ):
        raise NodeAuthorityError(error)
    return payloads[0]


def _safe_root_file(path: Path, *, mode: int, limit: int = MAX_REQUEST_BYTES) -> bytes:
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise NodeAuthorityError("node authority asset is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise NodeAuthorityError("node authority asset exceeds its size bound")
            chunks.append(chunk)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            raise NodeAuthorityError("node authority asset metadata is unsafe")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _safe_root_directory(path: Path, *, mode: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise NodeAuthorityError(f"node authority directory is unavailable: {path}") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise NodeAuthorityError(f"node authority directory metadata is unsafe: {path}")


def _ensure_root_directory(
    path: Path,
    *,
    mode: int,
    parent_mode: int = 0o755,
) -> bool:
    try:
        _safe_root_directory(path, mode=mode)
        return False
    except NodeAuthorityError:
        if path.exists() or path.is_symlink():
            raise
    _safe_root_directory(path.parent, mode=parent_mode)
    created = False
    try:
        path.mkdir(mode=mode)
        created = True
        os.chown(path, 0, 0)
        os.chmod(path, mode)
        _safe_root_directory(path, mode=mode)
    except Exception:
        if created:
            try:
                path.rmdir()
            except OSError as exc:
                raise NodeAuthorityError(
                    "node authority directory rollback failed safely",
                ) from exc
        raise
    return True


def _ensure_stage_root() -> None:
    _ensure_root_directory(
        STAGE_ROOT,
        mode=0o700,
        parent_mode=0o755,
    )


def _atomic_install(
    path: Path,
    payload: bytes,
    mode: int,
    *,
    parent_mode: int = 0o755,
) -> bool:
    _safe_root_directory(path.parent, mode=parent_mode)
    try:
        existing = _safe_root_file(path, mode=mode)
    except NodeAuthorityError:
        if path.exists() or path.is_symlink():
            raise
    else:
        if existing != payload:
            raise NodeAuthorityError("node authority installed asset drifted")
        return False
    descriptor = os.open(
        path.parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    temporary = f".{path.name}.new-{os.getpid()}"
    try:
        output = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=descriptor,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(output, view)
                if written <= 0:
                    raise NodeAuthorityError("node authority asset write failed safely")
                view = view[written:]
            os.fchown(output, 0, 0)
            os.fchmod(output, mode)
            os.fsync(output)
        finally:
            os.close(output)
        os.link(
            temporary,
            path.name,
            src_dir_fd=descriptor,
            dst_dir_fd=descriptor,
            follow_symlinks=False,
        )
        os.unlink(temporary, dir_fd=descriptor)
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=descriptor)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    return True


def _fsync_directory(path: Path, *, mode: int) -> None:
    _safe_root_directory(path, mode=mode)
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace(
    path: Path,
    payload: bytes,
    mode: int,
    *,
    parent_mode: int = 0o755,
) -> None:
    _safe_root_directory(path.parent, mode=parent_mode)
    descriptor = os.open(
        path.parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    temporary = f".{path.name}.replace-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        output = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=descriptor,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(output, view)
                if written <= 0:
                    raise NodeAuthorityError(
                        "node authority replacement write failed safely",
                    )
                view = view[written:]
            os.fchown(output, 0, 0)
            os.fchmod(output, mode)
            os.fsync(output)
        finally:
            os.close(output)
        os.replace(
            temporary,
            path.name,
            src_dir_fd=descriptor,
            dst_dir_fd=descriptor,
        )
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=descriptor)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)


def _unlink_root_file(path: Path, *, mode: int, parent_mode: int = 0o755) -> bytes:
    payload = _safe_root_file(path, mode=mode)
    descriptor = os.open(
        path.parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    try:
        rebound = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
        if (
            not stat.S_ISREG(rebound.st_mode)
            or rebound.st_uid != 0
            or rebound.st_gid != 0
            or stat.S_IMODE(rebound.st_mode) != mode
        ):
            raise NodeAuthorityError("node authority admission file changed")
        os.unlink(path.name, dir_fd=descriptor)
        os.fsync(descriptor)
    except OSError as exc:
        raise NodeAuthorityError("node authority admission disable failed safely") from exc
    finally:
        os.close(descriptor)
    _safe_root_directory(path.parent, mode=parent_mode)
    return payload


def _registry_module() -> Any:
    sealed = SOURCE_ROOT / REGISTRY_MODULE_RELATIVE
    source = (
        sealed
        if sealed.exists()
        else REPO_ROOT / REGISTRY_MODULE_RELATIVE
        if Path(__file__).resolve().is_relative_to(REPO_ROOT)
        else sealed
    )
    spec = importlib.util.spec_from_file_location(
        "_loom_developer_environment_registry",
        source,
    )
    if spec is None or spec.loader is None:
        raise NodeAuthorityError("developer environment registry verifier is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except (OSError, ImportError) as exc:
        raise NodeAuthorityError(
            "developer environment registry verifier is unavailable",
        ) from exc
    return module


def _load_registry_snapshot() -> dict[str, Any]:
    try:
        raw = _safe_root_file(REGISTRY_SNAPSHOT, mode=0o600, limit=8 << 20)
        payload = _registry_module().DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except NodeAuthorityError:
        raise
    except Exception as exc:
        raise NodeAuthorityError("developer environment registry snapshot is invalid") from exc
    if not isinstance(payload, dict):
        raise NodeAuthorityError("developer environment registry snapshot is invalid")
    return payload


def _verify_registry_snapshot_bytes(raw: bytes) -> dict[str, Any]:
    try:
        payload = _registry_module().DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except Exception as exc:
        raise NodeAuthorityError("developer environment registry snapshot is invalid") from exc
    if not isinstance(payload, dict):
        raise NodeAuthorityError("developer environment registry snapshot is invalid")
    return payload


def _registry_snapshot_archive_path(snapshot: Mapping[str, Any]) -> Path:
    return REGISTRY_SNAPSHOT_ARCHIVE / (
        f"registry-{snapshot['generation']}-{snapshot['payload_sha256']}.json"
    )


def _validated_registry_snapshot_archive() -> list[tuple[Path, bytes, dict[str, Any]]]:
    _safe_root_directory(REGISTRY_SNAPSHOT_ARCHIVE, mode=0o700)
    try:
        paths = sorted(REGISTRY_SNAPSHOT_ARCHIVE.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise NodeAuthorityError("registry snapshot archive is unavailable") from exc
    records: list[tuple[Path, bytes, dict[str, Any]]] = []
    generations: dict[int, str] = {}
    for path in paths:
        matched = REGISTRY_SNAPSHOT_ARCHIVE_RE.fullmatch(path.name)
        if matched is None:
            raise NodeAuthorityError("registry snapshot archive contains an unknown entry")
        raw = _safe_root_file(path, mode=0o600, limit=8 << 20)
        snapshot = _verify_registry_snapshot_bytes(raw)
        generation = int(matched.group(1))
        digest = matched.group(2)
        if (
            snapshot["generation"] != generation
            or snapshot["payload_sha256"] != digest
            or (generation in generations and generations[generation] != digest)
        ):
            raise NodeAuthorityError("registry snapshot archive binding is invalid")
        generations[generation] = digest
        records.append((path, raw, snapshot))
    return records


def _publish_registry_snapshot(
    raw: bytes,
    *,
    policy: AuthorityPolicy,
) -> dict[str, Any]:
    incoming = _verify_registry_snapshot_bytes(raw)
    _ensure_root_directory(
        REGISTRY_SNAPSHOT.parent,
        mode=0o700,
        parent_mode=0o755,
    )
    _ensure_root_directory(
        REGISTRY_SNAPSHOT_ARCHIVE,
        mode=0o700,
        parent_mode=0o700,
    )
    archived = _validated_registry_snapshot_archive()
    try:
        current_raw = _safe_root_file(
            REGISTRY_SNAPSHOT,
            mode=0o600,
            limit=8 << 20,
        )
    except NodeAuthorityError:
        if REGISTRY_SNAPSHOT.exists() or REGISTRY_SNAPSHOT.is_symlink():
            raise
        current_raw = None
        current = None
    else:
        current = _verify_registry_snapshot_bytes(current_raw)
    incoming_generation = int(incoming["generation"])
    incoming_digest = str(incoming["payload_sha256"])
    if current is not None:
        current_generation = int(current["generation"])
        current_digest = str(current["payload_sha256"])
        if incoming_generation < current_generation:
            raise NodeAuthorityError("registry snapshot generation cannot move backward")
        if incoming_generation == current_generation and (
            incoming_digest != current_digest or raw != current_raw
        ):
            raise NodeAuthorityError("registry snapshot generation conflicts")
    if any(
        int(snapshot["generation"]) > incoming_generation for _path, _payload, snapshot in archived
    ):
        raise NodeAuthorityError("registry snapshot archive contains a newer pending version")
    if current is not None:
        _atomic_install(
            _registry_snapshot_archive_path(current),
            current_raw,
            0o600,
            parent_mode=0o700,
        )
    _atomic_install(
        _registry_snapshot_archive_path(incoming),
        raw,
        0o600,
        parent_mode=0o700,
    )
    if current_raw != raw:
        _atomic_replace(
            REGISTRY_SNAPSHOT,
            raw,
            0o600,
            parent_mode=0o700,
        )
    published_raw = _safe_root_file(
        REGISTRY_SNAPSHOT,
        mode=0o600,
        limit=8 << 20,
    )
    published = _verify_registry_snapshot_bytes(published_raw)
    if published_raw != raw or published != incoming:
        raise NodeAuthorityError("registry snapshot publication readback drifted")

    # Validate the complete archive before deleting any old candidate.  The
    # fixed current file is already durable, so pruning can never remove the
    # last usable snapshot after a crash.
    archived = _validated_registry_snapshot_archive()
    current_path = _registry_snapshot_archive_path(incoming)
    previous = max(
        (record for record in archived if int(record[2]["generation"]) < incoming_generation),
        key=lambda record: int(record[2]["generation"]),
        default=None,
    )
    retained = {current_path}
    if previous is not None:
        retained.add(previous[0])
    for path, _candidate_raw, _candidate in archived:
        if path not in retained:
            _unlink_root_file(path, mode=0o600, parent_mode=0o700)
    final_archive = _validated_registry_snapshot_archive()
    if len(final_archive) > 2 or current_path not in {record[0] for record in final_archive}:
        raise NodeAuthorityError("registry snapshot archive retention is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-environment.registry-snapshot-sync",
        "generation": incoming_generation,
        "registry_payload_sha256": incoming_digest,
        "source_sha": policy.source_sha,
        "source_tree": policy.source_tree,
        "status": "published",
    }


def _registry_finalization_exact(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    candidate: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> bool:
    digest = deployment.get("finalization_payload_sha256")
    records = snapshot.get("deployment_finalizations")
    if not _is_sha(digest, length=64) or not isinstance(records, list):
        return False
    matched = [
        record
        for record in records
        if isinstance(record, Mapping)
        and record.get("deployment_id") == deployment["deployment_id"]
        and record.get("payload_sha256") == digest
    ]
    if len(matched) != 1:
        return False
    record = matched[0]
    fields = {
        "deployment_id",
        "env_id",
        "principal_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "applied_resource_generation",
        "applied_registry_generation",
        "applied_registry_payload_sha256",
        "capacity_finalize_receipt_sha256",
        "capacity_finalize_check_receipt_sha256",
        "runtime_reconcile_receipt_sha256",
        "runtime_prepare_check_receipt_sha256",
        "acceptance_probe_receipt_sha256",
        "created_at",
        "payload_sha256",
    }
    unsigned = {field: value for field, value in record.items() if field != "payload_sha256"}
    return (
        set(record) == fields
        and record.get("payload_sha256") == hashlib.sha256(_canonical(unsigned)).hexdigest()
        and record.get("env_id") == environment["env_id"]
        and record.get("principal_id") == environment["principal_id"]
        and record.get("candidate_id") == candidate["candidate_id"]
        and record.get("candidate_sha") == candidate["candidate_sha"]
        and record.get("candidate_tree") == candidate["candidate_tree"]
        and record.get("applied_resource_generation")
        == deployment.get("applied_resource_generation")
        and record.get("applied_registry_generation")
        == deployment.get("applied_registry_generation")
        and record.get("applied_registry_payload_sha256")
        == deployment.get("applied_registry_payload_sha256")
        and all(
            _is_sha(record.get(field), length=64)
            for field in (
                "capacity_finalize_receipt_sha256",
                "capacity_finalize_check_receipt_sha256",
                "runtime_reconcile_receipt_sha256",
                "runtime_prepare_check_receipt_sha256",
                "acceptance_probe_receipt_sha256",
            )
        )
        and isinstance(record.get("created_at"), str)
    )


def _registry_cohort(
    snapshot: Mapping[str, Any],
    *,
    include_provisioning: bool,
    deployment_id: str | None = None,
    target_resource_generation: int | None = None,
    include_retiring: bool = False,
) -> dict[str, tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]]:
    cohort: dict[str, tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]] = {}
    target_env_id: str | None = None
    if deployment_id is not None:
        targets = [
            deployment
            for deployment in snapshot["deployments"]
            if deployment["deployment_id"] == deployment_id
        ]
        if len(targets) != 1:
            raise NodeAuthorityError("registry deployment selector is invalid")
        target_env_id = str(targets[0]["env_id"])
    for environment in snapshot["environments"]:
        state = environment["state"]
        deployment_matches = environment["env_id"] == target_env_id
        projected_environment: Mapping[str, Any] = environment
        committed_binding = False
        if state == "active":
            candidate_id = environment["current_candidate_id"]
            phases = {"committed"}
            committed_binding = True
        elif state == "deploying" and include_provisioning and deployment_matches:
            candidates_in_flight = [
                deployment
                for deployment in snapshot["deployments"]
                if deployment["env_id"] == environment["env_id"]
                and deployment["principal_id"] == environment["principal_id"]
                and (not deployment_matches or deployment["deployment_id"] == deployment_id)
                and deployment["expected_resource_generation"] == environment["resource_generation"]
                and deployment["phase"] not in {"committed", "failed"}
            ]
            if len(candidates_in_flight) != 1:
                raise NodeAuthorityError("registry provisioning binding is invalid")
            candidate_id = candidates_in_flight[0]["candidate_id"]
            phases = {candidates_in_flight[0]["phase"]}
            target_generation = (
                environment["resource_generation"]
                if target_resource_generation is None
                else target_resource_generation
            )
            if target_generation not in {
                environment["resource_generation"],
                *(
                    [environment["resource_generation"] + 1]
                    if candidates_in_flight[0]["phase"] == "verified"
                    else []
                ),
            }:
                raise NodeAuthorityError("registry provisioning generation is invalid")
            if target_generation != environment["resource_generation"]:
                if (
                    candidates_in_flight[0].get("applied_resource_generation") != target_generation
                    or type(
                        candidates_in_flight[0].get("applied_registry_generation"),
                    )
                    is not int
                    or not 1
                    <= candidates_in_flight[0]["applied_registry_generation"]
                    < snapshot["generation"]
                    or not _is_sha(
                        candidates_in_flight[0].get(
                            "applied_registry_payload_sha256",
                        ),
                        length=64,
                    )
                ):
                    raise NodeAuthorityError(
                        "registry provisioning applied binding is invalid",
                    )
                projected_environment = {
                    **environment,
                    "resource_generation": target_generation,
                    "current_candidate_id": candidate_id,
                }
        elif state == "quarantined" and include_retiring and deployment_matches:
            retiring = [
                deployment
                for deployment in snapshot["deployments"]
                if deployment["deployment_id"] == deployment_id
                and deployment["env_id"] == environment["env_id"]
                and deployment["principal_id"] == environment["principal_id"]
                and deployment["candidate_id"] == environment["current_candidate_id"]
                and deployment["phase"] == "committed"
                and deployment.get("applied_resource_generation")
                == environment["resource_generation"]
            ]
            if len(retiring) != 1:
                raise NodeAuthorityError("registry retiring binding is invalid")
            candidate_id = retiring[0]["candidate_id"]
            phases = {"committed"}
            committed_binding = True
        else:
            continue
        candidates = [
            candidate
            for candidate in snapshot["candidates"]
            if candidate["candidate_id"] == candidate_id
            and candidate["env_id"] == environment["env_id"]
            and candidate["principal_id"] == environment["principal_id"]
        ]
        deployments = [
            deployment
            for deployment in snapshot["deployments"]
            if deployment["candidate_id"] == candidate_id
            and deployment["env_id"] == environment["env_id"]
            and deployment["principal_id"] == environment["principal_id"]
            and (not deployment_matches or deployment["deployment_id"] == deployment_id)
            and deployment["phase"] in phases
            and (
                (
                    deployment.get("applied_resource_generation")
                    == environment["resource_generation"]
                    and deployment.get("expected_resource_generation", 0) + 1
                    == deployment["applied_resource_generation"]
                    and isinstance(deployment.get("applied_registry_generation"), int)
                    and not isinstance(deployment.get("applied_registry_generation"), bool)
                    and 1 <= deployment["applied_registry_generation"] < snapshot["generation"]
                    and _is_sha(
                        deployment.get("applied_registry_payload_sha256"),
                        length=64,
                    )
                )
                if committed_binding
                else deployment["expected_resource_generation"]
                == environment["resource_generation"]
            )
        ]
        if len(candidates) != 1 or len(deployments) != 1:
            raise NodeAuthorityError("registry candidate binding is invalid")
        if committed_binding and not _registry_finalization_exact(
            snapshot,
            environment,
            candidates[0],
            deployments[0],
        ):
            raise NodeAuthorityError(
                "registry committed deployment finalization binding is invalid",
            )
        runtime_id = environment["runtime_id"]
        if SAFE_RUNTIME_RE.fullmatch(runtime_id) is None or runtime_id in cohort:
            raise NodeAuthorityError("registry runtime identity is invalid")
        cohort[runtime_id] = (projected_environment, candidates[0], deployments[0])
    return cohort


def _source_asset_mode(relative: Path) -> int:
    if relative == SUDOERS_RELATIVE:
        return 0o440
    return 0o755 if relative.parts[:2] == ("scripts", "ops") else 0o644


def _managed_assets() -> tuple[tuple[Path, int, int], ...]:
    return (
        *tuple(
            (SOURCE_ROOT / relative, _source_asset_mode(relative), 0o755)
            for relative in SOURCE_ASSETS
        ),
        (LIBEXEC, 0o755, 0o755),
        (POLICY, 0o600, 0o755),
        (SUDOERS, 0o440, 0o755),
        *tuple(
            (target, mode, parent_mode)
            for _relative, target, mode, parent_mode in SYSTEM_INSTALL_ASSETS
        ),
    )


def _policy_asset_generation(asset_sha256: Mapping[str, str]) -> str:
    keys = frozenset(asset_sha256)
    if keys == CURRENT_POLICY_ASSET_KEYS:
        return "current"
    if keys == LEGACY_V1_POLICY_ASSET_KEYS:
        return "legacy-v1"
    raise NodeAuthorityError("node authority policy asset identity is invalid")


def _upgrade_managed_assets(
    old_policy: AuthorityPolicy,
) -> tuple[tuple[Path, int, int], ...]:
    managed = _managed_assets()
    if _policy_asset_generation(old_policy.asset_sha256) == "current":
        return managed
    return (
        *managed,
        *tuple(
            (SOURCE_ROOT / relative, _source_asset_mode(relative), 0o755)
            for relative in RETIRED_LEGACY_SOURCE_ASSETS
        ),
    )


def _system_sudoers_paths() -> frozenset[Path]:
    return frozenset(
        target
        for relative, target, _mode, _parent_mode in SYSTEM_INSTALL_ASSETS
        if relative
        in {
            PLATFORM_HEALTH_SUDOERS_RELATIVE,
            STAGING_PRESSURE_SUDOERS_RELATIVE,
            STAGING_EXTERNAL_SUDOERS_RELATIVE,
        }
    )


def _system_service_paths() -> frozenset[Path]:
    return frozenset(
        target
        for relative, target, _mode, _parent_mode in SYSTEM_INSTALL_ASSETS
        if relative
        in {
            SLURM_RECOVERY_SERVICE_RELATIVE,
            SLURM_RECOVERY_TIMER_RELATIVE,
            PLATFORM_HEALTH_SERVICE_RELATIVE,
            STAGING_PRESSURE_SERVICE_RELATIVE,
            STAGING_EXTERNAL_SERVICE_RELATIVE,
            Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
        }
    )


def _validate_sudoers(path: Path, *, label: str) -> None:
    validation = subprocess.run(
        ("/usr/sbin/visudo", "-cf", str(path)),
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if validation.returncode != 0:
        raise NodeAuthorityError(f"{label} sudoers is invalid")


def _validate_systemd_service(path: Path, *, label: str) -> None:
    validation = subprocess.run(
        ("/usr/bin/systemd-analyze", "verify", str(path)),
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if validation.returncode != 0:
        raise NodeAuthorityError(f"{label} systemd service is invalid")


def _systemd_daemon_reload() -> None:
    result = subprocess.run(
        ("/usr/bin/systemctl", "daemon-reload"),
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise NodeAuthorityError("node authority systemd daemon reload failed safely")


def _systemd_enable_recovery_timer(*, start: bool) -> None:
    argv = ["/usr/bin/systemctl", "enable"]
    if start:
        argv.append("--now")
    argv.extend(("--quiet", SLURM_RECOVERY_TIMER_UNIT))
    result = subprocess.run(
        argv,
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise NodeAuthorityError("Slurm recovery timer enable failed safely")


def _systemd_disable_recovery_timer() -> None:
    result = subprocess.run(
        (
            "/usr/bin/systemctl",
            "disable",
            "--now",
            "--quiet",
            SLURM_RECOVERY_TIMER_UNIT,
        ),
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if result.returncode not in {0, 1}:
        raise NodeAuthorityError("Slurm recovery timer disable failed safely")


def _validate_recovery_timer(*, require_active: bool) -> None:
    states = ("is-enabled", "is-active") if require_active else ("is-enabled",)
    for state in states:
        result = subprocess.run(
            ("/usr/bin/systemctl", state, "--quiet", SLURM_RECOVERY_TIMER_UNIT),
            env=_clean_env(),
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            raise NodeAuthorityError(f"Slurm recovery timer is not {state.removeprefix('is-')}")


def _sync_recovery_timer_after_restore() -> None:
    recovery_paths = (
        SLURM_RECOVERY_LIBEXEC,
        SLURM_RECOVERY_SERVICE,
        SLURM_RECOVERY_TIMER,
    )
    if all(path.exists() and not path.is_symlink() for path in recovery_paths):
        _systemd_enable_recovery_timer(start=True)
        _validate_recovery_timer(require_active=True)
    else:
        _systemd_disable_recovery_timer()


def _validate_system_install_sources() -> None:
    for relative, _target, _mode, _parent_mode in SYSTEM_INSTALL_ASSETS:
        source = SOURCE_ROOT / relative
        if relative in {
            PLATFORM_HEALTH_SUDOERS_RELATIVE,
            STAGING_PRESSURE_SUDOERS_RELATIVE,
            STAGING_EXTERNAL_SUDOERS_RELATIVE,
        }:
            _validate_sudoers(source, label="source")
        elif relative in {
            NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
            Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
        }:
            expected_directories = (
                NODE_AUTHORITY_TMPFILES_DIRECTORIES
                if relative == NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE
                else STAGING_SHARED_TMPFILES_DIRECTORIES
            )
            _validate_tmpfiles(
                source,
                apply=False,
                expected_directories=expected_directories,
            )


def _validate_tmpfiles(
    path: Path,
    *,
    apply: bool,
    expected_directories: tuple[tuple[Path, int], ...],
) -> None:
    if (
        not expected_directories
        or len({relative for relative, _mode in expected_directories}) != len(expected_directories)
        or any(
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or mode not in {0o700, 0o755}
            for relative, mode in expected_directories
        )
    ):
        raise NodeAuthorityError("staging shared tmpfiles validation contract is invalid")
    expected_payload = b"".join(
        (
            f"d /{relative.as_posix()} {mode:04o} root root -\n".encode(
                "ascii",
            )
        )
        for relative, mode in expected_directories
    )
    try:
        payload = _safe_root_file(path, mode=0o644, limit=4096) if apply else path.read_bytes()
    except OSError as exc:
        raise NodeAuthorityError("staging shared tmpfiles policy is unavailable") from exc
    if payload != expected_payload:
        raise NodeAuthorityError("staging shared tmpfiles policy is not an exact closed policy")
    if apply and path.parent != Path("/etc/tmpfiles.d"):
        raise NodeAuthorityError("staging shared tmpfiles boot policy path is unsafe")
    _ensure_stage_root()
    argv = ["/usr/bin/systemd-tmpfiles", "--create"]
    validation_root: Path | None = None
    if not apply:
        validation_root = STAGE_ROOT / f"tmpfiles-validation-{uuid.uuid4().hex}"
        if not _ensure_root_directory(
            validation_root,
            mode=0o700,
            parent_mode=0o700,
        ):
            raise NodeAuthorityError("staging shared tmpfiles validation root collided")
        argv.append(f"--root={validation_root}")
    # A basename makes systemd-tmpfiles use its normal boot-time configuration
    # precedence. The exact root-owned /etc readback above is therefore the
    # effective policy even if a lower-priority vendor copy remains installed.
    argv.append(path.name if apply else str(path))
    try:
        validation = subprocess.run(
            tuple(argv),
            env=_clean_env(),
            check=False,
            capture_output=True,
        )
        if validation.returncode != 0:
            raise NodeAuthorityError("staging shared tmpfiles policy is invalid")
        readback_root = Path("/") if validation_root is None else validation_root
        for relative, mode in expected_directories:
            _safe_root_directory(readback_root / relative, mode=mode)
    finally:
        if validation_root is not None:
            _safe_root_directory(validation_root, mode=0o700)
            try:
                shutil.rmtree(validation_root)
            except OSError as exc:
                raise NodeAuthorityError(
                    "staging shared tmpfiles validation cleanup failed safely",
                ) from exc
            if validation_root.exists() or validation_root.is_symlink():
                raise NodeAuthorityError(
                    "staging shared tmpfiles validation cleanup failed safely",
                )


def _ensure_system_install_directories() -> None:
    for directory, mode, parent_mode in (
        (CAPACITY_CONTRACT_LIBEXEC.parent.parent, 0o755, 0o755),
        (CAPACITY_CONTRACT_LIBEXEC.parent, 0o755, 0o755),
        (STAGING_EXTERNAL_INSTALL_ROOT, 0o755, 0o755),
        (STAGING_EXTERNAL_CONSUMER.parent, 0o755, 0o755),
        (STAGING_EXTERNAL_CONFIG.parent, 0o700, 0o755),
    ):
        _ensure_root_directory(
            directory,
            mode=mode,
            parent_mode=parent_mode,
        )


def _validate_system_install_assets(
    *,
    allow_absent: bool = False,
) -> tuple[dict[str, Any], ...]:
    readbacks: list[dict[str, Any]] = []
    for relative, target, mode, _parent_mode in SYSTEM_INSTALL_ASSETS:
        try:
            payload = _safe_root_file(target, mode=mode)
        except NodeAuthorityError:
            if allow_absent and not target.exists() and not target.is_symlink():
                continue
            raise
        source = _safe_root_file(
            SOURCE_ROOT / relative,
            mode=_source_asset_mode(relative),
        )
        if payload != source:
            raise NodeAuthorityError("node authority system install drifted")
        readbacks.append(
            {
                "path": str(target),
                "mode": f"{mode:04o}",
                "sha256": hashlib.sha256(payload).hexdigest(),
            },
        )
    if not allow_absent and len(readbacks) != len(SYSTEM_INSTALL_ASSETS):
        raise NodeAuthorityError("node authority system install is incomplete")
    recovery_targets = {
        SLURM_RECOVERY_LIBEXEC,
        SLURM_RECOVERY_SERVICE,
        SLURM_RECOVERY_TIMER,
    }
    if recovery_targets.issubset({Path(row["path"]) for row in readbacks}):
        _validate_recovery_timer(require_active=False)
    return tuple(readbacks)


def _system_install_assets(
    assets: Mapping[str, bytes],
    *,
    replace: bool,
) -> tuple[dict[str, Any], ...]:
    _ensure_system_install_directories()
    _validate_system_install_sources()
    sudoers_paths = _system_sudoers_paths()
    service_paths = _system_service_paths()
    for relative, target, mode, parent_mode in SYSTEM_INSTALL_ASSETS:
        payload = assets[str(relative)]
        if target in sudoers_paths or target in service_paths:
            continue
        if replace:
            _atomic_replace(target, payload, mode, parent_mode=parent_mode)
        else:
            _atomic_install(target, payload, mode, parent_mode=parent_mode)
    for relative, target, mode, parent_mode in SYSTEM_INSTALL_ASSETS:
        if target not in service_paths:
            continue
        _validate_systemd_service(
            SOURCE_ROOT / relative,
            label="source",
        )
        payload = assets[str(relative)]
        if replace:
            _atomic_replace(target, payload, mode, parent_mode=parent_mode)
        else:
            _atomic_install(target, payload, mode, parent_mode=parent_mode)
    for relative, target, mode, parent_mode in SYSTEM_INSTALL_ASSETS:
        if target not in sudoers_paths:
            continue
        payload = assets[str(relative)]
        if replace:
            _atomic_replace(target, payload, mode, parent_mode=parent_mode)
        else:
            _atomic_install(target, payload, mode, parent_mode=parent_mode)
        _validate_sudoers(target, label="installed")
    for service in _system_service_paths():
        _validate_systemd_service(service, label="installed")
    _validate_tmpfiles(
        NODE_AUTHORITY_TMPFILES,
        apply=True,
        expected_directories=NODE_AUTHORITY_TMPFILES_DIRECTORIES,
    )
    _validate_tmpfiles(
        STAGING_EXTERNAL_TMPFILES,
        apply=True,
        expected_directories=STAGING_SHARED_TMPFILES_DIRECTORIES,
    )
    _systemd_daemon_reload()
    _systemd_enable_recovery_timer(start=False)
    return _validate_system_install_assets()


def _high_value_state_identity() -> dict[str, Any]:
    _safe_root_directory(STATE_ROOT, mode=0o700)
    _safe_root_directory(RECEIPT_ROOT, mode=0o700)
    journal = _safe_root_file(JOURNAL, mode=0o600, limit=MAX_REQUEST_BYTES)
    receipts: dict[str, str] = {}
    try:
        names = sorted(path.name for path in RECEIPT_ROOT.iterdir())
    except OSError as exc:
        raise NodeAuthorityError("node authority receipt inventory is unavailable") from exc
    for name in names:
        if len(name) != 69 or not name.endswith(".json") or not _is_sha(name[:-5], length=64):
            raise NodeAuthorityError("node authority receipt inventory drifted")
        payload = _safe_root_file(RECEIPT_ROOT / name, mode=0o600, limit=1 << 20)
        receipts[name] = hashlib.sha256(payload).hexdigest()
    return {
        "journal_sha256": hashlib.sha256(journal).hexdigest(),
        "receipts": receipts,
    }


def _validate_high_value_state_identity(value: object) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != {"journal_sha256", "receipts"}
        or not _is_sha(value.get("journal_sha256"), length=64)
        or not isinstance(value.get("receipts"), dict)
    ):
        raise NodeAuthorityError("node authority high-value state identity is invalid")
    receipts = value["receipts"]
    if any(
        not isinstance(name, str)
        or len(name) != 69
        or not name.endswith(".json")
        or not _is_sha(name[:-5], length=64)
        or not _is_sha(digest, length=64)
        for name, digest in receipts.items()
    ):
        raise NodeAuthorityError("node authority high-value state identity is invalid")
    return {
        "journal_sha256": value["journal_sha256"],
        "receipts": dict(receipts),
    }


def _upgrade_journal_append(record: Mapping[str, Any]) -> None:
    if set(record) != {
        "schema_version",
        "upgrade_id",
        "old_source_sha",
        "old_source_tree",
        "new_source_sha",
        "new_source_tree",
        "phase",
        "timestamp_ns",
    }:
        raise NodeAuthorityError("node authority upgrade journal record is invalid")
    descriptor = os.open(
        UPGRADE_JOURNAL,
        os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise NodeAuthorityError("node authority upgrade journal is unsafe")
        view = memoryview(_canonical(record))
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise NodeAuthorityError(
                    "node authority upgrade journal write failed safely",
                )
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_upgrade_journal() -> None:
    raw = _safe_root_file(
        UPGRADE_JOURNAL,
        mode=0o600,
        limit=MAX_REQUEST_BYTES,
    )
    for line in raw.splitlines(keepends=True):
        try:
            record = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("node authority upgrade journal is invalid") from exc
        if (
            not isinstance(record, dict)
            or frozenset(record)
            != {
                "schema_version",
                "upgrade_id",
                "old_source_sha",
                "old_source_tree",
                "new_source_sha",
                "new_source_tree",
                "phase",
                "timestamp_ns",
            }
            or record.get("schema_version") != SCHEMA_VERSION
            or not isinstance(record.get("upgrade_id"), str)
            or not _is_sha(record.get("old_source_sha"))
            or not _is_sha(record.get("old_source_tree"))
            or not _is_sha(record.get("new_source_sha"))
            or not _is_sha(record.get("new_source_tree"))
            or record.get("phase") not in UPGRADE_PHASES
            or not isinstance(record.get("timestamp_ns"), int)
            or int(record["timestamp_ns"]) <= 0
            or line != _canonical(record)
        ):
            raise NodeAuthorityError("node authority upgrade journal is invalid")


def _upgrade_event(snapshot: UpgradeSnapshot, phase: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "upgrade_id": snapshot.upgrade_id,
        "old_source_sha": snapshot.old_source_sha,
        "old_source_tree": snapshot.old_source_tree,
        "new_source_sha": snapshot.new_source_sha,
        "new_source_tree": snapshot.new_source_tree,
        "phase": phase,
        "timestamp_ns": time.time_ns(),
    }


def _snapshot_manifest_payload(snapshot: UpgradeSnapshot) -> bytes:
    return _canonical(
        {
            "schema_version": SCHEMA_VERSION,
            "upgrade_id": snapshot.upgrade_id,
            "old_source_sha": snapshot.old_source_sha,
            "old_source_tree": snapshot.old_source_tree,
            "new_source_sha": snapshot.new_source_sha,
            "new_source_tree": snapshot.new_source_tree,
            "high_value_state": snapshot.high_value_state,
            "entries": list(snapshot.entries),
        },
    )


def _prepare_upgrade_snapshot(
    old_policy: AuthorityPolicy,
    *,
    new_source_sha: str,
    new_source_tree: str,
    high_value_state: Mapping[str, Any],
) -> UpgradeSnapshot:
    _ensure_root_directory(UPGRADE_ROOT, mode=0o700, parent_mode=0o700)
    upgrade_id = (
        f"{time.time_ns()}-{old_policy.source_tree[:12]}-"
        f"{new_source_tree[:12]}-{uuid.uuid4().hex[:12]}"
    )
    root = UPGRADE_ROOT / upgrade_id
    _ensure_root_directory(root, mode=0o700, parent_mode=0o700)
    entries: list[Mapping[str, Any]] = []
    try:
        optional_paths = {
            *(target for _relative, target, _mode, _parent in SYSTEM_INSTALL_ASSETS),
            *(
                SOURCE_ROOT / relative
                for relative in LEGACY_V1_MIGRATABLE_SOURCE_ASSETS
                if str(relative) not in old_policy.asset_sha256
            ),
        }
        for index, (path, mode, parent_mode) in enumerate(
            _upgrade_managed_assets(old_policy),
        ):
            try:
                payload = _safe_root_file(path, mode=mode)
            except NodeAuthorityError:
                if path not in optional_paths or path.exists() or path.is_symlink():
                    raise
                entries.append(
                    {
                        "path": str(path),
                        "present": False,
                        "mode": f"{mode:04o}",
                        "parent_mode": f"{parent_mode:04o}",
                        "snapshot": None,
                        "sha256": None,
                    },
                )
                continue
            snapshot_name = f"{index:04d}.bin"
            _atomic_install(
                root / snapshot_name,
                payload,
                0o600,
                parent_mode=0o700,
            )
            entries.append(
                {
                    "path": str(path),
                    "present": True,
                    "mode": f"{mode:04o}",
                    "parent_mode": f"{parent_mode:04o}",
                    "snapshot": snapshot_name,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                },
            )
        snapshot = UpgradeSnapshot(
            upgrade_id=upgrade_id,
            root=root,
            manifest=root / "manifest.json",
            entries=tuple(entries),
            old_source_sha=old_policy.source_sha,
            old_source_tree=old_policy.source_tree,
            new_source_sha=new_source_sha,
            new_source_tree=new_source_tree,
            high_value_state=_validate_high_value_state_identity(high_value_state),
        )
        _atomic_install(
            snapshot.manifest,
            _snapshot_manifest_payload(snapshot),
            0o600,
            parent_mode=0o700,
        )
        _fsync_directory(root, mode=0o700)
        _fsync_directory(UPGRADE_ROOT, mode=0o700)
        return snapshot
    except Exception:
        try:
            shutil.rmtree(root)
            _fsync_directory(UPGRADE_ROOT, mode=0o700)
        except OSError:
            pass
        raise


def _load_upgrade_snapshot(root: Path) -> UpgradeSnapshot:
    try:
        resolved = root.resolve(strict=True)
        expected_parent = UPGRADE_ROOT.resolve(strict=True)
    except OSError as exc:
        raise NodeAuthorityError("node authority upgrade snapshot is unavailable") from exc
    if resolved.parent != expected_parent:
        raise NodeAuthorityError("node authority upgrade snapshot path is invalid")
    _safe_root_directory(resolved, mode=0o700)
    raw = _safe_root_file(resolved / "manifest.json", mode=0o600, limit=1 << 20)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("node authority upgrade snapshot is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "upgrade_id",
            "old_source_sha",
            "old_source_tree",
            "new_source_sha",
            "new_source_tree",
            "high_value_state",
            "entries",
        }
        or payload.get("schema_version") != SCHEMA_VERSION
        or not isinstance(payload.get("upgrade_id"), str)
        or payload["upgrade_id"] != resolved.name
        or not _is_sha(payload.get("old_source_sha"))
        or not _is_sha(payload.get("old_source_tree"))
        or not _is_sha(payload.get("new_source_sha"))
        or not _is_sha(payload.get("new_source_tree"))
        or not isinstance(payload.get("high_value_state"), dict)
        or not isinstance(payload.get("entries"), list)
        or raw != _canonical(payload)
    ):
        raise NodeAuthorityError("node authority upgrade snapshot is invalid")
    entries = payload["entries"]
    current_assets = _managed_assets()
    legacy_assets = (
        *current_assets,
        *tuple(
            (SOURCE_ROOT / relative, _source_asset_mode(relative), 0o755)
            for relative in RETIRED_LEGACY_SOURCE_ASSETS
        ),
    )
    candidate_inventories = (current_assets, legacy_assets)
    matching_inventories = tuple(
        inventory for inventory in candidate_inventories if len(entries) == len(inventory)
    )
    if len(matching_inventories) != 1:
        raise NodeAuthorityError("node authority upgrade snapshot inventory is invalid")
    expected_assets = [
        (str(path), f"{mode:04o}", f"{parent_mode:04o}")
        for path, mode, parent_mode in matching_inventories[0]
    ]
    for index, (entry, expected_asset) in enumerate(
        zip(entries, expected_assets, strict=True),
    ):
        expected_path, expected_mode, expected_parent_mode = expected_asset
        if (
            not isinstance(entry, dict)
            or set(entry)
            != {
                "path",
                "present",
                "mode",
                "parent_mode",
                "snapshot",
                "sha256",
            }
            or entry.get("path") != expected_path
            or not isinstance(entry.get("present"), bool)
            or entry.get("mode") != expected_mode
            or entry.get("parent_mode") != expected_parent_mode
        ):
            raise NodeAuthorityError("node authority upgrade snapshot inventory is invalid")
        if entry["present"]:
            if entry.get("snapshot") != f"{index:04d}.bin" or not _is_sha(
                entry.get("sha256"), length=64
            ):
                raise NodeAuthorityError("node authority upgrade snapshot inventory is invalid")
        elif entry.get("snapshot") is not None or entry.get("sha256") is not None:
            raise NodeAuthorityError("node authority upgrade snapshot inventory is invalid")
        if not entry["present"]:
            continue
        snapshot_payload = _safe_root_file(
            resolved / str(entry["snapshot"]),
            mode=0o600,
        )
        if hashlib.sha256(snapshot_payload).hexdigest() != entry["sha256"]:
            raise NodeAuthorityError("node authority upgrade snapshot digest drifted")
    return UpgradeSnapshot(
        upgrade_id=payload["upgrade_id"],
        root=resolved,
        manifest=resolved / "manifest.json",
        entries=tuple(entries),
        old_source_sha=payload["old_source_sha"],
        old_source_tree=payload["old_source_tree"],
        new_source_sha=payload["new_source_sha"],
        new_source_tree=payload["new_source_tree"],
        high_value_state=_validate_high_value_state_identity(
            payload["high_value_state"],
        ),
    )


def _active_payload(snapshot: UpgradeSnapshot, phase: str) -> bytes:
    return _canonical(
        {
            "schema_version": SCHEMA_VERSION,
            "upgrade_id": snapshot.upgrade_id,
            "snapshot": str(snapshot.root),
            "phase": phase,
        },
    )


def _write_upgrade_active(snapshot: UpgradeSnapshot, phase: str) -> None:
    payload = _active_payload(snapshot, phase)
    if UPGRADE_ACTIVE.exists():
        _atomic_replace(
            UPGRADE_ACTIVE,
            payload,
            0o600,
            parent_mode=0o700,
        )
    else:
        _atomic_install(
            UPGRADE_ACTIVE,
            payload,
            0o600,
            parent_mode=0o700,
        )


def _read_upgrade_active() -> tuple[UpgradeSnapshot, str] | None:
    if not UPGRADE_ACTIVE.exists():
        return None
    raw = _safe_root_file(UPGRADE_ACTIVE, mode=0o600, limit=1 << 20)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("node authority active upgrade is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "upgrade_id", "snapshot", "phase"}
        or payload.get("schema_version") != SCHEMA_VERSION
        or not isinstance(payload.get("upgrade_id"), str)
        or not isinstance(payload.get("snapshot"), str)
        or payload.get("phase")
        not in {"prepared", "admission-disabled", "assets-replaced", "committed"}
        or raw != _canonical(payload)
    ):
        raise NodeAuthorityError("node authority active upgrade is invalid")
    snapshot = _load_upgrade_snapshot(Path(payload["snapshot"]))
    if snapshot.upgrade_id != payload["upgrade_id"]:
        raise NodeAuthorityError("node authority active upgrade binding is invalid")
    return snapshot, str(payload["phase"])


def _reject_active_upgrade() -> None:
    try:
        UPGRADE_ACTIVE.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise NodeAuthorityError("node authority upgrade admission state is unavailable") from exc
    raise NodeAuthorityError("node authority runtime admission is disabled during upgrade")


def _remove_upgrade_active() -> None:
    if not UPGRADE_ACTIVE.exists():
        return
    _safe_root_file(UPGRADE_ACTIVE, mode=0o600, limit=1 << 20)
    UPGRADE_ACTIVE.unlink()
    _fsync_directory(STATE_ROOT, mode=0o700)


def _restore_upgrade_snapshot(snapshot: UpgradeSnapshot) -> None:
    sudoers_paths = {SUDOERS, *_system_sudoers_paths()}
    expected_modes = {
        Path(str(entry["path"])): int(str(entry["mode"]), 8) for entry in snapshot.entries
    }
    for sudoers_path in sudoers_paths:
        if sudoers_path.exists() or sudoers_path.is_symlink():
            _unlink_root_file(
                sudoers_path,
                mode=expected_modes[sudoers_path],
            )
    for entry in snapshot.entries:
        path = Path(str(entry["path"]))
        if path in sudoers_paths:
            continue
        if not entry["present"]:
            if path.exists() or path.is_symlink():
                _unlink_root_file(
                    path,
                    mode=int(str(entry["mode"]), 8),
                    parent_mode=int(str(entry["parent_mode"]), 8),
                )
            continue
        payload = _safe_root_file(
            snapshot.root / str(entry["snapshot"]),
            mode=0o600,
        )
        _atomic_replace(
            path,
            payload,
            int(str(entry["mode"]), 8),
            parent_mode=int(str(entry["parent_mode"]), 8),
        )
    for entry in snapshot.entries:
        path = Path(str(entry["path"]))
        if path not in sudoers_paths or not entry["present"]:
            continue
        old_sudoers = _safe_root_file(
            snapshot.root / str(entry["snapshot"]),
            mode=0o600,
        )
        _atomic_install(
            path,
            old_sudoers,
            int(str(entry["mode"]), 8),
            parent_mode=int(str(entry["parent_mode"]), 8),
        )
        _validate_sudoers(path, label="restored")
    _systemd_daemon_reload()
    _sync_recovery_timer_after_restore()
    restored = _read_policy()
    _validate_runtime_assets(
        restored,
        allow_absent_system_install=True,
    )
    for entry in snapshot.entries:
        path = Path(str(entry["path"]))
        if path not in {target for _relative, target, _mode, _parent in SYSTEM_INSTALL_ASSETS}:
            continue
        if entry["present"]:
            payload = _safe_root_file(path, mode=int(str(entry["mode"]), 8))
            if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
                raise NodeAuthorityError("restored system install drifted")
        elif path.exists() or path.is_symlink():
            raise NodeAuthorityError("restored system install drifted")
    if (
        restored.source_sha != snapshot.old_source_sha
        or restored.source_tree != snapshot.old_source_tree
    ):
        raise NodeAuthorityError("restored node authority identity is invalid")


def _safe_source_directory(metadata: os.stat_result, *, expected_uid: int) -> bool:
    return (
        stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and metadata.st_uid in {0, expected_uid}
        and not stat.S_IMODE(metadata.st_mode) & 0o022
    )


def _source_asset(relative: Path, *, expected_uid: int = 0) -> bytes:
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise NodeAuthorityError("bootstrap source asset path is unsafe")
    repo = REPO_ROOT.absolute()
    if not repo.is_absolute():
        raise NodeAuthorityError("bootstrap source asset path is unsafe")
    directory_parts = (*repo.parts[1:], *relative.parent.parts)
    descriptors: list[int] = []
    directory_records: list[tuple[int, str, int, tuple[int, ...]]] = []
    try:
        root = os.open(
            "/",
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        descriptors.append(root)
        root_metadata = os.fstat(root)
        if not _safe_source_directory(root_metadata, expected_uid=expected_uid):
            raise NodeAuthorityError("bootstrap source asset parent is unsafe")
        parent = root
        for component in directory_parts:
            lexical = os.stat(component, dir_fd=parent, follow_symlinks=False)
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                dir_fd=parent,
            )
            descriptors.append(child)
            metadata = os.fstat(child)
            if (
                not _safe_source_directory(lexical, expected_uid=expected_uid)
                or not _safe_source_directory(metadata, expected_uid=expected_uid)
                or _metadata_identity(lexical) != _metadata_identity(metadata)
            ):
                raise NodeAuthorityError("bootstrap source asset parent is unsafe")
            directory_records.append(
                (parent, component, child, _metadata_identity(metadata)),
            )
            parent = child
        lexical = os.stat(relative.name, dir_fd=parent, follow_symlinks=False)
        descriptor = os.open(
            relative.name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
            dir_fd=parent,
        )
        descriptors.append(descriptor)
        before = os.fstat(descriptor)
        payload = _read_fd_twice(
            descriptor,
            limit=MAX_REQUEST_BYTES,
            error="bootstrap source asset changed during verification",
        )
        after = os.fstat(descriptor)
        current = os.stat(relative.name, dir_fd=parent, follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or before.st_uid != expected_uid
            or before.st_gid != expected_uid
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_nlink != 1
            or _metadata_identity(lexical) != _metadata_identity(before)
            or _metadata_identity(before) != _metadata_identity(after)
            or _metadata_identity(after) != _metadata_identity(current)
        ):
            raise NodeAuthorityError("bootstrap source asset metadata is unsafe")
        for parent_fd, component, child_fd, identity in directory_records:
            lexical_parent = os.stat(
                component,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            descriptor_parent = os.fstat(child_fd)
            if (
                _metadata_identity(lexical_parent) != identity
                or _metadata_identity(descriptor_parent) != identity
                or not _safe_source_directory(
                    descriptor_parent,
                    expected_uid=expected_uid,
                )
            ):
                raise NodeAuthorityError(
                    "bootstrap source asset parent changed during verification",
                )
        return payload
    except NodeAuthorityError:
        raise
    except OSError as exc:
        raise NodeAuthorityError("bootstrap source asset is unavailable") from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _git(*args: str) -> str:
    result = subprocess.run(
        (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            f"safe.directory={REPO_ROOT}",
            "-C",
            str(REPO_ROOT),
            *args,
        ),
        env=_clean_env(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or result.stderr:
        raise NodeAuthorityError("bootstrap exact source verification failed safely")
    return result.stdout.strip()


def _git_bytes(*args: str) -> bytes:
    result = subprocess.run(
        (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            f"safe.directory={REPO_ROOT}",
            "-C",
            str(REPO_ROOT),
            *args,
        ),
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0 or result.stderr:
        raise NodeAuthorityError("bootstrap exact source verification failed safely")
    return result.stdout


def _exact_source_assets(source_sha: str, source_tree: str) -> dict[str, bytes]:
    assets: dict[str, bytes] = {}
    for relative in SOURCE_ASSETS:
        payload = _source_asset(relative)
        committed = _git_bytes(
            "cat-file",
            "blob",
            f"{source_sha}:{relative.as_posix()}",
        )
        if (
            payload != committed
            or hashlib.sha256(payload).digest() != hashlib.sha256(committed).digest()
        ):
            raise NodeAuthorityError(
                "bootstrap source asset does not match the exact candidate",
            )
        assets[str(relative)] = payload
    if assets[str(SUDOERS_RELATIVE)] != NODE_AUTHORITY_SUDOERS_PAYLOAD:
        raise NodeAuthorityError("node authority sudoers operator contract drifted")
    if (
        _git("rev-parse", "--verify", "HEAD") != source_sha
        or _git("rev-parse", "--verify", "HEAD^{tree}") != source_tree
        or _git("status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise NodeAuthorityError("bootstrap source changed during verification")
    return assets


def _policy_payload(
    source_sha: str,
    source_tree: str,
    node: str,
    assets: Mapping[str, bytes],
) -> bytes:
    return _canonical(
        {
            "schema_version": SCHEMA_VERSION,
            "source_sha": source_sha,
            "source_tree": source_tree,
            "node": node,
            "asset_sha256": {
                name: hashlib.sha256(payload).hexdigest()
                for name, payload in sorted(assets.items())
            },
        },
    )


def _read_policy() -> AuthorityPolicy:
    raw = _safe_root_file(POLICY, mode=0o600, limit=1 << 20)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("node authority policy is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "source_sha", "source_tree", "node", "asset_sha256"}
        or payload.get("schema_version") != SCHEMA_VERSION
        or not _is_sha(payload.get("source_sha"))
        or not _is_sha(payload.get("source_tree"))
        or payload.get("node") not in NODE_HOSTNAMES
        or not isinstance(payload.get("asset_sha256"), dict)
        or raw != _canonical(payload)
    ):
        raise NodeAuthorityError("node authority policy is invalid")
    digests = payload["asset_sha256"]
    _policy_asset_generation(digests)
    if any(not _is_sha(value, length=64) for value in digests.values()):
        raise NodeAuthorityError("node authority policy asset identity is invalid")
    return AuthorityPolicy(
        source_sha=payload["source_sha"],
        source_tree=payload["source_tree"],
        node=payload["node"],
        asset_sha256=dict(digests),
    )


def _validate_runtime_assets(
    policy: AuthorityPolicy,
    *,
    allow_absent_system_install: bool = False,
) -> tuple[dict[str, Any], ...]:
    if _hostname() != NODE_HOSTNAMES[policy.node]:
        raise NodeAuthorityError("node authority policy host binding is invalid")
    generation = _policy_asset_generation(policy.asset_sha256)
    for relative in SOURCE_ASSETS:
        installed = SOURCE_ROOT / relative
        if str(relative) not in policy.asset_sha256:
            if (
                generation == "legacy-v1"
                and allow_absent_system_install
                and relative in LEGACY_V1_MIGRATABLE_SOURCE_ASSETS
                and not installed.exists()
                and not installed.is_symlink()
            ):
                continue
            raise NodeAuthorityError("node authority installed source identity is incomplete")
        payload = _safe_root_file(installed, mode=_source_asset_mode(relative))
        if hashlib.sha256(payload).hexdigest() != policy.asset_sha256[str(relative)]:
            raise NodeAuthorityError("node authority installed source drifted")
    for relative in RETIRED_LEGACY_SOURCE_ASSETS:
        installed = SOURCE_ROOT / relative
        if generation == "current":
            if installed.exists() or installed.is_symlink():
                raise NodeAuthorityError("node authority retired source asset is still installed")
            continue
        payload = _safe_root_file(installed, mode=_source_asset_mode(relative))
        if hashlib.sha256(payload).hexdigest() != policy.asset_sha256[str(relative)]:
            raise NodeAuthorityError("node authority installed source drifted")
    helper = _safe_root_file(LIBEXEC, mode=0o755)
    expected_helper = _safe_root_file(
        SOURCE_ROOT / "scripts/ops/developer_sandbox_node_authority.py",
        mode=0o755,
    )
    if helper != expected_helper:
        raise NodeAuthorityError("node authority wrapper drifted")
    sudoers = _safe_root_file(SUDOERS, mode=0o440)
    expected_sudoers = _safe_root_file(
        SOURCE_ROOT / SUDOERS_RELATIVE,
        mode=0o440,
    )
    if sudoers != expected_sudoers:
        raise NodeAuthorityError("node authority sudoers drifted")
    return _validate_system_install_assets(
        allow_absent=allow_absent_system_install,
    )


def _retire_legacy_source_assets(old_policy: AuthorityPolicy) -> None:
    if _policy_asset_generation(old_policy.asset_sha256) == "current":
        return
    for relative in RETIRED_LEGACY_SOURCE_ASSETS:
        payload = _unlink_root_file(
            SOURCE_ROOT / relative,
            mode=_source_asset_mode(relative),
        )
        if hashlib.sha256(payload).hexdigest() != old_policy.asset_sha256[str(relative)]:
            raise NodeAuthorityError("node authority retired source asset drifted")


def _validate_invoker(verb: str, environ: Mapping[str, str]) -> None:
    try:
        sudo_uid = int(environ.get("SUDO_UID", ""))
        sudo_gid = int(environ.get("SUDO_GID", ""))
        operator = pwd.getpwnam(OPERATOR)
    except (KeyError, ValueError) as exc:
        raise NodeAuthorityError("node authority caller identity is unavailable") from exc
    if (
        verb not in {"transact", "check"}
        or os.geteuid() != 0
        or environ.get("SUDO_USER") != OPERATOR
        or sudo_uid != operator.pw_uid
        or sudo_gid != operator.pw_gid
        or environ.get("SUDO_COMMAND") != f"{LIBEXEC} {verb}"
    ):
        raise NodeAuthorityError("node authority invocation is not approved")


def _validate_acceptance_probe_request(
    decoded: bytes,
    outer: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> None:
    try:
        request = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("acceptance probe payload is invalid") from exc
    unsigned = (
        {key: value for key, value in request.items() if key != "payload_sha256"}
        if isinstance(request, dict)
        else {}
    )
    domain = str(outer["domain"])
    route = ACCEPTANCE_PROBE_ROUTE[domain]
    environments = (
        [
            row
            for row in snapshot["environments"]
            if row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["runtime_id"] == request.get("runtime_id")
        ]
        if isinstance(request, dict)
        else []
    )
    deployments = (
        [
            row
            for row in snapshot["deployments"]
            if row["deployment_id"] == request.get("deployment_id")
            and row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["candidate_id"] == request.get("candidate_id")
        ]
        if isinstance(request, dict)
        else []
    )
    candidates = (
        [
            row
            for row in snapshot["candidates"]
            if row["candidate_id"] == request.get("candidate_id")
            and row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["candidate_sha"] == request.get("candidate_sha")
            and row["candidate_tree"] == request.get("candidate_tree")
        ]
        if isinstance(request, dict)
        else []
    )
    environment = environments[0] if len(environments) == 1 else {}
    deployment = deployments[0] if len(deployments) == 1 else {}
    if (
        not isinstance(request, dict)
        or set(request) != ACCEPTANCE_PROBE_REQUEST_FIELDS
        or decoded != _canonical(request)
        or request.get("schema_version") != SCHEMA_VERSION
        or request.get("kind") != ACCEPTANCE_PROBE_REQUEST_KIND
        or request.get("action") != ACCEPTANCE_PROBE_ACTION
        or request.get("domain") != domain
        or request.get("cluster") != route["cluster"]
        or request.get("submit_host") != route["submit_host"]
        or request.get("controller") != route["controller"]
        or outer.get("node") != route["node"]
        or request.get("runtime_id") != outer["sandbox"]
        or request.get("candidate_sha") != outer["candidate_sha"]
        or request.get("candidate_tree") != outer["candidate_tree"]
        or re.fullmatch(r"dep-[0-9a-f]{32}", str(request.get("deployment_id"))) is None
        or re.fullmatch(r"denv-[a-z0-9-]{8,64}", str(request.get("env_id"))) is None
        or re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9:._@/+%-]{1,254}",
            str(request.get("principal_id")),
        )
        is None
        or re.fullmatch(r"cand-[0-9a-f]{40}", str(request.get("candidate_id"))) is None
        or SAFE_RUNTIME_RE.fullmatch(str(request.get("runtime_id"))) is None
        or not _is_sha(request.get("candidate_sha"))
        or not _is_sha(request.get("candidate_tree"))
        or type(request.get("applied_resource_generation")) is not int
        or request["applied_resource_generation"] < 2
        or type(request.get("registry_generation")) is not int
        or request["registry_generation"] < 1
        or not _is_sha(request.get("registry_snapshot_sha256"), length=64)
        or re.fullmatch(r"[a-z][a-z0-9_-]{1,62}", str(request.get("service_user"))) is None
        or re.fullmatch(r"[a-z][a-z0-9_-]{1,62}", str(request.get("slurm_account"))) is None
        or re.fullmatch(r"[a-z][a-z0-9_-]{1,62}", str(request.get("slurm_qos"))) is None
        or re.fullmatch(
            r"loom-env-[a-z0-9][a-z0-9-]{0,62}-finalize-[0-9a-f]{12}",
            str(request.get("job_name")),
        )
        is None
        or request.get("time_limit_seconds") != 300
        or request.get("health_services") != ["control-plane", "gateway", "minio"]
        or request.get("general_admission_authorized") is not False
        or request.get("foreign_job_action") != "observe-only"
        or not _is_sha(request.get("idempotency_key"), length=64)
        or request.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
        or snapshot.get("generation") != request.get("registry_generation")
        or snapshot.get("payload_sha256") != request.get("registry_snapshot_sha256")
        or len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
        or environment.get("state") != "deploying"
        or environment.get("resource_generation") != deployment.get("expected_resource_generation")
        or environment.get("slurm_user") != request.get("service_user")
        or environment.get("service_user") != request.get("service_user")
        or environment.get("slurm_account") != request.get("slurm_account")
        or environment.get("slurm_qos") != request.get("slurm_qos")
        or deployment.get("phase") != "verified"
        or deployment.get("applied_resource_generation")
        != deployment.get("expected_resource_generation", 0) + 1
        or deployment.get("applied_resource_generation")
        != request.get("applied_resource_generation")
        or type(deployment.get("applied_registry_generation")) is not int
        or deployment["applied_registry_generation"] < 1
        or not _is_sha(
            deployment.get("applied_registry_payload_sha256"),
            length=64,
        )
        or deployment.get("finalization_payload_sha256") is not None
    ):
        raise NodeAuthorityError("acceptance probe registry binding is invalid")


def _runtime_retire_wal(env_id: str) -> dict[str, Any]:
    try:
        raw = _safe_root_file(
            RUNTIME_RETIRE_WAL_ROOT / f"{env_id}.json",
            mode=0o600,
            limit=1 << 20,
        )
        wal = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("runtime retirement WAL is invalid") from exc
    unsigned = (
        {key: value for key, value in wal.items() if key != "payload_sha256"}
        if isinstance(wal, dict)
        else {}
    )
    if (
        not isinstance(wal, dict)
        or set(wal) != RUNTIME_RETIRE_WAL_FIELDS
        or raw != _canonical(wal)
        or wal.get("schema_version") != SCHEMA_VERSION
        or wal.get("kind") != "loom.developer-environment.retire-journal"
        or wal.get("phase") != "capacity-retired"
        or not isinstance(wal.get("evidence"), dict)
        or set(wal["evidence"]) != {"capacity_retire"}
        or not _is_sha(wal["evidence"].get("capacity_retire"), length=64)
        or wal.get("object_checkpoints") != {}
        or wal.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
    ):
        raise NodeAuthorityError("runtime retirement WAL binding is invalid")
    return wal


def _runtime_retire_candidate_bindings(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
) -> list[dict[str, str]]:
    deployments = [row for row in snapshot["deployments"] if row["env_id"] == environment["env_id"]]
    candidate_ids = {
        str(environment["current_candidate_id"]),
        *(str(row["candidate_id"]) for row in deployments if row["phase"] == "failed"),
    }
    candidates = {
        str(row["candidate_id"]): row
        for row in snapshot["candidates"]
        if row["env_id"] == environment["env_id"]
        and row["principal_id"] == environment["principal_id"]
        and str(row["candidate_id"]) in candidate_ids
    }
    if set(candidates) != candidate_ids:
        raise NodeAuthorityError("runtime retirement candidate set is incomplete")
    bindings = [
        {
            "candidate_id": candidate_id,
            "candidate_sha": str(candidates[candidate_id]["candidate_sha"]),
            "candidate_tree": str(candidates[candidate_id]["candidate_tree"]),
        }
        for candidate_id in sorted(candidate_ids)
    ]
    if any(
        re.fullmatch(r"cand-[0-9a-f]{40}", row["candidate_id"]) is None
        or not _is_sha(row["candidate_sha"])
        or not _is_sha(row["candidate_tree"])
        for row in bindings
    ):
        raise NodeAuthorityError("runtime retirement candidate set is invalid")
    return bindings


def _validate_runtime_retire_request(
    decoded: bytes,
    outer: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> None:
    try:
        request = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("runtime retirement payload is invalid") from exc
    unsigned = (
        {key: value for key, value in request.items() if key != "payload_sha256"}
        if isinstance(request, dict)
        else {}
    )
    environments = (
        [
            row
            for row in snapshot["environments"]
            if row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["runtime_id"] == request.get("runtime_id")
        ]
        if isinstance(request, dict)
        else []
    )
    environment = environments[0] if len(environments) == 1 else {}
    deployments = (
        [
            row
            for row in snapshot["deployments"]
            if row["deployment_id"] == request.get("deployment_id")
            and row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
        ]
        if isinstance(request, dict)
        else []
    )
    deployment = deployments[0] if len(deployments) == 1 else {}
    expected_candidates = (
        _runtime_retire_candidate_bindings(snapshot, environment) if environment else []
    )
    current = (
        [
            row
            for row in expected_candidates
            if row["candidate_id"] == request.get("current_candidate_id")
        ]
        if isinstance(request, dict)
        else []
    )
    wal = (
        _runtime_retire_wal(str(request["env_id"]))
        if isinstance(request, dict)
        and re.fullmatch(r"denv-[a-z0-9-]{8,64}", str(request.get("env_id")))
        else {}
    )
    if (
        not isinstance(request, dict)
        or set(request) != RUNTIME_RETIRE_REQUEST_FIELDS
        or decoded != _canonical(request)
        or request.get("schema_version") != SCHEMA_VERSION
        or request.get("kind") != RUNTIME_RETIRE_REQUEST_KIND
        or request.get("action") != RUNTIME_RETIRE_ACTION
        or request.get("node") != outer["node"]
        or request.get("domain") != outer["domain"]
        or request.get("domain")
        != ("oldlab" if str(request.get("node")).startswith("oldlab-") else "gb10")
        or request.get("runtime_id") != outer["sandbox"]
        or request.get("foreign_path_action") != "preserve"
        or request.get("audit_action") != "append-only-preserve"
        or not _is_sha(request.get("retire_operation_sha256"), length=64)
        or request.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
        or snapshot.get("generation") != request.get("registry_generation")
        or snapshot.get("payload_sha256") != request.get("registry_snapshot_sha256")
        or len(environments) != 1
        or len(deployments) != 1
        or environment.get("state") != "quarantined"
        or environment.get("resource_generation") != request.get("resource_generation")
        or environment.get("current_candidate_id") != request.get("current_candidate_id")
        or deployment.get("phase") != "committed"
        or deployment.get("candidate_id") != request.get("current_candidate_id")
        or deployment.get("applied_resource_generation") != request.get("resource_generation")
        or request.get("candidate_bindings") != expected_candidates
        or len(current) != 1
        or current[0]["candidate_sha"] != outer["candidate_sha"]
        or current[0]["candidate_tree"] != outer["candidate_tree"]
        or wal.get("payload_sha256") != request.get("retire_operation_sha256")
        or wal.get("env_id") != request.get("env_id")
        or wal.get("principal_id") != request.get("principal_id")
        or wal.get("runtime_id") != request.get("runtime_id")
        or wal.get("expected_resource_generation") != request.get("resource_generation")
        or wal.get("current_candidate_id") != request.get("current_candidate_id")
        or wal.get("uid") != environment.get("uid")
        or wal.get("gid") != environment.get("gid")
        or wal.get("service_user") != environment.get("service_user")
        or wal.get("service_group") != environment.get("service_group")
        or wal.get("slurm_user") != environment.get("slurm_user")
        or wal.get("slurm_account") != environment.get("slurm_account")
        or wal.get("slurm_qos") != environment.get("slurm_qos")
    ):
        raise NodeAuthorityError("runtime retirement registry binding is invalid")


def _parse_request(raw: bytes, *, verb: str, policy: AuthorityPolicy) -> Request:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("node authority request is invalid") from exc
    actions = TRANSACT_ACTIONS if verb == "transact" else CHECK_ACTIONS
    requested_action = payload.get("action") if isinstance(payload, dict) else None
    expected_request_fields = (
        REGISTRY_SNAPSHOT_SYNC_FIELDS
        if requested_action == REGISTRY_SNAPSHOT_SYNC_ACTION
        else (
            DYNAMIC_REQUEST_FIELDS
            | (
                DEPLOYMENT_TARGET_BINDING_FIELDS
                if requested_action in DEPLOYMENT_TARGET_ACTIONS
                else set()
            )
            if requested_action in DYNAMIC_TARGET_ACTIONS
            else REQUEST_FIELDS
        )
    )
    if (
        not isinstance(payload, dict)
        or set(payload) != expected_request_fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or requested_action not in actions
        or payload.get("node") != policy.node
        or payload.get("domain") not in DOMAINS
        or (
            payload.get("sandbox") != STAGING_SCOPE
            if requested_action in STAGING_ACTIONS
            else SAFE_RUNTIME_RE.fullmatch(str(payload.get("sandbox"))) is None
        )
        or not _is_sha(payload.get("candidate_sha"))
        or not _is_sha(payload.get("candidate_tree"))
        or payload.get("payload_kind") != PAYLOAD_KIND.get(requested_action)
        or not _is_sha(payload.get("payload_sha256"), length=64)
        or not isinstance(payload.get("payload_base64"), str)
        or (
            requested_action == REGISTRY_SNAPSHOT_SYNC_ACTION
            and (
                type(payload.get("registry_generation")) is not int
                or int(payload["registry_generation"]) < 1
                or not _is_sha(
                    payload.get("registry_payload_sha256"),
                    length=64,
                )
            )
        )
        or (
            requested_action in DEPLOYMENT_TARGET_ACTIONS
            and re.fullmatch(
                r"dep-[0-9a-f]{32}",
                str(payload.get("deployment_id")),
            )
            is None
        )
        or (
            payload.get("prior_request_id") is not None
            and not _is_sha(payload.get("prior_request_id"), length=64)
        )
        or not _is_sha(payload.get("request_id"), length=64)
        or payload.get("request_id") != _request_digest(payload)
        or raw != _canonical(payload)
    ):
        raise NodeAuthorityError("node authority request binding is invalid")
    action = str(payload["action"])
    prior = payload["prior_request_id"]
    if (action in {"rollback", "slurm-rollback"}) != (prior is not None):
        raise NodeAuthorityError("node authority rollback binding is invalid")
    try:
        decoded = base64.b64decode(payload["payload_base64"], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise NodeAuthorityError("node authority request payload is invalid") from exc
    if (
        len(decoded) > MAX_PAYLOAD_BYTES
        or base64.b64encode(decoded).decode("ascii") != payload["payload_base64"]
        or hashlib.sha256(decoded).hexdigest() != payload["payload_sha256"]
        or (payload["payload_kind"] == "none" and decoded)
        or (payload["payload_kind"] != "none" and not decoded)
    ):
        raise NodeAuthorityError("node authority request payload binding is invalid")
    snapshot: dict[str, Any] | None = None
    binding: tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]] | None = None
    if action == REGISTRY_SNAPSHOT_SYNC_ACTION:
        snapshot = _verify_registry_snapshot_bytes(decoded)
        if (
            snapshot["generation"] != payload["registry_generation"]
            or snapshot["payload_sha256"] != payload["registry_payload_sha256"]
        ):
            raise NodeAuthorityError("registry snapshot sync binding is invalid")
    elif action in DYNAMIC_TARGET_ACTIONS:
        snapshot = _load_registry_snapshot()
        cohort = _registry_cohort(
            snapshot,
            include_provisioning=True,
            deployment_id=(
                str(payload["deployment_id"]) if action in DEPLOYMENT_TARGET_ACTIONS else None
            ),
            target_resource_generation=(
                int(payload["resource_generation"]) if action in DEPLOYMENT_TARGET_ACTIONS else None
            ),
            include_retiring=action == "slurm-identity-retire",
        )
        binding = cohort.get(str(payload["sandbox"]))
        if (
            binding is None
            or binding[0]["env_id"] != payload["env_id"]
            or binding[0]["resource_generation"] != payload["resource_generation"]
            or binding[1]["candidate_id"] != payload["candidate_id"]
            or binding[1]["candidate_sha"] != payload["candidate_sha"]
            or binding[1]["candidate_tree"] != payload["candidate_tree"]
            or snapshot["generation"] != payload["registry_generation"]
            or snapshot["payload_sha256"] != payload["registry_payload_sha256"]
        ):
            raise NodeAuthorityError("node authority dynamic target binding is invalid")
    elif action not in STAGING_ACTIONS and action not in {
        "slurm-rollback",
        ACCEPTANCE_PROBE_ACTION,
        RUNTIME_RETIRE_ACTION,
    }:
        snapshot = _load_registry_snapshot()
        provisioning_actions = {
            "slurm-node-converge",
            "slurm-controller-converge",
            "slurm-identity-preflight",
            "slurm-identity-converge",
        }
        cohort = _registry_cohort(
            snapshot,
            include_provisioning=action in provisioning_actions,
        )
        binding = cohort.get(str(payload["sandbox"]))
        if action == "slurm-identity-inventory":
            if cohort:
                if (
                    binding is None
                    or binding[1]["candidate_sha"] != payload["candidate_sha"]
                    or binding[1]["candidate_tree"] != payload["candidate_tree"]
                    or payload["candidate_sha"] != policy.source_sha
                    or payload["candidate_tree"] != policy.source_tree
                ):
                    raise NodeAuthorityError("node authority registry binding is invalid")
            elif (
                payload["sandbox"] != FLEET_BOOTSTRAP_SCOPE
                or payload["candidate_sha"] != policy.source_sha
                or payload["candidate_tree"] != policy.source_tree
            ):
                raise NodeAuthorityError("node authority bootstrap binding is invalid")
        elif (
            binding is None
            or binding[1]["candidate_sha"] != payload["candidate_sha"]
            or binding[1]["candidate_tree"] != payload["candidate_tree"]
        ):
            raise NodeAuthorityError("node authority registry binding is invalid")
    if payload["payload_kind"] == "developer-environment-acceptance-probe-json":
        if snapshot is None:
            snapshot = _load_registry_snapshot()
        _validate_acceptance_probe_request(decoded, payload, snapshot)
    if payload["payload_kind"] == "developer-environment-runtime-retire-json":
        if snapshot is None:
            snapshot = _load_registry_snapshot()
        _validate_runtime_retire_request(decoded, payload, snapshot)
    if payload["payload_kind"] == "developer-environment-identity-preflight-json":
        try:
            identity_preflight = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("developer identity preflight payload is invalid") from exc
        if snapshot is None or binding is None:
            raise NodeAuthorityError("developer identity preflight registry binding is invalid")
        cohort = _registry_cohort(
            snapshot,
            include_provisioning=True,
            deployment_id=(
                str(payload["deployment_id"]) if action in DEPLOYMENT_TARGET_ACTIONS else None
            ),
            target_resource_generation=(
                int(payload["resource_generation"]) if action in DEPLOYMENT_TARGET_ACTIONS else None
            ),
            include_retiring=action == "slurm-identity-retire",
        )
        candidate_bindings = {
            row[0]["slurm_account"]: {
                "env_id": row[0]["env_id"],
                "resource_generation": row[0]["resource_generation"],
                "sandbox": runtime_id,
                "service_user": row[0]["slurm_user"],
                "slurm_qos": row[0]["slurm_qos"],
                "candidate_id": row[1]["candidate_id"],
                "candidate_sha": row[1]["candidate_sha"],
                "candidate_tree": row[1]["candidate_tree"],
            }
            for runtime_id, row in cohort.items()
        }
        candidate_set_sha256 = hashlib.sha256(
            _canonical(candidate_bindings).rstrip(b"\n"),
        ).hexdigest()
        environment = binding[0]
        if (
            not isinstance(identity_preflight, dict)
            or set(identity_preflight) != IDENTITY_PREFLIGHT_FIELDS
            or decoded != _canonical(identity_preflight)
            or identity_preflight.get("schema_version") != 2
            or identity_preflight.get("kind") != IDENTITY_PREFLIGHT_KIND
            or identity_preflight.get("env_id") != environment["env_id"]
            or identity_preflight.get("principal_id") != environment["principal_id"]
            or identity_preflight.get("resource_generation") != environment["resource_generation"]
            or identity_preflight.get("service_user") != environment["service_user"]
            or identity_preflight.get("service_group") != environment["service_group"]
            or identity_preflight.get("uid") != environment["uid"]
            or identity_preflight.get("gid") != environment["gid"]
            or identity_preflight.get("slurm_account") != environment["slurm_account"]
            or identity_preflight.get("slurm_qos") != environment["slurm_qos"]
            or identity_preflight.get("registry_generation") != snapshot["generation"]
            or identity_preflight.get("registry_payload_sha256") != snapshot["payload_sha256"]
            or identity_preflight.get("candidate_set_sha256") != candidate_set_sha256
            or (
                identity_preflight.get("revive_journal_sha256") is not None
                and not _is_sha(
                    identity_preflight.get("revive_journal_sha256"),
                    length=64,
                )
            )
        ):
            raise NodeAuthorityError("developer identity preflight payload is invalid")
    if payload["payload_kind"] == "developer-environment-identity-inventory-json":
        try:
            identity_inventory = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("developer identity inventory payload is invalid") from exc
        if (
            snapshot is None
            or not isinstance(identity_inventory, dict)
            or set(identity_inventory) != IDENTITY_INVENTORY_FIELDS
            or decoded != _canonical(identity_inventory)
            or identity_inventory.get("schema_version") != SCHEMA_VERSION
            or identity_inventory.get("kind") != IDENTITY_INVENTORY_KIND
            or identity_inventory.get("uid_start") != IDENTITY_UID_START
            or identity_inventory.get("uid_end") != IDENTITY_UID_END
            or identity_inventory.get("registry_generation") != snapshot["generation"]
            or identity_inventory.get("registry_payload_sha256") != snapshot["payload_sha256"]
        ):
            raise NodeAuthorityError("developer identity inventory payload is invalid")
    if payload["payload_kind"] == "fleet-attestation-json":
        try:
            fleet = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError(
                "fleet attestation payload is invalid",
            ) from exc
        if (
            len(decoded) > MAX_FLEET_ATTESTATION_BYTES
            or not isinstance(fleet, dict)
            or decoded != _canonical(fleet)
        ):
            raise NodeAuthorityError("fleet attestation payload is invalid")
    if payload["payload_kind"] == "slurm-candidate-set-json":
        if snapshot is None:
            snapshot = _load_registry_snapshot()
        try:
            candidate_set = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("Slurm candidate-set payload is invalid") from exc
        bindings = (
            candidate_set.get("candidate_bindings") if isinstance(candidate_set, dict) else None
        )
        registry_cohort = _registry_cohort(snapshot, include_provisioning=True)
        expected_accounts = {
            binding[0]["slurm_account"]: (sandbox, binding)
            for sandbox, binding in registry_cohort.items()
        }
        if (
            not isinstance(candidate_set, dict)
            or decoded != _canonical(candidate_set)
            or set(candidate_set)
            != {
                "schema_version",
                "kind",
                "candidate_set_sha256",
                "candidate_bindings",
                "generation",
                "convergence_id",
                "registry_generation",
                "registry_payload_sha256",
            }
            or candidate_set.get("schema_version") != 2
            or candidate_set.get("kind") != "loom.developer-sandbox.slurm-candidate-set"
            or type(candidate_set.get("generation")) is not int
            or candidate_set["generation"] < 1
            or not _is_sha(candidate_set.get("convergence_id"), length=64)
            or candidate_set.get("registry_generation") != snapshot["generation"]
            or candidate_set.get("registry_payload_sha256") != snapshot["payload_sha256"]
            or not isinstance(bindings, dict)
            or set(bindings) != set(expected_accounts)
            or any(
                not isinstance(bindings[account], dict)
                or set(bindings[account])
                != {
                    "sandbox",
                    "env_id",
                    "resource_generation",
                    "service_user",
                    "slurm_qos",
                    "candidate_id",
                    "candidate_sha",
                    "candidate_tree",
                }
                or bindings[account].get("sandbox") != sandbox
                or bindings[account].get("env_id") != binding[0]["env_id"]
                or bindings[account].get("resource_generation") != binding[0]["resource_generation"]
                or bindings[account].get("service_user") != binding[0]["slurm_user"]
                or bindings[account].get("slurm_qos") != binding[0]["slurm_qos"]
                or bindings[account].get("candidate_id") != binding[1]["candidate_id"]
                or bindings[account].get("candidate_sha") != binding[1]["candidate_sha"]
                or bindings[account].get("candidate_tree") != binding[1]["candidate_tree"]
                for account, (sandbox, binding) in expected_accounts.items()
            )
            or candidate_set.get("candidate_set_sha256")
            != hashlib.sha256(_canonical(bindings).rstrip(b"\n")).hexdigest()
            or bindings[registry_cohort[str(payload["sandbox"])][0]["slurm_account"]].get(
                "candidate_sha"
            )
            != payload["candidate_sha"]
            or bindings[registry_cohort[str(payload["sandbox"])][0]["slurm_account"]].get(
                "candidate_tree"
            )
            != payload["candidate_tree"]
        ):
            raise NodeAuthorityError("Slurm candidate-set payload is invalid")
    if payload["payload_kind"] in {
        "live-overlap-collection-json",
        "live-overlap-job-json",
    }:
        try:
            live_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("live overlap payload is invalid") from exc
        if not isinstance(live_payload, dict) or decoded != _canonical(live_payload):
            raise NodeAuthorityError("live overlap payload is invalid")
        if payload["payload_kind"] == "live-overlap-collection-json":
            try:
                uuid.UUID(str(live_payload.get("collection_id")))
            except (ValueError, AttributeError) as exc:
                raise NodeAuthorityError("live overlap collection identity is invalid") from exc
            if (
                set(live_payload) != LIVE_COLLECTION_FIELDS
                or live_payload.get("schema_version") != SCHEMA_VERSION
                or live_payload.get("kind") != "loom.developer-sandbox.live-overlap-collection"
                or not _is_sha(live_payload.get("candidate_tree"))
                or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(live_payload.get("job_id"))) is None
            ):
                raise NodeAuthorityError("live overlap collection payload is invalid")
        elif binding is None:
            raise NodeAuthorityError("live overlap Slurm registry binding is invalid")
        elif (
            set(live_payload) != LIVE_SLURM_REQUEST_FIELDS
            or live_payload.get("schema_version") != SCHEMA_VERSION
            or live_payload.get("kind") != "loom.developer-sandbox.live-slurm-request"
            or live_payload.get("sandbox") != payload["sandbox"]
            or live_payload.get("pool") != payload["domain"]
            or live_payload.get("candidate_sha") != payload["candidate_sha"]
            or not _is_sha(live_payload.get("candidate_tree"))
            or live_payload.get("source_host")
            != ("trt-eai-oldlab-2" if payload["domain"] == "oldlab" else "trt-gb10-1")
            or live_payload.get("account") != binding[0]["slurm_account"]
            or live_payload.get("user") != binding[0]["slurm_user"]
            or not isinstance(live_payload.get("job_name"), str)
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", live_payload["job_name"]) is None
            or not isinstance(live_payload.get("node"), str)
            or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(live_payload.get("job_id"))) is None
            or any(
                not isinstance(live_payload.get(field), int)
                or isinstance(live_payload[field], bool)
                or live_payload[field] < minimum
                for field, minimum in (
                    ("requested_cpus", 1),
                    ("requested_memory_mib", 1),
                    ("job_pids_max", 1),
                    ("requested_gpus", 0),
                )
            )
            or not isinstance(live_payload.get("requested_gpu_tres"), str)
        ):
            raise NodeAuthorityError("live overlap Slurm payload is invalid")
    if payload["payload_kind"] == "platform-health-node-json":
        if snapshot is None:
            raise NodeAuthorityError("platform-health registry binding is invalid")
        try:
            platform_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("platform-health node payload is invalid") from exc
        candidates = (
            platform_payload.get("candidates") if isinstance(platform_payload, dict) else None
        )
        checkpoints = {
            "baseline": "baseline",
            "mixed_non_loom": "during",
            "cancel_cleanup": "during",
            "worker_crash": "during",
            "final_drain": "after",
        }
        registry_cohort = _registry_cohort(snapshot, include_provisioning=False)
        if (
            not isinstance(platform_payload, dict)
            or decoded != _canonical(platform_payload)
            or set(platform_payload) != PLATFORM_HEALTH_REQUEST_FIELDS
            or platform_payload.get("schema_version") != SCHEMA_VERSION
            or platform_payload.get("kind") != "loom.developer-sandbox.platform-health-node-request"
            or re.fullmatch(
                r"[0-9a-f]{32}",
                str(platform_payload.get("session_id")),
            )
            is None
            or platform_payload.get("checkpoint") not in checkpoints
            or platform_payload.get("checkpoint_group")
            != checkpoints.get(str(platform_payload.get("checkpoint")))
            or platform_payload.get("expected_node") != payload["node"]
            or platform_payload.get("expected_host") != NODE_HOSTNAMES.get(str(payload["node"]))
            or not isinstance(platform_payload.get("since_at"), str)
            or not isinstance(candidates, dict)
            or set(candidates) != set(registry_cohort)
            or any(
                not isinstance(candidates[sandbox], dict)
                or set(candidates[sandbox]) != {"sha", "tree"}
                or candidates[sandbox].get("sha") != binding[1]["candidate_sha"]
                or candidates[sandbox].get("tree") != binding[1]["candidate_tree"]
                for sandbox, binding in registry_cohort.items()
            )
            or candidates[str(payload["sandbox"])]["sha"] != payload["candidate_sha"]
            or candidates[str(payload["sandbox"])]["tree"] != payload["candidate_tree"]
        ):
            raise NodeAuthorityError("platform-health node payload is invalid")
    if payload["payload_kind"] == "staging-pressure-reclaim-observe-request":
        try:
            pressure_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging pressure observe payload is invalid") from exc
        owned_jobs = (
            pressure_payload.get("owned_jobs") if isinstance(pressure_payload, dict) else None
        )
        if (
            not isinstance(pressure_payload, dict)
            or decoded != _canonical(pressure_payload)
            or set(pressure_payload) != STAGING_PRESSURE_OBSERVE_FIELDS
            or pressure_payload.get("schema_version") != SCHEMA_VERSION
            or pressure_payload.get("kind") != "loom.staging-pressure-reclaim.observe-request"
            or pressure_payload.get("source_host") != "trt-eai-oldlab-1"
            or pressure_payload.get("submit_host") != "trt-gb10-1"
            or pressure_payload.get("environment") != "staging"
            or pressure_payload.get("pool") != "gb10"
            or pressure_payload.get("partition") != "gb10"
            or pressure_payload.get("account") != "loom-staging"
            or pressure_payload.get("qos") != "loom-staging"
            or pressure_payload.get("phase") not in {"before", "during", "after"}
            or re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                str(pressure_payload.get("session_id")),
            )
            is None
            or re.fullmatch(
                r"[0-9a-f]{32}",
                str(pressure_payload.get("acceptance_session_id")),
            )
            is None
            or pressure_payload.get("candidate_sha") != payload["candidate_sha"]
            or pressure_payload.get("candidate_tree") != payload["candidate_tree"]
            or not isinstance(owned_jobs, list)
            or not owned_jobs
            or len(owned_jobs) > 64
            or any(
                not isinstance(job, dict)
                or set(job) != {"job_id", "user", "account", "qos", "name"}
                or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(job.get("job_id"))) is None
                or job.get("user") != "loom-staging-worker"
                or job.get("account") != "loom-staging"
                or job.get("qos") != "loom-staging"
                or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", str(job.get("name"))) is None
                for job in owned_jobs
            )
            or len({str(job["job_id"]) for job in owned_jobs}) != len(owned_jobs)
        ):
            raise NodeAuthorityError("staging pressure observe payload is invalid")
    if payload["payload_kind"] in {
        "staging-allocation-probe-request",
        "staging-allocation-submit-request",
    }:
        try:
            staging_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging allocation payload is invalid") from exc
        if (
            not isinstance(staging_payload, dict)
            or decoded != _canonical(staging_payload)
            or set(staging_payload)
            != (
                STAGING_ALLOCATION_PROBE_FIELDS
                if payload["payload_kind"] == "staging-allocation-probe-request"
                else STAGING_ALLOCATION_SUBMIT_FIELDS
            )
            or staging_payload.get("schema_version") != SCHEMA_VERSION
            or staging_payload.get("kind")
            != (
                "staging_external_slurm_allocation_probe_request"
                if payload["payload_kind"] == "staging-allocation-probe-request"
                else "staging_external_slurm_allocation_submit_request"
            )
            or not _is_sha(staging_payload.get("request_id"), length=64)
            or staging_payload.get("candidate_sha") != payload["candidate_sha"]
            or staging_payload.get("candidate_tree") != payload["candidate_tree"]
            or (
                payload["payload_kind"] == "staging-allocation-submit-request"
                and staging_payload.get("requested_node")
                not in {f"trt-gb10-{index}" for index in range(1, 16)}
            )
        ):
            raise NodeAuthorityError("staging allocation payload is invalid")
    if payload["payload_kind"] == "staging-allocation-cancel-request":
        try:
            staging_cancel = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging allocation cancel payload is invalid") from exc
        if (
            not isinstance(staging_cancel, dict)
            or decoded != _canonical(staging_cancel)
            or set(staging_cancel) != STAGING_ALLOCATION_CANCEL_FIELDS
            or staging_cancel.get("schema_version") != SCHEMA_VERSION
            or staging_cancel.get("kind") != "staging_external_slurm_allocation_cancel_request"
            or not _is_sha(staging_cancel.get("request_id"), length=64)
            or not _is_sha(staging_cancel.get("submit_request_id"), length=64)
            or re.fullmatch(r"[1-9][0-9]*", str(staging_cancel.get("job_id"))) is None
            or staging_cancel.get("requested_node")
            not in {f"trt-gb10-{index}" for index in range(1, 16)}
            or staging_cancel.get("candidate_sha") != payload["candidate_sha"]
            or staging_cancel.get("candidate_tree") != payload["candidate_tree"]
        ):
            raise NodeAuthorityError("staging allocation cancel payload is invalid")
    if payload["payload_kind"] == "staging-infrastructure-operation-request":
        try:
            operation_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging infrastructure operation payload is invalid") from exc
        operation_unsigned = (
            {key: value for key, value in operation_payload.items() if key != "request_id"}
            if isinstance(operation_payload, dict)
            else {}
        )
        if (
            not isinstance(operation_payload, dict)
            or decoded != _canonical(operation_payload)
            or set(operation_payload) != STAGING_INFRASTRUCTURE_OPERATION_FIELDS
            or operation_payload.get("schema_version") != SCHEMA_VERSION
            or operation_payload.get("kind")
            != "loom.staging-external-slurm.infrastructure-operation-request"
            or operation_payload.get("action") != requested_action
            or operation_payload.get("node") != payload["node"]
            or operation_payload.get("candidate_sha") != payload["candidate_sha"]
            or operation_payload.get("candidate_tree") != payload["candidate_tree"]
            or type(operation_payload.get("generation")) is not int
            or operation_payload["generation"] < 1
            or not _is_sha(operation_payload.get("convergence_id"), length=64)
            or _canonical_utc(operation_payload.get("requested_at")) is None
            or operation_payload.get("request_id")
            != hashlib.sha256(_canonical(operation_unsigned)).hexdigest()
        ):
            raise NodeAuthorityError("staging infrastructure operation payload is invalid")
    if payload["payload_kind"] == "staging-infrastructure-converge-request":
        try:
            converge_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging infrastructure converge payload is invalid") from exc
        if (
            not isinstance(converge_payload, dict)
            or decoded != _canonical(converge_payload)
            or set(converge_payload) != STAGING_INFRASTRUCTURE_CONVERGE_FIELDS
            or converge_payload.get("schema_version") != SCHEMA_VERSION
            or converge_payload.get("kind")
            != "loom.staging-external-slurm.infrastructure-converge-request"
            or converge_payload.get("candidate_sha") != payload["candidate_sha"]
            or converge_payload.get("candidate_tree") != payload["candidate_tree"]
            or not _is_sha(converge_payload.get("convergence_id"), length=64)
            or _canonical_utc(converge_payload.get("requested_at")) is None
        ):
            raise NodeAuthorityError("staging infrastructure converge payload is invalid")
    if payload["payload_kind"] == "staging-infrastructure-receipt-json":
        try:
            infrastructure_payload = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("staging infrastructure receipt payload is invalid") from exc
        if (
            not isinstance(infrastructure_payload, dict)
            or decoded != _canonical(infrastructure_payload)
            or set(infrastructure_payload) != STAGING_INFRASTRUCTURE_RECEIPT_FIELDS
            or infrastructure_payload.get("schema_version") != SCHEMA_VERSION
            or infrastructure_payload.get("kind")
            != "loom.staging-external-slurm.infrastructure-receipt"
            or infrastructure_payload.get("candidate_sha") != payload["candidate_sha"]
            or infrastructure_payload.get("candidate_tree") != payload["candidate_tree"]
            or not _is_sha(infrastructure_payload.get("convergence_id"), length=64)
            or _canonical_utc(infrastructure_payload.get("requested_at")) is None
            or not isinstance(infrastructure_payload.get("generation"), int)
            or isinstance(infrastructure_payload.get("generation"), bool)
            or infrastructure_payload["generation"] < 1
        ):
            raise NodeAuthorityError("staging infrastructure receipt payload is invalid")
    node = str(payload["node"])
    domain = str(payload["domain"])
    if requested_action == "inspect-link-client" and domain != (
        "oldlab" if node.startswith("oldlab-") else "gb10"
    ):
        raise NodeAuthorityError("link client inspection binding is invalid")
    if requested_action in {"inspect-link-server", "persist-fleet-attestation"} and (
        node != "oldlab-2" or domain != "oldlab"
    ):
        raise NodeAuthorityError("link server authority binding is invalid")
    if requested_action == "collect-live-overlap" and node != "oldlab-2":
        raise NodeAuthorityError("live overlap collector is OLDLAB2-only")
    if requested_action == "observe-live-overlap-job" and (
        node != ("oldlab-2" if domain == "oldlab" else "trt-gb10-1")
    ):
        raise NodeAuthorityError("live overlap observation is source-host-only")
    if requested_action == "staging-pressure-reclaim-observe" and (
        node != "trt-gb10-1" or domain != "gb10"
    ):
        raise NodeAuthorityError("staging pressure observation is GB10 controller-only")
    if requested_action == "staging-allocation-bootstrap" and (
        domain != "gb10" or node not in STAGING_INFRASTRUCTURE_NODES
    ):
        raise NodeAuthorityError(
            "staging allocation bootstrap is restricted to the GB10 infrastructure set",
        )
    if requested_action == "staging-shared-source-bootstrap" and (
        domain != "gb10" or node != "trt-gb10-2"
    ):
        raise NodeAuthorityError(
            "staging shared source bootstrap is restricted to trt-gb10-2",
        )
    if requested_action == "staging-slurm-accounting-converge" and (
        domain != "gb10" or node != "trt-gb10-1"
    ):
        raise NodeAuthorityError(
            "staging accounting convergence is restricted to trt-gb10-1",
        )
    if requested_action == "staging-infrastructure-converge" and (
        domain != "oldlab" or node != "oldlab-2"
    ):
        raise NodeAuthorityError(
            "staging infrastructure producer is restricted to oldlab-2",
        )
    if requested_action == "staging-infrastructure-install" and (
        domain != "oldlab" or node != "oldlab-1"
    ):
        raise NodeAuthorityError(
            "staging infrastructure receipt install is restricted to oldlab-1",
        )
    if requested_action in {
        "staging-shared-source-bootstrap",
        "staging-slurm-accounting-converge",
        "staging-allocation-bootstrap",
        "staging-infrastructure-converge",
        "staging-infrastructure-install",
    } and (
        payload["candidate_sha"] != policy.source_sha
        or payload["candidate_tree"] != policy.source_tree
    ):
        raise NodeAuthorityError(
            "staging infrastructure request is not the installed exact candidate",
        )
    if requested_action == "staging-allocation-probe" and (
        domain != "gb10" or node != "trt-gb10-1"
    ):
        raise NodeAuthorityError(
            "staging allocation probe is restricted to the fixed GB10 submit host",
        )
    if requested_action in {"staging-allocation-submit", "staging-allocation-cancel"} and (
        domain != "gb10" or node != "trt-gb10-1"
    ):
        raise NodeAuthorityError(
            "staging allocation broker is restricted to the fixed GB10 submit host",
        )
    if requested_action in SLURM_ACTIONS:
        expected_domain = "oldlab" if node.startswith("oldlab-") else "gb10"
        if domain != expected_domain:
            raise NodeAuthorityError("Slurm node domain binding is invalid")
        is_controller = node == SLURM_CONTROLLER[domain]
        if requested_action == "slurm-node-converge" and is_controller:
            raise NodeAuthorityError("Slurm compute convergence excludes the controller")
        if requested_action == "slurm-controller-converge" and not is_controller:
            raise NodeAuthorityError("Slurm controller convergence is controller-only")
        if (
            requested_action
            in {
                "slurm-identity-preflight",
                "slurm-identity-converge",
                "slurm-identity-retire",
            }
            and not is_controller
        ):
            raise NodeAuthorityError(
                "incremental Slurm identity authority is controller-only",
            )
    return Request(payload=payload, payload_bytes=decoded)


def _open_lock(*, exclusive: bool) -> int:
    return _open_named_lock(LOCK, exclusive=exclusive)


def _open_named_lock(path: Path, *, exclusive: bool) -> int:
    flags = os.O_RDWR if exclusive else os.O_RDONLY
    try:
        descriptor = os.open(
            path,
            flags | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise NodeAuthorityError("node authority lock is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        os.close(descriptor)
        raise NodeAuthorityError("node authority lock metadata is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
    return descriptor


def _receipt_path(request_id: str) -> Path:
    return RECEIPT_ROOT / f"{request_id}.json"


def _read_receipt(request_id: str) -> dict[str, Any] | None:
    path = _receipt_path(request_id)
    if not path.exists():
        return None
    raw = _safe_root_file(path, mode=0o600, limit=1 << 20)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("node authority receipt is invalid") from exc
    inner = payload.get("inner_receipt") if isinstance(payload, dict) else None
    action = payload.get("action") if isinstance(payload, dict) else None
    expected_dynamic_fields = DYNAMIC_RECEIPT_FIELDS | (
        DEPLOYMENT_TARGET_BINDING_FIELDS if action in DEPLOYMENT_TARGET_ACTIONS else set()
    )
    dynamic_bound = (
        isinstance(payload, dict)
        and action in DYNAMIC_TARGET_ACTIONS
        and set(payload) == expected_dynamic_fields
    )
    registry_sync_bound = (
        isinstance(payload, dict)
        and action == REGISTRY_SNAPSHOT_SYNC_ACTION
        and set(payload) == REGISTRY_SNAPSHOT_SYNC_RECEIPT_FIELDS
    )
    receipt_fields_valid = isinstance(payload, dict) and (
        set(payload) == RECEIPT_FIELDS or dynamic_bound or registry_sync_bound
    )
    slurm_inner = isinstance(inner, str) and SLURM_BINDING_RE.fullmatch(inner) is not None
    domain_inner = isinstance(inner, str) and inner.startswith(
        "/var/lib/loom-developer-domain-runtime/"
    )
    identity_inner = (
        isinstance(inner, str)
        and re.fullmatch(
            r"/var/lib/loom-developer-sandbox-slurm-policy/identity-tombstones/"
            r"(?:trt-oldlab|trt-gb10)/denv-[a-z0-9-]{8,64}\.json",
            inner,
        )
        is not None
    )
    staging_inner = (
        isinstance(inner, str)
        and re.fullmatch(
            r"(?:staging-probe/v1|staging-accounting/v1|"
            r"staging-infrastructure/v1|"
            r"staging-broker/v1/(?:submission|cancellation))/[0-9a-f]{64}"
            r"|staging-infrastructure-install/v1/[1-9][0-9]*",
            inner,
        )
        is not None
    )
    if (
        not isinstance(payload, dict)
        or not receipt_fields_valid
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("request_id") != request_id
        or payload.get("action") not in TRANSACT_ACTIONS
        or payload.get("node") not in NODE_HOSTNAMES
        or payload.get("domain") not in DOMAINS
        or (
            payload.get("sandbox") != STAGING_SCOPE
            if action in STAGING_ACTIONS
            else SAFE_RUNTIME_RE.fullmatch(str(payload.get("sandbox"))) is None
        )
        or not _is_sha(payload.get("candidate_sha"))
        or not _is_sha(payload.get("candidate_tree"))
        or not _is_sha(payload.get("payload_sha256"), length=64)
        or (
            registry_sync_bound
            and (
                type(payload.get("registry_generation")) is not int
                or int(payload["registry_generation"]) < 1
                or not _is_sha(
                    payload.get("registry_payload_sha256"),
                    length=64,
                )
                or not _is_sha(payload.get("source_sha"))
                or not _is_sha(payload.get("source_tree"))
            )
        )
        or (
            dynamic_bound
            and (
                not isinstance(payload.get("env_id"), str)
                or not isinstance(payload.get("resource_generation"), int)
                or isinstance(payload.get("resource_generation"), bool)
                or int(payload["resource_generation"]) < 1
                or re.fullmatch(r"cand-[0-9a-f]{40}", str(payload.get("candidate_id"))) is None
                or not isinstance(payload.get("registry_generation"), int)
                or isinstance(payload.get("registry_generation"), bool)
                or int(payload["registry_generation"]) < 1
                or not _is_sha(payload.get("registry_payload_sha256"), length=64)
                or (
                    action in DEPLOYMENT_TARGET_ACTIONS
                    and re.fullmatch(
                        r"dep-[0-9a-f]{32}",
                        str(payload.get("deployment_id")),
                    )
                    is None
                )
            )
        )
        or payload.get("status") != "succeeded"
        or not _is_sha(payload.get("result_sha256"), length=64)
        or (
            inner is not None
            and not (
                slurm_inner
                if action
                in {
                    "slurm-node-converge",
                    "slurm-controller-converge",
                    "slurm-rollback",
                }
                else staging_inner
                if action
                in {
                    "staging-allocation-probe",
                    "staging-allocation-submit",
                    "staging-allocation-cancel",
                    "staging-slurm-accounting-converge",
                    "staging-infrastructure-converge",
                    "staging-infrastructure-install",
                }
                else identity_inner
                if action == "slurm-identity-retire"
                else domain_inner
            )
        )
        or not isinstance(payload.get("completed_at"), str)
        or raw != _canonical(payload)
    ):
        raise NodeAuthorityError("node authority receipt binding is invalid")
    try:
        datetime.fromisoformat(str(payload["completed_at"]))
    except ValueError as exc:
        raise NodeAuthorityError("node authority receipt time is invalid") from exc
    return payload


def _write_receipt(payload: Mapping[str, Any]) -> None:
    path = _receipt_path(str(payload["request_id"]))
    if _atomic_install(path, _canonical(payload), 0o600, parent_mode=0o700):
        return
    existing = _read_receipt(str(payload["request_id"]))
    if existing != payload:
        raise NodeAuthorityError("node authority receipt changed during publication")


def _journal_record(receipt: Mapping[str, Any]) -> dict[str, Any]:
    record = {
        "schema_version": SCHEMA_VERSION,
        "request_id": receipt["request_id"],
        "action": receipt["action"],
        "candidate_sha": receipt["candidate_sha"],
        "candidate_tree": receipt["candidate_tree"],
        "result_sha256": receipt["result_sha256"],
        "completed_at": receipt["completed_at"],
        "status": receipt["status"],
    }
    if receipt["action"] == REGISTRY_SNAPSHOT_SYNC_ACTION:
        record.update(
            {
                "registry_generation": receipt["registry_generation"],
                "registry_payload_sha256": receipt["registry_payload_sha256"],
            },
        )
    return record


def _journal_contains(receipt: Mapping[str, Any]) -> bool:
    expected = _journal_record(receipt)
    raw = _safe_root_file(JOURNAL, mode=0o600, limit=MAX_REQUEST_BYTES)
    found = False
    for line in raw.splitlines(keepends=True):
        try:
            record = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("node authority journal is invalid") from exc
        expected_fields = (
            REGISTRY_SNAPSHOT_SYNC_JOURNAL_FIELDS
            if isinstance(record, dict) and record.get("action") == REGISTRY_SNAPSHOT_SYNC_ACTION
            else JOURNAL_FIELDS
        )
        if (
            not isinstance(record, dict)
            or set(record) != expected_fields
            or line != _canonical(record)
        ):
            raise NodeAuthorityError("node authority journal is invalid")
        if record.get("request_id") == receipt["request_id"]:
            if record != expected or found:
                raise NodeAuthorityError("node authority journal receipt drifted")
            found = True
    return found


def _append_journal(receipt: Mapping[str, Any]) -> None:
    record = _journal_record(receipt)
    descriptor = os.open(
        JOURNAL,
        os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise NodeAuthorityError("node authority journal metadata is unsafe")
        view = memoryview(_canonical(record))
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise NodeAuthorityError("node authority journal write failed safely")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _run_fixed(argv: Sequence[str]) -> dict[str, Any]:
    result = subprocess.run(
        tuple(argv),
        env=_clean_env(),
        check=False,
        capture_output=True,
        text=True,
    )
    if (
        result.returncode != 0
        or result.stderr
        or len(result.stdout.encode("utf-8")) > MAX_HELPER_STDOUT_BYTES
    ):
        raise NodeAuthorityError("fixed node helper failed safely")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NodeAuthorityError("fixed node helper returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise NodeAuthorityError("fixed node helper returned invalid JSON")
    return payload


def _run_fixed_input(argv: Sequence[str], payload: bytes) -> dict[str, Any]:
    result = subprocess.run(
        tuple(argv),
        input=payload,
        env=_clean_env(),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0 or result.stderr or len(result.stdout) > MAX_HELPER_STDOUT_BYTES:
        raise NodeAuthorityError("fixed node helper failed safely")
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NodeAuthorityError("fixed node helper returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise NodeAuthorityError("fixed node helper returned invalid JSON")
    return parsed


def _prepare_stage(request: Request) -> Path:
    _ensure_stage_root()
    path = STAGE_ROOT / request.request_id
    try:
        path.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise NodeAuthorityError("node authority request stage is busy") from exc
    os.chown(path, 0, 0)
    os.chmod(path, 0o700)
    _safe_root_directory(path, mode=0o700)
    return path


def _write_stage_file(path: Path, payload: bytes, mode: int) -> None:
    _atomic_install(path, payload, mode, parent_mode=0o700)


def _extract_client_archive(payload: bytes, destination: Path) -> None:
    try:
        archive = tarfile.open(fileobj=io.BytesIO(payload), mode="r:")
    except tarfile.TarError as exc:
        raise NodeAuthorityError("client credential archive is invalid") from exc
    with archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if (
            len(names) != len(set(names))
            or set(names) != CLIENT_ARCHIVE_FILES
            or any(
                not member.isfile()
                or Path(member.name).name != member.name
                or member.size < 1
                or member.size > 16_384
                for member in members
            )
        ):
            raise NodeAuthorityError("client credential archive shape is invalid")
        for member in members:
            source = archive.extractfile(member)
            if source is None:
                raise NodeAuthorityError("client credential archive is invalid")
            content = source.read(16_385)
            if len(content) != member.size or len(content) > 16_384:
                raise NodeAuthorityError("client credential archive entry is invalid")
            mode = 0o644 if member.name in {"ca.pem", "client.pem"} else 0o600
            _write_stage_file(destination / member.name, content, mode)


def _extract_attestation_archive(payload: bytes, destination: Path) -> None:
    try:
        archive = tarfile.open(fileobj=io.BytesIO(payload), mode="r:")
    except tarfile.TarError as exc:
        raise NodeAuthorityError("attestation seed archive is invalid") from exc
    with archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if (
            len(names) != len(set(names))
            or set(names) != ATTESTATION_ARCHIVE_FILES
            or any(
                not member.isfile()
                or Path(member.name).name != member.name
                or member.size < 1
                or member.size > (1 << 20)
                for member in members
            )
        ):
            raise NodeAuthorityError("attestation seed archive shape is invalid")
        for member in members:
            source = archive.extractfile(member)
            if source is None:
                raise NodeAuthorityError("attestation seed archive is invalid")
            content = source.read((1 << 20) + 1)
            if len(content) != member.size or len(content) > (1 << 20):
                raise NodeAuthorityError("attestation seed archive entry is invalid")
            _write_stage_file(destination / member.name, content, 0o600)


def _domain_argv(request: Request, command: str, *extra: str) -> tuple[str, ...]:
    return (
        "/usr/bin/python3",
        "-I",
        "-B",
        str(SOURCE_ROOT / DOMAIN_RUNTIME_RELATIVE),
        command,
        "--config",
        str(SOURCE_ROOT / RUNTIME_CONFIG_RELATIVE),
        *extra,
    )


def _slurm_policy_argv(request: Request, command: str) -> tuple[str, ...]:
    expected_action = {
        "apply": {"slurm-node-converge", "slurm-controller-converge"},
        "rollback": {"slurm-rollback"},
        "node-check": {"slurm-check"},
    }
    if command not in expected_action or request.action not in expected_action[command]:
        raise NodeAuthorityError("Slurm policy command binding is invalid")
    domain = str(request.payload["domain"])
    try:
        candidate_set = json.loads(request.payload_bytes)
        bindings = candidate_set["candidate_bindings"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise NodeAuthorityError("Slurm candidate-set binding is invalid") from exc
    bindings_json = json.dumps(bindings, sort_keys=True, separators=(",", ":"))
    argv = [
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / SLURM_POLICY_RELATIVE),
        command,
        "--profile",
        str(SOURCE_ROOT / "deploy/slurm/developer-sandboxes" / SLURM_PROFILE_NAME[domain]),
        "--candidate-sha",
        str(request.payload["candidate_sha"]),
        "--candidate-bindings-json",
        bindings_json,
        "--transaction-id",
        request.request_id,
        "--candidate-set-generation",
        str(candidate_set["generation"]),
        "--candidate-set-convergence-id",
        str(candidate_set["convergence_id"]),
        "--candidate-set-payload-sha256",
        str(request.payload["payload_sha256"]),
    ]
    if command == "node-check":
        argv.extend(
            (
                "--sandbox",
                str(request.payload["sandbox"]),
            ),
        )
    elif command in {"apply", "rollback"}:
        argv.extend(("--execute", "--restart"))
        if command == "apply" and request.action == "slurm-controller-converge":
            argv.append("--apply-accounting")
    return tuple(argv)


def _slurm_identity_policy_argv(request: Request, command: str) -> tuple[str, ...]:
    expected_action = {
        "identity-check": "slurm-identity-preflight",
        "identity-reconcile": "slurm-identity-converge",
        "identity-retire": "slurm-identity-retire",
    }
    if expected_action.get(command) != request.action:
        raise NodeAuthorityError("incremental Slurm identity command binding is invalid")
    domain = str(request.payload["domain"])
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / SLURM_POLICY_RELATIVE),
        command,
        "--profile",
        str(SOURCE_ROOT / "deploy/slurm/developer-sandboxes" / SLURM_PROFILE_NAME[domain]),
        "--transaction-id",
        request.request_id,
        *(("--execute",) if command != "identity-check" else ()),
    )


def _acceptance_probe_policy_argv(request: Request) -> tuple[str, ...]:
    if request.action != ACCEPTANCE_PROBE_ACTION:
        raise NodeAuthorityError("acceptance probe policy command binding is invalid")
    domain = str(request.payload["domain"])
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / SLURM_POLICY_RELATIVE),
        "acceptance-probe-domain",
        "--profile",
        str(SOURCE_ROOT / "deploy/slurm/developer-sandboxes" / SLURM_PROFILE_NAME[domain]),
        "--transport-request-id",
        request.request_id,
        "--execute",
    )


def _validated_acceptance_probe_result(
    request: Request,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    inner = json.loads(request.payload_bytes)
    domain = str(request.payload["domain"])
    route = ACCEPTANCE_PROBE_ROUTE[domain]
    unsigned = {key: value for key, value in result.items() if key != "payload_sha256"}
    job = result.get("job")
    health = result.get("health")
    terminal = result.get("terminal")
    if (
        set(result) != ACCEPTANCE_PROBE_RECEIPT_FIELDS
        or result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != "loom.developer-environment.acceptance-probe-domain-receipt"
        or result.get("status") != "passed"
        or result.get("action") != "acceptance-probe"
        or result.get("domain") != domain
        or result.get("cluster") != route["cluster"]
        or result.get("submit_host") != route["submit_host"]
        or result.get("controller") != route["controller"]
        or any(
            result.get(field) != inner[field]
            for field in (
                "deployment_id",
                "env_id",
                "principal_id",
                "runtime_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "applied_resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
            )
        )
        or result.get("probe_request_sha256") != inner["payload_sha256"]
        or result.get("transport_request_id") != request.request_id
        or result.get("submission_count") != 1
        or not isinstance(job, dict)
        or set(job) != ACCEPTANCE_PROBE_JOB_FIELDS
        or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(job.get("job_id"))) is None
        or job.get("job_name") != inner["job_name"]
        or job.get("user") != inner["service_user"]
        or job.get("account") != inner["slurm_account"]
        or job.get("qos") != inner["slurm_qos"]
        or job.get("submit_host") != route["submit_host"]
        or job.get("controller") != route["controller"]
        or not isinstance(job.get("allocation_nodes"), list)
        or not job["allocation_nodes"]
        or len(job["allocation_nodes"]) != len(set(job["allocation_nodes"]))
        or job.get("time_limit_seconds") != 300
        or not isinstance(health, dict)
        or set(health) != {"control-plane", "gateway", "minio"}
        or any(
            not isinstance(health[name], dict)
            or set(health[name]) != ACCEPTANCE_PROBE_HEALTH_FIELDS
            or health[name].get("service") != name
            or health[name].get("status") != "healthy"
            or health[name].get("http_status") != 200
            or not _is_sha(
                health[name].get("candidate_binding_sha256"),
                length=64,
            )
            or not _is_sha(health[name].get("response_sha256"), length=64)
            for name in ("control-plane", "gateway", "minio")
        )
        or terminal
        != {
            "state": "COMPLETED",
            "exit_code": "0:0",
            "natural_exit": True,
            "cancel_requested": False,
            "timed_out": False,
        }
        or not _is_sha(result.get("job_output_sha256"), length=64)
        or not _is_sha(result.get("authority_receipt_sha256"), length=64)
        or _canonical_utc(result.get("completed_at")) is None
        or result.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
    ):
        raise NodeAuthorityError("acceptance probe readback is invalid")
    return dict(result)


def _runtime_retire_peer_digest(
    *,
    candidate_root: Path,
    runtime_root: Path,
    runtime_id: str,
) -> str:
    roots = (
        (Path("/etc/loom/developer-sandbox-links/clients"), runtime_id),
        (Path("/etc/loom/developer-sandbox-links/server"), runtime_id),
        (Path("/var/lib/loom-developer-domain-attestations"), runtime_id),
        (Path("/var/lib/loom-developer-sandbox-links/attestations"), runtime_id),
        (Path("/var/lib/loom-shared-capacity/runtime-attestations"), runtime_id),
        (candidate_root.parent, candidate_root.name),
        (runtime_root.parent, runtime_root.name),
    )
    rows: list[dict[str, object]] = []
    for root, excluded in roots:
        try:
            root_metadata = root.lstat()
        except FileNotFoundError:
            rows.append({"root": str(root), "present": False, "entries": []})
            continue
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement peer inventory failed safely") from exc
        if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
            raise NodeAuthorityError("runtime retirement peer root is unsafe")
        entries: list[dict[str, object]] = []
        try:
            for child in sorted(root.iterdir(), key=lambda path: path.name):
                if child.name == excluded:
                    continue
                metadata = child.lstat()
                entries.append(
                    {
                        "name": child.name,
                        "mode": stat.S_IFMT(metadata.st_mode),
                        "uid": metadata.st_uid,
                        "gid": metadata.st_gid,
                        "size": metadata.st_size,
                        "link": os.readlink(child) if stat.S_ISLNK(metadata.st_mode) else None,
                    },
                )
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement peer inventory failed safely") from exc
        rows.append({"root": str(root), "present": True, "entries": entries})
    return hashlib.sha256(_canonical({"roots": rows})).hexdigest()


def _runtime_retire_remove_tree(
    path: Path,
    *,
    allowed_uids: frozenset[int],
) -> None:
    current = path
    while True:
        try:
            ancestry = current.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement ancestry is unavailable") from exc
        else:
            if stat.S_ISLNK(ancestry.st_mode):
                if current == path:
                    break
                raise NodeAuthorityError("runtime retirement ancestry contains a symlink")
        if current.parent == current:
            break
        current = current.parent
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise NodeAuthorityError("runtime retirement target is unavailable") from exc
    if metadata.st_uid not in allowed_uids:
        raise NodeAuthorityError("runtime retirement target ownership drifted")
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        try:
            children = list(path.iterdir())
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement target is unreadable") from exc
        for child in children:
            _runtime_retire_remove_tree(child, allowed_uids=allowed_uids)
        try:
            path.rmdir()
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement directory removal failed safely") from exc
        return
    try:
        path.unlink()
    except OSError as exc:
        raise NodeAuthorityError("runtime retirement file removal failed safely") from exc


def _runtime_retire_remove_current(
    runtime_id: str,
    candidate_shas: frozenset[str],
) -> None:
    current = Path("/etc/loom/developer-sandbox-links/server") / runtime_id / "current"
    try:
        metadata = current.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise NodeAuthorityError("runtime retirement active link is unavailable") from exc
    if not stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != 0:
        raise NodeAuthorityError("runtime retirement active link is unsafe")
    target = os.readlink(current)
    match = re.fullmatch(r"candidates/([0-9a-f]{40})", target)
    if match is None or match.group(1) not in candidate_shas:
        raise NodeAuthorityError("runtime retirement active link is foreign")
    completed = subprocess.run(
        (
            "/usr/bin/systemctl",
            "stop",
            f"loom-developer-sandbox-link@{runtime_id}.service",
        ),
        env=_clean_env(),
        check=False,
        capture_output=True,
        timeout=60,
    )
    if completed.returncode != 0:
        raise NodeAuthorityError("runtime retirement link stop failed safely")
    try:
        current.unlink()
        descriptor = os.open(
            current.parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise NodeAuthorityError("runtime retirement active link removal failed safely") from exc


def _runtime_retire_paths(
    *,
    candidate_root: Path,
    runtime_root: Path,
    runtime_id: str,
    candidate_shas: frozenset[str],
) -> dict[str, tuple[Path, ...]]:
    clients = tuple(
        Path("/etc/loom/developer-sandbox-links/clients") / runtime_id / sha
        for sha in candidate_shas
    )
    servers = tuple(
        Path("/etc/loom/developer-sandbox-links/server") / runtime_id / "candidates" / sha
        for sha in candidate_shas
    )
    runtime = tuple(runtime_root / sha for sha in candidate_shas)
    candidates = tuple(candidate_root / sha for sha in candidate_shas)
    attestations = tuple(
        root / runtime_id / sha
        for root in (
            Path("/var/lib/loom-developer-domain-attestations"),
            Path("/var/lib/loom-developer-sandbox-links/attestations"),
            Path("/var/lib/loom-shared-capacity/runtime-attestations"),
        )
        for sha in candidate_shas
    )
    return {
        "clients": clients,
        "servers": servers,
        "runtime": runtime,
        "candidates": candidates,
        "attestations": attestations,
    }


def _runtime_retire_absence(
    *,
    candidate_root: Path,
    runtime_root: Path,
    runtime_id: str,
    candidate_shas: frozenset[str],
) -> dict[str, bool]:
    paths = _runtime_retire_paths(
        candidate_root=candidate_root,
        runtime_root=runtime_root,
        runtime_id=runtime_id,
        candidate_shas=candidate_shas,
    )

    def exact_absent(path: Path) -> bool:
        try:
            path.lstat()
        except FileNotFoundError:
            return True
        except OSError as exc:
            raise NodeAuthorityError("runtime retirement absence readback failed safely") from exc
        return False

    absent = {name: all(exact_absent(path) for path in values) for name, values in paths.items()}
    current = Path("/etc/loom/developer-sandbox-links/server") / runtime_id / "current"
    return {
        "link_client_credentials": absent["clients"],
        "tls_private_keys": absent["clients"] and absent["servers"],
        "token_files": absent["clients"],
        "domain_environment": absent["runtime"],
        "domain_config": absent["servers"],
        "candidate_material": absent["candidates"],
        "active_attestation_pointers": (exact_absent(current) and absent["attestations"]),
    }


def _execute_runtime_retire(request: Request) -> dict[str, Any]:
    payload = json.loads(request.payload_bytes)
    snapshot = _load_registry_snapshot()
    _validate_runtime_retire_request(request.payload_bytes, request.payload, snapshot)
    environments = [row for row in snapshot["environments"] if row["env_id"] == payload["env_id"]]
    if len(environments) != 1:
        raise NodeAuthorityError("runtime retirement environment is unavailable")
    environment = environments[0]
    candidate_root = Path(str(environment["candidate_root"]))
    runtime_root = Path(str(environment["runtime_root"]))
    expected_dynamic_candidate = Path("/shared_work/loom/candidates/environments") / str(
        payload["env_id"]
    )
    expected_dynamic_runtime = Path("/shared_work/loom/runtime/environments") / str(
        payload["env_id"]
    )
    expected_legacy_candidate = Path("/shared_work/loom/candidates/sandboxes") / str(
        payload["runtime_id"]
    )
    expected_legacy_runtime = Path("/shared_work/loom/runtime/sandboxes") / str(
        payload["runtime_id"]
    )
    if (candidate_root, runtime_root) not in {
        (expected_dynamic_candidate, expected_dynamic_runtime),
        (expected_legacy_candidate, expected_legacy_runtime),
    }:
        raise NodeAuthorityError("runtime retirement storage binding is invalid")
    candidate_shas = frozenset(str(row["candidate_sha"]) for row in payload["candidate_bindings"])
    tombstone_root = (
        RUNTIME_RETIRE_ROOT / "tombstones" / str(payload["node"]) / str(payload["runtime_id"])
    )
    tombstone_path = tombstone_root / f"{payload['retire_operation_sha256']}.json"
    for path, mode, parent_mode in (
        (RUNTIME_RETIRE_ROOT, 0o700, 0o755),
        (RUNTIME_RETIRE_ROOT / "tombstones", 0o700, 0o700),
        (RUNTIME_RETIRE_ROOT / "tombstones" / str(payload["node"]), 0o700, 0o700),
        (tombstone_root, 0o700, 0o700),
    ):
        _ensure_root_directory(path, mode=mode, parent_mode=parent_mode)
    existing: dict[str, Any] | None = None
    if tombstone_path.exists() or tombstone_path.is_symlink():
        try:
            existing = json.loads(
                _safe_root_file(tombstone_path, mode=0o600, limit=1 << 20),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("runtime retirement tombstone is invalid") from exc
    if existing is None:
        peer_before = _runtime_retire_peer_digest(
            candidate_root=candidate_root,
            runtime_root=runtime_root,
            runtime_id=str(payload["runtime_id"]),
        )
        _runtime_retire_remove_current(str(payload["runtime_id"]), candidate_shas)
        allowed_uids = frozenset({0, int(environment["uid"])})
        paths = _runtime_retire_paths(
            candidate_root=candidate_root,
            runtime_root=runtime_root,
            runtime_id=str(payload["runtime_id"]),
            candidate_shas=candidate_shas,
        )
        for target in (
            *paths["clients"],
            *paths["servers"],
            *paths["runtime"],
            *paths["candidates"],
            *paths["attestations"],
        ):
            _runtime_retire_remove_tree(target, allowed_uids=allowed_uids)
        absent = _runtime_retire_absence(
            candidate_root=candidate_root,
            runtime_root=runtime_root,
            runtime_id=str(payload["runtime_id"]),
            candidate_shas=candidate_shas,
        )
        peer_after = _runtime_retire_peer_digest(
            candidate_root=candidate_root,
            runtime_root=runtime_root,
            runtime_id=str(payload["runtime_id"]),
        )
        if (
            set(absent) != RUNTIME_RETIRE_ABSENCE_FIELDS
            or any(value is not True for value in absent.values())
            or peer_after != peer_before
        ):
            raise NodeAuthorityError("runtime retirement readback failed safely")
        tombstone_unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.runtime-retire-node-tombstone",
            "node": payload["node"],
            "domain": payload["domain"],
            "deployment_id": payload["deployment_id"],
            "env_id": payload["env_id"],
            "principal_id": payload["principal_id"],
            "runtime_id": payload["runtime_id"],
            "resource_generation": payload["resource_generation"],
            "registry_generation": payload["registry_generation"],
            "registry_snapshot_sha256": payload["registry_snapshot_sha256"],
            "retire_operation_sha256": payload["retire_operation_sha256"],
            "request_sha256": payload["payload_sha256"],
            "transport_request_id": request.request_id,
            "candidate_bindings": payload["candidate_bindings"],
            "absent": absent,
            "peer_digest_before": peer_before,
            "peer_digest_after": peer_after,
            "foreign_path_action": "preserve",
            "audit_action": "append-only-preserve",
            "completed_at": _timestamp(),
        }
        existing = {
            **tombstone_unsigned,
            "payload_sha256": hashlib.sha256(_canonical(tombstone_unsigned)).hexdigest(),
        }
        _atomic_install(
            tombstone_path,
            _canonical(existing),
            0o600,
            parent_mode=0o700,
        )
    tombstone_unsigned = {key: value for key, value in existing.items() if key != "payload_sha256"}
    if (
        existing.get("kind") != "loom.developer-environment.runtime-retire-node-tombstone"
        or existing.get("request_sha256") != payload["payload_sha256"]
        or existing.get("transport_request_id") != request.request_id
        or existing.get("candidate_bindings") != payload["candidate_bindings"]
        or existing.get("peer_digest_before") != existing.get("peer_digest_after")
        or existing.get("payload_sha256")
        != hashlib.sha256(_canonical(tombstone_unsigned)).hexdigest()
        or _safe_root_file(tombstone_path, mode=0o600, limit=1 << 20) != _canonical(existing)
    ):
        raise NodeAuthorityError("runtime retirement tombstone binding is invalid")
    rebound_absent = _runtime_retire_absence(
        candidate_root=candidate_root,
        runtime_root=runtime_root,
        runtime_id=str(payload["runtime_id"]),
        candidate_shas=candidate_shas,
    )
    if rebound_absent != existing.get("absent") or any(
        value is not True for value in rebound_absent.values()
    ):
        raise NodeAuthorityError("runtime retirement tombstone target reappeared")
    receipt_unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": RUNTIME_RETIRE_RECEIPT_KIND,
        "status": "cleaned",
        "action": RUNTIME_RETIRE_ACTION,
        **{
            field: payload[field]
            for field in (
                "node",
                "domain",
                "deployment_id",
                "env_id",
                "principal_id",
                "runtime_id",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "retire_operation_sha256",
            )
        },
        "request_sha256": payload["payload_sha256"],
        "transport_request_id": request.request_id,
        "candidate_bindings": payload["candidate_bindings"],
        "absent": existing["absent"],
        "tombstone": {
            "path": str(tombstone_path),
            "payload_sha256": existing["payload_sha256"],
            "persisted": True,
        },
        "peer_digest_before": existing["peer_digest_before"],
        "peer_digest_after": existing["peer_digest_after"],
        "foreign_path_action": "preserve",
        "audit_action": "append-only-preserve",
        "completed_at": existing["completed_at"],
    }
    return {
        **receipt_unsigned,
        "payload_sha256": hashlib.sha256(_canonical(receipt_unsigned)).hexdigest(),
    }


def _validated_slurm_identity_result(
    request: Request,
    result: Mapping[str, Any],
    *,
    operation: str,
) -> dict[str, Any]:
    identity = json.loads(request.payload_bytes)
    expected_fields = {
        "schema_version",
        "kind",
        "operation",
        "cluster",
        "env_id",
        "resource_generation",
        "service_user",
        "slurm_account",
        "slurm_qos",
        "status",
        "jobs",
        "state_sha256",
        "mutations",
        "completed_at",
    }
    if operation == "retire":
        expected_fields.add("tombstone")
    expected_cluster = "trt-oldlab" if request.payload["domain"] == "oldlab" else "trt-gb10"
    if (
        set(result) != expected_fields
        or result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != "loom.developer-environment.slurm-identity-result"
        or result.get("operation") != operation
        or result.get("cluster") != expected_cluster
        or result.get("env_id") != identity["env_id"]
        or result.get("resource_generation") != identity["resource_generation"]
        or result.get("service_user") != identity["service_user"]
        or result.get("slurm_account") != identity["slurm_account"]
        or result.get("slurm_qos") != identity["slurm_qos"]
        or result.get("status")
        not in (
            {"available", "exact-existing", "retired"}
            if operation == "check"
            else {"exact-existing"}
            if operation == "reconcile"
            else {"retired"}
        )
        or not isinstance(result.get("jobs"), list)
        or any(
            not isinstance(job, dict)
            or set(job) != {"job_id", "state", "account", "user"}
            or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(job.get("job_id"))) is None
            or re.fullmatch(r"[A-Z][A-Z0-9_+*~-]{1,63}", str(job.get("state"))) is None
            or job.get("account") != identity["slurm_account"]
            or job.get("user") != identity["service_user"]
            for job in result["jobs"]
        )
        or (operation == "retire" and result["jobs"])
        or not _is_sha(result.get("state_sha256"), length=64)
        or not isinstance(result.get("mutations"), list)
        or any(not isinstance(value, str) for value in result["mutations"])
        or not isinstance(result.get("completed_at"), str)
        or (
            operation == "retire"
            and (
                not isinstance(result.get("tombstone"), str)
                or result.get("tombstone")
                != (
                    "/var/lib/loom-developer-sandbox-slurm-policy/"
                    f"identity-tombstones/{expected_cluster}/{identity['env_id']}.json"
                )
            )
        )
    ):
        raise NodeAuthorityError("incremental Slurm identity readback is invalid")
    try:
        completed_at = datetime.fromisoformat(str(result["completed_at"]))
    except ValueError as exc:
        raise NodeAuthorityError("incremental Slurm identity time is invalid") from exc
    if completed_at.tzinfo is None:
        raise NodeAuthorityError("incremental Slurm identity time is invalid")
    return dict(result)


def _live_authority_argv(request: Request, command: str) -> tuple[str, ...]:
    if (
        command not in {"collect", "observe-slurm-job"}
        or (command == "collect" and request.action != "collect-live-overlap")
        or (command == "observe-slurm-job" and request.action != "observe-live-overlap-job")
    ):
        raise NodeAuthorityError("live overlap command binding is invalid")
    argv = [
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / LIVE_AUTHORITY_RELATIVE),
        command,
    ]
    if command == "collect":
        argv.extend(
            (
                "--sandbox",
                str(request.payload["sandbox"]),
                "--pool",
                str(request.payload["domain"]),
                "--candidate-sha",
                str(request.payload["candidate_sha"]),
                "--authority-tree",
                str(request.payload["candidate_tree"]),
            ),
        )
    return tuple(argv)


def _platform_health_authority_argv(
    request: Request,
    command: str,
) -> tuple[str, ...]:
    if command != "observe-node" or request.action != "observe-platform-health-node":
        raise NodeAuthorityError("platform-health command binding is invalid")
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / PLATFORM_HEALTH_AUTHORITY_RELATIVE),
        "observe-node",
    )


def _staging_pressure_authority_argv(request: Request) -> tuple[str, ...]:
    if request.action != "staging-pressure-reclaim-observe":
        raise NodeAuthorityError("staging pressure command binding is invalid")
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / STAGING_PRESSURE_AUTHORITY_RELATIVE),
        "observe-slurm",
    )


def _staging_allocation_probe_argv(request: Request) -> tuple[str, ...]:
    if request.action != "staging-allocation-probe":
        raise NodeAuthorityError("staging allocation probe command binding is invalid")
    payload = json.loads(request.payload_bytes)
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / HOST_AUTHORITY_RELATIVE),
        "staging-allocation-probe",
        "--candidate-sha",
        str(payload["candidate_sha"]),
        "--candidate-tree",
        str(payload["candidate_tree"]),
        "--request-id",
        str(payload["request_id"]),
        "--execute",
    )


def _staging_identity_converge_argv(request: Request) -> tuple[str, ...]:
    if request.action != "staging-allocation-bootstrap":
        raise NodeAuthorityError("staging identity command binding is invalid")
    payload = json.loads(request.payload_bytes)
    return (
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / HOST_AUTHORITY_RELATIVE),
        "staging-allocation-identity-converge",
        "--candidate-sha",
        str(payload["candidate_sha"]),
        "--candidate-tree",
        str(payload["candidate_tree"]),
        "--authority-generation",
        str(payload["generation"]),
        "--authority-convergence-id",
        str(payload["convergence_id"]),
        "--authority-request-id",
        str(payload["request_id"]),
        "--authority-requested-at",
        str(payload["requested_at"]),
        "--execute",
    )


def _staging_broker_argv(request: Request) -> tuple[str, ...]:
    if request.action not in {"staging-allocation-submit", "staging-allocation-cancel"}:
        raise NodeAuthorityError("staging broker command binding is invalid")
    payload = json.loads(request.payload_bytes)
    argv = [
        "/usr/bin/python3",
        "-I",
        str(SOURCE_ROOT / HOST_AUTHORITY_RELATIVE),
        request.action,
        "--candidate-sha",
        str(payload["candidate_sha"]),
        "--candidate-tree",
        str(payload["candidate_tree"]),
        "--request-id",
        str(payload["request_id"]),
        "--requested-node",
        str(payload["requested_node"]),
    ]
    if request.action == "staging-allocation-cancel":
        argv.extend(
            (
                "--submit-request-id",
                str(payload["submit_request_id"]),
                "--job-id",
                str(payload["job_id"]),
            ),
        )
    argv.append("--execute")
    return tuple(argv)


def _validate_slurm_candidate(request: Request, policy: AuthorityPolicy) -> None:
    candidate_set = json.loads(request.payload_bytes)
    bindings = candidate_set["candidate_bindings"]
    cohort = _registry_cohort(_load_registry_snapshot(), include_provisioning=True)
    surface = (
        "scripts/ops/developer_sandbox_slurm_policy.py",
        "scripts/ops/slurm_job_cgroup_guard.py",
        f"deploy/slurm/developer-sandboxes/{SLURM_PROFILE_NAME[str(request.payload['domain'])]}",
        "deploy/slurm/loom-slurm-job-cgroup-guard.service",
    )
    surface_identities: list[tuple[str, ...]] = []
    for sandbox, registry_binding in sorted(cohort.items()):
        binding = bindings[registry_binding[0]["slurm_account"]]
        result = _run_fixed(
            _domain_argv(
                request,
                "inspect-candidate",
                "--domain",
                str(request.payload["domain"]),
                "--sandbox",
                sandbox,
                "--candidate-sha",
                str(binding["candidate_sha"]),
                "--candidate-tree",
                str(binding["candidate_tree"]),
            ),
        )
        if (
            result.get("operation") != "inspect-candidate"
            or result.get("domain") != request.payload["domain"]
            or result.get("sandbox") != sandbox
            or result.get("candidate_sha") != binding["candidate_sha"]
            or result.get("candidate_tree") != binding["candidate_tree"]
            or result.get("candidate_clean") is not True
        ):
            raise NodeAuthorityError("Slurm candidate readback binding is invalid")
        candidate_root = Path(registry_binding[0]["candidate_root"]) / str(
            binding["candidate_sha"],
        )
        identities: list[str] = []
        for relative in surface:
            git_prefix = (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "-c",
                f"safe.directory={candidate_root}",
                "-C",
                str(candidate_root),
            )
            object_name = f"{binding['candidate_sha']}:{relative}"
            try:
                sized = subprocess.run(
                    (*git_prefix, "cat-file", "-s", object_name),
                    env=_clean_env(),
                    check=False,
                    capture_output=True,
                    timeout=30,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise NodeAuthorityError(
                    "Slurm candidate policy surface is unavailable",
                ) from exc
            try:
                object_size = int(sized.stdout.decode("ascii").strip())
            except (UnicodeDecodeError, ValueError) as exc:
                raise NodeAuthorityError(
                    "Slurm candidate policy surface size is invalid",
                ) from exc
            if (
                sized.returncode != 0
                or sized.stderr
                or not 0 <= object_size <= MAX_SLURM_POLICY_SURFACE_BYTES
            ):
                raise NodeAuthorityError("Slurm candidate policy surface size is invalid")
            try:
                completed = subprocess.run(
                    (*git_prefix, "show", object_name),
                    env=_clean_env(),
                    check=False,
                    capture_output=True,
                    timeout=30,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise NodeAuthorityError(
                    "Slurm candidate policy surface is unavailable",
                ) from exc
            if (
                completed.returncode != 0
                or completed.stderr
                or len(completed.stdout) != object_size
            ):
                raise NodeAuthorityError("Slurm candidate policy surface is unavailable")
            identity = hashlib.sha256(completed.stdout).hexdigest()
            expected_identity = policy.asset_sha256.get(relative)
            if identity != expected_identity:
                raise NodeAuthorityError(
                    "Slurm candidate policy surface differs from installed authority",
                )
            identities.append(identity)
        surface_identities.append(tuple(identities))
    if len(set(surface_identities)) != 1:
        raise NodeAuthorityError("Slurm candidate policy surfaces are incompatible")


def _validated_slurm_snapshot_path(value: object) -> Path:
    if not isinstance(value, str):
        raise NodeAuthorityError("Slurm policy snapshot path is invalid")
    snapshot = Path(value)
    try:
        snapshot.relative_to(SLURM_SNAPSHOT_ROOT)
    except ValueError as exc:
        raise NodeAuthorityError("Slurm policy snapshot path is invalid") from exc
    if snapshot.parent != SLURM_SNAPSHOT_ROOT:
        raise NodeAuthorityError("Slurm policy snapshot path is invalid")
    _safe_root_directory(SLURM_STATE_ROOT, mode=0o700)
    _safe_root_directory(SLURM_TRANSACTION_ROOT, mode=0o700)
    _safe_root_directory(SLURM_SNAPSHOT_ROOT, mode=0o700)
    _safe_root_directory(snapshot, mode=0o700)
    return snapshot


def _slurm_snapshot_manifest_bytes(snapshot: Path) -> bytes:
    raw = _safe_root_file(
        snapshot / "manifest.json",
        mode=0o600,
        limit=1 << 20,
    )
    try:
        manifest = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("Slurm policy snapshot manifest is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema_version", "files"}
        or manifest.get("schema_version") != SCHEMA_VERSION
        or not isinstance(manifest.get("files"), list)
        or raw
        != (json.dumps(manifest, sort_keys=True, ensure_ascii=True) + "\n").encode(
            "ascii",
        )
    ):
        raise NodeAuthorityError("Slurm policy snapshot manifest is invalid")
    rows = manifest["files"]
    if (
        len(rows) != len(SLURM_SNAPSHOT_RELATIVE_PATHS)
        or tuple(row.get("path") if isinstance(row, dict) else None for row in rows)
        != SLURM_SNAPSHOT_RELATIVE_PATHS
    ):
        raise NodeAuthorityError("Slurm policy snapshot inventory is invalid")
    for row, relative_name in zip(
        rows,
        SLURM_SNAPSHOT_RELATIVE_PATHS,
        strict=True,
    ):
        if (
            not isinstance(row, dict)
            or set(row) != SLURM_SNAPSHOT_ROW_FIELDS
            or row.get("path") != relative_name
            or type(row.get("present")) is not bool
        ):
            raise NodeAuthorityError("Slurm policy snapshot row is invalid")
        archive = snapshot / relative_name
        if row["present"] is True:
            mode = row.get("mode")
            uid = row.get("uid")
            gid = row.get("gid")
            nlink = row.get("nlink")
            size = row.get("size")
            digest = row.get("sha256")
            if (
                type(mode) is not int
                or type(uid) is not int
                or type(gid) is not int
                or type(nlink) is not int
                or type(size) is not int
                or not 0 <= mode <= 0o7777
                or mode & 0o022
                or uid != 0
                or gid != 0
                or nlink != 1
                or not 0 <= size <= MAX_REQUEST_BYTES
                or not _is_sha(digest, length=64)
            ):
                raise NodeAuthorityError("Slurm policy snapshot row is invalid")
            content = _safe_root_file(
                archive,
                mode=0o600,
                limit=MAX_REQUEST_BYTES,
            )
            if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
                raise NodeAuthorityError(
                    "Slurm policy snapshot archive identity drifted",
                )
        elif any(
            row.get(field) is not None
            for field in ("mode", "uid", "gid", "nlink", "size", "sha256")
        ):
            raise NodeAuthorityError("Slurm policy snapshot absent row is invalid")
        else:
            try:
                archive.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise NodeAuthorityError(
                    "Slurm policy snapshot absent archive is unavailable",
                ) from exc
            raise NodeAuthorityError("Slurm policy snapshot absent archive exists")
    return raw


def _validate_slurm_snapshot_archive_inventory(
    snapshot: Path,
    manifest_bytes: bytes,
    *,
    journal: Mapping[str, Any],
    cluster: str,
    require_accounting: bool,
) -> str:
    manifest = json.loads(manifest_bytes)
    present_paths = {
        str(row["path"])
        for row in manifest["files"]
        if isinstance(row, dict) and row.get("present") is True
    }
    accounting = snapshot / "accounting-cas.json"
    expected_accounting = str(accounting) if require_accounting else None
    if (
        journal.get("apply_accounting") is not require_accounting
        or journal.get("accounting_snapshot") != expected_accounting
    ):
        raise NodeAuthorityError("Slurm policy accounting snapshot binding is invalid")
    accounting_sha256: str | None = None
    if require_accounting:
        accounting_bytes = _safe_root_file(
            accounting,
            mode=0o600,
            limit=1 << 20,
        )
        try:
            accounting_payload = json.loads(accounting_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NodeAuthorityError("Slurm policy accounting snapshot is invalid") from exc
        if (
            not isinstance(accounting_payload, dict)
            or set(accounting_payload)
            != {
                "schema_version",
                "cluster",
                "before",
                "desired",
            }
            or accounting_payload.get("schema_version") != SCHEMA_VERSION
            or accounting_payload.get("cluster") != cluster
            or not isinstance(accounting_payload.get("before"), dict)
            or not isinstance(accounting_payload.get("desired"), dict)
            or accounting_bytes != _canonical(accounting_payload)
        ):
            raise NodeAuthorityError("Slurm policy accounting snapshot is invalid")
        accounting_sha256 = hashlib.sha256(accounting_bytes).hexdigest()
    else:
        try:
            accounting.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise NodeAuthorityError(
                "Slurm policy accounting snapshot is unavailable",
            ) from exc
        else:
            raise NodeAuthorityError("unexpected Slurm policy accounting snapshot exists")

    expected_files = {"manifest.json", *present_paths}
    if require_accounting:
        expected_files.add("accounting-cas.json")
    actual_files: set[str] = set()
    try:
        for directory, directory_names, file_names in os.walk(
            snapshot,
            followlinks=False,
        ):
            directory_path = Path(directory)
            _safe_root_directory(directory_path, mode=0o700)
            for name in directory_names:
                _safe_root_directory(directory_path / name, mode=0o700)
            actual_files.update(
                (directory_path / name).relative_to(snapshot).as_posix() for name in file_names
            )
    except OSError as exc:
        raise NodeAuthorityError("Slurm policy snapshot inventory is unavailable") from exc
    if actual_files != expected_files:
        raise NodeAuthorityError("Slurm policy snapshot archive set is not closed")
    return hashlib.sha256(
        _canonical(
            {
                "schema_version": SCHEMA_VERSION,
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "accounting_sha256": accounting_sha256,
            },
        ),
    ).hexdigest()


def _slurm_policy_journal_payload(
    request: Request,
    raw: bytes,
    *,
    operation: str,
    snapshot: Path,
    require_accounting: bool,
    rollback_target: Path | None = None,
    transaction_id: str | None = None,
    expected_phase: str = "committed",
) -> dict[str, Any]:
    if expected_phase not in {"committed", "rolled_back"} or (
        expected_phase == "rolled_back" and operation != "rollback"
    ):
        raise NodeAuthorityError("Slurm policy journal phase binding is invalid")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("Slurm policy state is invalid") from exc
    expected_fields = set(SLURM_POLICY_JOURNAL_COMMON_FIELDS)
    if operation == "rollback":
        expected_fields.add("rollback_target")
    domain = str(request.payload["domain"])
    node = str(request.payload["node"])
    cluster = SLURM_CLUSTER[domain]
    try:
        candidate_set = json.loads(request.payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("Slurm candidate-set payload is invalid") from exc
    expected_accounting = str(snapshot / "accounting-cas.json") if require_accounting else None
    if (
        not isinstance(payload, dict)
        or set(payload) != expected_fields
        or raw != _canonical(payload)
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("operation") != operation
        or payload.get("cluster") != cluster
        or payload.get("host") != NODE_HOSTNAMES[node]
        or payload.get("slurm_node") != node
        or payload.get("candidate_sha") != request.payload["candidate_sha"]
        or payload.get("candidate_set_sha256") != candidate_set.get("candidate_set_sha256")
        or payload.get("candidate_bindings") != candidate_set.get("candidate_bindings")
        or payload.get("transaction_id") != (transaction_id or request.request_id)
        or payload.get("candidate_set_generation") != candidate_set.get("generation")
        or payload.get("candidate_set_convergence_id") != candidate_set.get("convergence_id")
        or payload.get("candidate_set_payload_sha256") != request.payload["payload_sha256"]
        or payload.get("snapshot") != str(snapshot)
        or payload.get("accounting_snapshot") != expected_accounting
        or payload.get("restart") is not True
        or payload.get("apply_accounting") is not require_accounting
        or payload.get("phase") != expected_phase
        or not isinstance(payload.get("created_at"), str)
        or not isinstance(payload.get("updated_at"), str)
    ):
        raise NodeAuthorityError("Slurm policy journal/snapshot binding is invalid")
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
        updated_at = datetime.fromisoformat(payload["updated_at"])
    except ValueError as exc:
        raise NodeAuthorityError("Slurm policy journal timestamp is invalid") from exc
    if created_at.tzinfo is None or updated_at.tzinfo is None or updated_at < created_at:
        raise NodeAuthorityError("Slurm policy journal timestamp is invalid")
    if operation == "apply":
        if rollback_target is not None:
            raise NodeAuthorityError("Slurm policy apply rollback binding is invalid")
    else:
        target = _validated_slurm_snapshot_path(payload.get("rollback_target"))
        if rollback_target is None or target != rollback_target:
            raise NodeAuthorityError("Slurm policy rollback target binding is invalid")
    return payload


def _slurm_policy_binding(
    request: Request,
    result: Mapping[str, Any],
    *,
    snapshot_field: str,
) -> str:
    domain = str(request.payload["domain"])
    cluster = SLURM_CLUSTER[domain]
    journal = SLURM_TRANSACTION_ROOT / f"{cluster}.json"
    if (
        result.get("cluster") != cluster
        or result.get("phase") != "committed"
        or result.get("journal") != str(journal)
    ):
        raise NodeAuthorityError("Slurm policy result binding is invalid")
    snapshot_path = _validated_slurm_snapshot_path(result.get(snapshot_field))
    journal_bytes = _safe_root_file(journal, mode=0o600, limit=1 << 20)
    manifest_bytes = _slurm_snapshot_manifest_bytes(snapshot_path)
    expected_operation = "rollback" if request.action == "slurm-rollback" else "apply"
    require_accounting = request.payload["node"] == SLURM_CONTROLLER[domain]
    rollback_target = None
    if expected_operation == "rollback":
        rollback_target = _validated_slurm_snapshot_path(result.get("restored_snapshot"))
    journal_payload = _slurm_policy_journal_payload(
        request,
        journal_bytes,
        operation=expected_operation,
        snapshot=snapshot_path,
        require_accounting=require_accounting,
        rollback_target=rollback_target,
    )
    archive_identity = _validate_slurm_snapshot_archive_inventory(
        snapshot_path,
        manifest_bytes,
        journal=journal_payload,
        cluster=cluster,
        require_accounting=require_accounting,
    )
    return (
        f"slurm-policy-v1:{cluster}:{hashlib.sha256(journal_bytes).hexdigest()}:{archive_identity}"
    )


def _validate_prior_slurm_binding(request: Request, prior: Mapping[str, Any]) -> None:
    binding = prior.get("inner_receipt")
    match = SLURM_BINDING_RE.fullmatch(binding) if isinstance(binding, str) else None
    domain = str(request.payload["domain"])
    if match is None or match.group(1) != SLURM_CLUSTER[domain]:
        raise NodeAuthorityError("Slurm rollback receipt binding is invalid")
    journal = SLURM_TRANSACTION_ROOT / f"{SLURM_CLUSTER[domain]}.json"
    journal_bytes = _safe_root_file(journal, mode=0o600, limit=1 << 20)
    try:
        untrusted = json.loads(journal_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("Slurm rollback journal is invalid") from exc
    require_accounting = request.payload["node"] == SLURM_CONTROLLER[domain]
    if (
        isinstance(untrusted, dict)
        and untrusted.get("operation") == "rollback"
        and untrusted.get("phase") in {"committed", "rolled_back"}
        and untrusted.get("transaction_id") == request.request_id
    ):
        recovery_snapshot = _validated_slurm_snapshot_path(untrusted.get("snapshot"))
        restored_snapshot = _validated_slurm_snapshot_path(untrusted.get("rollback_target"))
        _slurm_policy_journal_payload(
            request,
            journal_bytes,
            operation="rollback",
            snapshot=recovery_snapshot,
            require_accounting=require_accounting,
            rollback_target=restored_snapshot,
            expected_phase=str(untrusted["phase"]),
        )
        manifest_bytes = _slurm_snapshot_manifest_bytes(restored_snapshot)
        archive_identity = _validate_slurm_snapshot_archive_inventory(
            restored_snapshot,
            manifest_bytes,
            journal={
                "apply_accounting": require_accounting,
                "accounting_snapshot": (
                    str(restored_snapshot / "accounting-cas.json") if require_accounting else None
                ),
            },
            cluster=SLURM_CLUSTER[domain],
            require_accounting=require_accounting,
        )
        if archive_identity != match.group(3):
            raise NodeAuthorityError("Slurm rollback restored snapshot identity drifted")
        return
    if hashlib.sha256(journal_bytes).hexdigest() != match.group(2):
        raise NodeAuthorityError("Slurm rollback journal identity advanced")
    snapshot_raw = untrusted.get("snapshot") if isinstance(untrusted, dict) else None
    snapshot_path = _validated_slurm_snapshot_path(snapshot_raw)
    journal_payload = _slurm_policy_journal_payload(
        request,
        journal_bytes,
        operation="apply",
        snapshot=snapshot_path,
        require_accounting=require_accounting,
        transaction_id=str(prior["request_id"]),
    )
    manifest_bytes = _slurm_snapshot_manifest_bytes(snapshot_path)
    archive_identity = _validate_slurm_snapshot_archive_inventory(
        snapshot_path,
        manifest_bytes,
        journal=journal_payload,
        cluster=SLURM_CLUSTER[domain],
        require_accounting=require_accounting,
    )
    if archive_identity != match.group(3):
        raise NodeAuthorityError("Slurm rollback snapshot identity drifted")


def _run_staging_identity_command(argv: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            tuple(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env=_clean_env(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise NodeAuthorityError(
            "staging allocation identity command failed safely",
        ) from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout.encode()) > 64 * 1024:
        raise NodeAuthorityError("staging allocation identity command failed safely")
    return completed.stdout


def _replace_staging_system_file(source: Path, target: Path) -> None:
    raw = _safe_root_file(source, mode=0o644, limit=64 * 1024)
    try:
        existing = target.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise NodeAuthorityError("staging system file is unavailable") from exc
    if existing is not None:
        if (
            not stat.S_ISREG(existing.st_mode)
            or stat.S_ISLNK(existing.st_mode)
            or existing.st_uid != 0
            or existing.st_gid != 0
            or existing.st_nlink != 1
            or stat.S_IMODE(existing.st_mode) != 0o644
        ):
            raise NodeAuthorityError("staging system file metadata drifted")
        if _safe_root_file(target, mode=0o644, limit=64 * 1024) == raw:
            return
    temporary = target.parent / f".{target.name}.{os.getpid()}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
            0o600,
        )
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o644)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise NodeAuthorityError("staging system file write failed safely")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if _safe_root_file(target, mode=0o644, limit=64 * 1024) != raw:
            raise NodeAuthorityError("staging system file readback drifted")
    except NodeAuthorityError:
        raise
    except OSError as exc:
        raise NodeAuthorityError("staging system file convergence failed safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _staging_mount_readback() -> dict[str, Any]:
    _replace_staging_system_file(
        SOURCE_ROOT / STAGING_MOUNT_SOURCE_RELATIVE,
        STAGING_MOUNT_UNIT_PATH,
    )
    _replace_staging_system_file(
        SOURCE_ROOT / STAGING_TMPFILES_SOURCE_RELATIVE,
        STAGING_TMPFILES_PATH,
    )
    _run_staging_identity_command(
        ("/usr/bin/systemd-tmpfiles", "--create", str(STAGING_TMPFILES_PATH))
    )
    _run_staging_identity_command(("/usr/bin/systemctl", "daemon-reload"))
    _run_staging_identity_command(
        ("/usr/bin/systemctl", "enable", "--now", "--quiet", STAGING_MOUNT_UNIT),
    )
    active = _run_staging_identity_command(
        ("/usr/bin/systemctl", "is-active", STAGING_MOUNT_UNIT),
    ).strip()
    mount = _run_staging_identity_command(
        (
            "/usr/bin/findmnt",
            "-n",
            "-o",
            "SOURCE,FSTYPE,TARGET",
            "-T",
            str(STAGING_SHARED_ROOT),
        ),
    ).split()
    if active != "active" or mount != [
        "192.168.20.12:/shared_work2/loom/staging",
        "nfs4",
        str(STAGING_SHARED_ROOT),
    ]:
        raise NodeAuthorityError("staging shared mount readback drifted")
    metadata = STAGING_SHARED_ROOT.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise NodeAuthorityError("staging shared mount target is unsafe")
    return {
        "unit": STAGING_MOUNT_UNIT,
        "source": mount[0],
        "filesystem_type": mount[1],
        "target": mount[2],
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "active": True,
    }


def _staging_service_identity() -> tuple[pwd.struct_passwd, tuple[str, ...]]:
    try:
        service_group = grp.getgrnam(STAGING_SERVICE_GROUP)
    except KeyError:
        _run_staging_identity_command(
            (
                "/usr/sbin/groupadd",
                "--gid",
                str(STAGING_SERVICE_GID),
                STAGING_SERVICE_GROUP,
            ),
        )
        service_group = grp.getgrnam(STAGING_SERVICE_GROUP)
    if service_group.gr_gid != STAGING_SERVICE_GID:
        raise NodeAuthorityError("staging allocation service group identity drifted")
    try:
        account = pwd.getpwnam(STAGING_SERVICE_USER)
    except KeyError:
        _run_staging_identity_command(
            (
                "/usr/sbin/useradd",
                "--system",
                "--uid",
                str(STAGING_SERVICE_UID),
                "--gid",
                str(STAGING_SERVICE_GID),
                "--home-dir",
                str(STAGING_SERVICE_HOME),
                "--shell",
                STAGING_SERVICE_SHELL,
                "--no-create-home",
                STAGING_SERVICE_USER,
            ),
        )
        account = pwd.getpwnam(STAGING_SERVICE_USER)
    if (
        account.pw_uid != STAGING_SERVICE_UID
        or account.pw_gid != STAGING_SERVICE_GID
        or account.pw_dir != str(STAGING_SERVICE_HOME)
        or account.pw_shell != STAGING_SERVICE_SHELL
    ):
        raise NodeAuthorityError("staging allocation service user identity drifted")
    for name in STAGING_SUPPLEMENTARY_GROUPS:
        try:
            grp.getgrnam(name)
        except KeyError as exc:
            raise NodeAuthorityError(
                "staging allocation supplementary group is unavailable",
            ) from exc
    groups = {
        item.gr_name
        for item in grp.getgrall()
        if item.gr_gid == account.pw_gid or STAGING_SERVICE_USER in item.gr_mem
    }
    if not set(STAGING_SUPPLEMENTARY_GROUPS).issubset(groups):
        _run_staging_identity_command(
            (
                "/usr/sbin/usermod",
                "--append",
                "--groups",
                ",".join(STAGING_SUPPLEMENTARY_GROUPS),
                STAGING_SERVICE_USER,
            ),
        )
        groups = {
            item.gr_name
            for item in grp.getgrall()
            if item.gr_gid == account.pw_gid or STAGING_SERVICE_USER in item.gr_mem
        }
    if not set(STAGING_SUPPLEMENTARY_GROUPS).issubset(groups):
        raise NodeAuthorityError("staging allocation supplementary groups did not converge")
    return account, tuple(sorted(STAGING_SUPPLEMENTARY_GROUPS))


def _converge_service_directory(
    path: Path,
    *,
    managed_root: Path,
    mode: int,
) -> dict[str, Any]:
    if (
        not path.is_absolute()
        or managed_root not in {path, *path.parents}
        or any(part in {"", ".", ".."} for part in path.parts[1:])
    ):
        raise NodeAuthorityError("staging allocation service path is invalid")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    current = Path("/")
    try:
        for part in path.parts[1:]:
            try:
                child = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                    dir_fd=descriptor,
                )
            except FileNotFoundError:
                os.mkdir(part, mode=mode, dir_fd=descriptor)
                child = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                    dir_fd=descriptor,
                )
            os.close(descriptor)
            descriptor = child
            current /= part
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise NodeAuthorityError("staging allocation service path is unsafe")
            if current == managed_root or managed_root in current.parents:
                os.fchown(descriptor, STAGING_SERVICE_UID, STAGING_SERVICE_GID)
                os.fchmod(descriptor, mode)
        metadata = os.fstat(descriptor)
        if (metadata.st_uid, metadata.st_gid) != (
            STAGING_SERVICE_UID,
            STAGING_SERVICE_GID,
        ) or stat.S_IMODE(metadata.st_mode) != mode:
            raise NodeAuthorityError("staging allocation service path did not converge")
        return {
            "path": str(path),
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
            "mode": stat.S_IMODE(metadata.st_mode),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
        }
    except NodeAuthorityError:
        raise
    except OSError as exc:
        raise NodeAuthorityError(
            "staging allocation service path convergence failed safely",
        ) from exc
    finally:
        os.close(descriptor)


def _staging_accounting_readback() -> None:
    output = _run_staging_identity_command(
        (
            "/usr/bin/sacctmgr",
            "--noheader",
            "--parsable2",
            "show",
            "association",
            "where",
            "cluster=trt-gb10",
            "account=loom-staging",
            f"user={STAGING_SERVICE_USER}",
            "format=Cluster,Account,User,QOS",
        ),
    )
    rows = [line.split("|") for line in output.splitlines() if line.strip()]
    if (
        len(rows) != 1
        or len(rows[0]) != 5
        or rows[0][-1] != ""
        or rows[0][:3] != ["trt-gb10", "loom-staging", STAGING_SERVICE_USER]
        or "loom-staging" not in rows[0][3].split(",")
    ):
        raise NodeAuthorityError("staging allocation accounting association is unavailable")


def _staging_boot_id() -> str:
    try:
        descriptor = os.open(
            STAGING_BOOT_ID_PATH,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
    except OSError as exc:
        raise NodeAuthorityError("staging node boot identity is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        raw = os.read(descriptor, 129)
        after = os.fstat(descriptor)
    except OSError as exc:
        raise NodeAuthorityError("staging node boot identity cannot be read") from exc
    finally:
        os.close(descriptor)
    try:
        value = raw.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise NodeAuthorityError("staging node boot identity is invalid") from exc
    if (
        len(raw) > 128
        or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        or STAGING_BOOT_ID_RE.fullmatch(value) is None
    ):
        raise NodeAuthorityError("staging node boot identity is invalid")
    return value


def _staging_allocation_bootstrap(request: Request) -> dict[str, Any]:
    if (
        request.action != "staging-allocation-bootstrap"
        or request.payload["domain"] != "gb10"
        or request.payload["node"] not in STAGING_INFRASTRUCTURE_NODES
    ):
        raise NodeAuthorityError("staging allocation bootstrap request is invalid")
    mount = _staging_mount_readback()
    account, supplementary = _staging_service_identity()
    _staging_accounting_readback()
    operation = json.loads(request.payload_bytes)
    converged = _run_fixed(_staging_identity_converge_argv(request))
    expected_identity = {
        "username": STAGING_SERVICE_USER,
        "group": STAGING_SERVICE_GROUP,
        "uid": STAGING_SERVICE_UID,
        "gid": STAGING_SERVICE_GID,
        "home": str(STAGING_SERVICE_HOME),
        "shell": STAGING_SERVICE_SHELL,
        "supplementary_groups": list(STAGING_SUPPLEMENTARY_GROUPS),
    }
    if (
        set(converged)
        != {
            "schema_version",
            "kind",
            "node",
            "canonical_host",
            "service_identity",
            "namespace",
            "guard_binding",
            "result",
        }
        or converged.get("schema_version") != SCHEMA_VERSION
        or converged.get("kind") != "staging_external_slurm_identity_bootstrap"
        or converged.get("node") != request.payload["node"]
        or converged.get("canonical_host") != NODE_HOSTNAMES[str(request.payload["node"])]
        or converged.get("service_identity") != expected_identity
        or converged.get("namespace")
        != {
            "root": str(STAGING_SHARED_ROOT),
            "mount_source": mount["source"],
            "mount_fstype": mount["filesystem_type"],
            "mount_device": mount["device"],
            "mount_inode": mount["inode"],
            "repository_root": str(STAGING_SHARED_PATHS[0]),
            "worker_env_root": str(STAGING_SHARED_PATHS[1]),
            "result_root": str(STAGING_SHARED_PATHS[2]),
            "service_uid": STAGING_SERVICE_UID,
            "service_gid": STAGING_SERVICE_GID,
            "root_mode": "0o750",
            "repository_root_mode": "0o750",
            "worker_env_root_mode": "0o750",
            "result_root_mode": "0o2770",
        }
        or not isinstance(converged.get("guard_binding"), dict)
        or converged["guard_binding"].get("kind")
        != "loom.staging-external-slurm.guard-binding-convergence"
        or converged["guard_binding"].get("cluster") != "trt-gb10"
        or converged["guard_binding"].get("candidate_sha") != request.payload["candidate_sha"]
        or converged["guard_binding"].get("candidate_tree") != request.payload["candidate_tree"]
        or converged["guard_binding"].get("authority_generation") != operation["generation"]
        or converged["guard_binding"].get("authority_convergence_id") != operation["convergence_id"]
        or converged["guard_binding"].get("authority_request_id") != operation["request_id"]
        or not _is_sha(
            converged["guard_binding"].get("candidate_set_sha256"),
            length=64,
        )
        or not _is_sha(
            converged["guard_binding"].get("binding_payload_sha256"),
            length=64,
        )
        or converged["guard_binding"].get("policy_phase") != "committed"
        or converged["guard_binding"].get("status") != "committed"
        or converged.get("result") != "pass"
    ):
        raise NodeAuthorityError("staging allocation bootstrap readback is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-external-slurm.node-bootstrap",
        "node": request.payload["node"],
        "canonical_host": NODE_HOSTNAMES[str(request.payload["node"])],
        "boot_id": _staging_boot_id(),
        "user": STAGING_SERVICE_USER,
        "uid": account.pw_uid,
        "gid": account.pw_gid,
        "home": str(STAGING_SERVICE_HOME),
        "shell": STAGING_SERVICE_SHELL,
        "supplementary_groups": list(supplementary),
        "account": "loom-staging",
        "qos": "loom-staging",
        "repository_root": str(STAGING_SHARED_PATHS[0]),
        "env_root": str(STAGING_SHARED_PATHS[1]),
        "result_root": str(STAGING_SHARED_PATHS[2]),
        "mount": mount,
        "mount_digest": hashlib.sha256(_canonical(mount)).hexdigest(),
        "path_readback": converged["namespace"],
        "guard_binding": converged["guard_binding"],
        "status": "converged",
    }


def _staging_shared_source_bootstrap(request: Request) -> dict[str, Any]:
    if (
        request.action != "staging-shared-source-bootstrap"
        or request.payload["node"] != "trt-gb10-2"
        or request.payload["domain"] != "gb10"
    ):
        raise NodeAuthorityError("staging shared source bootstrap request is invalid")
    parent = STAGING_RAW_SOURCE_ROOT.parent
    try:
        parent_metadata = parent.lstat()
    except OSError as exc:
        raise NodeAuthorityError("staging shared source parent is unavailable") from exc
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or parent_metadata.st_uid != 0
        or parent_metadata.st_mode & 0o002
    ):
        raise NodeAuthorityError("staging shared source parent is unsafe")
    readbacks: list[dict[str, Any]] = []
    for path, uid, gid, mode in (
        (STAGING_RAW_SOURCE_ROOT, 0, STAGING_SERVICE_GID, 0o750),
        (STAGING_RAW_SOURCE_ROOT / "candidates", 0, STAGING_SERVICE_GID, 0o750),
        (STAGING_RAW_SOURCE_ROOT / "generated", 0, STAGING_SERVICE_GID, 0o750),
        (
            STAGING_RAW_SOURCE_ROOT / "results",
            STAGING_SERVICE_UID,
            STAGING_SERVICE_GID,
            0o2770,
        ),
    ):
        try:
            path.mkdir(mode=mode)
        except FileExistsError:
            pass
        except OSError as exc:
            raise NodeAuthorityError("staging shared source convergence failed safely") from exc
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise NodeAuthorityError("staging shared source path is unsafe")
        if (metadata.st_uid, metadata.st_gid) != (uid, gid) or stat.S_IMODE(
            metadata.st_mode,
        ) != mode:
            try:
                os.chown(path, uid, gid, follow_symlinks=False)
                os.chmod(path, mode, follow_symlinks=False)
            except OSError as exc:
                raise NodeAuthorityError(
                    "staging shared source metadata convergence failed safely",
                ) from exc
            metadata = path.lstat()
        if (
            (metadata.st_uid, metadata.st_gid) != (uid, gid)
            or stat.S_IMODE(metadata.st_mode) != mode
            or stat.S_ISLNK(metadata.st_mode)
        ):
            raise NodeAuthorityError("staging shared source metadata drifted")
        readbacks.append(
            {
                "path": str(path),
                "uid": uid,
                "gid": gid,
                "mode": mode,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
            },
        )
    result = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-shared-source-bootstrap",
        "node": "trt-gb10-2",
        "canonical_host": NODE_HOSTNAMES["trt-gb10-2"],
        "paths": readbacks,
        "status": "converged",
    }
    return {
        **result,
        "source_digest": hashlib.sha256(_canonical(result)).hexdigest(),
    }


def _staging_broker_submission_path(submit_request_id: str) -> Path:
    if not _is_sha(submit_request_id, length=64):
        raise NodeAuthorityError("staging broker submission identity is invalid")
    return STAGING_BROKER_ROOT / f"{submit_request_id}.json"


def _staging_guard_binding(
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    raw = _safe_root_file(STAGING_GUARD_BINDING_PATH, mode=0o600, limit=64 * 1024)
    try:
        binding = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("staging guard binding is invalid") from exc
    fields = {
        "schema_version",
        "kind",
        "cluster",
        "account",
        "service_user",
        "slurm_qos",
        "runtime_id",
        "env_id",
        "resource_generation",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "authority_generation",
        "authority_convergence_id",
        "authority_request_id",
        "authority_requested_at",
        "payload_sha256",
    }
    unsigned = (
        {key: value for key, value in binding.items() if key != "payload_sha256"}
        if isinstance(binding, dict)
        else {}
    )
    if (
        not isinstance(binding, dict)
        or set(binding) != fields
        or raw != _canonical(binding)
        or binding.get("schema_version") != SCHEMA_VERSION
        or binding.get("kind") != "loom.staging-external-slurm.guard-binding"
        or binding.get("cluster") != "trt-gb10"
        or binding.get("account") != "loom-staging"
        or binding.get("service_user") != STAGING_SERVICE_USER
        or binding.get("slurm_qos") != "loom-staging"
        or binding.get("runtime_id") != "staging"
        or binding.get("env_id") != f"denv-staging-{candidate_sha}"
        or binding.get("candidate_id") != f"cand-{candidate_sha}"
        or binding.get("candidate_sha") != candidate_sha
        or binding.get("candidate_tree") != candidate_tree
        or type(binding.get("resource_generation")) is not int
        or binding["resource_generation"] < 1
        or binding.get("authority_generation") != binding["resource_generation"]
        or not _is_sha(binding.get("authority_convergence_id"), length=64)
        or not _is_sha(binding.get("authority_request_id"), length=64)
        or _canonical_utc(binding.get("authority_requested_at")) is None
        or binding.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
    ):
        raise NodeAuthorityError("staging guard binding is invalid")
    return binding


def _staging_broker_submit(request: Request) -> tuple[dict[str, Any], str]:
    inner = json.loads(request.payload_bytes)
    binding = _staging_guard_binding(
        candidate_sha=str(request.payload["candidate_sha"]),
        candidate_tree=str(request.payload["candidate_tree"]),
    )
    result = _run_fixed(_staging_broker_argv(request))
    expected = {
        "schema_version",
        "kind",
        "request_id",
        "candidate_sha",
        "candidate_tree",
        "job_id",
        "job_name",
        "candidate_set_sha256",
        "resource_generation",
        "docker_cgroup_driver",
        "node",
        "cluster",
        "account",
        "qos",
        "user",
        "uid",
        "gid",
        "service_identity",
        "mount",
        "submitted_at",
        "status",
    }
    if (
        set(result) != expected
        or result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != "staging_external_slurm_allocation_submission"
        or result.get("request_id") != inner["request_id"]
        or result.get("candidate_sha") != request.payload["candidate_sha"]
        or result.get("candidate_tree") != request.payload["candidate_tree"]
        or result.get("job_id") is None
        or re.fullmatch(r"[1-9][0-9]*", str(result["job_id"])) is None
        or result.get("job_name")
        != (
            f"loom827-staging-{request.payload['candidate_sha'][:12]}-"
            f"{inner['requested_node']}-g{result.get('candidate_set_sha256')}-"
            f"a{binding['resource_generation']}"
        )
        or not _is_sha(result.get("candidate_set_sha256"), length=64)
        or result.get("resource_generation") != binding["resource_generation"]
        or result.get("docker_cgroup_driver") != "systemd"
        or result.get("node") != inner["requested_node"]
        or result.get("cluster") != "trt-gb10"
        or result.get("account") != "loom-staging"
        or result.get("qos") != "loom-staging"
        or result.get("user") != STAGING_SERVICE_USER
        or result.get("uid") != STAGING_SERVICE_UID
        or result.get("gid") != STAGING_SERVICE_GID
        or result.get("status") != "submitted"
    ):
        raise NodeAuthorityError("staging broker submission readback is invalid")
    ledger = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-allocation-broker-submission",
        "authority_request_id": request.request_id,
        "submit_request_id": inner["request_id"],
        "candidate_sha": request.payload["candidate_sha"],
        "candidate_tree": request.payload["candidate_tree"],
        "node": inner["requested_node"],
        "job_id": result["job_id"],
        "job_name": result["job_name"],
        "candidate_set_sha256": result["candidate_set_sha256"],
        "resource_generation": result["resource_generation"],
        "user": STAGING_SERVICE_USER,
        "account": "loom-staging",
        "qos": "loom-staging",
        "submitted_at": result["submitted_at"],
        "result_sha256": hashlib.sha256(_canonical(result)).hexdigest(),
    }
    if not _atomic_install(
        _staging_broker_submission_path(inner["request_id"]),
        _canonical(ledger),
        0o600,
        parent_mode=0o700,
    ):
        raise NodeAuthorityError("staging broker submission identity was already used")
    return result, f"staging-broker/v1/submission/{inner['request_id']}"


def _staging_broker_cancel(request: Request) -> tuple[dict[str, Any], str]:
    inner = json.loads(request.payload_bytes)
    binding = _staging_guard_binding(
        candidate_sha=str(request.payload["candidate_sha"]),
        candidate_tree=str(request.payload["candidate_tree"]),
    )
    submission_path = _staging_broker_submission_path(inner["submit_request_id"])
    raw = _safe_root_file(submission_path, mode=0o600, limit=64 * 1024)
    try:
        submission = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NodeAuthorityError("staging broker submission ledger is invalid") from exc
    if (
        not isinstance(submission, dict)
        or raw != _canonical(submission)
        or submission.get("kind") != "loom.staging-allocation-broker-submission"
        or submission.get("submit_request_id") != inner["submit_request_id"]
        or submission.get("candidate_sha") != request.payload["candidate_sha"]
        or submission.get("candidate_tree") != request.payload["candidate_tree"]
        or submission.get("node") != inner["requested_node"]
        or submission.get("job_id") != inner["job_id"]
        or not _is_sha(submission.get("candidate_set_sha256"), length=64)
        or submission.get("resource_generation") != binding["resource_generation"]
        or submission.get("job_name")
        != (
            f"loom827-staging-{request.payload['candidate_sha'][:12]}-"
            f"{inner['requested_node']}-g{submission.get('candidate_set_sha256')}-"
            f"a{binding['resource_generation']}"
        )
        or submission.get("user") != STAGING_SERVICE_USER
        or submission.get("account") != "loom-staging"
        or submission.get("qos") != "loom-staging"
    ):
        raise NodeAuthorityError("staging broker cancel escaped its submission ledger")
    marker = STAGING_BROKER_ROOT / f"{inner['submit_request_id']}-cancel.json"
    if marker.exists() or marker.is_symlink():
        raise NodeAuthorityError("staging broker submission was already cancelled")
    result = _run_fixed(_staging_broker_argv(request))
    if (
        result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != "staging_external_slurm_allocation_cancellation"
        or result.get("request_id") != inner["request_id"]
        or result.get("submit_request_id") != inner["submit_request_id"]
        or result.get("candidate_sha") != request.payload["candidate_sha"]
        or result.get("candidate_tree") != request.payload["candidate_tree"]
        or result.get("job_id") != inner["job_id"]
        or result.get("node") != inner["requested_node"]
        or result.get("status") != "cancelled"
        or any(
            result.get(field) != 0
            for field in ("orphan_containers", "orphan_networks", "orphan_volumes")
        )
    ):
        raise NodeAuthorityError("staging broker cancellation readback is invalid")
    cancellation = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-allocation-broker-cancellation",
        "authority_request_id": request.request_id,
        "cancel_request_id": inner["request_id"],
        "submit_request_id": inner["submit_request_id"],
        "job_id": inner["job_id"],
        "node": inner["requested_node"],
        "candidate_sha": request.payload["candidate_sha"],
        "candidate_tree": request.payload["candidate_tree"],
        "result_sha256": hashlib.sha256(_canonical(result)).hexdigest(),
        "cancelled_at": result["cancelled_at"],
    }
    if not _atomic_install(marker, _canonical(cancellation), 0o600, parent_mode=0o700):
        raise NodeAuthorityError("staging broker cancellation publication collided")
    return result, f"staging-broker/v1/cancellation/{inner['request_id']}"


def _staging_accounting_rows(*arguments: str) -> list[list[str]]:
    output = _run_staging_identity_command(
        ("/usr/bin/sacctmgr", "--noheader", "--parsable2", *arguments),
    )
    rows: list[list[str]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        if fields and fields[-1] == "":
            fields.pop()
        rows.append(fields)
    return rows


def _staging_accounting_snapshot() -> dict[str, Any]:
    return {
        "account": _staging_accounting_rows(
            "show",
            "account",
            "where",
            "name=loom-staging",
            "cluster=trt-gb10",
            "format=Cluster,Account,Descr,Org",
        ),
        "qos": _staging_accounting_rows(
            "show",
            "qos",
            "where",
            "name=loom-staging",
            "format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU,GrpTRES,MaxTRES",
        ),
        "association": _staging_accounting_rows(
            "show",
            "association",
            "where",
            "cluster=trt-gb10",
            "account=loom-staging",
            f"user={STAGING_SERVICE_USER}",
            "format=Cluster,Account,User,QOS,DefaultQOS",
        ),
        "user": _staging_accounting_rows(
            "show",
            "user",
            "where",
            f"name={STAGING_SERVICE_USER}",
            "format=User,DefaultAccount",
        ),
    }


def _staging_accounting_state(snapshot: Mapping[str, Any]) -> dict[str, bool]:
    if set(snapshot) != {"account", "qos", "association", "user"} or any(
        not isinstance(snapshot[name], list) or len(snapshot[name]) > 1 for name in snapshot
    ):
        raise NodeAuthorityError("staging accounting snapshot shape is invalid")
    account = snapshot["account"]
    qos = snapshot["qos"]
    association = snapshot["association"]
    user = snapshot["user"]
    if account and account[0] != [
        "trt-gb10",
        "loom-staging",
        "Loom staging external workers",
        "loom",
    ]:
        raise NodeAuthorityError("staging accounting account drifted")
    if qos:
        row = qos[0]
        if (
            len(row) != 6
            or row[0] != "loom-staging"
            or set(item for item in row[1].split(",") if item) != {"DenyOnLimit"}
            or row[2:4] != ["15", "15"]
            or row[4:] != ["", ""]
        ):
            raise NodeAuthorityError("staging accounting QoS drifted")
    if association:
        row = association[0]
        if (
            len(row) != 5
            or row[:3] != ["trt-gb10", "loom-staging", STAGING_SERVICE_USER]
            or set(item for item in row[3].split(",") if item) != {"loom-staging"}
            or row[4] != "loom-staging"
        ):
            raise NodeAuthorityError("staging accounting association drifted")
    if user and user[0] != [STAGING_SERVICE_USER, "loom-staging"]:
        raise NodeAuthorityError("staging accounting user drifted")
    return {name: bool(snapshot[name]) for name in snapshot}


def _staging_accounting_mutate(*arguments: str) -> None:
    _run_staging_identity_command(("/usr/bin/sacctmgr", "--immediate", *arguments))


def _staging_accounting_rollback(snapshot: Mapping[str, Any]) -> None:
    original = _staging_accounting_state(snapshot)
    current = _staging_accounting_snapshot()
    current_state = _staging_accounting_state(current)
    if not original["association"] and current_state["association"]:
        _staging_accounting_mutate(
            "delete",
            "user",
            "where",
            f"name={STAGING_SERVICE_USER}",
            "account=loom-staging",
            "cluster=trt-gb10",
        )
    if not original["qos"] and current_state["qos"]:
        _staging_accounting_mutate("delete", "qos", "where", "name=loom-staging")
    if not original["account"] and current_state["account"]:
        _staging_accounting_mutate(
            "delete",
            "account",
            "where",
            "name=loom-staging",
            "cluster=trt-gb10",
        )
    if _staging_accounting_snapshot() != snapshot:
        raise NodeAuthorityError("staging accounting rollback did not restore its snapshot")


def _staging_accounting_recover() -> bool:
    if not STAGING_ACCOUNTING_JOURNAL.exists() and not STAGING_ACCOUNTING_JOURNAL.is_symlink():
        return False
    raw = _safe_root_file(
        STAGING_ACCOUNTING_JOURNAL,
        mode=0o600,
        limit=256 * 1024,
    )
    try:
        journal = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NodeAuthorityError("staging accounting journal is invalid") from exc
    expected_fields = {
        "schema_version",
        "kind",
        "authority_request_id",
        "request_id",
        "candidate_sha",
        "candidate_tree",
        "snapshot",
        "snapshot_sha256",
        "phase",
        "created_at",
        "updated_at",
    }
    if (
        not isinstance(journal, dict)
        or raw != _canonical(journal)
        or set(journal) != expected_fields
        or journal.get("schema_version") != SCHEMA_VERSION
        or journal.get("kind") != "loom.staging-slurm-accounting-transaction"
        or not _is_sha(journal.get("authority_request_id"), length=64)
        or not _is_sha(journal.get("request_id"), length=64)
        or not _is_sha(journal.get("candidate_sha"))
        or not _is_sha(journal.get("candidate_tree"))
        or not _is_sha(journal.get("snapshot_sha256"), length=64)
        or journal.get("phase")
        not in {"prepared", "account", "qos", "association", "verified", "committed", "rolled-back"}
        or not isinstance(journal.get("snapshot"), dict)
        or hashlib.sha256(
            _canonical(
                cast(Mapping[str, Any], journal.get("snapshot")),
            ),
        ).hexdigest()
        != journal.get("snapshot_sha256")
        or not isinstance(journal.get("created_at"), str)
        or not isinstance(journal.get("updated_at"), str)
    ):
        raise NodeAuthorityError("staging accounting journal binding is invalid")
    _staging_accounting_state(journal["snapshot"])
    if journal["phase"] in {"committed", "rolled-back"}:
        return False
    _staging_accounting_rollback(journal["snapshot"])
    journal["phase"] = "rolled-back"
    journal["updated_at"] = datetime.now(UTC).isoformat()
    _atomic_replace(
        STAGING_ACCOUNTING_JOURNAL,
        _canonical(journal),
        0o600,
        parent_mode=0o700,
    )
    return True


def _staging_accounting_converge(request: Request) -> tuple[dict[str, Any], str]:
    if (
        request.action != "staging-slurm-accounting-converge"
        or request.payload["node"] != "trt-gb10-1"
        or request.payload["domain"] != "gb10"
    ):
        raise NodeAuthorityError("staging accounting convergence request is invalid")
    recovered = _staging_accounting_recover()
    inner = json.loads(request.payload_bytes)
    snapshot = _staging_accounting_snapshot()
    present = _staging_accounting_state(snapshot)
    timestamp = datetime.now(UTC).isoformat()
    journal = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-slurm-accounting-transaction",
        "authority_request_id": request.request_id,
        "request_id": inner["request_id"],
        "candidate_sha": request.payload["candidate_sha"],
        "candidate_tree": request.payload["candidate_tree"],
        "snapshot": snapshot,
        "snapshot_sha256": hashlib.sha256(_canonical(snapshot)).hexdigest(),
        "phase": "prepared",
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    _atomic_replace(
        STAGING_ACCOUNTING_JOURNAL,
        _canonical(journal),
        0o600,
        parent_mode=0o700,
    )
    try:
        if not present["account"]:
            _staging_accounting_mutate(
                "add",
                "account",
                "name=loom-staging",
                "cluster=trt-gb10",
                "description=Loom staging external workers",
                "organization=loom",
            )
        journal["phase"] = "account"
        journal["updated_at"] = datetime.now(UTC).isoformat()
        _atomic_replace(
            STAGING_ACCOUNTING_JOURNAL,
            _canonical(journal),
            0o600,
            parent_mode=0o700,
        )
        if not present["qos"]:
            _staging_accounting_mutate(
                "add",
                "qos",
                "name=loom-staging",
                "flags=DenyOnLimit",
                "maxjobspu=15",
                "maxsubmitjobspu=15",
            )
        journal["phase"] = "qos"
        journal["updated_at"] = datetime.now(UTC).isoformat()
        _atomic_replace(
            STAGING_ACCOUNTING_JOURNAL,
            _canonical(journal),
            0o600,
            parent_mode=0o700,
        )
        if not present["association"]:
            _staging_accounting_mutate(
                "add",
                "user",
                f"name={STAGING_SERVICE_USER}",
                "account=loom-staging",
                "cluster=trt-gb10",
                "defaultaccount=loom-staging",
            )
            _staging_accounting_mutate(
                "modify",
                "association",
                "where",
                f"user={STAGING_SERVICE_USER}",
                "account=loom-staging",
                "cluster=trt-gb10",
                "set",
                "qos=loom-staging",
                "defaultqos=loom-staging",
            )
        journal["phase"] = "association"
        journal["updated_at"] = datetime.now(UTC).isoformat()
        _atomic_replace(
            STAGING_ACCOUNTING_JOURNAL,
            _canonical(journal),
            0o600,
            parent_mode=0o700,
        )
        verified = _staging_accounting_snapshot()
        if not all(_staging_accounting_state(verified).values()):
            raise NodeAuthorityError("staging accounting convergence is incomplete")
        journal["phase"] = "verified"
        journal["updated_at"] = datetime.now(UTC).isoformat()
        _atomic_replace(
            STAGING_ACCOUNTING_JOURNAL,
            _canonical(journal),
            0o600,
            parent_mode=0o700,
        )
    except Exception:
        _staging_accounting_rollback(snapshot)
        journal["phase"] = "rolled-back"
        journal["updated_at"] = datetime.now(UTC).isoformat()
        _atomic_replace(
            STAGING_ACCOUNTING_JOURNAL,
            _canonical(journal),
            0o600,
            parent_mode=0o700,
        )
        raise
    journal["phase"] = "committed"
    journal["updated_at"] = datetime.now(UTC).isoformat()
    _atomic_replace(
        STAGING_ACCOUNTING_JOURNAL,
        _canonical(journal),
        0o600,
        parent_mode=0o700,
    )
    result = {
        "schema_version": SCHEMA_VERSION,
        "kind": "staging_external_slurm_accounting_convergence",
        "request_id": inner["request_id"],
        "candidate_sha": request.payload["candidate_sha"],
        "candidate_tree": request.payload["candidate_tree"],
        "cluster": "trt-gb10",
        "account": {
            "name": "loom-staging",
            "description": "Loom staging external workers",
            "organization": "loom",
        },
        "qos": {
            "name": "loom-staging",
            "flags": ["DenyOnLimit"],
            "max_jobs_per_user": 15,
            "max_submit_jobs_per_user": 15,
            "group_tres": None,
            "max_tres": None,
        },
        "association": {
            "user": STAGING_SERVICE_USER,
            "account": "loom-staging",
            "qos": ["loom-staging"],
            "default_qos": "loom-staging",
            "default_account": "loom-staging",
        },
        "snapshot_sha256": journal["snapshot_sha256"],
        "journal_sha256": hashlib.sha256(_canonical(journal)).hexdigest(),
        "recovered": recovered,
        "status": "converged",
    }
    artifact = STAGING_ACCOUNTING_ROOT / f"{inner['request_id']}.json"
    if not _atomic_install(
        artifact,
        _canonical(result),
        0o600,
        parent_mode=0o700,
    ):
        existing = _safe_root_file(artifact, mode=0o600, limit=256 * 1024)
        if existing != _canonical(result):
            raise NodeAuthorityError("staging accounting result artifact drifted")
    return result, f"staging-accounting/v1/{inner['request_id']}"


def _load_optional_canonical_root(
    path: Path,
    *,
    fields: set[str],
    label: str,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw = _safe_root_file(path, mode=0o600, limit=2 * 1024 * 1024)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError(f"{label} is invalid") from exc
    if not isinstance(payload, dict) or set(payload) != fields or raw != _canonical(payload):
        raise NodeAuthorityError(f"{label} is invalid")
    return payload


def _staging_infrastructure_operation_envelope(
    *,
    action: str,
    node: str,
    candidate_sha: str,
    candidate_tree: str,
    generation: int,
    convergence_id: str,
    requested_at: str,
) -> bytes:
    inner_unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-external-slurm.infrastructure-operation-request",
        "action": action,
        "node": node,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "generation": generation,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
    }
    inner = {
        **inner_unsigned,
        "request_id": hashlib.sha256(_canonical(inner_unsigned)).hexdigest(),
    }
    payload_bytes = _canonical(inner)
    outer_unsigned = {
        "schema_version": SCHEMA_VERSION,
        "action": action,
        "node": node,
        "domain": "gb10",
        "sandbox": STAGING_SCOPE,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "payload_kind": "staging-infrastructure-operation-request",
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
        "prior_request_id": None,
    }
    return _canonical(
        {
            **outer_unsigned,
            "request_id": hashlib.sha256(_canonical(outer_unsigned)).hexdigest(),
        },
    )


def _staging_infrastructure_install_envelope(
    receipt: Mapping[str, Any],
) -> bytes:
    payload_bytes = _canonical(receipt)
    outer_unsigned = {
        "schema_version": SCHEMA_VERSION,
        "action": "staging-infrastructure-install",
        "node": "oldlab-1",
        "domain": "oldlab",
        "sandbox": STAGING_SCOPE,
        "candidate_sha": receipt["candidate_sha"],
        "candidate_tree": receipt["candidate_tree"],
        "payload_kind": "staging-infrastructure-receipt-json",
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
        "prior_request_id": None,
    }
    return _canonical(
        {
            **outer_unsigned,
            "request_id": hashlib.sha256(_canonical(outer_unsigned)).hexdigest(),
        },
    )


def _staging_infrastructure_transport(
    node: str,
    envelope: bytes,
) -> dict[str, Any]:
    expected = json.loads(envelope)
    expected_inner_payload = json.loads(
        base64.b64decode(expected["payload_base64"], validate=True),
    )
    expected_inner_receipt = (
        f"staging-accounting/v1/{expected_inner_payload['request_id']}"
        if expected["action"] == "staging-slurm-accounting-converge"
        else None
    )
    receipt = _run_fixed_input(
        (
            str(NODE_TRANSPORT),
            "invoke",
            "--node",
            node,
            "--verb",
            "transact",
        ),
        envelope,
    )
    if (
        set(receipt) != RECEIPT_FIELDS
        or receipt.get("schema_version") != SCHEMA_VERSION
        or any(
            receipt.get(field) != expected[field]
            for field in (
                "request_id",
                "action",
                "node",
                "domain",
                "sandbox",
                "candidate_sha",
                "candidate_tree",
                "payload_sha256",
            )
        )
        or not _is_sha(receipt.get("result_sha256"), length=64)
        or receipt.get("inner_receipt") != expected_inner_receipt
        or receipt.get("status") != "succeeded"
        or _canonical_utc(receipt.get("completed_at")) is None
    ):
        raise NodeAuthorityError("staging infrastructure transport receipt is invalid")
    return receipt


def _staging_infrastructure_mount_contract() -> dict[str, Any]:
    return {
        "source": "192.168.20.12:/shared_work2/loom/staging",
        "target": str(STAGING_SHARED_ROOT),
        "filesystem_type": "nfs4",
        "repository_root": str(STAGING_SHARED_PATHS[0]),
        "worker_env_root": str(STAGING_SHARED_PATHS[1]),
        "result_root": str(STAGING_SHARED_PATHS[2]),
        "root_uid": 0,
        "root_gid": STAGING_SERVICE_GID,
        "root_mode": "0o750",
        "repository_root_mode": "0o750",
        "worker_env_root_mode": "0o750",
        "result_uid": STAGING_SERVICE_UID,
        "result_gid": STAGING_SERVICE_GID,
        "result_root_mode": "0o2770",
    }


def _validate_staging_infrastructure_receipt(
    receipt: Mapping[str, Any],
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> tuple[datetime, datetime]:
    requested_at = _canonical_utc(receipt.get("requested_at"))
    created_at = _canonical_utc(receipt.get("created_at"))
    expires_at = _canonical_utc(receipt.get("expires_at"))
    observed_at = datetime.now(UTC)
    expected_converge_request = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "convergence_id": receipt.get("convergence_id"),
        "requested_at": receipt.get("requested_at"),
    }
    node_bootstraps = receipt.get("node_bootstraps")
    if (
        set(receipt) != STAGING_INFRASTRUCTURE_RECEIPT_FIELDS
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("kind") != "loom.staging-external-slurm.infrastructure-receipt"
        or receipt.get("candidate_sha") != candidate_sha
        or receipt.get("candidate_tree") != candidate_tree
        or not _is_sha(receipt.get("convergence_id"), length=64)
        or not _is_sha(receipt.get("request_sha256"), length=64)
        or receipt.get("request_sha256")
        != hashlib.sha256(_canonical(expected_converge_request)).hexdigest()
        or not isinstance(receipt.get("generation"), int)
        or isinstance(receipt.get("generation"), bool)
        or receipt["generation"] < 1
        or receipt.get("source_controller") != "oldlab-2"
        or receipt.get("source_controller_host") != NODE_HOSTNAMES["oldlab-2"]
        or receipt.get("mount_contract") != _staging_infrastructure_mount_contract()
        or receipt.get("result") != "pass"
        or not isinstance(node_bootstraps, list)
        or len(node_bootstraps) != len(STAGING_INFRASTRUCTURE_NODES)
        or requested_at is None
        or created_at is None
        or expires_at is None
        or not (requested_at <= created_at < expires_at)
        or expires_at - created_at
        > timedelta(seconds=STAGING_INFRASTRUCTURE_MAX_TRANSACTION_SECONDS)
        or requested_at > observed_at + timedelta(seconds=30)
        or created_at > observed_at + timedelta(seconds=30)
        or expires_at <= observed_at
        or observed_at - requested_at
        > timedelta(seconds=STAGING_INFRASTRUCTURE_MAX_TRANSACTION_SECONDS)
    ):
        raise NodeAuthorityError("staging infrastructure receipt binding is invalid")
    operations = [
        (
            "staging-shared-source-bootstrap",
            "trt-gb10-2",
            receipt.get("source_bootstrap"),
        ),
        (
            "staging-slurm-accounting-converge",
            "trt-gb10-1",
            receipt.get("accounting"),
        ),
        *[
            ("staging-allocation-bootstrap", node, operation)
            for node, operation in zip(
                STAGING_INFRASTRUCTURE_NODES,
                node_bootstraps,
                strict=True,
            )
        ],
    ]
    completed: list[datetime] = []
    for action, node, operation in operations:
        expected = json.loads(
            _staging_infrastructure_operation_envelope(
                action=action,
                node=node,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                generation=int(receipt["generation"]),
                convergence_id=str(receipt["convergence_id"]),
                requested_at=str(receipt["requested_at"]),
            ),
        )
        completed_at = (
            _canonical_utc(operation.get("completed_at")) if isinstance(operation, dict) else None
        )
        expected_inner_payload = json.loads(
            base64.b64decode(expected["payload_base64"], validate=True),
        )
        inner_receipt = operation.get("inner_receipt") if isinstance(operation, dict) else None
        expected_inner_receipt = (
            f"staging-accounting/v1/{expected_inner_payload['request_id']}"
            if action == "staging-slurm-accounting-converge"
            else None
        )
        inner_receipt_valid = (
            inner_receipt == expected_inner_receipt
            if action == "staging-slurm-accounting-converge"
            else (
                re.fullmatch(
                    r"staging-shared-source-bootstrap/v1/[0-9a-f]{64}",
                    str(inner_receipt),
                )
                is not None
                if action == "staging-shared-source-bootstrap"
                else re.fullmatch(
                    (
                        r"staging-allocation-bootstrap/v1/"
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}/[0-9a-f]{64}"
                    ),
                    str(inner_receipt),
                )
                is not None
            )
        )
        if (
            not isinstance(operation, dict)
            or set(operation) != RECEIPT_FIELDS
            or any(
                operation.get(field) != expected[field]
                for field in (
                    "schema_version",
                    "request_id",
                    "action",
                    "node",
                    "domain",
                    "sandbox",
                    "candidate_sha",
                    "candidate_tree",
                    "payload_sha256",
                )
            )
            or not _is_sha(operation.get("result_sha256"), length=64)
            or not inner_receipt_valid
            or operation.get("status") != "succeeded"
            or completed_at is None
            or completed_at < requested_at
            or completed_at > observed_at + timedelta(seconds=30)
        ):
            raise NodeAuthorityError("staging infrastructure operation receipt is invalid")
        completed.append(completed_at)
    if completed != sorted(completed) or completed[-1] > created_at:
        raise NodeAuthorityError("staging infrastructure completion order is invalid")
    return requested_at, created_at


def _staging_infrastructure_install(
    request: Request,
) -> tuple[dict[str, Any], str]:
    receipt = json.loads(request.payload_bytes)
    requested_at, _created_at = _validate_staging_infrastructure_receipt(
        receipt,
        candidate_sha=str(request.payload["candidate_sha"]),
        candidate_tree=str(request.payload["candidate_tree"]),
    )
    request_sha256 = hashlib.sha256(request.payload_bytes).hexdigest()
    generation = int(receipt["generation"])
    convergence_id = str(receipt["convergence_id"])
    descriptor = _open_named_lock(
        STAGING_INFRASTRUCTURE_INSTALL_LOCK,
        exclusive=True,
    )
    try:
        high_water_fields = {
            "schema_version",
            "generation",
            "convergence_id",
            "requested_at",
            "request_sha256",
        }
        high_water = _load_optional_canonical_root(
            STAGING_INFRASTRUCTURE_INSTALL_HIGH_WATER,
            fields=high_water_fields,
            label="staging infrastructure install high-water",
        )
        generation_path = STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS / f"{generation}.json"
        existing_generation = (
            _safe_root_file(generation_path, mode=0o600, limit=2 * 1024 * 1024)
            if generation_path.exists()
            else None
        )
        if high_water is not None:
            previous_generation = int(high_water["generation"])
            previous_requested = _canonical_utc(high_water["requested_at"])
            if generation < previous_generation or generation > previous_generation + 1:
                raise NodeAuthorityError(
                    "staging infrastructure generation regressed or skipped",
                )
            if generation == previous_generation and (
                convergence_id != high_water["convergence_id"]
                or request_sha256 != high_water["request_sha256"]
                or existing_generation != request.payload_bytes
            ):
                raise NodeAuthorityError(
                    "staging infrastructure high-water replay was not byte-identical",
                )
            if generation == previous_generation + 1 and (
                previous_requested is None
                or requested_at <= previous_requested
                or convergence_id == high_water["convergence_id"]
            ):
                raise NodeAuthorityError(
                    "staging infrastructure generation regressed or replayed",
                )
        elif generation != 1:
            raise NodeAuthorityError("staging infrastructure first generation is invalid")
        if existing_generation is not None and existing_generation != request.payload_bytes:
            raise NodeAuthorityError(
                "staging infrastructure generation was replayed with different bytes",
            )
        if existing_generation is None and high_water is not None:
            if generation == int(high_water["generation"]):
                raise NodeAuthorityError(
                    "staging infrastructure high-water artifact is missing",
                )
        candidate_path = STAGING_INFRASTRUCTURE_RECEIPT_ROOT / (
            f"{request.payload['candidate_sha']}.json"
        )
        if existing_generation is None:
            _atomic_install(
                generation_path,
                request.payload_bytes,
                0o600,
                parent_mode=0o700,
            )
        _atomic_replace(
            candidate_path,
            request.payload_bytes,
            0o600,
            parent_mode=0o700,
        )
        _atomic_replace(
            STAGING_INFRASTRUCTURE_INSTALL_JOURNAL,
            _canonical(
                {
                    "schema_version": SCHEMA_VERSION,
                    "generation": generation,
                    "convergence_id": convergence_id,
                    "candidate_sha": request.payload["candidate_sha"],
                    "candidate_tree": request.payload["candidate_tree"],
                    "requested_at": receipt["requested_at"],
                    "request_sha256": request_sha256,
                    "receipt_sha256": request_sha256,
                    "phase": "committed",
                },
            ),
            0o600,
            parent_mode=0o700,
        )
        _atomic_replace(
            STAGING_INFRASTRUCTURE_INSTALL_HIGH_WATER,
            _canonical(
                {
                    "schema_version": SCHEMA_VERSION,
                    "generation": generation,
                    "convergence_id": convergence_id,
                    "requested_at": receipt["requested_at"],
                    "request_sha256": request_sha256,
                },
            ),
            0o600,
            parent_mode=0o700,
        )
    finally:
        os.close(descriptor)
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.staging-external-slurm.infrastructure-installation",
            "candidate_sha": request.payload["candidate_sha"],
            "candidate_tree": request.payload["candidate_tree"],
            "generation": generation,
            "convergence_id": convergence_id,
            "receipt_path": str(candidate_path),
            "receipt_sha256": request_sha256,
            "status": "committed",
        },
        f"staging-infrastructure-install/v1/{generation}",
    )


def _write_staging_infrastructure_producer_journal(
    *,
    request: Request,
    generation: int,
    convergence_id: str,
    requested_at: str,
    request_sha256: str,
    operation_receipts: Sequence[Mapping[str, Any]],
    phase: str,
) -> None:
    _atomic_replace(
        STAGING_INFRASTRUCTURE_PRODUCER_JOURNAL,
        _canonical(
            {
                "schema_version": SCHEMA_VERSION,
                "generation": generation,
                "convergence_id": convergence_id,
                "candidate_sha": request.payload["candidate_sha"],
                "candidate_tree": request.payload["candidate_tree"],
                "requested_at": requested_at,
                "request_sha256": request_sha256,
                "operation_receipts": list(operation_receipts),
                "phase": phase,
            },
        ),
        0o600,
        parent_mode=0o700,
    )


def _resume_staging_infrastructure_generation(
    *,
    request: Request,
    convergence_id: str,
    requested_at: str,
    requested_time: datetime,
    request_sha256: str,
    high_water: Mapping[str, Any] | None,
) -> tuple[int, list[dict[str, Any]]]:
    journal_fields = {
        "schema_version",
        "generation",
        "convergence_id",
        "candidate_sha",
        "candidate_tree",
        "requested_at",
        "request_sha256",
        "operation_receipts",
        "phase",
    }
    journal = _load_optional_canonical_root(
        STAGING_INFRASTRUCTURE_PRODUCER_JOURNAL,
        fields=journal_fields,
        label="staging infrastructure producer journal",
    )
    if journal is not None and journal["phase"] != "committed":
        if (
            journal["convergence_id"] != convergence_id
            or journal["request_sha256"] != request_sha256
            or journal["candidate_sha"] != request.payload["candidate_sha"]
            or journal["candidate_tree"] != request.payload["candidate_tree"]
            or journal["requested_at"] != requested_at
            or not isinstance(journal["operation_receipts"], list)
        ):
            raise NodeAuthorityError(
                "staging infrastructure producer has another active generation",
            )
        generation = int(journal["generation"])
        operation_receipts = list(journal["operation_receipts"])
    else:
        previous_requested = (
            _canonical_utc(high_water["requested_at"]) if high_water is not None else None
        )
        if high_water is not None and (
            previous_requested is None or requested_time <= previous_requested
        ):
            raise NodeAuthorityError("staging infrastructure convergence request regressed")
        if high_water is not None and convergence_id == high_water["convergence_id"]:
            raise NodeAuthorityError("staging infrastructure convergence ID was replayed")
        generation = int(high_water["generation"]) + 1 if high_water else 1
        operation_receipts = []
        _write_staging_infrastructure_producer_journal(
            request=request,
            generation=generation,
            convergence_id=convergence_id,
            requested_at=requested_at,
            request_sha256=request_sha256,
            operation_receipts=operation_receipts,
            phase="running",
        )
    operation_specs = [
        ("staging-shared-source-bootstrap", "trt-gb10-2"),
        ("staging-slurm-accounting-converge", "trt-gb10-1"),
        *[("staging-allocation-bootstrap", node) for node in STAGING_INFRASTRUCTURE_NODES],
    ]
    if len(operation_receipts) > len(operation_specs):
        raise NodeAuthorityError("staging infrastructure producer journal is invalid")
    for index, (action, node) in enumerate(
        operation_specs[len(operation_receipts) :],
        start=len(operation_receipts),
    ):
        envelope = _staging_infrastructure_operation_envelope(
            action=action,
            node=node,
            candidate_sha=str(request.payload["candidate_sha"]),
            candidate_tree=str(request.payload["candidate_tree"]),
            generation=generation,
            convergence_id=convergence_id,
            requested_at=requested_at,
        )
        operation_receipts.append(_staging_infrastructure_transport(node, envelope))
        _write_staging_infrastructure_producer_journal(
            request=request,
            generation=generation,
            convergence_id=convergence_id,
            requested_at=requested_at,
            request_sha256=request_sha256,
            operation_receipts=operation_receipts,
            phase=f"operation-{index + 1}",
        )
    return generation, operation_receipts


def _staging_infrastructure_converge(
    request: Request,
) -> tuple[dict[str, Any], str]:
    converge = json.loads(request.payload_bytes)
    convergence_id = str(converge["convergence_id"])
    requested_at = str(converge["requested_at"])
    requested_time = _canonical_utc(requested_at)
    if requested_time is None:
        raise NodeAuthorityError("staging infrastructure convergence time is invalid")
    observed_time = datetime.now(UTC)
    if requested_time > observed_time + timedelta(
        seconds=30
    ) or observed_time - requested_time > timedelta(
        seconds=STAGING_INFRASTRUCTURE_MAX_TRANSACTION_SECONDS
    ):
        raise NodeAuthorityError(
            "staging infrastructure convergence time is outside its transaction window",
        )
    request_sha256 = hashlib.sha256(request.payload_bytes).hexdigest()
    descriptor = _open_named_lock(
        STAGING_INFRASTRUCTURE_PRODUCER_LOCK,
        exclusive=True,
    )
    try:
        high_water_fields = {
            "schema_version",
            "generation",
            "convergence_id",
            "requested_at",
            "request_sha256",
        }
        high_water = _load_optional_canonical_root(
            STAGING_INFRASTRUCTURE_PRODUCER_HIGH_WATER,
            fields=high_water_fields,
            label="staging infrastructure producer high-water",
        )
        receipt_path = STAGING_INFRASTRUCTURE_PRODUCER_RECEIPTS / f"{convergence_id}.json"
        if receipt_path.exists():
            existing_raw = _safe_root_file(
                receipt_path,
                mode=0o600,
                limit=2 * 1024 * 1024,
            )
            existing = json.loads(existing_raw)
            if (
                not isinstance(existing, dict)
                or existing_raw != _canonical(existing)
                or existing.get("request_sha256") != request_sha256
                or existing.get("candidate_sha") != request.payload["candidate_sha"]
                or existing.get("candidate_tree") != request.payload["candidate_tree"]
            ):
                raise NodeAuthorityError(
                    "staging infrastructure convergence ID was replayed or tampered",
                )
            receipt = existing
            generation = int(receipt["generation"])
            if (high_water is None and generation != 1) or (
                high_water is not None
                and (
                    generation < int(high_water["generation"])
                    or generation > int(high_water["generation"]) + 1
                    or (
                        generation == int(high_water["generation"])
                        and (
                            convergence_id != high_water["convergence_id"]
                            or request_sha256 != high_water["request_sha256"]
                        )
                    )
                )
            ):
                raise NodeAuthorityError(
                    "staging infrastructure producer receipt is not current",
                )
        else:
            generation, operation_receipts = _resume_staging_infrastructure_generation(
                request=request,
                convergence_id=convergence_id,
                requested_at=requested_at,
                requested_time=requested_time,
                request_sha256=request_sha256,
                high_water=high_water,
            )
            created = datetime.now(UTC)
            receipt = {
                "schema_version": SCHEMA_VERSION,
                "kind": "loom.staging-external-slurm.infrastructure-receipt",
                "candidate_sha": request.payload["candidate_sha"],
                "candidate_tree": request.payload["candidate_tree"],
                "generation": generation,
                "convergence_id": convergence_id,
                "requested_at": requested_at,
                "request_sha256": request_sha256,
                "source_controller": "oldlab-2",
                "source_controller_host": NODE_HOSTNAMES["oldlab-2"],
                "created_at": _timestamp(created),
                "expires_at": _timestamp(created + timedelta(seconds=3600)),
                "source_bootstrap": operation_receipts[0],
                "accounting": operation_receipts[1],
                "node_bootstraps": operation_receipts[2:],
                "mount_contract": _staging_infrastructure_mount_contract(),
                "result": "pass",
            }
            _validate_staging_infrastructure_receipt(
                receipt,
                candidate_sha=str(request.payload["candidate_sha"]),
                candidate_tree=str(request.payload["candidate_tree"]),
            )
            _atomic_install(
                receipt_path,
                _canonical(receipt),
                0o600,
                parent_mode=0o700,
            )
        install_envelope = _staging_infrastructure_install_envelope(receipt)
        install_receipt = _staging_infrastructure_transport(
            "oldlab-1",
            install_envelope,
        )
        if install_receipt.get("inner_receipt") != (
            f"staging-infrastructure-install/v1/{generation}"
        ):
            raise NodeAuthorityError(
                "staging infrastructure install receipt binding is invalid",
            )
        high_water_record = {
            "schema_version": SCHEMA_VERSION,
            "generation": generation,
            "convergence_id": convergence_id,
            "requested_at": requested_at,
            "request_sha256": request_sha256,
        }
        _atomic_replace(
            STAGING_INFRASTRUCTURE_PRODUCER_HIGH_WATER,
            _canonical(high_water_record),
            0o600,
            parent_mode=0o700,
        )
        _write_staging_infrastructure_producer_journal(
            request=request,
            generation=generation,
            convergence_id=convergence_id,
            requested_at=requested_at,
            request_sha256=request_sha256,
            operation_receipts=[
                receipt["source_bootstrap"],
                receipt["accounting"],
                *receipt["node_bootstraps"],
            ],
            phase="committed",
        )
    finally:
        os.close(descriptor)
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.staging-external-slurm.infrastructure-convergence",
            "candidate_sha": request.payload["candidate_sha"],
            "candidate_tree": request.payload["candidate_tree"],
            "generation": generation,
            "convergence_id": convergence_id,
            "requested_at": requested_at,
            "receipt_sha256": hashlib.sha256(_canonical(receipt)).hexdigest(),
            "install_request_id": json.loads(install_envelope)["request_id"],
            "status": "converged",
        },
        f"staging-infrastructure/v1/{convergence_id}",
    )


def _execute_request(
    request: Request,
    policy: AuthorityPolicy,
) -> tuple[dict[str, Any], str | None]:
    payload = request.payload
    action = request.action
    domain = str(payload["domain"])
    sandbox = str(payload["sandbox"])
    sha = str(payload["candidate_sha"])
    tree = str(payload["candidate_tree"])
    if action == REGISTRY_SNAPSHOT_SYNC_ACTION:
        return _publish_registry_snapshot(request.payload_bytes, policy=policy), None
    if action == ACCEPTANCE_PROBE_ACTION:
        result = _run_fixed_input(
            _acceptance_probe_policy_argv(request),
            request.payload_bytes,
        )
        return _validated_acceptance_probe_result(request, result), None
    if action == RUNTIME_RETIRE_ACTION:
        return _execute_runtime_retire(request), None
    if action == "staging-allocation-bootstrap":
        result = _staging_allocation_bootstrap(request)
        return (
            result,
            (f"staging-allocation-bootstrap/v1/{result['boot_id']}/{result['mount_digest']}"),
        )
    if action == "staging-shared-source-bootstrap":
        result = _staging_shared_source_bootstrap(request)
        return result, f"staging-shared-source-bootstrap/v1/{result['source_digest']}"
    if action == "staging-allocation-submit":
        return _staging_broker_submit(request)
    if action == "staging-allocation-cancel":
        return _staging_broker_cancel(request)
    if action == "staging-slurm-accounting-converge":
        return _staging_accounting_converge(request)
    if action == "staging-infrastructure-converge":
        return _staging_infrastructure_converge(request)
    if action == "staging-infrastructure-install":
        return _staging_infrastructure_install(request)
    if action == "staging-allocation-probe":
        result = _run_fixed(_staging_allocation_probe_argv(request))
        expected_fields = {
            "schema_version",
            "kind",
            "request_id",
            "candidate_sha",
            "candidate_tree",
            "cluster",
            "pool",
            "submit_host",
            "controller",
            "service_identity",
            "namespace",
            "slurm_account",
            "qos",
            "allowed_nodes",
            "repository",
            "worker_env",
            "nodes",
            "result",
        }
        inner = json.loads(request.payload_bytes)
        if (
            set(result) != expected_fields
            or result.get("schema_version") != SCHEMA_VERSION
            or result.get("kind") != "staging_external_slurm_allocation_probe"
            or result.get("request_id") != inner["request_id"]
            or result.get("candidate_sha") != sha
            or result.get("candidate_tree") != tree
            or result.get("cluster") != "trt-gb10"
            or result.get("pool") != "gb10"
            or result.get("submit_host") != "trt-gb10-1"
            or result.get("controller") != "trt-gb10-1"
            or result.get("service_identity")
            != {
                "username": STAGING_SERVICE_USER,
                "group": STAGING_SERVICE_GROUP,
                "uid": STAGING_SERVICE_UID,
                "gid": STAGING_SERVICE_GID,
                "home": str(STAGING_SERVICE_HOME),
                "shell": STAGING_SERVICE_SHELL,
                "supplementary_groups": list(STAGING_SUPPLEMENTARY_GROUPS),
            }
            or not isinstance(result.get("namespace"), dict)
            or result["namespace"].get("root") != str(STAGING_SHARED_ROOT)
            or result["namespace"].get("mount_source") != "192.168.20.12:/shared_work2/loom/staging"
            or result["namespace"].get("mount_fstype") != "nfs4"
            or not isinstance(result["namespace"].get("mount_inode"), int)
            or result.get("slurm_account") != "loom-staging"
            or result.get("qos") != "loom-staging"
            or result.get("allowed_nodes") != [f"trt-gb10-{index}" for index in range(1, 16)]
            or not isinstance(result.get("nodes"), list)
            or len(result["nodes"]) != 15
            or result.get("result") != "pass"
        ):
            raise NodeAuthorityError("staging allocation probe readback is invalid")
        return result, f"staging-probe/v1/{inner['request_id']}"
    if action == "collect-live-overlap":
        result = _run_fixed_input(
            _live_authority_argv(request, "collect"),
            request.payload_bytes,
        )
        expected_path = (
            f"/var/lib/loom-developer-sandbox-live-authority/overlap/"
            f"{domain}/{sandbox}/{sha}/{result.get('job_id')}.json"
        )
        if (
            set(result)
            != {
                "schema_version",
                "kind",
                "path",
                "payload_sha256",
                "job_id",
                "observation_sequence",
                "observed_at",
            }
            or result.get("schema_version") != SCHEMA_VERSION
            or result.get("kind") != "loom.developer-sandbox.live-overlap-result"
            or result.get("path") != expected_path
            or not _is_sha(result.get("payload_sha256"), length=64)
            or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(result.get("job_id"))) is None
            or not isinstance(result.get("observation_sequence"), int)
            or isinstance(result.get("observation_sequence"), bool)
            or result["observation_sequence"] < 1
            or not isinstance(result.get("observed_at"), str)
        ):
            raise NodeAuthorityError("live overlap collection readback is invalid")
        return result, expected_path
    if action == "host-converge":
        return (
            _run_fixed(
                _domain_argv(
                    request,
                    "host-converge",
                    "--domain",
                    domain,
                    "--execute",
                ),
            ),
            None,
        )
    if action == "slurm-identity-converge":
        local = _identity_converge(request)
        accounting = _validated_slurm_identity_result(
            request,
            _run_fixed_input(
                _slurm_identity_policy_argv(request, "identity-reconcile"),
                request.payload_bytes,
            ),
            operation="reconcile",
        )
        return (
            {
                **local,
                "slurm_accounting_status": accounting["status"],
                "slurm_accounting_receipt_sha256": hashlib.sha256(
                    _canonical(accounting),
                ).hexdigest(),
                "owned_jobs": accounting["jobs"],
            },
            None,
        )
    if action == "slurm-identity-retire":
        accounting = _validated_slurm_identity_result(
            request,
            _run_fixed_input(
                _slurm_identity_policy_argv(request, "identity-retire"),
                request.payload_bytes,
            ),
            operation="retire",
        )
        return accounting, str(accounting["tombstone"])
    if action in {"slurm-node-converge", "slurm-controller-converge"}:
        _validate_slurm_candidate(request, policy)
        result = _run_fixed(_slurm_policy_argv(request, "apply"))
        return result, _slurm_policy_binding(request, result, snapshot_field="snapshot")
    if action == "slurm-rollback":
        prior = _read_receipt(str(payload["prior_request_id"]))
        expected_action = (
            "slurm-controller-converge"
            if payload["node"] == SLURM_CONTROLLER[domain]
            else "slurm-node-converge"
        )
        if (
            prior is None
            or prior.get("node") != payload["node"]
            or prior.get("domain") != domain
            or prior.get("sandbox") != sandbox
            or prior.get("candidate_sha") != sha
            or prior.get("candidate_tree") != tree
            or prior.get("action") != expected_action
        ):
            raise NodeAuthorityError("Slurm rollback receipt is invalid")
        _validate_prior_slurm_binding(request, prior)
        _validate_slurm_candidate(request, policy)
        result = _run_fixed(_slurm_policy_argv(request, "rollback"))
        return (
            result,
            _slurm_policy_binding(
                request,
                result,
                snapshot_field="recovery_snapshot",
            ),
        )
    if action == "rollback":
        prior = _read_receipt(str(payload["prior_request_id"]))
        if (
            prior is None
            or prior.get("node") != payload["node"]
            or prior.get("domain") != domain
            or prior.get("sandbox") != sandbox
            or prior.get("candidate_sha") != sha
            or prior.get("candidate_tree") != tree
            or any(prior.get(field) != payload[field] for field in DYNAMIC_TARGET_BINDING_FIELDS)
            or prior.get("action") not in {"materialize", "attest"}
            or not isinstance(prior.get("inner_receipt"), str)
        ):
            raise NodeAuthorityError("node authority rollback receipt is invalid")
        receipt_path = Path(str(prior["inner_receipt"]))
        expected_root = Path("/var/lib/loom-developer-domain-runtime").resolve()
        try:
            resolved = receipt_path.resolve(strict=True)
        except OSError as exc:
            raise NodeAuthorityError("node authority rollback receipt is unavailable") from exc
        if expected_root not in resolved.parents:
            raise NodeAuthorityError("node authority rollback receipt path is invalid")
        return (
            _run_fixed(
                _domain_argv(
                    request,
                    "rollback",
                    "--receipt",
                    str(resolved),
                    "--execute",
                ),
            ),
            str(resolved),
        )
    if action == "persist-fleet-attestation":
        result = _run_fixed_input(
            (
                "/usr/bin/python3",
                "-I",
                "-B",
                str(SOURCE_ROOT / REMOTE_LINK_HOST_RELATIVE),
                "persist-attestation",
                "--sandbox",
                sandbox,
                "--candidate-sha",
                sha,
                "--execute",
            ),
            request.payload_bytes,
        )
        fleet = json.loads(request.payload_bytes)
        if (
            set(result)
            != {
                "schema_version",
                "sandbox",
                "candidate_sha",
                "path",
                "payload_sha256",
            }
            or result.get("schema_version") != SCHEMA_VERSION
            or result.get("sandbox") != sandbox
            or result.get("candidate_sha") != sha
            or result.get("path")
            != f"/var/lib/loom-developer-sandbox-links/attestations/{sandbox}/{sha}/fleet.json"
            or result.get("payload_sha256") != fleet.get("payload_sha256")
        ):
            raise NodeAuthorityError(
                "fleet attestation persistence readback is invalid",
            )
        return (
            result,
            None,
        )
    stage = _prepare_stage(request)
    try:
        if action == "materialize":
            bundle = stage / "candidate.bundle"
            _write_stage_file(bundle, request.payload_bytes, 0o600)
            result = _run_fixed(
                _domain_argv(
                    request,
                    "materialize",
                    "--domain",
                    domain,
                    "--sandbox",
                    sandbox,
                    "--candidate-sha",
                    sha,
                    "--candidate-tree",
                    tree,
                    "--source-bundle",
                    str(bundle),
                    "--execute",
                ),
            )
            if (
                result.get("schema_version") != SCHEMA_VERSION
                or result.get("operation") != "materialize"
                or result.get("mode") != "applied"
                or result.get("domain") != domain
                or result.get("sandbox") != sandbox
                or result.get("candidate_sha") != sha
                or result.get("candidate_tree") != tree
                or not isinstance(result.get("journal"), str)
                or not str(result["journal"]).startswith(
                    "/var/lib/loom-developer-domain-runtime/",
                )
            ):
                raise NodeAuthorityError("candidate bundle materialization readback is invalid")
        elif action == "install-client":
            _extract_client_archive(request.payload_bytes, stage)
            result = _run_fixed(
                (
                    "/usr/bin/python3",
                    "-I",
                    "-B",
                    str(SOURCE_ROOT / REMOTE_LINK_HOST_RELATIVE),
                    "install-client",
                    "--sandbox",
                    sandbox,
                    "--candidate-sha",
                    sha,
                    "--node",
                    str(payload["node"]),
                    "--credential-source",
                    str(stage),
                    "--worker-token-file",
                    str(stage / "worker-token"),
                    "--minio-access-key-file",
                    str(stage / "minio-access-key"),
                    "--minio-secret-key-file",
                    str(stage / "minio-secret-key"),
                    "--execute",
                ),
            )
        elif action == "attest":
            _extract_attestation_archive(request.payload_bytes, stage)
            seed = stage / "worker.env"
            fleet = stage / "fleet.json"
            result = _run_fixed(
                _domain_argv(
                    request,
                    "attest",
                    "--domain",
                    domain,
                    "--sandbox",
                    sandbox,
                    "--candidate-sha",
                    sha,
                    "--candidate-tree",
                    tree,
                    "--worker-env-seed",
                    str(seed),
                    "--fleet-attestation-seed",
                    str(fleet),
                    "--execute",
                ),
            )
        else:  # pragma: no cover - request parser owns this invariant
            raise NodeAuthorityError("node authority action is invalid")
        inner = result.get("journal") if action == "materialize" else result.get("receipt")
        return result, str(inner) if isinstance(inner, str) else None
    finally:
        try:
            _safe_root_directory(stage, mode=0o700)
            shutil.rmtree(stage)
        except OSError as exc:
            raise NodeAuthorityError("node authority stage cleanup failed safely") from exc


def _identity_inventory_sha256() -> str:
    passwd_rows = [
        {"name": entry.pw_name, "uid": entry.pw_uid, "gid": entry.pw_gid}
        for entry in sorted(
            pwd.getpwall(),
            key=lambda entry: (entry.pw_uid, entry.pw_name, entry.pw_gid),
        )
    ]
    group_rows = [
        {"name": entry.gr_name, "gid": entry.gr_gid}
        for entry in sorted(
            grp.getgrall(),
            key=lambda entry: (entry.gr_gid, entry.gr_name),
        )
    ]
    return hashlib.sha256(
        _canonical({"passwd": passwd_rows, "group": group_rows}),
    ).hexdigest()


def _identity_preflight(request: Request) -> dict[str, Any]:
    try:
        preflight = json.loads(request.payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:  # pragma: no cover
        raise NodeAuthorityError("developer identity preflight payload is invalid") from exc
    service_user = str(preflight["service_user"])
    service_group = str(preflight["service_group"])
    uid = int(preflight["uid"])
    gid = int(preflight["gid"])
    try:
        passwd_by_name = pwd.getpwnam(service_user)
    except KeyError:
        passwd_by_name = None
    try:
        passwd_by_uid = pwd.getpwuid(uid)
    except KeyError:
        passwd_by_uid = None
    try:
        group_by_name = grp.getgrnam(service_group)
    except KeyError:
        group_by_name = None
    try:
        group_by_gid = grp.getgrgid(gid)
    except KeyError:
        group_by_gid = None
    rows = (passwd_by_name, passwd_by_uid, group_by_name, group_by_gid)
    if all(row is None for row in rows):
        status = "available"
        passwd_name: str | None = None
        group_name: str | None = None
    elif (
        passwd_by_name is not None
        and passwd_by_uid is not None
        and group_by_name is not None
        and group_by_gid is not None
        and passwd_by_name.pw_name == service_user
        and passwd_by_name.pw_uid == uid
        and passwd_by_name.pw_gid == gid
        and passwd_by_uid.pw_name == service_user
        and passwd_by_uid.pw_uid == uid
        and passwd_by_uid.pw_gid == gid
        and group_by_name.gr_name == service_group
        and group_by_name.gr_gid == gid
        and group_by_gid.gr_name == service_group
        and group_by_gid.gr_gid == gid
    ):
        status = "exact-existing"
        passwd_name = service_user
        group_name = service_group
    else:
        raise NodeAuthorityError("developer identity preflight collision detected")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": IDENTITY_PREFLIGHT_RESULT_KIND,
        "node": request.payload["node"],
        "domain": request.payload["domain"],
        "env_id": preflight["env_id"],
        "service_user": service_user,
        "service_group": service_group,
        "uid": uid,
        "gid": gid,
        "status": status,
        "passwd_name": passwd_name,
        "group_name": group_name,
        "identity_inventory_sha256": _identity_inventory_sha256(),
        "checked_at": _timestamp(),
    }


def _identity_local_state(preflight: Mapping[str, Any]) -> str:
    service_user = str(preflight["service_user"])
    service_group = str(preflight["service_group"])
    uid = int(preflight["uid"])
    gid = int(preflight["gid"])
    try:
        passwd_by_name = pwd.getpwnam(service_user)
    except KeyError:
        passwd_by_name = None
    try:
        passwd_by_uid = pwd.getpwuid(uid)
    except KeyError:
        passwd_by_uid = None
    try:
        group_by_name = grp.getgrnam(service_group)
    except KeyError:
        group_by_name = None
    try:
        group_by_gid = grp.getgrgid(gid)
    except KeyError:
        group_by_gid = None
    if all(row is None for row in (passwd_by_name, passwd_by_uid, group_by_name, group_by_gid)):
        return "available"
    group_exact = (
        group_by_name is not None
        and group_by_gid is not None
        and group_by_name.gr_name == service_group
        and group_by_name.gr_gid == gid
        and group_by_gid.gr_name == service_group
        and group_by_gid.gr_gid == gid
        and not tuple(getattr(group_by_name, "gr_mem", ()))
        and not tuple(getattr(group_by_gid, "gr_mem", ()))
    )
    if group_exact and passwd_by_name is None and passwd_by_uid is None:
        return "group-only-exact"
    user_exact = (
        passwd_by_name is not None
        and passwd_by_uid is not None
        and passwd_by_name.pw_name == service_user
        and passwd_by_name.pw_uid == uid
        and passwd_by_name.pw_gid == gid
        and passwd_by_uid.pw_name == service_user
        and passwd_by_uid.pw_uid == uid
        and passwd_by_uid.pw_gid == gid
        and passwd_by_name.pw_dir == "/nonexistent"
        and passwd_by_uid.pw_dir == "/nonexistent"
        and passwd_by_name.pw_shell == "/usr/sbin/nologin"
        and passwd_by_uid.pw_shell == "/usr/sbin/nologin"
    )
    if group_exact and user_exact:
        return "exact-existing"
    raise NodeAuthorityError("developer identity convergence collision detected")


def _identity_transaction_path(request_id: str) -> Path:
    return IDENTITY_TRANSACTION_ROOT / f"{request_id}.json"


def _identity_transaction_read(request: Request) -> dict[str, Any] | None:
    path = _identity_transaction_path(request.request_id)
    try:
        raw = _safe_root_file(path, mode=0o600, limit=64 * 1024)
    except NodeAuthorityError:
        if not path.exists() and not path.is_symlink():
            return None
        raise
    try:
        transaction = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NodeAuthorityError("developer identity transaction is invalid") from exc
    preflight = json.loads(request.payload_bytes)
    if (
        not isinstance(transaction, dict)
        or set(transaction) != IDENTITY_TRANSACTION_FIELDS
        or raw != _canonical(transaction)
        or transaction.get("schema_version") != SCHEMA_VERSION
        or transaction.get("kind") != "loom.developer-environment.identity-convergence-transaction"
        or transaction.get("request_id") != request.request_id
        or transaction.get("payload_sha256") != request.payload["payload_sha256"]
        or transaction.get("node") != request.payload["node"]
        or transaction.get("domain") != request.payload["domain"]
        or transaction.get("env_id") != preflight["env_id"]
        or transaction.get("service_user") != preflight["service_user"]
        or transaction.get("service_group") != preflight["service_group"]
        or transaction.get("uid") != preflight["uid"]
        or transaction.get("gid") != preflight["gid"]
        or transaction.get("phase")
        not in {"prepared", "group-created", "user-created", "committed"}
        or not isinstance(transaction.get("created_at"), str)
        or not isinstance(transaction.get("updated_at"), str)
    ):
        raise NodeAuthorityError("developer identity transaction binding is invalid")
    return transaction


def _identity_transaction_write(
    request: Request,
    transaction: Mapping[str, Any],
    phase: str,
) -> dict[str, Any]:
    if phase not in {"prepared", "group-created", "user-created", "committed"}:
        raise NodeAuthorityError("developer identity transaction phase is invalid")
    _ensure_root_directory(
        IDENTITY_TRANSACTION_ROOT,
        mode=0o700,
        parent_mode=0o700,
    )
    now = _timestamp()
    if transaction:
        updated = {**transaction, "phase": phase, "updated_at": now}
    else:
        preflight = json.loads(request.payload_bytes)
        updated = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.identity-convergence-transaction",
            "request_id": request.request_id,
            "payload_sha256": request.payload["payload_sha256"],
            "node": request.payload["node"],
            "domain": request.payload["domain"],
            "env_id": preflight["env_id"],
            "service_user": preflight["service_user"],
            "service_group": preflight["service_group"],
            "uid": preflight["uid"],
            "gid": preflight["gid"],
            "phase": phase,
            "created_at": now,
            "updated_at": now,
        }
    _atomic_replace(
        _identity_transaction_path(request.request_id),
        _canonical(updated),
        0o600,
        parent_mode=0o700,
    )
    rebound = _identity_transaction_read(request)
    if rebound != updated:
        raise NodeAuthorityError("developer identity transaction publication drifted")
    return updated


def _run_identity_command(argv: Sequence[str]) -> None:
    try:
        completed = subprocess.run(
            tuple(argv),
            check=False,
            capture_output=True,
            timeout=30,
            env=_clean_env(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise NodeAuthorityError("developer identity command failed safely") from exc
    if completed.returncode != 0 or completed.stderr or completed.stdout:
        raise NodeAuthorityError("developer identity command failed safely")


def _identity_converge(request: Request) -> dict[str, Any]:
    preflight = json.loads(request.payload_bytes)
    transaction = _identity_transaction_read(request)
    if transaction is None:
        transaction = _identity_transaction_write(request, {}, "prepared")
    state = _identity_local_state(preflight)
    changed = transaction["phase"] != "committed" and state != "exact-existing"
    if transaction["phase"] == "committed":
        if state != "exact-existing":
            raise NodeAuthorityError("committed developer identity drifted")
    else:
        if state == "available":
            if transaction["phase"] != "prepared":
                raise NodeAuthorityError("developer identity transaction state regressed")
            _run_identity_command(
                (
                    "/usr/sbin/groupadd",
                    "--gid",
                    str(preflight["gid"]),
                    str(preflight["service_group"]),
                ),
            )
            transaction = _identity_transaction_write(
                request,
                transaction,
                "group-created",
            )
            state = _identity_local_state(preflight)
        if state == "group-only-exact":
            if transaction["phase"] == "prepared":
                transaction = _identity_transaction_write(
                    request,
                    transaction,
                    "group-created",
                )
            if transaction["phase"] != "group-created":
                raise NodeAuthorityError("developer identity transaction state drifted")
            _run_identity_command(
                (
                    "/usr/sbin/useradd",
                    "--uid",
                    str(preflight["uid"]),
                    "--gid",
                    str(preflight["service_group"]),
                    "--no-create-home",
                    "--home-dir",
                    "/nonexistent",
                    "--shell",
                    "/usr/sbin/nologin",
                    str(preflight["service_user"]),
                ),
            )
            transaction = _identity_transaction_write(
                request,
                transaction,
                "user-created",
            )
            state = _identity_local_state(preflight)
        if state != "exact-existing":
            raise NodeAuthorityError("developer identity did not converge")
        transaction = _identity_transaction_write(request, transaction, "committed")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": IDENTITY_CONVERGENCE_RESULT_KIND,
        "node": request.payload["node"],
        "domain": request.payload["domain"],
        "env_id": preflight["env_id"],
        "service_user": preflight["service_user"],
        "service_group": preflight["service_group"],
        "uid": preflight["uid"],
        "gid": preflight["gid"],
        "status": "exact-existing",
        "passwd_name": preflight["service_user"],
        "group_name": preflight["service_group"],
        "identity_inventory_sha256": _identity_inventory_sha256(),
        "transaction_sha256": hashlib.sha256(_canonical(transaction)).hexdigest(),
        "changed": changed,
        "completed_at": _timestamp(),
    }


def _identity_inventory(request: Request) -> dict[str, Any]:
    inventory = json.loads(request.payload_bytes)
    occupied = sorted(
        {
            identity
            for identity in (
                *(int(account.pw_uid) for account in pwd.getpwall()),
                *(int(account.pw_gid) for account in pwd.getpwall()),
                *(int(group.gr_gid) for group in grp.getgrall()),
            )
            if IDENTITY_UID_START <= identity <= IDENTITY_UID_END
        },
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": IDENTITY_INVENTORY_RESULT_KIND,
        "node": request.payload["node"],
        "domain": request.payload["domain"],
        "uid_start": inventory["uid_start"],
        "uid_end": inventory["uid_end"],
        "occupied_ids": occupied,
        "identity_inventory_sha256": _identity_inventory_sha256(),
        "checked_at": _timestamp(),
    }


def _execute_check(request: Request, policy: AuthorityPolicy) -> dict[str, Any]:
    payload = request.payload
    action = str(payload["action"])
    if action == "slurm-identity-preflight":
        local = _identity_preflight(request)
        accounting = _validated_slurm_identity_result(
            request,
            _run_fixed_input(
                _slurm_identity_policy_argv(request, "identity-check"),
                request.payload_bytes,
            ),
            operation="check",
        )
        local_status = str(local["status"])
        accounting_status = str(accounting["status"])
        if accounting_status == "retired":
            status = "retired"
        elif local_status == accounting_status == "exact-existing":
            status = "exact-existing"
        else:
            status = "available"
        return {
            **local,
            "status": status,
            "local_identity_status": local_status,
            "slurm_accounting_status": accounting_status,
            "slurm_accounting_receipt_sha256": hashlib.sha256(
                _canonical(accounting),
            ).hexdigest(),
            "owned_jobs": accounting["jobs"],
        }
    if action == "slurm-identity-inventory":
        return _identity_inventory(request)
    if action == "observe-platform-health-node":
        result = _run_fixed_input(
            _platform_health_authority_argv(request, "observe-node"),
            request.payload_bytes,
        )
        if (
            result.get("schema_version") != SCHEMA_VERSION
            or result.get("kind") != "loom.developer-sandbox.platform-health-node-observation"
            or result.get("session_id") != json.loads(request.payload_bytes).get("session_id")
            or result.get("node") != payload["node"]
            or result.get("host") != NODE_HOSTNAMES[str(payload["node"])]
            or result.get("checkpoint") != json.loads(request.payload_bytes).get("checkpoint")
            or result.get("orphan_container_ids") != []
        ):
            raise NodeAuthorityError("platform-health node readback is invalid")
        return result
    if action == "observe-live-overlap-job":
        result = _run_fixed_input(
            _live_authority_argv(request, "observe-slurm-job"),
            request.payload_bytes,
        )
        if (
            result.get("schema_version") != SCHEMA_VERSION
            or result.get("kind") != "loom.developer-sandbox.live-slurm-observation"
            or result.get("source_host")
            != ("trt-eai-oldlab-2" if payload["domain"] == "oldlab" else "trt-gb10-1")
            or result.get("sandbox") != payload["sandbox"]
            or result.get("pool") != payload["domain"]
            or result.get("candidate_sha") != payload["candidate_sha"]
        ):
            raise NodeAuthorityError("live overlap Slurm readback is invalid")
        return result
    if action == "staging-pressure-reclaim-observe":
        result = _run_fixed_input(
            _staging_pressure_authority_argv(request),
            request.payload_bytes,
        )
        inner = json.loads(request.payload_bytes)
        expected_fields = {
            "schema_version",
            "kind",
            "submit_host",
            "environment",
            "pool",
            "partition",
            "account",
            "qos",
            "phase",
            "session_id",
            "acceptance_session_id",
            "candidate_sha",
            "candidate_tree",
            "observed_at",
            "jobs",
            "snapshot_sha256",
        }
        jobs = result.get("jobs") if isinstance(result, dict) else None
        owned = {str(job["job_id"]): job for job in inner["owned_jobs"]}
        observed_owned = (
            {str(job["job_id"]): job for job in jobs if str(job.get("job_id")) in owned}
            if isinstance(jobs, list)
            else {}
        )
        unsigned = (
            {key: value for key, value in result.items() if key != "snapshot_sha256"}
            if isinstance(result, dict)
            else {}
        )
        if (
            set(result) != expected_fields
            or result.get("schema_version") != SCHEMA_VERSION
            or result.get("kind") != "loom.staging-pressure-reclaim.observe-result"
            or result.get("submit_host") != "trt-gb10-1"
            or result.get("environment") != "staging"
            or result.get("pool") != "gb10"
            or result.get("partition") != "gb10"
            or result.get("account") != "loom-staging"
            or result.get("qos") != "loom-staging"
            or any(
                result.get(field) != inner[field]
                for field in (
                    "phase",
                    "session_id",
                    "acceptance_session_id",
                    "candidate_sha",
                    "candidate_tree",
                )
            )
            or not isinstance(jobs, list)
            or any(
                not isinstance(job, dict)
                or set(job) != {"job_id", "user", "account", "qos", "state", "nodes", "name"}
                or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", str(job.get("job_id"))) is None
                or job.get("state")
                not in {
                    "PENDING",
                    "RUNNING",
                    "CONFIGURING",
                    "COMPLETING",
                    "SIGNALING",
                    "STAGE_OUT",
                    "STOPPED",
                    "SUSPENDED",
                    "RESIZING",
                }
                or any(
                    not isinstance(job.get(field), str) or "\n" in str(job[field])
                    for field in ("user", "account", "qos", "nodes", "name")
                )
                for job in jobs
            )
            or result.get("snapshot_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
            or (
                inner["phase"] == "before"
                and (
                    set(observed_owned) != set(owned)
                    or any(
                        any(
                            observed_owned[job_id][field] != expected[field]
                            for field in ("user", "account", "qos", "name")
                        )
                        for job_id, expected in owned.items()
                    )
                )
            )
            or (inner["phase"] != "before" and bool(observed_owned))
        ):
            raise NodeAuthorityError("staging pressure observation readback is invalid")
        return result
    if action == "slurm-check":
        _validate_slurm_candidate(request, policy)
        result = _run_fixed(_slurm_policy_argv(request, "node-check"))
        if (
            result.get("cluster") != SLURM_CLUSTER[str(payload["domain"])]
            or result.get("candidate_sha") != payload["candidate_sha"]
            or result.get("file_plan", {}).get("converged") is not True
            or result.get("live_readback", {}).get("converged") is not True
        ):
            raise NodeAuthorityError("Slurm policy check readback is invalid")
        return result
    if action == "inspect-link-client":
        return _run_fixed(
            (
                "/usr/bin/python3",
                "-I",
                "-B",
                str(SOURCE_ROOT / REMOTE_LINK_HOST_RELATIVE),
                "check-client",
                "--sandbox",
                str(payload["sandbox"]),
                "--candidate-sha",
                str(payload["candidate_sha"]),
                "--node",
                str(payload["node"]),
            ),
        )
    if action == "inspect-link-server":
        return _run_fixed(
            (
                "/usr/bin/python3",
                "-I",
                "-B",
                str(SOURCE_ROOT / REMOTE_LINK_HOST_RELATIVE),
                "check-server",
                "--sandbox",
                str(payload["sandbox"]),
                "--candidate-sha",
                str(payload["candidate_sha"]),
            ),
        )
    extra: tuple[str, ...] = ()
    if action == "export-runtime-proof-artifact":
        try:
            artifact_id = request.payload_bytes.decode("ascii")
        except UnicodeDecodeError as exc:
            raise NodeAuthorityError("runtime proof artifact identity is invalid") from exc
        fields = artifact_id.split("/")
        if (
            len(fields) != 7
            or fields[:2] != ["runtime-proof", "v1"]
            or fields[2] != payload["sandbox"]
            or fields[3] != payload["candidate_sha"]
            or fields[4] != payload["candidate_tree"]
            or fields[5] != "artifact"
            or fields[6] not in RUNTIME_PROOF_ARTIFACT_NAMES
            or RUNTIME_PROOF_ARTIFACT_SOURCES[fields[6]] != (payload["domain"], payload["node"])
        ):
            raise NodeAuthorityError("runtime proof artifact identity is invalid")
        extra = ("--artifact-id", artifact_id)
    return _run_fixed(
        _domain_argv(
            request,
            action,
            "--domain",
            str(payload["domain"]),
            "--sandbox",
            str(payload["sandbox"]),
            "--candidate-sha",
            str(payload["candidate_sha"]),
            "--candidate-tree",
            str(payload["candidate_tree"]),
            *extra,
        ),
    )


def dispatch(
    verb: str,
    raw: bytes,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    environment = os.environ if environ is None else environ
    _validate_invoker(verb, environment)
    descriptor = _open_lock(exclusive=verb == "transact")
    try:
        _reject_active_upgrade()
        policy = _read_policy()
        _validate_runtime_assets(policy)
        request = _parse_request(raw, verb=verb, policy=policy)
        if verb == "transact" and request.action in {
            ACCEPTANCE_PROBE_ACTION,
            RUNTIME_RETIRE_ACTION,
        }:
            result, _inner_receipt = _execute_request(request, policy)
            return {
                "schema_version": SCHEMA_VERSION,
                "request_id": request.request_id,
                "status": "succeeded",
                "action": request.action,
                "node": request.payload["node"],
                "domain": request.payload["domain"],
                "sandbox": request.payload["sandbox"],
                "candidate_sha": request.payload["candidate_sha"],
                "candidate_tree": request.payload["candidate_tree"],
                "payload_sha256": request.payload["payload_sha256"],
                "result": result,
                "result_sha256": hashlib.sha256(_canonical(result)).hexdigest(),
                "completed_at": str(result["completed_at"]),
            }
        if verb == "check":
            result = _execute_check(request, policy)
            return {
                "schema_version": SCHEMA_VERSION,
                "request_id": request.request_id,
                "status": "succeeded",
                "result": result,
            }
        existing = _read_receipt(request.request_id)
        if existing is not None:
            if not _journal_contains(existing):
                _append_journal(existing)
            return existing
        result, inner_receipt = _execute_request(request, policy)
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "request_id": request.request_id,
            "action": request.action,
            "node": request.payload["node"],
            "domain": request.payload["domain"],
            "sandbox": request.payload["sandbox"],
            "candidate_sha": request.payload["candidate_sha"],
            "candidate_tree": request.payload["candidate_tree"],
            "payload_sha256": request.payload["payload_sha256"],
            "result_sha256": hashlib.sha256(_canonical(result)).hexdigest(),
            "inner_receipt": inner_receipt,
            "completed_at": _timestamp(),
            "status": "succeeded",
        }
        if request.action in DYNAMIC_TARGET_ACTIONS:
            receipt.update(
                {field: request.payload[field] for field in DYNAMIC_TARGET_BINDING_FIELDS},
            )
        if request.action == REGISTRY_SNAPSHOT_SYNC_ACTION:
            receipt.update(
                {
                    "registry_generation": request.payload["registry_generation"],
                    "registry_payload_sha256": request.payload["registry_payload_sha256"],
                    "source_sha": policy.source_sha,
                    "source_tree": policy.source_tree,
                },
            )
        if request.action in DEPLOYMENT_TARGET_ACTIONS:
            receipt.update(
                {field: request.payload[field] for field in DEPLOYMENT_TARGET_BINDING_FIELDS},
            )
        _write_receipt(receipt)
        _append_journal(receipt)
        return receipt
    finally:
        os.close(descriptor)


def _require_persistent_root_view(
    *,
    root_path: Path = Path("/"),
    pid1_root_path: Path = Path("/proc/1/root"),
    pid1_comm_path: Path = Path("/proc/1/comm"),
) -> None:
    if os.getuid() != 0 or os.geteuid() != 0:
        raise NodeAuthorityError(
            "node authority bootstrap/upgrade requires persistent host-root authority",
        )
    try:
        root = root_path.stat()
        pid1_root = pid1_root_path.stat()
        pid1_comm = pid1_comm_path.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise NodeAuthorityError(
            "persistent host-root systemd view is unavailable",
        ) from exc
    if (root.st_dev, root.st_ino) != (pid1_root.st_dev, pid1_root.st_ino) or pid1_comm != "systemd":
        raise NodeAuthorityError("persistent host-root systemd view is invalid")


def _validate_persistent_root_source(
    source_sha: str,
    source_tree: str,
    *,
    root_path: Path = Path("/"),
    pid1_root_path: Path = Path("/proc/1/root"),
    pid1_comm_path: Path = Path("/proc/1/comm"),
) -> str:
    _require_persistent_root_view(
        root_path=root_path,
        pid1_root_path=pid1_root_path,
        pid1_comm_path=pid1_comm_path,
    )
    if not _is_sha(source_sha) or not _is_sha(source_tree):
        raise NodeAuthorityError("node authority candidate identity is invalid")
    if (
        _git("rev-parse", "--verify", "HEAD") != source_sha
        or _git("rev-parse", "--verify", "HEAD^{tree}") != source_tree
        or _git("status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise NodeAuthorityError("bootstrap source is not the clean exact candidate")
    return _node_for_hostname(_hostname())


def validate_install() -> dict[str, Any]:
    _require_persistent_root_view()
    policy = _read_policy()
    system_installs = _validate_runtime_assets(policy)
    return {
        "schema_version": SCHEMA_VERSION,
        "action": "validate-install",
        "node": policy.node,
        "source_sha": policy.source_sha,
        "source_tree": policy.source_tree,
        "system_installs": list(system_installs or ()),
        "status": "succeeded",
    }


def _ensure_upgrade_state() -> None:
    _ensure_root_directory(UPGRADE_ROOT, mode=0o700, parent_mode=0o700)
    for directory in (
        STAGING_INFRASTRUCTURE_PRODUCER_ROOT,
        STAGING_INFRASTRUCTURE_PRODUCER_RECEIPTS,
        STAGING_INFRASTRUCTURE_RECEIPT_ROOT,
        STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS,
    ):
        _ensure_root_directory(directory, mode=0o700, parent_mode=0o700)
    for lock in (
        STAGING_INFRASTRUCTURE_PRODUCER_LOCK,
        STAGING_INFRASTRUCTURE_INSTALL_LOCK,
    ):
        if not lock.exists():
            _atomic_install(lock, b"", 0o600, parent_mode=0o700)
        else:
            _safe_root_file(lock, mode=0o600)
    if not UPGRADE_JOURNAL.exists():
        _atomic_install(
            UPGRADE_JOURNAL,
            b"",
            0o600,
            parent_mode=0o700,
        )
    else:
        _validate_upgrade_journal()


def _recover_upgrade_if_needed() -> str | None:
    active = _read_upgrade_active()
    if active is None:
        return None
    snapshot, phase = active
    if phase == "committed":
        policy = _read_policy()
        _validate_runtime_assets(policy)
        if (
            policy.source_sha != snapshot.new_source_sha
            or policy.source_tree != snapshot.new_source_tree
        ):
            raise NodeAuthorityError("committed node authority upgrade identity drifted")
        if _high_value_state_identity() != snapshot.high_value_state:
            raise NodeAuthorityError(
                "committed node authority high-value state drifted",
            )
        _upgrade_journal_append(_upgrade_event(snapshot, "recovered-committed"))
        _remove_upgrade_active()
        return "committed"
    _restore_upgrade_snapshot(snapshot)
    if _high_value_state_identity() != snapshot.high_value_state:
        raise NodeAuthorityError(
            "rolled-back node authority high-value state drifted",
        )
    _upgrade_journal_append(_upgrade_event(snapshot, "recovered-rolled-back"))
    _remove_upgrade_active()
    return "rolled-back"


def upgrade(source_sha: str, source_tree: str) -> dict[str, Any]:
    node = _validate_persistent_root_source(source_sha, source_tree)
    assets = _exact_source_assets(source_sha, source_tree)
    descriptor = _open_lock(exclusive=True)
    try:
        _ensure_stage_root()
        _ensure_upgrade_state()
        recovered = _recover_upgrade_if_needed()
        old_policy = _read_policy()
        old_system_installs = _validate_runtime_assets(
            old_policy,
            allow_absent_system_install=True,
        )
        if old_policy.node != node:
            raise NodeAuthorityError("node authority upgrade host binding drifted")
        old_policy_generation = _policy_asset_generation(old_policy.asset_sha256)
        system_install_complete = old_system_installs is None or len(old_system_installs) == len(
            SYSTEM_INSTALL_ASSETS
        )
        if (
            old_policy_generation == "current"
            and old_policy.source_sha == source_sha
            and old_policy.source_tree == source_tree
            and system_install_complete
        ):
            _systemd_enable_recovery_timer(start=True)
            _validate_recovery_timer(require_active=True)
            return {
                "schema_version": SCHEMA_VERSION,
                "action": "upgrade",
                "node": node,
                "source_sha": old_policy.source_sha,
                "source_tree": old_policy.source_tree,
                "changed": False,
                "recovered": recovered,
                "system_installs": list(old_system_installs or ()),
                "status": "succeeded",
            }
        high_value_before = _high_value_state_identity()
        snapshot = _prepare_upgrade_snapshot(
            old_policy,
            new_source_sha=source_sha,
            new_source_tree=source_tree,
            high_value_state=high_value_before,
        )
        created_upgrade_directories: list[Path] = []
        try:
            _write_upgrade_active(snapshot, "prepared")
            _upgrade_journal_append(_upgrade_event(snapshot, "prepared"))
            _unlink_root_file(SUDOERS, mode=0o440)
            for sudoers_path in _system_sudoers_paths():
                if sudoers_path.exists() or sudoers_path.is_symlink():
                    _unlink_root_file(sudoers_path, mode=0o440)
            _write_upgrade_active(snapshot, "admission-disabled")
            _upgrade_journal_append(
                _upgrade_event(snapshot, "admission-disabled"),
            )
            for relative_parent in SOURCE_ASSET_PARENT_PATHS:
                directory = SOURCE_ROOT / relative_parent
                if _ensure_root_directory(
                    directory,
                    mode=0o755,
                    parent_mode=0o755,
                ):
                    created_upgrade_directories.append(directory)
            for relative in SOURCE_ASSETS:
                _atomic_replace(
                    SOURCE_ROOT / relative,
                    assets[str(relative)],
                    _source_asset_mode(relative),
                )
            _retire_legacy_source_assets(old_policy)
            authority_payload = assets["scripts/ops/developer_sandbox_node_authority.py"]
            _atomic_replace(LIBEXEC, authority_payload, 0o755)
            _atomic_replace(
                POLICY,
                _policy_payload(source_sha, source_tree, node, assets),
                0o600,
            )
            system_installs = _system_install_assets(
                assets,
                replace=True,
            )
            _write_upgrade_active(snapshot, "assets-replaced")
            _upgrade_journal_append(_upgrade_event(snapshot, "assets-replaced"))
            _validate_sudoers(
                SOURCE_ROOT / SUDOERS_RELATIVE,
                label="upgraded node authority source",
            )
            _atomic_install(
                SUDOERS,
                assets[str(SUDOERS_RELATIVE)],
                0o440,
            )
            _validate_sudoers(SUDOERS, label="upgraded node authority")
            installed = _read_policy()
            _validate_runtime_assets(installed)
            if (
                installed.source_sha != source_sha
                or installed.source_tree != source_tree
                or installed.node != node
            ):
                raise NodeAuthorityError("upgraded node authority identity is invalid")
            if _high_value_state_identity() != high_value_before:
                raise NodeAuthorityError(
                    "node authority high-value state changed during upgrade",
                )
            _write_upgrade_active(snapshot, "committed")
            _upgrade_journal_append(_upgrade_event(snapshot, "committed"))
            _remove_upgrade_active()
            _systemd_enable_recovery_timer(start=True)
            _validate_recovery_timer(require_active=True)
            return {
                "schema_version": SCHEMA_VERSION,
                "action": "upgrade",
                "node": node,
                "previous_source_sha": old_policy.source_sha,
                "previous_source_tree": old_policy.source_tree,
                "source_sha": source_sha,
                "source_tree": source_tree,
                "changed": True,
                "recovered": recovered,
                "snapshot": str(snapshot.root),
                "system_installs": list(system_installs),
                "status": "succeeded",
            }
        except Exception as upgrade_exc:
            try:
                _restore_upgrade_snapshot(snapshot)
                if _high_value_state_identity() != high_value_before:
                    raise NodeAuthorityError(
                        "node authority high-value state changed during rollback",
                    )
                _upgrade_journal_append(_upgrade_event(snapshot, "rolled-back"))
                _remove_upgrade_active()
                for directory in reversed(created_upgrade_directories):
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
            except Exception as rollback_exc:
                try:
                    if SUDOERS.exists() and not SUDOERS.is_symlink():
                        _unlink_root_file(SUDOERS, mode=0o440)
                except Exception:
                    pass
                raise NodeAuthorityError(
                    "node authority upgrade and rollback both failed safely",
                ) from rollback_exc
            raise NodeAuthorityError(
                "node authority upgrade failed and was rolled back",
            ) from upgrade_exc
    finally:
        os.close(descriptor)


def bootstrap(source_sha: str, source_tree: str) -> dict[str, Any]:
    node = _validate_persistent_root_source(source_sha, source_tree)
    assets = _exact_source_assets(source_sha, source_tree)
    created_directories: list[Path] = []
    created_files: list[Path] = []
    preexisting_system_paths = {
        target
        for _relative, target, _mode, _parent_mode in SYSTEM_INSTALL_ASSETS
        if target.exists() or target.is_symlink()
    }
    try:
        for directory, mode, parent_mode in BOOTSTRAP_DIRECTORIES:
            if _ensure_root_directory(
                directory,
                mode=mode,
                parent_mode=parent_mode,
            ):
                created_directories.append(directory)
        for relative in SOURCE_ASSETS:
            mode = (
                0o440
                if relative == SUDOERS_RELATIVE
                else (0o755 if relative.parts[:2] == ("scripts", "ops") else 0o644)
            )
            target = SOURCE_ROOT / relative
            if _atomic_install(target, assets[str(relative)], mode):
                created_files.append(target)
        authority_payload = assets["scripts/ops/developer_sandbox_node_authority.py"]
        if _atomic_install(LIBEXEC, authority_payload, 0o755):
            created_files.append(LIBEXEC)
        policy_payload = _policy_payload(source_sha, source_tree, node, assets)
        if _atomic_install(POLICY, policy_payload, 0o600):
            created_files.append(POLICY)
        system_installs = _system_install_assets(
            assets,
            replace=False,
        )
        for path in (
            LOCK,
            JOURNAL,
            UPGRADE_JOURNAL,
            STAGING_INFRASTRUCTURE_PRODUCER_LOCK,
            STAGING_INFRASTRUCTURE_INSTALL_LOCK,
        ):
            if _atomic_install(path, b"", 0o600, parent_mode=0o700):
                created_files.append(path)
        source_sudoers = SOURCE_ROOT / SUDOERS_RELATIVE
        _validate_sudoers(source_sudoers, label="node authority source")
        if _atomic_install(SUDOERS, assets[str(SUDOERS_RELATIVE)], 0o440):
            created_files.append(SUDOERS)
        _validate_sudoers(SUDOERS, label="installed node authority")
        policy = _read_policy()
        _validate_runtime_assets(policy)
        _systemd_enable_recovery_timer(start=True)
        _validate_recovery_timer(require_active=True)
        return {
            "schema_version": SCHEMA_VERSION,
            "action": "bootstrap",
            "node": node,
            "source_sha": source_sha,
            "source_tree": source_tree,
            "system_installs": list(system_installs),
            "status": "succeeded",
        }
    except Exception:
        if SLURM_RECOVERY_TIMER not in preexisting_system_paths:
            try:
                _systemd_disable_recovery_timer()
            except Exception:
                pass
        for _relative, target, mode, parent_mode in reversed(SYSTEM_INSTALL_ASSETS):
            if target in preexisting_system_paths:
                continue
            try:
                if target.exists() or target.is_symlink():
                    _unlink_root_file(
                        target,
                        mode=mode,
                        parent_mode=parent_mode,
                    )
            except OSError:
                pass
        for path in reversed(created_files):
            try:
                path.unlink()
            except OSError:
                pass
        for path in reversed(created_directories):
            try:
                path.rmdir()
            except OSError:
                pass
        try:
            _systemd_daemon_reload()
        except Exception:
            pass
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("transact", allow_abbrev=False)
    subparsers.add_parser("check", allow_abbrev=False)
    subparsers.add_parser("validate-install", allow_abbrev=False)
    for command in ("bootstrap", "upgrade"):
        install = subparsers.add_parser(command, allow_abbrev=False)
        install.add_argument("--candidate-sha", required=True)
        install.add_argument("--candidate-tree", required=True)
        install.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command in {"bootstrap", "upgrade"}:
            if not args.execute:
                result = {
                    "schema_version": SCHEMA_VERSION,
                    "action": args.command,
                    "mutation_authorized": False,
                    "candidate_sha": args.candidate_sha,
                    "candidate_tree": args.candidate_tree,
                    "persistent_host_root_required": True,
                    "supported_root_channels": ["direct", "docker-chroot"],
                }
            else:
                result = (
                    bootstrap(args.candidate_sha, args.candidate_tree)
                    if args.command == "bootstrap"
                    else upgrade(args.candidate_sha, args.candidate_tree)
                )
        elif args.command == "validate-install":
            result = validate_install()
        else:
            result = dispatch(args.command, _read_all_stdin())
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except NodeAuthorityError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except OSError:
        sys.stderr.write("error: node authority filesystem operation failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
