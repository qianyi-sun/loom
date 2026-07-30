#!/usr/bin/env python3
"""Check or render the fixed staging OLDLAB capacity promotion.

This is a repository-only boundary.  It reads the checked-in staging profile,
the checked-in OLDLAB shared-capacity policy, and the fixed root authority
pointer.  It never executes a system command, writes a file, applies
environment state, or starts a supervisor.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
import sys
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

if __package__ in {None, ""}:  # Direct ``python scripts/ops/...`` invocation.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ops import developer_sandbox_live_acceptance as live_acceptance
from scripts.ops import developer_sandbox_platform_health_authority as health_authority
from scripts.ops.developer_sandbox_capacity_contract import (
    CapacityContractError,
    load_capacity_policy,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
STAGING_PROFILE = REPO_ROOT / "deploy/environment-state/staging.toml"
POLICY_SOURCE = "deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml"
AUTHORITY_ROOT = Path("/var/lib/loom-developer-sandbox-platform-health-authority")
AUTHORITY_CURRENT = AUTHORITY_ROOT / "current.json"
OLDLAB_NODES = tuple(f"trt-eai-oldlab-{index}" for index in range(1, 6))
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
SESSION_RE = re.compile(r"^[0-9a-f]{32}$")

PROMOTION_FIELDS = {
    "enabled",
    "authority_current_path",
    "evidence_path",
    "evidence_payload_sha256",
    "evidence_session_id",
    "evidence_candidates_sha256",
    "evidence_expires_at",
    "policy_source",
    "policy_source_sha256",
    "recommendation_sha256",
    "require_external_allocation_authority",
}
PROMOTED_VALUE_FIELDS = {
    "max_slots",
    "requested_cpus",
    "requested_memory_mib",
    "requested_concurrency",
    "max_jobs",
    "pending_job_cap",
    "container_cpus",
    "container_memory_mib",
    "container_pids",
    "job_pids_max",
    "exclusive",
    "external_runner",
    "shared_capacity_managed",
    "gpu_tres",
}
PROMOTED_ACTUATOR_FIELDS = PROMOTED_VALUE_FIELDS - {"max_slots"}
DISABLED_REASON = (
    "gated on #896 container isolation + double-duty headroom measurement; "
    "exclusive=true is not viable on nodes shared with k3s/MinIO, so the "
    "disable IS the safety gate"
)
DISABLED_ENV_TEMPLATE_GLOB = "/srv/loom/staging-shared/generated/staging-gb10-worker-staging-*.env"
PROMOTED_ENV_TEMPLATE_GLOB = "/srv/loom/staging-shared/generated/staging-*-worker-staging-*.env"
PROMOTED_OLDLAB_ENV_FILE = (
    "/srv/loom/staging-shared/generated/staging-oldlab-worker-${IMAGE_TAG}.env"
)
DISABLED_OLDLAB_POLICY_COMMENT = """# GATED: enabled=false until #896 lands. OLDLAB nodes (trt-eai-oldlab-1..5) also
# run the k3s control plane + MinIO/Longhorn, so workers must be non-exclusive
# (cannot claim a whole node) and capped with headroom for k8s/MinIO. The
# resource values below are PROVISIONAL placeholders reduced from the historical
# single-tenant profile; #896 must measure real double-duty headroom and set the
# final values before flipping enabled=true.
"""
DISABLED_OLDLAB_SUPERVISOR_COMMENT = """# OLDLAB staging autoscaler supervisor — GATED enabled=false/active=false until
# #896 (container isolation) + #827 (external-Slurm acceptance), mirroring the
# oldlab pool's own disable above. Applying writes the unit files but does NOT
# enable or start the timer. Port 15448 per the reserved oldlab-staging slot.
"""
PROMOTED_OLDLAB_SUPERVISOR_COMMENT = """# Evidence-bound OLDLAB staging supervisor. Repository enablement is inert until
# the independent runtime authority verifies fresh exact-candidate evidence.
"""


class PromotionError(ValueError):
    """The repository promotion is absent, stale, or inconsistent."""


@dataclass(frozen=True)
class PromotionEvidence:
    session_id: str
    evidence_path: str
    payload_sha256: str
    candidates_sha256: str
    expires_at: str
    policy_sha256: str
    recommendation_sha256: str
    values: Mapping[str, Any]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value or value.endswith(("+00:00", "-00:00")):
        raise PromotionError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PromotionError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PromotionError(f"{label} is invalid")
    return parsed.astimezone(UTC)


def _load_toml(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        parsed = tomllib.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise PromotionError(f"{label} is invalid") from exc
    if not isinstance(parsed, dict):
        raise PromotionError(f"{label} is invalid")
    return parsed, raw


def _policy_contract() -> tuple[dict[str, Any], str]:
    try:
        contract = load_capacity_policy(
            REPO_ROOT,
            "oldlab",
            expected_nodes=OLDLAB_NODES,
        )
    except CapacityContractError as exc:
        raise PromotionError("OLDLAB capacity policy contract is invalid") from exc
    values = dict(contract.values)
    if set(values) != PROMOTED_VALUE_FIELDS or contract.source != POLICY_SOURCE:
        raise PromotionError("OLDLAB capacity policy values are invalid")
    return values, contract.source_sha256


def _validate_candidates(value: object) -> str:
    if (
        not isinstance(value, dict)
        or len(value) < 2
        or any(not isinstance(environment, str) or not environment for environment in value)
    ):
        raise PromotionError("platform-health candidate set is invalid")
    shas: list[str] = []
    for environment in sorted(value):
        candidate = value[environment]
        if (
            not isinstance(candidate, dict)
            or set(candidate) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidate.get("sha"))) is None
            or SHA_RE.fullmatch(str(candidate.get("tree"))) is None
        ):
            raise PromotionError("platform-health candidate set is invalid")
        shas.append(str(candidate["sha"]))
    if len(set(shas)) != len(shas):
        raise PromotionError("platform-health candidate set is invalid")
    return _digest(value)


def _load_promotion_evidence(
    *,
    now: datetime | None = None,
) -> PromotionEvidence:
    try:
        current, _current_raw = health_authority._secure_json(
            AUTHORITY_CURRENT,
            label="platform-health current pointer",
        )
    except health_authority.PlatformHealthError as exc:
        raise PromotionError("platform-health current pointer is unavailable or unsafe") from exc
    if (
        not isinstance(current, dict)
        or set(current) != {"schema_version", "session_id", "evidence_path", "payload_sha256"}
        or current.get("schema_version") != 1
        or SESSION_RE.fullmatch(str(current.get("session_id"))) is None
        or DIGEST_RE.fullmatch(str(current.get("payload_sha256"))) is None
    ):
        raise PromotionError("platform-health current pointer is invalid")
    session_id = str(current["session_id"])
    evidence_path = AUTHORITY_ROOT / "sessions" / session_id / "evidence.json"
    if current.get("evidence_path") != str(evidence_path):
        raise PromotionError("platform-health current pointer path is invalid")
    try:
        evidence, _evidence_raw = health_authority._secure_json(
            evidence_path,
            label="platform-health promotion evidence",
        )
    except health_authority.PlatformHealthError as exc:
        raise PromotionError("platform-health evidence is unavailable or unsafe") from exc
    if not isinstance(evidence, dict):
        raise PromotionError("platform-health evidence is invalid")
    registry_snapshot = evidence.get("registry_snapshot")
    if not isinstance(registry_snapshot, dict):
        raise PromotionError("platform-health registry snapshot is invalid")
    candidates = evidence.get("candidates")
    candidates_sha256 = _validate_candidates(candidates)
    candidate_mapping = cast(Mapping[str, Any], candidates)
    try:
        live_acceptance._validate_platform_health_authority(
            evidence,
            session_id=session_id,
            registry_snapshot=registry_snapshot,
            candidates=candidate_mapping,
        )
    except live_acceptance.AcceptanceError as exc:
        raise PromotionError("platform-health evidence is invalid") from exc
    payload_sha256 = str(evidence.get("payload_sha256"))
    if payload_sha256 != current["payload_sha256"]:
        raise PromotionError("platform-health current pointer digest drifted")
    completed_at = _timestamp(evidence.get("completed_at"), label="evidence completed_at")
    expires_at = _timestamp(evidence.get("expires_at"), label="evidence expires_at")
    observed_now = (now or datetime.now(UTC)).astimezone(UTC)
    if completed_at > observed_now or observed_now >= expires_at:
        raise PromotionError("platform-health evidence is not currently fresh")
    recommendation = evidence.get("oldlab_capacity_recommendation")
    values, policy_sha256 = _policy_contract()
    if (
        not isinstance(recommendation, dict)
        or set(recommendation)
        != {"schema_version", "pool", "source", "source_sha256", "values", "derivation"}
        or recommendation.get("schema_version") != 1
        or recommendation.get("pool") != "oldlab"
        or recommendation.get("source") != POLICY_SOURCE
        or recommendation.get("source_sha256") != policy_sha256
        or recommendation.get("values") != evidence.get("policy_capacity", {}).get("oldlab")
        or any(recommendation["values"].get(field) != values[field] for field in values)
    ):
        raise PromotionError("OLDLAB recommendation does not match the checked-in policy")
    return PromotionEvidence(
        session_id=session_id,
        evidence_path=str(evidence_path),
        payload_sha256=payload_sha256,
        candidates_sha256=candidates_sha256,
        expires_at=str(evidence["expires_at"]),
        policy_sha256=policy_sha256,
        recommendation_sha256=_digest(recommendation),
        values=values,
    )


def _oldlab_rows(profile: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    policies = profile.get("worker_pool_autoscaler_policies")
    supervisors = profile.get("external_slurm_autoscaler_supervisors")
    if not isinstance(policies, list) or not isinstance(supervisors, list):
        raise PromotionError("staging OLDLAB desired state is invalid")
    oldlab_policies = [
        row for row in policies if isinstance(row, dict) and row.get("pool_name") == "oldlab"
    ]
    oldlab_supervisors = [
        row
        for row in supervisors
        if isinstance(row, dict)
        and row.get("name") == "oldlab-staging"
        and row.get("pool_name") == "oldlab"
    ]
    if len(oldlab_policies) != 1 or len(oldlab_supervisors) != 1:
        raise PromotionError("staging OLDLAB desired state is not unique")
    return oldlab_policies[0], oldlab_supervisors[0]


def _validate_disabled(profile: Mapping[str, Any]) -> None:
    promotion = profile.get("oldlab_capacity_promotion")
    policy, supervisor = _oldlab_rows(profile)
    actuator = policy.get("actuator_config")
    if (
        not isinstance(promotion, dict)
        or set(promotion) != PROMOTION_FIELDS
        or promotion
        != {
            "enabled": False,
            "authority_current_path": str(AUTHORITY_CURRENT),
            "evidence_path": "",
            "evidence_payload_sha256": "",
            "evidence_session_id": "",
            "evidence_candidates_sha256": "",
            "evidence_expires_at": "",
            "policy_source": POLICY_SOURCE,
            "policy_source_sha256": "",
            "recommendation_sha256": "",
            "require_external_allocation_authority": True,
        }
        or set(policy)
        != {
            "pool_name",
            "actuator",
            "enabled",
            "disabled_reason",
            "min_slots",
            "max_slots",
            "scale_up_threshold_slots",
            "scale_down_idle_seconds",
            "scale_up_cooldown_seconds",
            "scale_down_cooldown_seconds",
            "drain_timeout_seconds",
            "force",
            "actuator_config",
        }
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not False
        or policy.get("disabled_reason") != DISABLED_REASON
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != 4
        or policy.get("scale_up_threshold_slots") != 1
        or policy.get("scale_down_idle_seconds") != 600
        or policy.get("scale_up_cooldown_seconds") != 60
        or policy.get("scale_down_cooldown_seconds") != 300
        or policy.get("drain_timeout_seconds") != 600
        or policy.get("force") is not False
        or not isinstance(actuator, dict)
        or set(actuator)
        != {
            "backend",
            "cpu_arch",
            "partition",
            "allowed_nodes",
            "env_file",
            "repo_dir",
            "requested_cpus",
            "requested_memory_mib",
            "requested_concurrency",
            "max_jobs",
            "pending_job_cap",
            "time_limit",
            "exclusive",
            "external_runner",
        }
        or actuator.get("requested_cpus") != 4
        or actuator.get("requested_memory_mib") != 16000
        or actuator.get("requested_concurrency") != 2
        or actuator.get("max_jobs") != 2
        or actuator.get("pending_job_cap") != 1
        or actuator.get("backend") != "docker"
        or actuator.get("cpu_arch") != "x86_64"
        or actuator.get("partition") != ""
        or actuator.get("allowed_nodes") != list(OLDLAB_NODES)
        or actuator.get("env_file")
        != "/var/lib/loom-staging-rollout/generated/staging-oldlab-worker-${IMAGE_TAG}.env"
        or actuator.get("repo_dir")
        != "/srv/loom/staging-shared/candidates/loom-remote-worker-${IMAGE_TAG}"
        or actuator.get("time_limit") != "2-00:00:00"
        or actuator.get("exclusive") is not False
        or actuator.get("external_runner") is not True
        or supervisor.get("enabled") is not False
        or supervisor.get("active") is not False
    ):
        raise PromotionError("disabled OLDLAB state is not the fixed fail-closed profile")


def _validate_disabled_external_base(profile: Mapping[str, Any]) -> None:
    prerequisites = profile.get("external_slurm_runner_prerequisites")
    if (
        not isinstance(prerequisites, dict)
        or set(prerequisites)
        != {
            "pools",
            "expected_repo_ref",
            "require_clean_repo",
            "require_worker_token_parity",
            "materialize",
            "require_external_allocation_authority",
            "env_template_glob",
        }
        or prerequisites.get("pools") != ["gb10"]
        or prerequisites.get("expected_repo_ref") != "${IMAGE_TAG}"
        or prerequisites.get("require_clean_repo") is not True
        or prerequisites.get("require_worker_token_parity") is not True
        or prerequisites.get("materialize") is not True
        or prerequisites.get("require_external_allocation_authority") is not True
        or prerequisites.get("env_template_glob") != DISABLED_ENV_TEMPLATE_GLOB
    ):
        raise PromotionError("disabled external-allocation base is not exact")


def _validate_external_authority(profile: Mapping[str, Any]) -> None:
    prerequisites = profile.get("external_slurm_runner_prerequisites")
    if (
        not isinstance(prerequisites, dict)
        or set(prerequisites)
        != {
            "pools",
            "expected_repo_ref",
            "require_clean_repo",
            "require_worker_token_parity",
            "materialize",
            "require_external_allocation_authority",
            "env_template_glob",
        }
        or prerequisites.get("expected_repo_ref") != "${IMAGE_TAG}"
        or prerequisites.get("require_clean_repo") is not True
        or prerequisites.get("require_worker_token_parity") is not True
        or prerequisites.get("materialize") is not True
        or prerequisites.get("require_external_allocation_authority") is not True
        or not isinstance(prerequisites.get("pools"), list)
        or prerequisites["pools"] != ["gb10", "oldlab"]
        or prerequisites.get("env_template_glob") != PROMOTED_ENV_TEMPLATE_GLOB
    ):
        raise PromotionError(
            "OLDLAB external-allocation authority is not wired in staging",
        )


def _expected_promotion(evidence: PromotionEvidence) -> dict[str, Any]:
    return {
        "enabled": True,
        "authority_current_path": str(AUTHORITY_CURRENT),
        "evidence_path": evidence.evidence_path,
        "evidence_payload_sha256": evidence.payload_sha256,
        "evidence_session_id": evidence.session_id,
        "evidence_candidates_sha256": evidence.candidates_sha256,
        "evidence_expires_at": evidence.expires_at,
        "policy_source": POLICY_SOURCE,
        "policy_source_sha256": evidence.policy_sha256,
        "recommendation_sha256": evidence.recommendation_sha256,
        "require_external_allocation_authority": True,
    }


def _offline_binding_evidence(
    promotion: Mapping[str, Any],
) -> PromotionEvidence:
    values, policy_sha256 = _policy_contract()
    session_id = promotion.get("evidence_session_id")
    evidence_path = promotion.get("evidence_path")
    payload_sha256 = promotion.get("evidence_payload_sha256")
    candidates_sha256 = promotion.get("evidence_candidates_sha256")
    expires_at = promotion.get("evidence_expires_at")
    recommendation_sha256 = promotion.get("recommendation_sha256")
    if (
        set(promotion) != PROMOTION_FIELDS
        or promotion.get("enabled") is not True
        or promotion.get("authority_current_path") != str(AUTHORITY_CURRENT)
        or SESSION_RE.fullmatch(str(session_id)) is None
        or evidence_path != str(AUTHORITY_ROOT / "sessions" / str(session_id) / "evidence.json")
        or DIGEST_RE.fullmatch(str(payload_sha256)) is None
        or DIGEST_RE.fullmatch(str(candidates_sha256)) is None
        or promotion.get("policy_source") != POLICY_SOURCE
        or promotion.get("policy_source_sha256") != policy_sha256
        or DIGEST_RE.fullmatch(str(recommendation_sha256)) is None
        or promotion.get("require_external_allocation_authority") is not True
    ):
        raise PromotionError("enabled OLDLAB provenance binding is invalid")
    _timestamp(expires_at, label="evidence expires_at")
    return PromotionEvidence(
        session_id=str(session_id),
        evidence_path=str(evidence_path),
        payload_sha256=str(payload_sha256),
        candidates_sha256=str(candidates_sha256),
        expires_at=str(expires_at),
        policy_sha256=policy_sha256,
        recommendation_sha256=str(recommendation_sha256),
        values=values,
    )


def _validate_enabled(
    profile: Mapping[str, Any],
    evidence: PromotionEvidence,
) -> None:
    promotion = profile.get("oldlab_capacity_promotion")
    policy, supervisor = _oldlab_rows(profile)
    actuator = policy.get("actuator_config")
    _validate_external_authority(profile)
    if (
        promotion != _expected_promotion(evidence)
        or set(policy)
        != {
            "pool_name",
            "actuator",
            "enabled",
            "min_slots",
            "max_slots",
            "scale_up_threshold_slots",
            "scale_down_idle_seconds",
            "scale_up_cooldown_seconds",
            "scale_down_cooldown_seconds",
            "drain_timeout_seconds",
            "force",
            "actuator_config",
        }
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not True
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != evidence.values["max_slots"]
        or policy.get("scale_up_threshold_slots") != 1
        or policy.get("scale_down_idle_seconds") != 600
        or policy.get("scale_up_cooldown_seconds") != 60
        or policy.get("scale_down_cooldown_seconds") != 300
        or policy.get("drain_timeout_seconds") != 600
        or policy.get("force") is not False
        or not isinstance(actuator, dict)
        or any(actuator.get(field) != evidence.values[field] for field in PROMOTED_ACTUATOR_FIELDS)
        or {str(node).lower() for node in actuator.get("allowed_nodes", ())} != set(OLDLAB_NODES)
        or actuator.get("backend") != "docker"
        or actuator.get("cpu_arch") != "x86_64"
        or actuator.get("partition") != ""
        or actuator.get("env_file") != PROMOTED_OLDLAB_ENV_FILE
        or actuator.get("repo_dir")
        != "/srv/loom/staging-shared/candidates/loom-remote-worker-${IMAGE_TAG}"
        or actuator.get("time_limit") != "02:00:00"
        or actuator.get("slurm_account") != "loom-staging"
        or actuator.get("qos_normal") != "loom-staging"
        or actuator.get("candidate_sha") != "${GIT_SHA}"
        or supervisor.get("enabled") is not True
        or supervisor.get("active") is not True
    ):
        raise PromotionError("enabled OLDLAB state does not match fresh fixed evidence")


def check_profile(
    profile: Mapping[str, Any],
) -> str:
    promotion = profile.get("oldlab_capacity_promotion")
    if not isinstance(promotion, dict) or set(promotion) != PROMOTION_FIELDS:
        raise PromotionError("OLDLAB promotion binding is invalid")
    if promotion.get("enabled") is False:
        _validate_disabled(profile)
        return "disabled_fail_closed"
    if promotion.get("enabled") is not True:
        raise PromotionError("OLDLAB promotion state is invalid")
    evidence = _offline_binding_evidence(promotion)
    _validate_enabled(profile, evidence)
    return "enabled_evidence_bound"


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _promotion_block(evidence: PromotionEvidence) -> str:
    values = _expected_promotion(evidence)
    return "\n".join(
        (
            "[oldlab_capacity_promotion]",
            "enabled = true",
            f"authority_current_path = {_toml_string(values['authority_current_path'])}",
            f"evidence_path = {_toml_string(values['evidence_path'])}",
            f"evidence_payload_sha256 = {_toml_string(values['evidence_payload_sha256'])}",
            f"evidence_session_id = {_toml_string(values['evidence_session_id'])}",
            f"evidence_candidates_sha256 = {_toml_string(values['evidence_candidates_sha256'])}",
            f"evidence_expires_at = {_toml_string(values['evidence_expires_at'])}",
            f"policy_source = {_toml_string(values['policy_source'])}",
            f"policy_source_sha256 = {_toml_string(values['policy_source_sha256'])}",
            f"recommendation_sha256 = {_toml_string(values['recommendation_sha256'])}",
            "require_external_allocation_authority = true",
            "",
        ),
    )


def _oldlab_policy_block(evidence: PromotionEvidence) -> str:
    values = evidence.values
    nodes = "\n".join(f'  "{node}",' for node in OLDLAB_NODES)
    return f"""# Evidence-bound OLDLAB staging capacity. This row may be enabled only while
# oldlab_capacity_promotion check exact-matches the fixed root authority.
[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = {values["max_slots"]}
scale_up_threshold_slots = 1
scale_down_idle_seconds = 600
scale_up_cooldown_seconds = 60
scale_down_cooldown_seconds = 300
drain_timeout_seconds = 600
force = false

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "x86_64"
partition = ""
allowed_nodes = [
{nodes}
]
env_file = "{PROMOTED_OLDLAB_ENV_FILE}"
repo_dir = "/srv/loom/staging-shared/candidates/loom-remote-worker-${{IMAGE_TAG}}"
requested_cpus = {values["requested_cpus"]}
requested_memory_mib = {values["requested_memory_mib"]}
requested_concurrency = {values["requested_concurrency"]}
max_jobs = {values["max_jobs"]}
pending_job_cap = {values["pending_job_cap"]}
time_limit = "02:00:00"
exclusive = false
external_runner = true
shared_capacity_managed = true
slurm_account = "loom-staging"
qos_normal = "loom-staging"
container_cpus = {values["container_cpus"]}
container_memory_mib = {values["container_memory_mib"]}
container_pids = {values["container_pids"]}
job_pids_max = {values["job_pids_max"]}
candidate_sha = "${{GIT_SHA}}"
gpu_tres = {_toml_string(str(values["gpu_tres"]))}

"""


def _external_prerequisite_block() -> str:
    return f"""[external_slurm_runner_prerequisites]
pools = ["gb10", "oldlab"]
expected_repo_ref = "${{IMAGE_TAG}}"
require_clean_repo = true
require_worker_token_parity = true
materialize = true
require_external_allocation_authority = true
env_template_glob = "{PROMOTED_ENV_TEMPLATE_GLOB}"

# --- External autoscaler supervisors (systemd --user timers) ----------------
# Both pools use fixed candidate paths and distinct reserved local DB ports.
"""


def _table_spans(text: str, header: str) -> list[tuple[int, int]]:
    starts = [match.start() for match in re.finditer(rf"(?m)^{re.escape(header)}\s*$", text)]
    spans: list[tuple[int, int]] = []
    for start in starts:
        next_header = re.search(r"(?m)^\[\[?[A-Za-z0-9_.-]+\]?\]\s*$", text[start + 1 :])
        end = len(text) if next_header is None else start + 1 + next_header.start()
        spans.append((start, end))
    return spans


def _array_table_spans(text: str, header: str) -> list[tuple[int, int]]:
    starts = [match.start() for match in re.finditer(rf"(?m)^{re.escape(header)}\s*$", text)]
    spans: list[tuple[int, int]] = []
    for start in starts:
        next_array = re.search(r"(?m)^\[\[[A-Za-z0-9_.-]+\]\]\s*$", text[start + 1 :])
        end = len(text) if next_array is None else start + 1 + next_array.start()
        spans.append((start, end))
    return spans


def _replace_single_table(text: str, header: str, replacement: str) -> str:
    spans = _table_spans(text, header)
    if len(spans) != 1:
        raise PromotionError(f"{header} table is not unique")
    start, end = spans[0]
    return text[:start] + replacement + text[end:]


def _replace_exact_once(text: str, old: str, new: str, *, label: str) -> str:
    if text.count(old) != 1:
        raise PromotionError(f"{label} is not exact")
    return text.replace(old, new, 1)


def _replace_named_array_table(
    text: str,
    *,
    header: str,
    selector: str,
    replacement: str,
) -> str:
    spans = _array_table_spans(text, header)
    matches = [
        (start, end)
        for start, end in spans
        if re.search(rf"(?m)^name = {_toml_string(selector)}$", text[start:end])
        or re.search(rf"(?m)^pool_name = {_toml_string(selector)}$", text[start:end])
    ]
    if len(matches) != 1:
        raise PromotionError(f"{header} {selector} table is not unique")
    start, end = matches[0]
    return text[:start] + replacement + text[end:]


def _enable_oldlab_supervisor(text: str) -> str:
    spans = _array_table_spans(text, "[[external_slurm_autoscaler_supervisors]]")
    matches = [
        (start, end)
        for start, end in spans
        if re.search(r'(?m)^name = "oldlab-staging"$', text[start:end])
    ]
    if len(matches) != 1:
        raise PromotionError("OLDLAB supervisor table is not unique")
    start, end = matches[0]
    block = text[start:end]
    if (
        len(re.findall(r"(?m)^enabled = (?:true|false)$", block)) != 1
        or len(
            re.findall(r"(?m)^active = (?:true|false)$", block),
        )
        != 1
    ):
        raise PromotionError("OLDLAB supervisor booleans are invalid")
    block = re.sub(r"(?m)^enabled = (?:true|false)$", "enabled = true", block)
    block = re.sub(r"(?m)^active = (?:true|false)$", "active = true", block)
    return text[:start] + block + text[end:]


def render_profile_text(
    current_text: str,
    evidence: PromotionEvidence,
) -> str:
    try:
        current = tomllib.loads(current_text)
    except tomllib.TOMLDecodeError as exc:
        raise PromotionError("staging profile is invalid") from exc
    if not isinstance(current, dict):
        raise PromotionError("staging profile is invalid")
    promotion = current.get("oldlab_capacity_promotion")
    if not isinstance(promotion, dict) or promotion.get("enabled") is not False:
        raise PromotionError("render requires the fixed disabled promotion base")
    _validate_disabled(current)
    _validate_disabled_external_base(current)
    current_text = _replace_exact_once(
        current_text,
        DISABLED_OLDLAB_POLICY_COMMENT,
        "",
        label="disabled OLDLAB policy comment",
    )
    current_text = _replace_exact_once(
        current_text,
        DISABLED_OLDLAB_SUPERVISOR_COMMENT,
        PROMOTED_OLDLAB_SUPERVISOR_COMMENT,
        label="disabled OLDLAB supervisor comment",
    )
    rendered = _replace_single_table(
        current_text,
        "[oldlab_capacity_promotion]",
        _promotion_block(evidence),
    )
    rendered = _replace_named_array_table(
        rendered,
        header="[[worker_pool_autoscaler_policies]]",
        selector="oldlab",
        replacement=_oldlab_policy_block(evidence),
    )
    rendered = _enable_oldlab_supervisor(rendered)
    rendered = _replace_single_table(
        rendered,
        "[external_slurm_runner_prerequisites]",
        _external_prerequisite_block(),
    )
    try:
        parsed = tomllib.loads(rendered)
    except tomllib.TOMLDecodeError as exc:  # pragma: no cover - internal rendering invariant
        raise PromotionError("rendered staging profile is invalid") from exc
    _validate_enabled(parsed, evidence)
    return rendered


def _profile() -> tuple[dict[str, Any], str]:
    parsed, raw = _load_toml(STAGING_PROFILE, label="staging profile")
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:  # pragma: no cover - _load_toml already decoded it
        raise PromotionError("staging profile is invalid") from exc
    return parsed, text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", allow_abbrev=False)
    subparsers.add_parser("render", allow_abbrev=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        profile, current_text = _profile()
        if args.command == "check":
            status = check_profile(profile)
            sys.stdout.write(
                json.dumps(
                    {
                        "schema_version": 1,
                        "kind": "loom.developer-sandbox.capacity-promotion-check",
                        "environment": "staging",
                        "pool": "oldlab",
                        "status": status,
                        "live_mutations_supported": False,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
            return 0
        evidence = _load_promotion_evidence()
        rendered = render_profile_text(current_text, evidence)
        sys.stdout.writelines(
            difflib.unified_diff(
                current_text.splitlines(keepends=True),
                rendered.splitlines(keepends=True),
                fromfile="a/deploy/environment-state/staging.toml",
                tofile="b/deploy/environment-state/staging.toml",
            ),
        )
        return 0
    except PromotionError:
        sys.stderr.write("error: OLDLAB capacity promotion failed safely\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
