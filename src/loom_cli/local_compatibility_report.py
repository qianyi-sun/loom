"""Read-only, whole-input intake diagnostics; no build or runtime qualification."""

from __future__ import annotations

import shutil
import tempfile
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from loom.execution_architecture import execution_cpu_arch
from loom.models.task import EnvironmentConfig, TaskConfig
from loom.models.task_checksum import task_checksum
from loom.nebius_terminus_ingest import (
    NEBIUS_TERMINUS_PROFILE,
    adapt_bundle_for_nebius_terminus,
    preflight_nebius_terminus_admission,
)
from loom.service_execution_materialization import prepare_service_execution_input_manifest
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml


@dataclass
class TaskCompatibilityReport:
    task_id: str
    source_location: str
    status: str = "blocked"
    admission_passed: bool | None = None
    original_requirements: dict[str, Any] = field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    changes: list[dict[str, Any]] = field(default_factory=list)

    def add(self, category: str, code: str, reason: str, action: str, *, source: str = "") -> None:
        self.diagnostics.append({
            "category": category,
            "code": code,
            "source_location": source or self.source_location,
            "reason": reason,
            "suggested_action": action,
        })


def render_compatibility_payload(reports: tuple[TaskCompatibilityReport, ...]) -> dict[str, Any]:
    return {
        "schema_version": "loom.local-compatibility-report.v1",
        "runtime_verified": False,
        "blocked_tasks": sum(report.status == "blocked" for report in reports),
        "tasks": [asdict(report) for report in reports],
        "limitations": [
            "Static local checks only; no image build, registry lookup, model calls or runtime execution.",
            "Undeclared requirements in instructions and scripts require task-author review.",
            "Passing admission does not establish equivalent task semantics or trajectory delivery.",
        ],
    }


def collect_compatibility_reports(
    *, benchmark_id: str, task_root: Path, task_tomls: tuple[Path, ...],
    execution_profile: str | None,
) -> tuple[TaskCompatibilityReport, ...]:
    reports = []
    for path in task_tomls:
        relative = path.parent.relative_to(task_root)
        task_id = benchmark_id if relative == Path(".") else f"{benchmark_id}/{relative.as_posix()}"
        report = TaskCompatibilityReport(task_id=task_id, source_location=str(path))
        reports.append(report)
        _inspect_task(path, report, execution_profile=execution_profile)
    return tuple(reports)


def _inspect_task(path: Path, report: TaskCompatibilityReport, *, execution_profile: str | None) -> None:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        report.add("package_defect", "invalid_toml", str(exc), "Repair task.toml and rerun validation.")
        return
    report.original_requirements = {
        key: raw[key] for key in ("environment", "agent", "verifier", "steps", "required_agent_capabilities")
        if key in raw
    }
    if execution_profile == NEBIUS_TERMINUS_PROFILE:
        _declared_runtime_requirements(raw, report)
    try:
        normalized = normalize_terminal_bench_task_toml(raw)
        task = TaskConfig.model_validate(normalized)
        execution_cpu_arch(task.environment.cpu_arch)
    except (ValueError, TypeError) as exc:
        reason = str(exc)
        if isinstance(exc, ValidationError):
            reason = "; ".join(
                f"{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
                for error in exc.errors(include_url=False, include_input=False)
            )
        report.add("package_defect", "invalid_task_config", reason,
                   "Supply the missing Loom intake fields or repair the declared schema; preserve task requirements.")
        return
    if execution_profile != NEBIUS_TERMINUS_PROFILE:
        report.status = "schema_valid"
        return
    _dropped_environment_requirements(raw, normalized, report)
    try:
        with tempfile.TemporaryDirectory(prefix="loom-compatibility-") as stage_root:
            staged = Path(stage_root) / "bundle"
            shutil.copytree(path.parent, staged, symlinks=False)
            adapted, _ = adapt_bundle_for_nebius_terminus(staged, normalized)
            _record_changes(normalized, adapted, raw, report)
            _, provenance = prepare_service_execution_input_manifest(
                staged, task_checksum=task_checksum(staged), bucket="validate-local",
                manifest_key=f"{report.task_id}/service-execution-input.json",
            )
            reasons = preflight_nebius_terminus_admission(adapted, provenance)
    except (OSError, UnicodeError, ValueError, TypeError, shutil.Error) as exc:
        report.add("unsupported_conversion", "profile_adaptation_failed", str(exc),
                   "Provide a reviewed equivalent bootstrap/image adaptation; do not skip unknown dependencies.",
                   source=str(path.parent))
        return
    report.admission_passed = not reasons
    for reason in reasons:
        report.add("runtime_capability", reason, f"Profile admission rejected: {reason}.",
                   "Use a qualified runtime capability or retain this task as blocked.")
    if not report.diagnostics:
        report.status = "converted" if report.changes else "supported"


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name)
    return value if isinstance(value, dict) else {}


def _declared_runtime_requirements(raw: dict[str, Any], report: TaskCompatibilityReport) -> None:
    env = _section(raw, "environment")
    agent = _section(raw, "agent")
    verifier = _section(raw, "verifier")

    def add(code: str, key: str, reason: str, issue: int) -> None:
        report.add("runtime_capability", code, reason,
                   f"Preserve the requirement and qualify support through #{issue} before submission.",
                   source=f"{report.source_location}#{key}")

    if "user" in env and env["user"] != "agent":
        add("task_identity", "environment.user", f"Task declares user {env['user']!r}; profile uses 'agent'.", 2049)
    if agent.get("user") is not None:
        add("agent_identity", "agent.user", "Custom agent identity is not admitted by this profile.", 2049)
    if verifier.get("user") is not None:
        add("verifier_identity", "verifier.user", "Profile removes the declared verifier identity.", 2049)
    if "workdir" in env and env["workdir"] not in ("/app", "/workspace"):
        add("workspace_path", "environment.workdir", "Profile would replace the declared workdir with /app.", 2047)
    if "mutable_paths" in env and "mutable_paths" not in EnvironmentConfig.model_fields:
        add("mutable_paths", "environment.mutable_paths", "Declared mutable paths have no supported transfer contract.", 2047)
    if env.get("services") or env.get("sidecars"):
        add("services", "environment.services" if env.get("services") else "environment.sidecars",
            "Declared services need runtime initialization and verifier lifecycle support.", 2050)
    if env.get("allow_internet") is True:
        add("runtime_egress", "environment.allow_internet", "Internet access is declared; profile forces gateway-only egress.", 2048)
    if env.get("allow_internet") is False:
        add("network_policy_change", "environment.allow_internet", "No-network is declared; profile enables Gateway networking.", 2048)
    policy = env.get("baseline_network_policy")
    if isinstance(policy, dict) and policy.get("kind") != "gateway-only":
        add("runtime_egress", "environment.baseline_network_policy", "Declared network policy differs from gateway-only.", 2048)
    policies = env.get("network_policies_supported")
    if isinstance(policies, list) and policies != ["gateway-only"]:
        add("network_policy_change", "environment.network_policies_supported", "Profile replaces the declared supported policies.", 2048)
    if env.get("cpu_arch", env.get("architecture", "x86_64")) not in ("x86_64", "amd64", "any"):
        add("architecture", "environment.cpu_arch", "Declared architecture differs from this x86_64 execution profile.", 2051)
    if env.get("gpus") or env.get("gpu_vendor", "none") != "none":
        add("devices", "environment.gpus", "Declared GPU capability is not supported by this profile.", 2051)
    if verifier.get("env_mode", verifier.get("environment_mode", "shared")) != "shared":
        add("verifier_environment", "verifier.env_mode", "Profile replaces the declared verifier environment mode.", 2050)


def _dropped_environment_requirements(
    raw: dict[str, Any], normalized: dict[str, Any], report: TaskCompatibilityReport,
) -> None:
    """Do not silently label a lossy Harbor projection as compatible."""
    environment = _section(normalized, "environment")
    recognized = {"architecture", "allow_internet", "env", "services"}
    if "mutable_paths" not in EnvironmentConfig.model_fields:
        recognized.add("mutable_paths")  # already reported as a runtime gap
    for key in _section(raw, "environment"):
        if key in environment or key in recognized:
            continue
        report.add("unsupported_conversion", "unmapped_environment_requirement",
                   f"Harbor normalization discards environment.{key}.",
                   "Map this declaration explicitly or provide a reviewed equivalent task package.",
                   source=f"{report.source_location}#environment.{key}")


def _record_changes(
    normalized: dict[str, Any], adapted: dict[str, Any], raw: dict[str, Any],
    report: TaskCompatibilityReport,
) -> None:
    for section in ("environment", "verifier"):
        before = _section(normalized, section)
        after = _section(adapted, section)
        for key in sorted(before.keys() | after.keys()):
            if before.get(key) == after.get(key):
                continue
            report.changes.append({
                "field": f"{section}.{key}", "before": before.get(key), "after": after.get(key),
                "category": "profile_default" if key not in before else "profile_conversion",
            })
    if normalized.get("steps") != adapted.get("steps"):
        report.changes.append({
            "field": "steps", "before": normalized.get("steps"), "after": adapted.get("steps"),
            "category": "profile_conversion",
        })
        if "steps" in raw or raw.get("artifacts"):
            report.add("unsupported_conversion", "artifact_requirements_changed",
                       "Profile removes declared artifact paths or patterns.",
                       "Preserve artifact requirements through an explicit supported collection contract.")
