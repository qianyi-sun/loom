"""Shared pod sandbox deadline diagnostics for cluster rollout tooling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

FAILURE_CLASS = "node_runtime_sandbox_deadline"
RECOVERY_KIND = "bounded_pod_cleanup_retry"

_SANDBOX_EVENT_REASONS = frozenset({
    "FailedCreatePodSandBox",
    "FailedKillPod",
})
_RUNTIME_WAITING_REASONS = frozenset({
    "ContainerCreating",
    "RunContainerError",
})


@dataclass(frozen=True)
class PodSandboxDeadlineDiagnostic:
    pod: str
    reason: str
    operation: str
    target_generation: bool
    message: str = ""

    def summary(self) -> dict[str, Any]:
        return {
            "pod": self.pod,
            "reason": self.reason,
            "operation": self.operation,
            "target_generation": self.target_generation,
        }


def get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def labels(obj: Any) -> dict[str, str]:
    raw = get_field(obj, "labels", {}) or {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def deployment_selector_labels(
    deployment: Any,
    *,
    fallback_name: str,
) -> dict[str, str]:
    selector = get_field(get_field(deployment, "spec"), "selector")
    match_labels = get_field(selector, "match_labels")
    if isinstance(match_labels, dict) and match_labels:
        return {str(key): str(value) for key, value in match_labels.items()}
    return {"app": fallback_name}


def pod_matches_selector(pod: Any, selector: dict[str, str]) -> bool:
    pod_labels = labels(get_field(pod, "metadata"))
    return all(pod_labels.get(key) == value for key, value in selector.items())


def container_images(pod_spec: Any) -> frozenset[str]:
    images: set[str] = set()
    for container in get_field(pod_spec, "containers", []) or []:
        image = get_field(container, "image")
        if image:
            images.add(str(image))
    return frozenset(images)


def deployment_template_images(deployment: Any) -> frozenset[str]:
    template = get_field(get_field(deployment, "spec"), "template")
    return container_images(get_field(template, "spec"))


def pod_template_matches_deployment(
    pod: Any,
    deployment_images: frozenset[str],
) -> bool:
    if not deployment_images:
        return True
    pod_images = container_images(get_field(pod, "spec"))
    return deployment_images.issubset(pod_images)


def event_involved_pod_name(event: Any) -> str | None:
    involved = get_field(event, "involved_object")
    name = get_field(involved, "name")
    if name:
        return str(name)
    regarding = get_field(event, "regarding")
    name = get_field(regarding, "name")
    return str(name) if name else None


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _operation_from(reason: str, message: str) -> str:
    text = f"{reason} {message}".lower()
    if "create" in text:
        return "create"
    if "kill" in text or "delete" in text or "teardown" in text:
        return "kill"
    return "unknown"


def _is_sandbox_deadline(reason: str, message: str) -> bool:
    text = f"{reason} {message}".lower()
    has_deadline = (
        "context deadline exceeded" in text
        or "deadlineexceeded" in text
    )
    if not has_deadline:
        return False
    if reason in _SANDBOX_EVENT_REASONS:
        return True
    if reason in _RUNTIME_WAITING_REASONS:
        return True
    return (
        "podsandbox" in text
        or "pod sandbox" in text
        or "sandbox" in text
    )


def _dedupe_append(
    diagnostics: list[PodSandboxDeadlineDiagnostic],
    seen: set[tuple[str, str, str]],
    diagnostic: PodSandboxDeadlineDiagnostic,
) -> None:
    key = (diagnostic.pod, diagnostic.reason, diagnostic.operation)
    if key in seen:
        return
    seen.add(key)
    diagnostics.append(diagnostic)


def sandbox_deadline_diagnostics_for_deployment(
    *,
    deployment: Any,
    fallback_name: str,
    pods: list[Any],
    events: list[Any],
) -> list[PodSandboxDeadlineDiagnostic]:
    selector = deployment_selector_labels(deployment, fallback_name=fallback_name)
    deployment_images = deployment_template_images(deployment)
    matching_pods = [
        pod for pod in pods if pod_matches_selector(pod, selector)
    ]
    pod_by_name = {
        str(get_field(get_field(pod, "metadata"), "name", "")): pod
        for pod in matching_pods
        if get_field(get_field(pod, "metadata"), "name")
    }
    diagnostics: list[PodSandboxDeadlineDiagnostic] = []
    seen: set[tuple[str, str, str]] = set()

    for event in events:
        pod_name = event_involved_pod_name(event)
        if not pod_name or pod_name not in pod_by_name:
            continue
        reason = _safe_text(get_field(event, "reason")).strip() or "Unknown"
        message = _safe_text(get_field(event, "message"))
        if not _is_sandbox_deadline(reason, message):
            continue
        pod = pod_by_name[pod_name]
        _dedupe_append(
            diagnostics,
            seen,
            PodSandboxDeadlineDiagnostic(
                pod=pod_name,
                reason=reason,
                operation=_operation_from(reason, message),
                target_generation=pod_template_matches_deployment(
                    pod,
                    deployment_images,
                ),
                message=message,
            ),
        )

    for pod_name, pod in pod_by_name.items():
        statuses = (
            get_field(get_field(pod, "status"), "container_statuses", [])
            or []
        )
        for container_status in statuses:
            state = get_field(container_status, "state")
            waiting = get_field(state, "waiting")
            reason = _safe_text(get_field(waiting, "reason")).strip()
            message = _safe_text(get_field(waiting, "message"))
            if not reason or not _is_sandbox_deadline(reason, message):
                continue
            _dedupe_append(
                diagnostics,
                seen,
                PodSandboxDeadlineDiagnostic(
                    pod=pod_name,
                    reason=reason,
                    operation=_operation_from(reason, message),
                    target_generation=pod_template_matches_deployment(
                        pod,
                        deployment_images,
                    ),
                    message=message,
                ),
            )

    return diagnostics


def diagnostic_summaries(
    diagnostics: list[PodSandboxDeadlineDiagnostic],
) -> list[dict[str, Any]]:
    return [diagnostic.summary() for diagnostic in diagnostics]


def format_sandbox_deadline_note(
    diagnostics: list[PodSandboxDeadlineDiagnostic],
) -> str | None:
    if not diagnostics:
        return None
    parts = [
        f"{diagnostic.pod} {diagnostic.reason} ({diagnostic.operation})"
        for diagnostic in diagnostics
    ]
    return "node-runtime-sandbox-deadline: " + "; ".join(parts)


def status_has_sandbox_deadline_failures(status: Any) -> bool:
    components = get_field(status, "components", []) or []
    return any(
        get_field(component, "failure_class") == FAILURE_CLASS
        for component in components
    )


def pod_names_from_status(status: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for component in get_field(status, "components", []) or []:
        if get_field(component, "failure_class") != FAILURE_CLASS:
            continue
        for diagnostic in get_field(component, "runtime_failure_diagnostics", []) or []:
            if not isinstance(diagnostic, dict):
                continue
            name = diagnostic.get("pod")
            if not name:
                continue
            pod_name = str(name)
            if pod_name in seen:
                continue
            seen.add(pod_name)
            names.append(pod_name)
    return names
