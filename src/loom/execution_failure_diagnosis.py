"""Explain confirmed native termination facts without rewriting runtime outcomes."""

from __future__ import annotations

from typing import Any

from loom.execution_runtime_contract import ExecutionRuntimePlanV1

_FAILURES = {"failed", "oom_killed", "evicted", "node_lost", "deadline_exceeded"}


def execution_failure_diagnosis(
    events: list[dict[str, Any]], *, plan: ExecutionRuntimePlanV1,
    job_uid: str | None, pod_uid: str | None,
) -> dict[str, Any] | None:
    """Enrich only the first lost container incarnation in the bound Pod.

    Restart count identifies the original incarnation even if the first status
    did not yet include lastState. A subsequent replacement-container OOM must
    not become the explanation of the original loss. Exit 137 alone is never
    treated as kernel OOM evidence.
    """
    if not job_uid or not pod_uid:
        return None
    fixture_roles = {sidecar.role_name for sidecar in plan.sidecars if sidecar.task_fixture}
    observations = [event for event in sorted(events, key=lambda e: e["ordinal"])
                    if event["payload"].get("job_uid") == job_uid
                    and event["payload"].get("pod_uid") == pod_uid]
    anchor: tuple[str, int, str | None] | None = None
    for event in observations:
        payload = event["payload"]
        if payload.get("normalized_state") not in _FAILURES:
            continue
        for container in payload.get("container_diagnostics", []):
            name = container.get("name")
            if name not in {"execution", "task-sandbox", "verifier-sandbox", *fixture_roles}:
                continue
            restarts = container.get("restart_count", 0)
            previous = container.get("previous_termination")
            current = container.get("current_termination")
            if not (restarts or previous or current):
                continue
            # The loss of a native sandbox precedes downstream controller exit.
            if payload.get("reason") in {"SandboxRestarted", "SandboxTerminated"} and name == "execution":
                continue
            ending = previous or current or {}
            index = max(0, restarts - 1) if restarts or previous else restarts
            candidate = (name, index, ending.get("started_at"))
            if anchor is None:
                anchor = candidate
            if name != anchor[0]:
                continue
            for key, incarnation in (("previous_termination", restarts - 1),
                                     ("current_termination", restarts)):
                termination = container.get(key)
                if (not termination or termination.get("reason") != "OOMKilled"
                        or incarnation != anchor[1]):
                    continue
                started = termination.get("started_at")
                if anchor[2] is not None and started != anchor[2]:
                    continue
                limits = (plan.execution_resources if name == "execution" else
                          next((s.resources for s in plan.sidecars if s.role_name == name), None))
                memory = limits.memory_mib if limits else None
                size = f"{memory / 1024:g} GiB" if memory is not None else "unknown"
                message = (
                    f"The {name} container exceeded its {size} memory limit and was terminated "
                    "by the system (OOMKilled). Subsequent communication or cleanup errors do "
                    "not replace this cause. Periodic samples may miss the final memory peak."
                )
                return {
                    "reason": "oom_killed", "container_role": name,
                    "stage": ({"execution": "controller" if plan.controller_resources else "execution", "task-sandbox": "agent",
                               "verifier-sandbox": "verifier"}.get(name, "fixture")),
                    "container_incarnation": incarnation, "started_at": started,
                    "terminated_at": termination.get("finished_at"),
                    "exit_code": termination.get("exit_code"), "signal": termination.get("signal"),
                    "memory_limit_mib": memory, "limits": limits.model_dump() if limits else None,
                    "evidence_source": "kubernetes_container_termination",
                    "evidence_ordinal": event["ordinal"], "message": message,
                    "sampled_peak_status": "not_authoritative_for_termination",
                }
    return None
