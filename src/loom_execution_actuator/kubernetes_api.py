from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from loom_execution_actuator.contracts import (
    ExecutionTerminationSummaryV1,
    KubernetesApiError,
    KubernetesJobInventory,
    KubernetesJobObservation,
    NormalizedJobState,
)

_LEASE_LABEL = "loom.openai.com/lease-id"
_GENERATION_LABEL = "loom.openai.com/generation"
_TARGET_ANNOTATION = "loom.openai.com/target-id"
_EXECUTION_UNIT_ANNOTATION = "loom.openai.com/execution-unit-key"
_RUNTIME_CONTRACT_ANNOTATION = "loom.openai.com/runtime-contract-sha256"
_COMMAND_IDENTITY_ANNOTATION = "loom.openai.com/command-identity-sha256"
_EXECUTION_ROLE_ANNOTATION = "loom.openai.com/execution-role"


def _condition(conditions: list[Any] | None, condition_type: str) -> Any | None:
    return next(
        (
            condition
            for condition in conditions or []
            if getattr(condition, "type", None) == condition_type
        ),
        None,
    )


def _normalize(job: Any, pods: list[Any]) -> KubernetesJobObservation:
    metadata = job.metadata
    labels = dict(metadata.labels or {})
    annotations = dict(metadata.annotations or {})
    lease_id = labels.get(_LEASE_LABEL)
    generation = labels.get(_GENERATION_LABEL)
    target_id = annotations.get(_TARGET_ANNOTATION)
    execution_unit_key = annotations.get(_EXECUTION_UNIT_ANNOTATION)
    if not lease_id or not generation or not target_id or not execution_unit_key:
        raise KubernetesApiError("managed Job is missing Loom identity", status_code=409)
    try:
        generation_value = int(generation)
    except (TypeError, ValueError) as exc:
        raise KubernetesApiError("managed Job generation is invalid", status_code=409) from exc

    pod = max(
        pods,
        key=lambda item: (
            getattr(item.metadata, "creation_timestamp", None) or datetime.min.replace(tzinfo=UTC)
        ),
        default=None,
    )
    state = NormalizedJobState.PENDING
    reason: str | None = None
    message: str | None = None
    termination_summary: ExecutionTerminationSummaryV1 | None = None
    scheduled_at = None
    started_at = getattr(job.status, "start_time", None)
    terminated_at = getattr(job.status, "completion_time", None)
    node_name = None
    pod_uid = None
    pod_ip = None

    if metadata.deletion_timestamp is not None:
        state = NormalizedJobState.TERMINATING
    elif _condition(getattr(job.status, "conditions", None), "Complete") is not None:
        state = NormalizedJobState.SUCCEEDED
    elif (failed := _condition(getattr(job.status, "conditions", None), "Failed")) is not None:
        reason = getattr(failed, "reason", None)
        message = getattr(failed, "message", None)
        state = (
            NormalizedJobState.DEADLINE_EXCEEDED
            if reason == "DeadlineExceeded"
            else NormalizedJobState.FAILED
        )

    if pod is not None:
        pod_uid = str(pod.metadata.uid) if pod.metadata.uid is not None else None
        pod_ip = getattr(pod.status, "pod_ip", None)
        node_name = getattr(pod.spec, "node_name", None)
        scheduled = _condition(getattr(pod.status, "conditions", None), "PodScheduled")
        if getattr(scheduled, "status", None) == "True":
            scheduled_at = getattr(scheduled, "last_transition_time", None)
        started_at = getattr(pod.status, "start_time", None) or started_at
        # kubelet can publish ``status.startTime`` before the PodScheduled
        # condition controller publishes its transition timestamp. The latter
        # is therefore only an upper-bound observation, not proof that the Pod
        # started before it was scheduled.
        if (
            scheduled_at is not None
            and started_at is not None
            and scheduled_at > started_at
        ):
            scheduled_at = started_at
        if pod.metadata.deletion_timestamp is not None:
            state = NormalizedJobState.TERMINATING
        pod_reason = getattr(pod.status, "reason", None)
        pod_message = getattr(pod.status, "message", None)
        if pod_reason == "Evicted":
            state, reason, message = NormalizedJobState.EVICTED, pod_reason, pod_message
        elif pod_reason in {"NodeLost", "Shutdown"}:
            state, reason, message = NormalizedJobState.NODE_LOST, pod_reason, pod_message
        else:
            statuses = list(getattr(pod.status, "container_statuses", None) or [])
            terminated = [
                status.state.terminated
                for status in statuses
                if getattr(getattr(status, "state", None), "terminated", None) is not None
            ]
            waiting = [
                status.state.waiting
                for status in statuses
                if getattr(getattr(status, "state", None), "waiting", None) is not None
            ]
            execution_status = next(
                (status for status in statuses if getattr(status, "name", None) == "execution"),
                None,
            )
            execution_terminated = getattr(
                getattr(execution_status, "state", None), "terminated", None
            )
            raw_summary = getattr(execution_terminated, "message", None)
            if raw_summary:
                try:
                    if len(raw_summary.encode("utf-8")) > 4096:
                        raise ValueError("termination summary exceeds 4096 bytes")
                    termination_summary = ExecutionTerminationSummaryV1.model_validate(
                        json.loads(raw_summary)
                    )
                except (TypeError, ValueError) as exc:
                    state = NormalizedJobState.FAILED
                    reason = "InvalidTerminationSummary"
                    message = str(exc)[:2000]
            if any(getattr(item, "reason", None) == "OOMKilled" for item in terminated):
                item = next(item for item in terminated if item.reason == "OOMKilled")
                state, reason, message = (
                    NormalizedJobState.OOM_KILLED,
                    "OOMKilled",
                    getattr(item, "message", None),
                )
                terminated_at = getattr(item, "finished_at", None) or terminated_at
            elif any(
                getattr(item, "reason", None) in {"ImagePullBackOff", "ErrImagePull"}
                for item in waiting
            ):
                item = next(
                    item for item in waiting if item.reason in {"ImagePullBackOff", "ErrImagePull"}
                )
                state, reason, message = (
                    NormalizedJobState.IMAGE_PULL_BACKOFF,
                    item.reason,
                    getattr(item, "message", None),
                )
            elif (
                scheduled is not None
                and getattr(scheduled, "status", None) == "False"
                and getattr(scheduled, "reason", None) == "Unschedulable"
            ):
                state, reason, message = (
                    NormalizedJobState.UNSCHEDULABLE,
                    "Unschedulable",
                    getattr(scheduled, "message", None),
                )
            elif getattr(pod.status, "phase", None) == "Running":
                state = NormalizedJobState.RUNNING
            elif getattr(pod.status, "phase", None) == "Succeeded":
                state = NormalizedJobState.SUCCEEDED
            elif getattr(pod.status, "phase", None) == "Failed" and state not in {
                NormalizedJobState.EVICTED,
                NormalizedJobState.NODE_LOST,
                NormalizedJobState.OOM_KILLED,
            }:
                state, reason, message = (
                    NormalizedJobState.FAILED,
                    pod_reason or "PodFailed",
                    pod_message,
                )

    if termination_summary is not None and (
        termination_summary.runtime_contract_sha256 != annotations.get(_RUNTIME_CONTRACT_ANNOTATION)
        or termination_summary.command_identity_sha256
        != annotations.get(_COMMAND_IDENTITY_ANNOTATION)
        or termination_summary.execution_role != annotations.get(_EXECUTION_ROLE_ANNOTATION)
    ):
        state = NormalizedJobState.FAILED
        reason = "TerminationSummaryIdentityMismatch"
        message = "runtime termination summary does not match Job identity"
    elif state == NormalizedJobState.SUCCEEDED and termination_summary is None:
        state = NormalizedJobState.FAILED
        reason = "MissingTerminationSummary"
        message = "successful runtime Job has no termination summary"
    elif (
        state == NormalizedJobState.SUCCEEDED
        and termination_summary is not None
        and termination_summary.status != "succeeded"
    ):
        state = NormalizedJobState.FAILED
        reason = "RuntimeReportedFailure"
        message = f"runtime reported {termination_summary.status}"

    return KubernetesJobObservation(
        namespace=metadata.namespace,
        job_name=metadata.name,
        lease_id=lease_id,
        resource_generation=generation_value,
        target_id=target_id,
        execution_unit_key=execution_unit_key,
        normalized_state=state,
        job_uid=str(metadata.uid) if metadata.uid is not None else None,
        pod_uid=pod_uid,
        pod_ip=pod_ip,
        resource_version=(
            str(metadata.resource_version) if metadata.resource_version is not None else None
        ),
        node_name=node_name,
        scheduled_at=scheduled_at,
        started_at=started_at,
        terminated_at=terminated_at,
        reason=reason,
        message=message,
        termination_summary=termination_summary,
    )


class InClusterKubernetesJobApi:
    """Small async seam over namespace-scoped Batch/Core APIs."""

    def __init__(
        self,
        *,
        client_module: Any | None = None,
        batch_api: Any | None = None,
        core_api: Any | None = None,
    ) -> None:
        try:
            from kubernetes import client, config
        except ModuleNotFoundError as exc:
            raise RuntimeError("install Loom with the cluster extra") from exc
        if any(value is not None for value in (client_module, batch_api, core_api)):
            if client_module is None or batch_api is None or core_api is None:
                raise ValueError("client_module, batch_api, and core_api must be provided together")
            self._client = client_module
            self._batch = batch_api
            self._core = core_api
        else:
            config.load_incluster_config()
            self._client = client
            self._batch = client.BatchV1Api()
            self._core = client.CoreV1Api()

    def _pods_for_job(self, namespace: str, job_name: str) -> list[Any]:
        return list(
            self._core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}",
            ).items
        )

    def _translate(self, exc: Exception, operation: str) -> KubernetesApiError:
        raw_status = getattr(exc, "status", None)
        status = raw_status if isinstance(raw_status, int) else None
        headers = getattr(exc, "headers", None) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        return KubernetesApiError(
            f"Kubernetes {operation} failed",
            status_code=status,
            retry_after_seconds=(int(str(retry_after)) if str(retry_after).isdigit() else None),
            ambiguous=status is None or status >= 500,
        )

    def _get_sync(self, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        try:
            job = self._batch.read_namespaced_job(name=job_name, namespace=namespace)
            return _normalize(job, self._pods_for_job(namespace, job_name))
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise self._translate(exc, "get") from exc

    async def get_job(self, *, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        return await asyncio.to_thread(self._get_sync, namespace, job_name)

    async def create_job(
        self, *, namespace: str, manifest: dict[str, Any]
    ) -> KubernetesJobObservation:
        def create() -> KubernetesJobObservation:
            try:
                job = self._batch.create_namespaced_job(namespace=namespace, body=manifest)
                return _normalize(job, self._pods_for_job(namespace, job.metadata.name))
            except Exception as exc:
                raise self._translate(exc, "create") from exc

        return await asyncio.to_thread(create)

    async def delete_job(
        self,
        *,
        namespace: str,
        job_name: str,
        expected_uid: str,
        grace_period_seconds: int,
    ) -> None:
        def delete() -> None:
            try:
                options = self._client.V1DeleteOptions(
                    propagation_policy="Foreground",
                    grace_period_seconds=grace_period_seconds,
                    preconditions=self._client.V1Preconditions(uid=expected_uid),
                )
                self._batch.delete_namespaced_job(
                    name=job_name,
                    namespace=namespace,
                    body=options,
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    return
                raise self._translate(exc, "delete") from exc

        await asyncio.to_thread(delete)

    def _list_sync(self, namespace: str, label_selector: str) -> KubernetesJobInventory:
        try:
            jobs = self._batch.list_namespaced_job(
                namespace=namespace,
                label_selector=label_selector,
            ).items
            observations: list[KubernetesJobObservation] = []
            rejected_count = 0
            for job in jobs:
                try:
                    observations.append(
                        _normalize(job, self._pods_for_job(namespace, job.metadata.name))
                    )
                except KubernetesApiError as exc:
                    if exc.status_code != 409:
                        raise
                    # A malformed object inside the managed label scope is
                    # quarantined as drift instead of poisoning all repair.
                    rejected_count += 1
            return KubernetesJobInventory(tuple(observations), rejected_count)
        except Exception as exc:
            if isinstance(exc, KubernetesApiError):
                raise
            raise self._translate(exc, "list") from exc

    async def list_jobs(self, *, namespace: str, label_selector: str) -> KubernetesJobInventory:
        return await asyncio.to_thread(self._list_sync, namespace, label_selector)

    async def watch_jobs(
        self,
        *,
        namespace: str,
        label_selector: str,
        resource_version: str | None,
        timeout_seconds: int,
    ) -> tuple[KubernetesJobObservation, ...]:
        def watch_once() -> tuple[KubernetesJobObservation, ...]:
            try:
                from kubernetes import watch

                stream = watch.Watch()
                observations: list[KubernetesJobObservation] = []
                for event in stream.stream(
                    self._batch.list_namespaced_job,
                    namespace=namespace,
                    label_selector=label_selector,
                    resource_version=resource_version,
                    timeout_seconds=timeout_seconds,
                ):
                    job = event["object"]
                    observations.append(
                        _normalize(job, self._pods_for_job(namespace, job.metadata.name))
                    )
                return tuple(observations)
            except Exception as exc:
                if getattr(exc, "status", None) == 410:
                    raise self._translate(exc, "watch resource version expired") from exc
                raise self._translate(exc, "watch") from exc

        return await asyncio.to_thread(watch_once)


__all__ = ["InClusterKubernetesJobApi"]
