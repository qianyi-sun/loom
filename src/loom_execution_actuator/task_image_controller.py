"""Native build reconciliation inside the existing execution actuator.

The materialization lease owns retries; Kubernetes Jobs never retry a build.
One durable attempt retains the Job UID and resource/lifecycle observations.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    ServiceExecutionTarget,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from loom.nebius_kubernetes import NebiusKubernetesConnection, create_api_client
from loom.security.redaction import redact_text
from loom.task_image_build_plan import derive_task_image_build_components
from loom_control_plane.execution_capacity import (
    _CAPACITY_ADMISSION_LOCK,
    ExecutionProvisioningBlockedError,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from loom_control_plane.task_image_materializations import (
    TaskImageCompletionError,
    claim_task_image_materialization,
    complete_task_image_materialization,
    fail_task_image_materialization,
    has_nebius_task_image_demand,
    heartbeat_task_image_materialization,
    start_task_image_materialization,
    task_image_materialization_payload,
)
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import render_task_image_job
from loom_execution_actuator.task_image_settings import (
    NativeTaskImageSettings as NativeTaskImageSettings,
)

_MANAGER = "loom-task-image-builder"
_SELECTOR = "app.kubernetes.io/managed-by=" + _MANAGER
_LIVE = ("claimed", "running")
_LOG = logging.getLogger(__name__)


class NativeBuildApi(Protocol):
    async def inventory(self, namespace: str) -> list[dict[str, Any]]: ...
    async def ensure(self, configmap: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]: ...
    async def observe(self, namespace: str, name: str) -> dict[str, Any] | None: ...
    async def delete(self, namespace: str, name: str, uid: str | None, *, configmap: dict[str, Any]) -> bool: ...


def _matches(actual: Any, expected: Any, *, quantities: bool = False) -> bool:
    """Compare owned fields, allowing API defaults and equivalent quantities."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            _matches(actual.get(key), value, quantities=key in {"limits", "requests"} or quantities)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            _matches(a, e, quantities=quantities) for a, e in zip(actual, expected, strict=True)
        )
    if quantities and isinstance(actual, str) and isinstance(expected, str):
        from kubernetes.utils.quantity import parse_quantity
        try:
            return bool(parse_quantity(actual) == parse_quantity(expected))
        except ValueError:
            return False
    return bool(actual == expected)


def _owned_pods(observation: dict[str, Any], uid: str | None) -> list[dict[str, Any]]:
    return [pod for pod in observation.get("pods", []) if uid and any(
        owner.get("kind") == "Job" and owner.get("uid") == uid
        for owner in pod.get("metadata", {}).get("ownerReferences", [])
    )]


class NativeBuildKubernetesApi:
    def __init__(self, *, connection: NebiusKubernetesConnection | None = None) -> None:
        from kubernetes import client, config
        self._credentials = None
        if connection is None:
            config.load_incluster_config()
            self._api = client.ApiClient()
        else:
            self._api, self._credentials = create_api_client(connection)
        self._core, self._batch = client.CoreV1Api(self._api), client.BatchV1Api(self._api)

    async def close(self) -> None:
        await asyncio.to_thread(self._api.close)
        if self._credentials is not None:
            await self._credentials.close()

    def _json(self, item: Any) -> dict[str, Any]:
        return cast(dict[str, Any], self._api.sanitize_for_serialization(item))

    async def inventory(self, namespace: str) -> list[dict[str, Any]]:
        def run() -> list[dict[str, Any]]:
            return [self._json(job) for job in self._batch.list_namespaced_job(
                namespace, label_selector=_SELECTOR, _request_timeout=20,
            ).items]
        return await asyncio.to_thread(run)

    async def ensure(self, configmap: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            ns, name = job["metadata"]["namespace"], job["metadata"]["name"]
            try:
                self._core.create_namespaced_config_map(ns, configmap, _request_timeout=20)
            except Exception as error:
                if getattr(error, "status", None) != 409:
                    raise
                current = self._json(self._core.read_namespaced_config_map(name, ns, _request_timeout=20))
                if not _matches(current, configmap):
                    raise ValueError("existing build configuration differs from the frozen attempt") from None
            try:
                observed = self._batch.create_namespaced_job(ns, job, _request_timeout=20)
            except Exception as error:
                if getattr(error, "status", None) != 409:
                    raise
                observed = self._batch.read_namespaced_job(name, ns, _request_timeout=20)
            observed_json = self._json(observed)
            if not _matches(observed_json, job):
                raise ValueError("existing build Job differs from the frozen attempt")
            return observed_json
        return await asyncio.to_thread(run)

    async def observe(self, namespace: str, name: str) -> dict[str, Any] | None:
        def run() -> dict[str, Any] | None:
            try:
                job = self._json(self._batch.read_namespaced_job(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
                job = {"metadata": {"name": name, "namespace": namespace}, "job_missing": True}
            pods = self._core.list_namespaced_pod(namespace, label_selector="job-name=" + name, _request_timeout=20).items
            job["pods"] = [self._json(pod) for pod in pods]
            if job.get("job_missing") and not pods:
                return None
            if job.get("status", {}).get("succeeded") or job.get("status", {}).get("failed"):
                for pod in _owned_pods(job, job["metadata"].get("uid"))[:1]:
                    try:
                        log = self._core.read_namespaced_pod_log(
                            pod["metadata"]["name"], namespace, container="build", tail_lines=100,
                            limit_bytes=16384, _request_timeout=20,
                        )
                        job["builder_log"] = _safe_log(str(log))
                    except Exception:
                        pass  # An unscheduled Pod has no build log.
            return job
        return await asyncio.to_thread(run)

    async def delete(self, namespace: str, name: str, uid: str | None, *, configmap: dict[str, Any]) -> bool:
        def run() -> bool:
            try:
                current = self._json(self._batch.read_namespaced_job(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
            else:
                if not uid or current["metadata"]["uid"] != uid:
                    raise ValueError("build cleanup Job UID changed")
                self._batch.delete_namespaced_job(name, namespace, body={
                    "propagationPolicy": "Foreground", "gracePeriodSeconds": 0,
                    "preconditions": {"uid": uid},
                }, _request_timeout=20)
                return False
            pods = [self._json(pod) for pod in self._core.list_namespaced_pod(
                namespace, label_selector="job-name=" + name, _request_timeout=20,
            ).items]
            if pods:
                owned = _owned_pods({"pods": pods}, uid)
                if len(owned) != len(pods):
                    raise ValueError("build cleanup Pod ownership differs")
                for pod in owned:
                    try:
                        self._core.delete_namespaced_pod(pod["metadata"]["name"], namespace, body={
                            "gracePeriodSeconds": 0, "preconditions": {"uid": pod["metadata"]["uid"]},
                        }, _request_timeout=20)
                    except Exception as error:
                        if getattr(error, "status", None) != 404:
                            raise
                return False
            try:
                current_cm = self._json(self._core.read_namespaced_config_map(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) == 404:
                    return True
                raise
            if not _matches(current_cm, configmap):
                raise ValueError("build cleanup ConfigMap ownership differs")
            self._core.delete_namespaced_config_map(name, namespace, body={
                "preconditions": {"uid": current_cm["metadata"]["uid"]},
            }, _request_timeout=20)
            return False
        return await asyncio.to_thread(run)


def _safe_log(value: str) -> str:
    value = re.sub(r"(?i)((?:password|secret|access[_-]?key|token)\s*[=:]\s*)[^\s,;]+", r"\1[REDACTED]", value)
    return redact_text(value, limit=16384)


def publication_receipt(observation: dict[str, Any], *, materialization_id: UUID, lease_epoch: int) -> dict[str, str]:
    pods = _owned_pods(observation, observation.get("metadata", {}).get("uid"))
    if len(pods) != 1:
        raise ValueError("native build must have exactly one owned Pod")
    statuses = pods[0].get("status", {}).get("containerStatuses", [])
    publisher = next((status for status in statuses if status.get("name") == "publish"), None)
    terminated = (publisher or {}).get("state", {}).get("terminated", {})
    if observation.get("status", {}).get("succeeded") and terminated.get("exitCode") != 0:
        raise ValueError("successful publication requires publisher exit zero")
    message = terminated.get("message", "")
    if not isinstance(message, str) or len(message.encode()) > 4096:
        raise ValueError("invalid publication receipt size")
    receipt = json.loads(message)
    if not isinstance(receipt, dict) or receipt.get("materialization_id") != str(materialization_id) or receipt.get("lease_epoch") != lease_epoch:
        raise ValueError("publication receipt does not belong to this build")
    images = receipt.get("registry_images")
    if not isinstance(images, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in images.items()):
        raise ValueError("publication receipt has invalid images")
    return images


def _phase_error(observation: dict[str, Any], name: str) -> str:
    for pod in _owned_pods(observation, observation.get("metadata", {}).get("uid")):
        for row in (*pod.get("status", {}).get("initContainerStatuses", []), *pod.get("status", {}).get("containerStatuses", [])):
            if row.get("name") != name:
                continue
            message = row.get("state", {}).get("terminated", {}).get("message", "")
            if not isinstance(message, str) or len(message.encode()) > 4096:
                continue
            try:
                receipt = json.loads(message)
            except ValueError:
                continue
            if isinstance(receipt, dict) and isinstance(receipt.get("error"), str):
                return _safe_log(receipt["error"][:500])
    return "check source/cache/registry access and Pod status"


def build_observation(observation: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"observed_at": datetime.now(UTC).isoformat()}
    pods = _owned_pods(observation, observation.get("metadata", {}).get("uid"))
    if len(pods) > 1:
        raise ValueError("native build unexpectedly has multiple owned Pods")
    if pods:
        pod = pods[0]
        result.update(pod_uid=pod["metadata"]["uid"], node_name=pod.get("spec", {}).get("nodeName"))
        phases = []
        for row in (*pod.get("status", {}).get("initContainerStatuses", []), *pod.get("status", {}).get("containerStatuses", [])):
            if row.get("name") not in {"prepare", "build", "publish"}:
                continue
            phase: dict[str, Any] = {"name": row["name"]}
            for kind, detail in row.get("state", {}).items():
                if kind not in {"waiting", "running", "terminated"}:
                    continue
                safe = {key: detail[key] for key in ("exitCode", "signal", "startedAt", "finishedAt") if key in detail}
                reason = detail.get("reason", "")
                if isinstance(reason, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", reason):
                    safe["reason"] = reason
                phase["state"] = {kind: safe}
            phases.append(phase)
        result["phases"] = phases
    if observation.get("builder_log"):
        result["builder_log"] = _safe_log(observation["builder_log"])
    return result


class NativeTaskImageController:
    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], kubernetes: NativeBuildApi,
                 target: ExecutionTargetRuntime, settings: NativeTaskImageSettings) -> None:
        self.sessions, self.kubernetes, self.settings = sessions, kubernetes, settings
        self.target = replace(target, namespace=settings.namespace, runtime_class_name=None)
        self.builder_id = "nebius:" + target.target_id

    async def _outstanding(self, session: AsyncSession) -> list[TaskImageMaterializationAttempt]:
        return list((await session.scalars(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.native_build["target_id"].as_string() == self.target.target_id,
            TaskImageMaterializationAttempt.native_build["capacity_released_at"].as_string().is_(None),
        ))).all())

    async def run_once(self) -> None:
        # Database scans recover missing Jobs, stale epochs and ambiguous creates.
        async with self.sessions() as session:
            attempt_ids = [attempt.id for attempt in await self._outstanding(session)]
        unexpected = False
        for attempt_id in attempt_ids:
            try:
                await self._reconcile(attempt_id)
            except ExecutionProvisioningBlockedError:
                pass  # Waiting for quota or a fresh observation is normal health.
            except Exception as error:
                unexpected = True
                _LOG.warning("Native build reconcile deferred attempt=%s error=%s", attempt_id, type(error).__name__)
        if unexpected:
            raise RuntimeError("Native build reconciliation failed; inspect bounded actuator diagnostics")
        jobs = await self.kubernetes.inventory(self.settings.namespace)
        if len(jobs) >= self.settings.max_concurrent:
            return
        try:
            claimed_attempt_id = await self._claim()
        except ExecutionProvisioningBlockedError:
            return  # The entire claim/reservation transaction was rolled back.
        if claimed_attempt_id is not None:
            try:
                await self._reconcile(claimed_attempt_id)
            except ExecutionProvisioningBlockedError:
                pass

    async def _claim(self) -> UUID | None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            target = await session.get(ServiceExecutionTarget, self.target.target_id)
            if target is None or target.provider != "nebius" or target.desired_state != "active" or target.health_status != "healthy":
                return None
            if len(await self._outstanding(session)) >= self.settings.max_concurrent:
                return None
            row = await claim_task_image_materialization(session, builder_id=self.builder_id, cpu_arch="x86_64",
                                                        nebius_pool_id=self.settings.pool_id)
            if row is None:
                return None
            attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.materialization_id == row.id,
                TaskImageMaterializationAttempt.lease_epoch == row.lease_epoch,
            ).with_for_update())
            assert attempt is not None
            try:
                claim = {**task_image_materialization_payload(row), **self.settings.runtime_configuration()}
                components = derive_task_image_build_components(row.task_config)
                cm, job = render_task_image_job(materialization_id=row.id, lease_epoch=row.lease_epoch,
                                               claim=claim, components=components, target=self.target,
                                               config=self.settings.job_config())
            except (ValueError, TypeError, KeyError):
                await self._fail(session, row, "build_input_unsupported", retryable=False,
                                 message="Unsupported Dockerfile, context, architecture, or task build configuration")
                return None
            for metadata in (cm["metadata"], job["metadata"], job["spec"]["template"]["metadata"]):
                metadata["labels"]["app.kubernetes.io/managed-by"] = _MANAGER
                metadata.setdefault("annotations", {})["loom.openai.com/target-id"] = self.target.target_id
            now = datetime.now(UTC)
            attempt.native_build = {
                "target_id": self.target.target_id, "namespace": self.settings.namespace,
                "job_name": job["metadata"]["name"], "job_uid": None, "state": "reserved",
                "resources": {"vcpu_millis": self.settings.cpu_millis, "memory_mib": self.settings.memory_mib,
                              "storage_mib": self.settings.ephemeral_storage_mib},
                "max_processes": self.settings.max_processes, "reserved_at": now.isoformat(),
                "deadline_at": (now + timedelta(seconds=job["spec"]["activeDeadlineSeconds"])).isoformat(),
                "configmap": cm, "job": job,
            }
            await session.flush()
            await reserve_native_task_image_capacity(session, attempt_id=attempt.id)
            return attempt.id

    def _owned(self, row: TaskImageMaterialization | None, attempt: TaskImageMaterializationAttempt) -> bool:
        return bool(row is not None and row.lease_epoch == attempt.lease_epoch and row.claimed_by == self.builder_id
                    and row.state in _LIVE and row.lease_expires_at and row.lease_expires_at > datetime.now(UTC))

    async def _fail(self, session: AsyncSession, row: TaskImageMaterialization, reason: str, *,
                    retryable: bool, images: dict[str, str] | None = None, message: str | None = None) -> None:
        await fail_task_image_materialization(session, materialization_id=row.id, builder_id=self.builder_id,
                                             lease_epoch=row.lease_epoch, retryable=retryable,
                                             failure_reason=reason, failure_message=_safe_log(message or reason.replace("_", " "))[-2000:],
                                             registry_images=images or {})

    async def _reconcile(self, attempt_id: UUID) -> None:
        # Serialize one attempt across actuator replicas without holding the
        # regional admission lock during Kubernetes calls.
        async with self.sessions() as guard, guard.begin():
            locked = await guard.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 1550))"),
                                        {"key": "native-build:" + str(attempt_id)})
            if locked:
                await self._reconcile_locked(attempt_id)

    async def _reconcile_locked(self, attempt_id: UUID) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
            if attempt is None or not attempt.native_build or attempt.native_build.get("capacity_released_at"):
                return
            native = dict(attempt.native_build)
            row = await session.get(TaskImageMaterialization, attempt.materialization_id, with_for_update=True)
            owned = self._owned(row, attempt)
            demand = owned and await has_nebius_task_image_demand(session, materialization_id=attempt.materialization_id,
                                                                 pool_id=self.settings.pool_id)
            deadline = native.get("deadline_at") or (datetime.fromisoformat(native["reserved_at"]) + timedelta(
                seconds=native["job"]["spec"]["activeDeadlineSeconds"])).isoformat()
            expired = datetime.fromisoformat(deadline) <= datetime.now(UTC)
            if owned and (not demand or expired):
                assert row is not None
                await self._fail(session, row, "build_deadline_exceeded" if expired else "build_cancelled", retryable=True)
                owned = False
            if owned:
                await heartbeat_task_image_materialization(session, materialization_id=attempt.materialization_id,
                                                           builder_id=self.builder_id, lease_epoch=attempt.lease_epoch)
            native["state"] = native.get("state", "reserved") if owned else "cleaning"
            attempt.native_build = native
        observed = await self.kubernetes.observe(native["namespace"], native["job_name"])
        uid = native.get("job_uid")
        if observed is not None and not observed.get("job_missing"):
            actual_uid = observed.get("metadata", {}).get("uid")
            if not actual_uid or uid not in (None, actual_uid) or not _matches(observed, native["job"]):
                raise ValueError("observed build identity differs from its durable attempt")
            uid = actual_uid
        elif observed is not None and uid is None:
            # Recover a create whose response and Job were lost before restart.
            owners = {owner.get("uid") for pod in observed.get("pods", [])
                      if _matches(pod.get("metadata", {}).get("labels", {}), native["job"]["spec"]["template"]["metadata"]["labels"])
                      for owner in pod.get("metadata", {}).get("ownerReferences", [])
                      if owner.get("kind") == "Job" and owner.get("name") == native["job_name"]}
            if len(owners) != 1 or None in owners:
                raise ValueError("orphan build Pod has no unique owner")
            uid = next(iter(owners))
        if uid != native.get("job_uid"):
            native["job_uid"] = uid
            await self._save_native(attempt_id, native)
        if not owned:
            await self._cleanup(attempt_id, native)
            return
        if observed is None or observed.get("job_missing"):
            if uid is not None:
                await self._finish_failure(attempt_id, "build_job_missing", retryable=True)
                await self._cleanup(attempt_id, native)
                return
            # Retry only an unacknowledged frozen create after admission recheck.
            async with self.sessions() as session, session.begin():
                await session.execute(_CAPACITY_ADMISSION_LOCK)
                native = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, revalidate_existing=True)
                native = {**native, "state": "creating"}
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
                assert attempt is not None
                attempt.native_build = native
            observed = await self.kubernetes.ensure(native["configmap"], native["job"])
            native["job_uid"] = observed["metadata"]["uid"]
            await self._save_native(attempt_id, native)
        await self._record_result(attempt_id, observed)

    async def _save_native(self, attempt_id: UUID, native: dict[str, Any]) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
            assert attempt is not None
            attempt.native_build = native

    async def _finish_failure(self, attempt_id: UUID, reason: str, *, retryable: bool) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
            assert attempt is not None
            row = await session.get(TaskImageMaterialization, attempt.materialization_id, with_for_update=True)
            if self._owned(row, attempt):
                assert row is not None
                await self._fail(session, row, reason, retryable=retryable)

    async def _cleanup(self, attempt_id: UUID, native: dict[str, Any]) -> None:
        if await self.kubernetes.delete(native["namespace"], native["job_name"], native.get("job_uid"), configmap=native["configmap"]):
            native.update(state="released", capacity_released_at=datetime.now(UTC).isoformat())
            await self._save_native(attempt_id, native)

    async def _record_result(self, attempt_id: UUID, observed: dict[str, Any]) -> None:
        status = observed.get("status", {})
        terminal = bool(status.get("succeeded") or status.get("failed"))
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
            assert attempt is not None and attempt.native_build is not None
            row = await session.get(TaskImageMaterialization, attempt.materialization_id, with_for_update=True)
            if not self._owned(row, attempt):
                return  # Next DB scan cleans the old epoch without touching its successor.
            assert row is not None
            if row.state == "claimed":
                await start_task_image_materialization(session, materialization_id=row.id, builder_id=self.builder_id,
                                                       lease_epoch=attempt.lease_epoch)
            try:
                summary = build_observation(observed)
                old_uid = attempt.native_build.get("pod_uid")
                if old_uid and summary.get("pod_uid") not in (None, old_uid):
                    raise ValueError("build Pod identity changed")
            except ValueError:
                await self._fail(session, row, "build_pod_identity_changed", retryable=False,
                                 message="Native build created multiple Pods or replaced its recorded Pod")
                return
            native = {**attempt.native_build, **summary, "state": "terminal" if terminal else "pending"}
            attempt.native_build = native
            if not terminal:
                return
            images: dict[str, str] = {}
            invalid = False
            try:
                images = publication_receipt(observed, materialization_id=row.id, lease_epoch=attempt.lease_epoch)
                repository = json.loads(native["configmap"]["data"]["claim.json"])["registry_repository"]
                if any(not re.fullmatch(re.escape(repository) + r"@sha256:[a-f0-9]{64}", ref) for ref in images.values()):
                    raise ValueError("publication outside the frozen task repository")
                if status.get("succeeded"):
                    await complete_task_image_materialization(session, materialization_id=row.id, builder_id=self.builder_id,
                                                             lease_epoch=attempt.lease_epoch, registry_images=images)
                    return
            except (ValueError, TypeError, KeyError):
                invalid, images = True, {}
            if status.get("succeeded") and invalid:
                await self._fail(session, row, "build_publication_receipt_invalid", retryable=False,
                                 message="Publisher receipt is missing, malformed, or differs from the frozen build identity/components/repository")
            else:
                phase = next((phase["name"] for phase in native.get("phases", [])
                              if phase.get("state", {}).get("terminated", {}).get("exitCode", 0)), "job")
                try:
                    await self._fail(session, row, "build_" + phase + "_failed", retryable=phase != "build", images=images,
                                     message=("Native build phase " + phase + " failed. " + native.get("builder_log", ""))
                                     if phase == "build" else "Native build phase " + phase + " failed: " + _phase_error(observed, phase))
                except TaskImageCompletionError:
                    await self._fail(session, row, "build_publication_receipt_invalid", retryable=False,
                                 message="Publisher receipt is missing, malformed, or differs from the frozen build identity/components/repository")
