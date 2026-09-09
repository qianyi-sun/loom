"""A namespace-owned attempt floor that outlives individual migration Jobs."""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from loom.dev_instance import DevInstanceIdentity
from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations

_UID = "loom.dev/storage-namespace-uid"
_PREFIX = "loom-workload-fence-"


def _json(document: dict[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _decode(payload: str, *, optional: bool = False) -> dict[str, Any] | None:
    if optional and not payload.strip():
        return None
    try:
        value = json.loads(payload)
        if not isinstance(value, dict) or not value:
            raise ValueError
        return value
    except (TypeError, ValueError, RecursionError):
        raise DevInstanceRuntimeError("workload reservation readback is invalid") from None


async def _read(kubectl: KubectlClient, namespace: str, name: str) -> dict[str, Any] | None:
    reply = await kubectl.runner.run(kubectl._argv(
        "get", "configmap", name, "--namespace", namespace, "--ignore-not-found", "-o", "json",
    ))
    return _decode(reply.stdout, optional=True)


def _owned(observed: Any, desired: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(observed, dict) or not isinstance(observed.get("metadata"), dict):
        raise DevInstanceRuntimeError("workload reservation object is unavailable")
    metadata, expected = observed["metadata"], desired["metadata"]
    if (
        observed.get("apiVersion") != "v1" or observed.get("kind") != "ConfigMap"
        or observed.get("binaryData") or observed.get("immutable")
        or any(metadata.get(key) for key in ("deletionTimestamp", "finalizers"))
        or any(not isinstance(metadata.get(key), str) or not metadata[key] for key in ("uid", "resourceVersion"))
        or any(metadata.get(key) != expected[key] for key in ("name", "namespace", "labels", "annotations", "ownerReferences"))
        or not isinstance(observed.get("data"), dict)
        or set(observed["data"]) != set(desired["data"])
        or any(not isinstance(value, str) for value in observed["data"].values())
        or any(observed["data"][key] != desired["data"][key] for key in ("job", "intent"))
    ):
        raise DevInstanceRuntimeError("workload reservation ownership or immutable intent differs")
    return observed


def _order(data: dict[str, str]) -> tuple[int, int, UUID]:
    try:
        if re.fullmatch(r"[1-9][0-9]{0,18}", data["epoch"]) is None:
            raise ValueError
        if re.fullmatch(r"0|[1-9][0-9]{0,18}", data["sequence"]) is None:
            raise ValueError
        epoch, sequence, attempt = int(data["epoch"]), int(data["sequence"]), UUID(data["attempt"])
        if epoch >= 2**63 or sequence >= 2**63 or attempt.int == 0 or str(attempt) != data["attempt"]:
            raise ValueError
        return epoch, sequence, attempt
    except (KeyError, TypeError, ValueError, AttributeError):
        raise DevInstanceRuntimeError("workload reservation attempt is invalid") from None


@dataclass(frozen=True)
class JobAttemptReservation:
    identity: DevInstanceIdentity
    namespace_uid: str
    document: dict[str, Any]

    async def verify(self, kubectl: KubectlClient) -> None:
        namespace = await kubectl.read_storage_namespace(self.identity)
        if namespace is None or kubectl._namespace_uid(namespace) != self.namespace_uid:
            raise DevInstanceRuntimeError("workload reservation namespace changed")
        observed = _owned(await _read(kubectl, self.identity.namespace, self.document["metadata"]["name"]), self.document)
        if observed["metadata"]["uid"] != self.document["metadata"]["uid"] or observed["data"] != self.document["data"]:
            raise DevInstanceRuntimeError("workload reservation attempt was superseded")


async def reserve_job_attempt(
    kubectl: KubectlClient, identity: DevInstanceIdentity, *, namespace_uid: str,
    job_name: str, intent: str, operation_epoch: int, attempt: tuple[int, UUID],
) -> JobAttemptReservation:
    if (
        identity.storage_binding is None or not namespace_uid
        or not isinstance(job_name, str)
        or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]{0,231}[a-z0-9])?", job_name) is None
        or len(_PREFIX + job_name) > 253
        or not isinstance(intent, str) or len(intent.encode()) > 64 * 1024
    ):
        raise DevInstanceRuntimeError("workload reservation target is invalid")
    try:
        parsed_intent = json.loads(intent)
        if not isinstance(parsed_intent, dict) or not isinstance(parsed_intent.get("template"), dict) or _json(parsed_intent) != intent:
            raise ValueError
    except (TypeError, ValueError, RecursionError):
        raise DevInstanceRuntimeError("workload reservation intent is not canonical") from None
    annotations = {**personal_dev_storage_annotations(identity), _UID: namespace_uid}
    desired: dict[str, Any] = {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": _PREFIX + job_name, "namespace": identity.namespace,
            "labels": {"app.kubernetes.io/managed-by": "loom-dev-instance-controller",
                       "app.kubernetes.io/part-of": "loom", "loom.dev/instance": identity.name},
            "annotations": annotations,
            "ownerReferences": [{"apiVersion": "v1", "kind": "Namespace", "name": identity.namespace, "uid": namespace_uid}],
        },
        "data": {"job": job_name, "intent": intent, "epoch": str(operation_epoch),
                 "sequence": str(attempt[0]), "attempt": str(attempt[1])},
    }
    requested = _order(desired["data"])
    namespace = await kubectl.read_storage_namespace(identity)
    if namespace is None or kubectl._namespace_uid(namespace) != namespace_uid:
        raise DevInstanceRuntimeError("workload reservation namespace changed")
    observed = await _read(kubectl, identity.namespace, desired["metadata"]["name"])
    if observed is None:
        try:
            reply = await kubectl.runner.run(kubectl._argv("create", "-f", "-", "-o", "json"), stdin=_json(desired))
            observed = _decode(reply.stdout)
        except DevInstanceRuntimeError:
            # CREATE may have persisted before a lost reply, or another caller
            # may have created the exact record. Only inspect; never retry CREATE.
            observed = await _read(kubectl, identity.namespace, desired["metadata"]["name"])
            if observed is None:
                raise
    observed = _owned(observed, desired)
    existing = _order(observed["data"])
    if requested[:2] < existing[:2] or (requested[:2] == existing[:2] and requested[2] != existing[2]):
        raise DevInstanceRuntimeError("workload reservation attempt was superseded")
    reservation = JobAttemptReservation(identity, namespace_uid, copy.deepcopy(observed))
    await reservation.verify(kubectl)
    if requested[:2] > existing[:2]:
        desired["metadata"].update({key: observed["metadata"][key] for key in ("uid", "resourceVersion")})
        reply = await kubectl.runner.run(kubectl._argv("replace", "-f", "-", "-o", "json"), stdin=_json(desired))
        observed = _owned(_decode(reply.stdout), desired)
        if observed["data"] != desired["data"] or observed["metadata"]["uid"] != desired["metadata"]["uid"]:
            raise DevInstanceRuntimeError("workload reservation update was not acknowledged")
        reservation = JobAttemptReservation(identity, namespace_uid, copy.deepcopy(observed))
        await reservation.verify(kubectl)
    return reservation
