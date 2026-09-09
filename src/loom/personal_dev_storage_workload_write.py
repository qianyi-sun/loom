"""Namespace-incarnation fenced writes for personal Jobs and Deployments.

CREATE is always inert. Activation uses only acknowledged object UID/RV PUTs.
This does not fence delayed writes by Kubernetes' own descendant controllers.
"""

from __future__ import annotations

import asyncio
import copy
import json
import re
from typing import Any
from uuid import UUID

from loom.dev_instance import DevInstanceIdentity
from loom.dev_instance_runtime import (
    CommandResult,
    DevInstanceRuntimeError,
    KubectlClient,
    KubernetesResourceVersionConflictError,
    WorkloadStatusConflictError,
)
from loom.personal_dev_incarnation_storage import (
    personal_dev_storage_annotations,
    validate_personal_dev_storage_identity,
)

_UID = "loom.dev/storage-namespace-uid"
_EPOCH = "loom.dev/storage-operation-epoch"
_PHASE = "loom.dev/workload-write-phase"
_SPEC = "loom.dev/workload-requested-spec"
_KINDS = {"Deployment": ("apps/v1", "deployment"), "Job": ("batch/v1", "job")}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _decode(payload: str, *, optional: bool = False) -> dict[str, Any] | None:
    if optional and not payload.strip():
        return None
    try:
        value = json.loads(payload)
    except (TypeError, ValueError, RecursionError):
        raise DevInstanceRuntimeError("workload JSON readback is invalid") from None
    if not isinstance(value, dict) or not value:
        raise DevInstanceRuntimeError("workload JSON readback must be a nonempty object")
    return value


def _object(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict) or not isinstance(document.get("metadata"), dict):
        raise DevInstanceRuntimeError("workload object metadata is unavailable")
    metadata = document["metadata"]
    if any(not isinstance(metadata.get(key), str) or not metadata[key] for key in ("uid", "resourceVersion")):
        raise DevInstanceRuntimeError("workload UID/version is unavailable")
    return document


def _inactive(spec: dict[str, Any], kind: str) -> dict[str, Any]:
    result = copy.deepcopy(spec)
    result["suspend" if kind == "Job" else "replicas"] = True if kind == "Job" else 0
    return result


def _authority_body(document: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(document)
    body.pop("status", None)
    metadata = body["metadata"]
    metadata.pop("resourceVersion", None)
    metadata.pop("managedFields", None)
    if document["kind"] == "Deployment":
        metadata.get("annotations", {}).pop("deployment.kubernetes.io/revision", None)
    return body


async def _replace_workload(
    kubectl: KubectlClient, identity: DevInstanceIdentity,
    observed: dict[str, Any], desired: dict[str, Any], *, dry_run: bool = False,
) -> CommandResult:
    try:
        return await kubectl.runner.run(kubectl._argv(
            "replace", *(["--dry-run=server", "-o", "json"] if dry_run else []), "-f", "-",
        ), stdin=_canonical(desired))
    except KubernetesResourceVersionConflictError:
        status_only = False
        try:
            async with asyncio.timeout(10):
                namespace = await kubectl.read_storage_namespace(identity)
                if namespace is not None and kubectl._namespace_uid(namespace) == observed["metadata"]["annotations"][_UID]:
                    reply = await kubectl.runner.run(kubectl._argv(
                        "get", _KINDS[observed["kind"]][1], observed["metadata"]["name"],
                        "--namespace", identity.namespace, "-o", "json",
                    ))
                    latest = _object(_decode(reply.stdout))
                    status_only = (
                        latest["metadata"]["resourceVersion"] != observed["metadata"]["resourceVersion"]
                        and _authority_body(latest) == _authority_body(observed)
                    )
        except (DevInstanceRuntimeError, KeyError, TypeError, ValueError, AttributeError, TimeoutError):
            pass  # Unavailable or ambiguous observation cannot authorize a retry.
        if status_only:
            raise WorkloadStatusConflictError("workload status advanced during its versioned update") from None
        raise


def _replacement(
    observed: dict[str, Any], desired: dict[str, Any], spec: dict[str, Any],
) -> dict[str, Any]:
    result = copy.deepcopy(desired)
    result["metadata"].update({key: observed["metadata"][key] for key in ("uid", "resourceVersion")})
    result["spec"] = copy.deepcopy(spec)
    if desired["kind"] == "Job":
        # Job's selector and controller labels are generated from its UID at
        # CREATE. Retain exactly those fields, not arbitrary observed pod fields.
        uid, name = observed["metadata"]["uid"], desired["metadata"]["name"]
        selector = {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}
        if observed["spec"].get("selector") != selector:
            raise DevInstanceRuntimeError("workload Job selector differs from its UID")
        labels = {
            "batch.kubernetes.io/controller-uid": uid, "controller-uid": uid,
            "batch.kubernetes.io/job-name": name, "job-name": name,
        }
        observed_labels = observed["spec"]["template"]["metadata"].get("labels", {})
        if any(observed_labels.get(key) != value for key, value in labels.items()):
            raise DevInstanceRuntimeError("workload Job controller labels are invalid")
        result["spec"]["selector"] = selector
        result["spec"]["template"].setdefault("metadata", {}).setdefault("labels", {}).update(labels)
    return result


def _failed_previous_attempt(observed: dict[str, Any], desired: dict[str, Any]) -> bool:
    """A terminal failure may be retried by a different authenticated attempt."""
    try:
        previous = UUID(observed["metadata"]["labels"]["loom.dev/attempt"])
        requested = UUID(desired["metadata"]["labels"]["loom.dev/attempt"])
        status = observed.get("status", {})
        conditions = status.get("conditions", [])
        terminal = {item["type"] for item in conditions if item.get("status") == "True"}
        return (
            previous.int != 0 and requested.int != 0 and previous != requested
            and {"Failed", "FailureTarget"} <= terminal and "Complete" not in terminal
            and all(status.get(key, 0) == 0 for key in ("active", "ready", "terminating"))
            and not any(status.get("uncountedTerminatedPods", {}).values())
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _attempt(metadata: dict[str, Any]) -> tuple[int, UUID] | None:
    labels = metadata.get("labels", {})
    if not isinstance(labels, dict):
        raise DevInstanceRuntimeError("workload attempt labels are invalid")
    attempt, sequence = labels.get("loom.dev/attempt"), labels.get("loom.dev/attempt-sequence")
    if attempt is None and sequence is None:
        return None
    try:
        if not isinstance(sequence, str) or re.fullmatch(r"0|[1-9][0-9]{0,18}", sequence) is None:
            raise ValueError
        parsed = UUID(attempt)
        if parsed.int == 0 or str(parsed) != attempt or int(sequence) >= 2**63:
            raise ValueError
        return int(sequence), parsed
    except (ValueError, TypeError, AttributeError):
        raise DevInstanceRuntimeError("workload attempt authority is invalid") from None


async def _delete_failed_job(kubectl: KubectlClient, observed: dict[str, Any]) -> None:
    metadata = observed["metadata"]
    namespace, name, uid = metadata["namespace"], metadata["name"], metadata["uid"]
    await kubectl.runner.run(kubectl._argv(
        "delete", f"--raw=/apis/batch/v1/namespaces/{namespace}/jobs/{name}", "-f", "-",
    ), stdin=_canonical({
        "apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
        "preconditions": {"uid": uid, "resourceVersion": metadata["resourceVersion"]},
    }))
    try:
        async with asyncio.timeout(30):
            while True:
                reply = await kubectl.runner.run(kubectl._argv(
                    "get", "job", name, "--namespace", namespace, "--ignore-not-found", "-o", "json",
                ), timeout_seconds=10)
                current = _decode(reply.stdout, optional=True)
                if not current:
                    return
                if _object(current)["metadata"]["uid"] != uid:
                    raise DevInstanceRuntimeError("failed workload Job was concurrently replaced")
                await asyncio.sleep(0.1)
    except TimeoutError:
        raise DevInstanceRuntimeError("failed workload Job deletion did not finish") from None


async def write_storage_workload(
    kubectl: KubectlClient,
    identity: DevInstanceIdentity,
    document: dict[str, Any],
    *,
    operation_epoch: int,
) -> None:
    """Install a bound workload without any executable CREATE or force fallback."""
    validate_personal_dev_storage_identity(identity)
    if identity.storage_binding is None or type(operation_epoch) is not int or not 0 < operation_epoch < 2**63:
        raise DevInstanceRuntimeError("workload write requires bound storage and a valid epoch")
    final = copy.deepcopy(document)
    kind = final.get("kind")
    metadata, spec = final.get("metadata"), final.get("spec")
    if (
        kind not in _KINDS or final.get("apiVersion") != _KINDS[kind][0]
        or set(final) - {"apiVersion", "kind", "metadata", "spec"}
        or not isinstance(metadata, dict) or not isinstance(spec, dict)
        or metadata.get("namespace") != identity.namespace
        or not isinstance(metadata.get("name"), str)
        or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?", metadata["name"]) is None
        or set(metadata) - {"name", "namespace", "labels", "annotations"}
        or not isinstance(spec.get("template"), dict)
    ):
        raise DevInstanceRuntimeError("workload write target is invalid")
    if kind == "Job" and (spec.get("manualSelector") or "selector" in spec):
        raise DevInstanceRuntimeError("workload Job must use an API-generated selector")
    encoded = _canonical(spec)
    requested_attempt = _attempt(metadata)
    if len(encoded.encode()) > 64 * 1024:
        raise DevInstanceRuntimeError("workload requested spec exceeds its byte bound")
    namespace = await kubectl.read_storage_namespace(identity)
    if namespace is None:
        raise DevInstanceRuntimeError("workload storage namespace is unavailable")
    namespace_uid = kubectl._namespace_uid(namespace)
    owners = [{"apiVersion": "v1", "kind": "Namespace", "name": identity.namespace, "uid": namespace_uid}]
    # Namespaced workloads may depend on a cluster-scoped Namespace. A delayed
    # inert CREATE carries the deleted UID, so GC can collect it even after the
    # namespace name is reused. This is cleanup, never activation authority.
    metadata["ownerReferences"] = owners
    ownership = {**personal_dev_storage_annotations(identity), _UID: namespace_uid}
    annotations = metadata.get("annotations", {})
    if not isinstance(annotations, dict) or (set(ownership) | {_EPOCH, _PHASE, _SPEC}) & set(annotations):
        raise DevInstanceRuntimeError("workload annotations conflict with ownership")
    name, resource = metadata["name"], _KINDS[kind][1]
    reply = await kubectl.runner.run(kubectl._argv(
        "get", resource, name, "--namespace", identity.namespace, "--ignore-not-found", "-o", "json",
    ))
    observed = _decode(reply.stdout, optional=True)
    created = not observed
    if not observed:
        staged = copy.deepcopy(final)
        staged["spec"] = _inactive(spec, kind)
        staged["metadata"]["annotations"] = {
            **annotations, **ownership, _EPOCH: str(operation_epoch), _PHASE: "staged", _SPEC: encoded,
        }
        reply = await kubectl.runner.run(
            kubectl._argv("create", "-f", "-", "-o", "json"), stdin=_canonical(staged),
        )
        observed = _decode(reply.stdout)
    observed = _object(observed)
    stored = observed["metadata"].get("annotations", {})
    epoch = stored.get(_EPOCH) if isinstance(stored, dict) else None
    if (
        observed.get("kind") != kind or observed.get("apiVersion") != final["apiVersion"]
        or observed["metadata"].get("name") != name
        or observed["metadata"].get("namespace") != identity.namespace
        or observed["metadata"].get("ownerReferences") != owners
        or any(observed["metadata"].get(key) for key in ("finalizers", "deletionTimestamp"))
        or not isinstance(stored, dict) or any(stored.get(key) != value for key, value in ownership.items())
        or not isinstance(epoch, str) or re.fullmatch(r"[1-9][0-9]{0,18}", epoch) is None
        or int(epoch) > operation_epoch or stored.get(_PHASE) not in {"staged", "ready"}
        or not isinstance(stored.get(_SPEC), str) or len(stored[_SPEC].encode()) > 64 * 1024
    ):
        raise DevInstanceRuntimeError("workload belongs to another namespace incarnation or epoch")
    stored_attempt = _attempt(observed["metadata"])
    if (stored_attempt is None) != (requested_attempt is None):
        raise DevInstanceRuntimeError("workload attempt authority cannot be added or removed")
    if int(epoch) == operation_epoch and stored_attempt is not None and requested_attempt is not None:
        if requested_attempt[0] < stored_attempt[0] or (
            requested_attempt[0] == stored_attempt[0] and requested_attempt[1] != stored_attempt[1]
        ):
            raise DevInstanceRuntimeError("workload attempt was superseded")
    previous_spec = _decode(stored[_SPEC])
    if not isinstance(previous_spec, dict) or _canonical(previous_spec) != stored[_SPEC]:
        raise DevInstanceRuntimeError("workload requested spec is invalid")
    current = await kubectl.read_storage_namespace(identity)
    if current is None or kubectl._namespace_uid(current) != namespace_uid:
        raise DevInstanceRuntimeError("storage namespace changed before workload update")
    if created:
        # CREATE acknowledgement can precede the controller's initial status
        # update. Observe that one object again BEFORE attempting any CAS. Only
        # status/controller bookkeeping may differ; new spec, phase, generation,
        # owner, epoch, UID or deletion state is not fresh write authority.
        reply = await kubectl.runner.run(kubectl._argv(
            "get", resource, name, "--namespace", identity.namespace, "-o", "json",
        ))
        latest = _object(_decode(reply.stdout))
        if _authority_body(latest) != _authority_body(observed):
            raise DevInstanceRuntimeError("acknowledged workload changed before activation")
        observed = latest
    # Ask the API to normalize the recorded desired spec using UPDATE defaults,
    # without mutating anything. This detects stored-template drift while keeping
    # Kubernetes defaulting rules out of a hand-maintained client allowlist.
    previous = _replacement(observed, final, previous_spec)
    if stored[_PHASE] == "staged":
        previous["spec"] = _inactive(previous["spec"], kind)
    normalized = await _replace_workload(kubectl, identity, observed, previous, dry_run=True)
    normalized_spec = _object(_decode(normalized.stdout))["spec"]
    if normalized_spec != observed.get("spec"):
        raise DevInstanceRuntimeError("workload persisted template differs from its recorded intent")
    if kind == "Job" and previous_spec != spec:
        raise DevInstanceRuntimeError("workload Job immutable intent changed")
    if kind == "Job" and stored[_PHASE] == "ready" and _failed_previous_attempt(observed, final):
        # Successful and active Jobs keep their UID. A genuinely failed previous
        # attempt is deleted only after authenticating its exact intent, UID/RV
        # and terminal state. Re-creation still passes through inert staging.
        await _delete_failed_job(kubectl, observed)
        await write_storage_workload(kubectl, identity, document, operation_epoch=operation_epoch)
        return
    final = _replacement(observed, final, spec)
    final["metadata"]["annotations"] = {
        **annotations, **ownership, _EPOCH: str(operation_epoch), _PHASE: "ready", _SPEC: encoded,
    }
    if kind == "Deployment" and "deployment.kubernetes.io/revision" in observed["metadata"].get("annotations", {}):
        final["metadata"]["annotations"]["deployment.kubernetes.io/revision"] = observed["metadata"]["annotations"]["deployment.kubernetes.io/revision"]
    # PUT cannot create a missing object and UID/RV prevents replacing a newer
    # object. Do not retry conflicts internally with freshly read authority.
    await _replace_workload(kubectl, identity, observed, final)
