"""Two-phase Secret writes that never CREATE credential-bearing resources.

This is a storage primitive, not a lifecycle lease or permission grant. Callers
must still authenticate the current operation and select its canonical identity.
"""

from __future__ import annotations

import base64
import copy
import json
import re
from typing import Any

from loom.dev_instance import DevInstanceIdentity
from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_incarnation_storage import (
    STORAGE_BINDING_ANNOTATION,
    STORAGE_BINDING_SHA_ANNOTATION,
    parse_personal_dev_storage_binding,
    personal_dev_storage_annotations,
    validate_personal_dev_storage_identity,
)

_NAMESPACE_UID = "loom.dev/storage-namespace-uid"
_PHASE = "loom.dev/storage-write-phase"
_EPOCH = "loom.dev/storage-operation-epoch"


def _empty(observed: dict[str, Any]) -> bool:
    return (
        isinstance(observed.get("data", {}), dict)
        and not observed.get("data")
        and "stringData" not in observed
        and observed.get("immutable", False) is False
    )


def _stale_placeholder(
    observed: dict[str, Any],
    identity: DevInstanceIdentity,
    namespace_uid: str,
    name: str,
    operation_epoch: int | None,
) -> bool:
    """Only an inert placeholder owned by this logical environment is disposable."""
    try:
        _object_identity(observed)
        metadata = observed["metadata"]
        annotations = metadata.get("annotations", {})
        old_uid = annotations.get(_NAMESPACE_UID)
        if (
            observed.get("apiVersion") != "v1"
            or observed.get("kind") != "Secret"
            or observed.get("type") != "Opaque"
            or not _empty(observed)
            or metadata.get("name") != name
            or metadata.get("namespace") != identity.namespace
            or metadata.get("ownerReferences")
            or metadata.get("finalizers")
            or metadata.get("deletionTimestamp")
            or annotations.get(_PHASE) != "empty"
            or not isinstance(old_uid, str)
            or not old_uid.strip()
            or old_uid == namespace_uid
            or set(annotations)
            != {
                STORAGE_BINDING_ANNOTATION,
                STORAGE_BINDING_SHA_ANNOTATION,
                _NAMESPACE_UID,
                _PHASE,
                *(() if operation_epoch is None else (_EPOCH,)),
            }
        ):
            return False
        _validate_epoch(annotations, operation_epoch)
        binding = parse_personal_dev_storage_binding(
            annotations[STORAGE_BINDING_ANNOTATION].encode("ascii"),
            expected_sha256=annotations[STORAGE_BINDING_SHA_ANNOTATION],
        )
        current = identity.storage_binding
        return (
            current is not None
            and binding.layout == "incarnation-v1"
            and binding.environment_name == current.environment_name
            and binding.subject_id == current.subject_id
            and binding.owner_user_id == current.owner_user_id
            and binding.owner_team_id == current.owner_team_id
        )
    except (AttributeError, KeyError, TypeError, ValueError, DevInstanceRuntimeError):
        return False


def _validate_epoch(annotations: dict[str, Any], requested: int | None) -> None:
    stored = annotations.get(_EPOCH)
    if requested is None:
        if stored is not None:
            raise DevInstanceRuntimeError("storage Secret operation epoch is required")
    elif (
        not isinstance(stored, str)
        or re.fullmatch(r"[1-9][0-9]{0,18}", stored) is None
        or int(stored) > requested
    ):
        raise DevInstanceRuntimeError("storage Secret operation epoch is newer or invalid")


def _validate_existing(
    observed: dict[str, Any],
    identity: DevInstanceIdentity,
    namespace_uid: str,
    name: str,
    operation_epoch: int | None,
) -> str:
    _object_identity(observed)
    metadata = observed["metadata"]
    annotations = metadata.get("annotations", {})
    expected = {**personal_dev_storage_annotations(identity), _NAMESPACE_UID: namespace_uid}
    if (
        observed.get("apiVersion") != "v1"
        or observed.get("kind") != "Secret"
        or observed.get("type", "Opaque") != "Opaque"
        or metadata.get("name") != name
        or metadata.get("namespace") != identity.namespace
        or metadata.get("ownerReferences")
        or metadata.get("finalizers")
        or metadata.get("deletionTimestamp")
        or not isinstance(annotations, dict)
        or any(annotations.get(key) != value for key, value in expected.items())
    ):
        raise DevInstanceRuntimeError("storage Secret belongs to another namespace incarnation")
    _validate_epoch(annotations, operation_epoch)
    phase = annotations.get(_PHASE)
    if phase == "empty":
        if not _empty(observed):
            raise DevInstanceRuntimeError("storage Secret placeholder is not empty")
    elif phase != "ready":
        raise DevInstanceRuntimeError("storage Secret has unknown provenance")
    return str(phase)


def _object_identity(document: dict[str, Any]) -> tuple[str, str]:
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        raise DevInstanceRuntimeError("storage Secret metadata is unavailable")
    uid, version = metadata.get("uid"), metadata.get("resourceVersion")
    if not isinstance(uid, str) or not uid or not isinstance(version, str) or not version:
        raise DevInstanceRuntimeError("storage Secret UID/version is unavailable")
    return uid, version


async def _read_secret(
    kubectl: KubectlClient,
    namespace: str,
    name: str,
) -> dict[str, Any] | None:
    result = await kubectl.runner.run(
        kubectl._argv(
            "get",
            "secret",
            name,
            "--namespace",
            namespace,
            "--ignore-not-found",
            "-o",
            "json",
        )
    )
    if not result.stdout.strip():
        return None
    try:
        document = json.loads(result.stdout)
    except (TypeError, ValueError):
        raise DevInstanceRuntimeError("storage Secret readback is invalid") from None
    if not isinstance(document, dict):
        raise DevInstanceRuntimeError("storage Secret readback is invalid")
    return document or None


async def read_storage_secret_data(
    kubectl: KubectlClient,
    identity: DevInstanceIdentity,
    name: str,
    *,
    operation_epoch: int,
) -> dict[str, bytes] | None:
    """Treat only an authenticated empty placeholder as missing credentials."""
    validate_personal_dev_storage_identity(identity)
    namespace = await kubectl.read_storage_namespace(identity)
    if identity.storage_binding is None or namespace is None:
        raise DevInstanceRuntimeError("bound storage namespace is unavailable")
    observed = await _read_secret(kubectl, identity.namespace, name)
    if observed is None:
        return None
    if _stale_placeholder(
        observed, identity, kubectl._namespace_uid(namespace), name, operation_epoch
    ):
        return None
    if (
        _validate_existing(
            observed, identity, kubectl._namespace_uid(namespace), name, operation_epoch
        )
        == "empty"
    ):
        return None
    return _secret_data(observed)


def _secret_data(document: dict[str, Any]) -> dict[str, bytes]:
    """Compare Secret bytes regardless of data/stringData wire encoding."""
    try:
        data = document.get("data", {})
        strings = document.get("stringData", {})
        if not isinstance(data, dict):
            raise ValueError
        if not isinstance(strings, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in (*data.items(), *strings.items())
        ):
            raise ValueError
        return {
            **{key: base64.b64decode(value, validate=True) for key, value in data.items()},
            **{key: value.encode("utf-8") for key, value in strings.items()},
        }
    except (TypeError, ValueError):
        raise DevInstanceRuntimeError("storage Secret data is invalid") from None


async def write_storage_secret(
    kubectl: KubectlClient,
    identity: DevInstanceIdentity,
    document: dict[str, Any],
    *,
    create_only: bool = False,
    operation_epoch: int | None = None,
) -> None:
    """Write a bound Secret by acknowledged inert CREATE and UID-pinned PUT.

    A delayed CREATE can at worst leave an empty placeholder. An acknowledged
    placeholder plus a subsequent authoritative namespace read establishes the
    child UID in that namespace incarnation. All meaningful writes then use
    UPDATE-only semantics; they cannot create absent or replace successor UIDs.
    """
    validate_personal_dev_storage_identity(identity)
    if identity.storage_binding is None:
        raise DevInstanceRuntimeError("two-phase Secret writes require bound storage")
    if operation_epoch is not None and (
        type(operation_epoch) is not int or not 0 < operation_epoch < 2**63
    ):
        raise DevInstanceRuntimeError("storage Secret operation epoch is invalid")
    final = copy.deepcopy(document)
    metadata = final.get("metadata")
    if (
        final.get("apiVersion") != "v1"
        or final.get("kind") != "Secret"
        or final.get("type", "Opaque") != "Opaque"
        or not isinstance(metadata, dict)
        or metadata.get("namespace") != identity.namespace
        or not isinstance(metadata.get("name"), str)
        or not metadata["name"]
        or len(metadata["name"]) > 253
        or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", metadata["name"]) is None
        or set(metadata) - {"name", "namespace", "labels", "annotations"}
    ):
        raise DevInstanceRuntimeError("storage Secret write target is invalid")
    name = metadata["name"]
    desired_data = _secret_data(final)
    namespace = await kubectl.read_storage_namespace(identity)
    if namespace is None:
        raise DevInstanceRuntimeError("storage namespace is unavailable")
    namespace_uid = kubectl._namespace_uid(namespace)
    binding_annotations = {
        **personal_dev_storage_annotations(identity),
        _NAMESPACE_UID: namespace_uid,
    }
    annotations = metadata.get("annotations", {})
    if (
        not isinstance(annotations, dict)
        or any(
            key in annotations and annotations[key] != value
            for key, value in binding_annotations.items()
        )
        or _PHASE in annotations
        or _EPOCH in annotations
    ):
        raise DevInstanceRuntimeError("storage Secret annotations conflict with ownership")
    epoch_annotations = {} if operation_epoch is None else {_EPOCH: str(operation_epoch)}
    observed = await _read_secret(kubectl, identity.namespace, name)
    if observed is not None and _stale_placeholder(
        observed, identity, namespace_uid, name, operation_epoch
    ):
        uid, version = _object_identity(observed)
        # Both checks are server-side: a concurrent fill changes resourceVersion,
        # while replacement changes UID. Neither may be deleted as "empty".
        await kubectl.runner.run(
            kubectl._argv(
                "delete", f"--raw=/api/v1/namespaces/{identity.namespace}/secrets/{name}", "-f", "-"
            ),
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "preconditions": {"uid": uid, "resourceVersion": version},
                }
            ),
        )
        observed = None
    if observed is None:
        placeholder = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "data": {},
            "metadata": {
                **metadata,
                "annotations": {
                    **annotations,
                    **binding_annotations,
                    **epoch_annotations,
                    _PHASE: "empty",
                },
            },
        }
        created = await kubectl.runner.run(
            kubectl._argv("create", "-f", "-", "-o", "json"),
            stdin=json.dumps(placeholder),
        )
        try:
            observed = json.loads(created.stdout)
        except (TypeError, ValueError):
            raise DevInstanceRuntimeError("storage Secret creation readback is invalid") from None
    if not isinstance(observed, dict):
        raise DevInstanceRuntimeError("storage Secret creation readback is invalid")
    uid, version = _object_identity(observed)
    phase = _validate_existing(observed, identity, namespace_uid, name, operation_epoch)
    if phase == "ready" and create_only:
        raise DevInstanceRuntimeError(
            "storage Secret is already initialized or has unknown provenance"
        )
    if (
        phase == "ready"
        and operation_epoch is not None
        and observed["metadata"]["annotations"][_EPOCH] == str(operation_epoch)
        and (
            _secret_data(observed) != desired_data
            or observed.get("immutable", False) != final.get("immutable", False)
        )
    ):
        # A caller may have prepared random credentials before another retry
        # won. A fresh GET here is not permission to overwrite that winner.
        raise DevInstanceRuntimeError("storage Secret operation already committed different data")
    # This read MUST follow the acknowledged CREATE/read of the child. A
    # pre-CREATE namespace read alone leaves the same cross-object race open.
    current = await kubectl.read_storage_namespace(identity)
    if current is None or kubectl._namespace_uid(current) != namespace_uid:
        raise DevInstanceRuntimeError("storage namespace changed before Secret update")
    metadata.update(uid=uid, resourceVersion=version)
    metadata["annotations"] = {
        **annotations,
        **binding_annotations,
        **epoch_annotations,
        _PHASE: "ready",
    }
    await kubectl.runner.run(
        kubectl._argv("replace", "-f", "-"),
        stdin=json.dumps(final),
    )
