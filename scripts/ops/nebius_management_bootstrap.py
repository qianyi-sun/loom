"""Create-only management Namespace and generated credentials for a protected installer.

The caller qualifies candidate, resource fit and dedicated installation authority
first. This stage neither activates management nor grants its runtime permissions.
The caller must retain an independent started marker if the entire state tree is
lost; a missing tree must never be assumed to authorize a new installation.
"""
from __future__ import annotations

import json
import re
import ssl
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import (
    HTTPSMaterialAPI,
    ManagementBinding,
    MaterialAPI,
    deliver_material,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import _namespace

_LABEL = "loom.nebius/management-installation"
_OPERATION = "loom.nebius/management-bootstrap-operation"
_STAGES = {"namespace_prepared", "namespace_create_intent", "namespace_created", "material_intent", "bootstrapped"}


class BootstrapError(RuntimeError):
    """Payload-free error; preserve Namespace, credentials and recovery evidence."""


@dataclass(frozen=True)
class BootstrapBinding:
    installation_id: str
    namespace: str
    kube_system_uid: str

    def __post_init__(self) -> None:
        try:
            for value in (self.installation_id, self.kube_system_uid):
                if str(UUID(value)) != value or UUID(value).int == 0:
                    raise ValueError()
            if (not isinstance(self.namespace, str) or len(self.namespace) > 53
                    or re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", self.namespace) is None):
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise BootstrapError("invalid management bootstrap identity") from None


class BootstrapAPI(Protocol):
    def verify_cluster(self, binding: BootstrapBinding) -> None: ...
    def get_namespace(self) -> dict[str, Any] | None: ...
    def create_namespace(self, document: dict[str, Any]) -> None:
        """Exactly one fixed Namespace create; no retry/apply/replace."""
        ...

    def material_api(self, binding: ManagementBinding) -> AbstractContextManager[MaterialAPI]: ...


class HTTPSBootstrapAPI(ManagementKubernetesTransport):
    """Only this installation's fixed Namespace and generated Secret operations."""

    error_type = BootstrapError

    def __init__(self, *, binding: BootstrapBinding, api_server: str, ssl_context: ssl.SSLContext,
                 token: str | None = None):
        self.binding, self._ssl_context, self._token = binding, ssl_context, token
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_cluster(self, binding: BootstrapBinding) -> None:
        if binding != self.binding:
            raise BootstrapError("management cluster binding differs")
        actual = self._request("GET", "/api/v1/namespaces/kube-system")
        try:
            if (actual is None or actual.get("kind") != "Namespace"
                    or actual["metadata"]["name"] != "kube-system" or _uid(actual) != binding.kube_system_uid
                    or actual["metadata"].get("deletionTimestamp") or actual["metadata"].get("ownerReferences")):
                raise ValueError()
        except Exception:
            raise BootstrapError("management cluster identity differs") from None

    def get_namespace(self) -> dict[str, Any] | None:
        return self._request("GET", "/api/v1/namespaces/" + self.binding.namespace)

    def create_namespace(self, document: dict[str, Any]) -> None:
        try:
            operation = document["metadata"]["annotations"][_OPERATION]
            if document != _document(self.binding, operation):
                raise ValueError()
        except Exception:
            raise BootstrapError("Namespace outside management bootstrap scope") from None
        self.verify_cluster(self.binding)
        self._request("POST", "/api/v1/namespaces", document=document)

    def material_api(self, binding: ManagementBinding) -> HTTPSMaterialAPI:
        if (binding.installation_id, binding.namespace, binding.kube_system_uid) != (
            self.binding.installation_id, self.binding.namespace, self.binding.kube_system_uid,
        ):
            raise BootstrapError("credentials outside management bootstrap scope")
        return HTTPSMaterialAPI(binding=binding, api_server=self.api_server, ssl_context=self._ssl_context, token=self._token)


class _CheckedMaterialAPI:
    def __init__(self, api: MaterialAPI, check: Callable[[], str]):
        self.api, self.check = api, check

    def verify_identity(self, binding: ManagementBinding) -> None:
        self.check()
        self.api.verify_identity(binding)

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None:
        return self.api.get_secret(namespace, name)

    def create_secret(self, document: dict[str, Any]) -> None:
        self.check()
        self.api.create_secret(document)


def _document(binding: BootstrapBinding, operation: str) -> dict[str, Any]:
    if str(UUID(operation)) != operation or UUID(operation).int == 0:
        raise BootstrapError("invalid management bootstrap operation")
    document = _namespace(binding.namespace)
    document["metadata"]["labels"][_LABEL] = binding.installation_id
    document["metadata"]["annotations"] = {_OPERATION: operation}
    return document


def _observe(api: BootstrapAPI, binding: BootstrapBinding, desired: dict[str, Any], uid: str | None) -> str:
    api.verify_cluster(binding)
    actual = api.get_namespace()
    if actual is None:
        raise BootstrapError("management namespace create unresolved; preserve intent")
    observed_uid = _uid(actual)
    snapshot = _snapshot(actual)
    # Kubernetes owns only these additional Namespace defaults. Every other
    # policy/ownership field must match the create intent, even on first readback.
    labels = snapshot["metadata"].get("labels", {})
    if labels.pop("kubernetes.io/metadata.name", binding.namespace) != binding.namespace:
        raise BootstrapError("management namespace identity differs")
    spec = snapshot.pop("spec", {})
    if (spec not in ({}, {"finalizers": ["kubernetes"]}) or snapshot != desired
            or (uid is not None and uid != observed_uid)):
        raise BootstrapError("management namespace identity or policy differs")
    api.verify_cluster(binding)
    return observed_uid


def bootstrap_management(*, binding: BootstrapBinding, api: BootstrapAPI, state_dir: Path) -> dict[str, Any]:
    """Freeze the owned namespace UID before generating/delivering fresh keys.

    Material-stage intent lives outside the helper's directory. Any restart after
    that intent requires the helper's original complete journals, not regeneration.
    A receipt proves bootstrap identities only, never management/API readiness.
    """
    try:
        with private_state._locked_state(state_dir):
            api.verify_cluster(binding)
            path = state_dir / "bootstrap.json"
            material = state_dir / "material"
            identity = {"schema": "loom.nebius-management-bootstrap.v1", "binding": asdict(binding)}
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path))
            else:
                if api.get_namespace() is not None or material.exists() or material.is_symlink():
                    raise BootstrapError("untracked management bootstrap state; refusing adoption")
                record = {**identity, "operation_id": str(uuid4()), "stage": "namespace_prepared", "namespace_uid": None}
                private_state._atomic_json(path, record)
            if (not isinstance(record, dict) or set(record) != {*identity, "operation_id", "stage", "namespace_uid"}
                    or any(record[key] != value for key, value in identity.items()) or record["stage"] not in _STAGES
                    or (record["stage"] in {"namespace_prepared", "namespace_create_intent"}) != (record["namespace_uid"] is None)):
                raise BootstrapError("management bootstrap journal differs")
            desired = _document(binding, record["operation_id"])
            uid = record["namespace_uid"]
            if uid is not None:
                ManagementBinding(binding.installation_id, binding.namespace, uid, binding.kube_system_uid)
            if record["stage"] in {"material_intent", "bootstrapped"}:
                # Inspect BEFORE the helper's mkdir/generator. Directory loss is
                # not a fresh start, even when the API currently has no Secrets.
                if material.is_symlink() or not material.is_dir() or any(
                    not (material / name).is_file() or (material / name).is_symlink()
                    for name in ("initialized.json", "material.json")
                ):
                    raise BootstrapError("management material recovery evidence missing; preserve bootstrap journal")
            if record["stage"] == "namespace_prepared":
                if api.get_namespace() is not None:
                    raise BootstrapError("untracked management namespace; refusing adoption")
                api.verify_cluster(binding)
                record["stage"] = "namespace_create_intent"
                private_state._atomic_json(path, record)
                try:
                    api.create_namespace(desired)
                except Exception:
                    pass  # Only readback can reconcile an ambiguous create.
            uid = _observe(api, binding, desired, uid)
            if record["stage"] == "namespace_create_intent":
                record.update(stage="namespace_created", namespace_uid=uid)
                private_state._atomic_json(path, record)
            material_binding = ManagementBinding(binding.installation_id, binding.namespace, uid, binding.kube_system_uid)
            if record["stage"] == "namespace_created":
                if material.exists() or material.is_symlink():
                    raise BootstrapError("untracked management material state; refusing adoption")
                record["stage"] = "material_intent"
                private_state._atomic_json(path, record)
            with api.material_api(material_binding) as material_api:
                checked = _CheckedMaterialAPI(material_api, lambda: _observe(api, binding, desired, uid))
                receipt = deliver_material(binding=material_binding, api=checked, state_dir=material)
            _observe(api, binding, desired, uid)
            if record["stage"] != "bootstrapped":
                record["stage"] = "bootstrapped"
                private_state._atomic_json(path, record)
            return {**receipt, "status": "management_bootstrapped"}
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("management bootstrap unavailable; preserve recovery evidence") from None
