"""Private create-only management credentials, called by a protected installer.

This module does not create namespaces, grant runtime permissions, or expose an
operator CLI. Its caller qualifies installation inputs and the namespace first.
Cloud, Kubernetes, publication and backup credentials are delivered separately.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom_service.environment_management.credentials import generate_management_material

_KEYS = {
    "loom-platform-db": {"ca.crt", "postgres-password", "admin-url", "service-password", "service-url"},
    "loom-management-db-tls": {"tls.crt", "tls.key"},
    "loom-platform-auth": {"secret-store-master-key"},
    "loom-admin-secret": {"secrets.toml"},
}
_LABEL = "loom.nebius/management-installation"
_MARKER = "loom.nebius/management-material-operation"


class MaterialError(RuntimeError):
    """Payload-free failure; preserve private recovery state and existing Secrets."""


def _uuid(value: str) -> None:
    if not isinstance(value, str) or str(UUID(value)) != value or UUID(value).int == 0:
        raise ValueError()


@dataclass(frozen=True)
class ManagementBinding:
    installation_id: str
    namespace: str
    namespace_uid: str
    kube_system_uid: str

    def __post_init__(self) -> None:
        try:
            for value in (self.installation_id, self.namespace_uid, self.kube_system_uid):
                _uuid(value)
            if (not isinstance(self.namespace, str) or len(self.namespace) > 53
                    or re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", self.namespace) is None):
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise MaterialError("invalid management namespace binding") from None


class MaterialAPI(Protocol):
    def verify_identity(self, binding: ManagementBinding) -> None:
        """Verify cluster and exact owned namespace before and after every write."""
        ...

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None: ...
    def create_secret(self, document: dict[str, Any]) -> None:
        """One immutable create, never apply/replace or a client-side retry."""
        ...


class HTTPSMaterialAPI(ManagementKubernetesTransport):
    """Fixed Secret transport with explicit TLS/auth, no redirects or retries.

    The protected installer supplies a qualified client-certificate context or
    bearer token. This transport never loads ambient kubeconfig or executes a
    credential plugin. In particular, kubectl POST's built-in retry is not used.
    """

    error_type = MaterialError

    def __init__(self, *, binding: ManagementBinding, api_server: str, ssl_context: ssl.SSLContext,
                 token: str | None = None):
        self.binding = binding
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            if binding != self.binding:
                raise ValueError()
            for name, uid in (("kube-system", binding.kube_system_uid), (binding.namespace, binding.namespace_uid)):
                value = self._request("GET", "/api/v1/namespaces/" + name)
                if (value is None or value.get("kind") != "Namespace" or value["metadata"]["name"] != name
                        or value["metadata"]["uid"] != uid or value["metadata"].get("deletionTimestamp")
                        or (name == binding.namespace and value["metadata"].get("labels", {}).get(_LABEL) != binding.installation_id)):
                    raise ValueError()
        except Exception:
            raise MaterialError("management cluster or owned namespace differs") from None

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None:
        if namespace != self.binding.namespace or name not in _KEYS:
            raise MaterialError("Secret outside management material scope")
        return self._request("GET", "/api/v1/namespaces/" + namespace + "/secrets/" + name)

    def create_secret(self, document: dict[str, Any]) -> None:
        try:
            metadata, data = document["metadata"], document["data"]
            name, operation = metadata["name"], metadata["annotations"][_MARKER]
            _uuid(operation)
            if (set(document) != {"apiVersion", "kind", "metadata", "type", "immutable", "data"}
                    or document["apiVersion"] != "v1" or document["kind"] != "Secret" or document["immutable"] is not True
                    or name not in _KEYS or set(data) != _KEYS[name]
                    or metadata != {"name": name, "namespace": self.binding.namespace,
                                    "labels": {_LABEL: self.binding.installation_id}, "annotations": {_MARKER: operation}}
                    or document["type"] != ("kubernetes.io/tls" if name == "loom-management-db-tls" else "Opaque")):
                raise ValueError()
            for value in data.values():
                if (not isinstance(value, str) or not 0 < len(value) <= 90_000
                        or not base64.b64decode(value, validate=True)):
                    raise ValueError()
        except Exception:
            raise MaterialError("Secret outside management material scope") from None
        self.verify_identity(self.binding)
        self._request("POST", "/api/v1/namespaces/" + self.binding.namespace + "/secrets", document=document)


def _digest(material: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _documents(material: dict[str, Any], binding: ManagementBinding, operation: str) -> dict[str, dict[str, Any]]:
    _uuid(operation)
    if not isinstance(material, dict) or material.keys() != _KEYS.keys():
        raise MaterialError("management material journal differs")
    documents = {}
    for name, keys in _KEYS.items():
        values = material[name]
        if (not isinstance(values, dict) or values.keys() != keys
                or any(not isinstance(value, str) or not 0 < len(value.encode()) <= 65_536 for value in values.values())):
            raise MaterialError("management material journal differs")
        documents[name] = {
            "apiVersion": "v1", "kind": "Secret", "immutable": True,
            "metadata": {"name": name, "namespace": binding.namespace,
                         "labels": {_LABEL: binding.installation_id}, "annotations": {_MARKER: operation}},
            "type": "kubernetes.io/tls" if name == "loom-management-db-tls" else "Opaque",
            "data": {key: base64.b64encode(value.encode()).decode() for key, value in values.items()},
        }
    return documents


def deliver_material(*, binding: ManagementBinding, api: MaterialAPI, state_dir: Path) -> dict[str, Any]:
    """Persist generated material once, then deliver without adoption or overwrite.

    The journal is the private recovery material. Never include it in workflow
    artifacts; its durable bytes precede every Secret write. Missing, changed or
    ambiguous state is an explicit recovery boundary, not a regeneration signal.
    """
    try:
        with private_state._locked_state(state_dir):
            api.verify_identity(binding)
            path = state_dir / "material.json"
            marker = state_dir / "initialized.json"
            identity = {"schema": "loom.nebius-management-material.v1", "binding": asdict(binding)}
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=1024 * 1024))
            else:
                if marker.exists() or marker.is_symlink():
                    raise MaterialError("management material journal missing; preserve initialization evidence")
                # Check every destination before generation or the first create.
                if any(api.get_secret(binding.namespace, name) is not None for name in _KEYS):
                    raise MaterialError("untracked management Secret exists; refusing adoption")
                material = generate_management_material(namespace=binding.namespace)
                record = {**identity, "status": "prepared", "operation_id": str(uuid4()), "material": material,
                          "material_sha256": _digest(material),
                          "resources": {name: {"status": "prepared", "uid": None} for name in _KEYS}}
                _documents(material, binding, record["operation_id"])
                # Retain independent evidence before material or writes. A crash
                # between these files is a recovery boundary, never a fresh run.
                private_state._atomic_json(marker, {**identity, "operation_id": record["operation_id"],
                                                     "material_sha256": record["material_sha256"]})
                private_state._atomic_json(path, record)
            if (not isinstance(record, dict)
                    or set(record) != {*identity, "status", "operation_id", "material", "material_sha256", "resources"}
                    or any(record[key] != value for key, value in identity.items())
                    or record["status"] not in {"prepared", "delivered"}
                    or record["material_sha256"] != _digest(record["material"])
                    or not isinstance(record["resources"], dict) or record["resources"].keys() != _KEYS.keys()):
                raise MaterialError("management material journal differs")
            if not marker.exists() and not marker.is_symlink():
                raise MaterialError("management material journal initialization evidence missing")
            initialized = json.loads(private_state._private_read(marker))
            if initialized != {**identity, "operation_id": record["operation_id"],
                               "material_sha256": record["material_sha256"]}:
                raise MaterialError("management material journal initialization evidence differs")
            documents = _documents(record["material"], binding, record["operation_id"])
            # Validate the entire retained chain before resuming any writes.
            for item in record["resources"].values():
                if (not isinstance(item, dict) or set(item) != {"status", "uid"}
                        or item["status"] not in {"prepared", "create_intent", "created"}
                        or (item["status"] == "created") != (item["uid"] is not None)
                        or (record["status"] == "delivered" and item["status"] != "created")):
                    raise MaterialError("management material journal differs")
                if item["uid"] is not None:
                    _uuid(item["uid"])

            def observe(name: str, desired: dict[str, Any]) -> str:
                observed = api.get_secret(binding.namespace, name)
                if observed is None:
                    raise MaterialError("management Secret outcome unresolved; preserve intent")
                uid = _uid(observed)
                if (_snapshot(observed) != desired
                        or record["resources"][name]["uid"] not in (None, uid)):
                    raise MaterialError("management Secret differs from recorded intent")
                return uid

            for name, desired in documents.items():
                item = record["resources"][name]
                api.verify_identity(binding)
                if item["status"] == "prepared":
                    if api.get_secret(binding.namespace, name) is not None:
                        raise MaterialError("untracked management Secret exists; refusing adoption")
                    item["status"] = "create_intent"
                    private_state._atomic_json(path, record)
                    try:
                        api.create_secret(copy.deepcopy(desired))
                    except Exception:
                        pass  # Readback can resolve a lost reply; never repeat create.
                uid = observe(name, desired)
                api.verify_identity(binding)
                if item["status"] != "created":
                    item.update(status="created", uid=uid)
                    private_state._atomic_json(path, record)
            for name, desired in documents.items():
                observe(name, desired)
            api.verify_identity(binding)
            if record["status"] != "delivered":
                record["status"] = "delivered"
                private_state._atomic_json(path, record)
            return {"status": "management_material_delivered", "installation_id": binding.installation_id,
                    "namespace": binding.namespace, "namespace_uid": binding.namespace_uid,
                    "secret_uids": {name: item["uid"] for name, item in record["resources"].items()}}
    except MaterialError:
        raise
    except Exception:
        raise MaterialError("management material delivery unavailable; preserve private state") from None
