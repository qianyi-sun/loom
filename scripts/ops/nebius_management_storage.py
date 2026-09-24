"""Read-only data-volume qualification around initial StatefulSet creation.

The Namespace is installation-owned. Record claim absence before creating the
controller, then bind its exact PVC, PV and CSI disk before any migration. Replay
cannot reinterpret missing/replaced storage as an empty new database.
"""
from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from kubernetes.utils.quantity import parse_quantity
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_material import ManagementBinding

from loom_service.environment_management.deployment import RenderedManagement
from loom_service.environment_management.kubernetes_provider import _contains


class ManagementStorageError(RuntimeError):
    """Payload-free error; never replace claims, disks or recovery evidence."""


class ManagementStorageAPI(Protocol):
    def verify_identity(self, binding: ManagementBinding) -> None: ...
    def get_database_claim(self) -> dict[str, Any] | None: ...
    def get_database_volume(self) -> dict[str, Any] | None: ...


def _intent(rendered: RenderedManagement, binding: ManagementBinding) -> dict[str, Any]:
    return {"schema": "loom.nebius-management-storage-intent.v1", "binding": asdict(binding),
            "revision": rendered.revision, "claim_name": "data-loom-postgres-0"}


def prepare_management_storage(*, rendered: RenderedManagement, binding: ManagementBinding,
                               api: ManagementStorageAPI, state_dir: Path) -> None:
    try:
        expected = _intent(rendered, binding)
        with private_state._locked_state(state_dir):
            api.verify_identity(binding)
            path = state_dir / "storage-intent.json"
            if path.exists() or path.is_symlink():
                if json.loads(private_state._private_read(path)) != expected:
                    raise ManagementStorageError("management storage intent differs")
            else:
                if (state_dir / "stage.json").exists() or api.get_database_claim() is not None:
                    raise ManagementStorageError("untracked management database claim; refusing adoption")
                private_state._atomic_json(path, expected)
    except ManagementStorageError:
        raise
    except Exception:
        raise ManagementStorageError("management storage preparation unavailable") from None


def verify_management_storage(*, rendered: RenderedManagement, binding: ManagementBinding, api: ManagementStorageAPI,
                              state_dir: Path, evidence_dir: Path) -> dict[str, Any]:
    try:
        intent = _intent(rendered, binding)
        if json.loads(private_state._private_read(state_dir / "storage-intent.json")) != intent:
            raise ManagementStorageError("management storage intent differs")
        stateful = next(doc for doc in rendered.files["20-database.yaml"] if doc["kind"] == "StatefulSet")
        template = stateful["spec"]["volumeClaimTemplates"][0]
        api.verify_identity(binding)
        claim = api.get_database_claim()
        volume = api.get_database_volume()
        if claim is None or volume is None:
            raise ManagementStorageError("management database storage missing or unbound")
        for value, kind in ((claim, "PersistentVolumeClaim"), (volume, "PersistentVolume")):
            if value.get("kind") != kind or value["metadata"].get("deletionTimestamp") or value["metadata"].get("ownerReferences"):
                raise ManagementStorageError("management database storage identity differs")
            _uid(value)
        if (claim["metadata"].get("name") != intent["claim_name"] or claim["metadata"].get("namespace") != binding.namespace
                or claim["metadata"].get("labels", {}).get("loom.nebius/management-installation") != binding.installation_id
                or claim.get("status", {}).get("phase") != "Bound" or volume.get("status", {}).get("phase") != "Bound"):
            raise ManagementStorageError("management database claim is not the bound installation claim")
        claim_spec = copy.deepcopy(claim["spec"])
        expected_spec = copy.deepcopy(template["spec"])
        for spec in (claim_spec, expected_spec):
            amount = parse_quantity(spec["resources"]["requests"]["storage"])
            if not amount.is_finite() or amount <= 0:
                raise ValueError()
            spec["resources"]["requests"]["storage"] = str(amount.normalize())
        if not _contains(claim_spec, expected_spec) or claim_spec.get("volumeMode", "Filesystem") != "Filesystem":
            raise ManagementStorageError("management database claim configuration differs")
        spec = copy.deepcopy(volume["spec"])
        reference = spec["claimRef"]
        if (not isinstance(claim_spec.get("volumeName"), str) or not claim_spec["volumeName"]
                or volume["metadata"].get("name") != claim_spec["volumeName"]
                or any(reference.get(key) != value for key, value in {
                    "namespace": binding.namespace, "name": intent["claim_name"], "uid": _uid(claim),
                }.items())
                or spec.get("storageClassName") != claim_spec.get("storageClassName")
                or not spec.get("csi", {}).get("driver") or not spec.get("csi", {}).get("volumeHandle")
                or parse_quantity(spec["capacity"]["storage"]) < parse_quantity(claim_spec["resources"]["requests"]["storage"])):
            raise ManagementStorageError("management database physical volume binding differs")
        # ResourceVersion in a claim reference is bookkeeping; UID is identity.
        reference.pop("resourceVersion", None)
        record = {**intent, "schema": "loom.nebius-management-storage.v1", "pvc_uid": _uid(claim),
                  "pv_uid": _uid(volume), "pv_name": volume["metadata"]["name"],
                  "pvc_spec": claim_spec, "pv_spec": spec}
        with private_state._locked_state(evidence_dir):
            path = evidence_dir / "stage.json"
            if path.exists() or path.is_symlink():
                if json.loads(private_state._private_read(path, limit=1024 * 1024)) != record:
                    raise ManagementStorageError("management database retained storage changed")
            else:
                private_state._atomic_json(path, record)
        if api.get_database_claim() != claim or api.get_database_volume() != volume:
            raise ManagementStorageError("management database storage changed during readback")
        api.verify_identity(binding)
        return {"status": "management_storage_verified", "pvc_uid": record["pvc_uid"], "pv_uid": record["pv_uid"]}
    except ManagementStorageError:
        raise
    except Exception:
        raise ManagementStorageError("management database storage qualification unavailable") from None
