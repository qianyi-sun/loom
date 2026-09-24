"""Recoverable initial management installation behind a protected, fixed caller.

The caller implements live prerequisite, actual-subject permission, off-node backup
and public authentication probes. No caller-provided readiness flags are accepted.
Every Kubernetes write uses the fixed bootstrap/authority/material/phase adapters.
This module is not a CLI and cannot grant itself installation authority.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_management_authority_stage import stage_management_authority
from scripts.ops.nebius_management_bootstrap import (
    BootstrapAPI,
    BootstrapBinding,
    bootstrap_management,
)
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    management_phase_ready,
    stage_management_resources,
)
from scripts.ops.nebius_management_supplied import deliver_supplied_material

from loom.nebius_environment_render import _envelope
from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import (
    ManagementDeployment,
    RenderedManagement,
    render_management,
)
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection

_PHASES = {
    "bootstrap": None, "config": "10-config-network.yaml", "authority": None, "supplied": None,
    "database": "20-database.yaml", "migration": "30-migrate.yaml", "backup": "85-backup-verify.yaml",
    "schedule": "80-backup.yaml", "service": "40-services.yaml", "public": "70-public.yaml",
}


class ManagementInstallError(RuntimeError):
    """Sanitized failure; preserve all resources and private recovery evidence."""


@dataclass(frozen=True, repr=False)
class ManagementInstallRequest:
    binding: BootstrapBinding
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    material: dict[str, dict[str, str]]


class ManagementInstallationAPI(Protocol):
    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None:
        """Read back exact candidate, cluster/ingress/DNS, scoped inputs and physical fit."""
        ...

    def bootstrap_api(self) -> AbstractContextManager[BootstrapAPI]: ...
    def resources(self, binding: ManagementBinding, phase: str) -> AbstractContextManager[ManagementStageAPI]: ...
    def qualify_authority(self, binding: ManagementBinding, state_dir: Path) -> None:
        """Require current exact policies and authenticated runtime-subject enforcement."""
        ...

    def verify_backup(self, binding: ManagementBinding, rendered: RenderedManagement, job_uid: str) -> dict[str, Any]:
        """Bind the completed Job's dump to off-node object readback, never CronJob presence."""
        ...

    def verify_public(self, binding: ManagementBinding, rendered: RenderedManagement, material_dir: Path) -> None:
        """Trusted public HTTPS, management readiness, admin auth and anonymous rejection."""
        ...


def render_installation(request: ManagementInstallRequest) -> RenderedManagement:
    deployment = request.deployment
    runtime = deployment.installation.provider_runtime
    if ((request.binding.installation_id, request.binding.namespace) != (str(deployment.installation_id), deployment.namespace)
            or deployment.installation.foundation.namespace_authority is None or runtime is None
            or not isinstance(runtime.kubernetes, ProjectedKubernetesConnection)):
        raise ManagementInstallError("management installation requires bound projected authority")
    rendered = render_management(deployment, candidate=request.candidate, profile=request.profile,
                                 repo_root=Path(__file__).resolve().parents[2])
    cronjob = rendered.files["80-backup.yaml"][0]
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": copy.deepcopy(cronjob["metadata"]),
           "spec": copy.deepcopy(cronjob["spec"]["jobTemplate"]["spec"])}
    job["metadata"]["name"] = "loom-management-backup-" + rendered.revision[7:19]
    # Keep first-install evidence for UID-bound off-node verification and recovery.
    # Kubernetes must not garbage-collect it while the operation is paused.
    job["spec"].pop("ttlSecondsAfterFinished", None)
    job["spec"]["backoffLimit"] = 0
    files = {**rendered.files, "85-backup-verify.yaml": [job]}
    return replace(rendered, files=files, platform_envelope=_envelope(files))


def _journal_names(phase: str) -> tuple[str, ...]:
    return ("bootstrap.json", "material/initialized.json", "material/material.json") if phase == "bootstrap" else ("stage.json",)


def _hash_journals(state: Path, phase: str) -> dict[str, str]:
    return {name: hashlib.sha256(private_state._private_read(state / phase / name, limit=4 * 1024 * 1024)).hexdigest()
            for name in _journal_names(phase)}


def _validate_history(record: dict[str, Any], identity: dict[str, Any], state: Path) -> None:
    if (not isinstance(record, dict) or set(record) != {*identity, "phases"}
            or any(record[key] != value for key, value in identity.items()) or not isinstance(record["phases"], dict)):
        raise ManagementInstallError("management installation recovery identity differs")
    history = record["phases"]
    if set(history) != set(list(_PHASES)[:len(history)]):
        raise ManagementInstallError("management installation recovery order differs")
    for index, phase in enumerate(list(_PHASES)[:len(history)]):
        item = history[phase]
        if (set(item) != {"status", "receipt", "journals"} or item["status"] not in {"started", "complete"}
                or (item["status"] == "started" and (index != len(history) - 1 or item["receipt"] is not None or item["journals"] is not None))):
            raise ManagementInstallError("management installation recovery phase differs")
        path = state / phase
        # A lost directory cannot be interpreted as a new phase, even if all its
        # external objects are currently missing. Never regenerate credentials.
        journal = path / _journal_names(phase)[0]
        if path.is_symlink() or not path.is_dir() or not journal.is_file() or journal.is_symlink():
            raise ManagementInstallError("management installation recovery evidence missing")
        if item["status"] == "complete" and item["journals"] != _hash_journals(state, phase):
            raise ManagementInstallError("management installation recovery journals changed")


def install_management(*, request: ManagementInstallRequest, api: ManagementInstallationAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Advance to the next readiness barrier; resumption never repeats unknown writes."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if state.resolve() == anchor.resolve() or state.resolve() in anchor.resolve().parents or anchor.resolve() in state.resolve().parents:
            raise ManagementInstallError("management recovery anchor must be independent of installation state")
        rendered = render_installation(request)
        fingerprint = digest({"binding": asdict(request.binding), "deployment": request.deployment.model_dump(mode="json"),
                              "candidate": request.candidate, "profile": request.profile, "material": request.material})
        identity = {"schema": "loom.nebius-management-install.v1", "input_digest": fingerprint,
                    "state_dir": str(state), "binding": asdict(request.binding)}
        with private_state._locked_state(anchor):
            marker = anchor / (request.binding.installation_id + ".json")
            journal = state / "installation.json"
            if marker.exists() or marker.is_symlink():
                started = json.loads(private_state._private_read(marker))
                if (not isinstance(started, dict) or set(started) != {*identity, "operation_id"}
                        or any(started[key] != value for key, value in identity.items())
                        or not journal.is_file() or journal.is_symlink() or state.is_symlink()):
                    raise ManagementInstallError("management installation recovery evidence missing or changed")
                record = json.loads(private_state._private_read(journal, limit=1024 * 1024))
                _validate_history(record, started, state)
            else:
                if state.exists() or state.is_symlink():
                    raise ManagementInstallError("untracked management recovery state; refusing adoption")
                # No remote writes and no start marker before full qualification.
                api.preflight(request, rendered)
                started = {**identity, "operation_id": str(uuid4())}
                record = {**started, "phases": {}}
                private_state._atomic_json(marker, started)
            with private_state._locked_state(state):
                if not journal.exists():
                    private_state._atomic_json(journal, record)
                api.preflight(request, rendered)
                binding: ManagementBinding | None = None
                backup: dict[str, Any] | None = None
                for phase, filename in _PHASES.items():
                    if phase not in record["phases"]:
                        record["phases"][phase] = {"status": "started", "receipt": None, "journals": None}
                        private_state._atomic_json(journal, record)
                    item = record["phases"][phase]
                    phase_state = state / phase
                    if phase == "bootstrap":
                        with api.bootstrap_api() as bootstrap_api:
                            receipt = bootstrap_management(binding=request.binding, api=bootstrap_api, state_dir=phase_state)
                        binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                                    receipt["namespace_uid"], request.binding.kube_system_uid)
                    else:
                        assert binding is not None
                        with api.resources(binding, phase) as stage_api:
                            if phase == "authority":
                                authority = request.deployment.installation.foundation.namespace_authority
                                assert authority is not None
                                receipt = stage_management_authority(authority=authority, binding=binding,
                                                                     api=stage_api, state_dir=phase_state)
                            elif phase == "supplied":
                                receipt = deliver_supplied_material(material=request.material, binding=binding,
                                                                   api=stage_api, state_dir=phase_state)
                            else:
                                assert filename is not None
                                receipt = stage_management_resources(rendered=rendered, phase=filename,
                                                                     binding=binding, api=stage_api, state_dir=phase_state)
                    if item["status"] == "complete":
                        if receipt != item["receipt"]:
                            raise ManagementInstallError("management installation recovery receipt changed")
                    else:
                        item.update(status="complete", receipt=receipt, journals=_hash_journals(state, phase))
                        private_state._atomic_json(journal, record)
                    assert binding is not None
                    if phase == "authority":
                        api.qualify_authority(binding, phase_state)
                    if phase in {"database", "migration", "backup", "service"}:
                        assert filename is not None
                        with api.resources(binding, phase) as stage_api:
                            ready = management_phase_ready(rendered=rendered, phase=filename, binding=binding,
                                                           api=stage_api, state_dir=phase_state)
                        if not ready:
                            return {"status": "pending", "phase": phase, "installation_id": binding.installation_id,
                                    "namespace_uid": binding.namespace_uid, "revision": rendered.revision}
                        if phase == "backup":
                            job_uid = next(iter(receipt["resource_uids"].values()))
                            backup = api.verify_backup(binding, rendered, job_uid)
                            if (set(backup) != {"job_uid", "sha256", "bytes", "key"} or backup["job_uid"] != job_uid
                                    or not isinstance(backup["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", backup["sha256"])
                                    or type(backup["bytes"]) is not int or backup["bytes"] <= 0
                                    or not isinstance(backup["key"], str) or not 0 < len(backup["key"]) <= 1024):
                                raise ManagementInstallError("management backup readback identity differs")
                assert binding is not None and backup is not None
                api.verify_public(binding, rendered, state / "bootstrap" / "material")
                return {"status": "management_installed", "installation_id": binding.installation_id,
                        "namespace_uid": binding.namespace_uid, "revision": rendered.revision, "backup": backup}
    except ManagementInstallError:
        raise
    except Exception:
        raise ManagementInstallError("management installation incomplete; preserve recovery evidence") from None
