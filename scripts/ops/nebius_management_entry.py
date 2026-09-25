"""Private fixed management entrypoint; no credentials or manifests from Actions."""
from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

import loom_bundle_checksum  # noqa: F401 -- qualify the installed first-party wheel
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_bootstrap import validate_config
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_operation import LiveIngressAPI
from scripts.ops.nebius_management_bootstrap import BootstrapBinding
from scripts.ops.nebius_management_gateway import DIAGNOSTIC_STAGES, safe_report, validate_operation
from scripts.ops.nebius_management_install import (
    ManagementInstallRequest,
    install_management,
    render_installation,
)
from scripts.ops.nebius_management_live import HTTPSManagementInstallationAPI
from scripts.ops.nebius_management_prerequisites import (
    HTTPSManagementPrerequisites,
    ManagementPrerequisiteSettings,
)
from scripts.ops.nebius_management_supplied import _KEYS

from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.deployment import ManagementDeployment


class EntryError(RuntimeError):
    """Private input values and paths are never diagnostics."""


class PrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-management-private-inputs.v1"]
    binding: BootstrapBinding
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    prerequisites: ManagementPrerequisiteSettings
    operator_connection: NebiusKubernetesConnection
    operator_cloud_credentials: Path
    ingress_config: Path
    foundation_candidate: str = Field(pattern=r"^[0-9a-f]{40}$")
    material_files: dict[str, dict[str, Path]]


def _private(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise EntryError("private management input unavailable")
    return private_state._private_read(path, limit=limit)


def load_inputs(operation: dict[str, Any]) -> tuple[PrivateInputs, ManagementInstallRequest, dict[str, Any]]:
    try:
        validate_operation(operation)
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError()
        inputs = PrivateInputs.model_validate(_json(raw))
        if ((inputs.binding.installation_id, inputs.binding.namespace) != (operation["installation_id"], operation["namespace"])
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or inputs.material_files.keys() != _KEYS.keys()):
            raise ValueError()
        connection = inputs.operator_connection
        operator_files = {connection.ca_file, connection.credentials_file, inputs.operator_cloud_credentials,
                          inputs.ingress_config, Path(operation["inputs_path"])}
        material: dict[str, dict[str, str]] = {}
        seen: set[Path] = set()
        for name, keys in _KEYS.items():
            selected = inputs.material_files[name]
            if selected.keys() != keys:
                raise ValueError()
            material[name] = {}
            for key, path in selected.items():
                if path in operator_files or path in seen:
                    raise ValueError()
                seen.add(path)
                value = _private(path, 65536).decode()
                if not value:
                    raise ValueError()
                material[name][key] = value
        request = ManagementInstallRequest(binding=inputs.binding, deployment=inputs.deployment,
                                           candidate=inputs.candidate, profile=inputs.profile, material=material)
        render_installation(request)
        config = inputs.deployment.installation.foundation.platform_config
        ingress = _json(_private(inputs.ingress_config, 16384))
        validate_config(ingress)
        if (connection.endpoint != config["kubernetes_api_server"].rstrip("/")
                or ingress["api_server"].rstrip("/") != connection.endpoint
                or ingress["cluster_id"] != config["cluster_id"]):
            raise ValueError()
        for path in (connection.ca_file, connection.credentials_file, inputs.operator_cloud_credentials):
            _private(path, 1024**2)
        return inputs, request, ingress
    except Exception:
        raise EntryError("management private installation inputs unqualified") from None


async def _operator_transport(connection: NebiusKubernetesConnection) -> tuple[ssl.SSLContext, str]:
    credentials = NebiusKubernetesCredentials(connection)
    try:
        return credentials.ssl_context, await credentials.get_token()
    finally:
        await credentials.close()


@contextmanager
def connected_api(inputs: PrivateInputs, request: ManagementInstallRequest,
                  ingress: dict[str, Any]) -> Iterator[HTTPSManagementInstallationAPI]:
    # Obtain a bounded operator bearer token through its explicit SDK; never use
    # it in runtime subject checks or copy the credential into any workload.
    connection = inputs.operator_connection
    context, token = asyncio.run(_operator_transport(connection))
    installed_ingress = LiveIngressAPI(Path(ingress["kubeconfig"]), binding=TLSBinding(**ingress["binding"]),
        executable=Path(ingress["kubectl"]), candidate=inputs.foundation_candidate,
        cluster_id=ingress["cluster_id"], api_server=ingress["api_server"],
        ingress_class=ingress["ingress_class"], image=ingress["image"])
    certificate = private_state.load_installation(Path(ingress["certificate_config"]))
    with HTTPSManagementPrerequisites(settings=inputs.prerequisites, ingress=installed_ingress,
        certificate_config=certificate, ingress_state=Path(ingress["state_dir"]),
        operator_cloud_credentials=inputs.operator_cloud_credentials, api_server=connection.endpoint,
        ssl_context=context, token=token) as checks:
        yield HTTPSManagementInstallationAPI(request=request, api_server=connection.endpoint,
            ssl_context=context, token=token, runtime_ca_pem=_private(connection.ca_file, 1024**2).decode(), checks=checks)


def main(operation_path: str, action: str) -> int:
    qualified: dict[str, Any] | None = None
    api: HTTPSManagementInstallationAPI | None = None
    try:
        if action not in {"qualify", "preflight", "install"}:
            raise ValueError()
        operation = _json(_private(Path(operation_path), 16384))
        validate_operation(operation)
        if action == "qualify":
            print(json.dumps({"status": "tooling_qualified"}))
            return 0
        inputs, request, ingress = load_inputs(operation)
        qualified = operation
        with connected_api(inputs, request, ingress) as api:
            if action == "preflight":
                api.preflight(request, render_installation(request))
                result: dict[str, Any] = {"status": "preflight_qualified"}
            else:
                result = install_management(request=request, api=api, state_dir=Path(operation["state_dir"]),
                                            anchor_dir=Path(operation["anchor_dir"]))
        report = {**result, **{key: operation[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
        print(json.dumps(safe_report(json.dumps(report).encode(), operation), sort_keys=True))
        return 0
    except Exception:
        if qualified is not None:
            stage = getattr(api, "diagnostic_stage", None) if api is not None else "connection"
            if stage == "prerequisites" and api is not None:
                stage = getattr(api.checks, "diagnostic_stage", None)
            if not isinstance(stage, str) or stage not in DIAGNOSTIC_STAGES:
                stage = "operation"
            failure = {"status": "blocked", "stage": stage,
                       **{key: qualified[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
            print(json.dumps(safe_report(json.dumps(failure).encode(), qualified), sort_keys=True))
            # Zero here means a bound protocol response was delivered. Only the
            # outer rollout CLI decides success, and blocked always exits one.
            return 0
        print(json.dumps({"status": "blocked", "reason": "management operation incomplete; retain private recovery state"}))
        return 1
