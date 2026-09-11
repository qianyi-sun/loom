"""Application-only node bootstrap view of the controller's exact typed routes.

The node receives a separately pinned application-only directory and executor
identity. Entry bytes, including credential path/digest, stay unchanged; no V2
translation or weakened route-hash comparison is allowed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2, PhysicalJobBindingV2
from loom_capacity_executor.admission_client import DatabaseExecutableAdmissionClient
from loom_capacity_executor.build_admission_client import BuildAdmissionExecutorV1
from loom_capacity_executor.typed_admission import (
    TypedAdmissionRouter,
    load_typed_admission_directory,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2


class ApplicationBootstrapAdmission:
    """No build backend or arbitrary admission method is exposed to the node."""

    def __init__(self, path: Path, *, expected_sha256: str, executor: BuildAdmissionExecutorV1) -> None:
        document = load_typed_admission_directory(path, expected_sha256=expected_sha256, executor=executor)
        if any(entry.purpose != "application-worker" for entry in document.entries):
            raise ValueError("node bootstrap requires application-only typed admission")
        self._router = TypedAdmissionRouter(path, expected_sha256=expected_sha256, executor=executor,
            application_client_factory=DatabaseExecutableAdmissionClient.from_database_url_bytes)

    def _application(self, binding: ExecutableIntentBindingV2) -> None:
        # The router rereads the pinned root and exact executor/candidate scope.
        # Purpose is checked before either route hashing or credential access.
        if self._router.purpose(binding) != "application-worker":
            raise ValueError("node bootstrap refuses non-application authority")

    def bootstrap_handoff_route_sha256(self, binding: ExecutableIntentBindingV2) -> str:
        self._application(binding)
        return self._router.bootstrap_handoff_route_sha256(binding)

    async def observe_current_bootstrap(self, request: PhysicalJobBindingV2) -> Any:
        self._application(request.binding)
        return await self._router.observe_current_bootstrap(request)

    async def register_worker(self, request: ExecutableWorkerRegistrationV2, *, bootstrap_capability: str) -> Any:
        self._application(request.binding)
        return await self._router.register_worker(request, bootstrap_capability=bootstrap_capability)
