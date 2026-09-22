"""Explicit native provider credentials and owned management-worker lifecycle."""

from __future__ import annotations

import asyncio
import logging
import stat
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field

from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
from loom_service.environment_management.credentials import EnvironmentCredentialProvider
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError
from loom_service.environment_management.provisioner import EnvironmentProvisioner
from loom_service.environment_management.registry import EnvironmentRegistry
from loom_service.environment_management.worker import EnvironmentWorker

_LOG = logging.getLogger(__name__)


class ProviderRuntimeSettings(BaseModel):
    """Protected installation input, never an owner-request or ambient kubeconfig."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    kubernetes: NebiusKubernetesConnection
    cloud_credentials_file: Path
    concurrency: int = Field(default=4, ge=1, le=16, strict=True)
    poll_seconds: int = Field(default=5, ge=1, le=60, strict=True)


class NebiusManagementAuth(httpx.Auth):
    def __init__(self, connection: NebiusKubernetesConnection, credentials: NebiusKubernetesCredentials):
        self.origin = httpx.URL(connection.endpoint)
        self.credentials = credentials

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if (request.url.scheme, request.url.host, request.url.port) != (
            self.origin.scheme, self.origin.host, self.origin.port,
        ):
            raise ProviderBlockedError("kubernetes_origin_mismatch")
        try:
            token = await self.credentials.get_token()
        except Exception:
            raise ProviderRetryError("kubernetes_credentials_unavailable") from None
        request.headers["Authorization"] = "Bearer " + token
        request.headers.pop("Cookie", None)
        yield request


class EnvironmentRuntime:
    def __init__(self, worker: EnvironmentWorker, kubernetes: KubernetesEnvironmentProvider, settings: ProviderRuntimeSettings):
        self.worker, self.kubernetes = worker, kubernetes
        self.task = asyncio.create_task(worker.run(concurrency=settings.concurrency, poll_seconds=settings.poll_seconds),
                                        name="loom-management-environment-worker")
        self.task.add_done_callback(self._finished)

    @staticmethod
    def _finished(task: asyncio.Task[None]) -> None:
        # Retrieve unexpected exceptions without emitting their potentially
        # secret-bearing text. Readiness remains false for every stopped task.
        if not task.cancelled() and task.exception() is not None:
            _LOG.error("environment_worker_stopped")

    @property
    def ready(self) -> bool:
        return not self.task.done() and self.worker.healthy

    async def close(self) -> None:
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)

    @classmethod
    @asynccontextmanager
    async def open(
        cls, settings: ProviderRuntimeSettings, registry: EnvironmentRegistry, *, child_http: httpx.AsyncClient,
    ) -> AsyncIterator[EnvironmentRuntime]:
        from nebius.sdk import SDK

        async with AsyncExitStack() as resources:
            try:
                metadata = settings.cloud_credentials_file.stat()
                if (not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o027
                        or not 0 < metadata.st_size <= 1024 * 1024):
                    raise ValueError("invalid credential file")
                credentials = NebiusKubernetesCredentials(settings.kubernetes)
                resources.push_async_callback(credentials.close)
                sdk = SDK(credentials_file_name=str(settings.cloud_credentials_file),
                          user_agent_prefix="loom-environment-management/1.0")
                resources.push_async_callback(sdk.close)
                # Fail startup cleanly if either explicit credential cannot be
                # exchanged. No anonymous or ambient-login fallback is allowed.
                await credentials.get_token()
                NebiusKubernetesCredentials._usable(await sdk.get_token(timeout=30))
            except Exception:
                raise ValueError("invalid_environment_provider_credentials") from None
            http = await resources.enter_async_context(httpx.AsyncClient(
                base_url=settings.kubernetes.endpoint, verify=credentials.ssl_context,
                auth=NebiusManagementAuth(settings.kubernetes, credentials),
                trust_env=False, timeout=30, follow_redirects=False,
            ))
            kubernetes = KubernetesEnvironmentProvider(http)
            cloud = NebiusSdkEnvironmentApi(sdk)
            provider = EnvironmentProvisioner(
                registry, kubernetes=kubernetes, cloud=NebiusEnvironmentCloudProvider(cloud),
                credentials=EnvironmentCredentialProvider(registry, cloud, kubernetes),
                child=ChildEnvironmentClient(child_http),
            )
            runtime = cls(EnvironmentWorker(registry, provider), kubernetes, settings)
            # Registered last, so all work/heartbeats stop before HTTP/SDK close.
            resources.push_async_callback(runtime.close)
            yield runtime
