"""Async-facing bridge over the SYNC modal Python SDK.

Modal's ``modal.Sandbox.create``, ``sandbox.exec``, ``sandbox.terminate`` are
all sync. Loom's Driver Protocol is async. This module is the only place
that calls ``modal`` directly — everything goes through ``asyncio.to_thread``
so the event loop is never blocked by a sync RPC. ``ModalDriver`` uses
this client exclusively.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
from typing import Any

from loom_drivers.modal.config import ModalConfig

logger = logging.getLogger(__name__)


class ModalSDKNotInstalledError(RuntimeError):
    """Raised when the optional modal SDK is not importable."""


def _require_modal() -> Any:
    """Import modal lazily. Friendly error if the optional extra is absent."""
    try:
        import modal  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ModalSDKNotInstalledError(
            "Modal SDK not installed. Install with: "
            "pip install 'loom[modal]'",
        ) from exc
    if modal is None:
        # Tests can null-out sys.modules['modal'] to simulate missing dep.
        raise ModalSDKNotInstalledError(
            "Modal SDK not installed. Install with: "
            "pip install 'loom[modal]'",
        )
    return modal


class ModalClient:
    """Thin async wrapper. Holds config; lazily imports modal on first call."""

    def __init__(self, config: ModalConfig) -> None:
        self._config = config

    async def lookup_app(self, app_name: str) -> Any:
        modal = _require_modal()
        return await asyncio.to_thread(
            modal.App.lookup, app_name, create_if_missing=True,
        )

    async def build_image(
        self,
        *,
        base: str,
        pip_packages: list[str] | None = None,
    ) -> Any:
        """Build (or look up cached) a ``modal.Image`` for ``base``.

        Modal hashes the image definition and caches builds server-side;
        ``ModalImageCache`` layers an in-process dict on top to skip even
        the build-RPC roundtrip when an image fingerprint repeats.
        """
        modal = _require_modal()

        def _build() -> Any:
            img = modal.Image.from_registry(base)
            if pip_packages:
                img = img.pip_install(*pip_packages)
            return img

        return await asyncio.to_thread(_build)

    async def create_sandbox(
        self,
        *,
        app: Any,
        image: Any,
        timeout_sec: int,
        gpu: str | None,
        block_network: bool,
        outbound_cidr_allowlist: list[str] | None,
        workdir: str,
        env: dict[str, str],
    ) -> Any:
        modal = _require_modal()
        kwargs: dict[str, Any] = {
            "app": app,
            "image": image,
            "timeout": timeout_sec,
            "workdir": workdir,
            "env": dict(env),
            "block_network": block_network,
        }
        if gpu is not None:
            kwargs["gpu"] = gpu
        if outbound_cidr_allowlist is not None:
            kwargs["outbound_cidr_allowlist"] = list(outbound_cidr_allowlist)
        return await asyncio.to_thread(modal.Sandbox.create, **kwargs)

    async def exec_sandbox(
        self,
        sandbox: Any,
        argv: list[str],
        *,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
    ) -> Any:
        """Start a process inside ``sandbox``. Returns a ``ContainerProcess``
        (sync object with sync-iterable ``.stdout`` / ``.stderr``).

        Per-exec timeouts are enforced on the Loom side via
        ``asyncio.wait_for`` + ``proc.terminate()`` — we don't pass a
        ``timeout=`` kwarg to Modal's ``sandbox.exec`` because:
          1. The skipped Task 1 probe means the kwarg's existence on the
             pinned Modal SDK is unverified;
          2. The sandbox-level timeout is already set at create time via
             ``ModalConfig.sandbox_timeout_sec``;
          3. Loom-side enforcement lets us actually kill the underlying
             process on timeout, instead of waiting for Modal's cap to
             fire (~minutes).
        """

        def _exec() -> Any:
            kwargs: dict[str, Any] = {}
            if workdir is not None:
                kwargs["workdir"] = workdir
            if env is not None:
                kwargs["env"] = dict(env)
            return sandbox.exec(*argv, **kwargs)

        return await asyncio.to_thread(_exec)

    async def poll_sandbox(self, sandbox: Any) -> int | None:
        """Returns exit code or None if still running."""
        return await asyncio.to_thread(sandbox.poll)

    async def terminate_sandbox(
        self, sandbox: Any, *, wait: bool = True,
    ) -> None:
        await asyncio.to_thread(sandbox.terminate, wait=wait)

    async def sandbox_billed_seconds(self, sandbox: Any) -> float:
        """Returns wall-clock seconds the sandbox was billed for.

        Modal exposes ``sandbox.created_at`` and ``sandbox.terminated_at`` as
        timezone-aware datetimes. Billed duration is their delta in seconds.
        If ``terminated_at`` is None, fall back to current UTC.
        """

        def _calc() -> float:
            created = getattr(sandbox, "created_at", None)
            terminated = getattr(sandbox, "terminated_at", None)
            if created is None:
                return 0.0
            end = (
                terminated
                if terminated is not None
                else _dt.datetime.now(_dt.UTC)
            )
            delta = (end - created).total_seconds()
            return float(max(0.0, delta))

        return await asyncio.to_thread(_calc)
