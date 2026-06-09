"""Modal driver configuration — token loading + app naming."""

from __future__ import annotations

import os
from dataclasses import dataclass


class ModalConfigError(RuntimeError):
    """Raised when required Modal credentials are missing or malformed."""


@dataclass(frozen=True)
class ModalConfig:
    """Modal driver runtime configuration.

    Tokens are loaded from env via ``ModalConfig.from_env()``. They MUST NOT
    appear in ``__repr__`` or logs.
    """

    token_id: str
    token_secret: str
    workspace: str | None
    app_name: str = "loom-runs"
    # Modal Sandbox.create timeout (seconds). Maps to Modal's per-sandbox
    # wall-clock cap; the driver enforces a tighter per-exec deadline on
    # top via asyncio.wait_for.
    sandbox_timeout_sec: int = 3600
    # If True, the driver keeps the sandbox image cache warm by reusing
    # modal.Image handles when we've already built a given image
    # fingerprint in this process.
    enable_image_cache: bool = True

    def __repr__(self) -> str:
        return (
            f"ModalConfig(token_id={self.token_id!r}, token_secret='***', "
            f"workspace={self.workspace!r}, app_name={self.app_name!r}, "
            f"sandbox_timeout_sec={self.sandbox_timeout_sec}, "
            f"enable_image_cache={self.enable_image_cache})"
        )

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> ModalConfig:
        src = env if env is not None else dict(os.environ)
        tid = src.get("MODAL_TOKEN_ID")
        tsecret = src.get("MODAL_TOKEN_SECRET")
        if not tid or not tsecret:
            missing = [
                k for k, v in (
                    ("MODAL_TOKEN_ID", tid),
                    ("MODAL_TOKEN_SECRET", tsecret),
                )
                if not v
            ]
            raise ModalConfigError(
                f"Missing Modal credentials: {missing}. "
                "Set MODAL_TOKEN_ID and MODAL_TOKEN_SECRET, or run "
                "`modal token new`.",
            )
        return cls(
            token_id=tid,
            token_secret=tsecret,
            workspace=src.get("MODAL_WORKSPACE") or None,
        )
