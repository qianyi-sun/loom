"""Env-driven configuration for the Daytona driver.

Reads DAYTONA_* (matches the upstream SDK's env-var contract) plus
Loom-specific LOOM_DAYTONA_* knobs. Exactly one of api_key / jwt_token
MUST be set; the loader raises ConfigError otherwise.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from loom.errors import ConfigError


@dataclass(frozen=True)
class DaytonaConfig:
    api_key: str | None
    jwt_token: str | None
    api_url: str | None
    target: str | None
    warm_pool_size: int
    delete_timeout_sec: float
    default_image: str

    @classmethod
    def from_env(cls) -> DaytonaConfig:
        api_key = os.environ.get("DAYTONA_API_KEY") or None
        jwt_token = os.environ.get("DAYTONA_JWT_TOKEN") or None
        if not api_key and not jwt_token:
            raise ConfigError(
                "DAYTONA_API_KEY or DAYTONA_JWT_TOKEN must be set "
                "for the Daytona driver",
            )
        return cls(
            api_key=api_key,
            jwt_token=jwt_token,
            api_url=os.environ.get("DAYTONA_API_URL") or None,
            target=os.environ.get("DAYTONA_TARGET") or None,
            warm_pool_size=int(os.environ.get("LOOM_DAYTONA_WARM_POOL", "0")),
            delete_timeout_sec=float(
                os.environ.get("LOOM_DAYTONA_DELETE_TIMEOUT_SEC", "60"),
            ),
            default_image=os.environ.get(
                "LOOM_DAYTONA_DEFAULT_IMAGE", "python:3.12-slim",
            ),
        )

    def to_sdk_config(self) -> Any:
        """Build a `daytona.DaytonaConfig` (kw-args differ across SDK minors)."""
        from daytona import DaytonaConfig as SDKConfig
        return SDKConfig(
            api_key=self.api_key,
            jwt_token=self.jwt_token,
            api_url=self.api_url,
            target=self.target,
        )
