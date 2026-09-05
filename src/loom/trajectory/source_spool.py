"""Explicit source-spool configuration, independent of canonical object storage."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

from pydantic import SecretStr

from loom.trajectory.storage import ObjectStore


class _SourceSettings(Protocol):
    service_execution_source_endpoint: str | None
    service_execution_source_access_key: SecretStr | None
    service_execution_source_secret_key: SecretStr | None
    service_execution_source_access_key_file: Path | None
    service_execution_source_secret_key_file: Path | None
    service_execution_source_region: str | None
    service_execution_source_bucket: str | None


def _credential(value: SecretStr | None, path: Path | None, name: str) -> SecretStr:
    if (value is None) == (path is None):
        raise ValueError(f"service_execution_source_{name} requires exactly one value or file")
    if path is not None:
        try:
            value = SecretStr(path.read_text(encoding="utf-8").strip())
        except (OSError, UnicodeError):
            raise ValueError(f"service_execution_source_{name}_file is unreadable") from None
    assert value is not None
    if not value.get_secret_value().strip():
        raise ValueError(f"service_execution_source_{name} must not be empty")
    return value


@dataclass(frozen=True)
class ServiceExecutionSourceConfig:
    endpoint: str
    access_key: SecretStr
    secret_key: SecretStr
    region: str
    bucket: str

    @classmethod
    def from_settings(cls, settings: _SourceSettings) -> ServiceExecutionSourceConfig | None:
        """Only an entirely unset source configuration may reuse canonical storage."""
        fields = (
            settings.service_execution_source_endpoint,
            settings.service_execution_source_access_key,
            settings.service_execution_source_secret_key,
            settings.service_execution_source_access_key_file,
            settings.service_execution_source_secret_key_file,
            settings.service_execution_source_region,
            settings.service_execution_source_bucket,
        )
        if all(value is None for value in fields):
            return None
        endpoint = settings.service_execution_source_endpoint
        region = settings.service_execution_source_region
        bucket = settings.service_execution_source_bucket
        if not all(value is not None and value.strip() for value in (endpoint, region, bucket)):
            raise ValueError("service_execution_source requires endpoint, region, and bucket")
        assert endpoint is not None and region is not None and bucket is not None
        try:
            parsed = urlsplit(endpoint)
            valid_endpoint = (
                parsed.scheme in {"http", "https"}
                and bool(parsed.hostname)
                and parsed.username is None
                and parsed.password is None
                and not parsed.query
                and not parsed.fragment
            )
        except ValueError:
            valid_endpoint = False
        if not valid_endpoint:
            raise ValueError(
                "service_execution_source_endpoint must be an HTTP(S) endpoint without credentials"
            )
        return cls(
            endpoint=endpoint,
            access_key=_credential(
                settings.service_execution_source_access_key,
                settings.service_execution_source_access_key_file,
                "access_key",
            ),
            secret_key=_credential(
                settings.service_execution_source_secret_key,
                settings.service_execution_source_secret_key_file,
                "secret_key",
            ),
            region=region,
            bucket=bucket,
        )

    def build_store(self, factory: Callable[..., ObjectStore]) -> ObjectStore:
        return factory(
            endpoint_url=self.endpoint,
            access_key=self.access_key.get_secret_value(),
            secret_key=self.secret_key.get_secret_value(),
            region=self.region,
        )
