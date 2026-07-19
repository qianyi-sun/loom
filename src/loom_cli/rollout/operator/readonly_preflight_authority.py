"""Fixed readonly credentials and sources for deep staging preflight."""

from __future__ import annotations

import json
import re
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.readonly_authority_source import probe_readonly_authority
from loom_cli.rollout.staging_baseline_readiness import ReadonlyProbe
from loom_cli.rollout.staging_baseline_source import (
    BaselineHttpResponse,
    HttpGet,
    StagingBaselineProbeSource,
    bounded_http_get,
    read_staging_capacity,
    read_staging_mutation_epoch,
)

from .config import OperatorConfig

READONLY_KUBECONFIG_PATH = Path(
    "/var/lib/loom-staging-rollout/credentials/readonly-kubeconfig"
)
READONLY_TOKEN_PATH = Path(
    "/var/lib/loom-staging-rollout/credentials/readonly-probe-token"
)
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)+$")
_ROUTE_RE = re.compile(r"^/[a-z0-9](?:[-a-z0-9/]{0,126}[a-z0-9])?$")


class CommandResult:
    """Structural command result used by the authority runner."""

    returncode: int
    stdout: str


JsonRunner = Callable[[Sequence[str], bytes], CommandResult]


def derive_staging_route(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> str:
    """Derive the canonical route from the exact trusted cluster config."""
    trusted = read_trusted_file(
        config.cluster_config_path,
        service_uid=service_uid,
        private=False,
        max_bytes=1024 * 1024,
        require_nonempty=True,
    )
    try:
        raw = tomllib.loads(trusted.payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError("staging cluster route authority is invalid") from exc
    host = raw.get("ingress_host")
    route = raw.get("frontend_route_path")
    api_route = raw.get("frontend_api_base_path")
    if (
        raw.get("namespace") != config.namespace
        or raw.get("runtime_environment") != config.environment
        or not isinstance(host, str)
        or _DNS_RE.fullmatch(host) is None
        or not isinstance(route, str)
        or _ROUTE_RE.fullmatch(route) is None
        or route != api_route
    ):
        raise ValueError("staging cluster route authority is invalid")
    return f"https://{host}{route}"


@dataclass(frozen=True, slots=True)
class ReadonlyPreflightAuthority:
    """One composition root for all current-staging readonly sources."""

    config: OperatorConfig
    service_uid: int
    kubernetes_run: JsonRunner
    http_get: HttpGet = bounded_http_get
    token_path: Path = READONLY_TOKEN_PATH
    kubeconfig_path: Path = READONLY_KUBECONFIG_PATH

    def __post_init__(self) -> None:
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid < 0
            or not self.token_path.is_absolute()
            or not self.kubeconfig_path.is_absolute()
        ):
            raise ValueError("readonly preflight authority is invalid")

    @property
    def route(self) -> str:
        return derive_staging_route(self.config, service_uid=self.service_uid)

    def mutation_epoch(self) -> int:
        return read_staging_mutation_epoch(
            route=self.route,
            token_path=self.token_path,
            service_uid=self.service_uid,
            http_get=self.http_get,
        )

    def capacity(self) -> StagingCapacity:
        return read_staging_capacity(
            route=self.route,
            token_path=self.token_path,
            service_uid=self.service_uid,
            http_get=self.http_get,
        )

    def baseline_probes(self, mutation_epoch: int) -> Mapping[str, ReadonlyProbe]:
        return StagingBaselineProbeSource(
            route=self.route,
            token_path=self.token_path,
            service_uid=self.service_uid,
            mutation_epoch=mutation_epoch,
            http_get=self.http_get,
        ).probes()

    def capabilities(self) -> ReadonlyAuthorityEvidence:
        return probe_readonly_authority(
            self.kubernetes_run,
            kubeconfig=self.kubeconfig_path,
            namespace=self.config.namespace,
            application_observation=self._application_observation,
        )

    def _application_observation(self) -> bytes:
        response: BaselineHttpResponse = self.http_get(
            self.route + "/api/v1/auth/whoami",
            self._token(),
        )
        if response.status_code != 200:
            raise ValueError("readonly application observation failed")
        return json.dumps(
            dict(response.body),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    def _token(self) -> str:
        trusted = read_trusted_file(
            self.token_path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=1024,
            require_nonempty=True,
        )
        try:
            token = trusted.payload.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("readonly token encoding is invalid") from exc
        if not token or any(character.isspace() for character in token):
            raise ValueError("readonly token payload is invalid")
        return token


__all__ = [
    "READONLY_KUBECONFIG_PATH",
    "READONLY_TOKEN_PATH",
    "JsonRunner",
    "ReadonlyPreflightAuthority",
    "derive_staging_route",
]
