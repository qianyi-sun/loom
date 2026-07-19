"""Fixed readonly credentials and sources for deep staging preflight."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_credential_paths import READONLY_KUBECONFIG_PATH
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.readonly_authority_source import probe_readonly_authority
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence
from loom_cli.rollout.staging_baseline_readiness import ReadonlyProbe
from loom_cli.rollout.staging_baseline_source import (
    CrossVersionStagingBaselineProbeSource,
    ObjectStoreProbe,
    PublicHttpGet,
    bounded_public_http_get,
)

from .config import OperatorConfig

_DNS_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)+$"
)
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
    database_evidence: Callable[[], ReadonlyDatabaseEvidence]
    object_store_probe: ObjectStoreProbe
    public_http_get: PublicHttpGet = bounded_public_http_get
    kubeconfig_path: Path = READONLY_KUBECONFIG_PATH

    def __post_init__(self) -> None:
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid < 0
            or not self.kubeconfig_path.is_absolute()
        ):
            raise ValueError("readonly preflight authority is invalid")

    @property
    def route(self) -> str:
        return derive_staging_route(self.config, service_uid=self.service_uid)

    def mutation_epoch(self) -> int:
        return self.database_evidence().mutation_epoch

    def capacity(self) -> StagingCapacity:
        raw = self.database_evidence().capacity
        if raw is None:
            raise ValueError("readonly capacity authority is unavailable")
        return StagingCapacity(
            object_count=cast(int, raw["object_count"]),
            bytes_used=cast(int, raw["bytes_used"]),
            disk_free_percent=cast(int, raw["disk_free_percent"]),
            inode_free_percent=cast(int, raw["inode_free_percent"]),
        )

    def baseline_probes(self, mutation_epoch: int) -> Mapping[str, ReadonlyProbe]:
        database = self.database_evidence()
        if database.mutation_epoch != mutation_epoch:
            raise ValueError("readonly baseline mutation epoch drifted")
        return CrossVersionStagingBaselineProbeSource(
            route=self.route,
            database=database,
            object_store_probe=self.object_store_probe,
            public_http_get=self.public_http_get,
        ).probes()

    def capabilities(self) -> ReadonlyAuthorityEvidence:
        return probe_readonly_authority(
            self.kubernetes_run,
            kubeconfig=self.kubeconfig_path,
            namespace=self.config.namespace,
            database_authority_digest=self.database_evidence().evidence_sha256,
        )


__all__ = [
    "READONLY_KUBECONFIG_PATH",
    "JsonRunner",
    "ReadonlyPreflightAuthority",
    "derive_staging_route",
]
