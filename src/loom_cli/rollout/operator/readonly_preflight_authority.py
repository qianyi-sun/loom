"""Fixed readonly credentials and sources for deep staging preflight."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_credential_paths import READONLY_KUBECONFIG_PATH
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.readonly_authority_source import probe_readonly_authority
from loom_cli.rollout.readonly_database_authority import (
    ReadonlyDatabaseEvidence,
    ReadonlyMutationEpochEvidence,
    ReadonlySmokeAuthorityEvidence,
)
from loom_cli.rollout.staging_baseline_readiness import (
    STAGING_BASELINE_CHECK_IDS,
    BaselineProbeResult,
    ReadonlyProbe,
)
from loom_cli.rollout.staging_baseline_source import (
    CrossVersionStagingBaselineProbeSource,
    ObjectStoreProbe,
    PublicHttpGet,
    TlsProbe,
    bounded_public_http_get,
    probe_tls_route,
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


def _read_route_authority(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> tuple[str, str, str]:
    """Read and fully validate the route authority from the trusted cluster config.

    Returns ``(host, target_path, predecessor_path)``. ``predecessor_path`` is
    ``""`` unless a route transition is declared via ``frontend_route_path_from``
    (set to a canonical route distinct from ``frontend_route_path``).
    """
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
    route_from = raw.get("frontend_route_path_from", "")
    if (
        raw.get("namespace") != config.namespace
        or raw.get("runtime_environment") != config.environment
        or not isinstance(host, str)
        or _DNS_RE.fullmatch(host) is None
        or not isinstance(route, str)
        or _ROUTE_RE.fullmatch(route) is None
        or route != api_route
        or not isinstance(route_from, str)
    ):
        raise ValueError("staging cluster route authority is invalid")
    if route_from and (_ROUTE_RE.fullmatch(route_from) is None or route_from == route):
        # A declared route transition must name a canonical predecessor route
        # that actually differs from the target; otherwise it is a no-op that
        # must be left empty.
        raise ValueError("staging cluster route authority is invalid")
    return host, route, route_from


def derive_staging_route(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> str:
    """Derive the canonical TARGET route from the exact trusted cluster config."""
    host, route, _ = _read_route_authority(config, service_uid=service_uid)
    return f"https://{host}{route}"


def derive_staging_baseline_probe_route(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> str:
    """Route the Tier-2 current-staging baseline probes hit on the LIVE service.

    During a declared route transition (``frontend_route_path_from`` set to a
    canonical route distinct from ``frontend_route_path``) the live service still
    serves the predecessor route — the driver has not moved it yet — so the
    read-only baseline probes must validate the live service there. The TARGET
    route stays attested by ``derive_staging_route`` (plan/digest) and by the
    post-deploy final gate. Absent a transition this is identical to
    ``derive_staging_route``.
    """
    host, route, route_from = _read_route_authority(config, service_uid=service_uid)
    return f"https://{host}{route_from or route}"


@dataclass(frozen=True, slots=True)
class ReadonlyPreflightAuthority:
    """One composition root for all current-staging readonly sources."""

    config: OperatorConfig
    service_uid: int
    kubernetes_run: JsonRunner
    database_evidence: Callable[[], ReadonlyDatabaseEvidence]
    smoke_authority_evidence: Callable[[], ReadonlySmokeAuthorityEvidence]
    mutation_epoch_evidence: Callable[[], ReadonlyMutationEpochEvidence]
    capacity_source: Callable[[], StagingCapacity]
    object_store_probe: ObjectStoreProbe
    public_http_get: PublicHttpGet = bounded_public_http_get
    tls_probe: TlsProbe = probe_tls_route
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

    @property
    def baseline_probe_route(self) -> str:
        """Live route the Tier-2 baseline probes hit (predecessor during a transition)."""
        return derive_staging_baseline_probe_route(self.config, service_uid=self.service_uid)

    def mutation_epoch(self) -> int:
        return self.mutation_epoch_evidence().mutation_epoch

    def capacity(self) -> StagingCapacity:
        return self.capacity_source()

    def baseline_probes(self, mutation_epoch: int) -> Mapping[str, ReadonlyProbe]:
        if mutation_epoch < 0:
            raise ValueError("readonly baseline mutation epoch is invalid")

        def probe(check_id: str) -> BaselineProbeResult:
            database = self.database_evidence()
            if database.mutation_epoch != mutation_epoch:
                raise ValueError("readonly baseline mutation epoch drifted")
            sources = CrossVersionStagingBaselineProbeSource(
                route=self.baseline_probe_route,
                database=database,
                smoke_authority=(
                    self.smoke_authority_evidence() if check_id == "staging.auth" else None
                ),
                object_store_probe=self.object_store_probe,
                public_http_get=self.public_http_get,
                tls_probe=self.tls_probe,
            ).probes()
            return sources[check_id]()

        def bound(check_id: str) -> ReadonlyProbe:
            def execute() -> BaselineProbeResult:
                return probe(check_id)

            return execute

        return {check_id: bound(check_id) for check_id in STAGING_BASELINE_CHECK_IDS}

    def capabilities(self) -> ReadonlyAuthorityEvidence:
        epoch = self.mutation_epoch_evidence()
        return probe_readonly_authority(
            self.kubernetes_run,
            kubeconfig=self.kubeconfig_path,
            namespace=self.config.namespace,
            database_authority_digest=epoch.evidence_sha256,
        )


__all__ = [
    "READONLY_KUBECONFIG_PATH",
    "JsonRunner",
    "ReadonlyPreflightAuthority",
    "derive_staging_baseline_probe_route",
    "derive_staging_route",
]
