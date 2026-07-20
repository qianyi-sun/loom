"""Concrete read-only current-staging sources for Tier 2 preflight."""

from __future__ import annotations

import hashlib
import json
import socket
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

import httpx

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence
from loom_cli.rollout.staging_baseline_readiness import (
    BaselineProbeResult,
    ReadonlyProbe,
)

_MAX_RESPONSE_BYTES = 1024 * 1024
_API_PATHS = MappingProxyType(
    {
        "health": "/api/v1/health",
        "ready": "/api/v1/health/ready",
        "whoami": "/api/v1/auth/whoami",
        "agents": "/api/v1/agents",
        "models": "/api/v1/models",
        "tasks": "/api/v1/tasks?limit=1",
    }
)


@dataclass(frozen=True, slots=True)
class BaselineHttpResponse:
    status_code: int
    http_version: str
    body: Mapping[str, Any]

    def __post_init__(self) -> None:
        if (
            self.status_code < 100
            or self.status_code > 599
            or self.http_version not in {"HTTP/1.0", "HTTP/1.1", "HTTP/2"}
        ):
            raise ValueError("baseline HTTP response metadata is invalid")
        object.__setattr__(self, "body", MappingProxyType(dict(self.body)))


@dataclass(frozen=True, slots=True)
class TlsRouteEvidence:
    dns_digest: str
    certificate_sha256: str
    port: int

    def __post_init__(self) -> None:
        if len(self.dns_digest) != 64 or len(self.certificate_sha256) != 64 or self.port != 443:
            raise ValueError("baseline TLS evidence is invalid")


HttpGet = Callable[[str, str], BaselineHttpResponse]
TlsProbe = Callable[[str], TlsRouteEvidence]
PublicHttpGet = Callable[[str], BaselineHttpResponse]


@dataclass(frozen=True, slots=True)
class ObjectStoreBaselineEvidence:
    ready: bool
    evidence_sha256: str

    def __post_init__(self) -> None:
        if len(self.evidence_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.evidence_sha256
        ):
            raise ValueError("object-store baseline evidence is invalid")


ObjectStoreProbe = Callable[[], ObjectStoreBaselineEvidence]


def _validated_route(route: str) -> tuple[str, str, int]:
    parsed = urlsplit(route)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.port not in (None, 443)
        or ".." in parsed.path.split("/")
    ):
        raise ValueError("staging baseline route is invalid")
    canonical = route.rstrip("/")
    return canonical, parsed.hostname, 443


def bounded_http_get(url: str, token: str) -> BaselineHttpResponse:
    """GET one fixed HTTPS endpoint without redirects or environment proxies."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.fragment:
        raise ValueError("baseline HTTP target is invalid")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "loom-staging-readonly-preflight/v1",
    }
    with httpx.Client(
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=False,
        trust_env=False,
    ) as client:
        with client.stream("GET", url, headers=headers) as response:
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > _MAX_RESPONSE_BYTES:
                    raise ValueError("baseline HTTP response exceeded size limit")
            try:
                body = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("baseline HTTP response is not JSON") from exc
    if not isinstance(body, dict):
        raise ValueError("baseline HTTP response body is not an object")
    return BaselineHttpResponse(
        status_code=response.status_code,
        http_version=response.http_version,
        body=body,
    )


def bounded_public_http_get(url: str) -> BaselineHttpResponse:
    """GET the fixed public health endpoint without ambient credentials."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or not url.endswith("/api/v1/health"):
        raise ValueError("public baseline HTTP target is invalid")
    with httpx.Client(
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=False,
        trust_env=False,
        headers={
            "Accept": "application/json",
            "User-Agent": "loom-staging-readonly-preflight/v1",
        },
    ) as client:
        response = client.get(url)
    if len(response.content) > _MAX_RESPONSE_BYTES:
        raise ValueError("baseline HTTP response exceeded size limit")
    try:
        body = response.json()
    except json.JSONDecodeError as exc:
        raise ValueError("baseline HTTP response is not JSON") from exc
    if not isinstance(body, dict):
        raise ValueError("baseline HTTP response body is not an object")
    return BaselineHttpResponse(response.status_code, response.http_version, body)


def probe_tls_route(route: str) -> TlsRouteEvidence:
    """Resolve and authenticate the canonical staging TLS endpoint."""
    _canonical, hostname, port = _validated_route(route)
    addresses = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    normalized = sorted(
        {f"{family}:{sockaddr[0]}" for family, _socktype, _proto, _canonname, sockaddr in addresses}
    )
    if not normalized:
        raise OSError("staging DNS returned no addresses")
    context = ssl.create_default_context()
    last_error: OSError | ssl.SSLError | None = None
    certificate: bytes | None = None
    for family, socktype, proto, _canonname, sockaddr in addresses:
        raw = socket.socket(family, socktype, proto)
        raw.settimeout(5.0)
        try:
            raw.connect(sockaddr)
            with context.wrap_socket(raw, server_hostname=hostname) as tls:
                certificate = tls.getpeercert(binary_form=True)
            break
        except (OSError, ssl.SSLError) as exc:
            last_error = exc
            raw.close()
    if not certificate:
        raise OSError("staging TLS authentication failed") from last_error
    return TlsRouteEvidence(
        dns_digest=hashlib.sha256("\n".join(normalized).encode()).hexdigest(),
        certificate_sha256=hashlib.sha256(certificate).hexdigest(),
        port=port,
    )


def _read_readonly_token(token_path: Path, *, service_uid: int) -> str:
    trusted = read_trusted_file(
        token_path,
        service_uid=service_uid,
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


def read_staging_mutation_epoch(
    *,
    route: str,
    token_path: Path,
    service_uid: int,
    http_get: HttpGet = bounded_http_get,
) -> int:
    """Read the exact staging mutation epoch through the readonly API."""
    canonical, _hostname, _port = _validated_route(route)
    response = http_get(
        canonical + _API_PATHS["ready"],
        _read_readonly_token(token_path, service_uid=service_uid),
    )
    epoch = response.body.get("mutation_epoch")
    if (
        response.status_code not in {200, 503}
        or response.body.get("environment") != "staging"
        or response.body.get("namespace") != "loom-staging"
        or type(epoch) is not int
        or epoch < 0
    ):
        raise ValueError("readonly mutation epoch evidence is invalid")
    return epoch


def read_staging_capacity(
    *,
    route: str,
    token_path: Path,
    service_uid: int,
    http_get: HttpGet = bounded_http_get,
) -> StagingCapacity:
    """Read exact freshness-validated capacity through the readonly API."""
    canonical, _hostname, _port = _validated_route(route)
    response = http_get(
        canonical + _API_PATHS["ready"],
        _read_readonly_token(token_path, service_uid=service_uid),
    )
    raw = response.body.get("capacity")
    if (
        response.status_code not in {200, 503}
        or response.body.get("environment") != "staging"
        or response.body.get("namespace") != "loom-staging"
        or response.body.get("capacity_ready") is not True
        or not isinstance(raw, dict)
        or set(raw)
        != {
            "object_count",
            "bytes_used",
            "disk_free_percent",
            "inode_free_percent",
            "policy_sha256",
            "evidence_sha256",
        }
        or raw.get("policy_sha256") != staging_capacity_policy_digest()
        or any(
            type(raw.get(name)) is not int
            for name in (
                "object_count",
                "bytes_used",
                "disk_free_percent",
                "inode_free_percent",
            )
        )
    ):
        raise ValueError("readonly capacity evidence is invalid")
    try:
        capacity = StagingCapacity(
            object_count=raw["object_count"],
            bytes_used=raw["bytes_used"],
            disk_free_percent=raw["disk_free_percent"],
            inode_free_percent=raw["inode_free_percent"],
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("readonly capacity evidence is invalid") from exc
    if raw["evidence_sha256"] != capacity.evidence_digest:
        raise ValueError("readonly capacity evidence is invalid")
    return capacity


class StagingBaselineProbeSource:
    """Build the five fixed Tier 2 probes from one bounded authority."""

    def __init__(
        self,
        *,
        route: str,
        token_path: Path,
        service_uid: int,
        mutation_epoch: int,
        http_get: HttpGet = bounded_http_get,
        tls_probe: TlsProbe = probe_tls_route,
    ) -> None:
        canonical, _hostname, _port = _validated_route(route)
        if not token_path.is_absolute() or service_uid < 0 or mutation_epoch < 0:
            raise ValueError("staging baseline source authority is invalid")
        self._route = canonical
        self._token_path = token_path
        self._service_uid = service_uid
        self._epoch = mutation_epoch
        self._http_get = http_get
        self._tls_probe = tls_probe

    def probes(self) -> Mapping[str, ReadonlyProbe]:
        return MappingProxyType(
            {
                "staging.health": lambda: self._health(),
                "staging.auth": lambda: self._auth(),
                "staging.catalog-task": lambda: self._catalog_task(),
                "staging.storage-db": lambda: self._storage_db(),
                "staging.network": lambda: self._network(),
            }
        )

    def _token(self) -> str:
        return _read_readonly_token(self._token_path, service_uid=self._service_uid)

    def _get(self, name: str) -> BaselineHttpResponse:
        return self._http_get(self._route + _API_PATHS[name], self._token())

    def _result(
        self,
        check_id: str,
        *,
        summaries: Mapping[str, object],
        blockers: Mapping[str, str],
    ) -> BaselineProbeResult:
        digest = hashlib.sha256(
            json.dumps(
                {"check_id": check_id, "summaries": summaries, "version": "v1"},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return BaselineProbeResult(
            check_id=check_id,
            environment="staging",
            namespace="loom-staging",
            route=self._route,
            readonly_principal="loom-rollout-readonly",
            observed_mutation_epoch=self._epoch,
            resource_digest=digest,
            blockers=blockers,
        )

    def _health(self) -> BaselineProbeResult:
        response = self._get("health")
        ok = response.status_code == 200 and response.body.get("status") == "ok"
        return self._result(
            "staging.health",
            summaries={"http": response.status_code, "ok": ok},
            blockers={} if ok else {"service": "health-not-ok"},
        )

    def _auth(self) -> BaselineProbeResult:
        response = self._get("whoami")
        body = response.body
        ok = (
            response.status_code == 200
            and body.get("auth_kind") == "readonly_probe"
            and body.get("credential_type") == "staging_readonly_probe"
            and body.get("principal_type") == "readonly_probe"
            and body.get("scopes") == ["read:own"]
            and body.get("allowed_http_methods") == ["GET", "HEAD"]
            and body.get("readonly_authority_version") == "v1"
        )
        return self._result(
            "staging.auth",
            summaries={"http": response.status_code, "authority": ok},
            blockers={} if ok else {"principal": "readonly-authority-drift"},
        )

    def _catalog_task(self) -> BaselineProbeResult:
        responses = {name: self._get(name) for name in ("agents", "models", "tasks")}
        blockers: dict[str, str] = {}
        summaries: dict[str, object] = {}
        for name, response in responses.items():
            items = response.body.get("items")
            ok = response.status_code == 200 and isinstance(items, list)
            if name == "tasks":
                ok = ok and isinstance(response.body.get("total"), int)
            summaries[name] = {
                "http": response.status_code,
                "items": len(items) if isinstance(items, list) else -1,
                "shape": ok,
            }
            if not ok:
                blockers[name] = f"{name}-catalog-unavailable"
        return self._result(
            "staging.catalog-task",
            summaries=summaries,
            blockers=blockers,
        )

    def _storage_db(self) -> BaselineProbeResult:
        response = self._get("ready")
        body = response.body
        postgres_ready = body.get("postgres") == "ready"
        object_store_ready = body.get("object_store") == "ready"
        digest = body.get("resource_digest")
        valid_digest = isinstance(digest, str) and len(digest) == 64
        observed_epoch = body.get("mutation_epoch")
        exact_binding = (
            body.get("environment") == "staging"
            and body.get("namespace") == "loom-staging"
            and observed_epoch == self._epoch
        )
        capacity_ready = body.get("capacity_ready") is True
        blockers: dict[str, str] = {}
        if not postgres_ready:
            blockers["postgres"] = "postgres-readiness-failed"
        if not object_store_ready:
            blockers["object-store"] = "object-store-readiness-failed"
        if not valid_digest:
            blockers["evidence"] = "dependency-evidence-invalid"
        if not exact_binding:
            blockers["epoch"] = "dependency-epoch-drift"
        if not capacity_ready:
            blockers["capacity"] = "dependency-capacity-unready"
        if response.status_code not in {200, 503}:
            blockers["http"] = "dependency-readiness-unreachable"
        return self._result(
            "staging.storage-db",
            summaries={
                "http": response.status_code,
                "object_store": object_store_ready,
                "postgres": postgres_ready,
                "epoch_bound": exact_binding,
                "capacity_ready": capacity_ready,
                "server_digest": digest if valid_digest else "invalid",
            },
            blockers=blockers,
        )

    def _network(self) -> BaselineProbeResult:
        try:
            evidence = self._tls_probe(self._route)
        except (OSError, ValueError, ssl.SSLError):
            return self._result(
                "staging.network",
                summaries={"tls": False},
                blockers={"route": "dns-tls-authentication-failed"},
            )
        return self._result(
            "staging.network",
            summaries={
                "certificate_sha256": evidence.certificate_sha256,
                "dns_digest": evidence.dns_digest,
                "port": evidence.port,
                "tls": True,
            },
            blockers={},
        )


class CrossVersionStagingBaselineProbeSource:
    """Tier 2 source that works before and after lifecycle migrations."""

    def __init__(
        self,
        *,
        route: str,
        database: ReadonlyDatabaseEvidence,
        object_store_probe: ObjectStoreProbe,
        public_http_get: PublicHttpGet = bounded_public_http_get,
        tls_probe: TlsProbe = probe_tls_route,
    ) -> None:
        canonical, _hostname, _port = _validated_route(route)
        self._route = canonical
        self._database = database
        self._object_store_probe = object_store_probe
        self._public_http_get = public_http_get
        self._tls_probe = tls_probe

    def probes(self) -> Mapping[str, ReadonlyProbe]:
        return MappingProxyType(
            {
                "staging.health": self._health,
                "staging.auth": self._auth,
                "staging.catalog-task": self._catalog_task,
                "staging.storage-db": self._storage_db,
                "staging.network": self._network,
            }
        )

    def _result(
        self,
        check_id: str,
        *,
        summaries: Mapping[str, object],
        blockers: Mapping[str, str],
    ) -> BaselineProbeResult:
        digest = hashlib.sha256(
            json.dumps(
                {"check_id": check_id, "summaries": summaries, "version": "v2"},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return BaselineProbeResult(
            check_id=check_id,
            environment="staging",
            namespace="loom-staging",
            route=self._route,
            readonly_principal="loom-rollout-readonly",
            observed_mutation_epoch=self._database.mutation_epoch,
            resource_digest=digest,
            blockers=blockers,
        )

    def _health(self) -> BaselineProbeResult:
        try:
            response = self._public_http_get(self._route + _API_PATHS["health"])
            ready = response.status_code == 200 and response.body.get("status") == "ok"
            status = response.status_code
        except (OSError, ValueError, httpx.HTTPError):
            ready = False
            status = 0
        return self._result(
            "staging.health",
            summaries={"http": status, "ready": ready},
            blockers={} if ready else {"service": "health-not-ok"},
        )

    def _auth(self) -> BaselineProbeResult:
        return self._result(
            "staging.auth",
            summaries={
                "authority": "postgres-select-only-v1",
                "database-evidence": self._database.evidence_sha256,
            },
            blockers={},
        )

    def _catalog_task(self) -> BaselineProbeResult:
        counts = dict(self._database.baseline_counts)
        return self._result(
            "staging.catalog-task",
            summaries=counts,
            blockers={},
        )

    def _storage_db(self) -> BaselineProbeResult:
        try:
            object_store = self._object_store_probe()
        except (OSError, RuntimeError, ValueError):
            object_store = ObjectStoreBaselineEvidence(False, "0" * 64)
        blockers: dict[str, str] = {}
        if not object_store.ready:
            blockers["object-store"] = "object-store-readiness-failed"
        if int(self._database.schema_revision) >= 67 and self._database.capacity is None:
            blockers["capacity"] = "dependency-capacity-unready"
        return self._result(
            "staging.storage-db",
            summaries={
                "database-evidence": self._database.evidence_sha256,
                "epoch-authority": self._database.epoch_authority,
                "object-store-evidence": object_store.evidence_sha256,
                "object-store-ready": object_store.ready,
                "schema-revision": self._database.schema_revision,
            },
            blockers=blockers,
        )

    def _network(self) -> BaselineProbeResult:
        try:
            evidence = self._tls_probe(self._route)
        except (OSError, ValueError, ssl.SSLError):
            return self._result(
                "staging.network",
                summaries={"tls": False},
                blockers={"route": "dns-tls-authentication-failed"},
            )
        return self._result(
            "staging.network",
            summaries={
                "certificate_sha256": evidence.certificate_sha256,
                "dns_digest": evidence.dns_digest,
                "port": evidence.port,
                "tls": True,
            },
            blockers={},
        )


__all__ = [
    "BaselineHttpResponse",
    "CrossVersionStagingBaselineProbeSource",
    "HttpGet",
    "ObjectStoreBaselineEvidence",
    "ObjectStoreProbe",
    "PublicHttpGet",
    "StagingBaselineProbeSource",
    "TlsProbe",
    "TlsRouteEvidence",
    "bounded_http_get",
    "bounded_public_http_get",
    "probe_tls_route",
    "read_staging_capacity",
    "read_staging_mutation_epoch",
]
