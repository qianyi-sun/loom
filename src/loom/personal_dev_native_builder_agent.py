"""Agent-side authority and runtime for isolated personal-dev native builds."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, NoReturn, Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx

from loom.personal_dev_native_builder_protocol import (
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderGrantPayload,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
    PersonalDevNativeBuilderSigner,
)

_SIGNATURE_HEADER = "X-Loom-Native-Builder-Signature"
_POLL_PATH = "/api/v1/internal/personal-dev/native-builder/poll"
_GRANT_PATH = "/api/v1/internal/personal-dev/native-builder/grants"
_MAX_RESPONSE_BYTES = 512 * 1024
_DOCKER_SOCKET = "/run/loom-personal-dev-builder/docker.sock"
_DOCKER_ROOT = "/var/lib/loom-personal-dev-builder"
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}")
_MANAGED_LABEL_KEYS = {
    "loom.personal-dev-native-builder.managed",
    "loom.personal-dev-native-builder.role",
    "loom.personal-dev-native-builder.grant-id",
    "loom.personal-dev-native-builder.candidate-id",
    "loom.personal-dev-native-builder.candidate-sha256",
    "loom.personal-dev-native-builder.attempt-id",
    "loom.personal-dev-native-builder.lease-epoch",
    "loom.personal-dev-native-builder.platform",
    "loom.personal-dev-native-builder.provider",
    "loom.personal-dev-native-builder.agent-instance-id",
    "loom.personal-dev-native-builder.agent-image",
    "loom.personal-dev-native-builder.builder-image",
    "loom.personal-dev-native-builder.runtime-profile-sha256",
    "loom.personal-dev-native-builder.contract-sha256",
}


def _buildkit_tmpfs() -> dict[str, str]:
    return {
        "/tmp": "rw,nosuid,nodev,size=2g,uid=1000,gid=1000,mode=0700",
        "/var/lib/loom-buildkit": ("rw,nosuid,nodev,size=16g,uid=1000,gid=1000,mode=0700"),
        "/var/run/loom-buildkit": ("rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700"),
    }


def _client_tmpfs() -> dict[str, str]:
    return {
        "/tmp": "rw,nosuid,nodev,size=1g,uid=1000,gid=1000,mode=0700",
        "/workspace": "rw,nosuid,nodev,size=12g,uid=1000,gid=1000,mode=0700",
    }


def _buildkit_healthcheck() -> dict[str, object]:
    return {
        "Test": [
            "CMD",
            "/usr/bin/buildctl",
            "--addr=tcp://127.0.0.1:1234",
            "debug",
            "workers",
        ],
        "Interval": 1_000_000_000,
        "Timeout": 5_000_000_000,
        "Retries": 30,
        "StartPeriod": 1_000_000_000,
    }


_GRANT_FIELDS = {
    "active_deadline_seconds",
    "agent_instance_id",
    "agent_key_id",
    "artifact_max_bytes",
    "artifact_upload_fields",
    "artifact_upload_url",
    "attempt_id",
    "attempt_lease_epoch",
    "builder_image",
    "candidate_id",
    "candidate_sha",
    "capability_expires_at",
    "contract_json",
    "contract_sha256",
    "grant_id",
    "platform",
    "provider",
    "runtime_profile_sha256",
    "source_get_url",
}


@dataclass(frozen=True, slots=True)
class NativeBuilderAgentPollResult:
    """One strictly parsed response from the management authority."""

    grant: NativeBuilderGrantPayload | None
    cancel_grant_ids: tuple[UUID, ...]

    def __post_init__(self) -> None:
        cancellations = self.cancel_grant_ids
        if (self.grant is not None and not isinstance(self.grant, NativeBuilderGrantPayload)) or (
            not isinstance(cancellations, tuple)
            or len(cancellations) > 64
            or any(not isinstance(value, UUID) or value.int == 0 for value in cancellations)
            or cancellations != tuple(sorted(set(cancellations), key=str))
            or (
                isinstance(self.grant, NativeBuilderGrantPayload)
                and self.grant.grant_id in cancellations
            )
        ):
            raise ValueError("native builder cancellation inventory is invalid")


@dataclass(frozen=True, slots=True)
class NativeBuilderRuntimeInventory:
    """Secret-free inventory observed from the dedicated Docker daemon."""

    managed_grant_ids: tuple[UUID, ...]
    active_grant_ids: tuple[UUID, ...]
    available: bool
    unavailable_reason: str | None
    readiness_evidence_sha256: str


@dataclass(frozen=True, slots=True)
class NativeBuilderRuntimeObservation:
    """One bounded observation of a running or terminal native grant."""

    state: Literal["running", "succeeded", "failed"]
    failure_reason: str | None
    evidence: NativeBuilderRuntimeEvidence | None

    def __post_init__(self) -> None:
        if self.state == "running":
            valid = self.failure_reason is None and self.evidence is None
        elif self.state == "succeeded":
            valid = self.failure_reason is None and isinstance(
                self.evidence, NativeBuilderRuntimeEvidence
            )
        elif self.state == "failed":
            valid = (
                isinstance(self.failure_reason, str)
                and re.fullmatch(r"[a-z][a-z0-9_]{0,127}", self.failure_reason) is not None
                and self.evidence is None
            )
        else:
            valid = False
        if not valid:
            raise ValueError("native builder runtime observation is invalid")


@dataclass(frozen=True, slots=True)
class NativeBuilderAgentIdentity:
    """Release-pinned identity fields shared by every agent poll."""

    agent_instance_id: UUID
    agent_key_id: str
    provider: str
    platform: str
    protocol_version: int
    host_name: str
    host_architecture: str
    host_boot_id: UUID
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    max_concurrency: int

    def status(self, inventory: NativeBuilderRuntimeInventory) -> NativeBuilderAgentStatus:
        return NativeBuilderAgentStatus(
            agent_instance_id=self.agent_instance_id,
            agent_key_id=self.agent_key_id,
            provider=self.provider,
            platform=self.platform,
            protocol_version=self.protocol_version,
            host_name=self.host_name,
            host_architecture=self.host_architecture,
            host_boot_id=self.host_boot_id,
            agent_image=self.agent_image,
            builder_image=self.builder_image,
            runtime_profile_sha256=self.runtime_profile_sha256,
            max_concurrency=self.max_concurrency,
            managed_grant_ids=inventory.managed_grant_ids,
            active_grant_ids=inventory.active_grant_ids,
            available=inventory.available,
            unavailable_reason=inventory.unavailable_reason,
            readiness_evidence_sha256=inventory.readiness_evidence_sha256,
        )


class PersonalDevNativeBuilderAuthority(Protocol):
    async def poll(
        self,
        request: NativeBuilderPollRequest,
        *,
        signature: str,
    ) -> NativeBuilderAgentPollResult: ...

    async def heartbeat(
        self,
        request: NativeBuilderHeartbeatRequest,
        *,
        signature: str,
    ) -> bool: ...

    async def complete(
        self,
        completion: NativeBuilderCompletion,
        *,
        signature: str,
    ) -> None: ...


class PersonalDevNativeBuildRuntime(Protocol):
    async def inventory(self) -> NativeBuilderRuntimeInventory: ...

    async def start(self, grant: NativeBuilderGrantPayload) -> None: ...

    async def observe(
        self,
        grant: NativeBuilderGrantPayload,
    ) -> NativeBuilderRuntimeObservation: ...

    async def cancel(self, grant_id: UUID) -> None: ...

    async def cleanup(self, grant_id: UUID) -> None: ...


@dataclass(slots=True)
class _DockerGrantResources:
    grant: NativeBuilderGrantPayload | None
    network: Any | None
    buildkit: Any | None
    client: Any | None
    safe: bool = True
    complete: bool = True


def _invalid_response(operation: str) -> NoReturn:
    raise RuntimeError(f"native builder {operation} response is invalid")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject_constant(_value: str) -> NoReturn:
    raise ValueError("non-finite JSON number")


def _json_object(body: bytes, *, operation: str) -> dict[str, object]:
    try:
        value = json.loads(
            body,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        _invalid_response(operation)
    if not isinstance(value, dict):
        _invalid_response(operation)
    return cast(dict[str, object], value)


def _uuid(value: object) -> UUID:
    if not isinstance(value, str):
        raise ValueError("UUID is invalid")
    return UUID(value)


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("timestamp is invalid")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo is None:
        raise ValueError("timestamp is invalid")
    return parsed


def _grant(value: object) -> NativeBuilderGrantPayload | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != _GRANT_FIELDS:
        raise ValueError("grant response fields are invalid")
    fields = value.get("artifact_upload_fields")
    if not isinstance(fields, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in fields.items()
    ):
        raise ValueError("grant response upload fields are invalid")
    return NativeBuilderGrantPayload(
        grant_id=_uuid(value["grant_id"]),
        candidate_id=_uuid(value["candidate_id"]),
        candidate_sha=cast(str, value["candidate_sha"]),
        attempt_id=_uuid(value["attempt_id"]),
        attempt_lease_epoch=cast(int, value["attempt_lease_epoch"]),
        platform=cast(str, value["platform"]),
        provider=cast(str, value["provider"]),
        agent_instance_id=_uuid(value["agent_instance_id"]),
        agent_key_id=cast(str, value["agent_key_id"]),
        builder_image=cast(str, value["builder_image"]),
        runtime_profile_sha256=cast(str, value["runtime_profile_sha256"]),
        contract_json=cast(str, value["contract_json"]),
        contract_sha256=cast(str, value["contract_sha256"]),
        source_get_url=cast(str, value["source_get_url"]),
        artifact_upload_url=cast(str, value["artifact_upload_url"]),
        artifact_upload_fields=cast(dict[str, str], fields),
        artifact_max_bytes=cast(int, value["artifact_max_bytes"]),
        capability_expires_at=_timestamp(value["capability_expires_at"]),
        active_deadline_seconds=cast(int, value["active_deadline_seconds"]),
    )


class HttpPersonalDevNativeBuilderAuthority:
    """Strict HTTP adapter for the signed management-side builder API."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        base_url = urlsplit(str(client.base_url))
        if (
            base_url.scheme != "https"
            or not base_url.netloc
            or base_url.hostname is None
            or base_url.username is not None
            or base_url.password is not None
            or base_url.path not in {"", "/"}
            or base_url.query
            or base_url.fragment
            or getattr(client, "_trust_env", None) is not False
        ):
            raise ValueError("native builder service client is invalid")
        self._client = client

    async def _post(
        self,
        path: str,
        *,
        body: bytes,
        signature: str,
        operation: str,
    ) -> tuple[int, bytes, bool]:
        request = self._client.build_request(
            "POST",
            path,
            content=body,
            headers={
                "Content-Type": "application/json",
                _SIGNATURE_HEADER: signature,
            },
        )
        response: httpx.Response | None = None
        try:
            response = await self._client.send(request, stream=True)
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > _MAX_RESPONSE_BYTES:
                    _invalid_response(operation)
                chunks.append(chunk)
            cache_control = response.headers.get("Cache-Control", "")
            no_store = "no-store" in {
                directive.strip().lower() for directive in cache_control.split(",")
            }
            return response.status_code, b"".join(chunks), no_store
        except Exception:
            _invalid_response(operation)
        finally:
            if response is not None:
                with contextlib.suppress(Exception):
                    await response.aclose()

    async def poll(
        self,
        request: NativeBuilderPollRequest,
        *,
        signature: str,
    ) -> NativeBuilderAgentPollResult:
        status, body, no_store = await self._post(
            _POLL_PATH,
            body=request.canonical_bytes(),
            signature=signature,
            operation="poll",
        )
        if status == 204:
            if body:
                _invalid_response("poll")
            return NativeBuilderAgentPollResult(grant=None, cancel_grant_ids=())
        if status != 200:
            _invalid_response("poll")
        value = _json_object(body, operation="poll")
        if set(value) != {"grant", "cancel_grant_ids"}:
            _invalid_response("poll")
        cancellations = value["cancel_grant_ids"]
        try:
            if not isinstance(cancellations, list) or len(cancellations) > 64:
                raise ValueError("cancellation inventory is invalid")
            cancel_grant_ids = tuple(_uuid(item) for item in cancellations)
            if cancel_grant_ids != tuple(sorted(set(cancel_grant_ids), key=str)):
                raise ValueError("cancellation inventory is invalid")
            grant = _grant(value["grant"])
        except (KeyError, TypeError, ValueError):
            _invalid_response("poll")
        if grant is not None and not no_store:
            _invalid_response("poll")
        return NativeBuilderAgentPollResult(
            grant=grant,
            cancel_grant_ids=cancel_grant_ids,
        )

    async def heartbeat(
        self,
        request: NativeBuilderHeartbeatRequest,
        *,
        signature: str,
    ) -> bool:
        status, body, _no_store = await self._post(
            f"{_GRANT_PATH}/{request.grant_id}/heartbeat",
            body=request.canonical_bytes(),
            signature=signature,
            operation="heartbeat",
        )
        if status != 200:
            _invalid_response("heartbeat")
        value = _json_object(body, operation="heartbeat")
        if set(value) != {"continue"} or type(value["continue"]) is not bool:
            _invalid_response("heartbeat")
        return value["continue"]

    async def complete(
        self,
        completion: NativeBuilderCompletion,
        *,
        signature: str,
    ) -> None:
        status, body, _no_store = await self._post(
            f"{_GRANT_PATH}/{completion.grant_id}/complete",
            body=completion.canonical_bytes(),
            signature=signature,
            operation="completion",
        )
        if status != 200:
            _invalid_response("completion")
        value = _json_object(body, operation="completion")
        if (
            set(value) != {"accepted", "state"}
            or value["accepted"] is not True
            or value["state"] != completion.outcome
        ):
            _invalid_response("completion")


class DockerPersonalDevNativeBuildRuntime:
    """Exact two-sandbox runtime on the dedicated arm64 Docker daemon."""

    def __init__(
        self,
        *,
        client: Any,
        socket_path: str,
        identity: NativeBuilderAgentIdentity,
        health_timeout_seconds: int,
        health_poll_seconds: float,
    ) -> None:
        if socket_path != _DOCKER_SOCKET:
            raise ValueError("native builder Docker socket is invalid")
        if (
            type(health_timeout_seconds) is not int
            or not 1 <= health_timeout_seconds <= 300
            or not isinstance(health_poll_seconds, (int, float))
            or isinstance(health_poll_seconds, bool)
            or not 0 < health_poll_seconds <= 10
        ):
            raise ValueError("native builder Docker health interval is invalid")
        self._client = client
        self._identity = identity
        self._health_timeout_seconds = health_timeout_seconds
        self._health_poll_seconds = float(health_poll_seconds)
        self._resources: dict[UUID, _DockerGrantResources] = {}

    def _labels(self, grant: NativeBuilderGrantPayload, *, role: str) -> dict[str, str]:
        return {
            "loom.personal-dev-native-builder.managed": "true",
            "loom.personal-dev-native-builder.role": role,
            "loom.personal-dev-native-builder.grant-id": str(grant.grant_id),
            "loom.personal-dev-native-builder.candidate-id": str(grant.candidate_id),
            "loom.personal-dev-native-builder.candidate-sha256": grant.candidate_sha,
            "loom.personal-dev-native-builder.attempt-id": str(grant.attempt_id),
            "loom.personal-dev-native-builder.lease-epoch": str(grant.attempt_lease_epoch),
            "loom.personal-dev-native-builder.platform": grant.platform,
            "loom.personal-dev-native-builder.provider": grant.provider,
            "loom.personal-dev-native-builder.agent-instance-id": str(
                self._identity.agent_instance_id
            ),
            "loom.personal-dev-native-builder.agent-image": self._identity.agent_image,
            "loom.personal-dev-native-builder.builder-image": grant.builder_image,
            "loom.personal-dev-native-builder.runtime-profile-sha256": (
                grant.runtime_profile_sha256
            ),
            "loom.personal-dev-native-builder.contract-sha256": grant.contract_sha256,
        }

    def _validate_grant(self, grant: NativeBuilderGrantPayload) -> None:
        if (
            grant.agent_instance_id != self._identity.agent_instance_id
            or grant.agent_key_id != self._identity.agent_key_id
            or grant.provider != self._identity.provider
            or grant.platform != self._identity.platform
            or grant.builder_image != self._identity.builder_image
            or grant.runtime_profile_sha256 != self._identity.runtime_profile_sha256
        ):
            raise RuntimeError("native builder grant identity is invalid")

    def _validate_daemon(self, grant: NativeBuilderGrantPayload) -> None:
        info = self._client.info()
        if not isinstance(info, dict):
            raise RuntimeError("native builder Docker daemon identity is invalid")
        runtimes = info.get("Runtimes")
        if (
            info.get("Architecture") != "aarch64"
            or info.get("DockerRootDir") != _DOCKER_ROOT
            or not isinstance(runtimes, dict)
            or "runsc-personal-dev-native" not in runtimes
        ):
            raise RuntimeError("native builder Docker daemon identity is invalid")
        image = self._client.images.get(grant.builder_image)
        attrs = getattr(image, "attrs", None)
        if (
            not isinstance(attrs, dict)
            or attrs.get("Architecture") != "arm64"
            or attrs.get("Os") != "linux"
            or not isinstance(attrs.get("RepoDigests"), list)
            or grant.builder_image not in attrs["RepoDigests"]
        ):
            raise RuntimeError("native builder image identity is invalid")

    def _resource_labels(self, resource: Any, *, network: bool) -> dict[str, str]:
        attrs = getattr(resource, "attrs", None)
        if not isinstance(attrs, dict):
            raise ValueError("managed resource inspect is invalid")
        if network:
            raw = attrs.get("Labels")
        else:
            config = attrs.get("Config")
            if not isinstance(config, dict):
                raise ValueError("managed resource inspect is invalid")
            raw = config.get("Labels")
        if (
            not isinstance(raw, dict)
            or set(raw) != _MANAGED_LABEL_KEYS
            or any(
                not isinstance(key, str) or not isinstance(value, str) for key, value in raw.items()
            )
            or raw.get("loom.personal-dev-native-builder.managed") != "true"
        ):
            raise ValueError("managed resource labels are invalid")
        return cast(dict[str, str], raw)

    def _label_identity(self, labels: dict[str, str]) -> tuple[UUID, str]:
        try:
            grant_id = UUID(labels["loom.personal-dev-native-builder.grant-id"])
            candidate_id = UUID(labels["loom.personal-dev-native-builder.candidate-id"])
            attempt_id = UUID(labels["loom.personal-dev-native-builder.attempt-id"])
            lease_epoch = int(labels["loom.personal-dev-native-builder.lease-epoch"])
            agent_instance_id = UUID(labels["loom.personal-dev-native-builder.agent-instance-id"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("managed resource binding is invalid") from None
        if (
            grant_id.int == 0
            or candidate_id.int == 0
            or attempt_id.int == 0
            or lease_epoch <= 0
            or agent_instance_id != self._identity.agent_instance_id
            or labels["loom.personal-dev-native-builder.platform"] != self._identity.platform
            or labels["loom.personal-dev-native-builder.provider"] != self._identity.provider
            or labels["loom.personal-dev-native-builder.agent-image"] != self._identity.agent_image
            or labels["loom.personal-dev-native-builder.builder-image"]
            != self._identity.builder_image
            or labels["loom.personal-dev-native-builder.runtime-profile-sha256"]
            != self._identity.runtime_profile_sha256
            or _HEX_DIGEST.fullmatch(labels["loom.personal-dev-native-builder.candidate-sha256"])
            is None
            or _HEX_DIGEST.fullmatch(labels["loom.personal-dev-native-builder.contract-sha256"])
            is None
        ):
            raise ValueError("managed resource binding is invalid")
        role = labels["loom.personal-dev-native-builder.role"]
        if role not in {"network", "buildkit", "client"}:
            raise ValueError("managed resource role is invalid")
        return grant_id, role

    @staticmethod
    def _binding_labels(labels: dict[str, str]) -> dict[str, str]:
        return {
            key: value
            for key, value in labels.items()
            if key != "loom.personal-dev-native-builder.role"
        }

    def _container_shape_valid(
        self,
        container: Any,
        *,
        labels: dict[str, str],
        role: str,
    ) -> bool:
        attrs = getattr(container, "attrs", None)
        if not isinstance(attrs, dict):
            return False
        grant_id = UUID(labels["loom.personal-dev-native-builder.grant-id"])
        short_id = grant_id.hex[:12]
        network_name = f"loom-pdev-{short_id}"
        name = f"{network_name}-{role}"
        buildkit_host = f"buildkit-{short_id}"
        config = attrs.get("Config")
        host = attrs.get("HostConfig")
        networking = attrs.get("NetworkSettings")
        if (
            not isinstance(config, dict)
            or not isinstance(host, dict)
            or not isinstance(networking, dict)
        ):
            return False
        attached_networks = networking.get("Networks")
        if not isinstance(attached_networks, dict):
            return False
        endpoint = attached_networks.get(network_name)
        aliases = endpoint.get("Aliases") if isinstance(endpoint, dict) else None
        expected_alias = buildkit_host if role == "buildkit" else name
        common = (
            attrs.get("Name") == f"/{name}"
            and config.get("Image") == self._identity.builder_image
            and config.get("Labels") == labels
            and config.get("User") == "1000:1000"
            and host.get("Runtime") == "runsc-personal-dev-native"
            and host.get("ReadonlyRootfs") is True
            and host.get("Memory") == 16 * 1024 * 1024 * 1024
            and host.get("MemorySwap") == 16 * 1024 * 1024 * 1024
            and host.get("Binds") in (None, [])
            and host.get("Devices") == []
            and host.get("RestartPolicy")
            in ({"Name": "no"}, {"Name": "no", "MaximumRetryCount": 0})
            and host.get("NetworkMode") == network_name
            and set(attached_networks) == {network_name}
            and isinstance(aliases, list)
            and expected_alias in aliases
            and all(isinstance(alias, str) for alias in aliases)
            and networking.get("Ports") in ({}, None)
        )
        if not common:
            return False
        if role == "buildkit":
            return (
                config.get("Hostname") == buildkit_host
                and config.get("Entrypoint") == ["/usr/local/bin/loom-personal-dev-buildkitd"]
                and config.get("Cmd") == ["--native-tcp-buildkit-child"]
                and host.get("CapDrop") == ["ALL"]
                and host.get("CapAdd") == ["SETUID", "SETGID"]
                and host.get("SecurityOpt") == ["seccomp=unconfined"]
                and host.get("NanoCpus") == 3_000_000_000
                and host.get("PidsLimit") == 2048
                and host.get("Tmpfs") == _buildkit_tmpfs()
                and config.get("Healthcheck") == _buildkit_healthcheck()
            )
        expected_command = [
            "build",
            "--contract-file",
            "/opt/loom-personal-dev-builder/native-input/contract.json",
            "--capability-directory",
            "/opt/loom-personal-dev-builder/native-input/capabilities",
            "--workspace",
            "/workspace",
            "--native-buildkit-address",
            f"tcp://{buildkit_host}:1234",
        ]
        return (
            config.get("Hostname") == name
            and config.get("Entrypoint") == ["python3", "-m", "loom.personal_dev_sandbox_builder"]
            and config.get("Cmd") == expected_command
            and host.get("CapDrop") == ["ALL"]
            and host.get("CapAdd") == []
            and host.get("SecurityOpt") == ["no-new-privileges:true"]
            and host.get("NanoCpus") == 1_000_000_000
            and host.get("PidsLimit") == 1024
            and host.get("Tmpfs") == _client_tmpfs()
            and config.get("Healthcheck") in ({"Test": ["NONE"]}, {"Test": ["NONE"], "Interval": 0})
        )

    def _network_shape_valid(
        self,
        network: Any,
        *,
        labels: dict[str, str],
    ) -> bool:
        attrs = getattr(network, "attrs", None)
        grant_id = UUID(labels["loom.personal-dev-native-builder.grant-id"])
        return isinstance(attrs, dict) and (
            attrs.get("Name") == f"loom-pdev-{grant_id.hex[:12]}"
            and attrs.get("Driver") == "bridge"
            and attrs.get("Internal") is False
            and attrs.get("Attachable") is False
            and attrs.get("EnableIPv6") is False
            and attrs.get("Labels") == labels
        )

    def _validate_bound_resources(
        self,
        grant: NativeBuilderGrantPayload,
        resources: _DockerGrantResources,
    ) -> None:
        if (
            not resources.complete
            or resources.network is None
            or resources.buildkit is None
            or resources.client is None
        ):
            resources.safe = False
            raise RuntimeError("native builder managed resource shape drift")
        expected = {
            role: self._labels(grant, role=role) for role in ("network", "buildkit", "client")
        }
        try:
            network_labels = self._resource_labels(resources.network, network=True)
            buildkit_labels = self._resource_labels(resources.buildkit, network=False)
            client_labels = self._resource_labels(resources.client, network=False)
        except ValueError:
            resources.safe = False
            raise RuntimeError("native builder managed resource shape drift") from None
        if (
            network_labels != expected["network"]
            or buildkit_labels != expected["buildkit"]
            or client_labels != expected["client"]
            or not self._network_shape_valid(resources.network, labels=network_labels)
            or not self._container_shape_valid(
                resources.buildkit,
                labels=buildkit_labels,
                role="buildkit",
            )
            or not self._container_shape_valid(
                resources.client,
                labels=client_labels,
                role="client",
            )
        ):
            resources.safe = False
            raise RuntimeError("native builder managed resource shape drift")
        resources.safe = True
        resources.grant = grant

    @staticmethod
    def _inspect_digest(value: object) -> str:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _capability_archive(grant: NativeBuilderGrantPayload) -> bytes:
        upload = json.dumps(
            {
                "fields": dict(grant.artifact_upload_fields),
                "max_bytes": grant.artifact_max_bytes,
                "url": grant.artifact_upload_url,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        files = {
            "native-input/capabilities/artifact-upload.json": upload,
            "native-input/capabilities/source-get-url": grant.source_get_url.encode("utf-8"),
            "native-input/contract.json": grant.contract_json.encode("ascii"),
        }
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for name in ("native-input", "native-input/capabilities"):
                member = tarfile.TarInfo(name)
                member.type = tarfile.DIRTYPE
                member.mode = 0o500
                member.uid = 1000
                member.gid = 1000
                member.mtime = 0
                archive.addfile(member)
            for name in sorted(files):
                payload = files[name]
                member = tarfile.TarInfo(name)
                member.mode = 0o400
                member.uid = 1000
                member.gid = 1000
                member.mtime = 0
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))
        return buffer.getvalue()

    async def _wait_for_buildkit(self, container: Any) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._health_timeout_seconds
        while loop.time() <= deadline:
            await asyncio.to_thread(container.reload)
            state = getattr(container, "attrs", {}).get("State", {})
            health = state.get("Health", {}) if isinstance(state, dict) else {}
            status = health.get("Status") if isinstance(health, dict) else None
            if status == "healthy":
                return
            if status == "unhealthy" or (
                isinstance(state, dict) and state.get("Status") == "exited"
            ):
                raise RuntimeError("native builder BuildKit failed health validation")
            await asyncio.sleep(self._health_poll_seconds)
        raise RuntimeError("native builder BuildKit health validation timed out")

    async def inventory(self) -> NativeBuilderRuntimeInventory:
        try:
            info, image, containers, networks = await asyncio.gather(
                asyncio.to_thread(self._client.info),
                asyncio.to_thread(
                    self._client.images.get,
                    self._identity.builder_image,
                ),
                asyncio.to_thread(
                    self._client.containers.list,
                    all=True,
                    filters={"label": "loom.personal-dev-native-builder.managed=true"},
                ),
                asyncio.to_thread(
                    self._client.networks.list,
                    filters={"label": "loom.personal-dev-native-builder.managed=true"},
                ),
            )
        except Exception:
            return NativeBuilderRuntimeInventory(
                managed_grant_ids=(),
                active_grant_ids=(),
                available=False,
                unavailable_reason="host_runtime_unavailable",
                readiness_evidence_sha256=self._inspect_digest(
                    {"available": False, "reason": "host_runtime_unavailable"}
                ),
            )
        image_attrs = getattr(image, "attrs", None)
        runtimes = info.get("Runtimes") if isinstance(info, dict) else None
        host_valid = (
            isinstance(info, dict)
            and info.get("Architecture") == "aarch64"
            and info.get("DockerRootDir") == _DOCKER_ROOT
            and isinstance(runtimes, dict)
            and "runsc-personal-dev-native" in runtimes
            and isinstance(image_attrs, dict)
            and image_attrs.get("Architecture") == "arm64"
            and image_attrs.get("Os") == "linux"
            and isinstance(image_attrs.get("RepoDigests"), list)
            and self._identity.builder_image in image_attrs["RepoDigests"]
        )
        groups: dict[UUID, dict[str, list[tuple[Any, dict[str, str]]]]] = {}
        drift = False
        inspected: list[dict[str, str]] = []
        for resource, network in [
            *((container, False) for container in containers),
            *((item, True) for item in networks),
        ]:
            try:
                await asyncio.to_thread(resource.reload)
                labels = self._resource_labels(resource, network=network)
                grant_id, role = self._label_identity(labels)
                if (network and role != "network") or (
                    not network and role not in {"buildkit", "client"}
                ):
                    raise ValueError("managed resource role does not match type")
                groups.setdefault(grant_id, {}).setdefault(role, []).append((resource, labels))
                inspected.append(
                    {
                        "id": str(getattr(resource, "id", "")),
                        "inspect_sha256": self._inspect_digest(getattr(resource, "attrs", None)),
                        "role": role,
                    }
                )
            except (TypeError, ValueError):
                drift = True

        managed_ids = tuple(sorted(groups, key=str))
        if len(managed_ids) > 64:
            drift = True
        active: list[UUID] = []
        discovered: dict[UUID, _DockerGrantResources] = {}
        for grant_id, roles in groups.items():
            if any(len(values) != 1 for values in roles.values()):
                drift = True
                existing = self._resources.get(grant_id)
                if existing is not None:
                    existing.safe = False
                    discovered[grant_id] = existing
                else:
                    discovered[grant_id] = _DockerGrantResources(
                        grant=None,
                        network=(roles.get("network") or [(None, {})])[0][0],
                        buildkit=(roles.get("buildkit") or [(None, {})])[0][0],
                        client=(roles.get("client") or [(None, {})])[0][0],
                        safe=False,
                        complete=False,
                    )
                continue
            network_entry = roles.get("network", [(None, {})])[0]
            buildkit_entry = roles.get("buildkit", [(None, {})])[0]
            client_entry = roles.get("client", [(None, {})])[0]
            network, network_labels = network_entry
            buildkit, buildkit_labels = buildkit_entry
            restricted, client_labels = client_entry
            present_labels = [
                labels
                for resource, labels in (
                    network_entry,
                    buildkit_entry,
                    client_entry,
                )
                if resource is not None
            ]
            bindings = {
                tuple(sorted(self._binding_labels(labels).items()))
                for labels in present_labels
            }
            safe = (
                len(bindings) == 1
                and (
                    network is None
                    or self._network_shape_valid(network, labels=network_labels)
                )
                and (
                    buildkit is None
                    or self._container_shape_valid(
                        buildkit,
                        labels=buildkit_labels,
                        role="buildkit",
                    )
                )
                and (
                    restricted is None
                    or self._container_shape_valid(
                        restricted,
                        labels=client_labels,
                        role="client",
                    )
                )
            )
            complete = set(roles) == {"network", "buildkit", "client"}
            if not safe or not complete:
                drift = True
            resources = _DockerGrantResources(
                grant=(
                    self._resources[grant_id].grant
                    if complete and grant_id in self._resources
                    else None
                ),
                network=network,
                buildkit=buildkit,
                client=restricted,
                safe=safe,
                complete=complete,
            )
            discovered[grant_id] = resources
            if safe and complete and any(
                getattr(container, "attrs", {}).get("State", {}).get("Running") is True
                for container in (buildkit, restricted)
            ):
                active.append(grant_id)
        self._resources = discovered
        available = host_valid and not drift
        reason = (
            None
            if available
            else ("managed_resource_shape_drift" if drift else "host_runtime_drift")
        )
        readiness = self._inspect_digest(
            {
                "available": available,
                "host": {
                    "architecture": info.get("Architecture") if isinstance(info, dict) else None,
                    "docker_root": info.get("DockerRootDir") if isinstance(info, dict) else None,
                    "runtime_present": isinstance(runtimes, dict)
                    and "runsc-personal-dev-native" in runtimes,
                },
                "managed": inspected,
                "reason": reason,
            }
        )
        return NativeBuilderRuntimeInventory(
            managed_grant_ids=managed_ids,
            active_grant_ids=(tuple(sorted(active, key=str)) if available else ()),
            available=available,
            unavailable_reason=reason,
            readiness_evidence_sha256=readiness,
        )

    async def start(self, grant: NativeBuilderGrantPayload) -> None:
        self._validate_grant(grant)
        if grant.grant_id in self._resources:
            resources = self._resources[grant.grant_id]
            if not resources.safe:
                raise RuntimeError("native builder managed resource shape drift")
            self._validate_bound_resources(grant, resources)
            assert resources.buildkit is not None
            assert resources.client is not None
            await asyncio.gather(
                asyncio.to_thread(resources.buildkit.reload),
                asyncio.to_thread(resources.client.reload),
            )
            buildkit_state = resources.buildkit.attrs.get("State", {})
            client_state = resources.client.attrs.get("State", {})
            if client_state.get("Status") == "created":
                archived = await asyncio.to_thread(
                    resources.client.put_archive,
                    "/opt/loom-personal-dev-builder",
                    self._capability_archive(grant),
                )
                if archived is not True:
                    raise RuntimeError("native builder capability staging failed")
                if buildkit_state.get("Status") == "created":
                    await asyncio.to_thread(resources.buildkit.start)
                    await self._wait_for_buildkit(resources.buildkit)
                elif buildkit_state.get("Running") is not True:
                    return
                await asyncio.to_thread(resources.client.start)
            return
        await asyncio.to_thread(self._validate_daemon, grant)
        short_id = grant.grant_id.hex[:12]
        network_name = f"loom-pdev-{short_id}"
        buildkit_name = f"{network_name}-buildkit"
        client_name = f"{network_name}-client"
        buildkit_host = f"buildkit-{short_id}"
        network = await asyncio.to_thread(
            self._client.networks.create,
            network_name,
            driver="bridge",
            internal=False,
            attachable=False,
            enable_ipv6=False,
            check_duplicate=True,
            labels=self._labels(grant, role="network"),
        )
        common: dict[str, object] = {
            "image": grant.builder_image,
            "detach": True,
            "user": "1000:1000",
            "runtime": "runsc-personal-dev-native",
            "network": network_name,
            "read_only": True,
            "mem_limit": 16 * 1024 * 1024 * 1024,
            "memswap_limit": 16 * 1024 * 1024 * 1024,
            "restart_policy": {"Name": "no"},
            "volumes": {},
            "devices": [],
        }
        buildkit = await asyncio.to_thread(
            self._client.containers.create,
            **common,
            name=buildkit_name,
            hostname=buildkit_host,
            networking_config={
                network_name: self._client.api.create_endpoint_config(aliases=[buildkit_host])
            },
            entrypoint=["/usr/local/bin/loom-personal-dev-buildkitd"],
            command=["--native-tcp-buildkit-child"],
            labels=self._labels(grant, role="buildkit"),
            cap_drop=["ALL"],
            cap_add=["SETUID", "SETGID"],
            security_opt=["seccomp=unconfined"],
            nano_cpus=3_000_000_000,
            pids_limit=2048,
            tmpfs=_buildkit_tmpfs(),
            healthcheck={
                "test": [
                    "CMD",
                    "/usr/bin/buildctl",
                    "--addr=tcp://127.0.0.1:1234",
                    "debug",
                    "workers",
                ],
                "interval": 1_000_000_000,
                "timeout": 5_000_000_000,
                "retries": 30,
                "start_period": 1_000_000_000,
            },
        )
        restricted = await asyncio.to_thread(
            self._client.containers.create,
            **common,
            name=client_name,
            hostname=client_name,
            networking_config={
                network_name: self._client.api.create_endpoint_config(aliases=[client_name])
            },
            entrypoint=["python3", "-m", "loom.personal_dev_sandbox_builder"],
            command=[
                "build",
                "--contract-file",
                "/opt/loom-personal-dev-builder/native-input/contract.json",
                "--capability-directory",
                "/opt/loom-personal-dev-builder/native-input/capabilities",
                "--workspace",
                "/workspace",
                "--native-buildkit-address",
                f"tcp://{buildkit_host}:1234",
            ],
            labels=self._labels(grant, role="client"),
            cap_drop=["ALL"],
            cap_add=[],
            security_opt=["no-new-privileges:true"],
            nano_cpus=1_000_000_000,
            pids_limit=1024,
            tmpfs=_client_tmpfs(),
            healthcheck={"test": ["NONE"]},
        )
        self._resources[grant.grant_id] = _DockerGrantResources(
            grant=grant,
            network=network,
            buildkit=buildkit,
            client=restricted,
        )
        archived = await asyncio.to_thread(
            restricted.put_archive,
            "/opt/loom-personal-dev-builder",
            self._capability_archive(grant),
        )
        if archived is not True:
            raise RuntimeError("native builder capability staging failed")
        await asyncio.to_thread(buildkit.start)
        await self._wait_for_buildkit(buildkit)
        await asyncio.to_thread(restricted.start)
        self._validate_bound_resources(grant, self._resources[grant.grant_id])

    async def observe(
        self,
        grant: NativeBuilderGrantPayload,
    ) -> NativeBuilderRuntimeObservation:
        resources = self._resources.get(grant.grant_id)
        if resources is None:
            await self.inventory()
            resources = self._resources.get(grant.grant_id)
        if resources is None or not resources.safe or not resources.complete:
            raise RuntimeError("native builder managed resource shape drift")
        assert resources.network is not None
        assert resources.buildkit is not None
        assert resources.client is not None
        await asyncio.gather(
            asyncio.to_thread(resources.network.reload),
            asyncio.to_thread(resources.buildkit.reload),
            asyncio.to_thread(resources.client.reload),
        )
        self._validate_bound_resources(grant, resources)
        buildkit_state = resources.buildkit.attrs.get("State", {})
        client_state = resources.client.attrs.get("State", {})
        client_restarts = resources.client.attrs.get("RestartCount")
        buildkit_restarts = resources.buildkit.attrs.get("RestartCount")
        if client_restarts != 0 or buildkit_restarts != 0:
            return NativeBuilderRuntimeObservation(
                state="failed",
                failure_reason="container_restart_detected",
                evidence=None,
            )
        if buildkit_state.get("Running") is not True:
            return NativeBuilderRuntimeObservation(
                state="failed",
                failure_reason="buildkit_exit_nonzero",
                evidence=None,
            )
        buildkit_health = buildkit_state.get("Health")
        if not isinstance(buildkit_health, dict) or buildkit_health.get("Status") != "healthy":
            return NativeBuilderRuntimeObservation(
                state="failed",
                failure_reason="buildkit_unhealthy",
                evidence=None,
            )
        if client_state.get("Running") is True:
            return NativeBuilderRuntimeObservation(
                state="running",
                failure_reason=None,
                evidence=None,
            )
        if client_state.get("Status") != "exited":
            return NativeBuilderRuntimeObservation(
                state="running",
                failure_reason=None,
                evidence=None,
            )
        if client_state.get("OOMKilled") is True:
            return NativeBuilderRuntimeObservation(
                state="failed",
                failure_reason="client_oom_killed",
                evidence=None,
            )
        exit_code = client_state.get("ExitCode")
        if type(exit_code) is not int or exit_code != 0:
            return NativeBuilderRuntimeObservation(
                state="failed",
                failure_reason="client_exit_nonzero",
                evidence=None,
            )
        evidence = NativeBuilderRuntimeEvidence(
            agent_instance_id=self._identity.agent_instance_id,
            grant_id=grant.grant_id,
            attempt_id=grant.attempt_id,
            attempt_lease_epoch=grant.attempt_lease_epoch,
            provider=grant.provider,
            platform=grant.platform,
            host_name=self._identity.host_name,
            host_architecture=self._identity.host_architecture,
            host_boot_id=self._identity.host_boot_id,
            agent_image=self._identity.agent_image,
            builder_image=grant.builder_image,
            runtime_profile_sha256=grant.runtime_profile_sha256,
            contract_sha256=grant.contract_sha256,
            runtime_name="runsc-personal-dev-native",
            client_container_id=str(resources.client.id),
            buildkit_container_id=str(resources.buildkit.id),
            network_id=str(resources.network.id),
            client_inspect_sha256=self._inspect_digest(resources.client.attrs),
            buildkit_inspect_sha256=self._inspect_digest(resources.buildkit.attrs),
            network_inspect_sha256=self._inspect_digest(resources.network.attrs),
            client_exit_code=exit_code,
            client_oom_killed=False,
            client_restart_count=client_restarts,
            buildkit_restart_count=buildkit_restarts,
            buildkit_running=True,
            observed_at=datetime.now(UTC),
        )
        return NativeBuilderRuntimeObservation(
            state="succeeded",
            failure_reason=None,
            evidence=evidence,
        )

    async def cancel(self, grant_id: UUID) -> None:
        resources = self._resources.get(grant_id)
        if resources is None:
            await self.inventory()
            resources = self._resources.get(grant_id)
        if resources is None:
            return
        if not resources.safe:
            raise RuntimeError("native builder managed resource shape drift")
        for container in (resources.client, resources.buildkit):
            if container is None:
                continue
            await asyncio.to_thread(container.reload)
            if container.attrs.get("State", {}).get("Running") is True:
                await asyncio.to_thread(container.stop, timeout=10)

    async def cleanup(self, grant_id: UUID) -> None:
        resources = self._resources.get(grant_id)
        if resources is None:
            await self.inventory()
            resources = self._resources.get(grant_id)
        if resources is None:
            return
        if not resources.safe:
            raise RuntimeError("native builder managed resource shape drift")
        present = [
            (role, resource, network)
            for role, resource, network in (
                ("network", resources.network, True),
                ("buildkit", resources.buildkit, False),
                ("client", resources.client, False),
            )
            if resource is not None
        ]
        await asyncio.gather(
            *(asyncio.to_thread(resource.reload) for _role, resource, _network in present)
        )
        if resources.grant is not None and resources.complete:
            self._validate_bound_resources(resources.grant, resources)
        else:
            try:
                labeled = [
                    (
                        role,
                        resource,
                        self._resource_labels(resource, network=network),
                    )
                    for role, resource, network in present
                ]
                identities = {
                    self._label_identity(labels) for _role, _resource, labels in labeled
                }
                bindings = {
                    tuple(sorted(self._binding_labels(labels).items()))
                    for _role, _resource, labels in labeled
                }
                if (
                    not labeled
                    or identities != {(grant_id, role) for role, _resource, _labels in labeled}
                    or len(bindings) != 1
                    or any(
                        (
                            not self._network_shape_valid(resource, labels=labels)
                            if role == "network"
                            else not self._container_shape_valid(
                                resource,
                                labels=labels,
                                role=role,
                            )
                        )
                        for role, resource, labels in labeled
                    )
                ):
                    raise ValueError("managed resource shape changed")
            except (TypeError, ValueError):
                resources.safe = False
                raise RuntimeError("native builder managed resource shape drift") from None
        if resources.client is not None:
            await asyncio.to_thread(resources.client.remove, force=True, v=True)
        if resources.buildkit is not None:
            await asyncio.to_thread(resources.buildkit.remove, force=True, v=True)
        if resources.network is not None:
            await asyncio.to_thread(resources.network.remove)
        self._resources.pop(grant_id, None)


class PersonalDevNativeBuilderAgent:
    """Poll and reconcile exact native resources without central credentials."""

    def __init__(
        self,
        *,
        authority: PersonalDevNativeBuilderAuthority,
        runtime: PersonalDevNativeBuildRuntime,
        signer: PersonalDevNativeBuilderSigner,
        identity: NativeBuilderAgentIdentity,
        heartbeat_grace_seconds: int,
        heartbeat_interval_seconds: float = 10.0,
    ) -> None:
        if type(heartbeat_grace_seconds) is not int or not 1 <= heartbeat_grace_seconds <= 300:
            raise ValueError("native builder heartbeat grace is invalid")
        if (
            not isinstance(heartbeat_interval_seconds, (int, float))
            or isinstance(heartbeat_interval_seconds, bool)
            or not 0 < heartbeat_interval_seconds <= 60
            or heartbeat_grace_seconds < 2 * heartbeat_interval_seconds
        ):
            raise ValueError("native builder heartbeat interval is invalid")
        identity.status(
            NativeBuilderRuntimeInventory(
                managed_grant_ids=(),
                active_grant_ids=(),
                available=True,
                unavailable_reason=None,
                readiness_evidence_sha256="1" * 64,
            )
        )
        self._authority = authority
        self._runtime = runtime
        self._signer = signer
        self._identity = identity
        self._heartbeat_grace = timedelta(seconds=heartbeat_grace_seconds)
        self._heartbeat_interval = timedelta(seconds=heartbeat_interval_seconds)
        self._authority_failure_since: datetime | None = None
        self._heartbeat_failure_since: dict[UUID, datetime] = {}
        self._last_heartbeat_at: dict[UUID, datetime] = {}
        self._grants: dict[UUID, NativeBuilderGrantPayload] = {}
        self._last_request_at: datetime | None = None

    def _next_request_time(self, now: datetime) -> datetime:
        candidate = now
        if self._last_request_at is not None and candidate <= self._last_request_at:
            candidate = self._last_request_at + timedelta(microseconds=1)
        self._last_request_at = candidate
        return candidate

    @staticmethod
    def _grant_binding(grant: NativeBuilderGrantPayload) -> tuple[object, ...]:
        return (
            grant.grant_id,
            grant.candidate_id,
            grant.candidate_sha,
            grant.attempt_id,
            grant.attempt_lease_epoch,
            grant.platform,
            grant.provider,
            grant.agent_instance_id,
            grant.agent_key_id,
            grant.builder_image,
            grant.runtime_profile_sha256,
            grant.contract_sha256,
            grant.artifact_max_bytes,
            grant.active_deadline_seconds,
        )

    async def _cancel_and_cleanup(self, grant_id: UUID) -> None:
        await self._runtime.cancel(grant_id)
        await self._runtime.cleanup(grant_id)
        self._grants.pop(grant_id, None)
        self._heartbeat_failure_since.pop(grant_id, None)
        self._last_heartbeat_at.pop(grant_id, None)

    async def _advance_grant(
        self,
        grant: NativeBuilderGrantPayload,
        *,
        now: datetime,
    ) -> None:
        observation = await self._runtime.observe(grant)
        if observation.state == "running":
            previous_heartbeat = self._last_heartbeat_at.get(grant.grant_id)
            if (
                previous_heartbeat is not None
                and now - previous_heartbeat < self._heartbeat_interval
            ):
                return
            heartbeat = NativeBuilderHeartbeatRequest(
                agent_instance_id=self._identity.agent_instance_id,
                agent_key_id=self._identity.agent_key_id,
                grant_id=grant.grant_id,
                attempt_id=grant.attempt_id,
                attempt_lease_epoch=grant.attempt_lease_epoch,
                requested_at=self._next_request_time(now),
                request_nonce=uuid4(),
            )
            try:
                should_continue = await self._authority.heartbeat(
                    heartbeat,
                    signature=self._signer.sign_heartbeat(heartbeat),
                )
            except Exception:
                failed_at = self._heartbeat_failure_since.setdefault(
                    grant.grant_id,
                    now,
                )
                if now - failed_at >= self._heartbeat_grace:
                    await self._runtime.cancel(grant.grant_id)
                raise
            self._heartbeat_failure_since.pop(grant.grant_id, None)
            if not should_continue:
                await self._cancel_and_cleanup(grant.grant_id)
            else:
                self._last_heartbeat_at[grant.grant_id] = now
            return
        completion = NativeBuilderCompletion(
            agent_instance_id=self._identity.agent_instance_id,
            agent_key_id=self._identity.agent_key_id,
            grant_id=grant.grant_id,
            attempt_id=grant.attempt_id,
            attempt_lease_epoch=grant.attempt_lease_epoch,
            outcome=("succeeded" if observation.state == "succeeded" else "failed"),
            failure_reason=observation.failure_reason,
            evidence=observation.evidence,
            requested_at=self._next_request_time(now),
            request_nonce=uuid4(),
        )
        await self._authority.complete(
            completion,
            signature=self._signer.sign_completion(completion),
        )
        await self._runtime.cancel(grant.grant_id)
        await self._runtime.cleanup(grant.grant_id)
        self._grants.pop(grant.grant_id, None)
        self._heartbeat_failure_since.pop(grant.grant_id, None)
        self._last_heartbeat_at.pop(grant.grant_id, None)

    async def reconcile_once(self, *, now: datetime) -> bool:
        inventory = await self._runtime.inventory()
        request = NativeBuilderPollRequest(
            status=self._identity.status(inventory),
            requested_at=self._next_request_time(now),
            request_nonce=uuid4(),
        )
        try:
            result = await self._authority.poll(
                request,
                signature=self._signer.sign_poll(request),
            )
        except Exception:
            if self._authority_failure_since is None:
                self._authority_failure_since = now
            if now - self._authority_failure_since >= self._heartbeat_grace:
                for grant_id in inventory.active_grant_ids:
                    await self._runtime.cancel(grant_id)
            raise
        self._authority_failure_since = None
        changed = False
        for grant_id in result.cancel_grant_ids:
            await self._cancel_and_cleanup(grant_id)
            changed = True
        if result.grant is not None:
            if not inventory.available:
                raise RuntimeError("native builder local runtime is unavailable")
            existing = self._grants.get(result.grant.grant_id)
            if existing is None and len(self._grants) >= self._identity.max_concurrency:
                raise RuntimeError("native builder local concurrency is exhausted")
            if existing is not None and self._grant_binding(existing) != self._grant_binding(
                result.grant
            ):
                raise RuntimeError("native builder grant binding changed")
            self._grants[result.grant.grant_id] = result.grant
            await self._runtime.start(result.grant)
            changed = True
        cancelled = set(result.cancel_grant_ids)
        active = [
            self._grants[grant_id]
            for grant_id in sorted(tuple(self._grants), key=str)
            if grant_id not in cancelled
        ]
        if active:
            outcomes = await asyncio.gather(
                *(self._advance_grant(grant, now=now) for grant in active),
                return_exceptions=True,
            )
            changed = True
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    raise outcome
        return changed


__all__ = [
    "DockerPersonalDevNativeBuildRuntime",
    "HttpPersonalDevNativeBuilderAuthority",
    "NativeBuilderAgentIdentity",
    "NativeBuilderAgentPollResult",
    "NativeBuilderRuntimeInventory",
    "PersonalDevNativeBuildRuntime",
    "PersonalDevNativeBuilderAgent",
    "PersonalDevNativeBuilderAuthority",
]
