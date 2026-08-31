from __future__ import annotations

import asyncio
import io
import json
import tarfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.personal_dev_native_builder_agent import (
    DockerPersonalDevNativeBuildRuntime,
    HttpPersonalDevNativeBuilderAuthority,
    NativeBuilderAgentIdentity,
    NativeBuilderAgentPollResult,
    NativeBuilderRuntimeInventory,
    NativeBuilderRuntimeObservation,
    PersonalDevNativeBuilderAgent,
)
from loom.personal_dev_native_builder_protocol import (
    PersonalDevNativeBuilderSigner,
    PersonalDevNativeBuilderVerifier,
)
from tests.unit.test_personal_dev_native_builder_protocol import (
    _completion,
    _evidence,
    _grant,
    _heartbeat,
    _poll,
    _status,
)

_ROOT = Path(__file__).resolve().parents[2]
_DOCKER_DATA_ROOT = json.loads(
    (_ROOT / "deploy/personal-dev-native-builder/dockerd.json").read_text(
        encoding="utf-8"
    )
)["data-root"]


def _grant_response() -> dict[str, object]:
    grant = _grant()
    return {
        "active_deadline_seconds": grant.active_deadline_seconds,
        "agent_instance_id": str(grant.agent_instance_id),
        "agent_key_id": grant.agent_key_id,
        "artifact_max_bytes": grant.artifact_max_bytes,
        "artifact_upload_fields": dict(grant.artifact_upload_fields),
        "artifact_upload_url": grant.artifact_upload_url,
        "attempt_id": str(grant.attempt_id),
        "attempt_lease_epoch": grant.attempt_lease_epoch,
        "builder_image": grant.builder_image,
        "candidate_id": str(grant.candidate_id),
        "candidate_sha": grant.candidate_sha,
        "capability_expires_at": (
            grant.capability_expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        ),
        "contract_json": grant.contract_json,
        "contract_sha256": grant.contract_sha256,
        "grant_id": str(grant.grant_id),
        "platform": grant.platform,
        "provider": grant.provider,
        "runtime_profile_sha256": grant.runtime_profile_sha256,
        "source_get_url": grant.source_get_url,
    }


async def test_http_authority_sends_canonical_signed_poll_and_parses_exact_grant() -> None:
    poll = _poll()
    cancellation = "00000000-0000-0000-0000-000000000099"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "cancel_grant_ids": [cancellation],
                "grant": _grant_response(),
            },
            headers={"Cache-Control": "no-store"},
        )

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(handler),
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevNativeBuilderAuthority(client)
        result = await authority.poll(poll, signature="1" * 128)

    assert len(requests) == 1
    assert requests[0].url.path == "/api/v1/internal/personal-dev/native-builder/poll"
    assert requests[0].content == poll.canonical_bytes()
    assert requests[0].headers["Content-Type"] == "application/json"
    assert requests[0].headers["X-Loom-Native-Builder-Signature"] == "1" * 128
    assert result.grant == _grant()
    assert [str(value) for value in result.cancel_grant_ids] == [cancellation]


async def test_http_authority_accepts_only_bodyless_no_grant_response() -> None:
    poll = _poll()

    def bodyless(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(bodyless),
        trust_env=False,
    ) as client:
        result = await HttpPersonalDevNativeBuilderAuthority(client).poll(
            poll,
            signature="1" * 128,
        )

    assert result.grant is None
    assert result.cancel_grant_ids == ()


async def test_http_authority_sends_canonical_heartbeat_and_completion() -> None:
    heartbeat = _heartbeat()
    completion = _completion()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json={"continue": False})
        return httpx.Response(200, json={"accepted": True, "state": "succeeded"})

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(handler),
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevNativeBuilderAuthority(client)
        should_continue = await authority.heartbeat(heartbeat, signature="2" * 128)
        await authority.complete(completion, signature="3" * 128)

    assert should_continue is False
    assert requests[0].content == heartbeat.canonical_bytes()
    assert requests[1].content == completion.canonical_bytes()
    assert requests[0].headers["X-Loom-Native-Builder-Signature"] == "2" * 128
    assert requests[1].headers["X-Loom-Native-Builder-Signature"] == "3" * 128
    assert str(heartbeat.grant_id) in requests[0].url.path
    assert str(completion.grant_id) in requests[1].url.path


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json={"cancel_grant_ids": [], "grant": None, "extra": "secret"}),
        httpx.Response(200, content=b"not-json-secret"),
        httpx.Response(503, content=b"upstream-secret"),
        httpx.Response(204, content=b"unexpected-secret"),
    ],
)
async def test_http_authority_rejects_invalid_poll_without_echoing_body(
    response: httpx.Response,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(lambda _request: response),
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevNativeBuilderAuthority(client)
        with pytest.raises(RuntimeError, match="native builder poll response is invalid") as exc:
            await authority.poll(_poll(), signature="1" * 128)

    combined = str(exc.value) + caplog.text
    for secret in ("secret", "not-json", "upstream"):
        assert secret not in combined


async def test_http_authority_requires_no_store_and_hides_transport_errors() -> None:
    responses = [
        httpx.Response(
            200,
            json={"cancel_grant_ids": [], "grant": _grant_response()},
        ),
        RuntimeError("transport-secret"),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(handler),
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevNativeBuilderAuthority(client)
        with pytest.raises(RuntimeError, match="native builder poll response is invalid"):
            await authority.poll(_poll(), signature="1" * 128)
        with pytest.raises(RuntimeError, match="native builder poll response is invalid") as exc:
            await authority.poll(_poll(), signature="1" * 128)

    assert "transport-secret" not in str(exc.value)


async def test_http_authority_hides_response_close_errors() -> None:
    class _CloseErrorStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b""

        async def aclose(self) -> None:
            raise RuntimeError("close-secret")

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(204, stream=_CloseErrorStream())
        ),
        trust_env=False,
    ) as client:
        with pytest.raises(RuntimeError, match="native builder poll response is invalid") as exc:
            await HttpPersonalDevNativeBuilderAuthority(client).poll(
                _poll(),
                signature="1" * 128,
            )

    assert "close-secret" not in str(exc.value)


def test_agent_poll_result_rejects_ambiguous_cancellation_inventory() -> None:
    first = UUID("00000000-0000-0000-0000-000000000010")
    second = UUID("00000000-0000-0000-0000-000000000011")
    with pytest.raises(ValueError, match="cancellation inventory"):
        NativeBuilderAgentPollResult(
            grant=None,
            cancel_grant_ids=(second, first),
        )
    with pytest.raises(ValueError, match="cancellation inventory"):
        NativeBuilderAgentPollResult(
            grant=None,
            cancel_grant_ids=(first, first),
        )
    with pytest.raises(ValueError, match="cancellation inventory"):
        NativeBuilderAgentPollResult(
            grant=_grant(),
            cancel_grant_ids=(_grant().grant_id,),
        )


@pytest.mark.parametrize(
    ("base_url", "trust_env"),
    [
        ("http://management.example", False),
        ("https://user@management.example", False),
        ("https://management.example/prefix", False),
        ("https://management.example?query=1", False),
        ("https://management.example", True),
    ],
)
async def test_http_authority_requires_https_root_origin_without_environment_proxy(
    base_url: str,
    trust_env: bool,
) -> None:
    async with httpx.AsyncClient(
        base_url=base_url,
        transport=httpx.MockTransport(lambda _request: httpx.Response(204)),
        trust_env=trust_env,
    ) as client:
        with pytest.raises(ValueError, match="native builder service client"):
            HttpPersonalDevNativeBuilderAuthority(client)


def _identity() -> NativeBuilderAgentIdentity:
    status = _status()
    return NativeBuilderAgentIdentity(
        agent_instance_id=status.agent_instance_id,
        agent_key_id=status.agent_key_id,
        provider=status.provider,
        platform=status.platform,
        protocol_version=status.protocol_version,
        host_name=status.host_name,
        host_architecture=status.host_architecture,
        host_boot_id=status.host_boot_id,
        agent_image=status.agent_image,
        builder_image=status.builder_image,
        runtime_profile_sha256=status.runtime_profile_sha256,
        max_concurrency=status.max_concurrency,
    )


class _AgentAuthority:
    def __init__(self, result: NativeBuilderAgentPollResult | Exception) -> None:
        self.result = result
        self.polls = []

    async def poll(self, request, *, signature: str) -> NativeBuilderAgentPollResult:
        self.polls.append((request, signature))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def heartbeat(self, request, *, signature: str) -> bool:
        raise AssertionError("no heartbeat expected")

    async def complete(self, completion, *, signature: str) -> None:
        raise AssertionError("no completion expected")


class _AgentRuntime:
    def __init__(self, inventory: NativeBuilderRuntimeInventory) -> None:
        self.current_inventory = inventory
        self.cancelled: list[UUID] = []
        self.cleaned: list[UUID] = []

    async def inventory(self) -> NativeBuilderRuntimeInventory:
        return self.current_inventory

    async def start(self, grant) -> None:
        raise AssertionError("no grant start expected")

    async def observe(self, grant):
        raise AssertionError("no grant observation expected")

    async def cancel(self, grant_id: UUID) -> None:
        self.cancelled.append(grant_id)

    async def cleanup(self, grant_id: UUID) -> None:
        self.cleaned.append(grant_id)


def _signer() -> tuple[PersonalDevNativeBuilderSigner, PersonalDevNativeBuilderVerifier]:
    private = Ed25519PrivateKey.generate()
    private_bytes = private.private_bytes_raw()
    signer = PersonalDevNativeBuilderSigner(
        keys={_status().agent_key_id: private_bytes},
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={_status().agent_key_id: private.public_key().public_bytes_raw()},
    )
    return signer, verifier


async def test_agent_polls_with_exact_runtime_inventory_and_applies_cancellation() -> None:
    grant_id = _grant().grant_id
    inventory = NativeBuilderRuntimeInventory(
        managed_grant_ids=(grant_id,),
        active_grant_ids=(),
        available=True,
        unavailable_reason=None,
        readiness_evidence_sha256="d" * 64,
    )
    runtime = _AgentRuntime(inventory)
    authority = _AgentAuthority(
        NativeBuilderAgentPollResult(grant=None, cancel_grant_ids=(grant_id,))
    )
    signer, verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
    )
    now = datetime(2026, 8, 30, 16, 1, tzinfo=UTC)

    assert await agent.reconcile_once(now=now) is True

    assert runtime.cancelled == [grant_id]
    assert runtime.cleaned == [grant_id]
    request, signature = authority.polls[0]
    assert request.status.managed_grant_ids == (grant_id,)
    assert request.status.active_grant_ids == ()
    verifier.verify_poll(request, signature=signature, now=now)


async def test_agent_stops_active_resources_after_authority_grace_without_cleanup() -> None:
    grant_id = _grant().grant_id
    inventory = NativeBuilderRuntimeInventory(
        managed_grant_ids=(grant_id,),
        active_grant_ids=(grant_id,),
        available=True,
        unavailable_reason=None,
        readiness_evidence_sha256="d" * 64,
    )
    runtime = _AgentRuntime(inventory)
    authority = _AgentAuthority(RuntimeError("management unavailable"))
    signer, _verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
    )
    started = datetime(2026, 8, 30, 16, 1, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="management unavailable"):
        await agent.reconcile_once(now=started)
    assert runtime.cancelled == []

    with pytest.raises(RuntimeError, match="management unavailable"):
        await agent.reconcile_once(now=started + timedelta(seconds=30))

    assert runtime.cancelled == [grant_id]
    assert runtime.cleaned == []


async def test_agent_refuses_grant_while_local_runtime_is_unavailable() -> None:
    inventory = NativeBuilderRuntimeInventory(
        managed_grant_ids=(),
        active_grant_ids=(),
        available=False,
        unavailable_reason="host_runtime_drift",
        readiness_evidence_sha256="d" * 64,
    )
    runtime = _AgentRuntime(inventory)
    authority = _AgentAuthority(NativeBuilderAgentPollResult(grant=_grant(), cancel_grant_ids=()))
    signer, _verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
    )

    with pytest.raises(RuntimeError, match="runtime is unavailable"):
        await agent.reconcile_once(
            now=datetime(2026, 8, 30, 16, 2, tzinfo=UTC),
        )


class _LifecycleAuthority(_AgentAuthority):
    def __init__(
        self,
        poll_results: list[NativeBuilderAgentPollResult],
        *,
        continue_build: bool = True,
        completion_failures: int = 0,
    ) -> None:
        super().__init__(poll_results[0])
        self.poll_results = poll_results
        self.continue_build = continue_build
        self.completion_failures = completion_failures
        self.heartbeats = []
        self.completions = []

    async def poll(self, request, *, signature: str) -> NativeBuilderAgentPollResult:
        self.polls.append((request, signature))
        return self.poll_results.pop(0)

    async def heartbeat(self, request, *, signature: str) -> bool:
        self.heartbeats.append((request, signature))
        return self.continue_build

    async def complete(self, completion, *, signature: str) -> None:
        self.completions.append((completion, signature))
        if self.completion_failures:
            self.completion_failures -= 1
            raise RuntimeError("completion unavailable")


class _LifecycleRuntime(_AgentRuntime):
    def __init__(self, observation: NativeBuilderRuntimeObservation) -> None:
        super().__init__(
            NativeBuilderRuntimeInventory(
                managed_grant_ids=(),
                active_grant_ids=(),
                available=True,
                unavailable_reason=None,
                readiness_evidence_sha256="d" * 64,
            )
        )
        self.observation = observation
        self.started = []
        self.observed = []

    async def start(self, grant) -> None:
        self.started.append(grant)
        self.current_inventory = NativeBuilderRuntimeInventory(
            managed_grant_ids=(grant.grant_id,),
            active_grant_ids=(grant.grant_id,),
            available=True,
            unavailable_reason=None,
            readiness_evidence_sha256="d" * 64,
        )

    async def observe(self, grant) -> NativeBuilderRuntimeObservation:
        self.observed.append(grant)
        return self.observation


async def test_agent_heartbeats_running_grant_and_obeys_cancellation() -> None:
    grant = _grant()
    authority = _LifecycleAuthority(
        [NativeBuilderAgentPollResult(grant=grant, cancel_grant_ids=())],
        continue_build=False,
    )
    runtime = _LifecycleRuntime(
        NativeBuilderRuntimeObservation(
            state="running",
            failure_reason=None,
            evidence=None,
        )
    )
    signer, verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
    )
    now = datetime(2026, 8, 30, 16, 2, tzinfo=UTC)

    assert await agent.reconcile_once(now=now) is True

    assert runtime.started == [grant]
    assert runtime.observed == [grant]
    assert runtime.cancelled == [grant.grant_id]
    assert runtime.cleaned == [grant.grant_id]
    heartbeat, signature = authority.heartbeats[0]
    assert heartbeat.requested_at > authority.polls[0][0].requested_at
    verifier.verify_heartbeat(heartbeat, signature=signature, now=heartbeat.requested_at)


async def test_agent_retries_identical_completion_before_exact_cleanup() -> None:
    grant = _grant()
    authority = _LifecycleAuthority(
        [
            NativeBuilderAgentPollResult(grant=grant, cancel_grant_ids=()),
            NativeBuilderAgentPollResult(grant=None, cancel_grant_ids=()),
        ],
        completion_failures=1,
    )
    runtime = _LifecycleRuntime(
        NativeBuilderRuntimeObservation(
            state="succeeded",
            failure_reason=None,
            evidence=_evidence(),
        )
    )
    signer, verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
    )
    now = datetime(2026, 8, 30, 16, 2, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="completion unavailable"):
        await agent.reconcile_once(now=now)
    assert runtime.cleaned == []

    assert await agent.reconcile_once(now=now + timedelta(seconds=1)) is True

    assert runtime.cleaned == [grant.grant_id]
    assert runtime.cancelled == [grant.grant_id]
    assert len(authority.completions) == 2
    first, _first_signature = authority.completions[0]
    second, second_signature = authority.completions[1]
    assert first.outcome == second.outcome == "succeeded"
    assert first.evidence == second.evidence == _evidence()
    assert second.requested_at > first.requested_at
    verifier.verify_completion(
        second,
        signature=second_signature,
        now=second.requested_at,
    )


async def test_agent_honors_configured_heartbeat_interval() -> None:
    grant = _grant()
    authority = _LifecycleAuthority(
        [
            NativeBuilderAgentPollResult(grant=grant, cancel_grant_ids=()),
            NativeBuilderAgentPollResult(grant=None, cancel_grant_ids=()),
            NativeBuilderAgentPollResult(grant=None, cancel_grant_ids=()),
        ]
    )
    runtime = _LifecycleRuntime(
        NativeBuilderRuntimeObservation(
            state="running",
            failure_reason=None,
            evidence=None,
        )
    )
    signer, _verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
        heartbeat_interval_seconds=10,
    )
    now = datetime(2026, 8, 30, 16, 2, tzinfo=UTC)

    await agent.reconcile_once(now=now)
    await agent.reconcile_once(now=now + timedelta(seconds=5))
    await agent.reconcile_once(now=now + timedelta(seconds=10))

    assert len(authority.heartbeats) == 2


async def test_agent_advances_two_active_grants_concurrently() -> None:
    first = _grant()
    second = replace(
        first,
        grant_id=UUID("00000000-0000-0000-0000-000000000020"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000021"),
    )

    class _ConcurrentAuthority(_LifecycleAuthority):
        def __init__(self) -> None:
            super().__init__(
                [
                    NativeBuilderAgentPollResult(grant=first, cancel_grant_ids=()),
                    NativeBuilderAgentPollResult(grant=second, cancel_grant_ids=()),
                ]
            )
            self.require_pair = False
            self.round_calls = 0
            self.pair = asyncio.Event()

        async def heartbeat(self, request, *, signature: str) -> bool:
            self.heartbeats.append((request, signature))
            if not self.require_pair:
                return True
            self.round_calls += 1
            if self.round_calls == 2:
                self.pair.set()
            await asyncio.wait_for(self.pair.wait(), timeout=0.5)
            return True

    authority = _ConcurrentAuthority()
    runtime = _LifecycleRuntime(
        NativeBuilderRuntimeObservation(
            state="running",
            failure_reason=None,
            evidence=None,
        )
    )
    signer, _verifier = _signer()
    agent = PersonalDevNativeBuilderAgent(
        authority=authority,
        runtime=runtime,
        signer=signer,
        identity=_identity(),
        heartbeat_grace_seconds=30,
        heartbeat_interval_seconds=10,
    )
    now = datetime(2026, 8, 30, 16, 2, tzinfo=UTC)
    await agent.reconcile_once(now=now)
    authority.require_pair = True

    await asyncio.wait_for(
        agent.reconcile_once(now=now + timedelta(seconds=10)),
        timeout=1,
    )

    assert authority.round_calls == 2


class _FakeImage:
    def __init__(self, reference: str) -> None:
        self.attrs = {
            "Architecture": "arm64",
            "Os": "linux",
            "RepoDigests": [reference],
        }


class _FakeImages:
    def __init__(self, reference: str) -> None:
        self.reference = reference

    def get(self, reference: str) -> _FakeImage:
        assert reference == self.reference
        return _FakeImage(reference)


class _FakeContainer:
    def __init__(self, name: str, options: dict[str, object], events: list[str]) -> None:
        self.name = name
        self.id = ("1" if name.endswith("client") else "2") * 64
        self._events = events
        self.archives: list[tuple[str, bytes]] = []
        raw_healthcheck = options.get("healthcheck")
        healthcheck = (
            {
                "Test": raw_healthcheck.get("test"),
                "Interval": raw_healthcheck.get("interval"),
                "Timeout": raw_healthcheck.get("timeout"),
                "Retries": raw_healthcheck.get("retries"),
                "StartPeriod": raw_healthcheck.get("start_period"),
            }
            if isinstance(raw_healthcheck, dict)
            else None
        )
        if healthcheck is not None:
            healthcheck = {key: value for key, value in healthcheck.items() if value is not None}
        network_name = str(options.get("network"))
        networking_config = options.get("networking_config")
        endpoint = (
            networking_config.get(network_name) if isinstance(networking_config, dict) else None
        )
        aliases = endpoint.get("Aliases") if isinstance(endpoint, dict) else []
        self.attrs = {
            "Id": self.id,
            "Name": f"/{name}",
            "Config": {
                "Image": options.get("image"),
                "Labels": options.get("labels"),
                "User": options.get("user"),
                "Entrypoint": options.get("entrypoint"),
                "Cmd": options.get("command"),
                "Hostname": options.get("hostname"),
                "Healthcheck": healthcheck,
            },
            "HostConfig": {
                "Runtime": options.get("runtime"),
                "CgroupParent": options.get("cgroup_parent"),
                "ReadonlyRootfs": options.get("read_only"),
                "CapDrop": options.get("cap_drop"),
                "CapAdd": options.get("cap_add"),
                "SecurityOpt": options.get("security_opt"),
                "NanoCpus": options.get("nano_cpus"),
                "Memory": options.get("mem_limit"),
                "MemorySwap": options.get("memswap_limit"),
                "PidsLimit": options.get("pids_limit"),
                "Binds": None,
                "Devices": options.get("devices"),
                "Privileged": False,
                "PublishAllPorts": False,
                "PortBindings": {},
                "RestartPolicy": {
                    **dict(options.get("restart_policy", {})),
                    "MaximumRetryCount": 0,
                },
                "Tmpfs": options.get("tmpfs"),
                "NetworkMode": options.get("network"),
            },
            "NetworkSettings": {
                "Networks": {network_name: {"Aliases": list(aliases)}},
                "Ports": {},
            },
            "State": {
                "Status": "created",
                "Running": False,
                "OOMKilled": False,
                "ExitCode": 0,
                "Health": {"Status": "healthy"},
            },
            "RestartCount": 0,
        }

    def put_archive(self, path: str, data: bytes) -> bool:
        self._events.append(f"archive:{self.name}")
        self.archives.append((path, data))
        return True

    def start(self) -> None:
        self._events.append(f"start:{self.name}")
        self.attrs["State"]["Status"] = "running"
        self.attrs["State"]["Running"] = True

    def reload(self) -> None:
        self._events.append(f"reload:{self.name}")

    def stop(self, timeout: int) -> None:
        self._events.append(f"stop:{self.name}:{timeout}")
        self.attrs["State"]["Status"] = "exited"
        self.attrs["State"]["Running"] = False

    def remove(self, force: bool, v: bool) -> None:
        self._events.append(f"remove:{self.name}:{force}:{v}")


class _FakeContainers:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.created: list[tuple[str, dict[str, object], _FakeContainer]] = []

    def create(self, image: str, **kwargs) -> _FakeContainer:
        name = str(kwargs["name"])
        self.events.append(f"create:{name}")
        options = {"image": image, **kwargs}
        container = _FakeContainer(name, options, self.events)
        self.created.append((image, kwargs, container))
        return container

    def list(self, *, all: bool, filters: dict[str, object]) -> list[_FakeContainer]:
        assert all is True
        assert filters == {"label": "loom.personal-dev-native-builder.managed=true"}
        return [container for _image, _options, container in self.created]


class _FakeNetwork:
    def __init__(
        self,
        name: str,
        labels: dict[str, str],
        events: list[str],
        *,
        enable_ipv6: bool,
        ipam: dict[str, object] | None,
    ) -> None:
        self.name = name
        self.id = "3" * 64
        self.attrs = {
            "Id": self.id,
            "Name": name,
            "Driver": "bridge",
            "Internal": False,
            "Attachable": False,
            "EnableIPv6": enable_ipv6,
            "IPAM": self._inspect_ipam(ipam),
            "Labels": labels,
        }
        self._events = events

    @staticmethod
    def _inspect_ipam(ipam: dict[str, object] | None) -> dict[str, object]:
        if ipam is None:
            config = {"Subnet": "172.28.0.0/24", "Gateway": "172.28.0.1"}
        else:
            config = dict(ipam["Config"][0])
        return {
            "Driver": "default",
            "Options": {},
            "Config": [{**config, "IPRange": ""}],
        }

    def reload(self) -> None:
        self._events.append(f"reload:{self.name}")

    def remove(self) -> None:
        self._events.append(f"remove:{self.name}")


class _FakeNetworks:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.created: list[tuple[dict[str, object], _FakeNetwork]] = []

    def create(self, name: str, **kwargs) -> _FakeNetwork:
        self.events.append(f"create:{name}")
        network = _FakeNetwork(
            name,
            kwargs["labels"],
            self.events,
            enable_ipv6=kwargs["enable_ipv6"],
            ipam=kwargs.get("ipam"),
        )
        self.created.append(({"name": name, **kwargs}, network))
        return network

    def list(self, *, filters: dict[str, object]) -> list[_FakeNetwork]:
        assert filters == {"label": "loom.personal-dev-native-builder.managed=true"}
        return [network for _options, network in self.created]


class _FakeDockerClient:
    def __init__(self, reference: str) -> None:
        self.events: list[str] = []
        self.images = _FakeImages(reference)
        self.containers = _FakeContainers(self.events)
        self.networks = _FakeNetworks(self.events)
        self.api = _FakeDockerAPI()

    def info(self) -> dict[str, object]:
        return {
            "Architecture": "aarch64",
            "DockerRootDir": _DOCKER_DATA_ROOT,
            "Runtimes": {"runsc-personal-dev-native": {"path": "/opt/loom/gvisor/runsc"}},
        }


class _FakeDockerAPI:
    def create_endpoint_config(self, *, aliases: list[str]) -> dict[str, object]:
        return {"Aliases": aliases}


async def test_docker_runtime_creates_exact_two_sandbox_contract_before_start() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )

    await runtime.start(grant)

    assert len(client.networks.created) == 1
    network_options, network = client.networks.created[0]
    assert network_options == {
        "name": f"loom-pdev-{grant.grant_id.hex[:12]}",
        "driver": "bridge",
        "internal": False,
        "attachable": False,
        "enable_ipv6": False,
        "check_duplicate": True,
        "ipam": {
            "Driver": "default",
            "Config": [
                {"Subnet": "172.28.0.0/24", "Gateway": "172.28.0.1"},
            ],
        },
        "labels": network.attrs["Labels"],
    }
    assert len(client.containers.created) == 2
    created = {
        options["name"]: (image, options, container)
        for image, options, container in client.containers.created
    }
    buildkit_name = f"loom-pdev-{grant.grant_id.hex[:12]}-buildkit"
    client_name = f"loom-pdev-{grant.grant_id.hex[:12]}-client"
    buildkit_image, buildkit, buildkit_container = created[buildkit_name]
    client_image, restricted, restricted_container = created[client_name]

    assert buildkit_image == client_image == grant.builder_image
    for options in (buildkit, restricted):
        assert options["runtime"] == "runsc-personal-dev-native"
        assert options["cgroup_parent"] == "loom-personal-dev-builder.slice"
        assert options["user"] == "1000:1000"
        assert options["network"] == network.name
        assert options["read_only"] is True
        assert options["detach"] is True
        assert options["nano_cpus"] in {1_000_000_000, 3_000_000_000}
        assert options["mem_limit"] == 16 * 1024 * 1024 * 1024
        assert options["memswap_limit"] == 16 * 1024 * 1024 * 1024
        assert options["pids_limit"] > 0
        assert options["restart_policy"] == {"Name": "no"}
        assert options["volumes"] == {}
        assert options["devices"] == []
        assert "ports" not in options
        assert options["labels"]["loom.personal-dev-native-builder.grant-id"] == str(grant.grant_id)
        assert options["labels"]["loom.personal-dev-native-builder.attempt-id"] == str(
            grant.attempt_id
        )
        assert options["labels"]["loom.personal-dev-native-builder.lease-epoch"] == str(
            grant.attempt_lease_epoch
        )
        assert options["labels"]["loom.personal-dev-native-builder.contract-sha256"] == (
            grant.contract_sha256
        )

    assert buildkit["entrypoint"] == ["/usr/local/bin/loom-personal-dev-buildkitd"]
    assert buildkit["command"] == ["--native-tcp-buildkit-child"]
    assert buildkit["cap_drop"] == ["ALL"]
    assert buildkit["cap_add"] == ["SETUID", "SETGID"]
    assert buildkit["security_opt"] == ["seccomp=unconfined"]
    assert buildkit["nano_cpus"] == 3_000_000_000
    assert "no-new-privileges" not in " ".join(buildkit["security_opt"])
    assert buildkit["labels"]["loom.personal-dev-native-builder.role"] == "buildkit"

    buildkit_host = f"buildkit-{grant.grant_id.hex[:12]}"
    assert restricted["entrypoint"] == [
        "python3",
        "-m",
        "loom.personal_dev_sandbox_builder",
    ]
    assert restricted["command"][-2:] == [
        "--native-buildkit-address",
        f"tcp://{buildkit_host}:1234",
    ]
    assert restricted["cap_drop"] == ["ALL"]
    assert restricted["cap_add"] == []
    assert restricted["security_opt"] == ["no-new-privileges:true"]
    assert restricted["healthcheck"] == {"test": ["NONE"]}
    assert restricted["nano_cpus"] == 1_000_000_000
    assert restricted["labels"]["loom.personal-dev-native-builder.role"] == "client"
    assert buildkit["hostname"] == buildkit_host
    assert buildkit["networking_config"][network.name]["Aliases"] == [buildkit_host]
    assert restricted["networking_config"][network.name]["Aliases"] == [client_name]

    assert buildkit_container.archives == []
    assert len(restricted_container.archives) == 1
    archive_path, archive = restricted_container.archives[0]
    assert archive_path == "/opt/loom-personal-dev-builder"
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as package:
        members = {member.name: member for member in package.getmembers()}
        assert set(members) == {
            "native-input",
            "native-input/capabilities",
            "native-input/capabilities/artifact-upload.json",
            "native-input/capabilities/source-get-url",
            "native-input/contract.json",
        }
        for member in members.values():
            assert (member.uid, member.gid, member.mtime) == (1000, 1000, 0)
            assert member.mode == (0o500 if member.isdir() else 0o400)
        contract = package.extractfile(members["native-input/contract.json"])
        source = package.extractfile(members["native-input/capabilities/source-get-url"])
        upload = package.extractfile(members["native-input/capabilities/artifact-upload.json"])
        assert contract is not None and contract.read() == grant.contract_json.encode("ascii")
        assert source is not None and source.read() == grant.source_get_url.encode("utf-8")
        assert upload is not None
        assert json.loads(upload.read()) == {
            "fields": dict(grant.artifact_upload_fields),
            "max_bytes": grant.artifact_max_bytes,
            "url": grant.artifact_upload_url,
        }

    assert client.events.index(f"start:{buildkit_name}") < client.events.index(
        f"start:{client_name}"
    )
    assert client.events.index(f"archive:{client_name}") < client.events.index(
        f"start:{buildkit_name}"
    )


async def test_docker_runtime_assigns_two_exact_isolated_network_slots() -> None:
    first = _grant()
    second = replace(
        first,
        grant_id=UUID("00000000-0000-0000-0000-000000000013"),
        candidate_id=UUID("00000000-0000-0000-0000-000000000014"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000015"),
    )
    client = _FakeDockerClient(first.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )

    await runtime.start(first)
    await runtime.start(second)

    assert [options["ipam"] for options, _network in client.networks.created] == [
        {
            "Driver": "default",
            "Config": [
                {"Subnet": "172.28.0.0/24", "Gateway": "172.28.0.1"},
            ],
        },
        {
            "Driver": "default",
            "Config": [
                {"Subnet": "172.28.1.0/24", "Gateway": "172.28.1.1"},
            ],
        },
    ]


async def test_docker_runtime_inventories_and_resumes_exact_running_grant() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    first = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await first.start(grant)
    create_events = tuple(event for event in client.events if event.startswith("create:"))
    restarted = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )

    inventory = await restarted.inventory()
    await restarted.start(grant)

    assert inventory.managed_grant_ids == (grant.grant_id,)
    assert inventory.active_grant_ids == (grant.grant_id,)
    assert inventory.available is True
    assert inventory.unavailable_reason is None
    assert len(inventory.readiness_evidence_sha256) == 64
    assert tuple(event for event in client.events if event.startswith("create:")) == create_events


async def test_docker_runtime_emits_success_evidence_then_cleans_exact_resources() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    resources = {
        options["name"]: container for _image, options, container in client.containers.created
    }
    client_name = f"loom-pdev-{grant.grant_id.hex[:12]}-client"
    resources[client_name].attrs["State"].update(
        {"Status": "exited", "Running": False, "ExitCode": 0}
    )

    observation = await runtime.observe(grant)

    assert isinstance(observation, NativeBuilderRuntimeObservation)
    assert observation.state == "succeeded"
    assert observation.failure_reason is None
    assert observation.evidence is not None
    assert observation.evidence.grant_id == grant.grant_id
    assert observation.evidence.client_exit_code == 0
    assert observation.evidence.buildkit_running is True
    assert observation.evidence.runtime_name == "runsc-personal-dev-native"

    await runtime.cancel(grant.grant_id)
    await runtime.cleanup(grant.grant_id)
    buildkit_name = f"loom-pdev-{grant.grant_id.hex[:12]}-buildkit"
    network_name = f"loom-pdev-{grant.grant_id.hex[:12]}"
    assert client.events.index(f"remove:{client_name}:True:True") < client.events.index(
        f"remove:{buildkit_name}:True:True"
    )
    assert client.events.index(f"remove:{buildkit_name}:True:True") < client.events.index(
        f"remove:{network_name}"
    )


async def test_docker_runtime_rejects_unhealthy_buildkit_as_success() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    resources = {
        options["name"]: container for _image, options, container in client.containers.created
    }
    client_name = f"loom-pdev-{grant.grant_id.hex[:12]}-client"
    buildkit_name = f"loom-pdev-{grant.grant_id.hex[:12]}-buildkit"
    resources[client_name].attrs["State"].update(
        {"Status": "exited", "Running": False, "ExitCode": 0}
    )
    resources[buildkit_name].attrs["State"]["Health"]["Status"] = "unhealthy"

    observation = await runtime.observe(grant)

    assert observation == NativeBuilderRuntimeObservation(
        state="failed",
        failure_reason="buildkit_unhealthy",
        evidence=None,
    )


@pytest.mark.parametrize(
    ("client_update", "buildkit_update", "reason"),
    [
        ({"Status": "exited", "Running": False, "ExitCode": 7}, {}, "client_exit_nonzero"),
        (
            {"Status": "exited", "Running": False, "ExitCode": 137, "OOMKilled": True},
            {},
            "client_oom_killed",
        ),
        (
            {"Status": "running", "Running": True},
            {"Status": "exited", "Running": False, "ExitCode": 1},
            "buildkit_exit_nonzero",
        ),
    ],
)
async def test_docker_runtime_reports_bounded_terminal_failure(
    client_update: dict[str, object],
    buildkit_update: dict[str, object],
    reason: str,
) -> None:
    grant = _grant()
    docker_client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=docker_client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    resources = {
        options["name"]: container
        for _image, options, container in docker_client.containers.created
    }
    resources[f"loom-pdev-{grant.grant_id.hex[:12]}-client"].attrs["State"].update(client_update)
    resources[f"loom-pdev-{grant.grant_id.hex[:12]}-buildkit"].attrs["State"].update(
        buildkit_update
    )

    observation = await runtime.observe(grant)

    assert observation == NativeBuilderRuntimeObservation(
        state="failed",
        failure_reason=reason,
        evidence=None,
    )


async def test_docker_runtime_fails_closed_on_duplicate_managed_role() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    _image, options, _container = client.containers.created[0]
    duplicate = _FakeContainer(
        "duplicate-buildkit", {"image": grant.builder_image, **options}, client.events
    )
    client.containers.created.append((grant.builder_image, options, duplicate))

    inventory = await runtime.inventory()

    assert inventory.available is False
    assert inventory.unavailable_reason == "managed_resource_shape_drift"
    assert inventory.active_grant_ids == ()
    with pytest.raises(RuntimeError, match="shape drift"):
        await runtime.cleanup(grant.grant_id)


async def test_docker_runtime_revalidates_labels_immediately_before_cleanup() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    _image, _options, restricted = client.containers.created[1]
    restricted.attrs["Config"]["Labels"]["loom.personal-dev-native-builder.attempt-id"] = (
        "00000000-0000-0000-0000-000000000099"
    )

    with pytest.raises(RuntimeError, match="shape drift"):
        await runtime.cleanup(grant.grant_id)

    assert not any(event.startswith("remove:") for event in client.events)


@pytest.mark.parametrize(
    "mutation",
    [
        "outside-provider-pool",
        "unsupported-provider-subnet",
        "nonempty-ip-range",
    ],
)
async def test_docker_runtime_rejects_unsafe_managed_network_ipam(
    mutation: str,
) -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    _options, network = client.networks.created[0]
    if mutation == "outside-provider-pool":
        network.attrs["IPAM"]["Config"] = [
            {"Subnet": "10.0.0.0/24", "IPRange": "", "Gateway": "10.0.0.1"}
        ]
    elif mutation == "unsupported-provider-subnet":
        network.attrs["IPAM"]["Config"] = [
            {
                "Subnet": "172.28.2.0/24",
                "IPRange": "",
                "Gateway": "172.28.2.1",
            }
        ]
    else:
        network.attrs["IPAM"]["Config"][0]["IPRange"] = "172.28.0.128/25"

    inventory = await runtime.inventory()

    assert inventory.available is False
    assert inventory.unavailable_reason == "managed_resource_shape_drift"
    with pytest.raises(RuntimeError, match="shape drift"):
        await runtime.cleanup(grant.grant_id)
    assert not any(event == f"remove:{network.name}" for event in client.events)


@pytest.mark.parametrize(
    "mutation",
    ["tmpfs", "cgroup-parent", "config", "networks", "alias", "privileged"],
)
async def test_docker_runtime_detects_security_and_tmpfs_shape_drift(
    mutation: str,
) -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    runtime = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await runtime.start(grant)
    _image, _options, buildkit = client.containers.created[0]
    if mutation == "tmpfs":
        buildkit.attrs["HostConfig"]["Tmpfs"]["/var/lib/loom-buildkit"] = "rw,size=64g"
    elif mutation == "cgroup-parent":
        buildkit.attrs["HostConfig"]["CgroupParent"] = "system.slice"
    elif mutation == "config":
        buildkit.attrs["Config"] = None
    elif mutation == "networks":
        buildkit.attrs["NetworkSettings"]["Networks"] = []
    elif mutation == "alias":
        buildkit.attrs["NetworkSettings"]["Networks"][f"loom-pdev-{grant.grant_id.hex[:12]}"][
            "Aliases"
        ] = ["wrong-name"]
    else:
        buildkit.attrs["HostConfig"]["Privileged"] = True

    restarted = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    inventory = await restarted.inventory()

    assert inventory.available is False
    assert inventory.unavailable_reason == "managed_resource_shape_drift"


async def test_docker_runtime_restages_fresh_capabilities_before_created_resume() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    first = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await first.start(grant)
    for _image, _options, container in client.containers.created:
        container.attrs["State"].update({"Status": "created", "Running": False})
    _image, _options, restricted = client.containers.created[1]
    refreshed = replace(
        grant,
        source_get_url="https://objects.example/personal-dev/source?token=fresh",
    )
    restarted = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await restarted.inventory()

    await restarted.start(refreshed)

    assert len(restricted.archives) == 2
    _path, archive = restricted.archives[-1]
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as package:
        source = package.extractfile("native-input/capabilities/source-get-url")
        assert source is not None
        assert source.read() == refreshed.source_get_url.encode("utf-8")


async def test_docker_runtime_recovers_exact_partial_cleanup_after_restart() -> None:
    grant = _grant()
    client = _FakeDockerClient(grant.builder_image)
    first = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )
    await first.start(grant)
    client.containers.created = [
        item for item in client.containers.created if item[1]["name"].endswith("buildkit")
    ]
    restarted = DockerPersonalDevNativeBuildRuntime(
        client=client,
        socket_path="/run/loom-personal-dev-builder/docker.sock",
        identity=_identity(),
        health_timeout_seconds=10,
        health_poll_seconds=0.01,
    )

    inventory = await restarted.inventory()
    assert inventory.available is False
    assert inventory.managed_grant_ids == (grant.grant_id,)

    await restarted.cleanup(grant.grant_id)

    buildkit_name = f"loom-pdev-{grant.grant_id.hex[:12]}-buildkit"
    network_name = f"loom-pdev-{grant.grant_id.hex[:12]}"
    assert f"remove:{buildkit_name}:True:True" in client.events
    assert f"remove:{network_name}" in client.events
