from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from loom.dev_instance import derive_identity
from loom.dev_instance_runtime import (
    CommandResult,
    DevInstanceRuntimeError,
    KubectlClient,
    observe_personal_dev_candidate_generation,
)
from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationIntent,
    PersonalDevActivationIntentRequest,
    PersonalDevActivationSigner,
    PersonalDevActivationVerifier,
)
from loom.personal_dev_activation_agent import (
    HttpPersonalDevActivationAuthority,
    KubectlPersonalDevActivationExecutor,
    PersonalDevActivationAgent,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom.personal_dev_reconciler import personal_dev_intent_readiness_sha256

_NOW = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)


def _intent() -> PersonalDevActivationIntent:
    return PersonalDevActivationIntent(
        environment_name="alice",
        subject_id=UUID("00000000-0000-0000-0000-000000000001"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000002"),
        operation_id=UUID("00000000-0000-0000-0000-000000000003"),
        operation_epoch=5,
        attempt_id=UUID("00000000-0000-0000-0000-000000000004"),
        attempt_sequence=2,
        candidate_id=UUID("00000000-0000-0000-0000-000000000005"),
        candidate_sha="a" * 64,
        candidate_publication_sha256="b" * 64,
        deployment_generation=8,
        readiness_evidence_sha256="c" * 64,
        min_slots=0,
        max_slots=2,
        images={
            component: f"registry.test/loom-{component}@sha256:{str(index % 10) * 64}"
            for index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1)
        },
        intent_created_at=_NOW,
    )


class _Source:
    def __init__(self, intent: PersonalDevActivationIntent | None) -> None:
        self.intent = intent
        self.current = True
        self.requests: list[tuple[PersonalDevActivationIntentRequest, str]] = []

    async def next_intent(
        self,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> PersonalDevActivationIntent | None:
        self.requests.append((request, signature))
        return self.intent

    async def assert_current(
        self,
        intent: PersonalDevActivationIntent,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> bool:
        self.requests.append((request, signature))
        return self.current and intent == self.intent


class _Executor:
    def __init__(self) -> None:
        self.activated: list[PersonalDevActivationIntent] = []

    async def activate(self, intent: PersonalDevActivationIntent) -> str:
        self.activated.append(intent)
        return "d" * 64


class _Publisher:
    def __init__(self) -> None:
        self.published: list[tuple[PersonalDevActivationAcknowledgement, str]] = []

    async def publish(
        self,
        acknowledgement: PersonalDevActivationAcknowledgement,
        *,
        signature: str,
    ) -> None:
        self.published.append((acknowledgement, signature))


class _CohortSource(_Source):
    def __init__(self, intents: list[PersonalDevActivationIntent]) -> None:
        super().__init__(None)
        self.intents = intents

    async def next_intent(
        self,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> PersonalDevActivationIntent | None:
        self.requests.append((request, signature))
        return next(
            (
                intent
                for intent in self.intents
                if intent.operation_id not in request.exclude_operation_ids
            ),
            None,
        )

    async def assert_current(
        self,
        intent: PersonalDevActivationIntent,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> bool:
        self.requests.append((request, signature))
        return intent in self.intents and request.operation_id == intent.operation_id


class _SelectiveExecutor(_Executor):
    def __init__(self, failing_operation_id: UUID) -> None:
        super().__init__()
        self.failing_operation_id = failing_operation_id

    async def activate(self, intent: PersonalDevActivationIntent) -> str:
        self.activated.append(intent)
        if intent.operation_id == self.failing_operation_id:
            raise DevInstanceRuntimeError("one environment is broken")
        return "d" * 64


@pytest.mark.asyncio
async def test_activation_agent_polls_rechecks_activates_and_signs_exact_intent() -> None:
    key = bytes(range(32))
    signer = PersonalDevActivationSigner(keys={"personal-dev-agent-v1": key})
    source = _Source(_intent())
    executor = _Executor()
    publisher = _Publisher()
    agent = PersonalDevActivationAgent(
        source=source,
        executor=executor,
        publisher=publisher,
        signer=signer,
        agent_key_id="personal-dev-agent-v1",
    )

    progressed = await agent.reconcile_once(now=_NOW)

    assert progressed is True
    assert executor.activated == [_intent()]
    assert len(source.requests) == 2
    poll, poll_signature = source.requests[0]
    public_key = signer.public_key_bytes("personal-dev-agent-v1")
    verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": public_key},
        max_age_seconds=300,
    )
    verifier.verify_intent_request(poll, signature=poll_signature, now=_NOW)
    acknowledgement, signature = publisher.published[0]
    verified = verifier.verify(acknowledgement, signature=signature, now=_NOW)
    assert verified.acknowledgement.local_activation_sha256 == "d" * 64
    assert verified.acknowledgement.operation_id == _intent().operation_id


@pytest.mark.asyncio
async def test_activation_agent_stops_before_mutation_when_intent_is_superseded() -> None:
    source = _Source(_intent())
    source.current = False
    executor = _Executor()
    publisher = _Publisher()
    agent = PersonalDevActivationAgent(
        source=source,
        executor=executor,
        publisher=publisher,
        signer=PersonalDevActivationSigner(keys={"personal-dev-agent-v1": bytes(range(32))}),
        agent_key_id="personal-dev-agent-v1",
    )

    assert await agent.reconcile_once(now=_NOW) is False
    assert executor.activated == []
    assert publisher.published == []


@pytest.mark.asyncio
async def test_activation_agent_is_idle_without_an_intent() -> None:
    source = _Source(None)
    executor = _Executor()
    publisher = _Publisher()
    agent = PersonalDevActivationAgent(
        source=source,
        executor=executor,
        publisher=publisher,
        signer=PersonalDevActivationSigner(keys={"personal-dev-agent-v1": bytes(range(32))}),
        agent_key_id="personal-dev-agent-v1",
    )

    assert await agent.reconcile_once(now=_NOW) is False
    assert executor.activated == []
    assert publisher.published == []


@pytest.mark.asyncio
async def test_activation_agent_skips_one_failure_without_blocking_other_environments() -> None:
    first = _intent()
    second = replace(
        first,
        environment_name="bob",
        operation_id=UUID("00000000-0000-0000-0000-000000000013"),
        intent_created_at=_NOW + timedelta(seconds=1),
    )
    source = _CohortSource([first, second])
    executor = _SelectiveExecutor(first.operation_id)
    publisher = _Publisher()
    agent = PersonalDevActivationAgent(
        source=source,
        executor=executor,
        publisher=publisher,
        signer=PersonalDevActivationSigner(keys={"personal-dev-agent-v1": bytes(range(32))}),
        agent_key_id="personal-dev-agent-v1",
    )

    with pytest.raises(DevInstanceRuntimeError, match="one environment"):
        await agent.reconcile_once(now=_NOW)
    assert await agent.reconcile_once(now=_NOW + timedelta(seconds=1)) is True

    assert executor.activated == [first, second]
    assert source.requests[2][0].exclude_operation_ids == (first.operation_id,)
    assert publisher.published[0][0].operation_id == second.operation_id


@pytest.mark.asyncio
async def test_http_activation_authority_rejects_response_digest_drift_and_publishes() -> None:
    intent = _intent()
    response_payload = {
        "environment_name": intent.environment_name,
        "subject_id": str(intent.subject_id),
        "subject_incarnation": str(intent.subject_incarnation),
        "operation_id": str(intent.operation_id),
        "operation_epoch": intent.operation_epoch,
        "attempt_id": str(intent.attempt_id),
        "attempt_sequence": intent.attempt_sequence,
        "candidate_id": str(intent.candidate_id),
        "candidate_sha": intent.candidate_sha,
        "candidate_publication_sha256": intent.candidate_publication_sha256,
        "deployment_generation": intent.deployment_generation,
        "readiness_evidence_sha256": intent.readiness_evidence_sha256,
        "min_slots": intent.min_slots,
        "max_slots": intent.max_slots,
        "images": dict(intent.images),
        "intent_created_at": intent.intent_created_at.isoformat(),
        "intent_sha256": intent.intent_sha256,
    }
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/activation-intents/next"):
            return httpx.Response(200, json=response_payload)
        return httpx.Response(200, json={"operation": {"state": "activating"}})

    async with httpx.AsyncClient(
        base_url="https://management.example",
        transport=httpx.MockTransport(handler),
        trust_env=False,
    ) as client:
        authority = HttpPersonalDevActivationAuthority(client)
        poll = PersonalDevActivationIntentRequest(
            agent_key_id="personal-dev-agent-v1",
            request_nonce=UUID("00000000-0000-0000-0000-000000000099"),
            requested_at=_NOW,
        )
        loaded = await authority.next_intent(poll, signature="1" * 128)
        assert loaded == intent
        acknowledgement = _ack_from_intent(intent)
        await authority.publish(acknowledgement, signature="2" * 128)

    assert requests[0].headers["X-Loom-Activation-Signature"] == "1" * 128
    assert requests[1].headers["X-Loom-Activation-Signature"] == "2" * 128


def _ack_from_intent(
    intent: PersonalDevActivationIntent,
) -> PersonalDevActivationAcknowledgement:
    return PersonalDevActivationAcknowledgement(
        environment_name=intent.environment_name,
        subject_id=intent.subject_id,
        subject_incarnation=intent.subject_incarnation,
        operation_id=intent.operation_id,
        operation_epoch=intent.operation_epoch,
        attempt_id=intent.attempt_id,
        candidate_id=intent.candidate_id,
        candidate_sha=intent.candidate_sha,
        deployment_generation=intent.deployment_generation,
        readiness_evidence_sha256=intent.readiness_evidence_sha256,
        local_activation_sha256="d" * 64,
        agent_key_id="personal-dev-agent-v1",
        observed_at=_NOW,
    )


class _ActivationKubectlRunner:
    def __init__(self, intent: PersonalDevActivationIntent) -> None:
        self.intent = intent
        self.resources: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[tuple[list[str], str | None]] = []
        self.route_annotation_drift = False
        labels = {
            "loom.dev/subject": str(intent.subject_id),
            "loom.dev/incarnation": str(intent.subject_incarnation),
            "loom.dev/operation": str(intent.operation_id),
            "loom.dev/attempt": str(intent.attempt_id),
            "loom.dev/operation-epoch": str(intent.operation_epoch),
            "loom.dev/generation": str(intent.deployment_generation),
        }
        for component in ("control-plane", "llm-gateway", "service", "web"):
            env = []
            if component == "control-plane":
                env.append(
                    {"name": "LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED", "value": "false"},
                )
            if component == "service":
                env.append({"name": "LOOM_SVC_K8S_WORKER_ENABLED", "value": "false"})
            name = f"loom-{component}-g{intent.deployment_generation}"
            self.resources[("deployment", name)] = {
                "metadata": {"uid": f"uid-{name}", "generation": 1, "labels": labels},
                "spec": {
                    "replicas": 1,
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "containers": [{"image": intent.images[component], "env": env}],
                        },
                    },
                },
                "status": {
                    "observedGeneration": 1,
                    "availableReplicas": 1,
                    "updatedReplicas": 1,
                },
            }

    async def run(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> CommandResult:
        del timeout_seconds
        self.calls.append((argv, stdin))
        if "apply" in argv:
            assert stdin is not None
            for document in yaml.safe_load_all(stdin):
                kind = str(document["kind"]).lower()
                name = str(document["metadata"]["name"])
                self.resources[(kind, name)] = {
                    "metadata": {
                        **document["metadata"],
                        "uid": f"uid-{kind}-{name}",
                    },
                    "spec": document["spec"],
                }
                if kind == "ingress" and self.route_annotation_drift:
                    self.resources[(kind, name)]["metadata"]["annotations"][
                        "nginx.ingress.kubernetes.io/proxy-read-timeout"
                    ] = "/tampered"
            return CommandResult(stdout="", stderr="")
        if "get" in argv:
            index = argv.index("get")
            return CommandResult(
                stdout=json.dumps(self.resources[(argv[index + 1], argv[index + 2])]),
                stderr="",
            )
        return CommandResult(stdout="", stderr="")


@pytest.mark.asyncio
async def test_kubectl_activation_reobserves_readiness_and_attests_disabled_capacity() -> None:
    initial = _intent()
    runner = _ActivationKubectlRunner(initial)
    kubectl = KubectlClient(
        "kubectl",
        field_manager="loom-personal-dev-activation-agent",
        runner=runner,
    )
    executor = KubectlPersonalDevActivationExecutor(
        kubectl=kubectl,
        minio_endpoint="https://minio.example",
    )
    observation = await observe_personal_dev_candidate_generation(
        kubectl,
        derive_identity("alice"),
        executor._config(initial),
    )
    intent = replace(
        initial,
        readiness_evidence_sha256=personal_dev_intent_readiness_sha256(
            initial,
            observation,
        ),
    )
    runner.intent = intent

    digest = await executor.activate(intent)

    assert len(digest) == 64
    applied = [stdin for argv, stdin in runner.calls if "apply" in argv]
    assert len(applied) == 1
    assert applied[0] is not None and "kind: Ingress" in applied[0]
    assert "name: loom-service\n" in applied[0]

    runner.route_annotation_drift = True
    with pytest.raises(DevInstanceRuntimeError, match="route binding"):
        await executor.activate(intent)

    service = runner.resources[("deployment", "loom-service-g8")]
    service["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "true"
    with pytest.raises(DevInstanceRuntimeError, match="readiness changed"):
        await executor.activate(intent)
