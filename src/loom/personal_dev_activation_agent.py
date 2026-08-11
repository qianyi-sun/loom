"""Independent two-phase activator for personal development environments."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

import httpx
import yaml  # type: ignore[import-untyped]

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    PersonalDevManifestBinding,
    personal_dev_activation_manifest_documents,
)
from loom.dev_instance_runtime import (
    DevInstanceRuntimeError,
    KubectlClient,
    observe_personal_dev_candidate_generation,
)
from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationIntent,
    PersonalDevActivationIntentRequest,
    PersonalDevActivationSigner,
)
from loom.personal_dev_reconciler import personal_dev_intent_readiness_sha256


class PersonalDevActivationIntentSource(Protocol):
    async def next_intent(
        self,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> PersonalDevActivationIntent | None: ...

    async def assert_current(
        self,
        intent: PersonalDevActivationIntent,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> bool: ...


class PersonalDevActivationExecutor(Protocol):
    async def activate(self, intent: PersonalDevActivationIntent) -> str: ...


class PersonalDevActivationPublisher(Protocol):
    async def publish(
        self,
        acknowledgement: PersonalDevActivationAcknowledgement,
        *,
        signature: str,
    ) -> None: ...


@dataclass(slots=True)
class PersonalDevActivationAgent:
    """Converge exactly one current intent without holding central write authority."""

    source: PersonalDevActivationIntentSource
    executor: PersonalDevActivationExecutor
    publisher: PersonalDevActivationPublisher
    signer: PersonalDevActivationSigner
    agent_key_id: str
    _temporarily_skipped: set[UUID] = field(default_factory=set, init=False, repr=False)

    async def reconcile_once(self, *, now: datetime) -> bool:
        request = PersonalDevActivationIntentRequest(
            agent_key_id=self.agent_key_id,
            request_nonce=uuid4(),
            requested_at=now,
            exclude_operation_ids=tuple(sorted(self._temporarily_skipped, key=str)),
        )
        intent = await self.source.next_intent(
            request,
            signature=self.signer.sign_intent_request(request),
        )
        if intent is None:
            self._temporarily_skipped.clear()
            return False
        # Re-enter the authority immediately before the first local mutation.
        # This is intentionally a second round trip, not a cache check.
        recheck = PersonalDevActivationIntentRequest(
            agent_key_id=self.agent_key_id,
            request_nonce=uuid4(),
            requested_at=now,
            operation_id=intent.operation_id,
        )
        if not await self.source.assert_current(
            intent,
            recheck,
            signature=self.signer.sign_intent_request(recheck),
        ):
            return False
        try:
            local_activation_sha256 = await self.executor.activate(intent)
            acknowledgement = PersonalDevActivationAcknowledgement(
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
                local_activation_sha256=local_activation_sha256,
                agent_key_id=self.agent_key_id,
                observed_at=now,
            )
            await self.publisher.publish(
                acknowledgement,
                signature=self.signer.sign(acknowledgement),
            )
        except Exception:
            if len(self._temporarily_skipped) < 16:
                self._temporarily_skipped.add(intent.operation_id)
            raise
        self._temporarily_skipped.discard(intent.operation_id)
        return True


@dataclass(slots=True)
class HttpPersonalDevActivationAuthority:
    """Strict HTTPS adapter for intent reads and signed acknowledgements."""

    client: httpx.AsyncClient

    @staticmethod
    def _poll_json(request: PersonalDevActivationIntentRequest) -> dict[str, object]:
        value: dict[str, object] = {
            "agent_key_id": request.agent_key_id,
            "exclude_operation_ids": [
                str(operation_id) for operation_id in request.exclude_operation_ids
            ],
            "operation_id": (
                str(request.operation_id) if request.operation_id is not None else None
            ),
            "request_nonce": str(request.request_nonce),
            "requested_at": request.requested_at.astimezone(UTC).isoformat(),
        }
        return value

    async def next_intent(
        self,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> PersonalDevActivationIntent | None:
        response = await self.client.post(
            "/api/v1/internal/personal-dev/activation-intents/next",
            json=self._poll_json(request),
            headers={"X-Loom-Activation-Signature": signature},
        )
        if response.status_code == 204:
            return None
        if response.status_code != 200:
            raise RuntimeError("personal-dev activation intent read failed")
        try:
            value = response.json()
            expected_fields = {
                "environment_name",
                "subject_id",
                "subject_incarnation",
                "operation_id",
                "operation_epoch",
                "attempt_id",
                "attempt_sequence",
                "candidate_id",
                "candidate_sha",
                "candidate_publication_sha256",
                "deployment_generation",
                "readiness_evidence_sha256",
                "min_slots",
                "max_slots",
                "images",
                "intent_created_at",
                "intent_sha256",
            }
            if not isinstance(value, dict) or set(value) != expected_fields:
                raise ValueError
            supplied_digest = value.pop("intent_sha256")
            for field in (
                "subject_id",
                "subject_incarnation",
                "operation_id",
                "attempt_id",
                "candidate_id",
            ):
                value[field] = UUID(str(value[field]))
            created_at = value["intent_created_at"]
            if not isinstance(created_at, str):
                raise ValueError
            value["intent_created_at"] = datetime.fromisoformat(
                created_at.replace("Z", "+00:00"),
            )
            intent = PersonalDevActivationIntent(**value)
            if supplied_digest != intent.intent_sha256:
                raise ValueError
        except (TypeError, ValueError, KeyError):
            raise RuntimeError("personal-dev activation intent response is invalid") from None
        return intent

    async def assert_current(
        self,
        intent: PersonalDevActivationIntent,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
    ) -> bool:
        current = await self.next_intent(request, signature=signature)
        return current is not None and current.intent_sha256 == intent.intent_sha256

    async def publish(
        self,
        acknowledgement: PersonalDevActivationAcknowledgement,
        *,
        signature: str,
    ) -> None:
        value = json.loads(acknowledgement.canonical_bytes())
        value.pop("schema_version")
        response = await self.client.post(
            "/api/v1/internal/personal-dev/activation-acknowledgements",
            json=value,
            headers={"X-Loom-Activation-Signature": signature},
        )
        if response.status_code not in {200, 202}:
            raise RuntimeError("personal-dev activation acknowledgement failed")


@dataclass(slots=True)
class KubectlPersonalDevActivationExecutor:
    """Agent-owned stable route cutover with exact fail-closed capacity evidence."""

    kubectl: KubectlClient
    minio_endpoint: str
    minio_region: str = "us-east-1"
    ingress_class_name: str = "nginx"
    ingress_cert_manager_cluster_issuer: str = "letsencrypt-prod"
    image_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        if self.kubectl.field_manager != "loom-personal-dev-activation-agent":
            raise ValueError("activation executor requires its dedicated field manager")

    def _config(self, intent: PersonalDevActivationIntent) -> DevInstanceManifestConfig:
        return DevInstanceManifestConfig(
            image_tag="",
            candidate_sha=intent.candidate_sha,
            deployment_generation=intent.deployment_generation,
            container_registry="",
            minio_endpoint=self.minio_endpoint,
            image_references=intent.images,
            lifecycle_binding=PersonalDevManifestBinding(
                subject_id=intent.subject_id,
                subject_incarnation=intent.subject_incarnation,
                operation_id=intent.operation_id,
                attempt_id=intent.attempt_id,
                operation_epoch=intent.operation_epoch,
            ),
            minio_region=self.minio_region,
            ingress_class_name=self.ingress_class_name,
            ingress_cert_manager_cluster_issuer=self.ingress_cert_manager_cluster_issuer,
            image_pull_policy=self.image_pull_policy,
        )

    @staticmethod
    def _env_values(resource: dict[str, object]) -> dict[str, str]:
        try:
            containers = resource["spec"]["template"]["spec"]["containers"]  # type: ignore[index]
            env = containers[0]["env"]
            if len(containers) != 1 or not isinstance(env, list):
                raise TypeError
            values = {
                item["name"]: item["value"]
                for item in env
                if isinstance(item, dict)
                and isinstance(item.get("name"), str)
                and isinstance(item.get("value"), str)
            }
        except (KeyError, IndexError, TypeError):
            raise DevInstanceRuntimeError("candidate capacity fence response was invalid") from None
        return values

    async def activate(self, intent: PersonalDevActivationIntent) -> str:
        identity = derive_identity(intent.environment_name)
        config = self._config(intent)
        observation = await observe_personal_dev_candidate_generation(
            self.kubectl,
            identity,
            config,
        )
        if personal_dev_intent_readiness_sha256(intent, observation) != (
            intent.readiness_evidence_sha256
        ):
            raise DevInstanceRuntimeError("candidate readiness changed after central intent")

        disabled_evidence: dict[str, str] = {}
        disabled_requirements = {
            "control-plane": "LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED",
            "service": "LOOM_SVC_K8S_WORKER_ENABLED",
        }
        for component, variable in disabled_requirements.items():
            resource = await self.kubectl.read_resource_json(
                namespace=identity.namespace,
                kind="deployment",
                name=f"loom-{component}-g{intent.deployment_generation}",
            )
            value = self._env_values(resource).get(variable)
            if value != "false":
                raise DevInstanceRuntimeError("candidate legacy capacity path is not disabled")
            disabled_evidence[variable] = value

        documents = personal_dev_activation_manifest_documents(identity, config)
        await self.kubectl.apply(
            yaml.safe_dump_all(documents, sort_keys=False, explicit_start=True),
        )
        observed_routes: list[dict[str, object]] = []
        for desired in documents:
            kind = str(desired["kind"])
            name = str(desired["metadata"]["name"])
            resource = await self.kubectl.read_resource_json(
                namespace=identity.namespace,
                kind=kind.lower(),
                name=name,
            )
            try:
                metadata = resource["metadata"]
                spec = resource["spec"]
                labels = metadata["labels"]
                annotations = metadata.get("annotations", {})
                uid = str(metadata["uid"])
            except (KeyError, TypeError):
                raise DevInstanceRuntimeError("stable route response was invalid") from None
            desired_labels = desired["metadata"]["labels"]
            desired_annotations = desired["metadata"].get("annotations", {})
            if (
                not uid
                or not isinstance(labels, dict)
                or not isinstance(annotations, dict)
                or not isinstance(desired_annotations, dict)
                or any(labels.get(key) != value for key, value in desired_labels.items())
                or any(
                    annotations.get(key) != value
                    for key, value in desired_annotations.items()
                )
            ):
                raise DevInstanceRuntimeError("stable route binding did not converge")
            desired_spec = desired["spec"]
            if kind == "Service":
                raw_ports = spec.get("ports")
                desired_ports = desired_spec.get("ports")
                if (
                    not isinstance(raw_ports, list)
                    or not isinstance(desired_ports, list)
                    or any(not isinstance(port, dict) for port in raw_ports)
                    or any(not isinstance(port, dict) for port in desired_ports)
                ):
                    raise DevInstanceRuntimeError("stable route spec did not converge")
                comparable_ports = [
                    {
                        "port": port.get("port"),
                        "protocol": port.get("protocol", "TCP"),
                        "targetPort": port.get("targetPort"),
                    }
                    for port in raw_ports
                    if isinstance(port, dict)
                ]
                expected_ports = [
                    {
                        "port": port.get("port"),
                        "protocol": port.get("protocol", "TCP"),
                        "targetPort": port.get("targetPort"),
                    }
                    for port in desired_ports
                    if isinstance(port, dict)
                ]
                comparable = {
                    "ports": comparable_ports,
                    "selector": spec.get("selector"),
                    "type": spec.get("type", "ClusterIP"),
                }
                expected = {
                    "ports": expected_ports,
                    "selector": desired_spec.get("selector"),
                    "type": desired_spec.get("type", "ClusterIP"),
                }
            else:
                comparable = {
                    "ingressClassName": spec.get("ingressClassName"),
                    "rules": spec.get("rules"),
                    "tls": spec.get("tls"),
                }
                expected = {
                    "ingressClassName": desired_spec.get("ingressClassName"),
                    "rules": desired_spec.get("rules"),
                    "tls": desired_spec.get("tls"),
                }
            if comparable != expected:
                raise DevInstanceRuntimeError("stable route spec did not converge")
            observed_routes.append(
                {
                    "annotations": {
                        key: annotations[key]
                        for key in sorted(desired_annotations)
                    },
                    "kind": kind,
                    "labels": {
                        key: labels[key]
                        for key in sorted(desired_labels)
                    },
                    "name": name,
                    "spec": comparable,
                    "uid": uid,
                }
            )
        payload = {
            "capacity_paths": disabled_evidence,
            "intent_sha256": intent.intent_sha256,
            "routes": observed_routes,
            "schema_version": 1,
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        return hashlib.sha256(canonical).hexdigest()


__all__ = [
    "HttpPersonalDevActivationAuthority",
    "KubectlPersonalDevActivationExecutor",
    "PersonalDevActivationAgent",
    "PersonalDevActivationExecutor",
    "PersonalDevActivationIntentSource",
    "PersonalDevActivationPublisher",
]
