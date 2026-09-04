"""Protected convergence for the inert staging capacity-agent."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import yaml  # type: ignore[import-untyped]
from sqlalchemy import URL

from loom_capacity_guard.contracts import canonical_bytes

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_staging_capacity_database_component import (
    build_staging_reporter_configuration,
    staging_database_protected_admission_digest,
)

_NAMESPACE = "loom-staging"
_NAME = "loom-capacity-agent"
_COMPONENT_LABEL = "loom.carin.dev/protected-component"
_COMPONENT_VALUE = "staging-capacity-agent"
_FIELD_MANAGER = "loom-staging-capacity-agent"
_REQUEST_TIMEOUT = "60s"
_QUERY_TIMEOUT_SECONDS = 30.0
_MUTATION_TIMEOUT_SECONDS = 60.0
_ROLLOUT_TIMEOUT_SECONDS = 660.0
_EXPECTED = (
    ("Secret", _NAME),
    ("Deployment", _NAME),
    ("NetworkPolicy", "loom-capacity-agent-egress"),
    ("NetworkPolicy", "loom-capacity-agent-postgres-ingress"),
)


class ProtectedStagingCapacityAgentCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float
    ) -> bytes: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class _Sources:
    manifest: bytes
    digest: str


@dataclass(frozen=True, slots=True)
class _Snapshot:
    state: ComponentState
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityAgentComponent:
    runner: ProtectedStagingCapacityAgentCommandRunner
    container_registry: str
    seed_reader: Callable[[], dict[str, object]]
    reporter_tls_reader: Callable[[], Mapping[str, bytes]]
    postgres_ca_reader: Callable[[], bytes]

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            before_sources = self._sources(plan)
            snapshot = self._snapshot(plan, sources=before_sources)
            if self._sources(plan).digest != before_sources.digest:
                return ComponentState.DRIFTED, _hash_json(
                    {"sources": before_sources.digest, "status": "authority-changed"}
                )
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        return snapshot.state, snapshot.evidence_digest

    def apply(self, plan: FinalGatePlan) -> None:
        before_sources = self._sources(plan)
        before = self._snapshot(plan, sources=before_sources)
        if before.state is not ComponentState.READY:
            raise RuntimeError("protected staging capacity agent state drifted")
        if self._sources(plan).digest != before_sources.digest:
            raise RuntimeError("protected staging capacity agent authority changed before apply")
        documents = _documents(before_sources.manifest)
        foundation = _encode_documents(
            (
                documents[("Secret", _NAME)],
                documents[("NetworkPolicy", "loom-capacity-agent-egress")],
                documents[("NetworkPolicy", "loom-capacity-agent-postgres-ingress")],
            )
        )
        deployment = _encode_documents((documents[("Deployment", _NAME)],))
        self._apply(foundation)
        self._apply(deployment)
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "rollout",
                "status",
                f"deployment/{_NAME}",
                "--timeout=660s",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            input_payload=None,
            timeout_seconds=_ROLLOUT_TIMEOUT_SECONDS,
        )
        after_sources = self._sources(plan)
        if after_sources.digest != before_sources.digest:
            raise RuntimeError("protected staging capacity agent authority changed after apply")
        after = self._snapshot(plan, sources=after_sources)
        if after.state is not ComponentState.EXACT or not self._one_ready_candidate_pod(plan):
            raise RuntimeError("protected staging capacity agent did not converge")
        if self._sources(plan).digest != after_sources.digest:
            raise RuntimeError("protected staging capacity agent authority changed after readback")

    def _snapshot(self, plan: FinalGatePlan, *, sources: _Sources | None = None) -> _Snapshot:
        effective = self._sources(plan) if sources is None else sources
        desired = _documents(effective.manifest)
        inventory = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                "secret,deployments,networkpolicies",
                f"--selector={_COMPONENT_LABEL}={_COMPONENT_VALUE}",
                "--show-managed-fields",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        listed = _listed_resources(inventory)
        listed_identities = [_identity(item) for item in listed]
        if len(listed_identities) != len(set(listed_identities)) or not set(
            listed_identities
        ).issubset(set(_EXPECTED)):
            return _Snapshot(
                ComponentState.DRIFTED,
                _hash_json(
                    {
                        "sources": effective.digest,
                        "inventory": hashlib.sha256(inventory).hexdigest(),
                    }
                ),
            )
        observed: dict[tuple[str, str], dict[str, object] | None] = {}
        for kind, name in _EXPECTED:
            payload = self._get_one(kind, name)
            observed[(kind, name)] = None if not payload else _object(payload, label="resource")
        if any(
            item is not None and (kind, name) not in set(listed_identities)
            for (kind, name), item in observed.items()
        ):
            return _Snapshot(
                ComponentState.DRIFTED,
                _hash_json(
                    {
                        "sources": effective.digest,
                        "inventory": hashlib.sha256(inventory).hexdigest(),
                    }
                ),
            )
        present = {key: item for key, item in observed.items() if item is not None}
        if not present:
            return _Snapshot(
                ComponentState.READY, _hash_json({"sources": effective.digest, "status": "absent"})
            )
        for key, item in present.items():
            assert item is not None
            if not _safe_owned(item, desired[key]):
                return _Snapshot(
                    ComponentState.DRIFTED,
                    _hash_json({"sources": effective.digest, "resource": key, "status": "unsafe"}),
                )
        secret = observed[("Secret", _NAME)]
        if secret is not None:
            secret_status = self._diff(_encode_document(desired[("Secret", _NAME)]))
            if secret_status == 1:
                return _Snapshot(
                    ComponentState.DRIFTED,
                    _hash_json({"sources": effective.digest, "status": "immutable-secret-drift"}),
                )
        mutable_drift = False
        for key in _EXPECTED:
            if key == ("Secret", _NAME):
                continue
            observed_item = observed[key]
            if observed_item is None or self._diff(_encode_document(desired[key])) == 1:
                mutable_drift = True
        state = ComponentState.READY if mutable_drift or secret is None else ComponentState.EXACT
        return _Snapshot(
            state,
            _hash_json(
                {
                    "sources": effective.digest,
                    "inventory": hashlib.sha256(inventory).hexdigest(),
                    "resources": {
                        f"{kind}/{name}": hashlib.sha256(
                            json.dumps(item, sort_keys=True).encode()
                        ).hexdigest()
                        for (kind, name), item in observed.items()
                        if item is not None
                    },
                    "state": state.value,
                }
            ),
        )

    def _sources(self, plan: FinalGatePlan) -> _Sources:
        seed = self.seed_reader()
        tls = self.reporter_tls_reader()
        postgres_ca = self.postgres_ca_reader()
        if set(tls) != {"certificate.pem", "manager-ca.pem", "private-key.pem"} or not postgres_ca:
            raise ValueError("protected staging capacity agent source is invalid")
        if any(not isinstance(value, bytes) or not value for value in tls.values()):
            raise ValueError("protected staging capacity agent source is invalid")
        manifest = self._manifest(plan, seed=seed, tls=tls, postgres_ca=postgres_ca)
        return _Sources(
            manifest=manifest,
            digest=_hash_json(
                {
                    "seed": _hash_json(_nonsecret_seed(seed)),
                    "tls": {key: hashlib.sha256(value).hexdigest() for key, value in tls.items()},
                    "postgres_ca": hashlib.sha256(postgres_ca).hexdigest(),
                    "manifest": hashlib.sha256(manifest).hexdigest(),
                }
            ),
        )

    def _manifest(
        self,
        plan: FinalGatePlan,
        *,
        seed: Mapping[str, object],
        tls: Mapping[str, bytes],
        postgres_ca: bytes,
    ) -> bytes:
        configuration = build_staging_reporter_configuration(
            plan,
            seed,
            protected_admission_sha256=staging_database_protected_admission_digest(plan, seed),
        )
        password = seed.get("agent_database_password")
        token = seed.get("reporter_token")
        if not isinstance(password, str) or not isinstance(token, str):
            raise ValueError("protected staging capacity agent credentials are invalid")
        database_url = (
            URL.create(
                "postgresql+psycopg",
                username="loom_cap_staging_agent",
                password=password,
                host="loom-postgres-rw.loom-staging.svc.cluster.local",
                port=5432,
                database="loom",
                query={"sslmode": "verify-full", "sslrootcert": "/run/loom-postgres-ca/ca.crt"},
            )
            .render_as_string(hide_password=False)
            .encode("ascii")
        )
        labels = {
            "app.kubernetes.io/managed-by": "loom-staging-rollout",
            "app.kubernetes.io/name": _NAME,
            _COMPONENT_LABEL: _COMPONENT_VALUE,
        }
        image = f"{self.container_registry}/loom-control-plane@{plan.image_digests['loom-control-plane']}"
        restricted = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "readOnlyRootFilesystem": True,
            "runAsGroup": 65532,
            "runAsNonRoot": True,
            "runAsUser": 65532,
            "seccompProfile": {"type": "RuntimeDefault"},
        }
        secret: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": _NAME, "namespace": _NAMESPACE, "labels": labels},
            "immutable": True,
            "type": "Opaque",
            "data": {
                "ca.pem": _b64(tls["manager-ca.pem"]),
                "certificate.pem": _b64(tls["certificate.pem"]),
                "database-url": _b64(database_url),
                "private-key.pem": _b64(tls["private-key.pem"]),
                "reporter-configuration.json": _b64(canonical_bytes(configuration)),
                "reporter-token": _b64(token.encode("ascii")),
            },
        }
        command = [
            "python",
            "-m",
            "loom_capacity_agent.runtime",
            "--configuration-file",
            "/run/loom-capacity/files/reporter-configuration.json",
            "--database-url-file",
            "/run/loom-capacity/files/database-url",
            "--manager-origin",
            "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            "--bearer-token-file",
            "/run/loom-capacity/files/reporter-token",
            "--ca-file",
            "/run/loom-capacity/files/ca.pem",
            "--certificate-file",
            "/run/loom-capacity/files/certificate.pem",
            "--private-key-file",
            "/run/loom-capacity/files/private-key.pem",
            "--poll-interval-seconds",
            "5",
            "--max-attempts",
            "10000",
        ]
        deployment: dict[str, object] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": _NAME, "namespace": _NAMESPACE, "labels": labels},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app.kubernetes.io/name": _NAME}},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "enableServiceLinks": False,
                        "securityContext": {
                            "fsGroup": 65532,
                            "fsGroupChangePolicy": "OnRootMismatch",
                            "runAsGroup": 65532,
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "initContainers": [
                            {
                                "name": "credential-init",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python",
                                    "-m",
                                    "loom_capacity_agent.secret_init",
                                    "--source",
                                    "/var/run/loom-capacity-projected",
                                    "--destination",
                                    "/run/loom-capacity/files",
                                ],
                                "securityContext": restricted,
                                "volumeMounts": [
                                    {
                                        "name": "projected",
                                        "mountPath": "/var/run/loom-capacity-projected",
                                        "readOnly": True,
                                    },
                                    {"name": "runtime", "mountPath": "/run/loom-capacity"},
                                ],
                            }
                        ],
                        "containers": [
                            {
                                "name": "capacity-agent",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": command,
                                "ports": [{"name": "health", "containerPort": 8081}],
                                "readinessProbe": {
                                    "httpGet": {"path": "/ready", "port": "health"},
                                    "periodSeconds": 5,
                                    "timeoutSeconds": 2,
                                    "failureThreshold": 3,
                                },
                                "resources": {
                                    "requests": {"cpu": "25m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                                "securityContext": restricted,
                                "volumeMounts": [
                                    {
                                        "name": "runtime",
                                        "mountPath": "/run/loom-capacity",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "postgres-ca",
                                        "mountPath": "/run/loom-postgres-ca",
                                        "readOnly": True,
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "projected",
                                "secret": {"secretName": _NAME, "defaultMode": 0o440},
                            },
                            {"name": "runtime", "emptyDir": {"medium": "Memory"}},
                            {
                                "name": "postgres-ca",
                                "secret": {
                                    "secretName": "loom-postgres-ca",
                                    "defaultMode": 0o440,
                                    "items": [{"key": "ca.crt", "path": "ca.crt"}],
                                },
                            },
                        ],
                    },
                },
            },
        }
        egress = _network_policy(
            "loom-capacity-agent-egress",
            labels,
            {"app.kubernetes.io/name": _NAME},
            ["Egress"],
            egress=[
                _egress(
                    "loom-dev", {"app.kubernetes.io/name": "loom-capacity-manager"}, [("TCP", 8443)]
                ),
                _egress(
                    "loom-staging",
                    {"cnpg.io/cluster": "loom-postgres", "cnpg.io/instanceRole": "primary"},
                    [("TCP", 5432)],
                ),
                _egress("kube-system", {"k8s-app": "kube-dns"}, [("TCP", 53), ("UDP", 53)]),
            ],
        )
        ingress = _network_policy(
            "loom-capacity-agent-postgres-ingress",
            labels,
            {"cnpg.io/cluster": "loom-postgres", "cnpg.io/instanceRole": "primary"},
            ["Ingress"],
            ingress=[
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "loom-staging"}
                            },
                            "podSelector": {"matchLabels": {"app.kubernetes.io/name": _NAME}},
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 5432}],
                }
            ],
        )
        return cast(
            str,
            yaml.safe_dump_all(
                (secret, deployment, egress, ingress), sort_keys=True, explicit_start=True
            ),
        ).encode("ascii")

    def _get_one(self, kind: str, name: str) -> bytes:
        return self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                f"{kind.lower()}/{name}",
                "--ignore-not-found=true",
                "--show-managed-fields",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )

    def _diff(self, payload: bytes) -> int:
        status = self.runner.run_status(
            (
                "kubectl",
                "diff",
                "--server-side=true",
                f"--field-manager={_FIELD_MANAGER}",
                "--validate=strict",
                f"--request-timeout={_REQUEST_TIMEOUT}",
                "-f",
                "-",
            ),
            env=self.runner.environment,
            input_payload=payload,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        if status not in {0, 1}:
            raise RuntimeError("protected staging capacity agent diff failed")
        return status

    def _apply(self, payload: bytes) -> None:
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "apply",
                "--server-side=true",
                f"--field-manager={_FIELD_MANAGER}",
                "--validate=strict",
                f"--request-timeout={_REQUEST_TIMEOUT}",
                "-f",
                "-",
            ),
            env=self.runner.environment,
            input_payload=payload,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )

    def _one_ready_candidate_pod(self, plan: FinalGatePlan) -> bool:
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                "pods",
                "--selector=app.kubernetes.io/name=loom-capacity-agent",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        try:
            value = _object(payload, label="pod list")
            pods = value.get("items")
            if not isinstance(pods, list) or len(pods) != 1:
                return False
            pod = _object(json.dumps(pods[0]).encode(), label="agent pod")
            status, spec = pod.get("status"), pod.get("spec")
            if (
                not isinstance(status, dict)
                or not isinstance(spec, dict)
                or status.get("phase") != "Running"
            ):
                return False
            expected_image = f"{self.container_registry}/loom-control-plane@{plan.image_digests['loom-control-plane']}"
            containers, statuses = spec.get("containers"), status.get("containerStatuses")
            return (
                isinstance(containers, list)
                and isinstance(statuses, list)
                and len(containers) == len(statuses) == 1
                and isinstance(containers[0], dict)
                and isinstance(statuses[0], dict)
                and containers[0].get("name") == statuses[0].get("name") == "capacity-agent"
                and containers[0].get("image") == statuses[0].get("image") == expected_image
                and statuses[0].get("ready") is True
            )
        except (UnicodeError, ValueError):
            return False


def _network_policy(
    name: str,
    labels: Mapping[str, str],
    selector: Mapping[str, str],
    types: list[str],
    **rules: object,
) -> dict[str, object]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "namespace": _NAMESPACE, "labels": dict(labels)},
        "spec": {"podSelector": {"matchLabels": dict(selector)}, "policyTypes": types, **rules},
    }


def _egress(
    namespace: str, pod_labels: Mapping[str, str], ports: list[tuple[str, int]]
) -> dict[str, object]:
    return {
        "to": [
            {
                "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
                "podSelector": {"matchLabels": dict(pod_labels)},
            }
        ],
        "ports": [{"protocol": protocol, "port": port} for protocol, port in ports],
    }


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _documents(payload: bytes) -> dict[tuple[str, str], dict[str, object]]:
    result: dict[tuple[str, str], dict[str, object]] = {}
    for item in yaml.safe_load_all(payload):
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("kind"), str)
            or not isinstance(item.get("metadata"), dict)
            or not isinstance(item["metadata"].get("name"), str)
        ):
            raise ValueError("protected staging capacity agent manifest is invalid")
        result[(item["kind"], item["metadata"]["name"])] = item
    if set(result) != set(_EXPECTED):
        raise ValueError("protected staging capacity agent manifest is incomplete")
    return result


def _encode_document(value: Mapping[str, object]) -> bytes:
    return _encode_documents((value,))


def _encode_documents(values: Sequence[Mapping[str, object]]) -> bytes:
    return cast(str, yaml.safe_dump_all(values, sort_keys=True, explicit_start=True)).encode(
        "ascii"
    )


def _listed_resources(payload: bytes) -> list[dict[str, object]]:
    value = _object(payload, label="inventory")
    items = value.get("items")
    if not isinstance(items, list):
        raise ValueError("protected staging capacity agent inventory is invalid")
    return [_object(json.dumps(item).encode(), label="inventory resource") for item in items]


def _object(payload: bytes, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise ValueError(f"protected staging capacity agent {label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"protected staging capacity agent {label} is invalid")
    return value


def _identity(value: Mapping[str, object]) -> tuple[str, str]:
    metadata = value.get("metadata")
    if (
        not isinstance(metadata, dict)
        or not isinstance(value.get("kind"), str)
        or not isinstance(metadata.get("name"), str)
    ):
        raise ValueError("protected staging capacity agent inventory identity is invalid")
    if metadata.get("namespace") != _NAMESPACE:
        raise ValueError("protected staging capacity agent inventory identity is invalid")
    kind = value.get("kind")
    name = metadata.get("name")
    assert isinstance(kind, str) and isinstance(name, str)
    return kind, name


def _safe_owned(observed: Mapping[str, object], desired: Mapping[str, object]) -> bool:
    metadata = observed.get("metadata")
    desired_metadata = desired.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(desired_metadata, dict):
        return False
    if (
        observed.get("apiVersion") != desired.get("apiVersion")
        or observed.get("kind") != desired.get("kind")
        or metadata.get("name") != desired_metadata.get("name")
        or metadata.get("namespace") != _NAMESPACE
        or metadata.get("labels") != desired_metadata.get("labels")
        or not isinstance(metadata.get("managedFields"), list)
    ):
        return False
    if observed.get("kind") == "Secret":
        data = observed.get("data")
        desired_data = desired.get("data")
        if (
            observed.get("type") != "Opaque"
            or observed.get("immutable") is not True
            or not isinstance(data, dict)
            or not isinstance(desired_data, dict)
            or not all(isinstance(key, str) for key in data)
            or not all(isinstance(key, str) for key in desired_data)
            or {key for key in data if isinstance(key, str)}
            != {key for key in desired_data if isinstance(key, str)}
        ):
            return False
    owners = metadata["managedFields"]

    def allowed(entry: object) -> bool:
        if not isinstance(entry, dict) or entry.get("fieldsType") != "FieldsV1":
            return False
        if entry.get("manager") == _FIELD_MANAGER and entry.get("operation") == "Apply":
            return True
        fields = entry.get("fieldsV1")
        return (
            entry.get("manager") == "k3s"
            and entry.get("operation") == "Update"
            and entry.get("subresource") == "status"
            and isinstance(fields, dict)
            and set(fields) == {"f:status"}
        )

    return (
        bool(owners)
        and any(
            isinstance(entry, dict)
            and entry.get("manager") == _FIELD_MANAGER
            and entry.get("operation") == "Apply"
            for entry in owners
        )
        and all(allowed(entry) for entry in owners)
    )


def _nonsecret_seed(seed: Mapping[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in seed.items()
        if "password" not in key and key != "reporter_token"
    }


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("protected staging capacity agent JSON is ambiguous")
        value[key] = item
    return value


__all__ = ["KubernetesProtectedStagingCapacityAgentComponent"]
