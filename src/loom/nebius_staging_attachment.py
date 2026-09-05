"""Render a Nebius-local execution attachment to the existing staging authority.

This is an offline render, not a provisioner. All credentials are references to
pre-provisioned namespace-local Secrets; no database or canonical store is created.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_KEY = re.compile(r"[A-Za-z0-9._-]{1,253}\Z")
_IMAGE = re.compile(r"[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}\Z")
_NETWORKS = frozenset(
    {
        "database",
        "canonical_store",
        "source_store",
        "control_plane",
        "kubernetes_api",
        "provider_api",
        "model_api",
    }
)


class StagingAttachmentError(ValueError):
    """Attachment is incomplete, ambiguous or contains unsupported values."""


def _object(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise StagingAttachmentError(f"{label} fields are invalid")
    return value


def _name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise StagingAttachmentError(f"{label} is invalid")
    return value


def _secret(value: Any, fields: set[str], label: str) -> dict[str, Any]:
    result = _object(value, fields | {"name"}, label)
    _name(result["name"], label)
    for field in fields:
        if not isinstance(result[field], str) or not _KEY.fullmatch(result[field]):
            raise StagingAttachmentError(f"{label} key reference is invalid")
    return result


def _endpoint(value: Any, label: str) -> str:
    try:
        if not isinstance(value, str) or any(c.isspace() for c in value):
            raise ValueError
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
            or parsed.port == 0
        ):
            raise ValueError
    except ValueError:
        raise StagingAttachmentError(f"{label} must be a credential-free HTTPS origin") from None
    return value


def validate_staging_attachment(
    value: dict[str, Any], *, environment: str, target: dict[str, Any]
) -> None:
    _object(
        value,
        {
            "schema_version",
            "environment",
            "target_id",
            "namespace",
            "canonical_database",
            "gateway_image",
            "configuration_revision",
            "local_providers_secret_name",
            "canonical",
            "source",
            "gateway_secret",
            "collector",
            "network",
        },
        "staging attachment",
    )
    if (
        environment != "staging"
        or value["environment"] != "staging"
        or value["schema_version"] != "loom.nebius-staging-attachment.v1"
        or value["target_id"] != target["target_id"]
        or value["namespace"] != target["namespace_name"]
        or value["canonical_database"] != "loom_staging"
    ):
        raise StagingAttachmentError(
            "attachment must bind the selected staging target and database"
        )
    if not isinstance(value["gateway_image"], str) or not _IMAGE.fullmatch(value["gateway_image"]):
        raise StagingAttachmentError("staging gateway image must be digest-pinned")
    if not isinstance(value["configuration_revision"], str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["configuration_revision"]
    ):
        raise StagingAttachmentError("configuration_revision must be a SHA-256 deployment revision")
    _name(value["local_providers_secret_name"], "local providers Secret name")
    canonical = _object(
        value["canonical"],
        {
            "endpoint",
            "region",
            "artifacts_bucket",
            "trajectories_bucket",
            "db_secret",
            "storage_secret",
        },
        "canonical",
    )
    _endpoint(canonical["endpoint"], "canonical endpoint")
    _name(canonical["region"], "canonical region")
    if (
        canonical["artifacts_bucket"] != "loom-staging-artifacts"
        or canonical["trajectories_bucket"] != "loom-staging-trajectories"
    ):
        raise StagingAttachmentError("canonical buckets must be the existing staging buckets")
    _secret(canonical["db_secret"], {"gateway_key", "actuator_key"}, "canonical DB Secret")
    _secret(canonical["storage_secret"], {"access_key", "secret_key"}, "canonical store Secret")
    source = _object(
        value["source"], {"endpoint", "region", "bucket", "credentials_secret"}, "source"
    )
    _endpoint(source["endpoint"], "source endpoint")
    _name(source["region"], "source region")
    _name(source["bucket"], "source bucket")
    if source["endpoint"].rstrip("/") == canonical["endpoint"].rstrip("/"):
        raise StagingAttachmentError("staging attachment requires an independent source spool")
    _secret(source["credentials_secret"], {"access_key", "secret_key"}, "source Secret")
    _secret(value["gateway_secret"], {"step_jwt_key", "master_key"}, "gateway Secret")
    collector = _object(
        value["collector"], {"control_plane_url", "token_secret", "nebius_secret"}, "collector"
    )
    _endpoint(collector["control_plane_url"], "collector control-plane URL")
    _secret(collector["token_secret"], {"key"}, "collector token Secret")
    _secret(collector["nebius_secret"], {"key"}, "Nebius observer Secret")
    network = _object(value["network"], set(_NETWORKS), "network")
    for name, entries in network.items():
        if not isinstance(entries, list) or not entries or len(entries) > 32:
            raise StagingAttachmentError(f"network {name} requires bounded destinations")
        for item in entries:
            _object(item, {"cidr", "port"}, f"network {name}")
            try:
                subnet = ipaddress.ip_network(item["cidr"], strict=True)
            except (TypeError, ValueError):
                raise StagingAttachmentError(f"network {name} CIDR is invalid") from None
            if subnet.prefixlen == 0 or subnet.is_loopback or subnet.is_multicast:
                raise StagingAttachmentError(
                    f"network {name} CIDR must be a scoped routed destination"
                )
            if type(item["port"]) is not int or not 1 <= item["port"] <= 65535:
                raise StagingAttachmentError(f"network {name} TCP port is invalid")
    for endpoint, name in (
        (canonical["endpoint"], "canonical_store"),
        (source["endpoint"], "source_store"),
        (collector["control_plane_url"], "control_plane"),
    ):
        parsed = urlsplit(endpoint)
        if (parsed.port or 443) not in {item["port"] for item in network[name]}:
            raise StagingAttachmentError(f"network {name} does not permit its endpoint port")
        try:
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError:
            continue  # DNS/IP convergence is a deployment preflight, never a render side effect.
        if not any(
            address in ipaddress.ip_network(item["cidr"])
            for item in network[name]
            if item["port"] == (parsed.port or 443)
        ):
            raise StagingAttachmentError(f"network {name} does not contain its endpoint address")


def _env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


def _secret_env(name: str, reference: dict[str, Any], key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": reference["name"], "key": reference[key]}},
    }


def _set_env(container: dict[str, Any], item: dict[str, Any]) -> None:
    container["env"] = [row for row in container.get("env", []) if row["name"] != item["name"]] + [
        item
    ]


def _policy(
    namespace: str,
    name: str,
    selector: dict[str, Any],
    *,
    egress: list[dict[str, Any]],
    ingress: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"podSelector": selector, "policyTypes": ["Egress"], "egress": egress}
    if ingress is not None:
        spec.update(policyTypes=["Ingress", "Egress"], ingress=ingress)
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"namespace": namespace, "name": name},
        "spec": spec,
    }


def render_staging_attachment(
    value: dict[str, Any], *, actuator: bytes, collector: bytes
) -> dict[str, bytes]:
    """Adapt already environment-bound runtime objects without reading any Secret."""
    namespace = value["namespace"]
    canonical, source = value["canonical"], value["source"]
    gateway_name = "loom-llm-gateway"
    gateway_selector = {"matchLabels": {"app": gateway_name}}
    execution_selector = {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}}
    revision = {"loom.ca/nebius-configuration-revision": value["configuration_revision"]}
    actuator_docs = list(yaml.safe_load_all(actuator))
    for doc in actuator_docs:
        if doc["kind"] == "Deployment":
            doc["spec"]["template"]["metadata"].setdefault("annotations", {}).update(revision)
            container = doc["spec"]["template"]["spec"]["containers"][0]
            _set_env(
                container,
                _secret_env(
                    "LOOM_EXECUTION_ACTUATOR_DB_URL", canonical["db_secret"], "actuator_key"
                ),
            )
            _set_env(
                container,
                _env(
                    "LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL",
                    f"http://{gateway_name}.{namespace}.svc.cluster.local:9100/internal/service-execution",
                ),
            )
        if doc["metadata"]["name"] == "loom-execution-attempt-egress":
            doc["spec"]["egress"][-1]["to"] = [{"podSelector": gateway_selector}]
    collector_docs = list(yaml.safe_load_all(collector))
    for doc in collector_docs:
        if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
            doc["metadata"]["name"] += "-staging"
        if doc["kind"] == "ClusterRoleBinding":
            doc["roleRef"]["name"] += "-staging"
        if doc["kind"] == "ConfigMap":
            doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL"] = value["collector"][
                "control_plane_url"
            ]
        if doc["kind"] == "CronJob":
            doc["spec"]["jobTemplate"].setdefault("metadata", {}).setdefault(
                "annotations", {}
            ).update(revision)
            doc["spec"]["jobTemplate"]["spec"]["template"]["metadata"].setdefault(
                "annotations", {}
            ).update(revision)
            sources = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]["volumes"][0][
                "projected"
            ]["sources"]
            for entry, label in zip(sources, ("nebius_secret", "token_secret"), strict=True):
                reference = value["collector"][label]
                entry["secret"]["name"] = reference["name"]
                entry["secret"]["items"][0]["key"] = reference["key"]
    gateway_env = [
        _env("LOOM_ENV", "staging"),
        _env("LOOM_NAMESPACE", namespace),
        _env("LOOM_GW_MINIO_ENDPOINT", canonical["endpoint"]),
        _env("LOOM_GW_MINIO_REGION", canonical["region"]),
        _env("LOOM_GW_ARTIFACTS_BUCKET", canonical["artifacts_bucket"]),
        _secret_env("LOOM_GW_DB_URL", canonical["db_secret"], "gateway_key"),
        _secret_env("LOOM_GW_MINIO_ACCESS_KEY", canonical["storage_secret"], "access_key"),
        _secret_env("LOOM_GW_MINIO_SECRET_KEY", canonical["storage_secret"], "secret_key"),
        _secret_env("LOOM_GW_STEP_JWT_SIGNING_KEY", value["gateway_secret"], "step_jwt_key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", value["gateway_secret"], "master_key"),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT", source["endpoint"]),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_REGION", source["region"]),
        _env("LOOM_GW_SERVICE_EXECUTION_SOURCE_BUCKET", source["bucket"]),
        _secret_env(
            "LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
            source["credentials_secret"],
            "access_key",
        ),
        _secret_env(
            "LOOM_GW_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
            source["credentials_secret"],
            "secret_key",
        ),
    ]
    gateway_docs = [
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": gateway_name, "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": gateway_selector,
                "template": {
                    "metadata": {
                        "labels": gateway_selector["matchLabels"],
                        "annotations": revision,
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "terminationGracePeriodSeconds": 300,
                        "nodeSelector": {"loom.nebius/node-role": "system"},
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "gateway",
                                "image": value["gateway_image"],
                                "imagePullPolicy": "IfNotPresent",
                                "env": gateway_env,
                                "envFrom": [
                                    {"secretRef": {"name": value["local_providers_secret_name"]}}
                                ],
                                "ports": [{"containerPort": 9100}],
                                "readinessProbe": {"httpGet": {"path": "/healthz", "port": 9100}},
                                "lifecycle": {
                                    "preStop": {
                                        "exec": {
                                            "command": [
                                                "python",
                                                "-c",
                                                "import urllib.request\n"
                                                "try:\n"
                                                "    req = urllib.request.Request(\n"
                                                "        'http://127.0.0.1:9100/drain', method='POST', data=b''\n"
                                                "    )\n"
                                                "    urllib.request.urlopen(req, timeout=280).read()\n"
                                                "except Exception:\n"
                                                "    pass\n",
                                            ]
                                        }
                                    }
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "resources": {
                                    "requests": {"cpu": "200m", "memory": "256Mi"},
                                    "limits": {"cpu": "1", "memory": "1Gi"},
                                },
                            }
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": gateway_name, "namespace": namespace},
            "spec": {
                "selector": gateway_selector["matchLabels"],
                "ports": [{"port": 9100, "targetPort": 9100}],
            },
        },
    ]

    def destinations(*names: str) -> list[dict[str, Any]]:
        return [
            {
                "to": [{"ipBlock": {"cidr": row["cidr"]}}],
                "ports": [{"protocol": "TCP", "port": row["port"]}],
            }
            for name in names
            for row in value["network"][name]
        ]

    dns = {
        "to": [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                },
                "podSelector": {
                    "matchExpressions": [
                        {"key": "k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]}
                    ]
                },
            }
        ],
        "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
    }
    policies = [
        _policy(
            namespace,
            "loom-attachment-gateway",
            gateway_selector,
            ingress=[
                {
                    "from": [{"podSelector": execution_selector}],
                    "ports": [{"protocol": "TCP", "port": 9100}],
                }
            ],
            egress=[dns, *destinations("database", "canonical_store", "source_store", "model_api")],
        ),
        _policy(
            namespace,
            "loom-attachment-actuator",
            {"matchLabels": {"app.kubernetes.io/name": "loom-execution-actuator"}},
            ingress=[],
            egress=[dns, *destinations("database", "kubernetes_api")],
        ),
        _policy(
            namespace,
            "loom-attachment-collector",
            {"matchLabels": {"app.kubernetes.io/name": "loom-execution-capacity-collector"}},
            ingress=[],
            egress=[dns, *destinations("control_plane", "kubernetes_api", "provider_api")],
        ),
    ]
    return {
        name: yaml.safe_dump_all(documents, sort_keys=False).encode()
        for name, documents in (
            ("nebius-execution-actuator.yaml", actuator_docs),
            ("nebius-capacity-collector.yaml", collector_docs),
            ("nebius-staging-gateway.yaml", gateway_docs),
            ("nebius-staging-attachment-network.yaml", policies),
        )
    }
