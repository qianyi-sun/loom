"""Render an isolated Nebius platform from explicit, immutable inputs.

This renderer has no cloud or Kubernetes side effects. Existing execution
actuator/collector templates remain the single implementation of those pods.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1


class NebiusPlatformError(ValueError):
    """An input cannot describe an independent, reproducible environment."""


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(value)).hexdigest()


def _name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value
    ):
        raise NebiusPlatformError(f"{label} must be a DNS label")
    return value


def validate_environment(config: dict[str, Any]) -> None:
    if config.get("schema_version") != "loom.nebius-platform.v1":
        raise NebiusPlatformError("unsupported platform configuration")
    expected = {
        "schema_version",
        "namespace",
        "execution_namespace",
        "environment",
        "target_id",
        "cluster_scope_id",
        "region",
        "public_host",
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
        "kubernetes_api_server",
        "tls_secret_name",
        "db_tls_secret_name",
        "storage_class",
        "postgres_image",
        "backup_image",
        "postgres_storage_gi",
        "storage_endpoint",
        "buckets",
        "model_provider_base_url",
        "max_concurrent",
        "capacity_policy",
        "execution_price",
    }
    if set(config) - {"public_tls_bootstrap"} != expected:
        raise NebiusPlatformError("platform configuration has missing or unknown fields")
    if type(config.get("public_tls_bootstrap", False)) is not bool:
        raise NebiusPlatformError("public_tls_bootstrap must be a boolean")
    for key in (
        "namespace",
        "execution_namespace",
        "target_id",
        "cluster_scope_id",
        "storage_class",
    ):
        _name(config.get(key), key)
    if config["namespace"] == config["execution_namespace"] or any(
        not config[key].startswith("loom-nebius-") for key in ("namespace", "execution_namespace")
    ):
        raise NebiusPlatformError("independent Nebius system and execution namespaces are required")
    if config.get("environment") != "development":
        raise NebiusPlatformError(
            "this independent integration lane requires environment=development"
        )
    host = config.get("public_host", "")
    if not isinstance(host, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+\.[a-z]{2,63}", host):
        raise NebiusPlatformError("public_host must be a DNS hostname")
    for key in (
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
    ):
        if not isinstance(config.get(key), str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", config[key]):
            raise NebiusPlatformError(f"{key} is required")
    if not config["quota_parent_id"].startswith("tenant-"):
        raise NebiusPlatformError("quota_parent_id must identify the tenant, not the project")
    api = urlsplit(str(config["kubernetes_api_server"]))
    if (
        api.scheme != "https"
        or not api.hostname
        or api.username
        or api.password
        or api.query
        or api.fragment
        or api.path not in ("", "/")
    ):
        raise NebiusPlatformError("kubernetes_api_server must be an HTTPS control plane origin")
    region = config.get("region", "")
    if not isinstance(region, str) or not re.fullmatch(r"[a-z]+-[a-z]+[0-9]", region):
        raise NebiusPlatformError("region is invalid")
    for key in ("postgres_image", "backup_image"):
        if not re.fullmatch(
            r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-zA-Z0-9_./-]+@sha256:[0-9a-f]{64}",
            str(config.get(key, "")),
        ):
            raise NebiusPlatformError(f"{key} must be mirrored and digest-pinned in Nebius")
    endpoint = urlsplit(str(config.get("storage_endpoint", "")))
    if endpoint.geturl() != f"https://storage.{region}.nebius.cloud":
        raise NebiusPlatformError(
            "storage_endpoint must be the region's native Nebius HTTPS endpoint"
        )
    buckets = config.get("buckets", {})
    if set(buckets) != {"artifacts", "trajectories", "source", "backup"}:
        raise NebiusPlatformError(
            "four independent canonical, source and backup buckets are required"
        )
    for value in buckets.values():
        _name(value, "bucket")
    if len(set(buckets.values())) != 4:
        raise NebiusPlatformError("canonical, source and backup buckets must be distinct")
    if not isinstance(config.get("postgres_storage_gi"), int) or config["postgres_storage_gi"] < 10:
        raise NebiusPlatformError("postgres_storage_gi must be at least 10")
    for key in ("tls_secret_name", "db_tls_secret_name"):
        _name(config.get(key), key)
    policy = config.get("capacity_policy", {})
    required = (
        "max_nodes",
        "max_vcpu_millis",
        "max_memory_mib",
        "max_storage_mib",
        "node_cpu_millis",
        "node_memory_mib",
        "node_storage_mib",
        "max_pending_jobs",
        "max_create_per_minute",
        "observation_max_age_seconds",
    )
    if not isinstance(policy, dict) or any(
        type(policy.get(key)) is not int or policy[key] <= 0 for key in required
    ):
        raise NebiusPlatformError("capacity_policy requires positive measured resource limits")
    if set(policy) != {
        *required,
        "enabled",
        "max_unschedulable_jobs",
        "max_image_pull_backoff_jobs",
        "reason",
    }:
        raise NebiusPlatformError("capacity policy has missing or unknown fields")
    for key in ("max_unschedulable_jobs", "max_image_pull_backoff_jobs"):
        if type(policy[key]) is not int or policy[key] < 0:
            raise NebiusPlatformError("capacity failure allowances must be nonnegative integers")
    if type(config.get("max_concurrent")) is not int or config["max_concurrent"] < 1:
        raise NebiusPlatformError("max_concurrent must be positive")
    if policy.get("enabled") is not True:
        raise NebiusPlatformError("capacity policy must explicitly enable the bounded target")
    price = config.get("execution_price", {})
    rate_keys = (
        "base_microusd_per_hour",
        "vcpu_microusd_per_hour",
        "memory_gib_microusd_per_hour",
        "ephemeral_storage_gib_microusd_per_hour",
    )
    price_keys = {
        "provider",
        "region",
        "sku",
        "source",
        "source_version",
        "source_uri",
        "effective_at",
        "observed_at",
        *rate_keys,
    }
    if (
        not isinstance(price, dict)
        or set(price) != price_keys
        or price["provider"] != "nebius"
        or price["region"] != region
    ):
        raise NebiusPlatformError(
            "execution price must identify this Nebius region and exact source"
        )
    if (
        any(type(price[key]) is not int or price[key] < 0 for key in rate_keys)
        or sum(price[key] for key in rate_keys) <= 0
    ):
        raise NebiusPlatformError(
            "execution price requires measured nonnegative rates, including a positive rate"
        )
    for key in ("sku", "source", "source_version", "source_uri"):
        if not isinstance(price[key], str) or not price[key].strip():
            raise NebiusPlatformError("execution price source identity is incomplete")
    for key in ("effective_at", "observed_at"):
        try:
            parsed = datetime.fromisoformat(price[key].replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError
        except (ValueError, TypeError, AttributeError) as exc:
            raise NebiusPlatformError("execution price timestamps must be timezone-aware") from exc
    provider = urlsplit(str(config.get("model_provider_base_url", "")))
    if (
        provider.scheme != "https"
        or not provider.hostname
        or provider.username
        or provider.password
        or provider.query
        or provider.fragment
    ):
        raise NebiusPlatformError("model provider must be an HTTPS URL without credentials")


def _obj(
    kind: str, name: str, namespace: str | None, spec: Any = None, *, api: str = "v1"
) -> dict[str, Any]:
    value: dict[str, Any] = {"apiVersion": api, "kind": kind, "metadata": {"name": name}}
    if namespace:
        value["metadata"]["namespace"] = namespace
    if spec is not None:
        value["spec"] = spec
    return value


def _env(values: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"name": key, "value": str(value)} for key, value in values.items()]


def _secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


def _mount_secret(
    pod: dict[str, Any],
    name: str,
    secret: str,
    path: str,
    *,
    mode: int = 0o440,
    ca_only: bool = False,
) -> None:
    if name == "admin":
        pod.setdefault("volumes", []).extend(
            [
                {
                    "name": "admin-source",
                    "secret": {
                        "secretName": secret,
                        "defaultMode": 0o440,
                        "items": [{"key": "secrets.toml", "path": "secrets.toml"}],
                    },
                },
                {"name": "admin-owned", "emptyDir": {"medium": "Memory"}},
            ]
        )
        pod.setdefault("initContainers", []).append(
            {
                "name": "prepare-admin-secret",
                "image": pod["containers"][0]["image"],
                "command": [
                    "python",
                    "-c",
                    "from pathlib import Path; p=Path('/var/run/loom-admin-owned/secrets.toml'); p.write_bytes(Path('/var/run/loom-admin-source/secrets.toml').read_bytes()); p.chmod(0o400)",
                ],
                "volumeMounts": [
                    {
                        "name": "admin-source",
                        "mountPath": "/var/run/loom-admin-source",
                        "readOnly": True,
                    },
                    {"name": "admin-owned", "mountPath": "/var/run/loom-admin-owned"},
                ],
                "resources": {
                    "requests": {"cpu": "10m", "memory": "32Mi"},
                    "limits": {"cpu": "100m", "memory": "64Mi"},
                },
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                },
            }
        )
        for container in pod["containers"]:
            container.setdefault("volumeMounts", []).append(
                {"name": "admin-owned", "mountPath": path, "readOnly": True}
            )
        return
    pod.setdefault("volumes", []).append(
        {"name": name, "secret": {"secretName": secret, "defaultMode": mode}}
    )
    if ca_only:
        pod["volumes"][-1]["secret"]["items"] = [{"key": "ca.crt", "path": "ca.crt"}]
    for container in pod["containers"]:
        container.setdefault("volumeMounts", []).append(
            {"name": name, "mountPath": path, "readOnly": True}
        )


def _service(name: str, ns: str, port: int, target: int | None = None) -> dict[str, Any]:
    return _obj(
        "Service",
        name,
        ns,
        {
            "selector": {"app": name},
            "ports": [
                {
                    "name": "http" if port != 5432 else "postgres",
                    "port": port,
                    "targetPort": target or port,
                }
            ],
        },
    )


def _deployment(
    name: str,
    ns: str,
    image: str,
    port: int,
    health: str,
    env: list[dict[str, Any]],
    revision: str,
    *,
    cpu: str = "100m",
    memory: str = "256Mi",
) -> dict[str, Any]:
    return _obj(
        "Deployment",
        name,
        ns,
        {
            "replicas": 1,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
            },
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {
                    "labels": {"app": name},
                    "annotations": {"loom.nebius/configuration-revision": revision},
                },
                "spec": {
                    "automountServiceAccountToken": False,
                    "serviceAccountName": "loom-platform",
                    "nodeSelector": {
                        "loom.nebius/node-role": "system",
                        "loom.nebius/platform": "integration",
                    },
                    "tolerations": [
                        {
                            "key": "loom.nebius/platform",
                            "operator": "Equal",
                            "value": "integration",
                            "effect": "NoSchedule",
                        }
                    ],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": name,
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": env,
                            "ports": [{"containerPort": port}],
                            "readinessProbe": {
                                "httpGet": {"path": health, "port": port},
                                "periodSeconds": 5,
                            },
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                        }
                    ],
                },
            },
        },
        api="apps/v1",
    )


def _namespace(name: str) -> dict[str, Any]:
    obj = _obj("Namespace", name, None)
    obj["metadata"]["labels"] = {
        "loom.nebius/platform": "true",
        "pod-security.kubernetes.io/enforce": "restricted",
    }
    return obj


def _network_policy(
    name: str,
    ns: str,
    selector: dict[str, Any],
    ingress: list[Any],
    egress: list[Any] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"podSelector": selector, "policyTypes": ["Ingress"], "ingress": ingress}
    if egress is not None:
        spec.update(policyTypes=["Ingress", "Egress"], egress=egress)
    return _obj("NetworkPolicy", name, ns, spec, api="networking.k8s.io/v1")


def _peer(ns: str, app: str | None = None) -> dict[str, Any]:
    peer: dict[str, Any] = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ns}}
    }
    if app:
        peer["podSelector"] = {"matchLabels": {"app": app}}
    return peer


def _replace_tree(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [_replace_tree(row, replacements) for row in value]
    if isinstance(value, dict):
        return {key: _replace_tree(row, replacements) for key, row in value.items()}
    return value


def public_tls_config(config: dict[str, Any]) -> dict[str, Any]:
    """Native Caddy TLS automation with a valid manual certificate for migration."""
    host, ns = config["public_host"], config["namespace"]
    api_proxy = {
        "handler": "reverse_proxy",
        "upstreams": [{"dial": f"loom-service.{ns}.svc:8090"}],
        "transport": {"protocol": "http", "read_timeout": "300s"},
        "flush_interval": -1,
        "headers": {
            "request": {
                "set": {
                    "Host": ["{http.request.host}"],
                    "X-Forwarded-Proto": ["https"],
                    "X-Forwarded-For": ["{http.request.remote.host}"],
                }
            }
        },
    }
    result: dict[str, Any] = {
        "admin": {"disabled": True},
        "storage": {"module": "file_system", "root": "/data"},
        "apps": {
            "tls": {
                # Caddyfile `tls cert key` pins certificate selection to the manual
                # certificate's tag, even after expiry. Untagged JSON loading lets
                # CertMagic select an unexpired managed certificate automatically.
                "certificates": {
                    "load_files": [
                        {
                            "certificate": "/var/run/loom-public-tls/tls.crt",
                            "key": "/var/run/loom-public-tls/tls.key",
                        }
                    ]
                },
                "automation": {
                    "policies": [
                        {
                            "subjects": [host],
                            "issuers": [
                                {
                                    "module": "acme",
                                    "ca": "https://acme-v02.api.letsencrypt.org/directory",
                                    "challenges": {
                                        "http": {"disabled": True},
                                        "tls-alpn": {"alternate_port": 8443},
                                    },
                                }
                            ],
                        }
                    ]
                },
            },
            "http": {
                "https_port": 8443,
                "servers": {
                    "public": {
                        "listen": [":8443"],
                        "protocols": ["h1", "h2"],
                        "automatic_https": {
                            "disable_redirects": True,
                            "ignore_loaded_certificates": True,
                        },
                        # Kubelet probes a Pod IP without DNS SNI; the Host header
                        # still selects the public HTTP route after the handshake.
                        "tls_connection_policies": [{"default_sni": host}],
                        "routes": [
                            {
                                "match": [{"host": [host]}],
                                "handle": [
                                    {
                                        "handler": "headers",
                                        "response": {
                                            "set": {
                                                "Strict-Transport-Security": ["max-age=31536000"]
                                            }
                                        },
                                    },
                                    {"handler": "request_body", "max_size": 100 * 1024 * 1024},
                                    {
                                        "handler": "subroute",
                                        "routes": [
                                            {
                                                "match": [{"path": ["/api/v1/*"]}],
                                                "handle": [api_proxy],
                                                "terminal": True,
                                            },
                                            {
                                                "handle": [
                                                    {
                                                        "handler": "reverse_proxy",
                                                        "upstreams": [{"dial": "127.0.0.1:8080"}],
                                                    }
                                                ]
                                            },
                                        ],
                                    },
                                ],
                            }
                        ],
                    }
                },
            },
        },
    }
    if not config.get("public_tls_bootstrap", False):
        result["apps"]["tls"].pop("certificates")
    return result


def build_platform(
    config: dict[str, Any],
    candidate: dict[str, Any],
    profile: dict[str, Any],
    keyring: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, list[dict[str, Any]]]:
    """Build Kubernetes resources from published image refs and environment settings."""
    validate_environment(config)
    if (
        candidate.get("source_ref") != "refs/heads/codex/nebius-main"
        or candidate.get("repository") != "qianyi-sun/loom"
    ):
        raise NebiusPlatformError("candidate must originate from the Nebius integration branch")
    if profile.get("candidate_sha") != candidate.get("candidate_sha"):
        raise NebiusPlatformError("runtime profile does not match the candidate")
    images = {key: value["image_ref"] for key, value in candidate["images"].items()}
    if (
        profile.get("task_image_ref") != images["service"]
        or profile.get("runtime_image_ref") != images["execution_runtime"]
    ):
        raise NebiusPlatformError("execution images do not match the deployed candidate")
    ns, ex = config["namespace"], config["execution_namespace"]
    # One rollout fingerprint triggers config-only Pod updates and distinct Jobs.
    # It is an identifier, not an integrity or deployment admission check.
    revision = digest(
        {"config": config, "candidate": candidate, "profile": profile, "keyring": keyring}
    )
    short = revision.removeprefix("sha256:")[:12]
    files: dict[str, list[dict[str, Any]]] = {}
    files["00-namespaces.yaml"] = [_namespace(ns), _namespace(ex)]
    db_host = f"loom-postgres.{ns}.svc"
    cm = _obj("ConfigMap", "loom-platform-config", ns)
    target = {
        "schema_version": "loom.execution-target.v1",
        "target_id": config["target_id"],
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "cluster_scope_id": config["cluster_scope_id"],
        "environment": config["environment"],
        "provider": "nebius",
        "region": config["region"],
        "failure_domain": config["cluster_scope_id"],
        "data_residency": "eu",
        "namespace_name": ex,
        "health_role": "primary",
        "health_check_id": config["target_id"],
        "health_check_interval_seconds": 30,
        "health_stale_after_seconds": 90,
    }
    catalog = {
        "execution_class": NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json"),
        "topology": {
            "schema_version": "loom.execution-topology.v1",
            "logical_pool_id": "nebius-cpu",
            "execution_class_id": "linux-amd64-cpu-pod-v1",
            "placement_policy": "environment-local-health-first",
            "targets": [target],
        },
    }
    cm["data"] = {
        "profile.json": canonical(profile).decode(),
        "keyring.json": canonical(keyring).decode(),
        "environment.json": canonical(config).decode(),
        "catalog.json": canonical(catalog).decode(),
        "public-tls.json": canonical(public_tls_config(config)).decode(),
    }
    private_ingress = [
        {"from": [_peer(ns), _peer(ex)], "ports": [{"protocol": "TCP", "port": port}]}
        for port in (8080, 8090, 9100)
    ]
    platform_account = _obj("ServiceAccount", "loom-platform", ns)
    platform_account["automountServiceAccountToken"] = False
    files["10-config-network.yaml"] = [
        cm,
        platform_account,
        _network_policy("default-deny-ingress", ns, {}, []),
        _network_policy(
            "platform-internal",
            ns,
            {
                "matchExpressions": [
                    {
                        "key": "app",
                        "operator": "In",
                        "values": ["loom-service", "loom-control-plane", "loom-llm-gateway"],
                    }
                ]
            },
            private_ingress,
        ),
        _network_policy(
            "postgres-private",
            ns,
            {"matchLabels": {"app": "loom-postgres"}},
            [
                {
                    "from": [
                        _peer(ns),
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": ex}
                            },
                            "podSelector": {
                                "matchLabels": {"app.kubernetes.io/name": "loom-execution-actuator"}
                            },
                        },
                    ],
                    "ports": [{"protocol": "TCP", "port": 5432}],
                }
            ],
        ),
        _network_policy(
            "public-web",
            ns,
            {"matchLabels": {"app": "loom-web"}},
            [{"ports": [{"protocol": "TCP", "port": 8443}]}],
        ),
    ]
    postgres = _deployment(
        "loom-postgres",
        ns,
        config["postgres_image"],
        5432,
        "/",
        [
            _secret_env("POSTGRES_PASSWORD", "loom-platform-db", "postgres-password"),
            {"name": "POSTGRES_DB", "value": "loom"},
            {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
        ],
        revision,
        cpu="100m",
    )
    postgres["kind"] = "StatefulSet"
    postgres["spec"].pop("strategy")
    postgres["spec"]["serviceName"] = "loom-postgres"
    postgres["spec"]["persistentVolumeClaimRetentionPolicy"] = {
        "whenDeleted": "Retain",
        "whenScaled": "Retain",
    }
    postgres["spec"]["volumeClaimTemplates"] = [
        {
            "metadata": {"name": "data"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": config["storage_class"],
                "resources": {"requests": {"storage": f"{config['postgres_storage_gi']}Gi"}},
            },
        }
    ]
    pgpod = postgres["spec"]["template"]["spec"]
    pgpod["securityContext"].update(runAsUser=999, runAsGroup=999, fsGroup=999)
    pg = pgpod["containers"][0]
    pg["args"] = [
        "-c",
        "ssl=on",
        "-c",
        "ssl_cert_file=/var/run/loom-db-tls/tls.crt",
        "-c",
        "ssl_key_file=/var/run/loom-db-tls/tls.key",
    ]
    pg["readinessProbe"] = {
        "exec": {"command": ["pg_isready", "-U", "postgres", "-d", "loom"]},
        "periodSeconds": 5,
    }
    pg["volumeMounts"] = [{"name": "data", "mountPath": "/var/lib/postgresql/data"}]
    _mount_secret(pgpod, "db-tls", config["db_tls_secret_name"], "/var/run/loom-db-tls")
    files["20-database.yaml"] = [_service("loom-postgres", ns, 5432), postgres]

    def job(name: str, phase: str) -> dict[str, Any]:
        deployment = _deployment(
            name,
            ns,
            images["service"],
            8090,
            "/",
            _env(
                {
                    "LOOM_ENV": config["environment"],
                    "LOOM_NAMESPACE": ns,
                    "LOOM_PLATFORM_CONFIG": "/var/run/loom-platform/environment.json",
                }
            ),
            revision,
        )
        pod = deployment["spec"]["template"]["spec"]
        container = pod["containers"][0]
        container.pop("readinessProbe")
        container["command"] = ["python", "-m", "loom.nebius_platform_bootstrap", phase]
        container["env"].extend(
            [
                _secret_env("LOOM_DB_URL", "loom-platform-db", "admin-url"),
                _secret_env("LOOM_COLLECTOR_TOKEN", "loom-platform-collector", "token"),
            ]
        )
        if phase == "database":
            container["env"].append(
                _secret_env("LOOM_BATCH_RUNNER_TOKEN", "loom-platform-batch-runner", "token")
            )
        for component in ("service", "control-plane", "gateway", "actuator"):
            container["env"].append(
                _secret_env(
                    "LOOM_DB_" + component.upper().replace("-", "_") + "_PASSWORD",
                    "loom-platform-db",
                    component + "-password",
                )
            )
        pod["volumes"] = [
            {"name": "platform-config", "configMap": {"name": "loom-platform-config"}}
        ]
        container["volumeMounts"] = [
            {"name": "platform-config", "mountPath": "/var/run/loom-platform", "readOnly": True}
        ]
        _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
        _mount_secret(pod, "admin", "loom-admin-secret", "/var/run/loom/admin")
        pod["restartPolicy"] = "Never"
        return _obj(
            "Job",
            name,
            ns,
            {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 600,
                "template": deployment["spec"]["template"],
            },
            api="batch/v1",
        )

    files["30-migrate.yaml"] = [job(f"loom-platform-migrate-{short}", "database")]
    app_docs = []
    for component, name, prefix, port, health in (
        ("service", "loom-service", "LOOM_SVC", 8090, "/api/v1/health"),
        ("control_plane", "loom-control-plane", "LOOM_CP", 8080, "/healthz"),
        ("gateway", "loom-llm-gateway", "LOOM_GW", 9100, "/healthz"),
    ):
        env = _env(
            {
                "LOOM_ENV": config["environment"],
                "LOOM_NAMESPACE": ns,
                f"{prefix}_BIND_HOST": "0.0.0.0",
                f"{prefix}_BIND_PORT": port,
                f"{prefix}_ADMIN_SECRET_FILE": "/var/run/loom/admin/secrets.toml",
                f"{prefix}_MINIO_ENDPOINT": config["storage_endpoint"],
                f"{prefix}_MINIO_REGION": config["region"],
                f"{prefix}_STORAGE_BACKEND": "minio",
                f"{prefix}_STORAGE_AUTH_KIND": "static_keys",
                f"{prefix}_ARTIFACTS_BUCKET": config["buckets"]["artifacts"],
                f"{prefix}_TRAJECTORIES_BUCKET": config["buckets"]["trajectories"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_ENDPOINT": config["storage_endpoint"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_REGION": config["region"],
                f"{prefix}_SERVICE_EXECUTION_SOURCE_BUCKET": config["buckets"]["source"],
                f"{prefix}_DB_POOL_SIZE": 5,
                f"{prefix}_DB_MAX_OVERFLOW": 5,
            }
        )
        env += [
            _secret_env(
                f"{prefix}_DB_URL", "loom-platform-db", component.replace("_", "-") + "-url"
            ),
            _secret_env(f"{prefix}_MINIO_ACCESS_KEY", "loom-platform-storage", "access-key"),
            _secret_env(f"{prefix}_MINIO_SECRET_KEY", "loom-platform-storage", "secret-key"),
            _secret_env(
                f"{prefix}_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
                "loom-platform-storage",
                "source-access-key",
            ),
            _secret_env(
                f"{prefix}_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
                "loom-platform-storage",
                "source-secret-key",
            ),
            _secret_env(f"{prefix}_STEP_JWT_SIGNING_KEY", "loom-platform-auth", "jwt-signing-key"),
            _secret_env(
                "LOOM_SECRET_STORE_MASTER_KEY", "loom-platform-auth", "secret-store-master-key"
            ),
        ]
        if component == "control_plane":
            env += _env(
                {
                    "LOOM_CP_LLM_GATEWAY_URL": f"http://loom-llm-gateway.{ns}.svc:9100",
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED": "true",
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT": config["environment"],
                    "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID": "nebius-cpu",
                    "LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED": "true",
                    "LOOM_CP_SERVICE_EXECUTION_SOURCE_RETENTION_SEC": 86400,
                    "LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED": "false",
                    "LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON": canonical(
                        keyring
                    ).decode(),
                }
            )
        elif component == "service":
            env.append(
                _secret_env("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "loom-platform-batch-runner", "token")
            )
            env += _env(
                {
                    "LOOM_SVC_CONTROL_PLANE_URL": f"http://loom-control-plane.{ns}.svc:8080",
                    "LOOM_SVC_GATEWAY_URL": f"http://loom-llm-gateway.{ns}.svc:9100",
                    "LOOM_SVC_PUBLIC_BASE_URL": "https://" + config["public_host"],
                    "LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON": canonical(profile).decode(),
                    "LOOM_SVC_PERSONAL_DEV_CAPACITY_POOL_CAPABILITIES_JSON": "[]",
                    "LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED": "false",
                    "LOOM_SVC_TEAM_REGISTRATION_OPEN": "false",
                }
            )
        else:
            env += _env({"LOOM_GW_LOCAL_YIBU_BASE_URL": config["model_provider_base_url"]})
            env += [_secret_env("LOOM_GW_LOCAL_YIBU_API_KEY", "loom-model-provider", "api-key")]
        # Settings are service-specific. Do not ship ignored source/JWT secrets
        # to the public API or imply that unsupported pool knobs are active.
        if component == "service":
            env = [
                row
                for row in env
                if not row["name"].startswith("LOOM_SVC_SERVICE_EXECUTION_SOURCE_")
                and row["name"]
                not in {
                    "LOOM_SVC_STEP_JWT_SIGNING_KEY",
                    "LOOM_SVC_DB_POOL_SIZE",
                    "LOOM_SVC_DB_MAX_OVERFLOW",
                }
            ]
        elif component == "gateway":
            env = [
                row
                for row in env
                if row["name"]
                not in {
                    "LOOM_GW_TRAJECTORIES_BUCKET",
                    "LOOM_GW_DB_POOL_SIZE",
                    "LOOM_GW_DB_MAX_OVERFLOW",
                }
            ]
        deployment = _deployment(name, ns, images[component], port, health, env, revision)
        pod = deployment["spec"]["template"]["spec"]
        _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
        _mount_secret(pod, "admin", "loom-admin-secret", "/var/run/loom/admin")
        if component == "gateway":
            pod["terminationGracePeriodSeconds"] = 300
        app_docs += [deployment, _service(name, ns, port)]
    web = _deployment(
        "loom-web",
        ns,
        images["web"],
        8080,
        "/",
        _env(
            {
                "LOOM_FRONTEND_ENVIRONMENT": config["environment"],
                "LOOM_FRONTEND_ENVIRONMENT_LABEL": "Nebius integration",
                "LOOM_FRONTEND_ROUTE_PATH": "",
                "LOOM_FRONTEND_API_BASE": "",
                "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://" + config["public_host"],
            }
        ),
        revision,
        cpu="25m",
        memory="64Mi",
    )
    wpod = web["spec"]["template"]["spec"]
    wpod["securityContext"].update(runAsUser=101, runAsGroup=101, fsGroup=101)
    wpod["volumes"] = [
        {"name": "public-config", "configMap": {"name": "loom-platform-config"}},
        {"name": "tls-data", "persistentVolumeClaim": {"claimName": "loom-web-tls"}},
    ]
    wpod["containers"].append(
        {
            "name": "public-tls",
            "image": images["web"],
            "imagePullPolicy": "IfNotPresent",
            "command": ["/usr/bin/caddy"],
            "args": ["run", "--config", "/etc/loom-public/public-tls.json"],
            "env": _env({"XDG_CONFIG_HOME": "/data/config", "XDG_DATA_HOME": "/data"}),
            "ports": [{"name": "https", "containerPort": 8443}],
            "volumeMounts": [
                {"name": "public-config", "mountPath": "/etc/loom-public", "readOnly": True},
                {"name": "tls-data", "mountPath": "/data"},
            ],
            "readinessProbe": {
                "httpGet": {
                    "scheme": "HTTPS",
                    "path": "/",
                    "port": "https",
                    "httpHeaders": [{"name": "Host", "value": config["public_host"]}],
                },
                "periodSeconds": 5,
            },
            "resources": {
                "requests": {"cpu": "25m", "memory": "64Mi"},
                "limits": {"cpu": "500m", "memory": "256Mi"},
            },
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }
    )
    if config.get("public_tls_bootstrap", False):
        wpod["volumes"].append(
            {"name": "public-tls", "secret": {"secretName": config["tls_secret_name"]}}
        )
        wpod["containers"][-1]["volumeMounts"].append(
            {"name": "public-tls", "mountPath": "/var/run/loom-public-tls", "readOnly": True}
        )
    # Services are applied after the deployment's mandatory pre-mutation backup.
    # RWO permits the rolling Pods on the single integration system node.
    app_docs.append(
        _obj(
            "PersistentVolumeClaim",
            "loom-web-tls",
            ns,
            {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": config["storage_class"],
                "resources": {"requests": {"storage": "4Gi"}},
            },
        )
    )
    app_docs.append(web)
    files["40-services.yaml"] = app_docs
    files["50-configure.yaml"] = [job(f"loom-platform-configure-{short}", "configure")]
    # Reuse the existing least-privilege execution pod and collector definitions.
    execution_docs = []
    replacements = {
        "loom-nebius-development": ex,
        "nebius-eu-north1-development": config["target_id"],
        ".loom.svc.cluster.local": f".{ns}.svc.cluster.local",
    }
    for filename in ("nebius-execution-actuator.yaml", "nebius-capacity-collector.yaml"):
        docs = list(yaml.safe_load_all((repo_root / "deploy/k8s" / filename).read_text()))
        for doc in docs:
            if not doc or doc["kind"] in {"Namespace", "PodDisruptionBudget"}:
                continue
            doc = _replace_tree(doc, replacements)
            if doc["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                doc["metadata"]["name"] = ex + "-collector"
                if doc["kind"] == "ClusterRoleBinding":
                    doc["roleRef"]["name"] = ex + "-collector"
            if doc["kind"] == "ResourceQuota":
                policy = config["capacity_policy"]
                doc["spec"]["hard"] = {
                    "pods": str(policy["max_pending_jobs"] + 8),
                    "requests.cpu": f"{policy['max_vcpu_millis']}m",
                    "requests.memory": f"{policy['max_memory_mib']}Mi",
                }
            if doc["kind"] == "NetworkPolicy":
                for rule in doc["spec"].get("egress", []):
                    for peer in rule.get("to", []):
                        labels = peer.get("namespaceSelector", {}).get("matchLabels", {})
                        if labels.get("kubernetes.io/metadata.name") == "loom":
                            labels["kubernetes.io/metadata.name"] = ns
            if doc["kind"] == "ConfigMap":
                for suffix, key in (
                    ("NEBIUS_PROJECT_ID", "project_id"),
                    ("NEBIUS_QUOTA_PARENT_ID", "quota_parent_id"),
                    ("NEBIUS_NODE_GROUP_ID", "execution_node_group_id"),
                    ("NEBIUS_REGION", "region"),
                ):
                    doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_" + suffix] = config[key]
            pod = None
            if doc["kind"] == "Deployment":
                pod = doc["spec"]["template"]["spec"]
                _mount_secret(
                    pod, "db-ca", "loom-execution-actuator-db", "/var/run/loom-db", ca_only=True
                )
            elif doc["kind"] == "CronJob":
                pod = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            if pod is not None:
                pod["nodeSelector"] = {
                    "loom.nebius/node-role": "system",
                    "loom.nebius/platform": "integration",
                }
                pod["tolerations"] = [
                    {
                        "key": "loom.nebius/platform",
                        "operator": "Equal",
                        "value": "integration",
                        "effect": "NoSchedule",
                    }
                ]
                for container in pod.get("initContainers", []) + pod["containers"]:
                    container["image"] = images["execution_actuator"]
                    for env_row in container.get("env", []):
                        if env_row["name"] == "LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR":
                            env_row["value"] = json.dumps(
                                {
                                    "loom.nebius/node-role": "integration-execution",
                                    "loom.nebius/platform": "integration",
                                }
                            )
                        elif env_row["name"] == "LOOM_EXECUTION_ACTUATOR_TOLERATIONS":
                            env_row["value"] = json.dumps(
                                [
                                    {
                                        "key": "loom.nebius/execution",
                                        "operator": "Equal",
                                        "value": "true",
                                        "effect": "NoSchedule",
                                    },
                                    {
                                        "key": "loom.nebius/platform",
                                        "operator": "Equal",
                                        "value": "integration",
                                        "effect": "NoSchedule",
                                    },
                                ]
                            )
                pod.setdefault("securityContext", {}).update(
                    runAsUser=65532, runAsGroup=65532, fsGroup=65532
                )
            if doc["kind"] == "ConfigMap":
                doc["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR"] = (
                    "loom.nebius/node-role=integration-execution,loom.nebius/platform=integration"
                )
            execution_docs.append(doc)
    files["60-execution.yaml"] = execution_docs
    public = _service("loom-web", ns, 443, 8443)
    public["spec"]["type"] = "LoadBalancer"
    public["metadata"]["annotations"] = {
        "nebius.com/load-balancer-allocation-id": config["public_allocation_id"]
    }
    files["70-public.yaml"] = [public]
    backup_pod = job("loom-platform-backup", "backup")["spec"]["template"]
    backup_spec = backup_pod["spec"]
    # A database credential is needed only by pg_dump; the uploader receives
    # only the separate backup bucket key and a transient dump volume.
    uploader = backup_spec["containers"][0]
    uploader["env"] = [
        *_env({"LOOM_PLATFORM_CONFIG": "/var/run/loom-platform/environment.json"}),
        _secret_env("LOOM_BACKUP_ACCESS_KEY", "loom-platform-storage", "backup-access-key"),
        _secret_env("LOOM_BACKUP_SECRET_KEY", "loom-platform-storage", "backup-secret-key"),
    ]
    uploader["volumeMounts"] = [
        {"name": "platform-config", "mountPath": "/var/run/loom-platform", "readOnly": True},
        {"name": "dump", "mountPath": "/backup", "readOnly": True},
    ]
    backup_spec["volumes"] = [
        {"name": "platform-config", "configMap": {"name": "loom-platform-config"}},
        {
            "name": "db-ca",
            "secret": {
                "secretName": "loom-platform-db",
                "items": [{"key": "ca.crt", "path": "ca.crt"}],
                "defaultMode": 0o440,
            },
        },
        {"name": "dump", "emptyDir": {"sizeLimit": f"{config['postgres_storage_gi']}Gi"}},
    ]
    backup_spec["initContainers"] = [
        {
            "name": "pg-dump",
            "image": config["backup_image"],
            "command": ["pg_dump", "--format=custom", "--no-owner", "--file=/backup/loom.dump"],
            "env": [
                *_env(
                    {
                        "PGHOST": db_host,
                        "PGPORT": 5432,
                        "PGUSER": "postgres",
                        "PGDATABASE": "loom",
                        "PGSSLMODE": "verify-full",
                        "PGSSLROOTCERT": "/var/run/loom-db/ca.crt",
                    }
                ),
                _secret_env("PGPASSWORD", "loom-platform-db", "postgres-password"),
            ],
            "volumeMounts": [
                {"name": "db-ca", "mountPath": "/var/run/loom-db", "readOnly": True},
                {"name": "dump", "mountPath": "/backup"},
            ],
            "resources": {
                "requests": {"cpu": "100m", "memory": "128Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }
    ]
    files["80-backup.yaml"] = [
        _obj(
            "CronJob",
            "loom-platform-backup",
            ns,
            {
                "schedule": "17 */6 * * *",
                "concurrencyPolicy": "Forbid",
                "successfulJobsHistoryLimit": 2,
                "failedJobsHistoryLimit": 3,
                "jobTemplate": {
                    "spec": {
                        "backoffLimit": 1,
                        "activeDeadlineSeconds": 1800,
                        "template": backup_pod,
                    }
                },
            },
            api="batch/v1",
        )
    ]
    return files


def write_platform(
    files: dict[str, list[dict[str, Any]]],
    config: dict[str, Any],
    candidate: dict[str, Any],
    output: Path,
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    for filename, docs in files.items():
        (output / filename).write_text(yaml.safe_dump_all(docs, sort_keys=False))
    return {
        "candidate_sha": candidate["candidate_sha"],
        "namespace": config["namespace"],
        "execution_namespace": config["execution_namespace"],
        "files": list(files),
    }
