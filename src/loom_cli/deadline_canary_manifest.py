"""Render isolated fixture resources; rendering is not deployment authorization."""

import re
from typing import Any
from uuid import UUID

from loom.deadline_canary import CanaryBinding


def render_fixture_resources(
    *,
    run_id: UUID,
    binding: CanaryBinding,
    candidate_sha: str,
    gateway_image: str,
) -> list[dict[str, Any]]:
    """Use the candidate Gateway digest, never copy its environment/DB secrets.

    The deployment controller must match this image to the completed rollout
    manifest and running Gateway before applying. Existing private-endpoint
    authorization and working CNI policies remain preconditions, not toggles.
    No Secret object is rendered: provision the two run-specific capabilities
    through the protected secret channel, not evidence files or command argv.
    """
    if (
        re.fullmatch(r"[0-9a-f]{40}", candidate_sha) is None
        or re.fullmatch(r"[a-zA-Z0-9./:_-]+/loom-llm-gateway@sha256:[0-9a-f]{64}", gateway_image)
        is None
    ):
        raise ValueError("fixture requires a fixed candidate and immutable Gateway image")
    name = "loom-deadline-canary-" + run_id.hex
    labels = {"app": "loom-deadline-canary", "loom-canary-run": run_id.hex}
    metadata = {"name": name, "namespace": "loom-staging", "labels": labels}
    volume = {
        "name": "capability",
        "secret": {
            "secretName": name,
            "defaultMode": 0o440,
            "items": [{"key": key, "path": key} for key in ("provider-key", "operator-key")],
        },
    }
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
        "immutable": True,
        "data": {"binding.json": binding.model_dump_json()},
    }
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 190,
            "template": {
                "metadata": {
                    "labels": labels,
                    "annotations": {"loom-candidate-sha": candidate_sha},
                },
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "terminationGracePeriodSeconds": 3,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                    },
                    "containers": [
                        {
                            "name": "fixture",
                            "image": gateway_image,
                            "command": ["python", "-m", "loom_cli.deadline_fault_provider"],
                            "args": [
                                "--binding",
                                "/config/binding.json",
                                "--provider-key-source",
                                "file:/capability/provider-key",
                                "--operator-key-source",
                                "file:/capability/operator-key",
                                "--host",
                                "0.0.0.0",
                            ],
                            "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
                            "ports": [{"name": "http", "containerPort": 9000}],
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "128Mi"},
                                "limits": {"cpu": "500m", "memory": "256Mi"},
                            },
                            "securityContext": {
                                "readOnlyRootFilesystem": True,
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [
                                {
                                    "name": "capability",
                                    "mountPath": "/capability",
                                    "readOnly": True,
                                },
                                {"name": "binding", "mountPath": "/config", "readOnly": True},
                            ],
                            "readinessProbe": {
                                "httpGet": {"path": "/healthz", "port": 9000},
                                "periodSeconds": 2,
                            },
                        }
                    ],
                    "volumes": [volume, {"name": "binding", "configMap": {"name": name}}],
                },
            },
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": metadata,
        "spec": {"selector": labels, "ports": [{"port": 9000, "targetPort": 9000}]},
    }
    policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": metadata,
        "spec": {
            "podSelector": {"matchLabels": labels},
            "policyTypes": ["Ingress", "Egress"],
            "egress": [],
            "ingress": [
                {
                    "from": [
                        {"podSelector": {"matchLabels": {"app": app}}}
                        for app in ("loom-llm-gateway", "loom-egress-proxy", "loom-service")
                    ],
                    "ports": [{"protocol": "TCP", "port": 9000}],
                }
            ],
        },
    }
    return [config, job, service, policy]
