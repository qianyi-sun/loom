"""Complete core application fixtures for protected component/journal tests."""

from __future__ import annotations

import copy

CORE_NAMES = ("loom-control-plane", "loom-service", "loom-llm-gateway", "loom-web")


def application_manifest(name: str, replicas: int = 2) -> dict:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": "loom-staging"},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": name, "image": f"{name}@sha256:{'a' * 64}"}]},
            },
        },
    }


def ready_application(desired: dict) -> dict:
    live = copy.deepcopy(desired)
    name = live["metadata"]["name"]
    live["metadata"].update({"generation": 88, "uid": f"uid-{name}", "resourceVersion": "123"})
    live["status"] = {
        "observedGeneration": 88,
        **{
            key: live["spec"]["replicas"]
            for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")
        },
    }
    return live
