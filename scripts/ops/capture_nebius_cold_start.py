#!/usr/bin/env python3
"""Read-only native scaling evidence; never creates Pods or changes node groups."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

Json = dict[str, Any]
KubeRead = Callable[[list[str]], Json]


def selected(source: Json, keys: tuple[str, ...]) -> Json:
    return {key: source[key] for key in keys if key in source}


def scheduling_categories(message: str) -> list[str]:
    return [
        category
        for category in (
            "Insufficient cpu",
            "Insufficient memory",
            "Insufficient ephemeral-storage",
            "untolerated taint",
            "node affinity/selector",
            "Too many pods",
            "free ports",
        )
        if category in message
    ]


def capture(
    kube: KubeRead,
    native: Callable[[], Json],
    *,
    node_group_id: str,
    node_selector: str,
    namespace: str,
) -> Json:
    """Inject operator_transport adapters to reuse an existing protected SSH route.

    Every source has its own read timestamp. Errors deliberately omit raw CLI
    stderr, which can contain credentials or private connection details.
    """
    result: Json = {"observed_at": datetime.now(UTC).isoformat(), "node_group_id": node_group_id}

    def read(name: str, function: Callable[[], Any]) -> None:
        started = datetime.now(UTC).isoformat()
        try:
            result[name] = {
                "read_started_at": started,
                "data": function(),
                "read_finished_at": datetime.now(UTC).isoformat(),
            }
        except Exception as exc:
            result[name] = {"read_started_at": started, "unavailable": type(exc).__name__}

    def provider() -> Json:
        value = native()
        if value["metadata"]["id"] != node_group_id:
            raise ValueError("unexpected node group")
        return {
            # Nebius protobuf JSON omits scalar zero values and emits int64 as strings.
            "status": {
                "node_count": int(value["status"].get("node_count", 0)),
                "target_node_count": int(value["status"].get("target_node_count", 0)),
                "state": value["status"].get("state"),
            },
            "autoscaling": {
                key: int(value["spec"]["autoscaling"].get(key, 0))
                for key in ("min_node_count", "max_node_count")
            },
        }

    def nodes() -> list[Json]:
        items = kube(["get", "nodes", "-l", node_selector, "-o", "json"])["items"]
        return [
            {
                "name": node["metadata"]["name"],
                "created_at": node["metadata"].get("creationTimestamp"),
                "deleting_at": node["metadata"].get("deletionTimestamp"),
                "unschedulable": node.get("spec", {}).get("unschedulable", False),
                "taints": node.get("spec", {}).get("taints", []),
                "allocatable": node.get("status", {}).get("allocatable", {}),
                "conditions": [
                    selected(condition, ("type", "status", "reason", "lastTransitionTime"))
                    for condition in node.get("status", {}).get("conditions", [])
                ],
            }
            for node in items
        ]

    def pods() -> list[Json]:
        items = kube(["get", "pods", "-n", namespace, "-o", "json"])["items"]
        output = []
        for pod in items:
            spec, status, meta = pod["spec"], pod.get("status", {}), pod["metadata"]
            output.append(
                {
                    "name": meta["name"],
                    "uid": meta["uid"],
                    "created_at": meta.get("creationTimestamp"),
                    "deleting_at": meta.get("deletionTimestamp"),
                    "node": spec.get("nodeName"),
                    "phase": status.get("phase"),
                    "node_selector": spec.get("nodeSelector", {}),
                    "tolerations": spec.get("tolerations", []),
                    "containers": [
                        {
                            "name": c["name"],
                            "init": init,
                            "restart_policy": c.get("restartPolicy"),
                            "requests": c.get("resources", {}).get("requests", {}),
                        }
                        for init, containers in (
                            (False, spec.get("containers", [])),
                            (True, spec.get("initContainers", [])),
                        )
                        for c in containers
                    ],
                    "scheduled": [
                        {
                            **selected(c, ("status", "reason", "lastTransitionTime")),
                            "categories": scheduling_categories(c.get("message", "")),
                        }
                        for c in status.get("conditions", [])
                        if c["type"] == "PodScheduled"
                    ],
                }
            )
        return output

    def autoscaler() -> Json:
        status = kube(
            ["get", "configmap", "cluster-autoscaler-status", "-n", "kube-system", "-o", "json"]
        )
        parsed = yaml.safe_load(status["data"]["status"])
        return {
            **selected(parsed, ("time", "autoscalerStatus", "clusterWide")),
            "nodeGroups": [g for g in parsed.get("nodeGroups", []) if g["name"] == node_group_id],
        }

    def events() -> list[Json]:
        rows = kube(["get", "events", "-n", namespace, "-o", "json"])["items"]
        return [
            {
                **selected(row, ("reason", "firstTimestamp", "lastTimestamp", "count")),
                "object": selected(row.get("involvedObject", {}), ("kind", "name", "uid")),
                "categories": scheduling_categories(row.get("message", "")),
                # Autoscaler-generated capacity transitions, never arbitrary workload messages.
                "scale_up": row.get("message") if row.get("reason") == "TriggeredScaleUp" else None,
            }
            for row in rows
            if row.get("reason") in ("FailedScheduling", "TriggeredScaleUp", "NotTriggerScaleUp")
        ]

    read("provider", provider)
    read("autoscaler", autoscaler)
    read("nodes", nodes)
    read("pods", pods)
    read("events", events)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--node-group-id", required=True)
    parser.add_argument("--node-selector", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--nebius-profile", required=True)
    parser.add_argument("--nebius-bin", default="nebius")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New private JSONL file; existing files are never overwritten",
    )
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=10)
    args = parser.parse_args()
    if not 1 <= args.samples <= 360 or not 1 <= args.interval <= 60:
        parser.error("samples must be 1..360 and interval 1..60 seconds")

    def run(argv: list[str]) -> Json:
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=40, check=True)
        value = json.loads(completed.stdout)
        if not isinstance(value, dict):
            raise ValueError("CLI response is not an object")
        return value

    def kube(argv: list[str]) -> Json:
        return run(["kubectl", "--kubeconfig", args.kubeconfig, "--request-timeout=30s", *argv])

    def native() -> Json:
        return run(
            [
                args.nebius_bin,
                "--profile",
                args.nebius_profile,
                "mk8s",
                "v1",
                "node-group",
                "get",
                "--id",
                args.node_group_id,
                "--format",
                "json",
                "--no-progress",
                "--no-check-update",
                "--no-browser",
                "--timeout",
                "30s",
                "--retries",
                "1",
            ]
        )

    with os.fdopen(
        os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
    ) as stream:
        for index in range(args.samples):
            snapshot = capture(
                kube,
                native,
                node_group_id=args.node_group_id,
                node_selector=args.node_selector,
                namespace=args.namespace,
            )
            stream.write(json.dumps(snapshot) + "\n")
            stream.flush()
            if index + 1 < args.samples:
                time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
