#!/usr/bin/env python3
"""Plan or run a bounded, isolated restore of one saved Nebius database backup."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "src"))

from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    DeploymentError,
    Kubectl,
    load_render,
    secret_requirements,
    verify_cluster_identity,
    wait_for_job,
)

from loom.nebius_restore import (  # noqa: E402
    DEFAULT_MAX_BACKUP_BYTES,
    MAX_SNAPSHOT_BYTES,
    RESTORE_SCRIPT,
    RestoreError,
    snapshot_sql,
    validate_backup_request,
    validate_baseline,
)


def restore_resources(
    config: dict[str, Any], service_image: str, request: dict[str, Any], name: str
) -> list[dict[str, Any]]:
    ns = config["namespace"]
    if not re.fullmatch(r"loom-restore-[0-9a-f]{12}", name):
        raise RestoreError("invalid-restore-job-name")
    if (
        not service_image.startswith(f"cr.{config['region']}.nebius.cloud/")
        or "@sha256:" not in service_image
    ):
        raise RestoreError("restore-service-image-must-be-nebius-pinned")
    labels = {"app": "loom-platform-restore", "loom-restore-id": name}
    metadata = {"name": name, "namespace": ns, "labels": labels}
    common = {
        "resources": {
            "requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "128Mi"},
            "limits": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "8Gi"},
        },
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {"name": "restore", "mountPath": "/restore"},
            {"name": "code", "mountPath": "/code", "readOnly": True},
        ],
    }

    def worker(phase: str, key: str, secret: str) -> dict[str, Any]:
        return {
            **common,
            "name": phase,
            "image": service_image,
            "command": ["python", "-m", "loom.nebius_restore", phase],
            "env": [
                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                {
                    "name": "LOOM_RESTORE_ACCESS_KEY",
                    "valueFrom": {"secretKeyRef": {"name": "loom-platform-storage", "key": key}},
                },
                {
                    "name": "LOOM_RESTORE_SECRET_KEY",
                    "valueFrom": {"secretKeyRef": {"name": "loom-platform-storage", "key": secret}},
                },
            ],
        }

    return [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "data": {
                "request.json": json.dumps(request),
                "restore.sh": RESTORE_SCRIPT,
                "records.sql": snapshot_sql([row["id"] for row in request["baseline"]["trials"]]),
            },
        },
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": metadata,
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 1800,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "serviceAccountName": "loom-platform",
                        "nodeSelector": {
                            "loom.nebius/platform": "integration",
                            "loom.nebius/node-role": "system",
                            "kubernetes.io/arch": "amd64",
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
                            "runAsUser": 999,
                            "runAsGroup": 999,
                            "fsGroup": 999,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "volumes": [
                            {"name": "restore", "emptyDir": {"sizeLimit": "8Gi"}},
                            {"name": "code", "configMap": {"name": name}},
                        ],
                        "initContainers": [
                            worker("download", "backup-access-key", "backup-secret-key"),
                            {
                                **common,
                                "name": "restore",
                                "image": config["backup_image"],
                                "command": ["sh", "/code/restore.sh"],
                            },
                        ],
                        "containers": [worker("verify", "access-key", "secret-key")],
                    },
                },
            },
        },
    ]


def failure_summary(kube: Kubectl, name: str, namespace: str) -> list[dict[str, Any]]:
    """Read only container state and the worker's bounded safe error fields."""
    result = []
    try:
        pods = json.loads(
            kube.run("get", "pods", "-n", namespace, "-l", "job-name=" + name, "-o", "json")
        )
        for pod in pods.get("items", []):
            status = pod.get("status", {})
            for container_status in status.get("initContainerStatuses", []) + status.get(
                "containerStatuses", []
            ):
                state = container_status.get("state", {})
                detail = state.get("terminated", state.get("waiting", {}))
                reason = detail.get("reason", "")
                if re.fullmatch(r"[A-Za-z0-9_-]{1,80}", reason):
                    item: dict[str, Any] = {"container": container_status["name"], "reason": reason}
                    if isinstance(detail.get("exitCode"), int):
                        item["exit_code"] = detail["exitCode"]
                    result.append(item)
    except Exception:
        pass
    for container in ("download", "restore", "verify"):
        try:
            raw = kube.run("logs", "job/" + name, "-n", namespace, "-c", container)
            if len(raw) > 32768:
                continue
            for line in raw.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if value.get("status") != "failed":
                    continue
                safe: dict[str, Any] = {"container": container}
                for key in ("phase", "error_code"):
                    field = value.get(key)
                    if isinstance(field, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,80}", field):
                        safe[key] = field
                if isinstance(value.get("exit_code"), int):
                    safe["exit_code"] = value["exit_code"]
                result.append(safe)
        except Exception:
            continue
    return result


def run(args: argparse.Namespace, kube: Kubectl) -> dict[str, Any]:
    _, config, files = load_render(args.render_dir)
    if args.baseline.stat().st_size > MAX_SNAPSHOT_BYTES:
        raise RestoreError("baseline-size-limit")
    baseline = json.loads(args.baseline.read_text())
    validate_baseline(baseline)
    service = next(
        doc
        for doc in files["40-services.yaml"]
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service"
    )
    image = service["spec"]["template"]["spec"]["containers"][0]["image"]
    request = {key: config[key] for key in ("namespace", "region", "storage_endpoint", "buckets")}
    request.update(
        backup_key=args.backup_key,
        backup_version_id=args.backup_version_id,
        max_backup_bytes=args.max_backup_bytes,
        baseline=baseline,
    )
    validate_backup_request(request)
    name = "loom-restore-" + uuid4().hex[:12]
    resources = restore_resources(config, image, request, name)
    args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    manifest_path = args.evidence_dir / "restore.yaml"
    manifest_path.write_text(yaml.safe_dump_all(resources, sort_keys=False))
    manifest_path.chmod(0o600)
    evidence: dict[str, Any] = {
        "status": "planned",
        "job": name,
        "namespace": config["namespace"],
        "cluster_id": config["cluster_id"],
    }
    evidence_path = args.evidence_dir / "restore.json"

    def save() -> None:
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n")
        evidence_path.chmod(0o600)

    created = False
    try:
        verify_cluster_identity(kube, config, args.expected_cluster_id)
        for (namespace, secret), required in secret_requirements(
            {"restore.yaml": resources}, config
        ).items():
            observed = kube.run(
                "get",
                "secret",
                secret,
                "-n",
                namespace,
                "-o",
                r'go-template={{range $key,$value := .data}}{{if $value}}{{$key}}{{"\n"}}{{end}}{{end}}',
            )
            if not required <= set(observed.splitlines()):
                raise RestoreError("restore-secret-keys-missing")
        if not args.apply:
            return evidence
        evidence["status"] = "running"
        save()
        kube.run("create", "-f", str(manifest_path))
        created = True
        wait_for_job(kube, name, config["namespace"], 1860)
        raw = kube.run("logs", "job/" + name, "-n", config["namespace"], "-c", "verify")
        if len(raw) > 32768:
            raise RestoreError("restore-summary-size-limit")
        summary = json.loads(raw)
        if summary.get("status") != "verified" or summary.get("local_database_stopped") is not True:
            raise RestoreError("restore-verification-incomplete")
        evidence.update(status="verified", result=summary, cleanup="pending")
        save()  # Keep evidence before deleting only these uniquely created resources.
        for kind in ("job", "configmap"):
            resource = kube.get(kind, name, config["namespace"])
            if resource.get("metadata", {}).get("labels", {}).get("loom-restore-id") != name:
                raise RestoreError("restore-cleanup-identity-mismatch")
            kube.run("delete", kind, name, "-n", config["namespace"], "--wait=true")
        evidence["cleanup"] = "complete"
        return evidence
    except Exception as exc:
        evidence["status"] = "failed"
        if created:
            evidence["failure"] = failure_summary(kube, name, config["namespace"])
        evidence["error_code"] = (
            str(exc) if isinstance(exc, (RestoreError, DeploymentError)) else type(exc).__name__
        )
        raise
    finally:
        save()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render-dir", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--backup-key", required=True)
    parser.add_argument("--backup-version-id")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--max-backup-bytes", type=int, default=DEFAULT_MAX_BACKUP_BYTES)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        result = run(args, Kubectl(args.kubeconfig))
        print(json.dumps(result))
        return 0
    except Exception as exc:
        print(
            str(exc) if isinstance(exc, (RestoreError, DeploymentError)) else type(exc).__name__,
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
