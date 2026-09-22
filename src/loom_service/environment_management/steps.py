"""Freeze ordered, exact provider intents before a single resource is created."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from loom.nebius_environment_render import RenderedEnvironment

StepKind = Literal["kubernetes", "object_bucket", "credentials", "database_ready", "job_ready", "application_ready"]


@dataclass(frozen=True)
class ProvisioningStep:
    key: str
    kind: StepKind
    payload: dict[str, Any]


def creation_steps(prepared: RenderedEnvironment) -> list[ProvisioningStep]:
    row = prepared.registration
    namespace = row.application_namespace
    steps: list[ProvisioningStep] = []

    def documents(filename: str) -> None:
        for doc in prepared.files[filename]:
            metadata = doc["metadata"]
            key = f"k8s:{doc['kind']}:{metadata.get('namespace', '-')}:{metadata['name']}"
            steps.append(ProvisioningStep(key, "kubernetes", doc))
            if doc["kind"] == "Job":
                steps.append(ProvisioningStep("ready:" + key, "job_ready", {
                    "namespace": namespace, "name": metadata["name"], "resource_key": key,
                }))

    documents("00-namespaces.yaml")
    for purpose in ("canonical", "source", "backup"):
        for action in ("service_account", "group", "membership", "access_key"):
            steps.append(ProvisioningStep(f"iam:{purpose}:{action}", "credentials", {
                "action": action, "purpose": purpose,
            }))
    for purpose, bucket in sorted(prepared.config["buckets"].items()):
        steps.append(ProvisioningStep("bucket:" + purpose, "object_bucket", {
            "name": bucket, "purpose": purpose,
        }))
    steps.append(ProvisioningStep("credentials:material", "credentials", {
        "action": "material", "namespace": namespace,
    }))
    for name in ("loom-platform-db", prepared.config["db_tls_secret_name"], "loom-platform-auth",
                 "loom-admin-secret", "loom-platform-collector", "loom-platform-batch-runner",
                 "loom-platform-storage"):
        steps.append(ProvisioningStep("secret:" + name, "credentials", {
            "action": "kubernetes_secret", "namespace": namespace, "name": name,
        }))
    documents("10-config-network.yaml")
    documents("20-database.yaml")
    steps.append(ProvisioningStep("ready:database", "database_ready", {
        "namespace": namespace, "name": "loom-postgres",
    }))
    for filename in ("30-migrate.yaml", "40-services.yaml", "50-configure.yaml", "60-execution.yaml",
                     "70-public.yaml", "80-backup.yaml"):
        documents(filename)
        if filename == "40-services.yaml":
            steps.append(ProvisioningStep("ready:services", "application_ready", {
                "namespace": namespace, "phase": "services",
            }))
    steps.append(ProvisioningStep("child:owner", "credentials", {
        "action": "child_owner", "namespace": namespace,
        "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
    }))
    steps.append(ProvisioningStep("ready:application", "application_ready", {
        "namespace": namespace, "public_host": row.public_host,
        "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
        "candidate_id": str(row.candidate_id),
    }))
    if len({step.key for step in steps}) != len(steps):
        raise ValueError("duplicate provisioning resource identity")
    return steps


def retained_steps(source: list[ProvisioningStep], *, was_ready: bool) -> list[ProvisioningStep]:
    """Stop every possible named workload, including unconfirmed create replies."""
    steps = [ProvisioningStep("retain:" + step.key, "credentials", {
        "action": "retained_namespace", "source_key": step.key,
    }) for step in source if step.kind == "kubernetes" and step.payload["kind"] == "Namespace"]
    if was_ready:
        steps.append(ProvisioningStep("retain:owner", "credentials", {"action": "retained_owner_revoke"}))
    steps.extend(ProvisioningStep("retain:quota:" + step.payload["metadata"]["name"], "credentials", {
        "action": "retained_quota", "namespace": step.payload["metadata"]["name"],
    }) for step in source if step.kind == "kubernetes" and step.payload["kind"] == "Namespace")
    for kind in ("Ingress", "CronJob", "Job", "Deployment", "StatefulSet"):
        steps.extend(ProvisioningStep("retain:" + step.key, "credentials", {
            "action": "retained_stop", "source_key": step.key,
        }) for step in source if step.kind == "kubernetes" and step.payload["kind"] == kind)
    steps.extend(ProvisioningStep("retain:key:" + purpose, "credentials", {
        "action": "retained_key_revoke", "purpose": purpose,
    }) for purpose in ("canonical", "source", "backup"))
    steps.append(ProvisioningStep("ready:retained", "application_ready", {"phase": "retained"}))
    return steps
