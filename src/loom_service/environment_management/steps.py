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
    for purpose, bucket in sorted(prepared.config["buckets"].items()):
        steps.append(ProvisioningStep("bucket:" + purpose, "object_bucket", {
            "name": bucket, "purpose": purpose,
        }))
    steps.append(ProvisioningStep("credentials", "credentials", {
        "namespace": namespace, "buckets": prepared.config["buckets"],
    }))
    documents("10-config-network.yaml")
    documents("20-database.yaml")
    steps.append(ProvisioningStep("ready:database", "database_ready", {
        "namespace": namespace, "name": "loom-postgres",
    }))
    for filename in ("30-migrate.yaml", "40-services.yaml", "50-configure.yaml", "60-execution.yaml",
                     "70-public.yaml", "80-backup.yaml"):
        documents(filename)
    steps.append(ProvisioningStep("ready:application", "application_ready", {
        "namespace": namespace, "public_host": row.public_host,
        "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
        "candidate_id": str(row.candidate_id),
    }))
    if len({step.key for step in steps}) != len(steps):
        raise ValueError("duplicate provisioning resource identity")
    return steps
