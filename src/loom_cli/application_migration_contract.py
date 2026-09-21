"""Fixed non-secret inputs for the protected staging application migration Job."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping

APPLICATION_OWNER_ROLE = "loom_app_staging_owner"
APPLICATION_MIGRATION_CA_PATH = "/run/loom-application-migration/ca.crt"


def application_migration_secret_name(job_name: str) -> str:
    if re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", job_name) is None:
        raise ValueError("application migration Job name is invalid")
    return "loom-app-migration-" + hashlib.sha256(job_name.encode()).hexdigest()[:32]


def application_migration_authority(job_name: str) -> dict[str, object]:
    """The renderer and admission share only the fixed credential/process inputs."""
    secret = application_migration_secret_name(job_name)
    return {
        "env": [
            {"name": "LOOM_DB_URL", "valueFrom": {"secretKeyRef": {"name": secret, "key": "db-url"}}},
            {"name": "LOOM_DB_OWNER_ROLE", "value": APPLICATION_OWNER_ROLE},
            {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
        ],
        "volumeMounts": [{"name": "migration-authority", "mountPath": "/run/loom-application-migration", "readOnly": True}],
        "volumes": [{"name": "migration-authority", "secret": {"secretName": secret,
            "items": [{"key": "ca.crt", "path": "ca.crt"}], "defaultMode": 292}}],
        "podSecurityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
            "seccompProfile": {"type": "RuntimeDefault"}},
        "containerSecurityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]}},
    }


def require_application_migration_job(job: Mapping[str, object], *, owner_role: str) -> None:
    """Validate the unsubmitted attested Job, not a server-defaulted live object."""
    metadata, spec = job.get("metadata"), job.get("spec")
    if (owner_role != APPLICATION_OWNER_ROLE or not isinstance(metadata, dict)
            or metadata.get("namespace") != "loom-staging" or not isinstance(metadata.get("name"), str)
            or not isinstance(spec, dict) or set(spec) != {"backoffLimit", "activeDeadlineSeconds", "template"}
            or not isinstance(spec["template"], dict)):
        raise ValueError("application migration authority contract changed")
    pod = spec["template"].get("spec")
    if (not isinstance(pod, dict)
            or set(pod) != {"restartPolicy", "automountServiceAccountToken", "enableServiceLinks", "securityContext", "volumes", "containers"}
            or pod.get("restartPolicy") != "Never" or pod.get("automountServiceAccountToken") is not False
            or pod.get("enableServiceLinks") is not False
            or not isinstance(pod.get("containers"), list) or len(pod["containers"]) != 1
            or not isinstance(pod["containers"][0], dict)):
        raise ValueError("application migration process contract changed")
    container = pod["containers"][0]
    expected = application_migration_authority(metadata["name"])
    if (set(container) != {"name", "image", "imagePullPolicy", "command", "env", "resources", "volumeMounts", "securityContext"}
            or container.get("env") != expected["env"] or container.get("volumeMounts") != expected["volumeMounts"]
            or container.get("securityContext") != expected["containerSecurityContext"]
            or pod.get("securityContext") != expected["podSecurityContext"] or pod.get("volumes") != expected["volumes"]):
        raise ValueError("application migration credential contract changed")
