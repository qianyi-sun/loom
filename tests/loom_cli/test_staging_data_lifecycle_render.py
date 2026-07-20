from __future__ import annotations

import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import ClusterConfig


def _resource(config: ClusterConfig, kind: str, name: str) -> dict[str, object] | None:
    for document in yaml.safe_load_all(render_manifests(config)):
        if (
            isinstance(document, dict)
            and document.get("kind") == kind
            and document.get("metadata", {}).get("name") == name
        ):
            return document
    return None


def test_staging_lifecycle_cronjob_is_staging_only_and_least_authority() -> None:
    assert _resource(ClusterConfig(), "CronJob", "loom-staging-data-lifecycle") is None
    assert (
        _resource(
            ClusterConfig(runtime_environment="development"),
            "CronJob",
            "loom-staging-data-lifecycle",
        )
        is None
    )

    document = _resource(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            image_tag="staging-exact",
        ),
        "CronJob",
        "loom-staging-data-lifecycle",
    )
    assert document is not None
    assert document["metadata"]["namespace"] == "loom-staging"  # type: ignore[index]
    spec = document["spec"]  # type: ignore[index]
    assert spec["concurrencyPolicy"] == "Forbid"
    assert spec["jobTemplate"]["spec"]["backoffLimit"] == 0
    pod = spec["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    container = pod["containers"][0]
    assert container["image"] == "loom-control-plane:staging-exact"
    assert container["command"] == [
        "python",
        "-I",
        "-B",
        "-m",
        "loom.data_lifecycle_maintenance",
    ]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    assert container["args"][container["args"].index("--bucket") + 1] == "trajectories"
    env_names = {item["name"] for item in container["env"]}
    assert env_names == {
        "LOOM_LIFECYCLE_DB_URL",
        "LOOM_LIFECYCLE_MINIO_ENDPOINT",
        "LOOM_LIFECYCLE_MINIO_ACCESS_KEY",
        "LOOM_LIFECYCLE_MINIO_SECRET_KEY",
        "LOOM_LIFECYCLE_MINIO_REGION",
        "LOOM_LIFECYCLE_STORAGE_AUTH_KIND",
    }
    assert all(volume["persistentVolumeClaim"]["readOnly"] for volume in pod["volumes"])


def test_staging_lifecycle_uses_exact_physical_bucket_authority() -> None:
    document = _resource(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            artifacts_bucket="loom-staging-artifacts",
            trajectories_bucket="loom-staging-trajectories",
        ),
        "CronJob",
        "loom-staging-data-lifecycle",
    )

    assert document is not None
    args = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][  # type: ignore[index]
        "args"
    ]
    assert args.count("--bucket") == 2
    assert "loom-staging-artifacts" in args
    assert "loom-staging-trajectories" in args


def test_staging_lifecycle_network_policy_is_staging_only() -> None:
    assert _resource(ClusterConfig(), "NetworkPolicy", "loom-staging-data-lifecycle") is None
    policy = _resource(
        ClusterConfig(runtime_environment="staging"),
        "NetworkPolicy",
        "loom-staging-data-lifecycle",
    )
    assert policy is not None
    assert policy["spec"]["ingress"] == []  # type: ignore[index]
    ports = {
        (port["port"], port["protocol"])
        for rule in policy["spec"]["egress"]  # type: ignore[index]
        for port in rule["ports"]
    }
    assert ports == {(53, "TCP"), (53, "UDP"), (5432, "TCP"), (9000, "TCP")}
