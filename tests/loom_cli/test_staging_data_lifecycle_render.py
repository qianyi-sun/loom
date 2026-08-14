from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import ClusterConfig, load_cluster_config


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
    assert spec["suspend"] is False
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
    assert container["args"][:2] == ["--action", "auto"]
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


def test_single_node_lifecycle_stats_a_mounted_capacity_drive() -> None:
    document = _resource(
        ClusterConfig(runtime_environment="staging", namespace="loom-staging"),
        "CronJob",
        "loom-staging-data-lifecycle",
    )
    assert document is not None
    pod = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]  # type: ignore[index]
    args = pod["containers"][0]["args"]
    assert args[args.index("--capacity-source") + 1] == "filesystem"
    assert "--filesystem-path" in args
    # Single-node still mounts the one hostPath capacity PVC read-only.
    assert [v["name"] for v in pod["volumes"]] == ["minio-capacity-0"]


def test_multi_node_lifecycle_reads_capacity_via_minio_admin_not_pvc_mounts() -> None:
    # #1113: distributed MinIO drives are RWO PVCs held by the loom-minio-*
    # pods, so the maintenance Job must NOT mount them (Multi-Attach). It reads
    # per-drive headroom from the MinIO admin API instead.
    topology_cls = type(ClusterConfig().topology)  # type: ignore[attr-defined]
    config = ClusterConfig(
        runtime_environment="staging",
        namespace="loom-staging",
        persistent_storage_backend="dynamic",
        topology=topology_cls(
            multi_node=True,
            minio_replicas=4,
            anti_affinity="required",
            storage_backend="longhorn",
        ),
    )
    document = _resource(config, "CronJob", "loom-staging-data-lifecycle")
    assert document is not None
    pod = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]  # type: ignore[index]
    args = pod["containers"][0]["args"]
    assert args[args.index("--capacity-source") + 1] == "minio-admin"
    assert args[args.index("--expected-drive-count") + 1] == "4"
    assert "--filesystem-path" not in args
    # No drive PVC mounts/volumes at all — that is the Multi-Attach fix.
    assert "volumes" not in pod
    assert "volumeMounts" not in pod["containers"][0]


def test_staging_lifecycle_uses_exact_physical_bucket_authority() -> None:
    document = _resource(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            artifacts_bucket="loom-staging-artifacts",
            trajectories_bucket="loom-staging-trajectories",
            lifecycle_legacy_buckets=("artifacts", "trajectories"),
        ),
        "CronJob",
        "loom-staging-data-lifecycle",
    )

    assert document is not None
    args = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][  # type: ignore[index]
        "args"
    ]
    buckets = [args[index + 1] for index, value in enumerate(args) if value == "--bucket"]
    assert buckets == [
        "loom-staging-trajectories",
        "loom-staging-artifacts",
        "artifacts",
        "trajectories",
    ]


@pytest.mark.parametrize(
    "legacy_buckets",
    [
        ("artifacts", "artifacts"),
        ("", "trajectories"),
        (" artifacts",),
        ("loom-staging-artifacts",),
    ],
)
def test_staging_lifecycle_rejects_ambiguous_bucket_inventory(
    legacy_buckets: tuple[str, ...],
) -> None:
    config = ClusterConfig(
        runtime_environment="staging",
        artifacts_bucket="loom-staging-artifacts",
        trajectories_bucket="loom-staging-trajectories",
        lifecycle_legacy_buckets=legacy_buckets,
    )

    with pytest.raises(ValueError, match="lifecycle inventory buckets"):
        render_manifests(config)


def test_live_staging_profile_inventories_legacy_and_canonical_buckets() -> None:
    config = load_cluster_config(Path("deploy/environments/staging.multinode.cluster.toml"))

    document = _resource(config, "CronJob", "loom-staging-data-lifecycle")
    assert document is not None
    args = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][  # type: ignore[index]
        "args"
    ]
    buckets = [args[index + 1] for index, value in enumerate(args) if value == "--bucket"]
    assert buckets == [
        "loom-staging-trajectories",
        "loom-staging-artifacts",
        "artifacts",
        "trajectories",
    ]


def test_staging_lifecycle_network_policy_is_staging_only() -> None:
    assert _resource(ClusterConfig(), "NetworkPolicy", "loom-staging-data-lifecycle") is None
    policy = _resource(
        ClusterConfig(runtime_environment="staging"),
        "NetworkPolicy",
        "loom-staging-data-lifecycle",
    )
    assert policy is not None
    # Deny-all ingress is expressed by opting Ingress into policyTypes with no
    # ingress rules -- NOT an explicit `ingress: []`, which the API server
    # normalizes to an absent field and makes server-side apply churn
    # .metadata.generation forever (breaking exact-convergence). Assert the
    # field is omitted while Ingress stays in policyTypes.
    assert "ingress" not in policy["spec"]  # type: ignore[operator]
    assert "Ingress" in policy["spec"]["policyTypes"]  # type: ignore[index]
    ports = {
        (port["port"], port["protocol"])
        for rule in policy["spec"]["egress"]  # type: ignore[index]
        for port in rule["ports"]
    }
    assert ports == {(53, "TCP"), (53, "UDP"), (5432, "TCP"), (9000, "TCP")}

    minio = _resource(
        ClusterConfig(runtime_environment="staging"),
        "NetworkPolicy",
        "loom-minio",
    )
    assert minio is not None
    allowed_sources = {
        peer["podSelector"]["matchLabels"]["app"]
        for rule in minio["spec"]["ingress"]  # type: ignore[index]
        for peer in rule["from"]
    }
    assert "loom-staging-data-lifecycle" in allowed_sources
