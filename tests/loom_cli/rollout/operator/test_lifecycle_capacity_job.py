from __future__ import annotations

from dataclasses import replace

import pytest
import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import ClusterConfig
from loom_cli.rollout.operator.lifecycle_capacity_job import (
    LifecycleCapacityJobError,
    LifecycleCapacityJobPlan,
    build_lifecycle_capacity_job_plan,
)

_SHA = "a" * 40
_TREE = "b" * 40
_ARTIFACT = "c" * 64
_RENDERED = "d" * 64
_IMAGE_ID = "sha256:" + "e" * 64
_TAG = "staging-aaaaaaa"


def _rendered() -> str:
    return render_manifests(
        ClusterConfig(
            runtime_environment="staging",
            namespace="loom-staging",
            image_tag=_TAG,
        )
    )


def _plan(rendered: str | None = None) -> LifecycleCapacityJobPlan:
    return build_lifecycle_capacity_job_plan(
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=8,
        artifact_bundle_sha256=_ARTIFACT,
        rendered_manifest_sha256=_RENDERED,
        control_plane_image_id=_IMAGE_ID,
        image_tag=_TAG,
        rendered_yaml=_rendered() if rendered is None else rendered,
        expected_buckets=("trajectories", "artifacts"),
        expected_filesystem_paths=("/var/lib/loom-minio-capacity/0",),
    )


def test_plan_clones_only_exact_capacity_action_and_binds_identity() -> None:
    plan = _plan()
    document = yaml.safe_load(plan.job_manifest)

    assert document["kind"] == "Job"
    assert document["metadata"]["name"] == plan.job_name
    assert document["metadata"]["namespace"] == "loom-staging"
    container = document["spec"]["template"]["spec"]["containers"][0]
    assert container["args"][:2] == ["--action", "capacity"]
    assert container["image"] == f"loom-control-plane:{_TAG}"
    assert document["spec"]["backoffLimit"] == 0
    assert document["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
    assert plan == LifecycleCapacityJobPlan.from_dict(plan.to_dict())


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("spec", "suspend"), True, "execution policy"),
        (
            (
                "spec",
                "jobTemplate",
                "spec",
                "template",
                "spec",
                "automountServiceAccountToken",
            ),
            True,
            "Pod authority",
        ),
        (
            (
                "spec",
                "jobTemplate",
                "spec",
                "template",
                "spec",
                "containers",
                0,
                "args",
                1,
            ),
            "capacity",
            "input authority",
        ),
        (
            (
                "spec",
                "jobTemplate",
                "spec",
                "template",
                "spec",
                "containers",
                0,
                "env",
                0,
                "valueFrom",
                "secretKeyRef",
                "key",
            ),
            "other-db-url",
            "environment authority",
        ),
        (
            (
                "spec",
                "jobTemplate",
                "spec",
                "template",
                "spec",
                "containers",
                0,
                "volumeMounts",
                0,
                "mountPath",
            ),
            "/unexpected",
            "volume authority",
        ),
    ],
)
def test_plan_rejects_cronjob_authority_drift(
    path: tuple[str | int, ...],
    value: object,
    message: str,
) -> None:
    documents = list(yaml.safe_load_all(_rendered()))
    cronjob = next(item for item in documents if item and item.get("kind") == "CronJob")
    target = cronjob
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    rendered = "\n---\n".join(yaml.safe_dump(item) for item in documents if item)

    with pytest.raises(LifecycleCapacityJobError, match=message):
        _plan(rendered)


def test_plan_rejects_record_or_manifest_drift() -> None:
    plan = _plan()
    with pytest.raises(LifecycleCapacityJobError, match="identity"):
        replace(plan, mutation_epoch=9)
    with pytest.raises(LifecycleCapacityJobError, match="identity"):
        replace(plan, job_manifest=plan.job_manifest + "# drift\n")
