from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.manifest_ownership_adoption import (
    ManifestOwnershipAdoptionError,
    build_manifest_ownership_adoption_plan,
    ownership_adoption_argv,
    verify_ownership_adoption_dry_run,
)
from loom_cli.rollout.manifest_readiness import ManifestArtifact

_SHA = "1" * 40
_TREE = "2" * 40


def _desired() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "loom-staging-data-lifecycle",
                "namespace": "loom-staging",
                "labels": {"app": "loom-staging-data-lifecycle"},
            },
            "spec": {
                "suspend": False,
                "schedule": "*/5 * * * *",
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "lifecycle",
                                        "image": "loom-control-plane:staging-1111111",
                                        "resources": {"limits": {"cpu": "2"}},
                                    }
                                ]
                            }
                        }
                    }
                },
            },
        },
        *_network_policies(),
    ]


def _network_policies() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": name, "namespace": "loom-staging"},
            "spec": {
                "podSelector": {"matchLabels": {"app": name.removeprefix("loom-")}},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [{"from": [{"podSelector": {"matchLabels": {"app": "new"}}}]}],
            },
        }
        for name in ("loom-minio", "loom-postgres", "loom-staging-data-lifecycle")
    ]


def _artifact() -> ManifestArtifact:
    rendered = yaml.safe_dump_all(_desired(), sort_keys=True)
    digest = hashlib.sha256(rendered.encode()).hexdigest()
    return ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=digest,
        resource_count=4,
        resource_set_digest="3" * 64,
        image_identities={"loom-control-plane": "sha256:" + "4" * 64},
        artifact_digest="5" * 64,
    )


def _managed_fields(*managers: str) -> list[dict[str, object]]:
    return [
        {
            "manager": manager,
            "operation": "Update"
            if manager in {"kubectl-client-side-apply", "kubectl-patch"}
            else "Apply",
            "apiVersion": "v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:spec": {}},
        }
        for manager in managers
    ]


def _live() -> list[dict[str, object]]:
    desired = _desired()
    live = copy.deepcopy(desired)
    for index, item in enumerate(live, start=1):
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        metadata.update(
            {
                "uid": f"uid-{index}",
                "resourceVersion": str(100 + index),
                "generation": index,
                "managedFields": _managed_fields(
                    "loom-lifecycle-bootstrap" if index in {1, 4} else "kubectl-client-side-apply"
                ),
            }
        )
    cron_spec = live[0]["spec"]
    assert isinstance(cron_spec, dict)
    cron_spec["suspend"] = True
    containers = cron_spec["jobTemplate"]["spec"]["template"]["spec"]["containers"]  # type: ignore[index]
    containers[0]["resources"]["limits"]["cpu"] = "1"
    for item in live[1:]:
        spec = item["spec"]
        assert isinstance(spec, dict)
        spec.pop("ingress")
    return live


def _plan():
    return build_manifest_ownership_adoption_plan(
        artifact=_artifact(),
        live_resources=_live(),
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=2,
    )


def test_plan_projects_live_values_and_omits_new_fields() -> None:
    plan = _plan()
    overlays = list(yaml.safe_load_all(plan.overlay_yaml))

    cron = overlays[0]
    assert cron["spec"]["suspend"] is True
    assert (
        cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["resources"][
            "limits"
        ]["cpu"]
        == "1"
    )
    for policy in overlays[1:]:
        assert "ingress" not in policy["spec"]
    assert len(plan.plan_sha256) == 64
    for overlay in overlays:
        assert overlay["metadata"]["uid"].startswith("uid-")
        assert overlay["metadata"]["resourceVersion"].isdigit()
    assert [item.identity for item in plan.resources] == sorted(
        item.identity for item in plan.resources
    )


def test_plan_rejects_unknown_manager_target_and_prestate_drift() -> None:
    live = _live()
    live[0]["metadata"]["managedFields"] = _managed_fields("ambient-admin")  # type: ignore[index]
    with pytest.raises(ManifestOwnershipAdoptionError, match="unrecognized"):
        build_manifest_ownership_adoption_plan(
            artifact=_artifact(),
            live_resources=live,
            candidate_sha=_SHA,
            candidate_tree=_TREE,
            mutation_epoch=2,
        )

    live = _live()
    live.append(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "extra", "namespace": "loom-staging"},
        }
    )
    with pytest.raises(ManifestOwnershipAdoptionError, match="resource set"):
        build_manifest_ownership_adoption_plan(
            artifact=_artifact(),
            live_resources=live,
            candidate_sha=_SHA,
            candidate_tree=_TREE,
            mutation_epoch=2,
        )


def test_dry_run_must_be_semantic_noop_and_binds_exact_live_prestate() -> None:
    plan = _plan()
    live = _live()
    digest = verify_ownership_adoption_dry_run(
        plan,
        live_resources=live,
        dry_run_resources=copy.deepcopy(live),
    )
    assert len(digest) == 64

    changed = copy.deepcopy(live)
    changed[0]["spec"]["suspend"] = False  # type: ignore[index]
    with pytest.raises(ManifestOwnershipAdoptionError, match="change live state"):
        verify_ownership_adoption_dry_run(
            plan,
            live_resources=live,
            dry_run_resources=changed,
        )

    drifted = copy.deepcopy(live)
    drifted[0]["metadata"]["resourceVersion"] = "999"  # type: ignore[index]
    with pytest.raises(ManifestOwnershipAdoptionError, match="prestate drifted"):
        verify_ownership_adoption_dry_run(
            plan,
            live_resources=drifted,
            dry_run_resources=drifted,
        )


def test_maintenance_force_command_is_explicit_and_bounded() -> None:
    dry = ownership_adoption_argv(
        kubeconfig=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        dry_run=True,
    )
    apply = ownership_adoption_argv(
        kubeconfig=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        dry_run=False,
    )
    assert "--force-conflicts" in dry
    assert "--dry-run=server" in dry
    assert "--force-conflicts" in apply
    assert "--dry-run=server" not in apply
    assert dry[-2:] == ("-f", "-")
    output = ownership_adoption_argv(
        kubeconfig=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        dry_run=True,
        output_json=True,
    )
    assert output[-6:-4] == ("--output", "json")

    with pytest.raises(ValueError, match="kubeconfig"):
        ownership_adoption_argv(kubeconfig=Path("relative"), dry_run=True)
