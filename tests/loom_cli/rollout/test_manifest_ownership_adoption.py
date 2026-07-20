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


def test_plan_adopts_only_legacy_owned_api_normalized_empty_policy_fields() -> None:
    desired = _desired()
    lifecycle_desired = desired[-1]["spec"]
    assert isinstance(lifecycle_desired, dict)
    lifecycle_desired["ingress"] = []
    rendered = yaml.safe_dump_all(desired, sort_keys=True)
    artifact = ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        resource_count=4,
        resource_set_digest="3" * 64,
        image_identities={"loom-control-plane": "sha256:" + "4" * 64},
        artifact_digest="5" * 64,
    )
    live = _live()
    lifecycle_metadata = live[-1]["metadata"]
    assert isinstance(lifecycle_metadata, dict)
    lifecycle_metadata["managedFields"] = [
        {
            "manager": "loom-lifecycle-bootstrap",
            "operation": "Apply",
            "apiVersion": "networking.k8s.io/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:spec": {"f:ingress": {}, "f:policyTypes": {}}},
        }
    ]
    plan = build_manifest_ownership_adoption_plan(
        artifact=artifact,
        live_resources=live,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=3,
    )
    overlays = {item["metadata"]["name"]: item for item in yaml.safe_load_all(plan.overlay_yaml)}

    assert overlays["loom-staging-data-lifecycle"]["spec"]["ingress"] == []
    assert "ingress" not in overlays["loom-minio"]["spec"]
    assert "ingress" not in overlays["loom-postgres"]["spec"]
    dry_run = copy.deepcopy(live)
    dry_run_spec = dry_run[-1]["spec"]
    assert isinstance(dry_run_spec, dict)
    dry_run_spec["ingress"] = []
    assert (
        len(
            verify_ownership_adoption_dry_run(
                plan,
                live_resources=live,
                dry_run_resources=dry_run,
            )
        )
        == 64
    )

    changed = copy.deepcopy(dry_run)
    changed_spec = changed[-1]["spec"]
    assert isinstance(changed_spec, dict)
    changed_spec["ingress"] = [{"from": [{"podSelector": {}}]}]
    with pytest.raises(ManifestOwnershipAdoptionError, match="change live state"):
        verify_ownership_adoption_dry_run(
            plan,
            live_resources=live,
            dry_run_resources=changed,
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


def test_plan_accepts_exact_partially_adopted_state_for_new_request() -> None:
    live = _live()
    for item in live[1:3]:
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        metadata["managedFields"] = _managed_fields("loom-staging-rollout")

    plan = build_manifest_ownership_adoption_plan(
        artifact=_artifact(),
        live_resources=live,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=3,
    )

    assert plan.mutation_epoch == 3
    assert len(plan.resources) == 4


def test_plan_covers_namespaced_and_cluster_scoped_rendered_resources() -> None:
    desired = _desired()
    desired.extend(
        (
            {
                "apiVersion": "v1",
                "kind": "PersistentVolume",
                "metadata": {"name": "loom-staging-data"},
                "spec": {"capacity": {"storage": "20Gi"}},
            },
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "loom-settings", "namespace": "loom-staging"},
                "data": {"mode": "candidate"},
            },
        )
    )
    rendered = yaml.safe_dump_all(desired, sort_keys=True)
    artifact = ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        resource_count=6,
        resource_set_digest="3" * 64,
        image_identities={"loom-control-plane": "sha256:" + "4" * 64},
        artifact_digest="5" * 64,
    )
    live = _live()
    for index, resource in enumerate(copy.deepcopy(desired[-2:]), start=10):
        metadata = resource["metadata"]
        assert isinstance(metadata, dict)
        metadata.update(
            {
                "uid": f"uid-{index}",
                "resourceVersion": str(100 + index),
                "managedFields": _managed_fields("kubectl-client-side-apply"),
            }
        )
        live.append(resource)
    config_data = live[-1]["data"]
    assert isinstance(config_data, dict)
    config_data["mode"] = "live"

    plan = build_manifest_ownership_adoption_plan(
        artifact=artifact,
        live_resources=live,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=4,
    )

    assert len(plan.resources) == 6
    by_identity = {item.identity: item for item in plan.resources}
    pv = by_identity["v1|PersistentVolume||loom-staging-data"]
    assert pv.generation is None
    assert "namespace" not in pv.overlay["metadata"]  # type: ignore[operator]
    config = by_identity["v1|ConfigMap|loom-staging|loom-settings"]
    assert config.overlay["data"] == {"mode": "live"}
    assert (
        len(
            verify_ownership_adoption_dry_run(
                plan,
                live_resources=live,
                dry_run_resources=copy.deepcopy(live),
            )
        )
        == 64
    )


def test_plan_rejects_controller_only_managed_state() -> None:
    live = _live()
    live[1]["metadata"]["managedFields"] = _managed_fields("kube-controller-manager")  # type: ignore[index]

    with pytest.raises(ManifestOwnershipAdoptionError, match="recognized"):
        build_manifest_ownership_adoption_plan(
            artifact=_artifact(),
            live_resources=live,
            candidate_sha=_SHA,
            candidate_tree=_TREE,
            mutation_epoch=3,
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


def test_dry_run_ignores_only_legacy_last_applied_annotation() -> None:
    live = _live()
    for resource in live:
        metadata = resource["metadata"]
        assert isinstance(metadata, dict)
        metadata["annotations"] = {
            "kubectl.kubernetes.io/last-applied-configuration": "legacy",
            "loom.example/protected": "exact",
        }
    plan = build_manifest_ownership_adoption_plan(
        artifact=_artifact(),
        live_resources=live,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        mutation_epoch=3,
    )
    dry_run = copy.deepcopy(live)
    for resource in dry_run:
        metadata = resource["metadata"]
        assert isinstance(metadata, dict)
        metadata["annotations"] = {"loom.example/protected": "exact"}

    assert (
        len(
            verify_ownership_adoption_dry_run(
                plan,
                live_resources=live,
                dry_run_resources=dry_run,
            )
        )
        == 64
    )

    changed = copy.deepcopy(dry_run)
    changed_metadata = changed[0]["metadata"]
    assert isinstance(changed_metadata, dict)
    changed_metadata["annotations"] = {}
    with pytest.raises(ManifestOwnershipAdoptionError, match="change live state"):
        verify_ownership_adoption_dry_run(
            plan,
            live_resources=live,
            dry_run_resources=changed,
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
