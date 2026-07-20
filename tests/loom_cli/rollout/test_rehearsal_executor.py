from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    ProviderPricingDefault,
)
from loom_cli.rollout.rehearsal_action_source import (
    RehearsalPlan,
    RehearsalResources,
    RehearsalSmokeAuthority,
)
from loom_cli.rollout.rehearsal_browser import (
    BROWSER_JOB_NAME,
    BROWSER_REPORT_CHECK_IDS,
    build_rehearsal_browser_artifact,
)
from loom_cli.rollout.rehearsal_executor import IsolatedRehearsalExecutor, _default_stream_run
from loom_cli.rollout.rehearsal_release import RehearsalReleaseArtifact
from loom_cli.rollout.rehearsal_secret_restore import RehearsalSecretArtifact
from tests.loom_cli.rollout.rehearsal_fixtures import (
    PassingGB10RehearsalTransport,
    gb10_rehearsal_authority,
    passing_gb10_transport_factory,
)


def _plan() -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        cluster_name="loom-staging",
        checkpoint_request_id="req-abcdefgh",
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={
            "loom-control-plane": "sha256:" + "8" * 64,
            "loom-egress-xds": "sha256:" + "3" * 64,
            "loom-family-orchestrator": "sha256:" + "4" * 64,
            "loom-llm-gateway": "sha256:" + "5" * 64,
            "loom-rehearsal-postgres": "sha256:" + "9" * 64,
            "loom-service": "sha256:" + "1" * 64,
            "loom-staging-admin-browser-smoke": "sha256:" + "6" * 64,
            "loom-web": "sha256:" + "2" * 64,
            "loom-worker": "sha256:" + "7" * 64,
        },
        image_tag="staging-aaaaaaaa",
        image_artifact_sha256="2" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/artifact.json"
        ),
        rendered_manifest_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/rendered.yaml"
        ),
        production_defaults_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/"
            + "6" * 64
            + "/production-defaults.json"
        ),
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256="8" * 64,
        production_defaults_sha256="9" * 64,
        migration_plan_sha256="3" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
        smoke_authority=RehearsalSmokeAuthority(
            represented_username="devansh",
            team_id="11111111-1111-4111-8111-111111111111",
            admin_actor="loom-staging-rollout",
            task_id="loom-smoke/gb10-oracle-hello-world",
            required_worker_pool="gb10-arm64",
            agent="oracle",
        ),
        gb10_authority=gb10_rehearsal_authority(),
    )


def _runtime_images(plan: RehearsalPlan, names: Sequence[str]) -> dict[str, tuple[str, ...]]:
    return {name: (plan.image_digests[name],) for name in names}


def _release_artifact(plan: RehearsalPlan) -> RehearsalReleaseArtifact:
    resources: list[dict[str, object]] = []
    selectors: dict[str, dict[str, str]] = {}
    images: dict[str, str] = {}
    for name in ("loom-control-plane", "loom-service", "loom-web"):
        selector = {"app": name}
        selectors[name] = selector
        images[name] = plan.image_digests[name]
        resources.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                    "name": name,
                    "namespace": plan.resources.namespace,
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": selector},
                    "template": {
                        "metadata": {"labels": selector},
                        "spec": {
                            "automountServiceAccountToken": False,
                            "containers": [
                                {
                                    "image": f"{name}:{plan.image_tag}",
                                    "name": name,
                                }
                            ],
                        },
                    },
                },
            }
        )
    for name in ("loom-control-plane", "loom-postgres", "loom-service", "loom-web"):
        resources.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                    "name": name,
                    "namespace": plan.resources.namespace,
                },
                "spec": {
                    "ports": [{"port": 5432 if name == "loom-postgres" else 80}],
                    "selector": {"app": name},
                    "type": "ClusterIP",
                },
            }
        )
    resources.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                "name": "loom-rehearsal-release",
                "namespace": plan.resources.namespace,
            },
            "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
        }
    )
    payload = yaml.safe_dump_all(resources, sort_keys=True).encode()
    return RehearsalReleaseArtifact(
        payload=payload,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        deployment_images=images,
        deployment_selectors=selectors,
        resource_count=len(resources),
    )


def _secret_artifact(plan: RehearsalPlan) -> RehearsalSecretArtifact:
    names = ("loom-admin-secret", "loom-secrets", "loom-staging-tls")
    payload = yaml.safe_dump_all(
        [
            {
                "apiVersion": "v1",
                "data": {"key": "dmFsdWU="},
                "kind": "Secret",
                "metadata": {
                    "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                    "name": name,
                    "namespace": plan.resources.namespace,
                },
                "type": "Opaque",
            }
            for name in names
        ],
        sort_keys=True,
    ).encode()
    return RehearsalSecretArtifact(
        payload=payload,
        secret_names=names,
        source_component_sha256="7" * 64,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def test_namespace_uses_fixed_scoped_apply_and_exact_readback() -> None:
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []
    records: dict[str, object] = {}

    def run(argv, payload, timeout):
        calls.append((tuple(argv), payload, timeout))
        if payload is not None:
            record = json.loads(payload)
            records[str(record["kind"])] = record
        elif "rolebinding" in argv:
            record = records["RoleBinding"]
        elif "networkpolicy" in argv:
            record = records["NetworkPolicy"]
        else:
            record = records["Namespace"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    plan = _plan()
    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.namespace", plan)

    assert outcome.passed
    assert len(calls) == 6
    assert calls[0][0] == (
        "kubectl",
        "--kubeconfig",
        "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig",
        "apply",
        "--server-side=true",
        "--field-manager=loom-staging-preflight",
        "--request-timeout=30s",
        "-f",
        "-",
        "-o",
        "json",
    )
    assert calls[1][0][3:6] == ("--namespace", plan.resources.namespace, "apply")
    assert calls[2][0][3:6] == ("--namespace", plan.resources.namespace, "apply")
    assert calls[3][0][3:6] == ("get", "namespace", plan.resources.namespace)
    assert calls[4][0][3:6] == ("--namespace", plan.resources.namespace, "get")
    assert calls[5][0][3:6] == ("--namespace", plan.resources.namespace, "get")
    namespace = json.loads(calls[0][1] or b"{}")
    assert namespace["metadata"]["annotations"]["loom.openai.dev/plan-sha256"] == plan.plan_digest
    assert namespace["metadata"]["labels"]["loom.openai.dev/authority"] == "staging-preflight"
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce-version"] == "latest"
    network_policy = json.loads(calls[1][1] or b"{}")
    assert network_policy["spec"] == {
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress"],
    }


def test_namespace_returns_normalized_blockers_without_command_output() -> None:
    plan = _plan()
    failed = IsolatedRehearsalExecutor(
        run=lambda argv, payload, timeout: subprocess.CompletedProcess(
            argv, 1, "", "token=must-not-leak"
        )
    ).execute("rehearsal.namespace", plan)
    assert failed.blockers == {"namespace": "apply-failed"}
    assert "token" not in str(failed)

    calls = 0

    def drift(argv, payload, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(argv, 0, (payload or b"{}").decode(), "")
        return subprocess.CompletedProcess(argv, 0, '{"apiVersion":"v1","kind":"Namespace"}', "")

    blocked = IsolatedRehearsalExecutor(run=drift).execute("rehearsal.namespace", plan)
    assert blocked.blockers == {"namespace": "network-policy-failed"}


def test_database_streams_exact_checkpoint_into_restricted_pod() -> None:
    plan = _plan()
    postgres_config = "sha256:" + "a" * 64
    postgres_manifest = "sha256:" + "b" * 64
    control_plane_config = "sha256:" + "c" * 64
    control_plane_manifest = "sha256:" + "d" * 64
    runtime_images = {
        "loom-rehearsal-postgres": (postgres_config, postgres_manifest),
        "loom-control-plane": (control_plane_config, control_plane_manifest),
    }
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []
    streams: list[tuple[tuple[str, ...], Path, int]] = []
    pod: dict[str, object] = {}
    restored = False

    def run(argv, payload, timeout):
        nonlocal pod
        calls.append((tuple(argv), payload, timeout))
        if argv[:3] == ("docker", "image", "inspect"):
            tag = argv[-1]
            name = tag.split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
        if argv[:3] == ("kind", "load", "docker-image"):
            return subprocess.CompletedProcess(argv, 0, "loaded\n", "")
        if payload is not None:
            pod = json.loads(payload)
            for container in pod["spec"]["containers"]:
                container["terminationMessagePath"] = "/dev/termination-log"
                container["terminationMessagePolicy"] = "File"
                if "readinessProbe" in container:
                    container["readinessProbe"]["successThreshold"] = 1
            return subprocess.CompletedProcess(argv, 0, json.dumps(pod), "")
        if "wait" in argv:
            return subprocess.CompletedProcess(argv, 0, "ready\n", "")
        if "get" in argv and "pod" in argv:
            observed = {
                **pod,
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "imageID": "docker.io/library/import-2026-07-20@" + postgres_manifest,
                            "name": "postgres",
                            "ready": True,
                        },
                        {
                            "imageID": "sha256:" + control_plane_config.removeprefix("sha256:"),
                            "name": "migration",
                            "ready": True,
                        },
                    ],
                },
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(observed), "")
        if "psql" in argv:
            command = next(item for item in argv if item.startswith("--command="))
            if "to_regclass" in command:
                record = {"database": plan.resources.database, "restored": restored}
            else:
                record = {"schema_revision": plan.schema_revision}
            return subprocess.CompletedProcess(argv, 0, json.dumps(record) + "\n", "")
        raise AssertionError(argv)

    def stream(argv, source, timeout):
        nonlocal restored
        streams.append((tuple(argv), source, timeout))
        restored = True
        return subprocess.CompletedProcess(argv, 0, "", "")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        stream_run=stream,
        runtime_image_resolver=lambda _plan, names: {name: runtime_images[name] for name in names},
    ).execute("rehearsal.db-clone", plan)

    assert outcome.passed
    assert outcome.details == {
        "database": plan.resources.database,
        "schema-revision": plan.schema_revision,
        "status": "restored",
    }
    assert len(streams) == 1
    assert streams[0][1] == plan.checkpoint_manifest_path.parent / "postgres" / "loom.dump"
    assert streams[0][2] == 1800
    manifest_call = next(call for call in calls if call[1] is not None)
    manifest = json.loads(manifest_call[1] or b"{}")
    for container in manifest["spec"]["containers"]:
        env_names = [item["name"] for item in container.get("env", [])]
        assert len(env_names) == len(set(env_names))
    assert manifest["spec"]["automountServiceAccountToken"] is False
    assert manifest["spec"]["containers"][0]["imagePullPolicy"] == "Never"
    assert manifest["spec"]["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    assert manifest["spec"]["containers"][1]["image"] == ("loom-control-plane:" + plan.image_tag)
    assert manifest["spec"]["containers"][1]["command"] == ["/bin/sleep", "infinity"]


def test_database_rejects_unclassified_server_default_drift() -> None:
    plan = _plan()

    def run(argv, payload, _timeout):
        if tuple(argv[:3]) == ("docker", "image", "inspect"):
            name = argv[-1].split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
        if tuple(argv[:3]) == ("kind", "load", "docker-image"):
            return subprocess.CompletedProcess(argv, 0, "loaded\n", "")
        if payload is not None:
            pod = json.loads(payload)
            for container in pod["spec"]["containers"]:
                container["terminationMessagePath"] = "/dev/termination-log"
                container["terminationMessagePolicy"] = "File"
                if "readinessProbe" in container:
                    container["readinessProbe"]["successThreshold"] = 1
            pod["spec"]["containers"][0]["unexpectedDefault"] = True
            return subprocess.CompletedProcess(argv, 0, json.dumps(pod), "")
        raise AssertionError("drifted apply response must stop before restore")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.db-clone", plan)
    assert outcome.blockers == {"database": "pod-apply-failed"}


def test_plan_rejects_missing_exact_image_before_executor() -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="identity"):
        RehearsalPlan.from_record(
            {
                **plan.to_record(),
                "image_digests": {"loom-service": "sha256:" + "1" * 64},
            }
        )


def test_runtime_image_binding_resolves_exact_index_platform_config() -> None:
    plan = _plan()
    name = "loom-control-plane"
    expected = plan.image_digests[name]
    reference = f"docker.io/library/{name}:{plan.image_tag}"
    manifest_digest = "sha256:" + "c" * 64
    config_digest = "sha256:" + "d" * 64

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        if "images" in command and "list" in command:
            value = (
                "REF TYPE DIGEST SIZE PLATFORMS LABELS\n"
                f"{reference} application/vnd.oci.image.index.v1+json "
                f"{expected} 1MiB linux/amd64 managed\n"
            )
        elif "content" in command and command[-1] == expected:
            value = json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.index.v1+json",
                    "manifests": [
                        {
                            "digest": manifest_digest,
                            "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "platform": {"architecture": "amd64", "os": "linux"},
                        },
                        {
                            "digest": "sha256:" + "e" * 64,
                            "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "platform": {"architecture": "unknown", "os": "unknown"},
                        },
                    ],
                }
            )
        elif "content" in command and command[-1] == manifest_digest:
            value = json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": {
                        "digest": config_digest,
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                    },
                }
            )
        elif "crictl" in command:
            value = json.dumps(
                {
                    "status": {
                        "id": config_digest,
                        "repoTags": [reference],
                        "repoDigests": [f"docker.io/library/import@{manifest_digest}"],
                    },
                    "info": {
                        "imageSpec": {
                            "architecture": "amd64",
                            "os": "linux",
                            "config": {
                                "Labels": {"org.opencontainers.image.revision": plan.candidate_sha}
                            },
                        }
                    },
                }
            )
        else:
            raise AssertionError(command)
        return subprocess.CompletedProcess(argv, 0, value, "")

    resolved = IsolatedRehearsalExecutor(run=run)._runtime_image_ids(plan, (name,))

    assert resolved == {name: (config_digest, manifest_digest)}

    def repo_digest_drift(argv, payload, timeout):
        result = run(argv, payload, timeout)
        if "crictl" not in tuple(argv):
            return result
        value = json.loads(result.stdout)
        value["status"]["repoDigests"] = ["docker.io/library/import@sha256:" + "f" * 64]
        return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")

    assert (
        IsolatedRehearsalExecutor(run=repo_digest_drift)._runtime_image_ids(plan, (name,)) is None
    )


def test_runtime_image_binding_rejects_kind_tag_drift() -> None:
    plan = _plan()
    name = "loom-control-plane"
    reference = f"docker.io/library/{name}:{plan.image_tag}"

    def run(argv, _payload, _timeout):
        value = (
            "REF TYPE DIGEST SIZE PLATFORMS LABELS\n"
            f"{reference} application/vnd.oci.image.index.v1+json "
            f"sha256:{'0' * 64} 1MiB linux/amd64 managed\n"
        )
        return subprocess.CompletedProcess(argv, 0, value, "")

    assert IsolatedRehearsalExecutor(run=run)._runtime_image_ids(plan, (name,)) is None


def test_migration_runs_exact_candidate_against_restored_database() -> None:
    plan = _plan()
    revision = plan.schema_revision
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        nonlocal revision
        command = tuple(argv)
        calls.append(command)
        if "psql" in command:
            sql = next(item for item in command if item.startswith("--command="))
            record = (
                {"database": plan.resources.database, "restored": True}
                if "to_regclass" in sql
                else {"schema_revision": revision}
            )
            return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")
        if "alembic" in command:
            revision = plan.migration_target_revision
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)

    assert outcome.passed
    assert outcome.details == {
        "plan-sha256": plan.migration_plan_sha256,
        "schema-revision": plan.migration_target_revision,
        "status": "migrated",
    }
    migration = next(command for command in calls if "alembic" in command)
    assert "--container" in migration
    assert migration[migration.index("--container") + 1] == "migration"
    db_url = next(item for item in migration if item.startswith("LOOM_DB_URL="))
    assert plan.resources.database in db_url
    assert "loom-staging" not in db_url


def test_migration_is_idempotent_and_rejects_unexpected_baseline() -> None:
    plan = _plan()
    revision = plan.migration_target_revision
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if "psql" not in command:
            raise AssertionError(argv)
        sql = next(item for item in command if item.startswith("--command="))
        record = (
            {"database": plan.resources.database, "restored": True}
            if "to_regclass" in sql
            else {"schema_revision": revision}
        )
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)
    assert outcome.passed
    assert all("alembic" not in command for command in calls)

    revision = "unexpected"
    blocked = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)
    assert blocked.blockers == {"migration": "database-baseline-drift"}

    record = plan.to_record()
    record["migration_target_revision"] = "head"
    with pytest.raises(ValueError, match="identity"):
        RehearsalPlan.from_record(record)


def test_release_loads_exact_images_and_verifies_all_scoped_resources() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    secrets = _secret_artifact(plan)
    resources = {
        (item["kind"], item["metadata"]["name"]): item
        for item in yaml.safe_load_all(release.payload)
    }
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("docker", "image", "inspect"):
            name = command[-1].split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
        if command[:3] == ("kind", "load", "docker-image"):
            return subprocess.CompletedProcess(argv, 0, "loaded\n", "")
        if "apply" in command:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if "secret" in command and "get" in command:
            name = command[command.index("secret") + 1]
            return subprocess.CompletedProcess(argv, 0, f"{name}\t{plan.plan_digest}\n", "")
        if "rollout" in command:
            return subprocess.CompletedProcess(argv, 0, "ready\n", "")
        if "deployment" in command and "get" in command:
            name = command[command.index("deployment") + 1]
            resource = json.loads(json.dumps(resources[("Deployment", name)]))
            resource["metadata"]["generation"] = 2
            resource["status"] = {
                "availableReplicas": 1,
                "observedGeneration": 2,
                "readyReplicas": 1,
                "replicas": 1,
                "updatedReplicas": 1,
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(resource), "")
        if "pods" in command and "get" in command:
            selector = next(item for item in command if item.startswith("--selector="))
            name = selector.rsplit("=", 1)[1]
            pod = {
                "items": [
                    {
                        "metadata": {"labels": {"app": name}},
                        "status": {
                            "conditions": [{"status": "True", "type": "Ready"}],
                            "containerStatuses": [
                                {
                                    "imageID": (
                                        f"docker-pullable://{name}@" + plan.image_digests[name]
                                    ),
                                    "name": name,
                                    "ready": True,
                                }
                            ],
                            "phase": "Running",
                        },
                    }
                ]
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(pod), "")
        if "service" in command and "get" in command:
            name = command[command.index("service") + 1]
            resource = resources[("Service", name)]
            return subprocess.CompletedProcess(argv, 0, json.dumps(resource), "")
        if "networkpolicy" in command and "get" in command:
            resource = resources[("NetworkPolicy", "loom-rehearsal-release")]
            return subprocess.CompletedProcess(argv, 0, json.dumps(resource), "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        secret_artifacts=lambda _plan: secrets,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.release", plan)

    assert outcome.passed
    assert outcome.details == {
        "manifest-sha256": release.artifact_sha256,
        "secret-artifact-sha256": secrets.artifact_sha256,
        "status": "ready",
    }
    kind_load = next(command for command, _payload in calls if command[:2] == ("kind", "load"))
    assert kind_load[-2:] == ("--name", plan.cluster_name)
    assert set(kind_load[3:-2]) == {
        "loom-service:" + plan.image_tag,
        "loom-web:" + plan.image_tag,
    }
    secret_apply = next(payload for _command, payload in calls if payload == secrets.payload)
    assert secret_apply == secrets.payload


def test_release_refuses_local_image_drift_before_kubernetes_mutation() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    secrets = _secret_artifact(plan)
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return subprocess.CompletedProcess(argv, 0, "sha256:" + "0" * 64 + "\n", "")
        raise AssertionError("drifted images must fail before kind or Kubernetes")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
        secret_artifacts=lambda _plan: secrets,
    ).execute("rehearsal.release", plan)

    assert outcome.blockers == {"release": "image-load-failed"}
    assert all(command[0] == "docker" for command in calls)


def test_api_smoke_executes_fixed_probe_in_exact_service_pod() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    batch_id = "11111111-1111-4111-8111-111111111111"
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if "get" in command and "pods" in command:
            pod = {
                "items": [
                    {
                        "metadata": {
                            "annotations": {
                                "loom.openai.dev/plan-sha256": plan.plan_digest,
                            },
                            "labels": {"app": "loom-service"},
                            "name": "loom-service-abc123",
                        },
                        "status": {
                            "phase": "Running",
                            "conditions": [{"type": "Ready", "status": "True"}],
                            "containerStatuses": [
                                {
                                    "imageID": "docker-pullable://loom-service@"
                                    + plan.image_digests["loom-service"],
                                    "name": "loom-service",
                                    "ready": True,
                                }
                            ],
                        },
                    }
                ]
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(pod), "")
        if "exec" in command:
            evidence = {
                "get:/api/v1/auth/whoami": "1" * 64,
                "get:/api/v1/batches": "2" * 64,
                "get:/api/v1/batches/exact": "3" * 64,
                "get:/api/v1/benchmarks": "4" * 64,
                "get:/api/v1/health": "5" * 64,
                "get:/api/v1/tasks/exact": "6" * 64,
                "post:/api/v1/admin/batches/on-behalf": "7" * 64,
            }
            value = {
                "batch_id": batch_id,
                "batch_name": "rehearsal-" + "5" * 24,
                "evidence": evidence,
                "persisted": True,
                "plan_sha256": plan.plan_digest,
                "recovered": False,
                "schema_version": 1,
                "status": "ready",
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)

    assert outcome.passed
    assert outcome.details["batch-id"] == batch_id
    probe = next(command for command in calls if "exec" in command)
    assert probe[:7] == (
        "kubectl",
        "--kubeconfig",
        "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig",
        "--namespace",
        plan.resources.namespace,
        "exec",
        "pod/loom-service-abc123",
    )
    assert probe[probe.index("--") + 1 : probe.index("--") + 4] == (
        "python",
        "-m",
        "loom_cli.rollout.rehearsal_smoke_probe",
    )
    assert "loom_admin_" not in " ".join(probe)


def test_production_defaults_streams_exact_artifact_to_candidate_probe(tmp_path: Path) -> None:
    base = _plan()
    provider = ProviderPricingDefault(
        name="hosted-openai",
        pricing_source="tokens-only",
        rate_card_provider=None,
        required=True,
    )
    artifact_payload = {
        "schema_version": 1,
        "candidate_sha": base.candidate_sha,
        "candidate_tree": base.candidate_tree,
        "environment": "staging",
        "yibuapi_sync": None,
        "providers": [provider.to_dict()],
    }
    digest = hashlib.sha256(
        json.dumps(artifact_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    artifact = ProductionDefaultsArtifact(
        schema_version=1,
        candidate_sha=base.candidate_sha,
        candidate_tree=base.candidate_tree,
        environment="staging",
        yibuapi_sync=None,
        providers=(provider,),
        artifact_digest=digest,
    )
    root = tmp_path / "preflight-artifacts" / base.artifact_bundle_sha256
    root.mkdir(parents=True)
    path = root / "production-defaults.json"
    path.write_bytes(artifact.to_bytes())
    path.chmod(0o600)
    plan = replace(
        base,
        artifact_descriptor_path=root / "artifact.json",
        rendered_manifest_path=root / "rendered.yaml",
        production_defaults_path=path,
        production_defaults_sha256=digest,
    )
    release = _release_artifact(plan)
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        calls.append((command, payload))
        if "get" in command and "pods" in command:
            value = {
                "items": [
                    {
                        "metadata": {
                            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                            "labels": {"app": "loom-service"},
                            "name": "loom-service-abc123",
                        },
                        "status": {
                            "phase": "Running",
                            "conditions": [{"type": "Ready", "status": "True"}],
                            "containerStatuses": [
                                {
                                    "imageID": "docker-pullable://loom-service@"
                                    + plan.image_digests["loom-service"],
                                    "name": "loom-service",
                                    "ready": True,
                                }
                            ],
                        },
                    }
                ]
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        if "exec" in command:
            value = {
                "artifact_sha256": digest,
                "candidate_sha": plan.candidate_sha,
                "candidate_tree": plan.candidate_tree,
                "evidence_sha256": "e" * 64,
                "mutation_count": 1,
                "plan_sha256": plan.plan_digest,
                "schema_version": 1,
                "status": "ready",
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.production-defaults", plan)

    assert outcome.passed
    probe, payload = next(item for item in calls if "exec" in item[0])
    assert payload == artifact.to_bytes()
    assert "-i" in probe
    assert probe[probe.index("--") + 1 : probe.index("--") + 4] == (
        "python",
        "-m",
        "loom_cli.rollout.rehearsal_production_defaults_probe",
    )
    assert "loom_admin_" not in " ".join(probe)


def test_api_smoke_rejects_pod_or_probe_identity_drift() -> None:
    plan = _plan()
    release = _release_artifact(plan)

    def drifted_pod(argv, _payload, _timeout):
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "annotations": {"loom.openai.dev/plan-sha256": "0" * 64},
                                "labels": {"app": "loom-service"},
                                "name": "loom-service-abc123",
                            },
                            "status": {
                                "phase": "Running",
                                "conditions": [{"type": "Ready", "status": "True"}],
                                "containerStatuses": [
                                    {
                                        "imageID": plan.image_digests["loom-service"],
                                        "name": "loom-service",
                                        "ready": True,
                                    }
                                ],
                            },
                        }
                    ]
                }
            ),
            "",
        )

    outcome = IsolatedRehearsalExecutor(
        run=drifted_pod,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)
    assert outcome.blockers == {"api-smoke": "service-pod-readback-drift"}


def test_systemd_launch_uses_exact_isolated_transient_unit_and_budget() -> None:
    plan = _plan()
    calls: list[tuple[str, ...]] = []
    active = False

    def run(argv, _payload, _timeout):
        nonlocal active
        command = tuple(argv)
        calls.append(command)
        if command[0] == "systemd-run":
            active = True
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "show"):
            if not active:
                return subprocess.CompletedProcess(argv, 4, "", "unit absent")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = "\n".join(
                (
                    "LoadState=loaded",
                    "ActiveState=active",
                    "SubState=exited",
                    "Type=oneshot",
                    "Result=success",
                    "ExecMainStatus=0",
                    "NeedDaemonReload=no",
                    "Transient=yes",
                    f"Description={description}",
                )
            )
            return subprocess.CompletedProcess(argv, 0, output + "\n", "")
        raise AssertionError(argv)

    clock = iter((10.0, 10.125))
    outcome = IsolatedRehearsalExecutor(
        run=run,
        monotonic=lambda: next(clock),
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.systemd-launch", plan)

    assert outcome.passed
    assert outcome.details == {
        "latency-ms": "125",
        "gb10-evidence-sha256": PassingGB10RehearsalTransport._evidence().evidence_digest,
        "gb10-host-count": "14",
        "status": "active",
        "unit": plan.resources.systemd_unit,
    }
    activation = next(call for call in calls if call[0] == "systemd-run")
    assert f"--unit={plan.resources.systemd_unit}" in activation
    assert "loom-staging-rollout.service" not in activation
    assert activation[-2:] == ("--", "/usr/bin/true")


def test_systemd_launch_rejects_existing_drift_and_latency_overrun() -> None:
    plan = _plan()

    def drift(argv, _payload, _timeout):
        if argv[0] == "systemctl":
            return subprocess.CompletedProcess(argv, 0, "LoadState=loaded\n", "")
        raise AssertionError("drifted unit must not be replaced")

    outcome = IsolatedRehearsalExecutor(run=drift).execute("rehearsal.systemd-launch", plan)
    assert outcome.blockers == {"systemd": "existing-unit-drift"}

    active = False

    def slow(argv, _payload, _timeout):
        nonlocal active
        if argv[0] == "systemd-run":
            active = True
            return subprocess.CompletedProcess(argv, 0, "", "")
        if not active:
            return subprocess.CompletedProcess(argv, 4, "", "")
        description = f"Loom isolated rehearsal {plan.plan_digest}"
        output = (
            "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
            "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
            f"Description={description}\n"
        )
        return subprocess.CompletedProcess(argv, 0, output, "")

    clock = iter((1.0, 6.001))
    outcome = IsolatedRehearsalExecutor(
        run=slow,
        monotonic=lambda: next(clock),
    ).execute("rehearsal.systemd-launch", plan)
    assert outcome.blockers == {"systemd": "activation-readback-drift"}


def test_cleanup_deletes_only_exact_unit_and_namespace_with_preconditions() -> None:
    plan = _plan()
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            **json.loads(
                json.dumps(
                    {
                        "annotations": {
                            "loom.openai.dev/candidate-sha": plan.candidate_sha,
                            "loom.openai.dev/candidate-tree": plan.candidate_tree,
                            "loom.openai.dev/mutation-epoch": str(plan.mutation_epoch),
                            "loom.openai.dev/plan-sha256": plan.plan_digest,
                        },
                        "labels": {
                            "loom.openai.dev/authority": "staging-preflight",
                            "loom.openai.dev/isolation": plan.resources.namespace.removeprefix(
                                "loom-rehearsal-"
                            ),
                            "pod-security.kubernetes.io/audit": "restricted",
                            "pod-security.kubernetes.io/enforce": "restricted",
                            "pod-security.kubernetes.io/enforce-version": "latest",
                            "pod-security.kubernetes.io/warn": "restricted",
                        },
                        "name": plan.resources.namespace,
                    }
                )
            ),
            "resourceVersion": "42",
            "uid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        },
    }
    unit_active = True
    namespace_present = True
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        nonlocal unit_active, namespace_present
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(
                    argv, 0, "loaded\n" if unit_active else "not-found\n", ""
                )
            if not unit_active:
                return subprocess.CompletedProcess(argv, 4, "", "")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = (
                "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
                "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
                f"Description={description}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output, "")
        if command[:3] == ("systemctl", "--user", "stop"):
            unit_active = False
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "reset-failed"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "get" in command and "namespace" in command:
            if not namespace_present:
                return subprocess.CompletedProcess(argv, 1, "", "not found")
            return subprocess.CompletedProcess(argv, 0, json.dumps(namespace), "")
        if "delete" in command and "--raw" in command:
            options = json.loads(payload or b"{}")
            assert options["preconditions"] == {
                "resourceVersion": "42",
                "uid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            }
            namespace_present = False
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        if "wait" in command:
            return subprocess.CompletedProcess(argv, 0, "deleted\n", "")
        raise AssertionError(argv)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.passed and outcome.cleanup_verified
    assert outcome.details["status"] == "absent"
    delete = next(command for command, _payload in calls if "--raw" in command)
    assert delete[-2:] == ("-f", "-")
    assert plan.resources.namespace in delete[-3]


def test_cleanup_refuses_unknown_unit_or_namespace_identity() -> None:
    plan = _plan()

    def unit_drift(argv, _payload, _timeout):
        if argv[:3] == ("systemctl", "--user", "show"):
            return subprocess.CompletedProcess(argv, 0, "LoadState=loaded\n", "")
        raise AssertionError("unknown unit must not be deleted")

    unit = IsolatedRehearsalExecutor(
        run=unit_drift,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)
    assert unit.blockers == {"cleanup": "systemd-identity-drift"}

    def namespace_drift(argv, _payload, _timeout):
        if argv[:3] == ("systemctl", "--user", "show"):
            if argv[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
            return subprocess.CompletedProcess(argv, 4, "", "")
        if "get" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"other"}}',
                "",
            )
        raise AssertionError("unknown namespace must not be deleted")

    namespace = IsolatedRehearsalExecutor(
        run=namespace_drift,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)
    assert namespace.blockers == {"cleanup": "namespace-identity-drift"}


def test_stream_runner_reads_private_file_without_following_parent_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    source = real / "loom.dump"
    source.write_bytes(b"exact-dump")
    source.chmod(0o600)
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    observed: list[bytes] = []

    def run(*_args, stdin, **_kwargs):
        observed.append(stdin.read())
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.rehearsal_executor.subprocess.run", run)

    result = _default_stream_run(("consumer",), source, 30)
    assert result.returncode == 0
    assert observed == [b"exact-dump"]
    with pytest.raises(OSError):
        _default_stream_run(("consumer",), linked / "loom.dump", 30)


def test_stream_runner_rejects_mode_and_read_time_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "loom.dump"
    source.write_bytes(b"exact-dump")
    source.chmod(0o644)
    with pytest.raises(RuntimeError, match="authority"):
        _default_stream_run(("consumer",), source, 30)

    source.chmod(0o600)

    def drift(*_args, stdin, **_kwargs):
        stdin.read()
        source.write_bytes(b"changed")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.rehearsal_executor.subprocess.run", drift)
    with pytest.raises(RuntimeError, match="changed"):
        _default_stream_run(("consumer",), source, 30)

    assert os.stat(source).st_mode & 0o777 == 0o600


def test_browser_executes_exact_isolated_job_and_validates_report() -> None:
    plan = replace(
        _plan(),
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://yylx.world/dev",
        ),
    )
    artifact = build_rehearsal_browser_artifact(plan, ingress_ip="10.96.12.34")
    resources = {
        (item["kind"], item["metadata"]["name"]): item
        for item in yaml.safe_load_all(artifact.payload)
    }
    completed_job = deepcopy(resources[("Job", BROWSER_JOB_NAME)])
    completed_job["status"] = {
        "conditions": [{"status": "True", "type": "Complete"}],
        "succeeded": 1,
    }
    image_status = {
        "imageID": "docker-pullable://exact@" + artifact.browser_image_digest,
        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}},
    }
    pods = {
        "items": [
            {
                "metadata": {
                    "labels": {
                        "job-name": BROWSER_JOB_NAME,
                        "loom.openai.dev/plan-sha256": plan.plan_digest,
                    }
                },
                "status": {
                    "containerStatuses": [{"name": "browser", **image_status}],
                    "initContainerStatuses": [{"name": "prepare-token", **image_status}],
                    "phase": "Succeeded",
                },
            }
        ]
    }
    report = {
        "schema_version": 4,
        "status": "pass",
        "deployment_identity": {
            "expected_deployed_sha": plan.candidate_sha,
            "observed_deployed_sha": plan.candidate_sha,
            "matched": True,
        },
        "route": plan.resources.route,
        "request_id": "rehearsal-" + "5" * 24,
        "rehearsal_binding": {
            "plan_sha256": plan.plan_digest,
            "isolation_id": "5" * 24,
            "resolved_sha": plan.candidate_sha,
        },
        "target": {"username": "qianyi", "user_id": "user-qianyi"},
        "audit_event_id": "audit-event",
        "browser": {"name": "chromium", "version": "1.2.3"},
        "checks": {name: True for name in BROWSER_REPORT_CHECK_IDS},
        "cleanup": {"logout_status": 204, "auth_me_after_logout_status": 401},
        "failure_code": None,
    }
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []

    def run(argv, payload, timeout):
        calls.append((tuple(argv), payload, timeout))
        if argv[:3] == ("docker", "image", "inspect"):
            return subprocess.CompletedProcess(argv, 0, artifact.browser_image_digest + "\n", "")
        if argv[:3] == ("kind", "load", "docker-image") or "apply" in argv or "wait" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "logs" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps(report), "")
        if "pods" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps(pods), "")
        if "service" in argv and "ingress-nginx-controller" in argv:
            value = {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": "ingress-nginx-controller",
                    "namespace": "ingress-nginx",
                },
                "spec": {"clusterIP": "10.96.12.34", "type": "ClusterIP"},
            }
        elif "ingress" in argv:
            value = resources[("Ingress", "loom-rehearsal-browser")]
        elif "networkpolicy" in argv:
            value = resources[("NetworkPolicy", "loom-rehearsal-browser")]
        elif "job" in argv:
            value = (
                completed_job
                if len([call for call in calls if "job" in call[0]]) > 1
                else resources[("Job", BROWSER_JOB_NAME)]
            )
        else:
            raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        browser_artifacts=lambda _plan, _ip: artifact,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.browser", plan)

    assert outcome.passed
    assert outcome.details["status"] == "ready"
    assert len(outcome.details["browser-report-sha256"]) == 64
    assert any("--selector=job-name=loom-rehearsal-browser" in call[0] for call in calls)


def test_browser_fails_closed_before_mutation_when_ingress_readback_drifts() -> None:
    plan = replace(
        _plan(),
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://yylx.world/dev",
        ),
    )
    calls: list[tuple[str, ...]] = []

    def run(argv, payload, timeout):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "{}", "")

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.browser", plan)

    assert outcome.blockers == {"browser": "ingress-controller-readback-failed"}
    assert len(calls) == 1
    assert "apply" not in calls[0]
