from __future__ import annotations

import hashlib
import json
import os
import subprocess
import uuid
from collections import Counter
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import replace
from functools import cache
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

import loom_cli.rollout.operator.protected_external_supervisor_transport as transport_module
from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import CAPACITY_SOURCE
from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPredecessorAuthority,
)
from loom_cli.rollout.external_supervisor_readiness import (
    REHEARSAL_KUBECONFIG as EXTERNAL_SUPERVISOR_REHEARSAL_KUBECONFIG,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    staging_working_directory,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    ExternalSupervisorLiveObservation,
    ServiceRuntimeStatus,
    TimerRuntimeStatus,
)
from loom_cli.rollout.operator.protected_gb10_external_supervisor_transport import (
    GB10_CONTROLLER_UNIT_DIR,
)
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
from loom_cli.rollout.rehearsal_executor import (
    IsolatedRehearsalExecutor,
    _api_smoke_failure,
    _database_pod_manifest,
    _default_external_supervisor_artifact,
    _default_stream_run,
    _external_supervisor_validation_expected_properties,
    _external_supervisor_validation_unit,
    _namespace_manifest,
)
from loom_cli.rollout.rehearsal_release import RehearsalReleaseArtifact
from loom_cli.rollout.rehearsal_secret_restore import RehearsalSecretArtifact
from tests.loom_cli.rollout.rehearsal_fixtures import (
    PassingGB10RehearsalTransport,
    active_external_supervisor_artifact,
    active_staging_profile_text,
    gb10_rehearsal_authority,
    passing_gb10_transport_factory,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


@cache
def _external_supervisor_artifact() -> ExternalSupervisorArtifact:
    return active_external_supervisor_artifact(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaaa",
    )


def _external_supervisor_profile() -> bytes:
    payload = active_staging_profile_text().encode()
    assert hashlib.sha256(payload).hexdigest() == _external_supervisor_artifact().profile_sha256
    return payload


def _external_policy_seed_result(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "candidate_sha": plan.candidate_sha,
        "candidate_tree": plan.candidate_tree,
        "evidence_sha256": "e" * 64,
        "image_tag": plan.image_tag,
        "plan_sha256": plan.plan_digest,
        "policy_count": 2,
        "profile_sha256": plan.external_supervisor_profile_sha256,
        "schema_version": 1,
        "status": "ready",
    }


def _absent_external_supervisor_observation(
    artifact: ExternalSupervisorArtifact,
) -> ExternalSupervisorLiveObservation:
    return ExternalSupervisorLiveObservation(
        unit_payloads={
            name: None
            for supervisor in artifact.supervisors
            for name in (supervisor.service_name, supervisor.timer_name)
        },
        timer_statuses={
            supervisor.timer_name: TimerRuntimeStatus(
                load_state="not-found",
                unit_file_state="not-found",
                active_state="inactive",
                fragment_path="",
                need_daemon_reload="no",
            )
            for supervisor in artifact.supervisors
        },
        service_statuses={
            supervisor.service_name: ServiceRuntimeStatus(
                load_state="not-found",
                result="",
                exec_main_status=None,
                fragment_path="",
                need_daemon_reload="no",
            )
            for supervisor in artifact.supervisors
        },
        predecessor_authority=ExternalSupervisorPredecessorAuthority(
            kind="absent",
            authority_digest=ABSENT_PREDECESSOR_DIGEST,
            unit_sha256={},
        ),
    )


def _repairable_gb10_external_supervisor_observation(
    artifact: ExternalSupervisorArtifact,
) -> ExternalSupervisorLiveObservation:
    predecessor_artifact = active_external_supervisor_artifact(
        candidate_sha="1" * 40,
        candidate_tree="2" * 40,
        image_tag="staging-1111111",
    ).for_execution_host("gx10-01c7")
    predecessor = ExternalSupervisorCanonicalIdentity.build(
        predecessor_artifact,
        plan_digest="3" * 64,
        attestation_digest="4" * 64,
        transition_group_id="5" * 32,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(
            predecessor_artifact,
            unit_dir=GB10_CONTROLLER_UNIT_DIR,
        ),
        unit_dir=str(GB10_CONTROLLER_UNIT_DIR),
    )
    observation = ExternalSupervisorLiveObservation(
        unit_payloads={
            name: payload.encode() for name, payload in predecessor.unit_payloads.items()
        },
        timer_statuses={
            supervisor.timer_name: TimerRuntimeStatus(
                load_state="loaded",
                unit_file_state="enabled",
                active_state="active",
                fragment_path=str(GB10_CONTROLLER_UNIT_DIR / supervisor.timer_name),
                need_daemon_reload="no",
            )
            for supervisor in predecessor_artifact.supervisors
        },
        service_statuses={
            supervisor.service_name: ServiceRuntimeStatus(
                load_state="loaded",
                result="exit-code",
                exec_main_status=1,
                fragment_path=str(GB10_CONTROLLER_UNIT_DIR / supervisor.service_name),
                need_daemon_reload="no",
            )
            for supervisor in predecessor_artifact.supervisors
        },
        canonical_identity=predecessor,
    )
    assert (
        transport_module.classify_external_supervisor_live_state(
            artifact,
            observation,
            unit_dir=GB10_CONTROLLER_UNIT_DIR,
        )
        == "repairable"
    )
    return observation


def _plan() -> RehearsalPlan:
    supervisor = _external_supervisor_artifact()
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
            "loom-execution-actuator": "sha256:" + "b" * 64,
            "loom-execution-runtime": "sha256:" + "c" * 64,
            "loom-family-orchestrator": "sha256:" + "4" * 64,
            "loom-pipeline-orchestrator": "sha256:" + "a" * 64,
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
        external_supervisor_artifact_sha256=supervisor.artifact_digest,
        external_supervisor_profile_sha256=supervisor.profile_sha256,
        external_supervisor_script_sha256=supervisor.script_sha256,
        external_supervisor_unit_sha256=supervisor.unit_sha256,
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
            required_worker_pool="gb10",
            agent="oracle",
        ),
        gb10_authority=gb10_rehearsal_authority(),
    )


def _api_smoke_seed_value(command: tuple[str, ...], plan: RehearsalPlan) -> dict[str, object]:
    sql = next(item.removeprefix("--command=") for item in command if item.startswith("--command="))
    if "staging_lifecycle_capacity" in sql:
        capacity = StagingCapacity(
            object_count=12,
            bytes_used=34,
            disk_free_percent=80,
            inode_free_percent=90,
        )
        value: dict[str, object] = {
            "bytes_used": capacity.bytes_used,
            "disk_free_percent": capacity.disk_free_percent,
            "evidence_sha256": capacity.evidence_digest,
            "inode_free_percent": capacity.inode_free_percent,
            "namespace": "loom-staging",
            "object_count": capacity.object_count,
            "policy_sha256": staging_capacity_policy_digest(),
            "source": CAPACITY_SOURCE,
        }
        if "WITH refreshed AS" in sql:
            value.update(
                {
                    "admission_allowed": True,
                    "fresh": True,
                    "namespace": plan.resources.namespace,
                    "status": "ready",
                }
            )
        return value
    return {
        "backend": "docker",
        "fresh": True,
        "hostname": "rehearsal-" + "5" * 24,
        "pool_name": "gb10",
        "status": "ready",
        "worker_id": str(uuid.UUID(hex=plan.plan_digest[:32], version=4)),
    }


def _runtime_images(plan: RehearsalPlan, names: Sequence[str]) -> dict[str, tuple[str, ...]]:
    return {name: (plan.image_digests[name],) for name in names}


def _release_artifact(plan: RehearsalPlan) -> RehearsalReleaseArtifact:
    resources: list[dict[str, object]] = []
    selectors: dict[str, dict[str, str]] = {}
    images: dict[str, str] = {}
    for name in ("loom-control-plane", "loom-llm-gateway", "loom-service", "loom-web"):
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
    for name in (
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-postgres",
        "loom-service",
        "loom-web",
    ):
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
    postgres_import = "sha256:" + "e" * 64
    control_plane_config = "sha256:" + "c" * 64
    control_plane_manifest = "sha256:" + "d" * 64
    runtime_images = {
        "loom-rehearsal-postgres": (postgres_config, postgres_manifest, postgres_import),
        "loom-control-plane": (control_plane_config, control_plane_manifest),
    }
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []
    streams: list[tuple[tuple[str, ...], Path, int]] = []
    pod: dict[str, object] = {}
    restored = False
    staged = False

    def run(argv, payload, timeout):
        nonlocal pod, restored
        calls.append((tuple(argv), payload, timeout))
        if argv[:3] == ("docker", "image", "inspect"):
            tag = argv[-1]
            name = tag.split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
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
        if "sha256sum" in argv:
            assert staged
            return subprocess.CompletedProcess(
                argv,
                0,
                plan.db_snapshot_identity.removeprefix("pgdump-sha256:")
                + "  /var/lib/postgresql/data/loom-rehearsal.dump\n",
                "",
            )
        if "pg_restore" in argv:
            assert staged
            restored = True
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "rm" in argv:
            assert staged
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "get" in argv and "pod" in argv:
            observed = {
                **pod,
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "imageID": "sha256:" + postgres_config.removeprefix("sha256:"),
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
            assert "--username=loom_rehearsal" in argv
            command = next(item for item in argv if item.startswith("--command="))
            if "to_regclass" in command:
                record = {"database": plan.resources.database, "restored": restored}
            else:
                record = {"schema_revision": plan.schema_revision}
            return subprocess.CompletedProcess(argv, 0, json.dumps(record) + "\n", "")
        raise AssertionError(argv)

    def stream(argv, source, timeout):
        nonlocal staged
        streams.append((tuple(argv), source, timeout))
        staged = True
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
    assert streams[0][2] == 600
    assert streams[0][0][-3:] == (
        "tee",
        "--",
        "/var/lib/postgresql/data/loom-rehearsal.dump",
    )
    restore_call = next(call[0] for call in calls if "pg_restore" in call[0])
    assert "--jobs=4" in restore_call
    assert "--username=loom_rehearsal" in restore_call
    assert restore_call[-1] == "/var/lib/postgresql/data/loom-rehearsal.dump"
    remove_call = next(call[0] for call in calls if "rm" in call[0])
    assert remove_call[-4:] == (
        "rm",
        "-f",
        "--",
        "/var/lib/postgresql/data/loom-rehearsal.dump",
    )
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


@pytest.mark.parametrize(
    (
        "transfer_succeeds",
        "digest_matches",
        "restore_succeeds",
        "remove_succeeds",
        "expected_blocker",
    ),
    [
        (False, True, True, True, "restore-transfer-failed"),
        (False, True, True, False, "restore-staging-cleanup-failed"),
        (True, False, True, True, "restore-staging-verification-failed"),
        (True, False, True, False, "restore-staging-cleanup-failed"),
        (True, True, True, False, "restore-staging-cleanup-failed"),
        (True, True, False, True, "restore-command-failed"),
        (True, True, False, False, "restore-staging-cleanup-failed"),
    ],
)
def test_database_staged_restore_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    transfer_succeeds: bool,
    digest_matches: bool,
    restore_succeeds: bool,
    remove_succeeds: bool,
    expected_blocker: str,
) -> None:
    plan = _plan()
    commands: list[tuple[str, ...]] = []
    restore_called = False

    executor = IsolatedRehearsalExecutor(
        stream_run=lambda _argv, _source, _timeout: subprocess.CompletedProcess(
            (), 0 if transfer_succeeds else 1, "", ""
        )
    )
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_load_images",
        lambda _self, _plan, _names: True,
    )
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_runtime_image_ids",
        lambda _self, _plan, names: {name: (plan.image_digests[name],) for name in names},
    )
    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_executor._database_pod_matches",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(IsolatedRehearsalExecutor, "_command", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_database_identity",
        lambda _self, _plan: None,
    )

    def text_command(*_args, **_kwargs):
        digest = (
            plan.db_snapshot_identity.removeprefix("pgdump-sha256:") if digest_matches else "0" * 64
        )
        return f"{digest}  /var/lib/postgresql/data/loom-rehearsal.dump\n"

    monkeypatch.setattr(IsolatedRehearsalExecutor, "_text_command", text_command)

    def status(_self, argv, **_kwargs):
        nonlocal restore_called
        command = tuple(argv)
        commands.append(command)
        if "pg_restore" in command:
            restore_called = True
            return restore_succeeds
        if "rm" in command:
            return remove_succeeds
        return True

    monkeypatch.setattr(IsolatedRehearsalExecutor, "_status", status)

    outcome = executor.execute("rehearsal.db-clone", plan)

    assert outcome.blockers == {"database": expected_blocker}
    assert restore_called is (transfer_succeeds and digest_matches)
    assert any("rm" in command for command in commands)


@pytest.mark.parametrize(
    ("failure", "expected_blocker"),
    [
        (
            subprocess.TimeoutExpired(cmd=("kubectl", "exec"), timeout=600),
            "restore-transfer-timeout",
        ),
        (OSError("bounded transport failure"), "restore-transfer-failed"),
    ],
)
def test_database_staged_restore_classifies_transfer_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    expected_blocker: str,
) -> None:
    plan = _plan()
    commands: list[tuple[str, ...]] = []

    def fail_stream(_argv, _source, timeout):
        assert timeout == 600
        raise failure

    executor = IsolatedRehearsalExecutor(stream_run=fail_stream)
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_load_images",
        lambda _self, _plan, _names: True,
    )
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_runtime_image_ids",
        lambda _self, _plan, names: {name: (plan.image_digests[name],) for name in names},
    )
    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_executor._database_pod_matches",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(IsolatedRehearsalExecutor, "_command", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        IsolatedRehearsalExecutor,
        "_database_identity",
        lambda _self, _plan: None,
    )

    def status(_self, argv, **_kwargs):
        commands.append(tuple(argv))
        return True

    monkeypatch.setattr(IsolatedRehearsalExecutor, "_status", status)

    outcome = executor.execute("rehearsal.db-clone", plan)

    assert outcome.blockers == {"database": expected_blocker}
    assert sum("rm" in command for command in commands) == 1
    assert not any("pg_restore" in command for command in commands)


def test_plan_rejects_non_sha256_database_snapshot_identity() -> None:
    with pytest.raises(ValueError, match="identity"):
        replace(_plan(), db_snapshot_identity="pgdump-sha256:" + "z" * 64)


def test_database_rejects_unclassified_server_default_drift() -> None:
    plan = _plan()

    def run(argv, payload, _timeout):
        if tuple(argv[:3]) == ("docker", "image", "inspect"):
            name = argv[-1].split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
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


def test_database_pod_manifest_uses_bounded_termination_grace() -> None:
    manifest = _database_pod_manifest(_plan())

    assert manifest["spec"]["terminationGracePeriodSeconds"] == 10


def test_registry_rehearsal_uses_preflight_published_digest_without_republish() -> None:
    manifest_digests = {
        name: f"sha256:{hashlib.sha256((name + '-manifest').encode()).hexdigest()}"
        for name in _plan().image_digests
    }
    plan = replace(
        _plan(),
        container_registry="192.168.50.13:5000",
        container_registry_push="localhost:5000",
        registry_digests=manifest_digests,
    )
    name = "loom-control-plane"
    expected = plan.image_digests[name]
    manifest_digest = manifest_digests[name]
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return subprocess.CompletedProcess(argv, 0, expected + "\n", "")
        if command[:3] == ("docker", "manifest", "inspect"):
            value = {
                "Descriptor": {"digest": manifest_digest},
                "SchemaV2Manifest": {"config": {"digest": expected}},
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        raise AssertionError(command)

    executor = IsolatedRehearsalExecutor(run=run)
    assert executor._load_images(plan, (name,)) is True
    assert executor._runtime_image_ids(plan, (name,)) == {name: (expected, manifest_digest)}
    assert not any(command[:2] in {("docker", "tag"), ("docker", "push")} for command in calls)
    manifest = _database_pod_manifest(plan)
    containers = manifest["spec"]["containers"]
    assert containers[0]["image"] == (
        "192.168.50.13:5000/loom-rehearsal-postgres@" + manifest_digests["loom-rehearsal-postgres"]
    )
    assert all(container["imagePullPolicy"] == "IfNotPresent" for container in containers)


def test_registry_rehearsal_accepts_provenance_index() -> None:
    plan = replace(
        _plan(),
        container_registry="192.168.50.13:5000",
        container_registry_push="localhost:5000",
        registry_digests=_plan().image_digests,
    )
    name = "loom-control-plane"
    expected = plan.image_digests[name]
    workload_digest = "sha256:" + "a" * 64
    attestation_digest = "sha256:" + "b" * 64
    workload = {
        "digest": workload_digest,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "platform": {"architecture": "amd64", "os": "linux"},
    }
    attestation = {
        "annotations": {
            "vnd.docker.reference.digest": workload_digest,
            "vnd.docker.reference.type": "attestation-manifest",
        },
        "digest": attestation_digest,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "platform": {"architecture": "unknown", "os": "unknown"},
    }
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return subprocess.CompletedProcess(argv, 0, expected + "\n", "")
        if command[:3] == ("docker", "manifest", "inspect"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([{"Descriptor": workload}, {"Descriptor": attestation}]),
                "",
            )
        if command[:3] == ("docker", "buildx", "imagetools"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    {
                        "digest": expected,
                        "manifests": [workload, attestation],
                        "mediaType": "application/vnd.oci.image.index.v1+json",
                        "schemaVersion": 2,
                    }
                ),
                "",
            )
        raise AssertionError(command)

    executor = IsolatedRehearsalExecutor(run=run)
    assert executor._load_images(plan, (name,)) is True
    assert executor._runtime_image_ids(plan, (name,)) == {name: (expected, expected)}


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
    supervisor_artifact = _external_supervisor_artifact()
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
        if command[:3] == ("systemctl", "--user", "show"):
            assert command[-2:] == ("--property=LoadState", "--value")
            return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            assert payload == _external_supervisor_profile()
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
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
        external_supervisor_artifacts=lambda _plan: supervisor_artifact,
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=_absent_external_supervisor_observation,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.release", plan)

    assert outcome.passed
    assert outcome.details["manifest-sha256"] == release.artifact_sha256
    assert outcome.details["secret-artifact-sha256"] == secrets.artifact_sha256
    assert outcome.details["status"] == "ready"
    validation_digest = outcome.details["external-supervisor-validation-sha256"]
    assert len(validation_digest) == 64
    assert set(validation_digest) <= set("0123456789abcdef")
    assert {item.execution_host for item in supervisor_artifact.supervisors} == {
        "TRT-EAI-OLDLAB-1",
        "gx10-01c7",
    }
    secret_apply = next(payload for _command, payload in calls if payload == secrets.payload)
    assert secret_apply == secrets.payload
    secret_apply_index = next(
        index for index, (_command, payload) in enumerate(calls) if payload == secrets.payload
    )
    secret_readback_indexes = [
        index
        for index, (command, _payload) in enumerate(calls)
        if "get" in command and "secret" in command
    ]
    validation_calls = [
        (index, command)
        for index, (command, _payload) in enumerate(calls)
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect")
    ]
    validation_index = validation_calls[0][0]
    release_apply_index = next(
        index for index, (_command, payload) in enumerate(calls) if payload == release.payload
    )
    assert len(secret_readback_indexes) == len(secrets.secret_names)
    assert secret_apply_index < min(secret_readback_indexes)
    policy_seed_index, policy_seed_argv = next(
        (index, command)
        for index, (command, payload) in enumerate(calls)
        if payload == _external_supervisor_profile()
    )
    assert max(secret_readback_indexes) < policy_seed_index < validation_index < release_apply_index
    assert len(validation_calls) == 4
    assert policy_seed_argv[policy_seed_argv.index("--container") + 1] == "migration"
    assert (
        f"LOOM_DB_URL=postgresql+psycopg://loom_rehearsal@127.0.0.1:5432/{plan.resources.database}"
        in policy_seed_argv
    )

    working_directory = staging_working_directory(plan.candidate_sha)
    actual_validation_commands: Counter[tuple[str, ...]] = Counter()
    for _index, validation_argv in validation_calls:
        separator = validation_argv.index("--")
        assert validation_argv[:4] == ("systemd-run", "--user", "--wait", "--collect")
        assert f"--property=WorkingDirectory={working_directory}" in validation_argv
        assert (
            f"--property=Environment=PYTHONPATH={working_directory}/src PYTHONDONTWRITEBYTECODE=1"
        ) in validation_argv
        assert "--property=TimeoutStartSec=180s" in validation_argv
        assert "--property=RuntimeMaxSec=180s" in validation_argv
        assert "--property=KillMode=control-group" in validation_argv
        assert validation_argv[-1] == "--validate-only"
        actual_validation_commands[validation_argv[separator + 1 :]] += 1

    expected_validation_commands = Counter(
        supervisor_artifact.validation_argv(
            plan.resources.namespace,
            EXTERNAL_SUPERVISOR_REHEARSAL_KUBECONFIG,
        ).values()
    )
    assert actual_validation_commands == expected_validation_commands


def test_external_supervisor_validation_routes_gb10_controller_proof_remotely() -> None:
    plan = _plan()
    artifact = _external_supervisor_artifact()
    calls: list[tuple[tuple[str, ...], bytes | None]] = []
    controller_artifacts: list[ExternalSupervisorArtifact] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        calls.append((command, payload))
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            assert payload == _external_supervisor_profile()
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
        if command[0] == "kubectl" and "apply" in command:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if command[:3] == ("systemctl", "--user", "show"):
            return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(command)

    def observe_controller(
        controller_artifact: ExternalSupervisorArtifact,
    ) -> ExternalSupervisorLiveObservation:
        controller_artifacts.append(controller_artifact)
        return _absent_external_supervisor_observation(controller_artifact)

    digest, blocker = IsolatedRehearsalExecutor(
        run=run,
        external_supervisor_artifacts=lambda _plan: artifact,
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=observe_controller,
    )._validate_external_supervisors(plan)

    assert blocker is None
    assert digest is not None and len(digest) == 64
    assert len(controller_artifacts) == 1
    assert [item.name for item in controller_artifacts[0].supervisors] == [
        "gb10-staging",
        "task-image-builder-gb10-staging",
    ]
    validation_commands = [
        command[command.index("--") + 1 :]
        for command, _payload in calls
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect")
    ]
    assert len(validation_commands) == 4
    by_pool = {
        command[command.index("--pool-name") + 1]: command for command in validation_commands
    }
    assert set(by_pool) == {
        "gb10",
        "oldlab",
        "task-image-builder-gb10",
        "task-image-builder-oldlab",
    }
    supervisors = {item.pool_name: item for item in artifact.supervisors}
    assert by_pool["oldlab"][:2] == (
        supervisors["oldlab"].python_path,
        supervisors["oldlab"].script_path,
    )
    assert by_pool["task-image-builder-oldlab"][:2] == (
        supervisors["task-image-builder-oldlab"].python_path,
        supervisors["task-image-builder-oldlab"].script_path,
    )
    for pool_name in ("gb10", "task-image-builder-gb10"):
        assert by_pool[pool_name][:3] == (
            supervisors[pool_name].python_path,
            "-m",
            "loom_cli.rollout.rehearsal_external_supervisor_policy_probe",
        )
    assert all(
        command[:3]
        == (
            supervisors[pool_name].python_path,
            "-m",
            "loom_cli.rollout.rehearsal_external_supervisor_policy_probe",
        )
        for pool_name, command in by_pool.items()
        if pool_name in {"gb10", "task-image-builder-gb10"}
    )


def test_external_supervisor_validation_accepts_repairable_gb10_controller_proof() -> None:
    plan = _plan()
    artifact = _external_supervisor_artifact()
    validation_commands: list[tuple[str, ...]] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
        if command[0] == "kubectl" and "apply" in command:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if command[:3] == ("systemctl", "--user", "show"):
            return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect"):
            validation_commands.append(command)
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(command)

    digest, blocker = IsolatedRehearsalExecutor(
        run=run,
        external_supervisor_artifacts=lambda _plan: artifact,
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=(_repairable_gb10_external_supervisor_observation),
    )._validate_external_supervisors(plan)

    assert blocker is None
    assert digest is not None and len(digest) == 64
    assert len(validation_commands) == 4


def test_external_supervisor_validation_fails_closed_without_gb10_controller_proof() -> None:
    plan = _plan()
    artifact = _external_supervisor_artifact()

    def run(argv, payload, _timeout):
        command = tuple(argv)
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
        if command[0] == "kubectl" and "apply" in command:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        raise AssertionError("local validation must not start without controller proof")

    def unavailable(_artifact: ExternalSupervisorArtifact) -> ExternalSupervisorLiveObservation:
        raise RuntimeError("remote detail must remain secret")

    digest, blocker = IsolatedRehearsalExecutor(
        run=run,
        external_supervisor_artifacts=lambda _plan: artifact,
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=unavailable,
    )._validate_external_supervisors(plan)

    assert digest is None
    assert blocker == "external-supervisor-controller-proof-failed"


def test_external_supervisor_validation_rejects_drifted_gb10_controller_state() -> None:
    plan = _plan()
    artifact = _external_supervisor_artifact()

    def run(argv, payload, _timeout):
        command = tuple(argv)
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
        if command[0] == "kubectl" and "apply" in command:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        raise AssertionError("local validation must not start after controller drift")

    def drifted(
        controller_artifact: ExternalSupervisorArtifact,
    ) -> ExternalSupervisorLiveObservation:
        observation = _absent_external_supervisor_observation(controller_artifact)
        service = controller_artifact.supervisors[0].service_name
        return replace(
            observation,
            unit_payloads={**observation.unit_payloads, service: b"unreviewed-live-bytes\n"},
        )

    digest, blocker = IsolatedRehearsalExecutor(
        run=run,
        external_supervisor_artifacts=lambda _plan: artifact,
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=drifted,
    )._validate_external_supervisors(plan)

    assert digest is None
    assert blocker == "external-supervisor-controller-proof-failed"


def test_default_gb10_controller_observer_uses_candidate_bound_fixed_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _external_supervisor_artifact().for_execution_host("gx10-01c7")
    expected = _absent_external_supervisor_observation(artifact)
    captured: dict[str, object] = {}
    authorities: list[ExternalSupervisorPredecessorAuthority | None] = []

    class Transport:
        def __init__(self, controller_run) -> None:
            self.controller_run = controller_run

        def observe(
            self,
            actual: ExternalSupervisorArtifact,
            authority: ExternalSupervisorPredecessorAuthority | None = None,
        ) -> ExternalSupervisorLiveObservation:
            assert actual == artifact
            authorities.append(authority)
            result = self.controller_run(("ssh", "fixed-controller"), '{"request":1}\n')
            assert result.returncode == 0
            if authority is None:
                raise ValueError("bootstrap authority required")
            return expected

    def build(*, candidate_sha: str, candidate_tree: str, run):
        captured["candidate_sha"] = candidate_sha
        captured["candidate_tree"] = candidate_tree
        return Transport(run)

    def run(argv, payload, timeout):
        captured["argv"] = tuple(argv)
        captured["payload"] = payload
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(argv, 0, "ok\n", "")

    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_executor.build_fixed_gb10_external_supervisor_transport",
        build,
    )

    observed = IsolatedRehearsalExecutor(run=run)._observe_gb10_external_supervisor(artifact)

    assert observed == expected
    assert captured == {
        "argv": ("ssh", "fixed-controller"),
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "payload": b'{"request":1}\n',
        "timeout": 1740,
    }
    assert authorities[0] is None
    assert authorities[1] == ExternalSupervisorPredecessorAuthority(
        kind="absent",
        authority_digest=ABSENT_PREDECESSOR_DIGEST,
        unit_sha256={},
    )


def test_external_supervisor_default_rebuilds_only_from_fixed_staging_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    artifact = _external_supervisor_artifact()
    captured: dict[str, object] = {}

    def build(candidate_root: Path, **kwargs: object) -> ExternalSupervisorArtifact:
        captured["candidate_root"] = candidate_root
        captured.update(kwargs)
        return artifact

    monkeypatch.setattr(
        "loom_cli.rollout.rehearsal_executor.build_external_supervisor_artifact",
        build,
    )

    assert _default_external_supervisor_artifact(plan) is artifact
    assert captured == {
        "candidate_root": Path(staging_working_directory(plan.candidate_sha)),
        "candidate_sha": plan.candidate_sha,
        "candidate_tree": plan.candidate_tree,
        "environment": "staging",
        "image_tag": plan.image_tag,
    }


def test_release_rejects_external_supervisor_artifact_drift_after_secret_readback() -> None:
    plan = replace(_plan(), external_supervisor_artifact_sha256="0" * 64)
    release = _release_artifact(plan)
    secrets = _secret_artifact(plan)
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("docker", "image", "inspect"):
            name = command[-1].split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
        if payload == secrets.payload:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if (
            command[0] == "kubectl"
            and "apply" in command
            and payload is not None
            and b'"kind":"Service"' in payload
            and b'"name":"loom-postgres-rw"' in payload
        ):
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if "secret" in command and "get" in command:
            name = command[command.index("secret") + 1]
            return subprocess.CompletedProcess(argv, 0, f"{name}\t{plan.plan_digest}\n", "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        secret_artifacts=lambda _plan: secrets,
        external_supervisor_artifacts=lambda _plan: _external_supervisor_artifact(),
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.release", plan)

    assert outcome.blockers == {"release": "external-supervisor-artifact-drift"}
    assert any(payload == secrets.payload for _command, payload in calls)
    assert sum("secret" in command and "get" in command for command, _payload in calls) == len(
        secrets.secret_names
    )
    assert all(command[0] != "systemd-run" for command, _payload in calls)
    assert all(payload != release.payload for _command, payload in calls)


def test_release_external_supervisor_failure_is_secret_free_and_blocks_manifest() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    secrets = _secret_artifact(plan)
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("docker", "image", "inspect"):
            name = command[-1].split(":", 1)[0]
            return subprocess.CompletedProcess(argv, 0, plan.image_digests[name] + "\n", "")
        if command[:3] == ("systemctl", "--user", "show"):
            return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
        if command[:4] == ("systemd-run", "--user", "--wait", "--collect"):
            return subprocess.CompletedProcess(argv, 1, "", "token=must-not-leak")
        if "loom_cli.rollout.rehearsal_environment_state_probe" in command:
            assert payload == _external_supervisor_profile()
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(_external_policy_seed_result(plan)),
                "",
            )
        if payload == secrets.payload:
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if (
            command[0] == "kubectl"
            and "apply" in command
            and payload is not None
            and b'"kind":"Service"' in payload
            and b'"name":"loom-postgres-rw"' in payload
        ):
            return subprocess.CompletedProcess(argv, 0, "applied\n", "")
        if "secret" in command and "get" in command:
            name = command[command.index("secret") + 1]
            return subprocess.CompletedProcess(argv, 0, f"{name}\t{plan.plan_digest}\n", "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        secret_artifacts=lambda _plan: secrets,
        external_supervisor_artifacts=lambda _plan: _external_supervisor_artifact(),
        external_supervisor_profiles=lambda _plan: _external_supervisor_profile(),
        gb10_external_supervisor_observer=_absent_external_supervisor_observation,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.release", plan)

    assert outcome.blockers == {"release": "external-supervisor-validation-failed"}
    assert outcome.details == {"status": "blocked"}
    assert "token" not in str(outcome)
    assert all(payload != release.payload for _command, payload in calls)
    validation_index = next(
        index for index, (command, _payload) in enumerate(calls) if command[0] == "systemd-run"
    )
    assert (
        max(
            index
            for index, (command, _payload) in enumerate(calls)
            if "secret" in command and "get" in command
        )
        < validation_index
    )
    # The db-clone pod is bridged to service/loom-postgres-rw before the supervisor
    # (which port-forwards to that service) is launched.
    service_apply_index = next(
        index
        for index, (command, payload) in enumerate(calls)
        if command[0] == "kubectl"
        and "apply" in command
        and payload is not None
        and b'"kind":"Service"' in payload
        and b'"name":"loom-postgres-rw"' in payload
    )
    policy_seed_index = next(
        index
        for index, (_command, payload) in enumerate(calls)
        if payload == _external_supervisor_profile()
    )
    assert policy_seed_index < service_apply_index < validation_index


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
        raise AssertionError("drifted images must fail before Kubernetes mutation")

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
        if "psql" in command:
            return subprocess.CompletedProcess(
                argv, 0, json.dumps(_api_smoke_seed_value(command, plan)), ""
            )
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
    assert len(outcome.details["capacity-authority-sha256"]) == 64
    assert len(outcome.details["worker-authority-sha256"]) == 64
    seed = next(command for command in calls if "psql" in command)
    seed_sql = next(item for item in seed if item.startswith("--command="))
    assert "ON CONFLICT (id) DO UPDATE" in seed_sql
    assert "'gb10'" in seed_sql
    capacity_queries = [
        next(item for item in command if item.startswith("--command="))
        for command in calls
        if "psql" in command and any("staging_lifecycle_capacity" in item for item in command)
    ]
    assert len(capacity_queries) == 2
    assert "SELECT json_build_object" in capacity_queries[0]
    assert "WITH refreshed AS" in capacity_queries[1]
    assert plan.resources.namespace in capacity_queries[1]
    probe = next(
        command for command in calls if "loom_cli.rollout.rehearsal_smoke_probe" in command
    )
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


def test_api_smoke_fails_closed_when_isolated_worker_seed_drifts() -> None:
    plan = _plan()
    release = _release_artifact(plan)

    def run(argv, _payload, _timeout):
        command = tuple(argv)
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
        if "psql" in command:
            return subprocess.CompletedProcess(argv, 0, '{"status":"drift"}', "")
        raise AssertionError("service probe must not run without exact worker authority")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)

    assert outcome.blockers == {"api-smoke": "worker-authority-failed"}


def test_api_smoke_fails_before_refresh_when_snapshot_capacity_is_high_water() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    psql_calls: list[str] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
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
        if "psql" in command:
            sql = next(item for item in command if item.startswith("--command="))
            psql_calls.append(sql)
            if "staging_lifecycle_capacity" not in sql:
                value = _api_smoke_seed_value(command, plan)
            else:
                capacity = StagingCapacity(250_000, 34, 80, 90)
                value = {
                    "bytes_used": capacity.bytes_used,
                    "disk_free_percent": capacity.disk_free_percent,
                    "evidence_sha256": capacity.evidence_digest,
                    "inode_free_percent": capacity.inode_free_percent,
                    "namespace": "loom-staging",
                    "object_count": capacity.object_count,
                    "policy_sha256": staging_capacity_policy_digest(),
                    "source": CAPACITY_SOURCE,
                }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        raise AssertionError("candidate probe must not run without safe capacity authority")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)

    assert outcome.blockers == {"api-smoke": "capacity-authority-failed"}
    assert len(psql_calls) == 2
    assert "WITH refreshed AS" not in psql_calls[-1]


def test_api_smoke_persists_normalized_nonzero_probe_evidence() -> None:
    plan = _plan()
    release = _release_artifact(plan)
    response_sha256 = "d" * 64

    def run(argv, _payload, _timeout):
        command = tuple(argv)
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
        if "psql" in command:
            return subprocess.CompletedProcess(
                argv, 0, json.dumps(_api_smoke_seed_value(command, plan)), ""
            )
        if "exec" in command:
            failure = {
                "failure_code": "rehearsal-api-smoke-http-400",
                "reason_code": "invalid-task-config",
                "request_id": "batch-submit",
                "response_sha256": response_sha256,
                "schema_version": 1,
                "status": "blocked",
            }
            return subprocess.CompletedProcess(
                argv,
                1,
                json.dumps(failure),
                "service request returned HTTP 400\n",
            )
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)

    assert outcome.blockers == {
        "api-smoke": "http-400.batch-submit.invalid-task-config",
        "api-smoke-response-sha256": response_sha256,
    }
    assert outcome.details == {
        "failure-code": "rehearsal-api-smoke-http-400",
        "reason-code": "invalid-task-config",
        "request-id": "batch-submit",
        "response-sha256": response_sha256,
        "status": "blocked",
    }


def test_api_smoke_accepts_only_allowlisted_non_http_locus() -> None:
    value = {
        "failure_code": "rehearsal-api-smoke-failed",
        "reason_code": "transport-unavailable",
        "request_id": "health",
        "response_sha256": None,
        "schema_version": 1,
        "status": "blocked",
    }

    assert _api_smoke_failure(value) == (
        "rehearsal-api-smoke-failed",
        "health",
        "transport-unavailable",
        None,
    )
    value["request_id"] = "probe"
    assert _api_smoke_failure(value) is None


@pytest.mark.parametrize(
    "failure",
    [
        "not-json",
        json.dumps(
            {
                "failure_code": "rehearsal-api-smoke-http-400",
                "reason_code": "secret-shaped-untrusted-reason",
                "request_id": "batch-submit",
                "response_sha256": "d" * 64,
                "schema_version": 1,
                "status": "blocked",
            }
        ),
    ],
)
def test_api_smoke_rejects_malformed_nonzero_probe_output(failure: str) -> None:
    plan = _plan()
    release = _release_artifact(plan)

    def run(argv, _payload, _timeout):
        command = tuple(argv)
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
        if "psql" in command:
            return subprocess.CompletedProcess(
                argv, 0, json.dumps(_api_smoke_seed_value(command, plan)), ""
            )
        if "exec" in command:
            return subprocess.CompletedProcess(argv, 1, failure, "bounded")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        release_artifacts=lambda _plan: release,
        runtime_image_resolver=_runtime_images,
    ).execute("rehearsal.api-smoke", plan)

    assert outcome.blockers == {"api-smoke": "probe-failed"}


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
    assert probe[probe.index("--database") + 1] == plan.resources.database
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
        "gb10-host-count": "15",
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
    load_state_reads = 0
    namespace_present = True
    calls: list[tuple[tuple[str, ...], bytes | None]] = []
    sleeps: list[float] = []

    def run(argv, payload, _timeout):
        nonlocal load_state_reads, namespace_present, unit_active
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                if not unit_active:
                    load_state_reads += 1
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "loaded\n" if unit_active or load_state_reads <= 2 else "not-found\n",
                    "",
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
                assert "--ignore-not-found=true" in command
                return subprocess.CompletedProcess(argv, 0, "", "")
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
        sleep=sleeps.append,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.passed and outcome.cleanup_verified
    assert sleeps == [0.1, 0.1]
    assert outcome.details["status"] == "absent"
    delete = next(command for command, _payload in calls if "--raw" in command)
    assert delete[-2:] == ("-f", "-")
    assert plan.resources.namespace in delete[-3]


def test_cleanup_retires_only_exact_external_supervisor_validation_units() -> None:
    plan = _plan()
    service_names = tuple(
        sorted(name for name in plan.external_supervisor_unit_sha256 if name.endswith(".service"))
    )
    validation_units = {
        _external_supervisor_validation_unit(plan, service_name): service_name
        for service_name in service_names
    }
    assert set(service_names) == {
        "loom-autoscaler-gb10-staging.service",
        "loom-autoscaler-oldlab-staging.service",
        "loom-task-image-builder-gb10-staging.service",
        "loom-task-image-builder-oldlab-staging.service",
    }
    validation_active = dict.fromkeys(validation_units, True)
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("systemctl", "--user", "show"):
            unit = command[3]
            if unit == plan.resources.systemd_unit:
                if command[-1] == "--value":
                    return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
                return subprocess.CompletedProcess(argv, 4, "", "")
            assert unit in validation_units
            if command[-1] == "--value":
                state = "loaded" if validation_active[unit] else "not-found"
                return subprocess.CompletedProcess(argv, 0, state + "\n", "")
            properties = _external_supervisor_validation_expected_properties(
                plan,
                validation_units[unit],
            )
            output = "".join(f"{name}={value}\n" for name, value in properties.items())
            return subprocess.CompletedProcess(argv, 0, output, "")
        if command[:3] == ("systemctl", "--user", "stop"):
            assert command[3] in validation_units
            validation_active[command[3]] = False
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "reset-failed"):
            assert command[3] in validation_units
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "get" in command and "namespace" in command:
            assert "--ignore-not-found=true" in command
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.passed and outcome.cleanup_verified
    assert not any(validation_active.values())
    for validation_unit in validation_units:
        assert ("systemctl", "--user", "stop", validation_unit) in calls
        assert ("systemctl", "--user", "reset-failed", validation_unit) in calls
        assert (
            sum(
                command[:4] == ("systemctl", "--user", "show", validation_unit)
                and command[-1] == "--value"
                for command in calls
            )
            >= 3
        )
    assert all(service_name not in command for command in calls for service_name in service_names)


@pytest.mark.parametrize(
    ("final_returncode", "final_stdout", "final_stderr", "expected_blocker"),
    [
        (0, "", "", None),
        (0, "present", "", "namespace-delete-timeout"),
        (1, "", "transport unavailable", "namespace-final-readback-failed"),
    ],
)
def test_cleanup_classifies_wait_race_with_exact_final_namespace_readback(
    final_returncode: int,
    final_stdout: str,
    final_stderr: str,
    expected_blocker: str | None,
) -> None:
    plan = _plan()
    namespace = _namespace_manifest(plan)
    metadata = namespace["metadata"]
    assert isinstance(metadata, dict)
    metadata["resourceVersion"] = "42"
    metadata["uid"] = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    get_count = 0

    def run(argv, _payload, _timeout):
        nonlocal get_count
        command = tuple(argv)
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
            return subprocess.CompletedProcess(argv, 4, "", "")
        if "get" in command and "namespace" in command:
            assert "--ignore-not-found=true" in command
            get_count += 1
            if get_count == 1:
                return subprocess.CompletedProcess(argv, 0, json.dumps(namespace), "")
            output = json.dumps(namespace) if final_stdout == "present" else final_stdout
            return subprocess.CompletedProcess(
                argv,
                final_returncode,
                output,
                final_stderr,
            )
        if "delete" in command and "--raw" in command:
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        if "wait" in command:
            return subprocess.CompletedProcess(argv, 1, "", "timed out")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    if expected_blocker is None:
        assert outcome.passed and outcome.cleanup_verified
    else:
        assert outcome.blockers == {"cleanup": expected_blocker}


def test_cleanup_does_not_treat_namespace_transport_failure_as_absence() -> None:
    plan = _plan()

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
            return subprocess.CompletedProcess(argv, 4, "", "")
        if "get" in command and "namespace" in command:
            assert "--ignore-not-found=true" in command
            return subprocess.CompletedProcess(argv, 1, "", "connection refused")
        raise AssertionError("cleanup must stop before namespace deletion")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.blockers == {"cleanup": "namespace-readback-failed"}


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


def test_cleanup_accepts_reset_race_only_after_unit_is_absent() -> None:
    plan = _plan()
    unit_active = True
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        nonlocal unit_active
        command = tuple(argv)
        calls.append(command)
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "loaded\n" if unit_active else "not-found\n",
                    "",
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
            return subprocess.CompletedProcess(argv, 5, "", "unit not loaded")
        if "get" in command and "namespace" in command:
            assert "--ignore-not-found=true" in command
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(command)

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.passed and outcome.cleanup_verified
    assert any(command[-1] == "--value" for command in calls)


def test_cleanup_rejects_reset_failure_while_unit_is_still_loaded() -> None:
    plan = _plan()

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "loaded\n", "")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = (
                "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
                "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
                f"Description={description}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output, "")
        if command[:3] == ("systemctl", "--user", "stop"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "reset-failed"):
            return subprocess.CompletedProcess(argv, 5, "", "unit still loaded")
        raise AssertionError("namespace cleanup must not follow an ambiguous reset failure")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.blockers == {"cleanup": "systemd-reset-failed"}


def test_cleanup_fails_closed_when_transient_unit_does_not_unload() -> None:
    plan = _plan()
    monotonic_values = iter((0.0, 0.0, 1.0, 5.0))
    sleeps: list[float] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "loaded\n", "")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = (
                "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
                "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
                f"Description={description}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output, "")
        if command[:3] in {
            ("systemctl", "--user", "stop"),
            ("systemctl", "--user", "reset-failed"),
        }:
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError("namespace cleanup must not start before the unit disappears")

    outcome = IsolatedRehearsalExecutor(
        run=run,
        monotonic=lambda: next(monotonic_values),
        sleep=sleeps.append,
        gb10_transport_factory=passing_gb10_transport_factory,
    ).execute("rehearsal.cleanup", plan)

    assert outcome.blockers == {"cleanup": "systemd-remains"}
    assert sleeps == [0.1, 0.1]


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
    artifact = build_rehearsal_browser_artifact(
        plan,
        ingress_ip="10.96.12.34",
        ingress_endpoint_ips=("192.168.50.14",),
    )
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
                    "annotations": {
                        "loom.openai.dev/plan-sha256": plan.plan_digest,
                    },
                    "labels": {
                        "job-name": BROWSER_JOB_NAME,
                    },
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
        if "apply" in argv or "wait" in argv:
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
        elif "endpoints" in argv and "ingress-nginx-controller" in argv:
            value = {
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {
                    "name": "ingress-nginx-controller",
                    "namespace": "ingress-nginx",
                },
                "subsets": [
                    {
                        "addresses": [{"ip": "192.168.50.14", "nodeName": "trt-eai-oldlab-2"}],
                        "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}],
                    }
                ],
            }
        elif "ingress" in argv:
            value = resources[("Ingress", "loom-rehearsal-browser")]
        elif "networkpolicy" in argv:
            value = resources[("NetworkPolicy", argv[argv.index("networkpolicy") + 1])]
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
        browser_artifacts=lambda _plan, _ip, _endpoint_ips: artifact,
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


def test_browser_fails_closed_before_mutation_when_ingress_endpoints_drift() -> None:
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
        if "service" in argv:
            value = {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": "ingress-nginx-controller",
                    "namespace": "ingress-nginx",
                },
                "spec": {"clusterIP": "10.96.12.34", "type": "ClusterIP"},
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        return subprocess.CompletedProcess(argv, 0, "{}", "")

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.browser", plan)

    assert outcome.blockers == {"browser": "ingress-endpoints-readback-failed"}
    assert len(calls) == 2
    assert all("apply" not in call for call in calls)
