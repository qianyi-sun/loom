from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.rehearsal_action_source import (
    RehearsalPlan,
    RehearsalResources,
    RehearsalSmokeAuthority,
)
from loom_cli.rollout.rehearsal_browser import (
    BROWSER_INGRESS_NAME,
    BROWSER_JOB_NAME,
    BROWSER_NETWORK_POLICY_NAME,
    build_rehearsal_browser_artifact,
    ingress_controller_ip,
    rehearsal_browser_job_complete,
    rehearsal_browser_pod_complete,
    rehearsal_browser_report_ready,
    rehearsal_browser_resource_ready,
)
from tests.loom_cli.rollout.rehearsal_fixtures import gb10_rehearsal_authority


def _plan(tmp_path: Path) -> RehearsalPlan:
    root = tmp_path / "preflight-artifacts" / ("6" * 64)
    root.mkdir(parents=True)
    rendered = root / "rendered.yaml"
    rendered.write_text("---\n")
    rendered.chmod(0o600)
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
        artifact_descriptor_path=root / "artifact.json",
        rendered_manifest_path=rendered,
        production_defaults_path=root / "production-defaults.json",
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256=hashlib.sha256(b"---\n").hexdigest(),
        production_defaults_sha256="9" * 64,
        migration_plan_sha256="3" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://yylx.world/dev",
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


def _resources(artifact) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (item["kind"], item["metadata"]["name"]): item
        for item in yaml.safe_load_all(artifact.payload)
    }


def test_browser_artifact_is_exact_restricted_and_candidate_bound(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    artifact = build_rehearsal_browser_artifact(plan, ingress_ip="10.96.12.34")
    resources = _resources(artifact)

    assert set(resources) == {
        ("Ingress", BROWSER_INGRESS_NAME),
        ("Job", BROWSER_JOB_NAME),
        ("NetworkPolicy", BROWSER_NETWORK_POLICY_NAME),
    }
    ingress = resources[("Ingress", BROWSER_INGRESS_NAME)]
    assert ingress["spec"]["rules"][0]["host"] == "yylx.world"
    assert [
        item["backend"]["service"]["name"] for item in ingress["spec"]["rules"][0]["http"]["paths"]
    ] == ["loom-service", "loom-web"]
    job = resources[("Job", BROWSER_JOB_NAME)]
    pod = job["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["hostAliases"] == [{"hostnames": ["yylx.world"], "ip": "10.96.12.34"}]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    init = pod["initContainers"][0]
    browser = pod["containers"][0]
    assert init["image"] == f"loom-staging-admin-browser-smoke:{plan.image_tag}"
    assert browser["image"] == init["image"]
    assert browser["securityContext"]["readOnlyRootFilesystem"] is True
    assert browser["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert "--emit-sanitized-report" in browser["args"]
    assert browser["args"][browser["args"].index("--route") + 1] == plan.resources.route
    assert browser["args"][browser["args"].index("--rehearsal-plan-sha256") + 1] == plan.plan_digest
    assert browser["args"][browser["args"].index("--rehearsal-isolation-id") + 1] == "5" * 24
    token = next(item for item in pod["volumes"] if item["name"] == "source-token")
    assert token["secret"] == {
        "defaultMode": 0o440,
        "items": [{"key": "admin-token", "path": "admin-token"}],
        "secretName": "loom-admin-secret",
    }
    policy = resources[("NetworkPolicy", BROWSER_NETWORK_POLICY_NAME)]
    assert policy["spec"]["egress"] == [
        {
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "10.96.12.34/32"}}],
        }
    ]


def test_ingress_controller_service_requires_exact_ipv4_cluster_identity() -> None:
    value = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "ingress-nginx-controller", "namespace": "ingress-nginx"},
        "spec": {"clusterIP": "10.96.12.34", "type": "ClusterIP"},
    }
    assert ingress_controller_ip(value) == "10.96.12.34"
    node_port = deepcopy(value)
    node_port["spec"]["type"] = "NodePort"
    assert ingress_controller_ip(node_port) == "10.96.12.34"
    for drift in (
        {"metadata": {"name": "other", "namespace": "ingress-nginx"}},
        {"metadata": {"name": "ingress-nginx-controller", "namespace": "loom-staging"}},
        {"spec": {"clusterIP": "2001:db8::1", "type": "ClusterIP"}},
        {"spec": {"clusterIP": "10.96.12.34", "type": "LoadBalancer"}},
    ):
        changed = deepcopy(value)
        for key, item in drift.items():
            changed[key] = item
        assert ingress_controller_ip(changed) is None


def test_browser_resource_job_and_pod_readback_are_exact(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    artifact = build_rehearsal_browser_artifact(plan, ingress_ip="10.96.12.34")
    resources = _resources(artifact)
    job = deepcopy(resources[("Job", BROWSER_JOB_NAME)])
    job["status"] = {
        "conditions": [{"status": "True", "type": "Complete"}],
        "succeeded": 1,
    }
    assert rehearsal_browser_resource_ready(
        job,
        artifact=artifact,
        plan=plan,
        kind="Job",
        name=BROWSER_JOB_NAME,
    )
    assert rehearsal_browser_job_complete(job, artifact=artifact, plan=plan)

    status = {
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
                    "containerStatuses": [{"name": "browser", **status}],
                    "initContainerStatuses": [{"name": "prepare-token", **status}],
                    "phase": "Succeeded",
                },
            }
        ]
    }
    assert rehearsal_browser_pod_complete(pods, artifact=artifact, plan=plan)
    pods["items"][0]["status"]["containerStatuses"][0]["imageID"] = "sha256:" + "0" * 64
    assert not rehearsal_browser_pod_complete(pods, artifact=artifact, plan=plan)


def test_browser_report_requires_complete_exact_rehearsal_binding(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    checks = {
        "bootstrap_status_204": True,
        "bootstrap_empty_body": True,
        "bootstrap_no_store": True,
        "deployed_build_sha_present": True,
        "deployed_build_sha_matches_expected": True,
        "secure_http_only_lax_cookie": True,
        "authenticated_target_user": True,
        "platform_admin_authority": True,
        "audit_event_correlated": True,
        "admin_access_document_2xx": True,
        "authenticated_react_mount": True,
        "admin_tabs_accessibility": True,
        "admin_requests_apis_200": True,
        "admin_requests_ui_visible": True,
        "admin_accounts_apis_200": True,
        "admin_accounts_ui_visible": True,
        "admin_teams_api_200": True,
        "admin_teams_ui_visible": True,
        "admin_invites_apis_200": True,
        "admin_invites_ui_visible": True,
        "admin_tokens_api_200": True,
        "admin_tokens_ui_visible": True,
        "admin_audit_api_200": True,
        "all_admin_tabs_operable": True,
        "audit_tab_event_visible": True,
        "rate_cards_api_200": True,
        "rate_cards_ui_visible": True,
        "browser_console_clean": True,
        "browser_page_errors_clean": True,
        "browser_request_failures_clean": True,
        "browser_server_errors_clean": True,
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
        "checks": checks,
        "cleanup": {"logout_status": 204, "auth_me_after_logout_status": 401},
        "failure_code": None,
    }
    assert rehearsal_browser_report_ready(report, plan=plan)

    drifted = deepcopy(report)
    drifted["rehearsal_binding"]["plan_sha256"] = "0" * 64
    assert not rehearsal_browser_report_ready(drifted, plan=plan)
    drifted = deepcopy(report)
    drifted["checks"]["browser_console_clean"] = False
    assert not rehearsal_browser_report_ready(drifted, plan=plan)
    drifted = deepcopy(report)
    drifted["rollout_binding"] = {}
    assert not rehearsal_browser_report_ready(drifted, plan=plan)


@pytest.mark.parametrize("value", ["", "not-an-ip", "2001:db8::1"])
def test_browser_artifact_rejects_invalid_ingress_identity(tmp_path: Path, value: str) -> None:
    with pytest.raises(ValueError, match="ingress identity"):
        build_rehearsal_browser_artifact(_plan(tmp_path), ingress_ip=value)


def test_browser_artifact_rejects_noncanonical_rehearsal_route(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    record = plan.to_record()
    record["resources"]["route"] = "https://staging.example.test/dev/rehearsal/" + "5" * 24
    with pytest.raises(ValueError, match="route authority"):
        build_rehearsal_browser_artifact(
            RehearsalPlan.from_record(record),
            ingress_ip="10.96.12.34",
        )
