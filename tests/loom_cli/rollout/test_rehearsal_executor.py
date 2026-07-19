from __future__ import annotations

import json
import subprocess
from pathlib import Path

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan, RehearsalResources
from loom_cli.rollout.rehearsal_executor import IsolatedRehearsalExecutor


def _plan() -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        checkpoint_request_id="req-abcdefgh",
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={"loom-service": "sha256:" + "1" * 64},
        image_artifact_sha256="2" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/artifact.json"
        ),
        rendered_manifest_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/rendered.yaml"
        ),
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256="8" * 64,
        migration_plan_sha256="3" * 64,
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
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


def test_unimplemented_rehearsal_steps_remain_fail_closed() -> None:
    outcome = IsolatedRehearsalExecutor().execute("rehearsal.db-clone", _plan())
    assert outcome.blockers == {"executor": "isolated-action-not-implemented"}
