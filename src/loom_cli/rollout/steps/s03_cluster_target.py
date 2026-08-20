"""Step 03 — verify the existing protected Kubernetes target.

Protected rollouts operate only against the configured multi-node k3s target.
This step is read-only: it verifies the exact context, node readiness,
namespace, protected Secrets, and ingress admission endpoint before image
publication or any protected mutation.
"""

from __future__ import annotations

import json

from loom_cli.cluster_config import (
    load_cluster_config,
    validate_container_registry_publication,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured

INGRESS_NGINX_CONTROLLER_SELECTOR = (
    "app.kubernetes.io/component=controller,"
    "app.kubernetes.io/instance=ingress-nginx,"
    "app.kubernetes.io/name=ingress-nginx"
)
_DONE_ARTIFACT_KEYS = frozenset(
    {
        "cluster_name",
        "namespace",
        "candidate_sha",
        "cluster_config_sha256",
        "target_type",
        "container_registry",
        "container_registry_push",
        "protected_secrets",
        "ingress_class",
    }
)


def _required_registry_publication(ctx: RolloutContext) -> tuple[str, str]:
    publication = validate_container_registry_publication(
        load_cluster_config(ctx.cluster_config_path)
    )
    if publication is None:
        raise RuntimeError(
            "protected rollouts require container_registry and "
            "container_registry_push"
        )
    return publication


class ClusterTargetStep(BaseStep):
    number = 3
    name = "cluster-target"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        publication = _required_registry_publication(ctx)
        return {
            "resolved_sha": ctx.resolved_sha,
            "cluster_name": ctx.cluster_name,
            "namespace": ctx.namespace,
            "rollout_root": str(ctx.rollout_root),
            "cluster_config_path": str(ctx.cluster_config_path),
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "container_registry": publication[0],
            "container_registry_push": publication[1],
            "cluster_recovery_contract": "existing-multinode-k3s-readonly-v1",
        }

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        return self._verify_impl(ctx, step_dir)

    def requires_strict_live_verification(self) -> bool:
        return True

    def validate_done_artifacts(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
        artifacts: dict[str, str],
    ) -> bool:
        if set(artifacts) != _DONE_ARTIFACT_KEYS:
            return False
        try:
            publication = _required_registry_publication(ctx)
        except (OSError, RuntimeError, ValueError):
            return False
        return (
            artifacts["cluster_name"] == ctx.cluster_name
            and artifacts["namespace"] == ctx.namespace
            and artifacts["candidate_sha"] == ctx.resolved_sha
            and artifacts["cluster_config_sha256"] == ctx.cluster_config_sha256
            and artifacts["target_type"] == "multinode-k3s"
            and artifacts["container_registry"] == publication[0]
            and artifacts["container_registry_push"] == publication[1]
            and artifacts["protected_secrets"] == "loom-secrets,loom-admin-secret"
            and artifacts["ingress_class"] == "nginx"
        )

    def _verify_impl(
        self,
        ctx: RolloutContext,
        _step_dir: StepDir,
    ) -> VerifyOutcome:
        try:
            _required_registry_publication(ctx)
        except (OSError, RuntimeError, ValueError):
            return VerifyOutcome.UNKNOWN
        context = run_captured(["kubectl", "config", "current-context"])
        if context.returncode != 0:
            return VerifyOutcome.UNKNOWN
        if context.stdout.strip() != ctx.cluster_name:
            return VerifyOutcome.MISMATCH
        nodes = run_captured(["kubectl", "get", "nodes", "-o", "json"])
        if nodes.returncode != 0:
            return VerifyOutcome.UNKNOWN
        try:
            node_items = json.loads(nodes.stdout).get("items")
        except (AttributeError, json.JSONDecodeError):
            return VerifyOutcome.UNKNOWN
        if not isinstance(node_items, list) or len(node_items) < 2:
            return VerifyOutcome.MISMATCH
        for node in node_items:
            status = node.get("status") if isinstance(node, dict) else None
            conditions = status.get("conditions") if isinstance(status, dict) else None
            if not isinstance(conditions, list) or not any(
                isinstance(condition, dict)
                and condition.get("type") == "Ready"
                and condition.get("status") == "True"
                for condition in conditions
            ):
                return VerifyOutcome.MISMATCH
        checks = (
            ["kubectl", "get", "namespace", ctx.namespace],
            [
                "kubectl",
                "-n",
                ctx.namespace,
                "get",
                "secret",
                "loom-secrets",
                "loom-admin-secret",
            ],
            ["kubectl", "get", "ingressclass", "nginx"],
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "wait",
                "--for=condition=Ready",
                "pod",
                f"--selector={INGRESS_NGINX_CONTROLLER_SELECTOR}",
                "--timeout=5s",
            ],
        )
        if any(run_captured(command).returncode != 0 for command in checks):
            return VerifyOutcome.MISMATCH
        endpoint = run_captured(
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "get",
                "endpoints",
                "ingress-nginx-controller-admission",
                "-o",
                "jsonpath={.subsets[0].addresses[0].ip}",
            ]
        )
        return (
            VerifyOutcome.MATCH
            if endpoint.returncode == 0 and endpoint.stdout.strip()
            else VerifyOutcome.MISMATCH
        )

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            publication = _required_registry_publication(ctx)
        except (OSError, RuntimeError, ValueError) as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))
        if self._verify_impl(ctx, step_dir) is not VerifyOutcome.MATCH:
            return RunResult(
                exit_code=1,
                error="configured multi-node k3s cluster contract is not ready",
            )
        summary = (
            f"verified existing multi-node k3s context {ctx.cluster_name}; "
            f"registry publication {publication[1]} -> {publication[0]}; "
            f"reused namespace {ctx.namespace} and two protected secrets"
        )
        step_dir.stdout_path().write_text(summary + "\n", encoding="utf-8")
        return RunResult(
            exit_code=0,
            summary=summary,
            artifacts={
                "cluster_name": ctx.cluster_name,
                "namespace": ctx.namespace,
                "candidate_sha": ctx.resolved_sha,
                "cluster_config_sha256": ctx.cluster_config_sha256,
                "target_type": "multinode-k3s",
                "container_registry": publication[0],
                "container_registry_push": publication[1],
                "protected_secrets": "loom-secrets,loom-admin-secret",
                "ingress_class": "nginx",
            },
        )


__all__ = ["ClusterTargetStep"]
