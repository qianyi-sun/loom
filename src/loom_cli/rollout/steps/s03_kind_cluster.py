"""Step 03 - ensure the protected kind cluster exists (#206).

Host-runtime failures can remove the kind control-plane container while the
durable data root remains intact. Image loading, migrations, and release gates
all assume a live kube API, so the rollout must repair that boundary before it
tries to load images or apply jobs.
"""

from __future__ import annotations

import json
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured


def _kind_config(ctx: RolloutContext) -> str:
    root = str(ctx.rollout_root)
    return (
        "kind: Cluster\n"
        "apiVersion: kind.x-k8s.io/v1alpha4\n"
        "nodes:\n"
        "  - role: control-plane\n"
        "    extraPortMappings:\n"
        "      - containerPort: 80\n"
        "        hostPort: 80\n"
        "        protocol: TCP\n"
        "      - containerPort: 443\n"
        "        hostPort: 443\n"
        "        protocol: TCP\n"
        "    extraMounts:\n"
        f"      - hostPath: {root}\n"
        f"        containerPath: {root}\n"
    )


def _backup_secrets_dir(ctx: RolloutContext) -> Path:
    try:
        manifest = json.loads(ctx.backup_manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"could not read backup manifest {ctx.backup_manifest_path}: {exc}",
        ) from exc
    components = manifest.get("components")
    if not isinstance(components, dict):
        raise RuntimeError("backup manifest is missing components")
    k8s_secrets = components.get("k8s_secrets")
    if not isinstance(k8s_secrets, dict):
        raise RuntimeError("backup manifest is missing components.k8s_secrets")
    path = k8s_secrets.get("path")
    if not isinstance(path, str) or not path.strip():
        raise RuntimeError("backup manifest k8s_secrets component is missing path")
    secrets_dir = Path(path)
    if not secrets_dir.is_dir():
        raise RuntimeError(f"backup k8s secrets directory does not exist: {secrets_dir}")
    return secrets_dir


def _cluster_names(stdout: str) -> set[str]:
    return {line.strip() for line in stdout.splitlines() if line.strip()}


class KindClusterStep(BaseStep):
    number = 3
    name = "kind-cluster"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_name": ctx.cluster_name,
            "namespace": ctx.namespace,
            "rollout_root": str(ctx.rollout_root),
            "backup_manifest_path": str(ctx.backup_manifest_path),
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        clusters = run_captured(["kind", "get", "clusters"])
        if clusters.returncode != 0:
            return VerifyOutcome.UNKNOWN
        if ctx.cluster_name not in _cluster_names(clusters.stdout):
            return VerifyOutcome.MISMATCH
        namespace = run_captured(["kubectl", "get", "namespace", ctx.namespace])
        if namespace.returncode != 0:
            return VerifyOutcome.MISMATCH
        secrets = run_captured(
            [
                "kubectl",
                "-n",
                ctx.namespace,
                "get",
                "secret",
                "loom-secrets",
                "loom-admin-secret",
            ],
        )
        return VerifyOutcome.MATCH if secrets.returncode == 0 else VerifyOutcome.MISMATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            secrets_dir = _backup_secrets_dir(ctx)
        except RuntimeError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))

        clusters = run_captured(
            ["kind", "get", "clusters"],
            stdout_log=step_dir.artifact_path("kind-get-clusters.stdout"),
            stderr_log=step_dir.artifact_path("kind-get-clusters.stderr"),
        )
        if clusters.returncode != 0:
            return RunResult(
                exit_code=clusters.returncode,
                error=(
                    clusters.stderr.strip().splitlines()[-1]
                    if clusters.stderr.strip()
                    else "kind get clusters failed"
                ),
            )

        created = False
        if ctx.cluster_name not in _cluster_names(clusters.stdout):
            config_path = step_dir.artifact_path("kind-cluster.yaml")
            config_path.write_text(_kind_config(ctx), encoding="utf-8")
            create = run_captured(
                [
                    "kind",
                    "create",
                    "cluster",
                    "--name",
                    ctx.cluster_name,
                    "--config",
                    str(config_path),
                ],
                stdout_log=step_dir.artifact_path("kind-create.stdout"),
                stderr_log=step_dir.artifact_path("kind-create.stderr"),
            )
            if create.returncode != 0:
                return RunResult(
                    exit_code=create.returncode,
                    error=(
                        create.stderr.strip().splitlines()[-1]
                        if create.stderr.strip()
                        else "kind create cluster failed"
                    ),
                )
            created = True

        export = run_captured(
            ["kind", "export", "kubeconfig", "--name", ctx.cluster_name],
            stdout_log=step_dir.artifact_path("kind-export-kubeconfig.stdout"),
            stderr_log=step_dir.artifact_path("kind-export-kubeconfig.stderr"),
        )
        if export.returncode != 0:
            return RunResult(
                exit_code=export.returncode,
                error=(
                    export.stderr.strip().splitlines()[-1]
                    if export.stderr.strip()
                    else "kind export kubeconfig failed"
                ),
            )

        namespace = run_captured(
            ["kubectl", "get", "namespace", ctx.namespace],
            stdout_log=step_dir.artifact_path("namespace-get.stdout"),
            stderr_log=step_dir.artifact_path("namespace-get.stderr"),
        )
        namespace_created = False
        if namespace.returncode != 0:
            create_ns = run_captured(
                ["kubectl", "create", "namespace", ctx.namespace],
                stdout_log=step_dir.artifact_path("namespace-create.stdout"),
                stderr_log=step_dir.artifact_path("namespace-create.stderr"),
            )
            if create_ns.returncode != 0:
                return RunResult(
                    exit_code=create_ns.returncode,
                    error=(
                        create_ns.stderr.strip().splitlines()[-1]
                        if create_ns.stderr.strip()
                        else "kubectl create namespace failed"
                    ),
                )
            namespace_created = True

        apply_secrets = run_captured(
            ["kubectl", "-n", ctx.namespace, "apply", "-f", str(secrets_dir)],
            stdout_log=step_dir.artifact_path("secrets-apply.stdout"),
            stderr_log=step_dir.artifact_path("secrets-apply.stderr"),
        )
        if apply_secrets.returncode != 0:
            return RunResult(
                exit_code=apply_secrets.returncode,
                error=(
                    apply_secrets.stderr.strip().splitlines()[-1]
                    if apply_secrets.stderr.strip()
                    else "kubectl apply secrets failed"
                ),
            )

        summary = (
            f"{'created' if created else 'reused'} kind cluster "
            f"{ctx.cluster_name}; "
            f"{'created' if namespace_created else 'reused'} namespace "
            f"{ctx.namespace}; restored k8s secrets from backup manifest"
        )
        step_dir.stdout_path().write_text(summary + "\n", encoding="utf-8")
        return RunResult(
            exit_code=0,
            summary=summary,
            artifacts={
                "cluster_name": ctx.cluster_name,
                "namespace": ctx.namespace,
                "secrets_dir": str(secrets_dir),
            },
        )
