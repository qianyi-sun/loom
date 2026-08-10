"""Step 09 — Alembic migration via sanctioned Job (#340).

Uses ``loom cluster render-migration`` from #332 to build a proper
migration Job manifest, applies it via kubectl, and waits for the Job's
condition=complete. Deterministic job suffix so rerunning against the
same evidence dir is idempotent (same Job name → kubectl apply reuses
if pending, or hits AlreadyExists on completed Jobs cleaning up via TTL).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_config import (
    load_cluster_config,
    validate_container_registry_publication,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.operator.redaction import redact_rollout_text
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.s03_kind_load_images import registry_image_digests
from loom_cli.rollout.steps.subprocess_util import run_captured


def _deterministic_job_suffix(ctx: RolloutContext) -> str:
    """Return a stable short suffix so rerunning finds the same Job.

    Uses first 8 chars of sha256 over (image_tag, resolved_sha, config).
    """
    h = hashlib.sha256(
        f"{ctx.image_tag}|{ctx.resolved_sha}|{ctx.cluster_config_sha256}".encode(),
    ).hexdigest()
    return h[:8]


def _rendered_manifest_path(step_dir: StepDir) -> Path:
    return step_dir.path.parent / "07-render" / "rendered.yaml"


def _error_excerpt(value: str) -> str:
    return redact_rollout_text(value, limit=200).strip()


def _resource_name(doc: dict[Any, Any]) -> str | None:
    metadata = doc.get("metadata")
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    return name if isinstance(name, str) else None


def _stateful_substrate_resource_id(
    doc: dict[Any, Any],
    *,
    namespace: str,
) -> str | None:
    kind = doc.get("kind")
    if not isinstance(kind, str):
        return None
    name = _resource_name(doc)
    if name is None:
        return None
    pv_names = {
        f"{namespace}-postgres-data",
        f"{namespace}-minio-data",
        f"{namespace}-worker-trajectories-data",
    }
    if kind == "PersistentVolume" and name in pv_names:
        return f"{kind}/{name}"
    if kind == "PersistentVolumeClaim" and name == "loom-worker-trajectories":
        return f"{kind}/{name}"
    if kind == "StatefulSet" and name in {"loom-postgres", "loom-minio"}:
        return f"{kind}/{name}"
    if kind == "Cluster" and name == "loom-postgres":
        return f"{kind}/{name}"
    if kind == "Service" and name in {"loom-postgres", "loom-minio"}:
        return f"{kind}/{name}"
    return None


def _write_stateful_substrate_manifest(
    rendered_manifest: Path,
    target: Path,
    *,
    namespace: str,
) -> list[str]:
    """Write the storage and DB/object-store substrate needed before migration.

    A reconstructed kind cluster has namespace/secrets after step 03 but no
    standing Services or StatefulSets. Migration needs Postgres alive before
    full cluster-up starts application pods. Environment-state runs later,
    after cluster-up has recreated the Control Plane service in missing-kind
    recovery. Static worker trajectory storage is included when rendered so
    reruns do not leave protected preflight with a partial critical PVC set.
    """
    try:
        docs = list(yaml.safe_load_all(rendered_manifest.read_text(encoding="utf-8")))
    except OSError as exc:
        raise RuntimeError(f"rendered manifest missing for stateful substrate: {exc}") from exc
    except yaml.YAMLError as exc:
        raise RuntimeError(f"rendered manifest is not valid YAML: {exc}") from exc

    selected: list[dict[Any, Any]] = []
    resource_ids: list[str] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        resource_id = _stateful_substrate_resource_id(doc, namespace=namespace)
        if resource_id is None:
            continue
        selected.append(doc)
        resource_ids.append(resource_id)

    required = {
        "Service/loom-postgres",
        "StatefulSet/loom-minio",
        "Service/loom-minio",
    }
    missing = sorted(required - set(resource_ids))
    if missing:
        raise RuntimeError(
            "rendered manifest lacks required stateful substrate resources: " + ", ".join(missing),
        )
    postgres_controllers = {
        "Cluster/loom-postgres",
        "StatefulSet/loom-postgres",
    } & set(resource_ids)
    if len(postgres_controllers) != 1:
        raise RuntimeError(
            "rendered manifest must contain exactly one PostgreSQL controller",
        )

    target.write_text(
        yaml.safe_dump_all(selected, sort_keys=False),
        encoding="utf-8",
    )
    return resource_ids


class MigrateStep(BaseStep):
    number = 9
    name = "migrate"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "image_tag": ctx.image_tag,
            "namespace": ctx.namespace,
            "job_suffix": _deterministic_job_suffix(ctx),
        }

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        suffix = _deterministic_job_suffix(ctx)
        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
        except CandidateToolingError as exc:
            self.write_stderr(step_dir, str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        try:
            cluster_config = load_cluster_config(rollout_cluster_config(ctx, step_dir))
            publication = validate_container_registry_publication(cluster_config)
        except (OSError, RuntimeError, ValueError) as exc:
            self.write_stderr(step_dir, str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        # Render the migration manifest.
        render_argv = candidate_loom_argv(
            "cluster",
            "render-migration",
            "--image-tag",
            ctx.image_tag,
            "--namespace",
            ctx.namespace,
            "--job-suffix",
            suffix,
        )
        if publication is not None:
            try:
                control_plane_digest = registry_image_digests(ctx, step_dir)[
                    "loom-control-plane"
                ]
            except (KeyError, RuntimeError, ValueError) as exc:
                self.write_stderr(step_dir, str(exc) + "\n")
                return RunResult(
                    exit_code=2,
                    error="published control-plane manifest digest is unavailable",
                )
            render_argv.extend(
                [
                    "--container-registry",
                    publication[0],
                    "--registry-digest",
                    control_plane_digest,
                ]
            )
        render = run_captured(
            render_argv,
            cwd=cwd,
            env=env,
        )
        if render.returncode != 0:
            self.write_stderr(step_dir, render.stderr)
            return RunResult(
                exit_code=render.returncode,
                error=f"render-migration failed: {_error_excerpt(render.stderr)}",
            )
        manifest = step_dir.artifact_path("migration.yaml")
        manifest.write_text(render.stdout)

        substrate_manifest = step_dir.artifact_path("stateful-substrate.yaml")
        try:
            substrate_resources = _write_stateful_substrate_manifest(
                _rendered_manifest_path(step_dir),
                substrate_manifest,
                namespace=ctx.namespace,
            )
        except RuntimeError as exc:
            self.write_stderr(step_dir, str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        apply_substrate = run_captured(
            ["kubectl", "-n", ctx.namespace, "apply", "-f", str(substrate_manifest)],
        )
        if apply_substrate.returncode != 0:
            self.write_stderr(step_dir, apply_substrate.stderr)
            return RunResult(
                exit_code=apply_substrate.returncode,
                error=(
                    "kubectl apply stateful substrate failed: "
                    f"{_error_excerpt(apply_substrate.stderr)}"
                ),
            )

        wait_commands: list[tuple[str, list[str]]] = []
        if "Cluster/loom-postgres" in substrate_resources:
            wait_commands.append(
                (
                    "cluster.postgresql.cnpg.io/loom-postgres",
                    [
                        "kubectl",
                        "-n",
                        ctx.namespace,
                        "wait",
                        "--for=condition=Ready",
                        "cluster.postgresql.cnpg.io/loom-postgres",
                        "--timeout=300s",
                    ],
                )
            )
        else:
            wait_commands.append(
                (
                    "statefulset/loom-postgres",
                    [
                        "kubectl",
                        "-n",
                        ctx.namespace,
                        "rollout",
                        "status",
                        "statefulset/loom-postgres",
                        "--timeout=300s",
                    ],
                )
            )
        wait_commands.append(
            (
                "statefulset/loom-minio",
                [
                    "kubectl",
                    "-n",
                    ctx.namespace,
                    "rollout",
                    "status",
                    "statefulset/loom-minio",
                    "--timeout=300s",
                ],
            )
        )
        wait_outputs: list[tuple[str, str]] = []
        for target_name, command in wait_commands:
            wait_target = run_captured(command)
            wait_outputs.append((target_name, wait_target.stdout))
            if wait_target.returncode != 0:
                self.write_stderr(step_dir, wait_target.stderr)
                return RunResult(
                    exit_code=wait_target.returncode,
                    error=(
                        f"stateful substrate {target_name} did not become ready: "
                        f"{_error_excerpt(wait_target.stderr)}"
                    ),
                )

        # Apply.
        apply_ = run_captured(
            ["kubectl", "-n", ctx.namespace, "apply", "-f", str(manifest)],
        )
        if apply_.returncode != 0:
            self.write_stderr(step_dir, apply_.stderr)
            return RunResult(
                exit_code=apply_.returncode,
                error=f"kubectl apply migration failed: {_error_excerpt(apply_.stderr)}",
            )

        # Wait for the Job to complete or fail.
        job_selector = f"app=loom-migration,loom.image-tag={ctx.image_tag}"
        wait = run_captured(
            [
                "kubectl",
                "-n",
                ctx.namespace,
                "wait",
                "--for=condition=complete",
                f"--selector={job_selector}",
                "--timeout=600s",
                "job",
            ]
        )
        render_excerpt = redact_rollout_text(render.stdout, limit=2000)
        self.write_stdout(
            step_dir,
            "# stateful-substrate resources\n"
            + "\n".join(substrate_resources)
            + "\n# kubectl apply stateful substrate\n"
            + apply_substrate.stdout
            + "".join(
                f"# kubectl readiness {name}\n{stdout}\n"
                for name, stdout in wait_outputs
            )
            + f"# render-migration\n{render_excerpt}\n"
            + f"# kubectl apply\n{apply_.stdout}\n"
            + f"# kubectl wait\n{wait.stdout}\n",
        )
        if wait.returncode != 0:
            self.write_stderr(step_dir, wait.stderr)
            return RunResult(
                exit_code=wait.returncode,
                error=(f"migration Job did not complete: {_error_excerpt(wait.stderr)}"),
            )
        return RunResult(
            exit_code=0,
            summary=f"migration Job complete (suffix={suffix})",
            artifacts={
                "job_selector": job_selector,
                "suffix": suffix,
                "stateful_substrate_manifest": str(substrate_manifest),
            },
        )
