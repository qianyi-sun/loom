"""Step 03 - ensure the protected kind cluster exists (#206).

Host-runtime failures can remove the kind control-plane container while the
durable data root remains intact. Image loading, migrations, and release gates
all assume a live kube API, so the rollout must repair that boundary before it
tries to load images or apply jobs.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured

INGRESS_NGINX_KIND_MANIFEST = Path("deploy/k8s/ingress-nginx-kind.yaml")
INGRESS_NGINX_CONTROLLER_SELECTOR = (
    "app.kubernetes.io/component=controller,"
    "app.kubernetes.io/instance=ingress-nginx,"
    "app.kubernetes.io/name=ingress-nginx"
)
_LAST_APPLIED_ANNOTATION = "kubectl.kubernetes.io/last-applied-configuration"
_RUNTIME_METADATA_KEYS = frozenset(
    {
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "selfLink",
        "uid",
    },
)


def _kind_config(ctx: RolloutContext) -> str:
    root = str(ctx.rollout_root)
    return (
        "kind: Cluster\n"
        "apiVersion: kind.x-k8s.io/v1alpha4\n"
        "nodes:\n"
        "  - role: control-plane\n"
        "    kubeadmConfigPatches:\n"
        "      - |\n"
        "        kind: InitConfiguration\n"
        "        nodeRegistration:\n"
        "          kubeletExtraArgs:\n"
        '            node-labels: "ingress-ready=true"\n'
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


def _static_host_path_root(ctx: RolloutContext) -> str | None:
    try:
        config = load_cluster_config(ctx.cluster_config_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"could not read cluster config {ctx.cluster_config_path}: {exc}",
        ) from exc
    backend = str(config.persistent_storage_backend)
    if backend == "dynamic":
        return None
    if backend != "static-host-path":
        raise RuntimeError(
            "persistent_storage_backend must be 'dynamic' or 'static-host-path'; "
            f"got {backend!r}",
        )
    if config.namespace != ctx.namespace:
        raise RuntimeError(
            "cluster config namespace does not match rollout namespace: "
            f"{config.namespace!r} != {ctx.namespace!r}",
        )
    root = str(config.persistent_storage_host_path_root or "").strip().rstrip("/")
    if not root:
        raise RuntimeError(
            "persistent_storage_host_path_root is required when "
            "persistent_storage_backend = 'static-host-path'",
        )
    if not root.startswith("/") or root == "/":
        raise RuntimeError(
            "persistent_storage_host_path_root must be an absolute operator-managed "
            "host path such as /data/loom-staging",
        )
    return root


def _worker_trajectories_storage_docs(
    ctx: RolloutContext,
) -> list[dict[str, object]]:
    root = _static_host_path_root(ctx)
    if root is None:
        return []
    config = load_cluster_config(ctx.cluster_config_path)
    storage_gi = int(config.worker_trajectory_storage_gi)
    if storage_gi <= 0:
        raise RuntimeError(
            "worker_trajectory_storage_gi must be positive for static-host-path storage",
        )
    namespace = ctx.namespace
    pv_name = f"{namespace}-worker-trajectories-data"
    return [
        {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": pv_name},
            "spec": {
                "capacity": {"storage": f"{storage_gi}Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "storageClassName": "",
                "hostPath": {
                    "path": f"{root}/trajectories",
                    "type": "DirectoryOrCreate",
                },
                "claimRef": {
                    "namespace": namespace,
                    "name": "loom-worker-trajectories",
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": "loom-worker-trajectories",
                "namespace": namespace,
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "",
                "volumeName": pv_name,
                "resources": {"requests": {"storage": f"{storage_gi}Gi"}},
            },
        },
    ]


def _write_worker_trajectories_storage_manifest(
    ctx: RolloutContext,
    path: Path,
) -> bool:
    docs = _worker_trajectories_storage_docs(ctx)
    if not docs:
        return False
    path.write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding="utf-8")
    return True


def _cluster_names(stdout: str) -> set[str]:
    return {line.strip() for line in stdout.splitlines() if line.strip()}


def _control_plane_node_name(ctx: RolloutContext) -> str:
    return f"{ctx.cluster_name}-control-plane"


def _write_sanitized_secret_restore_dir(source_dir: Path, target_dir: Path) -> int:
    count = 0
    for source in sorted([*source_dir.glob("*.yaml"), *source_dir.glob("*.yml")]):
        docs: list[dict[str, object]] = []
        for raw_doc in yaml.safe_load_all(source.read_text(encoding="utf-8")):
            if raw_doc is None:
                continue
            if not isinstance(raw_doc, dict) or raw_doc.get("kind") != "Secret":
                raise RuntimeError(
                    f"backup secret restore file must contain Secret docs: {source}",
                )
            doc = dict(raw_doc)
            metadata = dict(doc.get("metadata") or {})
            for key in _RUNTIME_METADATA_KEYS:
                metadata.pop(key, None)
            annotations = dict(metadata.get("annotations") or {})
            annotations.pop(_LAST_APPLIED_ANNOTATION, None)
            if annotations:
                metadata["annotations"] = annotations
            else:
                metadata.pop("annotations", None)
            doc["metadata"] = metadata
            docs.append(doc)
        if not docs:
            continue
        (target_dir / source.name).write_text(
            yaml.safe_dump_all(docs, sort_keys=False),
            encoding="utf-8",
        )
        count += len(docs)
    return count


def _last_error(result: object, default: str) -> str:
    stderr = str(getattr(result, "stderr", "") or "")
    return stderr.strip().splitlines()[-1] if stderr.strip() else default


class KindClusterStep(BaseStep):
    number = 3
    name = "kind-cluster"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_name": ctx.cluster_name,
            "namespace": ctx.namespace,
            "rollout_root": str(ctx.rollout_root),
            "cluster_config_path": str(ctx.cluster_config_path),
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "ingress_nginx_recovery_contract": "node-label-and-admission-endpoint-v1",
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
        ingress_node_label = run_captured(
            [
                "kubectl",
                "get",
                "node",
                _control_plane_node_name(ctx),
                "-o",
                "jsonpath={.metadata.labels.ingress-ready}",
            ],
        )
        if (
            ingress_node_label.returncode != 0
            or ingress_node_label.stdout.strip() != "true"
        ):
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
        if secrets.returncode != 0:
            return VerifyOutcome.MISMATCH
        ingress_class = run_captured(["kubectl", "get", "ingressclass", "nginx"])
        if ingress_class.returncode != 0:
            return VerifyOutcome.MISMATCH
        ingress_controller = run_captured(
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
        if ingress_controller.returncode != 0:
            return VerifyOutcome.MISMATCH
        admission_endpoint = run_captured(
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "get",
                "endpoints",
                "ingress-nginx-controller-admission",
                "-o",
                "jsonpath={.subsets[0].addresses[0].ip}",
            ],
        )
        if admission_endpoint.returncode != 0 or not admission_endpoint.stdout.strip():
            return VerifyOutcome.MISMATCH
        try:
            storage_docs = _worker_trajectories_storage_docs(ctx)
        except RuntimeError:
            return VerifyOutcome.UNKNOWN
        if storage_docs:
            pv_name = str(storage_docs[0]["metadata"]["name"])  # type: ignore[index]
            pv = run_captured(["kubectl", "get", "pv", pv_name])
            if pv.returncode != 0:
                return VerifyOutcome.MISMATCH
            pvc = run_captured(
                [
                    "kubectl",
                    "-n",
                    ctx.namespace,
                    "get",
                    "pvc",
                    "loom-worker-trajectories",
                ],
            )
            if pvc.returncode != 0:
                return VerifyOutcome.MISMATCH
        return VerifyOutcome.MATCH

    def _ensure_ingress_ready_node(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> RunResult | None:
        label = run_captured(
            [
                "kubectl",
                "label",
                "node",
                _control_plane_node_name(ctx),
                "ingress-ready=true",
                "--overwrite",
            ],
            stdout_log=step_dir.artifact_path("ingress-node-label.stdout"),
            stderr_log=step_dir.artifact_path("ingress-node-label.stderr"),
        )
        if label.returncode != 0:
            return RunResult(
                exit_code=label.returncode,
                error=_last_error(label, "kubectl label ingress-ready node failed"),
            )
        return None

    def _ensure_ingress_nginx(self, step_dir: StepDir) -> tuple[RunResult | None, str]:
        ingress_class = run_captured(
            ["kubectl", "get", "ingressclass", "nginx"],
            stdout_log=step_dir.artifact_path("ingressclass-get.stdout"),
            stderr_log=step_dir.artifact_path("ingressclass-get.stderr"),
        )
        ingress_controller = run_captured(
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "get",
                "deployment",
                "ingress-nginx-controller",
            ],
            stdout_log=step_dir.artifact_path("ingress-controller-get.stdout"),
            stderr_log=step_dir.artifact_path("ingress-controller-get.stderr"),
        )

        installed = False
        if ingress_class.returncode != 0 or ingress_controller.returncode != 0:
            apply = run_captured(
                ["kubectl", "apply", "-f", str(INGRESS_NGINX_KIND_MANIFEST)],
                stdout_log=step_dir.artifact_path("ingress-nginx-apply.stdout"),
                stderr_log=step_dir.artifact_path("ingress-nginx-apply.stderr"),
            )
            if apply.returncode != 0:
                return (
                    RunResult(
                        exit_code=apply.returncode,
                        error=_last_error(apply, "kubectl apply ingress-nginx failed"),
                    ),
                    "failed",
                )
            installed = True

        wait_pod = run_captured(
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "wait",
                "--for=condition=Ready",
                "pod",
                f"--selector={INGRESS_NGINX_CONTROLLER_SELECTOR}",
                "--timeout=180s",
            ],
            stdout_log=step_dir.artifact_path("ingress-controller-pod-wait.stdout"),
            stderr_log=step_dir.artifact_path("ingress-controller-pod-wait.stderr"),
        )
        if wait_pod.returncode != 0:
            return (
                RunResult(
                    exit_code=wait_pod.returncode,
                    error=_last_error(wait_pod, "ingress-nginx controller pod not ready"),
                ),
                "failed",
            )

        admission_endpoint = run_captured(
            [
                "kubectl",
                "-n",
                "ingress-nginx",
                "get",
                "endpoints",
                "ingress-nginx-controller-admission",
                "-o",
                "jsonpath={.subsets[0].addresses[0].ip}",
            ],
            stdout_log=step_dir.artifact_path("ingress-admission-endpoint.stdout"),
            stderr_log=step_dir.artifact_path("ingress-admission-endpoint.stderr"),
        )
        if admission_endpoint.returncode != 0 or not admission_endpoint.stdout.strip():
            return (
                RunResult(
                    exit_code=admission_endpoint.returncode or 1,
                    error=_last_error(
                        admission_endpoint,
                        "ingress-nginx admission endpoint missing",
                    ),
                ),
                "failed",
            )

        final_ingress_class = run_captured(
            ["kubectl", "get", "ingressclass", "nginx"],
            stdout_log=step_dir.artifact_path("ingressclass-final.stdout"),
            stderr_log=step_dir.artifact_path("ingressclass-final.stderr"),
        )
        if final_ingress_class.returncode != 0:
            return (
                RunResult(
                    exit_code=final_ingress_class.returncode,
                    error=_last_error(final_ingress_class, "ingressclass nginx missing"),
                ),
                "failed",
            )
        return None, "installed" if installed else "reused"

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
                    error=_last_error(create, "kind create cluster failed"),
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
                error=_last_error(export, "kind export kubeconfig failed"),
            )

        label_result = self._ensure_ingress_ready_node(ctx, step_dir)
        if label_result is not None:
            return label_result

        ingress_result, ingress_state = self._ensure_ingress_nginx(step_dir)
        if ingress_result is not None:
            return ingress_result

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
                    error=_last_error(create_ns, "kubectl create namespace failed"),
                )
            namespace_created = True

        storage_state = "skipped"
        storage_manifest = step_dir.artifact_path("worker-trajectories-storage.yaml")
        try:
            should_apply_storage = _write_worker_trajectories_storage_manifest(
                ctx,
                storage_manifest,
            )
        except RuntimeError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))
        if should_apply_storage:
            apply_storage = run_captured(
                [
                    "kubectl",
                    "-n",
                    ctx.namespace,
                    "apply",
                    "-f",
                    str(storage_manifest),
                ],
                stdout_log=step_dir.artifact_path("worker-trajectories-storage-apply.stdout"),
                stderr_log=step_dir.artifact_path("worker-trajectories-storage-apply.stderr"),
            )
            if apply_storage.returncode != 0:
                return RunResult(
                    exit_code=apply_storage.returncode,
                    error=_last_error(
                        apply_storage,
                        "kubectl apply worker trajectories storage failed",
                    ),
                )
            storage_state = "applied"

        with tempfile.TemporaryDirectory(prefix="loom-secret-restore-") as tmp:
            sanitized_dir = Path(tmp)
            try:
                secret_count = _write_sanitized_secret_restore_dir(
                    secrets_dir,
                    sanitized_dir,
                )
            except RuntimeError as exc:
                step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
                return RunResult(exit_code=2, error=str(exc))
            apply_secrets = run_captured(
                [
                    "kubectl",
                    "-n",
                    ctx.namespace,
                    "apply",
                    "--server-side",
                    "--force-conflicts",
                    "--field-manager=loom-rollout-secret-restore",
                    "-f",
                    str(sanitized_dir),
                ],
                stdout_log=step_dir.artifact_path("secrets-apply.stdout"),
                stderr_log=step_dir.artifact_path("secrets-apply.stderr"),
            )
        if apply_secrets.returncode != 0:
            return RunResult(
                exit_code=apply_secrets.returncode,
                error=_last_error(apply_secrets, "kubectl apply secrets failed"),
            )

        summary = (
            f"{'created' if created else 'reused'} kind cluster "
            f"{ctx.cluster_name}; "
            f"{ingress_state} ingress-nginx; "
            f"{'created' if namespace_created else 'reused'} namespace "
            f"{ctx.namespace}; {storage_state} worker trajectories storage; "
            f"restored {secret_count} k8s secrets from backup manifest"
        )
        step_dir.stdout_path().write_text(summary + "\n", encoding="utf-8")
        return RunResult(
            exit_code=0,
            summary=summary,
            artifacts={
                "cluster_name": ctx.cluster_name,
                "namespace": ctx.namespace,
                "ingress_nginx": ingress_state,
                "worker_trajectories_storage": storage_state,
                "worker_trajectories_storage_manifest": str(storage_manifest),
                "secret_count": str(secret_count),
                "secrets_dir": str(secrets_dir),
            },
        )
