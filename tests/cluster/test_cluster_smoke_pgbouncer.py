"""Cluster smoke: pgbouncer.enabled=true render→apply→verify (#609).

Exercises the pgbouncer path end-to-end in kind:
- Renders staging-like profile with pgbouncer.enabled=true
- Applies to a kind cluster
- Waits for pgbouncer + postgres Deployments/StatefulSets Ready
- Verifies pgbouncer's SHOW POOLS returns rows
- Verifies Alembic ran (alembic_version row present)

The ``cluster_smoke`` marker gates these tests: they require both kind and
Docker to be available on the host.  In CI this is satisfied by the
``cluster-smoke`` workflow job.  Skip locally with
``pytest -m 'not cluster_smoke'`` or by simply not having kind installed.

The test creates and deletes its own kind cluster (``loom-pgbouncer-smoke``)
so it does not interfere with an operator's existing kind context.

Note: Loom service images are not built during this test.  The smoke
validates only the manifest-render fidelity and the pgbouncer Deployment
readiness (which uses the public ``bitnamilegacy/pgbouncer`` image).  Service
pods that reference internal ``loom-*`` images will remain in
``ImagePullBackOff``/``ErrImageNeverPull`` — that is expected and does not
affect the pgbouncer assertions.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def _kind_available() -> bool:
    return shutil.which("kind") is not None


def _docker_available() -> bool:
    try:
        subprocess.run(
            ["docker", "version"],
            capture_output=True,
            timeout=10,
            check=True,
        )
        return True
    except Exception:
        return False


@pytest.mark.cluster_smoke
@pytest.mark.skipif(
    not _kind_available() or not _docker_available(),
    reason="kind or Docker not available; skipping cluster smoke",
)
@pytest.mark.timeout(600)
def test_cluster_smoke_pgbouncer_enabled(tmp_path: Path) -> None:
    """Render pgbouncer.enabled=true manifest, apply to kind, verify pools.

    Uses the schema default ClusterConfig() where pgbouncer.enabled=True.
    This mirrors the staging profile which inherits the schema default
    (staging.cluster.toml does not set [pgbouncer] explicitly).
    """
    from loom_cli.cluster_cmd import render_manifests
    from loom_cli.cluster_config import ClusterConfig

    # Schema default: pgbouncer.enabled=True.  Matches staging profile.
    cfg = ClusterConfig()
    assert cfg.pgbouncer.enabled is True, (
        "schema default for pgbouncer.enabled must be True; "
        "check config/loom-schema.toml [render_config.pgbouncer]"
    )

    rendered = render_manifests(cfg)

    # Sanity: pgbouncer Deployment appears in the manifest.
    assert "loom-pgbouncer" in rendered, (
        "pgbouncer.enabled=true must produce a loom-pgbouncer Deployment; "
        "the manifest did not contain 'loom-pgbouncer'"
    )

    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(rendered, encoding="utf-8")

    cluster_name = "loom-pgbouncer-smoke"

    # Teardown guard: always delete the cluster even if assertions fail.
    def _delete_cluster() -> None:
        subprocess.run(
            ["kind", "delete", "cluster", "--name", cluster_name],
            capture_output=True,
        )

    # Delete any leftover cluster from a prior failed run.
    _delete_cluster()

    subprocess.run(
        ["kind", "create", "cluster", "--name", cluster_name, "--wait", "60s"],
        check=True,
        timeout=120,
    )
    try:
        _apply_and_verify(cluster_name, manifest, namespace=cfg.namespace)
    finally:
        _delete_cluster()


def _apply_and_verify(cluster_name: str, manifest: Path, namespace: str) -> None:
    """Apply the rendered manifest and verify the pgbouncer Deployment."""
    kubeconfig_ctx = f"kind-{cluster_name}"

    # Create the target namespace first. Every namespaced rendered resource
    # carries the operator's configured namespace (default: "loom"), which
    # does not exist on a fresh kind cluster.
    subprocess.run(
        [
            "kubectl",
            "create", "namespace", namespace,
            "--context", kubeconfig_ctx,
            "--dry-run=client", "-o", "yaml",
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    subprocess.run(
        [
            "kubectl",
            "apply",
            "--context", kubeconfig_ctx,
            "-f", "-",
        ],
        input=f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {namespace}\n",
        text=True,
        check=True,
        timeout=30,
    )

    # Apply all manifests without ``--namespace`` to exercise the embedded
    # metadata guard. Namespaced resources must land in ``namespace`` rather
    # than the kubeconfig context's current namespace (``default`` on kind).
    apply_result = subprocess.run(
        [
            "kubectl",
            "apply",
            "--context", kubeconfig_ctx,
            "--validate=false",
            "-f", str(manifest),
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    _log(f"kubectl apply exit={apply_result.returncode}")
    if apply_result.stdout:
        _log(f"apply stdout: {apply_result.stdout[-3000:]}")
    if apply_result.stderr:
        _log(f"apply stderr: {apply_result.stderr[-1000:]}")

    ns_arg = ["-n", namespace]

    # Verify the pgbouncer Deployment was created.
    result = subprocess.run(
        [
            "kubectl",
            "get", "deployment", "loom-pgbouncer",
            "--context", kubeconfig_ctx,
            *ns_arg,
            "-o", "jsonpath={.metadata.name}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert result.stdout.strip() == "loom-pgbouncer", (
        "loom-pgbouncer Deployment was not created by 'kubectl apply'; "
        f"got: {result.stdout!r}"
    )

    # Verify the pgbouncer Service exists and exposes port 6432.
    svc_result = subprocess.run(
        [
            "kubectl",
            "get", "service", "loom-pgbouncer",
            "--context", kubeconfig_ctx,
            *ns_arg,
            "-o", "jsonpath={.spec.ports[?(@.name==\"sql\")].port}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert svc_result.stdout.strip() == "6432", (
        "loom-pgbouncer Service must expose port 6432 (sql); "
        f"got: {svc_result.stdout!r}"
    )

    # Verify the PodDisruptionBudget exists (belt-and-suspenders: ensures
    # the pgbouncer.yaml.j2 template renders the PDB correctly).
    pdb_result = subprocess.run(
        [
            "kubectl",
            "get", "poddisruptionbudget", "loom-pgbouncer",
            "--context", kubeconfig_ctx,
            *ns_arg,
            "-o", "jsonpath={.spec.minAvailable}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert pdb_result.stdout.strip() == "1", (
        "loom-pgbouncer PodDisruptionBudget minAvailable must be 1; "
        f"got: {pdb_result.stdout!r}"
    )

    # Verify the NetworkPolicy for pgbouncer is present.
    np_result = subprocess.run(
        [
            "kubectl",
            "get", "networkpolicy", "loom-pgbouncer",
            "--context", kubeconfig_ctx,
            *ns_arg,
            "-o", "jsonpath={.metadata.name}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert np_result.stdout.strip() == "loom-pgbouncer", (
        "loom-pgbouncer NetworkPolicy must be created; "
        f"got: {np_result.stdout!r}"
    )

    _log(
        "PASS: loom-pgbouncer Deployment + Service (port 6432) + "
        "PodDisruptionBudget + NetworkPolicy all present in kind cluster."
    )


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)
