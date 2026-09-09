#!/usr/bin/env python3
"""Plan or apply operator-reviewed manifests for the Nebius integration platform."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "src"))

from loom.nebius_platform_render import canonical, validate_environment  # noqa: E402

PHASE_FILES = (
    "00-namespaces.yaml",
    "10-config-network.yaml",
    "20-database.yaml",
    "30-migrate.yaml",
    "40-services.yaml",
    "50-configure.yaml",
    "60-execution.yaml",
    "70-public.yaml",
    "80-backup.yaml",
)


class DeploymentError(ValueError):
    """A deployment boundary failed; its message contains no secret values."""


def load_render(
    render_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Read operator-reviewed manifests once; namespace boundaries remain explicit."""
    files = {
        filename: list(yaml.safe_load_all((render_dir / filename).read_text()))
        for filename in PHASE_FILES
    }
    configs = [
        row
        for row in files["10-config-network.yaml"]
        if row["kind"] == "ConfigMap" and row["metadata"]["name"] == "loom-platform-config"
    ]
    if len(configs) != 1:
        raise DeploymentError("platform configuration identity is ambiguous")
    config = json.loads(configs[0]["data"]["environment.json"])
    validate_environment(config)
    namespaces = {config["namespace"], config["execution_namespace"]}
    for filename, rows in files.items():
        for row in rows:
            metadata = row["metadata"]
            namespace = metadata.get("namespace")
            if row["kind"] == "Namespace":
                namespace = metadata["name"]
            if row["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                if metadata["name"] != config["execution_namespace"] + "-collector":
                    raise DeploymentError(
                        "cluster resource does not belong to the integration target"
                    )
                continue
            if namespace not in namespaces:
                raise DeploymentError(
                    f"rendered resource targets a different namespace: {filename}"
                )
    profile = json.loads(configs[0]["data"]["profile.json"])
    deployment = {
        "candidate_sha": profile["candidate_sha"],
        "cluster_id": config["cluster_id"],
        "namespace": config["namespace"],
        "execution_namespace": config["execution_namespace"],
        "target_id": config["target_id"],
        "public_origin": "https://" + config["public_host"],
    }
    return deployment, config, files


def secret_requirements(
    files: dict[str, list[dict[str, Any]]], config: dict[str, Any]
) -> dict[tuple[str, str], set[str]]:
    result: dict[tuple[str, str], set[str]] = {}

    def walk(value: Any, namespace: str) -> None:
        if isinstance(value, list):
            for child in value:
                walk(child, namespace)
        elif isinstance(value, dict):
            ref = value.get("secretKeyRef")
            if isinstance(ref, dict) and not ref.get("optional", False):
                result.setdefault((namespace, ref["name"]), set()).add(ref["key"])
            secret = value.get("secret")
            if isinstance(secret, dict) and not secret.get("optional", False):
                name = secret.get("secretName", secret.get("name"))
                if name:
                    keys = result.setdefault((namespace, name), set())
                    keys.update(item["key"] for item in secret.get("items", []))
                    if name in {config["tls_secret_name"], config["db_tls_secret_name"]}:
                        keys.update(("tls.crt", "tls.key"))
                    if name == "loom-admin-secret":
                        keys.add("secrets.toml")
            for child in value.values():
                walk(child, namespace)

    for rows in files.values():
        for doc in rows:
            namespace = doc["metadata"].get("namespace")
            if namespace:
                walk(doc, namespace)
    return result


class Kubectl:
    def __init__(self, kubeconfig: Path):
        self.kubeconfig = kubeconfig

    def run(self, *args: str, timeout: int = 90) -> str:
        result = subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                *([] if args[0] in {"wait", "rollout"} else ["--request-timeout=30s"]),
                *args,
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode:
            # Preserve API reason codes without serializing message bodies,
            # URLs, Secret data, or arbitrary application logs.
            match = re.search(r"Error from server \(([A-Za-z]{1,64})\)", result.stderr)
            reason = (
                match.group(1)
                if match
                else (
                    "DeadlineExceeded" if "timed out waiting" in result.stderr else "CommandFailed"
                )
            )
            raise DeploymentError(
                f"kubectl {args[0]} failed with exit code {result.returncode}: {reason}"
            )
        return result.stdout.strip()

    def get(self, kind: str, name: str, namespace: str) -> dict[str, Any]:
        result = self.run("get", kind, name, "-n", namespace, "--ignore-not-found", "-o", "json")
        return json.loads(result) if result else {}


def job_complete(job: dict[str, Any]) -> bool:
    return any(
        condition.get("type") == "Complete" and condition.get("status") == "True"
        for condition in job.get("status", {}).get("conditions", [])
    )


def job_failed(job: dict[str, Any]) -> bool:
    return any(
        condition.get("type") == "Failed" and condition.get("status") == "True"
        for condition in job.get("status", {}).get("conditions", [])
    )


def wait_for_job(kube: Kubectl, name: str, namespace: str, seconds: int) -> None:
    """Return on either terminal condition, preserving failures for diagnosis."""
    deadline = time.monotonic() + seconds
    while True:
        job = kube.get("job", name, namespace)
        if job_failed(job):
            raise DeploymentError(f"job {name} reached Failed condition")
        if job_complete(job):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeploymentError(f"job {name} completion timed out")
        time.sleep(min(5, remaining))


def verify_cluster_identity(
    kube: Kubectl,
    config: dict[str, Any],
    expected_cluster_id: str,
) -> None:
    """Check the existing independent target before deployment or maintenance."""
    if config["cluster_id"] != expected_cluster_id:
        raise DeploymentError("expected cluster id does not match environment")
    view = json.loads(kube.run("config", "view", "--minify", "-o", "json"))
    clusters = view.get("clusters", [])
    if (
        len(clusters) != 1
        or clusters[0]["cluster"].get("server") != config["kubernetes_api_server"]
    ):
        raise DeploymentError("selected kubeconfig API server does not match environment")
    cluster = clusters[0]["cluster"]
    if not clusters[0]["name"].endswith(expected_cluster_id.removeprefix("mk8s")):
        raise DeploymentError(
            "selected kubeconfig cluster identity does not match expected Nebius cluster"
        )
    if cluster.get("insecure-skip-tls-verify") or not (
        cluster.get("certificate-authority") or cluster.get("certificate-authority-data")
    ):
        raise DeploymentError("selected cluster must use its trusted certificate authority")
    nodes = json.loads(
        kube.run("get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json")
    )["items"]
    if not nodes or not any(
        node["metadata"].get("labels", {}).get("loom.nebius/node-role") == "system"
        for node in nodes
    ):
        raise DeploymentError("independent integration system nodes are not provisioned")
    for node in nodes:
        provider_id = node.get("spec", {}).get("providerID", "")
        if re.fullmatch(r"nebius://computeinstance-[a-z0-9]+", provider_id) is None or node[
            "metadata"
        ]["name"] != provider_id.removeprefix("nebius://"):
            raise DeploymentError("integration node provider identity is not Nebius")


def preflight(
    kube: Kubectl,
    manifest: dict[str, Any],
    config: dict[str, Any],
    files: dict[str, list[dict[str, Any]]],
    expected_cluster_id: str,
) -> dict[str, Any]:
    verify_cluster_identity(kube, config, expected_cluster_id)
    for (namespace, secret), required in sorted(secret_requirements(files, config).items()):
        # Return only names of populated keys, never secret values.
        observed = kube.run(
            "get",
            "secret",
            secret,
            "-n",
            namespace,
            "-o",
            r'go-template={{range $key,$value := .data}}{{if $value}}{{$key}}{{"\n"}}{{end}}{{end}}',
        )
        if not observed or not required <= set(observed.splitlines()):
            raise DeploymentError(f"required secret keys missing: {namespace}/{secret}")
    ns = config["namespace"]
    database = kube.get("statefulset", "loom-postgres", ns)
    pvc = kube.get("pvc", "data-loom-postgres-0", ns)
    if pvc and not database:
        raise DeploymentError(
            "existing database volume has no StatefulSet; use the explicit restore procedure"
        )
    migration = files["30-migrate.yaml"][0]
    migration_job = kube.get("job", migration["metadata"]["name"], ns)
    current = kube.get("configmap", "loom-platform-config", ns)
    same_candidate = False
    try:
        same_candidate = (
            json.loads(current["data"]["profile.json"])["candidate_sha"]
            == manifest["candidate_sha"]
            and json.loads(current["data"]["environment.json"]) == config
        )
    except (KeyError, ValueError, TypeError):
        pass
    backup_required = bool(database) and not (same_candidate and job_complete(migration_job))
    if backup_required and not kube.get("cronjob", "loom-platform-backup", ns):
        raise DeploymentError("upgrade requires an existing working backup CronJob before mutation")
    return {
        "database_exists": bool(database),
        "backup_required": backup_required,
        "migration_complete": job_complete(migration_job),
        "migration_failed": job_failed(migration_job),
    }


def _public_smoke_once(origin: str, environment: str) -> None:
    for path in ("/api/v1/health", "/loom-frontend-config.json"):
        with urllib.request.urlopen(origin + path, timeout=30) as response:
            if response.status != 200 or response.geturl() != origin + path:
                raise DeploymentError("public HTTPS smoke did not reach the intended endpoint")
            payload = json.loads(response.read(1024 * 1024))
            if not isinstance(payload, dict):
                raise DeploymentError("public HTTPS smoke did not return JSON")
            if path.endswith("config.json") and (
                payload.get("environment") != environment
                or payload.get("apiRouteBase") != origin + "/api"
            ):
                raise DeploymentError("public frontend points to a different environment")


def public_smoke(origin: str, environment: str) -> None:
    deadline = time.monotonic() + 180
    while True:
        try:
            _public_smoke_once(origin, environment)
            return
        except (OSError, ValueError):
            if time.monotonic() >= deadline:
                raise DeploymentError(
                    "public HTTPS API/frontend smoke failed after readiness interval"
                ) from None
            time.sleep(5)


def deploy(args: argparse.Namespace, *, kube: Kubectl | None = None) -> dict[str, Any]:
    snapshot = tempfile.TemporaryDirectory(prefix="loom-nebius-deploy-")
    snapshot_root = Path(snapshot.name)
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = args.evidence_dir / f"deployment-{uuid.uuid4().hex}.json"
    evidence: dict[str, Any] = {
        "schema_version": "loom.nebius-deployment.v1",
        "mode": "apply" if args.apply else "plan",
        "status": "running",
        "phases": [],
    }

    def phase(name: str) -> None:
        evidence["phases"].append({"name": name, "started_at": datetime.now(UTC).isoformat()})
        evidence_path.write_bytes(canonical(evidence))
        print(f"Nebius deployment: {name}", flush=True)

    try:
        phase("read-render")
        manifest, config, files = load_render(args.render_dir)
        for filename, rows in files.items():
            (snapshot_root / filename).write_bytes(
                yaml.safe_dump_all(rows, sort_keys=False).encode()
            )
        evidence.update(
            {
                key: manifest[key]
                for key in (
                    "candidate_sha",
                    "cluster_id",
                    "namespace",
                    "execution_namespace",
                )
            }
        )
        kube = kube or Kubectl(args.kubeconfig)
        phase("preflight")
        state = preflight(kube, manifest, config, files, args.expected_cluster_id)
        evidence["preflight"] = state
        if not args.apply:
            evidence["status"] = "planned"
            return evidence
        if state["migration_failed"] and not args.retry_failed_jobs:
            raise DeploymentError(
                "candidate migration previously failed; fix the cause and explicitly retry failed jobs"
            )
        ns = config["namespace"]

        def apply_file(filename: str) -> None:
            phase(filename.removesuffix(".yaml"))
            kube.run("apply", "-f", str(snapshot_root / filename))

        def wait_job(name: str, seconds: int = 660) -> None:
            wait_for_job(kube, name, ns, seconds)

        def run_job(filename: str) -> None:
            name = files[filename][0]["metadata"]["name"]
            existing = kube.get("job", name, ns)
            if job_complete(existing):
                phase(filename.removesuffix(".yaml") + "-already-complete")
                return
            if job_failed(existing):
                if not args.retry_failed_jobs:
                    raise DeploymentError(
                        "candidate job previously failed; fix the cause and explicitly retry failed jobs"
                    )
                phase("retry-failed-job-" + name)
                kube.run("delete", "job", name, "-n", ns, "--cascade=foreground", "--wait=true")
            if not existing or job_failed(existing):
                apply_file(filename)
            wait_job(name)

        if state["backup_required"]:
            phase("pre-upgrade-backup")
            backup_name = (
                "loom-predeploy-" + manifest["candidate_sha"][:12] + "-" + uuid.uuid4().hex[:8]
            )
            evidence["backup_job"] = backup_name
            kube.run("create", "job", backup_name, "--from=cronjob/loom-platform-backup", "-n", ns)
            wait_job(backup_name, 1860)
        for filename in ("00-namespaces.yaml", "10-config-network.yaml", "20-database.yaml"):
            apply_file(filename)
        phase("database-ready")
        kube.run(
            "rollout",
            "status",
            "statefulset/loom-postgres",
            "-n",
            ns,
            "--timeout=600s",
            timeout=640,
        )
        apply_file("80-backup.yaml")
        run_job("30-migrate.yaml")
        apply_file("40-services.yaml")
        for name in ("loom-service", "loom-control-plane", "loom-llm-gateway", "loom-web"):
            kube.run(
                "rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=300s", timeout=340
            )
        run_job("50-configure.yaml")
        apply_file("60-execution.yaml")
        for actuator in [
            "loom-execution-actuator",
            *[
                target["target_id"] + "-actuator"
                for target in config.get("regional_execution_targets", [])
            ],
        ]:
            kube.run(
                "rollout",
                "status",
                "deployment/" + actuator,
                "-n",
                config["execution_namespace"],
                "--timeout=300s",
                timeout=340,
            )
        apply_file("70-public.yaml")
        phase("public-load-balancer-ready")
        kube.run(
            "wait",
            "--for=jsonpath={.status.loadBalancer.ingress}",
            "service/loom-web",
            "-n",
            ns,
            "--timeout=300s",
            timeout=340,
        )
        phase("public-https-smoke")
        public_smoke(manifest["public_origin"], config["environment"])
        evidence["status"] = "complete"
        return evidence
    except Exception as exc:
        evidence["status"] = "failed"
        if kube is not None and "config" in locals():
            failures: list[dict[str, Any]] = []
            for namespace in (config["namespace"], config["execution_namespace"]):
                try:
                    pods = json.loads(kube.run("get", "pods", "-n", namespace, "-o", "json"))
                    for pod in pods.get("items", []):
                        status = pod.get("status", {})
                        for container in status.get("initContainerStatuses", []) + status.get(
                            "containerStatuses", []
                        ):
                            for state_name in ("waiting", "terminated"):
                                state = container.get("state", {}).get(state_name, {})
                                reason = state.get("reason", "")
                                if (
                                    re.fullmatch(r"[A-Za-z]{1,64}", reason)
                                    and reason != "Completed"
                                ):
                                    failure = {"namespace": namespace, "reason": reason}
                                    if isinstance(state.get("exitCode"), int):
                                        failure["exit_code"] = state["exitCode"]
                                    failures.append(failure)
                        for condition in status.get("conditions", []):
                            reason = condition.get("reason", "")
                            if condition.get("status") == "False" and re.fullmatch(
                                r"[A-Za-z]{1,64}", reason
                            ):
                                failures.append({"namespace": namespace, "reason": reason})
                except Exception:
                    failures.append({"namespace": namespace, "reason": "DiagnosticUnavailable"})
            evidence["workload_failures"] = failures

        evidence["error_type"] = type(exc).__name__
        if isinstance(exc, DeploymentError):
            evidence["reason"] = str(exc)
        raise
    finally:
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        evidence_path.write_bytes(canonical(evidence))
        snapshot.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("render-dir", "kubeconfig", "evidence-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--retry-failed-jobs", action="store_true")
    args = parser.parse_args()
    try:
        result = deploy(args)
    except Exception as exc:
        print(
            f"Nebius deployment failed ({type(exc).__name__}); inspect sanitized phase evidence",
            file=sys.stderr,
        )
        return 1
    print("Nebius deployment " + result["status"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
