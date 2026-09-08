#!/usr/bin/env python3
"""Seed and reconcile the explicit staging attachment; never deploy canonical workloads.

Install from the reviewed release through the authorized installer. Kubeconfigs
and this configuration remain protected files; only aggregate results reach logs.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import ssl
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

import yaml  # type: ignore[import-untyped]

SOURCE = "loom-staging"
DESTINATION = "loom-nebius-staging"
OWNER = {"loom.ca/nebius-credentials": "staging-v1"}
REVISION = "loom.ca/nebius-credential-revision"
CA_PATH = "/var/run/loom/postgres-tls/ca.crt"


class ReconcileError(RuntimeError):
    """A secret-safe reconciliation failure."""


def run(argv: list[str], *, payload: bytes | None = None) -> bytes:
    result = subprocess.run(argv, input=payload, capture_output=True, timeout=90, check=False)
    if result.returncode:
        raise ReconcileError("credential operation failed; no remote diagnostics exported")
    return result.stdout


def private_json(path: Path) -> dict[str, Any]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ReconcileError("input must be an owner-only regular file")
    try:
        result = json.loads(path.read_bytes())
    except (ValueError, OSError):
        raise ReconcileError("credential input is invalid") from None
    if not isinstance(result, dict):
        raise ReconcileError("credential input must be an object")
    return result


class Kubernetes:
    def __init__(self, kubeconfig: str, namespace: str, uid: str):
        self.prefix = ["kubectl", "--kubeconfig", kubeconfig, "--request-timeout=30s"]
        self.namespace = namespace
        observed = self.get("namespace", namespace, namespaced=False)
        if not uid or not observed or observed["metadata"]["uid"] != uid:
            raise ReconcileError("cluster/namespace identity does not match approved binding")

    def command(self, args: list[str], payload: bytes | None = None) -> bytes:
        return run([*self.prefix, "-n", self.namespace, *args], payload=payload)

    def get(self, kind: str, name: str, *, namespaced: bool = True) -> dict[str, Any] | None:
        argv = self.prefix + (["-n", self.namespace] if namespaced else [])
        raw = run([*argv, "get", kind, name, "--ignore-not-found", "-o", "json"])
        return json.loads(raw) if raw.strip() else None

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None:
        if namespace != self.namespace:
            raise ReconcileError("namespace outside credential binding")
        return self.get("secret", name)

    def create_secret(self, document: dict[str, Any]) -> bool:
        if document["metadata"]["namespace"] != self.namespace:
            raise ReconcileError("namespace outside credential binding")
        # A failed create can mean a competing bootstrap won. The bootstrap
        # adapter rereads and verifies; it must never replace another seed.
        try:
            self.command(["create", "-f", "-"], json.dumps(document).encode())
            return True
        except ReconcileError:
            if self.get("secret", document["metadata"]["name"]) is None:
                raise
            return False

    def exec_control_plane(self, source: str, input_payload: bytes) -> bytes:
        return self.command(
            ["exec", "-i", "deployment/loom-control-plane", "--", "python", "-c", source],
            input_payload,
        )

    def put(self, name: str, values: dict[str, str]) -> bool:
        desired = {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}
        current = self.get("secret", name)
        if current is not None:
            if any(current["metadata"].get("labels", {}).get(k) != v for k, v in OWNER.items()):
                raise ReconcileError("refusing to overwrite an unmanaged Secret")
            if current.get("data", {}) == desired:
                return False
            # Preserve unrelated metadata and use resourceVersion for optimistic
            # concurrency. A conflict fails this pass; the timer retries.
            current["data"] = desired
            self.command(["replace", "-f", "-"], json.dumps(current).encode())
        else:
            self.command(
                ["create", "-f", "-"],
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "Secret",
                        "type": "Opaque",
                        "metadata": {"name": name, "namespace": self.namespace, "labels": OWNER},
                        "data": desired,
                    }
                ).encode(),
            )
        observed = self.get("secret", name)
        if not observed or observed.get("data") != desired:
            raise ReconcileError("Secret readback did not converge")
        return True


def values(kube: Kubernetes, name: str, keys: list[str]) -> dict[str, str]:
    document = kube.get("secret", name)
    if document is None:
        raise ReconcileError("required source Secret is absent or incomplete")
    try:
        result = {
            key: base64.b64decode(document["data"][key], validate=True).decode() for key in keys
        }
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise ReconcileError("required source Secret is absent or incomplete") from None
    if any(not value for value in result.values()):
        raise ReconcileError("required source Secret has an empty value")
    return result


def validate_attachment(attachment: dict[str, Any]) -> None:
    from loom.nebius_staging_attachment import validate_staging_attachment

    validate_staging_attachment(
        attachment,
        environment="staging",
        target={"target_id": "nebius-eu-north1-staging", "namespace_name": DESTINATION},
    )
    if "database_tls" not in attachment or attachment["canonical_database"] != "loom":
        raise ReconcileError("credential reconciliation requires canonical loom and database TLS")


def database_url(server: str, username: str, password: str) -> str:
    return (
        f"postgresql+psycopg://{quote(username, safe='')}:{quote(password, safe='')}"
        f"@{server}:15432/loom?sslmode=verify-full&sslrootcert={CA_PATH}"
    )


def source_payloads(source: Kubernetes, attachment: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Read one canonical snapshot. No target mutation occurs until validation passes."""
    validate_attachment(attachment)
    cp = source.get("deployment", "loom-control-plane")
    if (
        not cp
        or cp["spec"]["template"]["metadata"]
        .get("annotations", {})
        .get("loom.ca/nebius-configuration-revision")
        != attachment["configuration_revision"]
    ):
        raise ReconcileError("canonical Control Plane is not the approved attachment revision")
    cp_status = cp.get("status", {})
    cp_replicas = cp["spec"].get("replicas", 1)
    if (
        cp_replicas < 1
        or cp_status.get("observedGeneration") != cp["metadata"]["generation"]
        or any(
            cp_status.get(key, 0) != cp_replicas
            for key in ("updatedReplicas", "readyReplicas", "availableReplicas", "replicas")
        )
    ):
        raise ReconcileError("canonical Control Plane rollout has not converged")
    result: dict[str, dict[str, str]] = {}

    def add(name: str, data: dict[str, str]) -> None:
        # Multiple references may intentionally share a Secret, but not conflict.
        previous = result.setdefault(name, {})
        for key, value in data.items():
            if key in previous and previous[key] != value:
                raise ReconcileError("credential references collide")
            previous[key] = value

    db = attachment["canonical"]["db_secret"]
    tls = attachment["database_tls"]
    for consumer in ("gateway", "actuator"):
        identity = values(source, f"loom-nebius-staging-db-{consumer}", ["username", "password"])
        if identity["username"] != f"loom_nebius_staging_{consumer}":
            raise ReconcileError("database service identity differs from CNPG binding")
        add(
            db["name"],
            {
                db[f"{consumer}_key"]: database_url(
                    tls["server_name"], identity["username"], identity["password"]
                )
            },
        )
    ca = values(source, "loom-postgres-ca", ["ca.crt"])["ca.crt"]
    try:
        ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_verify_locations(cadata=ca)
    except (ssl.SSLError, ValueError):
        raise ReconcileError("canonical PostgreSQL CA is invalid") from None
    add(tls["ca_secret"]["name"], {tls["ca_secret"]["key"]: ca})
    for section, secret in (
        ("source", "loom-nebius-staging-spool"),
        ("canonical", "loom-nebius-staging-canonical-inputs"),
    ):
        credentials = values(source, secret, ["access-key", "secret-key"])
        reference = attachment[section][
            "credentials_secret" if section == "source" else "storage_secret"
        ]
        add(
            reference["name"],
            {
                reference["access_key"]: credentials["access-key"],
                reference["secret_key"]: credentials["secret-key"],
            },
        )
    spool = values(source, "loom-nebius-staging-spool", ["endpoint", "region", "bucket"])
    if any(spool[field] != attachment["source"][field] for field in spool):
        raise ReconcileError("canonical and remote spool bindings differ")
    collector = attachment["collector"]
    token = values(source, "loom-nebius-staging-collector", ["token"])["token"]
    if not re.fullmatch(r"loom_ecc_[0-9a-f]{64}", token):
        raise ReconcileError("collector source identity is invalid")
    add(collector["token_secret"]["name"], {collector["token_secret"]["key"]: token})
    observer = values(source, "loom-nebius-staging-observer", ["credentials.json"])
    add(
        collector["nebius_secret"]["name"],
        {collector["nebius_secret"]["key"]: observer["credentials.json"]},
    )

    # Read actual running canonical Gateway settings, not newer Secret values
    # that a protected canonical rollout has not yet activated.
    before = source.get("deployment", "loom-llm-gateway")
    if (
        not before
        or before.get("status", {}).get("observedGeneration") != before["metadata"]["generation"]
    ):
        raise ReconcileError("canonical Gateway generation is not observed")
    desired = before["spec"].get("replicas", 1)
    status = before.get("status", {})
    if desired < 1 or any(
        status.get(key, 0) != desired
        for key in ("updatedReplicas", "readyReplicas", "replicas", "availableReplicas")
    ):
        raise ReconcileError("canonical Gateway rollout has not converged")
    env = json.loads(
        source.command(
            [
                "exec",
                "deployment/loom-llm-gateway",
                "--",
                "python",
                "-c",
                "import json,os; print(json.dumps({k:v for k,v in os.environ.items() if "
                "k in ('LOOM_GW_STEP_JWT_SIGNING_KEY','LOOM_SECRET_STORE_MASTER_KEY') "
                "or k.startswith('LOOM_GW_LOCAL_')}))",
            ]
        )
    )
    after = source.get("deployment", "loom-llm-gateway")
    if not after or after["metadata"]["resourceVersion"] != before["metadata"]["resourceVersion"]:
        raise ReconcileError("canonical Gateway changed during snapshot")
    gateway = attachment["gateway_secret"]
    try:
        add(
            gateway["name"],
            {
                gateway["step_jwt_key"]: env.pop("LOOM_GW_STEP_JWT_SIGNING_KEY"),
                gateway["master_key"]: env.pop("LOOM_SECRET_STORE_MASTER_KEY"),
            },
        )
    except KeyError:
        raise ReconcileError("canonical running Gateway identity is unavailable") from None
    if any(not re.fullmatch(r"LOOM_GW_LOCAL_[A-Z0-9_]+_(?:BASE_URL|API_KEY)", key) for key in env):
        raise ReconcileError("canonical provider settings exceed the supported allowlist")
    add(attachment["local_providers_secret_name"], env)
    return result


def reconcile(
    source: Kubernetes, destination: Kubernetes, attachment: dict[str, Any]
) -> dict[str, Any]:
    workloads = (
        ("deployment", "loom-llm-gateway"),
        ("deployment", "loom-execution-actuator"),
        ("cronjob", "loom-execution-capacity-collector"),
    )

    def check_workload(obj: dict[str, Any], kind: str) -> dict[str, str]:
        template = (
            obj["spec"]["jobTemplate"]["spec"]["template"]
            if kind == "cronjob"
            else obj["spec"]["template"]
        )
        annotations = template["metadata"].get("annotations", {})
        if (
            annotations.get("loom.ca/nebius-configuration-revision")
            != attachment["configuration_revision"]
        ):
            raise ReconcileError("remote workload is not the approved attachment revision")
        return cast(dict[str, str], annotations)

    for kind, name in workloads:
        obj = destination.get(kind, name)
        if obj:
            check_workload(obj, kind)
    payloads = source_payloads(source, attachment)
    # Hash full desired data for deterministic retry after a partial write;
    # only the combined high-entropy digest is used as a Pod annotation.
    revision = hashlib.sha256(json.dumps(payloads, sort_keys=True).encode()).hexdigest()
    changed = sum(destination.put(name, data) for name, data in payloads.items())
    # Recheck canonical source before touching Pod templates. A changing source
    # fails this pass; the next timer tick reconciles without minting credentials.
    if source_payloads(source, attachment) != payloads:
        raise ReconcileError("canonical credentials changed during reconciliation")
    rolled = 0
    for kind, name in workloads:
        obj = destination.get(kind, name)
        if not obj:
            continue  # Initial Secret preparation before remote manifest apply.
        annotations = check_workload(obj, kind)
        if annotations.get(REVISION) == revision:
            continue
        path = (
            "/spec/jobTemplate/spec/template/metadata/annotations"
            if kind == "cronjob"
            else "/spec/template/metadata/annotations"
        )
        destination.command(
            ["patch", kind, name, "--type=json", "--patch-file=/dev/stdin"],
            json.dumps(
                [
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": obj["metadata"]["resourceVersion"],
                    },
                    {"op": "add", "path": path, "value": {**annotations, REVISION: revision}},
                ]
            ).encode(),
        )
        rolled += 1
    return {
        "secrets_changed": changed,
        "workloads_reconciled": rolled,
        "canonical_workloads_changed": False,
    }


def seed_inputs(source: Kubernetes, config: dict[str, Any], attachment: dict[str, Any]) -> None:
    from loom.execution_image_admission import (
        ImageAdmissionKeyring,
        verify_execution_image_admission,
    )
    from loom.service_execution_materialization import load_service_execution_runtime_profile

    runtime = private_json(Path(config["runtime_profile_file"]))
    admission = private_json(Path(config["admission_keyring_file"]))
    profile = load_service_execution_runtime_profile(json.dumps(runtime))
    if profile is None:
        raise ReconcileError("runtime profile is empty")
    keyring = ImageAdmissionKeyring.from_json(json.dumps(admission))
    verify_execution_image_admission(
        profile.image_admission,
        required_image_refs=[profile.task_image_ref, profile.runtime_image_ref],
        keyring=keyring,
    )
    observer = private_json(Path(config["observer_credentials_file"]))
    subject = observer.get("subject-credentials", {})
    if (
        subject.get("alg") != "RS256"
        or not all(subject.get(k) for k in ("private-key", "kid", "iss", "sub"))
        or subject["iss"] != subject["sub"]
    ):
        raise ReconcileError("observer requires a renewable service-account credential")
    output = private_json(Path(config["terraform_spool_output"]))
    # Accept `terraform output -json staging_spool`, not an unreviewed bucket/key pair.
    if (
        output.get("environment") != "staging"
        or output.get("target_id") != attachment["target_id"]
        or output.get("secret_delivery_mode") != "EXPLICIT"
    ):
        raise ReconcileError("Terraform spool output is not the approved staging identity")
    if any(
        output.get(src) != attachment["source"][dst]
        for src, dst in (("endpoint", "endpoint"), ("region", "region"), ("bucket_name", "bucket"))
    ):
        raise ReconcileError("Terraform and attachment spool bindings differ")
    response = json.loads(
        run(
            [
                "nebius",
                "--config",
                config["nebius_config"],
                "--profile",
                config["nebius_profile"],
                "--no-browser",
                "--format",
                "json",
                "--timeout",
                "20s",
                "--auth-timeout",
                "20s",
                "--retries",
                "1",
                "--no-check-update",
                "iam",
                "v2",
                "access-key",
                "get-secret",
                "--id",
                output["access_key_resource_id"],
            ]
        )
    )
    if response.get("aws_access_key_id") != output["aws_access_key_id"] or not response.get(
        "secret"
    ):
        raise ReconcileError("Nebius spool key readback differs from Terraform identity")
    source.put(
        "loom-nebius-staging-spool",
        {
            "endpoint": output["endpoint"],
            "region": output["region"],
            "bucket": output["bucket_name"],
            "access-key": response["aws_access_key_id"],
            "secret-key": response["secret"],
        },
    )
    source.put("loom-nebius-staging-observer", {"credentials.json": json.dumps(observer)})
    for value, name, key in (
        (runtime, "loom-nebius-staging-runtime", "runtime-profile-json"),
        (admission, "loom-nebius-staging-admission", "public-keys-json"),
    ):
        source.put(name, {key: json.dumps(value)})


def validate_auth(config: dict[str, Any], *, destination: bool) -> None:
    if config["canonical_kubeconfig"] != "/etc/rancher/k3s/k3s.yaml":
        raise ReconcileError("canonical authentication must use the live k3s kubeconfig")
    path = Path(config["nebius_config"])
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ReconcileError("Nebius configuration must be an owner-only regular file")
    settings = yaml.safe_load(path.read_text())
    profile = settings.get("profiles", {}).get(config["nebius_profile"], {})
    if profile.get("auth-type") != "service account" or not all(
        profile.get(k) for k in ("service-account-id", "public-key-id", "private-key")
    ):
        raise ReconcileError("Nebius requires an explicit service-account authorized-key profile")
    if not destination:
        return
    kube = json.loads(
        run(
            [
                "kubectl",
                "--kubeconfig",
                config["nebius_kubeconfig"],
                "config",
                "view",
                "--minify",
                "-o",
                "json",
            ]
        )
    )
    users = kube.get("users", [])
    if len(users) != 1 or set(users[0].get("user", {})) != {"exec"}:
        raise ReconcileError("destination kubeconfig requires renewable exec authentication")
    plugin = users[0]["user"]["exec"]
    args = plugin.get("args", [])

    def has_option(name: str, value: str) -> bool:
        return args.count(name) == 1 and any(
            args[i : i + 2] == [name, value] for i in range(len(args))
        )

    if (
        Path(plugin.get("command", "")).name != "nebius"
        or not has_option("--config", config["nebius_config"])
        or not has_option("--profile", config["nebius_profile"])
        or "--no-browser" not in args
        or plugin.get("interactiveMode") != "Never"
        or plugin.get("env")
        or any(arg in {"-c", "-p"} or arg.startswith(("--config=", "--profile=")) for arg in args)
    ):
        raise ReconcileError("destination exec authentication is not bound to the service profile")


def configure_kubeconfig(config: dict[str, Any]) -> dict[str, bool]:
    """Generate the exec plugin with explicit service authentication, never a copied token."""
    target = Path(config["nebius_kubeconfig"])
    if target.is_symlink() or not target.parent.is_dir() or target.parent.stat().st_mode & 0o022:
        raise ReconcileError("destination kubeconfig directory must be protected")
    cluster_id = config["nebius_cluster_id"]
    if not re.fullmatch(r"mk8scluster-[a-z0-9]+", cluster_id):
        raise ReconcileError("Nebius cluster ID is invalid")
    with tempfile.TemporaryDirectory(prefix=".loom-nebius-auth-", dir=target.parent) as folder:
        temporary = Path(folder) / "kubeconfig"
        run(
            [
                "nebius",
                "--config",
                config["nebius_config"],
                "--profile",
                config["nebius_profile"],
                "--no-browser",
                "--timeout",
                "20s",
                "--auth-timeout",
                "20s",
                "--no-check-update",
                "mk8s",
                "cluster",
                "get-credentials",
                "--id",
                cluster_id,
                "--internal",
                "--kubeconfig",
                str(temporary),
            ]
        )
        document = yaml.safe_load(temporary.read_text())
        users = document.get("users", [])
        if len(users) != 1 or set(users[0].get("user", {})) != {"exec"}:
            raise ReconcileError("Nebius generated unsupported static authentication")
        plugin = users[0]["user"]["exec"]
        if Path(plugin.get("command", "")).name != "nebius":
            raise ReconcileError("Nebius generated an unexpected exec plugin")
        old = iter(plugin.get("args", []))
        remaining: list[str] = []
        for arg in old:
            if arg in {"--config", "-c", "--profile", "-p"}:
                next(old, None)
            elif arg != "--no-browser":
                if arg.startswith(("--config=", "--profile=")):
                    continue
                remaining.append(arg)
        plugin["args"] = [
            "--config",
            config["nebius_config"],
            "--profile",
            config["nebius_profile"],
            "--no-browser",
            *remaining,
        ]
        plugin["interactiveMode"] = "Never"
        plugin.pop("env", None)
        temporary.write_text(yaml.safe_dump(document))
        temporary.chmod(0o600)
        os.replace(temporary, target)
    return {"renewable_kubeconfig_configured": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("configure", "seed", "bootstrap", "reconcile"))
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    try:
        config = private_json(args.config)
        if config.get("schema_version") != "loom.nebius-staging-credentials.v1":
            raise ReconcileError("credential configuration schema is invalid")
        if os.geteuid() != 0:
            raise ReconcileError("use the authorized root installation")
        validate_auth(config, destination=args.command == "reconcile")
        attachment = private_json(Path(config["attachment_file"]))
        validate_attachment(attachment)
        # One installed timer/one-off bootstrap at a time, including all reads.
        descriptor = os.open(
            args.config.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            source = Kubernetes(
                config["canonical_kubeconfig"], SOURCE, config["canonical_namespace_uid"]
            )
            result: dict[str, Any]
            if args.command == "configure":
                result = configure_kubeconfig(config)
            elif args.command in {"seed", "bootstrap"}:
                from nebius_staging_identity_bootstrap import bootstrap_identities, seed_identities

                seed_inputs(source, config, attachment)
                result = (
                    seed_identities(source)
                    if args.command == "seed"
                    else bootstrap_identities(
                        source,
                        minio_endpoint=config["canonical_minio_endpoint"],
                        minio_resolve=config["canonical_minio_address"],
                        mc_binary=config.get("mc_binary", "mc"),
                    )
                )
            else:
                destination = Kubernetes(
                    config["nebius_kubeconfig"], DESTINATION, config["nebius_namespace_uid"]
                )
                result = reconcile(source, destination, attachment)
        print(json.dumps(result, sort_keys=True))
        return 0
    except ReconcileError as error:
        print(f"staging credential reconciliation failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        # Third-party parser/SDK exceptions may embed credential input. Keep
        # their details out of the journal even for an unexpected failure.
        print(
            "staging credential reconciliation failed; previous live resources were not deleted",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
