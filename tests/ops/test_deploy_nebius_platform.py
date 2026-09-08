from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import deploy_nebius_platform as deploy
from scripts.ops import nebius_candidate as candidate

from loom.nebius_platform_render import build_platform, canonical, write_platform


@pytest.fixture
def rendered(tmp_path: Path) -> tuple[argparse.Namespace, dict, dict, dict]:
    key = Ed25519PrivateKey.generate()
    signer = tmp_path / "signer.pem"
    signer.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    signer.chmod(0o600)
    trust = {
        "schema_version": 1,
        "keys": [
            {
                "signing_key_id": "publisher",
                "public_key_base64": base64.b64encode(
                    key.public_key().public_bytes(
                        serialization.Encoding.Raw, serialization.PublicFormat.Raw
                    )
                ).decode(),
            }
        ],
    }
    keyring = tmp_path / "trust.json"
    keyring.write_text(json.dumps(trust))
    record = {
        "schema_version": "loom.nebius-candidate.v1",
        "repository": candidate.REPOSITORY,
        "source_ref": candidate.SOURCE_REF,
        "candidate_sha": "a" * 40,
        "source_tree": "b" * 40,
        "workflow_path": candidate.WORKFLOW,
        "run_id": 1,
        "registry_prefix": "cr.eu-north1.nebius.cloud/test",
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "policy_sha256": "sha256:" + "d" * 64,
        "images": {},
    }
    for component, name in candidate.COMPONENTS.items():
        record["images"][component] = {
            "image_ref": record["registry_prefix"] + "/" + name + "@sha256:" + "e" * 64,
            "source_sha": record["candidate_sha"],
            "platform": "linux/amd64",
            "sbom_sha256": "sha256:" + "f" * 64,
            "vulnerability_report_sha256": "sha256:" + "0" * 64,
            "highest_vulnerability_severity": "none",
        }
    release, profile = candidate.create_candidate(
        record, signing_key=signer, signing_key_id="publisher", keyring_json=json.dumps(trust)
    )
    config = json.loads(
        (deploy.ROOT / "deploy/nebius/integration.platform.json.example").read_text()
    )
    for name in (
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "public_allocation_id",
    ):
        config[name] = "example-id"
    config["cluster_id"] = "mk8scluster-test"
    config["kubernetes_api_server"] = "https://api.cluster.test"
    config["execution_price"]["vcpu_microusd_per_hour"] = 1000
    config["execution_price"]["source_version"] = "test-fixture"
    for name in ("postgres_image", "backup_image"):
        config[name] = record["registry_prefix"] + "/postgres@sha256:" + "1" * 64
    config["buckets"] = {name: "loom-integration-" + name for name in config["buckets"]}
    files = build_platform(config, release, profile, trust, repo_root=deploy.ROOT)
    output = tmp_path / "render"
    manifest = write_platform(files, config, release, output)
    args = argparse.Namespace(
        render_dir=output,
        trusted_keyring=keyring,
        kubeconfig=tmp_path / "kubeconfig",
        evidence_dir=tmp_path / "evidence",
        expected_cluster_id=config["cluster_id"],
        apply=False,
        retry_failed_jobs=False,
    )
    return args, config, manifest, files


class FakeKubectl(deploy.Kubectl):
    def __init__(self, config: dict, files: dict, *, database: bool = False):
        self.config = config
        self.files = files
        self.commands: list[tuple[str, ...]] = []
        self.objects: dict[tuple[str, str], dict] = {}
        self.secrets = deploy.secret_requirements(files, config)
        self.fail_backup = False
        self.wrong_server = False
        if database:
            self.objects["statefulset", "loom-postgres"] = {"metadata": {"name": "loom-postgres"}}
            self.objects["cronjob", "loom-platform-backup"] = files["80-backup.yaml"][0]

    def get(self, kind: str, name: str, namespace: str) -> dict:
        self.commands.append(("get", kind, name, namespace))
        return self.objects.get((kind, name), {})

    def run(self, *args: str, timeout: int = 90) -> str:
        self.commands.append(args)
        if args[:2] == ("config", "view"):
            return json.dumps(
                {
                    "clusters": [
                        {
                            "name": "nebius-cluster-test",
                            "cluster": {
                                "server": "https://wrong.test"
                                if self.wrong_server
                                else self.config["kubernetes_api_server"],
                                "certificate-authority-data": "redacted",
                            },
                        }
                    ]
                }
            )
        if args[:2] == ("get", "nodes"):
            return json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": "computeinstance-test",
                                "labels": {"loom.nebius/node-role": "system"},
                            },
                            "spec": {"providerID": "nebius://computeinstance-test"},
                        }
                    ]
                }
            )
        if args[:2] == ("get", "secret"):
            assert "go-template=" in args[-1]
            assert r'{{"\n"}}' in args[-1]
            return "\n".join(sorted(self.secrets[(args[4], args[2])]))
        if args[0] == "apply":
            for obj in yaml.safe_load_all(Path(args[2]).read_text()):
                self.objects[obj["kind"].lower(), obj["metadata"]["name"]] = obj
        if args[:2] == ("create", "job"):
            self.objects["job", args[2]] = {"metadata": {"name": args[2]}}
        if args[0] == "wait" and args[2].startswith("job/"):
            name = args[2].removeprefix("job/")
            if self.fail_backup and name.startswith("loom-predeploy-"):
                raise deploy.DeploymentError("backup execution failed")
            self.objects["job", name]["status"] = {
                "conditions": [{"type": "Complete", "status": "True"}]
            }
        if args[:2] == ("delete", "job"):
            self.objects.pop(("job", args[2]))
        return ""


def test_real_render_revalidation_and_read_only_plan(rendered: tuple) -> None:
    args, config, _, files = rendered
    kube = FakeKubectl(config, files)
    result = deploy.deploy(args, kube=kube)
    assert result["status"] == "planned"
    assert all(command[0] in {"get", "config"} for command in kube.commands)
    evidence = next(iter(args.evidence_dir.glob("*.json"))).read_text()
    assert "api.cluster.test" not in evidence
    assert "certificate-authority-data" not in evidence


def test_rehashed_manifest_cannot_change_rendered_namespace(rendered: tuple) -> None:
    args, config, manifest, files = rendered
    path = args.render_dir / "40-services.yaml"
    path.write_text(path.read_text().replace(config["namespace"], "other-namespace"))
    manifest["files"][path.name] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    (args.render_dir / "manifest.json").write_bytes(canonical(manifest))
    kube = FakeKubectl(config, files)
    with pytest.raises(deploy.DeploymentError, match="deterministic source"):
        deploy.deploy(args, kube=kube)
    assert kube.commands == []


def test_wrong_cluster_and_missing_secret_fail_before_mutation(rendered: tuple) -> None:
    args, config, _, files = rendered
    kube = FakeKubectl(config, files)
    kube.wrong_server = True
    with pytest.raises(deploy.DeploymentError, match="API server"):
        deploy.deploy(args, kube=kube)
    kube.wrong_server = False
    kube.secrets[(config["namespace"], "loom-platform-db")] = set()
    with pytest.raises(deploy.DeploymentError, match="secret keys"):
        deploy.deploy(args, kube=kube)
    assert all(command[0] in {"get", "config"} for command in kube.commands)


def test_failed_upgrade_backup_prevents_all_apply(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, manifest, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    kube.fail_backup = True
    monkeypatch.setattr(
        deploy.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="" if args[0][1] == "status" else manifest["candidate_sha"]
        ),
    )
    with pytest.raises(deploy.DeploymentError, match="backup execution"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] in {"apply", "delete"} for command in kube.commands)
    result = json.loads(next(iter(args.evidence_dir.glob("*.json"))).read_text())
    assert result["phases"][-1]["name"] == "pre-upgrade-backup"
    assert result["status"] == "failed"


def test_fresh_apply_and_completed_jobs_are_idempotent(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, manifest, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files)
    monkeypatch.setattr(
        deploy.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="" if args[0][1] == "status" else manifest["candidate_sha"]
        ),
    )
    smoke: list[str] = []
    monkeypatch.setattr(deploy, "public_smoke", lambda origin, environment: smoke.append(origin))
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    applies = [Path(command[2]).name for command in kube.commands if command[0] == "apply"]
    assert (
        applies.index("80-backup.yaml")
        < applies.index("30-migrate.yaml")
        < applies.index("40-services.yaml")
    )
    kube.commands.clear()
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    applies = [Path(command[2]).name for command in kube.commands if command[0] == "apply"]
    assert "30-migrate.yaml" not in applies and "50-configure.yaml" not in applies
    assert not any(command[0] == "delete" for command in kube.commands)
    assert len(smoke) == 2


def test_failed_candidate_job_requires_explicit_retry(
    rendered: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, config, manifest, files = rendered
    args.apply = True
    kube = FakeKubectl(config, files, database=True)
    name = files["30-migrate.yaml"][0]["metadata"]["name"]
    kube.objects["job", name] = {"status": {"conditions": [{"type": "Failed", "status": "True"}]}}
    monkeypatch.setattr(
        deploy.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout="" if args[0][1] == "status" else manifest["candidate_sha"]
        ),
    )
    monkeypatch.setattr(deploy, "public_smoke", lambda *args: None)
    with pytest.raises(deploy.DeploymentError, match="explicitly retry"):
        deploy.deploy(args, kube=kube)
    assert not any(command[0] == "delete" for command in kube.commands)
    args.retry_failed_jobs = True
    assert deploy.deploy(args, kube=kube)["status"] == "complete"
    assert (
        "delete",
        "job",
        name,
        "-n",
        config["namespace"],
        "--cascade=foreground",
        "--wait=true",
    ) in kube.commands


def test_kubectl_error_retains_api_reason_without_secret_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        deploy.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="Error from server (Forbidden): token=secret https://private.test",
        ),
    )
    with pytest.raises(deploy.DeploymentError, match="Forbidden") as error:
        deploy.Kubectl(tmp_path / "config").run("apply", "-f", "test.yaml")
    assert "secret" not in str(error.value) and "private" not in str(error.value)
