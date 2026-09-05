from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from scripts.ops.render_nebius_runtime import main as render_main

from loom.nebius_runtime_render import NebiusRuntimeRenderError, render_nebius_runtime

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "registry.example/actuator@sha256:" + "a" * 64


def binding() -> dict:
    return {
        "schema_version": "loom.nebius-staging-attachment.v1",
        "environment": "staging",
        "target_id": "nebius-eu-north1-staging",
        "namespace": "loom-nebius-staging",
        "canonical_database": "loom_staging",
        "gateway_image": "registry.example/gateway@sha256:" + "b" * 64,
        "configuration_revision": "a" * 64,
        "local_providers_secret_name": "staging-local-providers",
        "canonical": {
            "endpoint": "https://canonical.example:9443",
            "region": "us-east-1",
            "artifacts_bucket": "loom-staging-artifacts",
            "trajectories_bucket": "loom-staging-trajectories",
            "db_secret": {
                "name": "staging-db",
                "gateway_key": "gw-url",
                "actuator_key": "actuator-url",
            },
            "storage_secret": {
                "name": "canonical-storage",
                "access_key": "access",
                "secret_key": "secret",
            },
        },
        "source": {
            "endpoint": "https://spool.example:9443",
            "region": "eu-north1",
            "bucket": "loom-staging-spool",
            "credentials_secret": {
                "name": "spool-storage",
                "access_key": "access",
                "secret_key": "secret",
            },
        },
        "gateway_secret": {
            "name": "staging-gateway",
            "step_jwt_key": "signing",
            "master_key": "master",
        },
        "collector": {
            "control_plane_url": "https://cp.example:8443",
            "token_secret": {"name": "staging-collector", "key": "token"},
            "nebius_secret": {"name": "nebius-observer", "key": "credentials.json"},
        },
        "network": {
            name: [{"cidr": f"192.0.2.{index}/32", "port": port}]
            for index, (name, port) in enumerate(
                [
                    ("database", 5432),
                    ("canonical_store", 9443),
                    ("source_store", 9443),
                    ("control_plane", 8443),
                    ("kubernetes_api", 443),
                    ("provider_api", 443),
                    ("model_api", 443),
                ],
                1,
            )
        },
    }


def render(tmp_path: Path, payload: dict, environment: str = "staging") -> tuple[dict, list[dict]]:
    attachment = tmp_path / "attachment.json"
    attachment.write_text(json.dumps(payload))
    capacity = json.loads((ROOT / "deploy/k8s/nebius-development-capacity-policy.json").read_text())
    capacity["schema_version"] = f"loom.nebius-{environment}-capacity.v1"
    capacity["target_id"] = f"nebius-eu-north1-{environment}"
    policy = tmp_path / "capacity.json"
    policy.write_text(json.dumps(capacity))
    manifest = render_nebius_runtime(
        repo_root=ROOT,
        environment=environment,
        image=IMAGE,
        topology_path=ROOT / "config/service-execution-topology.json",
        physical_binding_path=ROOT / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=policy,
        output_dir=tmp_path / "rendered",
        staging_attachment_path=attachment,
    )
    documents = []
    for item in manifest["files"]:
        if item["path"].endswith(".yaml"):
            documents.extend(yaml.safe_load_all((tmp_path / "rendered" / item["path"]).read_text()))
    return manifest, documents


def test_staging_attachment_separates_spool_without_second_control_plane(tmp_path: Path) -> None:
    manifest, docs = render(tmp_path, binding())
    assert not any("patch" in row["path"] for row in manifest["files"])
    assert not any(row["kind"] in {"Secret", "StatefulSet"} for row in docs)
    deployments = {row["metadata"]["name"]: row for row in docs if row["kind"] == "Deployment"}
    assert set(deployments) == {"loom-execution-actuator", "loom-llm-gateway"}
    assert all(
        row["metadata"]["namespace"] == "loom-nebius-staging" for row in deployments.values()
    )
    env = {
        row["name"]: row
        for row in deployments["loom-llm-gateway"]["spec"]["template"]["spec"]["containers"][0][
            "env"
        ]
    }
    assert env["LOOM_GW_MINIO_ENDPOINT"]["value"] == "https://canonical.example:9443"
    assert env["LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT"]["value"] == "https://spool.example:9443"
    assert env["LOOM_GW_DB_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": "staging-db",
        "key": "gw-url",
    }
    assert (
        env["LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"]
        == "spool-storage"
    )
    actuator = deployments["loom-execution-actuator"]["spec"]["template"]["spec"]["containers"][0]
    actuator_env = {row["name"]: row for row in actuator["env"]}
    assert (
        actuator_env["LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL"]["value"]
        == "http://loom-llm-gateway.loom-nebius-staging.svc.cluster.local:9100/internal/service-execution"
    )
    assert (
        actuator_env["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"]
        == "staging-db"
    )
    collector = next(row for row in docs if row["kind"] == "ConfigMap")
    assert (
        collector["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL"]
        == "https://cp.example:8443"
    )
    roles = [row for row in docs if row["kind"] in {"ClusterRole", "ClusterRoleBinding"}]
    assert all(row["metadata"]["name"].endswith("-staging") for row in roles)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda x: x.update(environment="production"),
        lambda x: x.update(canonical_database="loom_development"),
        lambda x: x.update(namespace="loom-nebius-development"),
        lambda x: x.update(gateway_image="registry.example/gateway:latest"),
        lambda x: x.update(configuration_revision=""),
        lambda x: x.update(configuration_revision="A" * 64),
        lambda x: x.pop("configuration_revision"),
        lambda x: x.update(local_providers_secret_name=""),
        lambda x: x.pop("local_providers_secret_name"),
        lambda x: x["canonical"].update(endpoint="https://user:password@canonical.example"),
        lambda x: x["source"].update(endpoint="https://spool.example?token=secret"),
        lambda x: x["canonical"]["db_secret"].update(password="secret"),
        lambda x: x["network"].update(database=[{"cidr": "0.0.0.0/0", "port": 5432}]),
        lambda x: x["network"].update(database=[]),
        lambda x: x["canonical"].update(endpoint="https://192.0.2.99:9443"),
        lambda x: x["collector"].update(control_plane_url="https://cp.example:9999"),
    ],
)
def test_attachment_rejects_unsafe_or_cross_environment_binding(tmp_path: Path, mutation) -> None:
    payload = binding()
    mutation(payload)
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


def test_attachment_cannot_change_development(tmp_path: Path) -> None:
    with pytest.raises(NebiusRuntimeRenderError, match="staging"):
        render(tmp_path, binding(), environment="development")


def test_configuration_revision_rolls_consumers_for_same_name_secret_rotation(
    tmp_path: Path,
) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    before.mkdir()
    after.mkdir()
    payload = binding()
    _, old_docs = render(before, payload)
    payload["configuration_revision"] = "c" * 64
    _, new_docs = render(after, payload)
    key = "loom.ca/nebius-configuration-revision"
    for name in (
        "loom-llm-gateway",
        "loom-execution-actuator",
        "loom-execution-capacity-collector",
    ):
        old = next(
            row
            for row in old_docs
            if row["kind"] in {"Deployment", "CronJob"} and row["metadata"]["name"] == name
        )
        new = next(
            row
            for row in new_docs
            if row["kind"] == old["kind"] and row["metadata"]["name"] == name
        )
        if old["kind"] == "CronJob":
            assert old["spec"]["jobTemplate"]["metadata"]["annotations"][key] == "a" * 64
            assert new["spec"]["jobTemplate"]["metadata"]["annotations"][key] == "c" * 64
            old, new = old["spec"]["jobTemplate"], new["spec"]["jobTemplate"]
        assert old["spec"]["template"]["metadata"]["annotations"][key] == "a" * 64
        assert new["spec"]["template"]["metadata"]["annotations"][key] == "c" * 64
        assert old["spec"]["template"] != new["spec"]["template"]
        assert old["spec"]["template"]["spec"] == new["spec"]["template"]["spec"]


def test_attachment_network_policies_scope_each_component(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    policies = {
        row["metadata"]["name"]: row["spec"] for row in docs if row["kind"] == "NetworkPolicy"
    }
    assert all(spec["podSelector"] for spec in policies.values())
    gateway = policies["loom-attachment-gateway"]
    assert gateway["ingress"] == [
        {
            "from": [
                {"podSelector": {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}}}
            ],
            "ports": [{"protocol": "TCP", "port": 9100}],
        }
    ]
    assert policies["loom-execution-attempt-egress"]["egress"][-1]["to"] == [
        {"podSelector": {"matchLabels": {"app": "loom-llm-gateway"}}}
    ]
    for name, expected in {
        "loom-attachment-gateway": {"192.0.2.1/32", "192.0.2.2/32", "192.0.2.3/32", "192.0.2.7/32"},
        "loom-attachment-actuator": {"192.0.2.1/32", "192.0.2.5/32"},
        "loom-attachment-collector": {"192.0.2.4/32", "192.0.2.5/32", "192.0.2.6/32"},
    }.items():
        actual = {
            peer["ipBlock"]["cidr"]
            for rule in policies[name]["egress"]
            for peer in rule["to"]
            if "ipBlock" in peer
        }
        assert actual == expected


def test_gateway_rollout_preserves_long_request_drain(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    gateway = next(
        row
        for row in docs
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-llm-gateway"
    )
    pod = gateway["spec"]["template"]["spec"]
    assert pod["terminationGracePeriodSeconds"] == 300
    command = pod["containers"][0]["lifecycle"]["preStop"]["exec"]["command"]
    assert command[:2] == ["python", "-c"]
    assert "http://127.0.0.1:9100/drain" in command[2]
    assert "method='POST'" in command[2]
    assert "timeout=280" in command[2]
    compile(command[2], "<rendered gateway preStop>", "exec")


def test_gateway_local_provider_secret_does_not_replace_explicit_identity(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    gateway = next(
        row
        for row in docs
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-llm-gateway"
    )
    container = gateway["spec"]["template"]["spec"]["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": "staging-local-providers"}}]
    explicit = {entry["name"] for entry in container["env"]}
    assert {
        "LOOM_ENV",
        "LOOM_NAMESPACE",
        "LOOM_GW_DB_URL",
        "LOOM_GW_STEP_JWT_SIGNING_KEY",
        "LOOM_SECRET_STORE_MASTER_KEY",
        "LOOM_GW_MINIO_ENDPOINT",
        "LOOM_GW_MINIO_ACCESS_KEY",
        "LOOM_GW_MINIO_SECRET_KEY",
        "LOOM_GW_ARTIFACTS_BUCKET",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_BUCKET",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
    } <= explicit


def test_default_development_render_retains_template_bytes(tmp_path: Path) -> None:
    output = tmp_path / "default"
    manifest = render_nebius_runtime(
        repo_root=ROOT,
        environment="development",
        image=IMAGE,
        topology_path=ROOT / "config/service-execution-topology.json",
        physical_binding_path=ROOT / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=ROOT / "deploy/k8s/nebius-development-capacity-policy.json",
        output_dir=output,
    )
    assert "staging_attachment" not in manifest["source_sha256"]
    for entry in manifest["files"]:
        path = entry["path"]
        if "patch.yaml" in path:
            assert (output / path).read_bytes() == (ROOT / "deploy/k8s" / path).read_bytes()
    assert not (output / "nebius-staging-gateway.yaml").exists()


def test_cli_attachment_render_and_rejection_are_offline(tmp_path: Path, capsys) -> None:
    render(tmp_path, binding())
    argv = [
        "--environment",
        "staging",
        "--image",
        IMAGE,
        "--capacity-policy",
        str(tmp_path / "capacity.json"),
        "--staging-attachment",
        str(tmp_path / "attachment.json"),
        "--output",
        str(tmp_path / "cli"),
    ]
    assert render_main(argv) == 0
    assert "target: nebius-eu-north1-staging" in capsys.readouterr().out
    payload = binding()
    payload["source"]["endpoint"] = "https://user:must-not-leak@spool.example"
    (tmp_path / "attachment.json").write_text(json.dumps(payload))
    argv[-1] = str(tmp_path / "rejected")
    assert render_main(argv) == 1
    stderr = capsys.readouterr().err
    assert "must-not-leak" not in stderr
    assert not (tmp_path / "rejected").exists()
