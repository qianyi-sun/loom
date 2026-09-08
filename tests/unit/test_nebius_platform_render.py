from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from loom.nebius_platform_bootstrap import MigrationError, database_url
from loom.nebius_platform_render import NebiusPlatformError, build_platform, digest, write_platform

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def platform_inputs() -> tuple[dict, dict, dict]:
    config = json.loads((ROOT / "deploy/nebius/integration.platform.json.example").read_text())
    for key in (
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
    ):
        config[key] = "test-" + key.replace("_", "-")
    config["kubernetes_api_server"] = "https://api.cluster.test"
    config["execution_price"]["vcpu_microusd_per_hour"] = 1000
    for key in ("postgres_image", "backup_image"):
        config[key] = "cr.eu-north1.nebius.cloud/test/postgres@sha256:" + "a" * 64
    config["buckets"] = {key: "loom-integration-" + key for key in config["buckets"]}
    images = {
        key: {"image_ref": "cr.eu-north1.nebius.cloud/test/" + key + "@sha256:" + "b" * 64}
        for key in (
            "service",
            "control_plane",
            "web",
            "gateway",
            "execution_runtime",
            "execution_actuator",
        )
    }
    profile = {
        "candidate_sha": "c" * 40,
        "task_image_ref": images["service"]["image_ref"],
        "runtime_image_ref": images["execution_runtime"]["image_ref"],
    }
    candidate = {
        "candidate_sha": "c" * 40,
        "source_ref": "refs/heads/codex/nebius-main",
        "repository": "qianyi-sun/loom",
        "images": images,
        "profile_sha256": digest(profile),
    }
    return config, candidate, profile


def test_independent_namespace_routing_storage_and_no_secret_material(
    platform_inputs: tuple, tmp_path: Path
) -> None:
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    serialized = json.dumps(files)
    assert "oldlab" not in serialized and "gb10" not in serialized
    assert "hostPath" not in serialized and "loom.svc" not in serialized
    docs = [doc for batch in files.values() for doc in batch]
    assert not any(doc["kind"] == "Secret" for doc in docs)
    assert {doc["metadata"].get("namespace") for doc in docs if "namespace" in doc["metadata"]} == {
        config["namespace"],
        config["execution_namespace"],
    }
    postgres = next(doc for doc in docs if doc["kind"] == "StatefulSet")
    assert (
        postgres["spec"]["volumeClaimTemplates"][0]["spec"]["storageClassName"]
        == "compute-csi-default-sc"
    )
    services = [doc for doc in docs if doc["kind"] == "Service"]
    assert [
        doc["metadata"]["name"] for doc in services if doc["spec"].get("type") == "LoadBalancer"
    ] == ["loom-web"]
    for deployment in [doc for doc in docs if doc["kind"] == "Deployment"]:
        pod = deployment["spec"]["template"]["spec"]
        assert pod["nodeSelector"]["loom.nebius/platform"] == "integration"
        for volume in pod.get("volumes", []):
            if volume["name"] == "db-ca":
                assert volume["secret"]["items"] == [{"key": "ca.crt", "path": "ca.crt"}]
    manifest = write_platform(files, config, candidate, tmp_path / "render")
    for name, expected in manifest["files"].items():
        assert (
            "sha256:" + hashlib.sha256((tmp_path / "render" / name).read_bytes()).hexdigest()
            == expected
        )
    with pytest.raises(NebiusPlatformError, match="empty"):
        write_platform(files, config, candidate, tmp_path / "render")


@pytest.mark.parametrize(
    "field,value",
    [
        ("namespace", "loom-staging"),
        ("environment", "production"),
        ("environment", "staging"),
        ("storage_endpoint", "https://storage.example.com"),
        ("postgres_image", "postgres:latest"),
        ("unknown_token", "secret"),
        ("max_concurrent", 0),
        ("model_provider_base_url", "https://user:secret@example.com"),
    ],
)
def test_rejects_implicit_or_unprotected_environment(
    platform_inputs: tuple, field: str, value: object
) -> None:
    config, candidate, profile = platform_inputs
    config[field] = value
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_profile_candidate_mismatch_rejected(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    profile["candidate_sha"] = "d" * 40
    with pytest.raises(NebiusPlatformError, match="profile"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_actual_execution_job_tolerates_both_dedicated_pool_taints(platform_inputs: tuple) -> None:
    from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
    from tests.unit.test_execution_actuator import _lease

    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    actuator = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "Deployment")
    env = {
        row["name"]: row.get("value")
        for row in actuator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    lease = _lease()
    lease.target_id = config["target_id"]
    lease.namespace_name = config["execution_namespace"]
    target = ExecutionTargetRuntime(
        target_id=lease.target_id,
        namespace=lease.namespace_name,
        node_selector=json.loads(env["LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR"]),
        tolerations=tuple(json.loads(env["LOOM_EXECUTION_ACTUATOR_TOLERATIONS"])),
        credential_broker_url=env["LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL"],
    )
    job = render_execution_job(lease, target=target)
    pod = job["spec"]["template"]["spec"]
    assert pod["nodeSelector"]["loom.nebius/platform"] == "integration"
    assert {(row["key"], row["value"]) for row in pod["tolerations"]} >= {
        ("loom.nebius/execution", "true"),
        ("loom.nebius/platform", "integration"),
    }


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:secret@localhost/loom",
        "postgresql://user:secret@loom-postgres.loom-nebius-platform.svc/loom?sslmode=disable",
        "postgresql://user:secret@loom-postgres.loom-staging.svc/loom?sslmode=verify-full&sslrootcert=/var/run/loom-db/ca.crt",
    ],
)
def test_database_cannot_attach_to_historical_or_unverified_authority(url: str) -> None:
    with pytest.raises(ValueError, match="namespace-local TLS"):
        database_url(url, "loom-nebius-platform")


def test_migration_diagnostic_preserves_revision_but_not_secret() -> None:
    import subprocess

    error = MigrationError(
        subprocess.CompletedProcess(
            [],
            1,
            "",
            "Running upgrade 0123 -> 0124\npsycopg.errors.UndefinedTable: postgres://user:secret@host/loom\n",
        )
    )
    assert error.details == {
        "exit_code": 1,
        "migration_revision": "0124",
        "database_error": "UndefinedTable",
    }
    assert "secret" not in json.dumps(error.details)


def test_rendered_application_settings_and_startup_factories_accept_signed_profile(
    platform_inputs: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts.ops.nebius_candidate import create_candidate

    from loom_control_plane.app import create_app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings
    from loom_llm_gateway.app import create_app as gateway_app
    from loom_llm_gateway.config import GatewaySettings
    from loom_service.app import create_app as service_app
    from loom_service.config import LoomServiceSettings
    from tests.ops.test_nebius_candidate import inputs

    config, _, _ = platform_inputs
    build_record, signer, trust = inputs(tmp_path)
    candidate, profile = create_candidate(
        build_record, signing_key=signer, signing_key_id="publisher", keyring_json=trust
    )
    files = build_platform(config, candidate, profile, json.loads(trust), repo_root=ROOT)
    for name, prefix, model, factory in (
        ("loom-service", "LOOM_SVC_", LoomServiceSettings, service_app),
        ("loom-control-plane", "LOOM_CP_", ControlPlaneSettings, control_plane_app),
        ("loom-llm-gateway", "LOOM_GW_", GatewaySettings, gateway_app),
    ):
        deployment = next(
            doc
            for doc in files["40-services.yaml"]
            if doc["kind"] == "Deployment" and doc["metadata"]["name"] == name
        )
        with monkeypatch.context() as scoped:
            for row in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
                if row["name"].startswith(prefix) and not row["name"].startswith("LOOM_GW_LOCAL_"):
                    assert row["name"][len(prefix) :].lower() in model.model_fields, row["name"]
                value = row.get("value", "synthetic-test-secret-" + "a" * 32)
                if row["name"].endswith("_DB_URL"):
                    value = (
                        "postgresql+psycopg://test:test@loom-postgres.loom-nebius-platform.svc/loom"
                    )
                scoped.setenv(row["name"], value)
            settings = model()
            app = factory(settings)
            assert app is not None
            assert settings.minio_endpoint == config["storage_endpoint"]
            if name == "loom-service":
                assert settings.gateway_url.host == f"loom-llm-gateway.{config['namespace']}.svc"
                assert (
                    settings.control_plane_url.host
                    == f"loom-control-plane.{config['namespace']}.svc"
                )
            elif name == "loom-control-plane":
                assert settings.service_execution_scheduler_enabled
                assert settings.service_execution_source_bucket == config["buckets"]["source"]
            else:
                assert (
                    settings.local_providers["yibu"].base_url == config["model_provider_base_url"]
                )
