"""Managed children reuse the stack without buying or controlling another pool."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom.nebius_platform_render import build_platform, validate_environment
from tests.unit.test_nebius_environment_contract import BOB, foundation_from, registration_for
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def rendered(platform_inputs, slug="alice", **kwargs):
    from loom.nebius_environment_render import render_environment

    config, candidate, profile = platform_inputs
    foundation = foundation_from(config)
    row = registration_for(foundation, slug, **kwargs)
    return render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)


def documents(result):
    return [doc for docs in result.files.values() for doc in docs]


def named(result, kind, name):
    return next(doc for doc in documents(result)
                if doc["kind"] == kind and doc["metadata"]["name"] == name)


def test_two_developers_keep_distinct_local_topologies(platform_inputs):
    a = rendered(platform_inputs)
    b = rendered(platform_inputs, "bob", identity=BOB)
    for result, namespace, host in (
        (a, "loom-dev-alice", "alice.dev.example.com"),
        (b, "loom-dev-bob", "bob.dev.example.com"),
    ):
        config = json.loads(named(result, "ConfigMap", "loom-platform-config")["data"]["environment.json"])
        validate_environment(config)  # The bootstrap job must accept its own persisted input.
        cm = named(result, "ConfigMap", "loom-platform-config")
        topology = json.loads(cm["data"]["catalog.json"])["topology"]
        assert topology["schema_version"] == "loom.execution-topology.v1"
        assert len(topology["targets"]) == 1
        assert topology["targets"][0]["namespace_name"] == result.registration.execution_namespace
        db = named(result, "StatefulSet", "loom-postgres")
        assert db["metadata"]["namespace"] == namespace
        assert db["spec"]["persistentVolumeClaimRetentionPolicy"] == {
            "whenDeleted": "Retain", "whenScaled": "Retain",
        }
        assert db["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "data"
        assert config["public_host"] == host
        assert config["capacity_policy"]["enabled"] is False
        assert result.execution_enabled is False
        for doc in documents(result):
            if doc["kind"] == "Namespace":
                assert doc["metadata"]["name"] in result.registration.namespaces
            else:
                assert doc["metadata"]["namespace"] in result.registration.namespaces
            assert doc["metadata"]["labels"]["loom.nebius/environment-id"] == str(result.registration.environment_id)
    assert a.config["buckets"].keys() == b.config["buckets"].keys()
    assert not set(a.config["buckets"].values()) & set(b.config["buckets"].values())
    assert not set(a.config["buckets"].values()) & set(platform_inputs[0]["buckets"].values())
    assert a.registration.physical_pool_id == b.registration.physical_pool_id


def test_shared_https_routes_only_to_own_application_services(platform_inputs):
    result = rendered(platform_inputs)
    ingress = named(result, "Ingress", "loom-web")
    assert ingress["spec"]["ingressClassName"] == "loom-shared"
    assert ingress["spec"]["tls"] == [{"hosts": ["alice.dev.example.com"]}]
    assert ingress["spec"]["rules"] == [{
        "host": "alice.dev.example.com", "http": {"paths": [
            {"path": "/api", "pathType": "Prefix", "backend": {
                "service": {"name": "loom-service", "port": {"number": 8090}},
            }},
            {"path": "/", "pathType": "Prefix", "backend": {
                "service": {"name": "loom-web", "port": {"number": 8080}},
            }},
        ]},
    }]
    assert not any(doc["kind"] in {"ClusterRole", "ClusterRoleBinding", "Secret"}
                   for doc in documents(result))
    for doc in documents(result):
        if doc["kind"] == "Service":
            assert doc["spec"].get("type", "ClusterIP") == "ClusterIP"
        assert doc["kind"] != "PersistentVolumeClaim"  # No per-child Caddy TLS storage.
    pod = named(result, "Deployment", "loom-web")["spec"]["template"]["spec"]
    assert len(pod["containers"]) == 1
    assert not pod.get("volumes")
    expected_peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "loom-ingress"}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-ingress"}},
    }
    for policy_name, port in (("public-web", 8080), ("public-api", 8090)):
        policy = named(result, "NetworkPolicy", policy_name)
        assert policy["spec"]["ingress"] == [{
            "from": [expected_peer], "ports": [{"protocol": "TCP", "port": port}],
        }]


def test_child_cannot_start_execution_or_duplicate_capacity_collection(platform_inputs):
    result = rendered(platform_inputs)
    for doc in documents(result):
        assert "collector" not in doc["metadata"]["name"]
        if doc["kind"] == "Role":
            for rule in doc["rules"]:
                assert set(rule["verbs"]) <= {"get", "list", "watch"}
        if doc["metadata"].get("namespace") != result.registration.application_namespace:
            assert doc["kind"] not in {"Deployment", "CronJob", "Job"}
    quotas = [doc for doc in documents(result) if doc["kind"] == "ResourceQuota"]
    assert {doc["metadata"]["namespace"] for doc in quotas} == {
        result.registration.execution_namespace, result.registration.build_namespace,
    }
    assert all(doc["spec"]["hard"]["pods"] == "0" for doc in quotas)
    cp = named(result, "Deployment", "loom-control-plane")
    env = {row["name"]: row.get("value") for row in cp["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED"] == "false"


def test_child_rejects_task_identity_readiness_under_restricted_pss(platform_inputs):
    platform_inputs[2]["supports_task_identity"] = True
    with pytest.raises(ValueError, match=r"task identity.*restricted"):
        rendered(platform_inputs)


def test_platform_envelope_covers_surge_bootstrap_backup_and_retained_storage(platform_inputs):
    result = rendered(platform_inputs)
    # DB 100m/256Mi + two copies of (3 apps 100m/256Mi + web 25m/64Mi)
    # + migration/configure/backup jobs at 100m/256Mi each.
    assert result.platform_envelope.cpu_millis == 1050
    assert result.platform_envelope.memory_mib == 2688
    assert result.platform_envelope.storage_mib == platform_inputs[0]["postgres_storage_gi"] * 1024
    assert result.platform_envelope.ephemeral_storage_mib >= result.platform_envelope.storage_mib
    backup = named(result, "CronJob", "loom-platform-backup")
    pod = backup["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["initContainers"][0]["resources"]["requests"]["ephemeral-storage"] == f"{platform_inputs[0]['postgres_storage_gi']}Gi"


@pytest.mark.parametrize("field,value", [
    ("cluster_id", "another-cluster"), ("physical_pool_id", "another-pool"),
    ("public_host", "bob.dev.example.com"), ("public_host", "alice.attacker.com"),
])
def test_renderer_rejects_registration_outside_protected_foundation(platform_inputs, field, value):
    from loom.nebius_environment_render import render_environment

    config, candidate, profile = platform_inputs
    foundation = foundation_from(config)
    row = registration_for(foundation, "alice")
    row = EnvironmentRegistrationV1.model_validate({**row.model_dump(), field: value})
    with pytest.raises(ValueError):
        render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)


@pytest.mark.parametrize("field,value", [
    ("namespace", "loom-dev-bob"), ("execution_namespace", "loom-run-bob"),
    ("cluster_id", "another-cluster"), ("execution_node_group_id", "another-pool"),
    ("public_host", "bob.dev.example.com"), ("environment", "production"),
    ("regional_execution_targets", [{}]), ("capacity_policy", None),
])
def test_persisted_managed_config_rejects_binding_drift(platform_inputs, field, value):
    result = rendered(platform_inputs)
    with pytest.raises(ValueError):
        validate_environment({**result.config, field: value})


def test_managed_config_cannot_enter_standalone_deployment_path(platform_inputs):
    result = rendered(platform_inputs)
    with pytest.raises(ValueError, match="managed"):
        build_platform(result.config, platform_inputs[1], platform_inputs[2], {}, repo_root=ROOT)


@pytest.mark.parametrize("kind,slug,ns", [
    ("development", "dev", "loom-dev"), ("staging", "staging", "loom-staging"),
    ("production", "prod", "loom-prod"),
])
def test_shared_environment_has_one_class_correct_primary(platform_inputs, kind, slug, ns):
    result = rendered(platform_inputs, slug, kind=kind, scope="shared")
    assert result.config["namespace"] == ns
    catalog = json.loads(named(result, "ConfigMap", "loom-platform-config")["data"]["catalog.json"])
    assert catalog["topology"]["targets"][0]["environment"] == kind


def test_render_does_not_mutate_protected_inputs(platform_inputs):
    before = json.dumps(platform_inputs, sort_keys=True)
    rendered(platform_inputs)
    assert json.dumps(platform_inputs, sort_keys=True) == before


def test_managed_offline_render_does_not_require_cloud_or_database_clients(platform_inputs, tmp_path):
    inputs = tmp_path / "inputs.json"
    inputs.write_text(json.dumps(platform_inputs))
    result = subprocess.run([sys.executable, "-I", "-c", """
import importlib.abc
import json
import sys
from pathlib import Path
from uuid import UUID
root, source = map(Path, sys.argv[1:])
sys.path.insert(0, str(root / 'src'))
class NoClients(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'kubernetes', 'sqlalchemy', 'asyncpg', 'nebius'}:
            raise ModuleNotFoundError('offline renderer imported ' + fullname)
sys.meta_path.insert(0, NoClients())
from loom.nebius_environment_contract import FoundationBinding, new_environment_registration
from loom.nebius_environment_render import render_environment
config, candidate, profile = json.loads(source.read_text())
foundation = FoundationBinding(platform_config_json=json.dumps(config), public_dns_zone='dev.example.com',
    ingress_class_name='shared', ingress_namespace='ingress', ingress_controller_label='ingress')
row = new_environment_registration(foundation, environment_id=UUID(int=1), incarnation=UUID(int=2),
    owner_user_id=UUID(int=3), owner_team_id=UUID(int=4), slug='alice')
rendered = render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=root)
assert rendered.platform_envelope.cpu_millis == 1050
""", str(ROOT), str(inputs)], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("state", ["suspended", "destroyed"])
def test_nonactive_registration_cannot_render_running_stack(platform_inputs, state):
    from loom.nebius_environment_render import render_environment

    config, candidate, profile = platform_inputs
    foundation = foundation_from(config)
    row = registration_for(foundation, "alice")
    row = EnvironmentRegistrationV1.model_validate({**row.model_dump(), "desired_state": state})
    with pytest.raises(ValueError, match="active"):
        render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)


@pytest.mark.parametrize("certificate_covers_host", [True, False])
def test_import_preserves_exact_existing_names_and_buckets(platform_inputs, certificate_covers_host):
    from loom.nebius_environment_contract import FoundationBinding
    from loom.nebius_environment_render import render_environment

    config, candidate, profile = platform_inputs
    foundation = foundation_from(config)
    row = registration_for(foundation, "alice")
    row = EnvironmentRegistrationV1.model_validate({
        **row.model_dump(), "binding_mode": "imported", "application_namespace": config["namespace"],
        "execution_namespace": config["execution_namespace"], "build_namespace": config["execution_namespace"] + "-build",
        "target_id": config["target_id"], "public_host": config["public_host"],
    })
    if not certificate_covers_host:
        with pytest.raises(ValueError, match="certificate"):
            render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)
        return
    foundation = FoundationBinding.model_validate({
        **foundation.model_dump(), "public_dns_zone": config["public_host"].split(".", 1)[1],
    })
    result = render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)
    assert result.config["buckets"] == config["buckets"]
    assert result.config["namespace"] == config["namespace"]
    changed = EnvironmentRegistrationV1.model_validate({**row.model_dump(), "application_namespace": "loom-imported-other"})
    with pytest.raises(ValueError, match="import"):
        render_environment(changed, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)


def test_standalone_builder_settings_do_not_enable_child_builds(platform_inputs):
    platform_inputs[0]["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "max_concurrent": 2,
    }
    result = rendered(platform_inputs)
    assert "task_image_builder" not in result.config
    assert all(doc["metadata"].get("namespace") == result.registration.application_namespace
               for doc in documents(result) if doc["kind"] in {"Job", "Deployment", "CronJob"})
