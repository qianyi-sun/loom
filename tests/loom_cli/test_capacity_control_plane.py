from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import yaml  # type: ignore[import-untyped]
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import ValidationError

import loom_cli.capacity_control_plane as capacity_control_plane
from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_manager.ownership import public_key_fingerprint
from loom_cli.capacity_control_plane import (
    CapacityControlPlaneProfile,
    CapacityPoolExecutorProfile,
    capacity_pool_executor_manifest_sha256,
    load_capacity_control_plane_profile,
    load_capacity_pool_executor_profile,
    render_capacity_control_plane_manifests,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _REPO_ROOT / "deploy/dev-fleet/capacity-control-plane.toml"
_EXECUTOR_PROFILE = _REPO_ROOT / "deploy/dev-fleet/capacity-pool-executor.toml.example"
_EXECUTOR_SERVICE = _REPO_ROOT / "deploy/dev-fleet/loom-capacity-pool-executor.service"
_MANAGER_IMAGE = "ghcr.io/qianyi-sun/loom-capacity-manager@sha256:" + "a" * 64
_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")


def test_checked_in_executor_profile_is_inert_and_pool_complete() -> None:
    profile = load_capacity_pool_executor_profile(_EXECUTOR_PROFILE)

    assert profile.namespace == "loom-dev"
    assert profile.executable_new_capacity_ceiling == 0
    assert {pool.pool_id for pool in profile.pools} == {"oldlab", "gb10"}
    assert len({pool.executor_id for pool in profile.pools}) == 2
    assert len({pool.state_directory for pool in profile.pools}) == 2
    assert len({pool.bearer_token_file for pool in profile.pools}) == 2
    rendered = render_capacity_pool_executor_configs(profile)
    assert tuple(rendered) == ("gb10", "oldlab")
    assert all(value.endswith("\n") for value in rendered.values())
    assert all('"execution_epoch":' in value for value in rendered.values())
    assert all("executable_new_capacity_ceiling" not in value for value in rendered.values())
    for pool in profile.pools:
        payload = json.loads(rendered[pool.pool_id])
        assert set(payload) == {
            "association",
            "approved_profiles_sha256",
            "authority_incarnation",
            "bearer_token_file",
            "configuration_epoch",
            "controller_authority_sha256",
            "controller_host",
            "execution_epoch",
            "execution_manifest_sha256",
            "executor_image",
            "executor_id",
            "executor_incarnation",
            "journal_file",
            "local_authority_sha256",
            "local_uid",
            "manager_origin",
            "ownership_key_file",
            "partition",
            "pool_generation",
            "pool_id",
            "profile_digest",
            "profile_generation",
            "profile_id",
            "qos",
            "signing_key_id",
            "signing_key_sha256",
            "slurm_cluster",
            "slurm_executables",
            "state_directory",
            "service_user",
            "submitter",
            "tls_ca_file",
            "tls_certificate_file",
            "tls_private_key_file",
            "trusted_fleet_release_sha256",
            "writer_epoch",
        }
        assert payload["approved_profiles_sha256"] == "0" * 64
        environment = render_capacity_pool_executor_service_environment(profile, pool.pool_id)
        assert environment == (
            f"LOOM_CAPACITY_EXECUTOR_CONFIG={pool.config_file}\n"
            "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING=0\n"
            f"LOOM_CAPACITY_EXECUTOR_EXPECTED_MANIFEST_SHA256="
            f"{capacity_pool_executor_manifest_sha256(profile, pool.pool_id)}\n"
            f"LOOM_CAPACITY_EXECUTOR_POOL={pool.pool_id}\n"
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("executor_image", "ghcr.io/qianyi-sun/loom-capacity-executor@sha256:" + "2" * 64),
        ("service_user", "loom_capacity_executor_next"),
    ],
)
def test_executor_package_inputs_change_consumed_config_and_manifest(
    field: str, value: str
) -> None:
    profile = load_capacity_pool_executor_profile(_EXECUTOR_PROFILE)
    changed = profile.model_copy(update={field: value})
    pool_id = profile.pools[0].pool_id
    assert (
        render_capacity_pool_executor_configs(profile)[pool_id]
        != render_capacity_pool_executor_configs(changed)[pool_id]
    )
    assert capacity_pool_executor_manifest_sha256(
        profile, pool_id
    ) != capacity_pool_executor_manifest_sha256(changed, pool_id)


def test_checked_in_executor_systemd_unit_is_validation_only() -> None:
    unit = _EXECUTOR_SERVICE.read_text(encoding="utf-8")
    directives = "\n".join(line for line in unit.splitlines() if not line.startswith("#"))

    assert "[Install]" not in {line.strip() for line in directives.splitlines()}
    assert "--validate-only" in directives
    assert "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING} = 0" in directives
    for forbidden in ("systemctl", " enable", " start", " apply", " activate"):
        assert forbidden not in directives


def test_rendered_executor_config_is_accepted_by_the_production_loader(
    tmp_path: Path,
) -> None:
    payload = load_capacity_pool_executor_profile(_EXECUTOR_PROFILE).model_dump(mode="json")
    private_key = Ed25519PrivateKey.from_private_bytes(b"k" * 32)
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    config_file = tmp_path / "executor.json"
    secret_paths = {
        "bearer_token_file": tmp_path / "bearer",
        "tls_ca_file": tmp_path / "ca.pem",
        "tls_certificate_file": tmp_path / "certificate.pem",
        "tls_private_key_file": tmp_path / "private-key.pem",
        "ownership_key_file": tmp_path / "ownership-key",
    }
    for name, path in secret_paths.items():
        path.write_bytes(private_key.private_bytes_raw() if name == "ownership_key_file" else b"x")
        path.chmod(0o600)
    pool = payload["pools"][0]
    pool.update(
        config_file=str(config_file),
        state_directory=str(state),
        journal_file=str(state / "executor.journal"),
        local_uid=os.geteuid(),
        signing_key_sha256=public_key_fingerprint(private_key.public_key()),
        **{name: str(path) for name, path in secret_paths.items()},
    )
    candidate = CapacityPoolExecutorProfile.model_validate(payload)
    config_file.write_text(
        render_capacity_pool_executor_configs(candidate)[pool["pool_id"]],
        encoding="utf-8",
    )
    config_file.chmod(0o600)

    loaded = PoolExecutorConfig.from_files(
        config_file,
        expected_manifest_sha256=capacity_pool_executor_manifest_sha256(candidate, pool["pool_id"]),
    )

    assert loaded.pool_id == pool["pool_id"]
    assert loaded.execution.execution_state == "prepared"
    assert loaded.execution.executable_new_capacity_ceiling == 0


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(executable_new_capacity_ceiling=1), "ceiling"),
        (lambda value: value.update(namespace="loom-dev-shared"), "namespace"),
        (lambda value: value.update(executor_image="ghcr.io/example/executor:latest"), "immutable"),
        (
            lambda value: value["pools"][1].update(
                bearer_token_file=value["pools"][0]["bearer_token_file"]
            ),
            "credential",
        ),
        (lambda value: value["pools"][0].update(pool_id="oldlab"), "pool"),
    ],
)
def test_executor_profile_rejects_live_or_cross_pool_inputs(mutation, message: str) -> None:
    payload = load_capacity_pool_executor_profile(_EXECUTOR_PROFILE).model_dump(mode="json")
    mutation(payload)
    with pytest.raises(ValidationError, match=message):
        CapacityPoolExecutorProfile.model_validate(payload)


def test_checked_in_capacity_control_plane_profile_is_strict_and_immutable() -> None:
    profile = load_capacity_control_plane_profile(_PROFILE)

    assert profile.schema_version == 1
    assert profile.namespace == "loom-dev"
    assert profile.secret_name == "loom-capacity-manager"
    assert profile.postgres_image == (
        "postgres:17.4@sha256:304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad"
    )
    assert profile.postgres_storage == "20Gi"
    assert profile.postgres_resources.model_dump() == {
        "cpu_request": "250m",
        "memory_request": "512Mi",
        "cpu_limit": "2",
        "memory_limit": "4Gi",
    }
    assert profile.dns.model_dump() == {
        "namespace": "kube-system",
        "pod_label_key": "k8s-app",
        "pod_label_value": "kube-dns",
        "port": 53,
    }


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("namespace",), "loom-dev-alice"),
        (("secret_name",), "Unsafe_Name"),
        (("postgres_image",), "postgres:17.4"),
        (("postgres_image",), "postgres@sha256:" + "0" * 64),
        (("postgres_storage",), "0Gi"),
        (("postgres_storage",), "65537Gi"),
        (("postgres_resources", "cpu_request"), "0"),
        (("postgres_resources", "cpu_limit"), "65"),
        (("postgres_resources", "memory_limit"), "128Mi"),
        (("manager_resources", "memory_limit"), "1025Gi"),
        (("dns", "namespace"), "Unsafe_Name"),
        (("dns", "pod_label_key"), "unsafe key"),
        (("dns", "port"), 0),
    ],
)
def test_profile_rejects_namespace_image_resource_and_selector_drift(
    path: tuple[str, ...],
    value: object,
) -> None:
    payload = load_capacity_control_plane_profile(_PROFILE).model_dump()
    target = payload
    for segment in path[:-1]:
        target = target[segment]
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        CapacityControlPlaneProfile.model_validate(payload)


def test_profile_rejects_unknown_fields() -> None:
    payload = load_capacity_control_plane_profile(_PROFILE).model_dump()
    payload["executable_new_capacity_ceiling"] = 1

    with pytest.raises(ValidationError, match="extra_forbidden"):
        CapacityControlPlaneProfile.model_validate(payload)


def _documents() -> list[dict[str, Any]]:
    rendered = render_capacity_control_plane_manifests(
        load_capacity_control_plane_profile(_PROFILE),
        manager_image=_MANAGER_IMAGE,
        authority_incarnation=_AUTHORITY,
    )
    return [document for document in yaml.safe_load_all(rendered) if document]


def test_renderer_emits_one_inert_control_plane_in_dependency_order() -> None:
    documents = _documents()

    assert [(document["kind"], document["metadata"]["name"]) for document in documents] == [
        ("Namespace", "loom-dev"),
        ("Service", "loom-capacity-postgres"),
        ("StatefulSet", "loom-capacity-postgres"),
        ("Job", "loom-capacity-migrate-capacity-0010-aaaaaaaaaa-e44018c5cd"),
        ("Service", "loom-capacity-manager"),
        ("Deployment", "loom-capacity-manager"),
        ("NetworkPolicy", "capacity-default-deny"),
        ("NetworkPolicy", "capacity-dns-egress"),
        ("NetworkPolicy", "capacity-database-egress"),
        ("NetworkPolicy", "capacity-postgres-ingress"),
        ("NetworkPolicy", "capacity-manager-ingress"),
    ]
    namespace = documents[0]
    assert {
        "app.kubernetes.io/managed-by": "loom-operator",
        "pod-security.kubernetes.io/enforce": "restricted",
        "pod-security.kubernetes.io/enforce-version": "latest",
    }.items() <= namespace["metadata"]["labels"].items()
    for document in documents[1:]:
        assert document["metadata"]["namespace"] == "loom-dev"
    assert not any(document["kind"] == "Secret" for document in documents)


def test_migration_job_name_changes_with_immutable_template_inputs() -> None:
    profile = load_capacity_control_plane_profile(_PROFILE)

    def job_name(
        candidate_profile: CapacityControlPlaneProfile,
        authority: UUID,
    ) -> str:
        rendered = render_capacity_control_plane_manifests(
            candidate_profile,
            manager_image=_MANAGER_IMAGE,
            authority_incarnation=authority,
        )
        return next(
            document["metadata"]["name"]
            for document in yaml.safe_load_all(rendered)
            if document and document["kind"] == "Job"
        )

    changed_resources = profile.migration_resources.model_copy(update={"cpu_limit": "2"})
    names = {
        job_name(profile, _AUTHORITY),
        job_name(profile, UUID("00000000-0000-4000-8000-000000000902")),
        job_name(
            profile.model_copy(update={"secret_name": "loom-capacity-manager-next"}),
            _AUTHORITY,
        ),
        job_name(
            profile.model_copy(update={"migration_resources": changed_resources}),
            _AUTHORITY,
        ),
    }

    assert len(names) == 4


def test_migration_job_name_binds_the_complete_immutable_spec() -> None:
    job = next(document for document in _documents() if document["kind"] == "Job")
    expected_suffix = hashlib.sha256(
        json.dumps(
            {"migration_head": "capacity_0010", "spec": job["spec"]},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()[:10]

    assert job["metadata"]["name"].endswith(f"-{expected_suffix}")
    assert len(job["metadata"]["name"]) <= 63


def test_migration_job_name_stays_a_bounded_dns_label_for_future_heads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        capacity_control_plane,
        "_capacity_head",
        lambda: "CAPACITY_0004_" + "future_segment_" * 8,
    )
    rendered = render_capacity_control_plane_manifests(
        load_capacity_control_plane_profile(_PROFILE),
        manager_image=_MANAGER_IMAGE,
        authority_incarnation=_AUTHORITY,
    )
    job = next(
        document
        for document in yaml.safe_load_all(rendered)
        if document and document["kind"] == "Job"
    )
    name = job["metadata"]["name"]

    assert len(name) <= 63
    assert re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", name)


def test_renderer_pins_images_credentials_security_and_zero_only_commands() -> None:
    documents = _documents()
    workloads = {
        document["kind"]: document
        for document in documents
        if document["kind"] in {"StatefulSet", "Job", "Deployment"}
    }
    postgres = workloads["StatefulSet"]
    migration = workloads["Job"]
    manager = workloads["Deployment"]

    assert postgres["spec"]["template"]["spec"]["containers"][0]["image"] == (
        load_capacity_control_plane_profile(_PROFILE).postgres_image
    )
    assert migration["spec"]["template"]["spec"]["containers"][0]["image"] == (_MANAGER_IMAGE)
    assert manager["spec"]["template"]["spec"]["containers"][0]["image"] == (_MANAGER_IMAGE)
    assert migration["spec"]["template"]["spec"]["containers"][0]["args"] == [
        "--db-url-file",
        "/var/run/loom-capacity-manager/runtime/credentials/database-url",
        "--expected-authority-incarnation",
        str(_AUTHORITY),
    ]
    assert manager["spec"]["replicas"] == 1
    assert manager["spec"]["strategy"] == {"type": "Recreate"}
    assert migration["spec"]["activeDeadlineSeconds"] == 900
    assert migration["spec"]["template"]["spec"]["restartPolicy"] == "Never"

    for workload in workloads.values():
        pod = workload["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
        for container in (*pod.get("initContainers", []), *pod["containers"]):
            assert container["securityContext"]["allowPrivilegeEscalation"] is False
            assert container["securityContext"]["capabilities"] == {"drop": ["ALL"]}
            assert set(container["resources"]) == {"requests", "limits"}

    for workload, profile in ((migration, "migration"), (manager, "manager")):
        pod = workload["spec"]["template"]["spec"]
        assert {
            "runAsNonRoot": True,
            "runAsUser": 65532,
            "runAsGroup": 65532,
            "fsGroup": 65532,
        }.items() <= pod["securityContext"].items()
        init = pod["initContainers"][0]
        assert init["image"] == _MANAGER_IMAGE
        assert init["args"] == [
            "--profile",
            profile,
            "--source",
            "/var/run/loom-capacity-manager/projected",
            "--destination",
            "/var/run/loom-capacity-manager/runtime/credentials",
        ]
        assert {mount["name"] for mount in init["volumeMounts"]} == {
            "projected",
            "runtime",
        }
        application_mounts = pod["containers"][0]["volumeMounts"]
        assert application_mounts == [
            {
                "name": "runtime",
                "mountPath": "/var/run/loom-capacity-manager/runtime",
                "readOnly": True,
            }
        ]
        projected = next(volume for volume in pod["volumes"] if volume["name"] == "projected")
        assert projected["secret"]["secretName"] == "loom-capacity-manager"
        assert projected["secret"]["defaultMode"] == 0o440
        assert all(set(item) == {"key", "path"} for item in projected["secret"]["items"])

    manager_container = manager["spec"]["template"]["spec"]["containers"][0]
    postgres_container = postgres["spec"]["template"]["spec"]["containers"][0]
    postgres_health_command = [
        "/bin/sh",
        "-ec",
        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
    ]
    assert postgres_container["startupProbe"] == {
        "exec": {"command": postgres_health_command},
        "periodSeconds": 5,
        "failureThreshold": 120,
        "timeoutSeconds": 3,
    }
    environment = {item["name"]: item["value"] for item in manager_container["env"]}
    assert environment["LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION"] == str(_AUTHORITY)
    assert "CEILING" not in " ".join(environment)
    health_command = [
        "python",
        "-m",
        "loom_capacity_manager.health_probe",
        "--url",
        "https://127.0.0.1:8443/healthz",
        "--ca-file",
        "/var/run/loom-capacity-manager/runtime/credentials/server-ca.pem",
        "--certificate-file",
        "/var/run/loom-capacity-manager/runtime/credentials/health-certificate.pem",
        "--private-key-file",
        "/var/run/loom-capacity-manager/runtime/credentials/health-private-key.pem",
        "--server-certificate-file",
        "/var/run/loom-capacity-manager/runtime/credentials/server-certificate.pem",
    ]
    assert manager_container["startupProbe"]["exec"]["command"] == health_command
    assert manager_container["readinessProbe"]["exec"]["command"] == health_command
    assert manager_container["livenessProbe"]["tcpSocket"] == {"port": 8443}


def test_renderer_exposes_only_cluster_internal_mtls_and_least_access_networks() -> None:
    documents = _documents()
    manager_service = next(
        document
        for document in documents
        if document["kind"] == "Service" and document["metadata"]["name"] == "loom-capacity-manager"
    )
    assert manager_service["spec"]["type"] == "ClusterIP"
    assert set(manager_service["spec"]) == {"type", "selector", "ports"}
    assert manager_service["spec"]["ports"] == [
        {"name": "https", "protocol": "TCP", "port": 8443, "targetPort": 8443}
    ]

    policies = {
        document["metadata"]["name"]: document["spec"]
        for document in documents
        if document["kind"] == "NetworkPolicy"
    }
    assert policies["capacity-default-deny"] == {
        "podSelector": {
            "matchExpressions": [
                {
                    "key": "loom.yylx.dev/capacity-component",
                    "operator": "Exists",
                }
            ]
        },
        "policyTypes": ["Ingress", "Egress"],
    }
    dns = policies["capacity-dns-egress"]["egress"]
    assert dns[0]["ports"] == [
        {"protocol": "UDP", "port": 53},
        {"protocol": "TCP", "port": 53},
    ]
    database = policies["capacity-database-egress"]["egress"]
    assert database[0]["ports"] == [{"protocol": "TCP", "port": 5432}]
    manager_ingress = policies["capacity-manager-ingress"]["ingress"][0]
    assert manager_ingress["ports"] == [{"protocol": "TCP", "port": 8443}]
    assert manager_ingress["from"] == [
        {
            "namespaceSelector": {},
            "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-capacity-agent"}},
        },
        {"podSelector": {"matchLabels": {"app": "loom-service"}}},
    ]
    assert not any(
        "ipBlock" in peer
        for policy in policies.values()
        for direction in ("ingress", "egress")
        for rule in policy.get(direction, [])
        for peer in rule.get("from", rule.get("to", []))
    )


@pytest.mark.parametrize(
    "manager_image",
    [
        "ghcr.io/qianyi-sun/loom-capacity-manager:latest",
        "ghcr.io/qianyi-sun/loom-capacity-manager@sha256:" + "0" * 64,
        "GHCR.io/qianyi-sun/loom-capacity-manager@sha256:" + "a" * 64,
        "foo:5000:bar@sha256:" + "a" * 64,
    ],
)
def test_renderer_rejects_every_mutable_or_noncanonical_manager_image(
    manager_image: str,
) -> None:
    with pytest.raises(ValueError, match="immutable"):
        render_capacity_control_plane_manifests(
            load_capacity_control_plane_profile(_PROFILE),
            manager_image=manager_image,
            authority_incarnation=_AUTHORITY,
        )


def test_renderer_rejects_the_nil_authority_incarnation() -> None:
    with pytest.raises(ValueError, match="non-nil"):
        render_capacity_control_plane_manifests(
            load_capacity_control_plane_profile(_PROFILE),
            manager_image=_MANAGER_IMAGE,
            authority_incarnation=UUID(int=0),
        )
