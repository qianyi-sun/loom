from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import scripts.ops.developer_sandbox_remote_link_host as host
from scripts.ops import developer_environment_registry as environment_registry

SHA = "a" * 40
TREE = "b" * 40


def _archive_binding(
    *,
    config_digest: str,
    archive_digest: str,
    index_digest: str,
    manifest_digest: str,
    size: int,
) -> dict[str, object]:
    media_type = "application/vnd.oci.image.manifest.v1+json"
    return {
        "sha256": archive_digest,
        "size": size,
        "config_digest": config_digest,
        "index_digest": index_digest,
        "manifest_digest": manifest_digest,
        "manifest_media_type": media_type,
        "load_descriptor_digest": manifest_digest,
        "load_descriptor_media_type": media_type,
    }


def _runtime_bindings(
    candidate: environment_registry.CandidateRecord,
) -> dict[str, object]:
    nodes: dict[str, dict[str, object]] = {}
    domains: dict[str, dict[str, object]] = {}
    for domain, architecture in environment_registry.WORKER_RUNTIME_BINDING_DOMAINS.items():
        archive = candidate.image_archives[architecture]
        backend = "containerd-snapshotter-v1" if domain == "oldlab" else "classic-overlay2"
        binding = {
            "architecture": architecture,
            "docker_driver": environment_registry.WORKER_RUNTIME_BACKENDS[backend],
            "docker_backend": backend,
            "config_digest": archive["config_digest"],
            "load_descriptor_digest": archive["load_descriptor_digest"],
            "load_descriptor_media_type": archive["load_descriptor_media_type"],
            "runtime_image_id": (
                archive["load_descriptor_digest"]
                if backend == "containerd-snapshotter-v1"
                else archive["config_digest"]
            ),
        }
        domains[domain] = binding
        for node in environment_registry.FLEET_NODES:
            node_domain = "oldlab" if node.startswith("oldlab-") else "gb10"
            if node_domain == domain:
                nodes[node] = {
                    "domain": domain,
                    **binding,
                    "docker_descriptor_digest": (
                        archive["load_descriptor_digest"]
                        if backend == "containerd-snapshotter-v1"
                        else None
                    ),
                    "docker_descriptor_media_type": (
                        archive["load_descriptor_media_type"]
                        if backend == "containerd-snapshotter-v1"
                        else None
                    ),
                    "receipt_sha256": hashlib.sha256(node.encode("ascii")).hexdigest(),
                }
    return {"nodes": nodes, "domains": domains}


def _registry_snapshot() -> dict[str, object]:
    environments: list[dict[str, object]] = []
    candidates: list[dict[str, object]] = []
    deployments: list[dict[str, object]] = []
    for index, sandbox in enumerate(("qianyi", "hongjian", "devansh"), start=0):
        candidate_id = f"cand-{SHA}"
        env_id = f"denv-legacy-{index:016x}"
        environments.append(
            {
                "env_id": env_id,
                "principal_id": f"unix-uid:{31021 + index}",
                "runtime_id": sandbox,
                "state": "active",
                "resource_generation": 2,
                "current_candidate_id": candidate_id,
                "ports": {
                    "control_plane": 20080 + index * 1000,
                    "llm_gateway": 20100 + index * 1000,
                    "minio": 20900 + index * 1000,
                    "relay_control_plane": 26080 + index * 1000,
                    "relay_gateway": 26100 + index * 1000,
                    "relay_minio": 26900 + index * 1000,
                },
            },
        )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "env_id": env_id,
                "principal_id": f"unix-uid:{31021 + index}",
                "candidate_sha": SHA,
                "candidate_tree": TREE,
            },
        )
        deployments.append(
            {
                "deployment_id": f"dep-{index:032x}",
                "env_id": env_id,
                "principal_id": f"unix-uid:{31021 + index}",
                "candidate_id": candidate_id,
                "expected_resource_generation": 1,
                "applied_resource_generation": 2,
                "applied_registry_generation": 6,
                "applied_registry_payload_sha256": "e" * 64,
                "phase": "committed",
            },
        )
    return {
        "generation": 7,
        "payload_sha256": "f" * 64,
        "environments": environments,
        "candidates": candidates,
        "deployments": deployments,
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        host, "_load_registry_snapshot", lambda _path=host.REGISTRY_SNAPSHOT: _registry_snapshot()
    )


@contextmanager
def _no_link_transaction(_profile: host.Profile) -> Iterator[None]:
    yield


def _valid_env(profile: host.Profile) -> str:
    root = host.CLIENT_ROOT / profile.sandbox / SHA
    control_plane = profile.service("control-plane")
    gateway = profile.service("gateway")
    minio = profile.service("minio")
    return "\n".join(
        (
            "LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080",
            "LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100",
            "LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000",
            f"LOOM_SANDBOX_LINK_CP_UPSTREAM=https://{profile.server_address}:{control_plane.server_port}",
            f"LOOM_SANDBOX_LINK_CP_EXPECTED_PORT={control_plane.server_port}",
            f"LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM=https://{profile.server_address}:{gateway.server_port}",
            f"LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT={gateway.server_port}",
            f"LOOM_SANDBOX_LINK_MINIO_UPSTREAM=https://{profile.server_address}:{minio.server_port}",
            f"LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT={minio.server_port}",
            f"LOOM_WORKER_TOKEN_FILE_HOST={root / 'worker-token'}",
            f"LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST={root / 'minio-access-key'}",
            f"LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST={root / 'minio-secret-key'}",
            f"LOOM_WORKER_CP_TLS_CA_FILE_HOST={root / 'ca.pem'}",
            f"LOOM_WORKER_CP_TLS_CERT_FILE_HOST={root / 'client.pem'}",
            f"LOOM_WORKER_CP_TLS_KEY_FILE_HOST={root / 'client-key.pem'}",
            "",
        ),
    )


def test_profiles_are_closed_and_collision_free() -> None:
    profiles = [host.load_profile(name) for name in host.registered_sandboxes()]

    assert {profile.server_address for profile in profiles} == {"192.168.50.14"}
    assert (
        len(
            {service.server_port for profile in profiles for service in profile.services},
        )
        == 9
    )
    assert (
        len(
            {service.target_port for profile in profiles for service in profile.services},
        )
        == 9
    )
    assert all(len(profile.services) == 3 for profile in profiles)
    assert len(host.INFRASTRUCTURE_LINK_NODES) == 20
    assert "trt-gb10-7" in host.INFRASTRUCTURE_LINK_NODES


def test_fourth_environment_is_discovered_without_a_profile_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _registry_snapshot()
    environment = dict(snapshot["environments"][0])
    environment.update(
        {
            "env_id": "denv-dynamic-44444444",
            "principal_id": "unix-uid:31444",
            "runtime_id": "e-fourth",
            "resource_generation": 4,
            "current_candidate_id": "cand-" + "4" * 40,
            "ports": {
                "control_plane": 34080,
                "llm_gateway": 34100,
                "minio": 34900,
                "relay_control_plane": 36080,
                "relay_gateway": 36100,
                "relay_minio": 36900,
            },
        },
    )
    snapshot["environments"].append(environment)
    snapshot["candidates"].append(
        {
            "candidate_id": "cand-" + "4" * 40,
            "env_id": environment["env_id"],
            "principal_id": environment["principal_id"],
            "candidate_sha": "4" * 40,
            "candidate_tree": "5" * 40,
        },
    )
    snapshot["deployments"].append(
        {
            "deployment_id": "dep-" + "4" * 32,
            "env_id": environment["env_id"],
            "principal_id": environment["principal_id"],
            "candidate_id": "cand-" + "4" * 40,
            "expected_resource_generation": 3,
            "applied_resource_generation": 4,
            "applied_registry_generation": 6,
            "applied_registry_payload_sha256": "e" * 64,
            "phase": "committed",
        },
    )
    monkeypatch.setattr(
        host, "_load_registry_snapshot", lambda _path=host.REGISTRY_SNAPSHOT: snapshot
    )

    profile = host.load_profile("e-fourth")
    assert profile.env_id == "denv-dynamic-44444444"
    assert profile.service("gateway").server_port == 36100
    assert "e-fourth" in host.registered_sandboxes()


def test_first_committed_registry_generation_is_an_active_link_member(
    tmp_path: Path,
) -> None:
    registry = environment_registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = registry.register(
        {
            "schema_version": 1,
            "kind": environment_registry.REGISTER_KIND,
            "principal_id": "oidc:example:link-binding",
            "idempotency_key": "link-register-0001",
            "display_name": "Link binding",
        }
    )
    candidate = registry.import_candidate(
        {
            "schema_version": 1,
            "kind": environment_registry.CANDIDATE_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "link-candidate-0001",
            "env_id": environment.env_id,
            "candidate_sha": "4" * 40,
            "candidate_tree": "5" * 40,
            "bundle_sha256": "6" * 64,
            "bundle_size": 1024,
            "image_digests": {
                "amd64": "sha256:" + "7" * 64,
                "arm64": "sha256:" + "8" * 64,
            },
            "image_archives": {
                "amd64": _archive_binding(
                    config_digest="sha256:" + "7" * 64,
                    archive_digest="9" * 64,
                    index_digest="sha256:" + "b" * 64,
                    manifest_digest="sha256:" + "c" * 64,
                    size=2048,
                ),
                "arm64": _archive_binding(
                    config_digest="sha256:" + "8" * 64,
                    archive_digest="a" * 64,
                    index_digest="sha256:" + "d" * 64,
                    manifest_digest="sha256:" + "e" * 64,
                    size=4096,
                ),
            },
        }
    )
    deployment = registry.begin_deployment(
        {
            "schema_version": 1,
            "kind": environment_registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "link-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    deployment = registry.record_worker_runtime_bindings(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
        bindings=_runtime_bindings(candidate),
    )
    for expected, following in zip(
        environment_registry.DEPLOY_PHASES[:-1],
        environment_registry.DEPLOY_PHASES[1:],
        strict=True,
    ):
        if following == "committed":
            registry.prepare_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=environment.resource_generation,
            )
            registry.record_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=environment.resource_generation,
                evidence={
                    "capacity_finalize_receipt_sha256": "1" * 64,
                    "capacity_finalize_check_receipt_sha256": "2" * 64,
                    "runtime_reconcile_receipt_sha256": "3" * 64,
                    "runtime_prepare_check_receipt_sha256": "4" * 64,
                    "acceptance_probe_receipt_sha256": "5" * 64,
                },
            )
        registry.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
    snapshot = registry.snapshot()

    environment_row, candidate_row, deployment_row = host._registry_environment(
        environment.runtime_id,
        snapshot=snapshot,
        allow_provisioning=False,
    )

    assert candidate_row["candidate_id"] == candidate.candidate_id
    assert deployment_row["applied_resource_generation"] == environment_row["resource_generation"]


def test_stale_resource_generation_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _registry_snapshot()
    snapshot["environments"][0]["resource_generation"] = 3
    monkeypatch.setattr(
        host, "_load_registry_snapshot", lambda _path=host.REGISTRY_SNAPSHOT: snapshot
    )
    with pytest.raises(host.LinkHostError, match="not an admitted"):
        host.load_profile("qianyi")


def test_provisioning_is_separate_from_positive_active_cohort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _registry_snapshot()
    snapshot["environments"][0]["state"] = "deploying"
    snapshot["environments"][0]["current_candidate_id"] = None
    snapshot["deployments"][0]["phase"] = "services-prepared"
    snapshot["deployments"][0]["expected_resource_generation"] = 2
    snapshot["deployments"][0]["applied_resource_generation"] = None
    snapshot["deployments"][0]["applied_registry_generation"] = None
    snapshot["deployments"][0]["applied_registry_payload_sha256"] = None
    monkeypatch.setattr(
        host, "_load_registry_snapshot", lambda _path=host.REGISTRY_SNAPSHOT: snapshot
    )
    with pytest.raises(host.LinkHostError, match="not an admitted"):
        host.load_profile("qianyi")
    assert host.load_profile("qianyi", allow_provisioning=True).lifecycle_state == "deploying"


def test_server_config_rejects_loopback_target_drift(tmp_path: Path) -> None:
    profile = host.load_profile("qianyi")
    candidate_root = tmp_path / SHA
    candidate_root.mkdir()
    services = "".join(
        (
            f"\n[services.{service.name}]\n"
            f"bind_port = {service.server_port}\n"
            f"target_port = {service.target_port}\n"
            f'health_path = "{service.health_path}"\n'
            f"allow_empty_health = {str(service.allow_empty_health).lower()}\n"
        )
        for service in profile.services
    )
    config = (
        "schema_version = 1\n"
        f'sandbox = "{profile.sandbox}"\n'
        f'candidate_sha = "{SHA}"\n'
        f'bind_address = "{profile.server_address}"\n'
        'target_host = "127.0.0.1"\n'
        f'ca_file = "{candidate_root / "ca.pem"}"\n'
        f'cert_file = "{candidate_root / "server.pem"}"\n'
        f'key_file = "{candidate_root / "server-key.pem"}"\n'
        f"{services}"
    )
    config_path = candidate_root / "config.toml"
    config_path.write_text(config, encoding="utf-8")
    host._validate_installed_server_config(profile, SHA, candidate_root)

    config_path.write_text(
        config.replace("target_port = 20100", "target_port = 20101"),
        encoding="utf-8",
    )
    with pytest.raises(host.LinkHostError, match="closed profile"):
        host._validate_installed_server_config(profile, SHA, candidate_root)


def test_worker_env_accepts_only_candidate_bound_host_local_references(
    tmp_path: Path,
) -> None:
    profile = host.load_profile("qianyi")
    env_file = tmp_path / "worker.env"
    env_file.write_text(_valid_env(profile), encoding="utf-8")

    expected = host.validate_worker_env(profile, SHA, env_file)

    assert expected["LOOM_WORKER_TOKEN_FILE_HOST"].startswith(
        "/etc/loom/developer-sandbox-links/clients/qianyi/",
    )


def test_worker_env_rejects_raw_token(tmp_path: Path) -> None:
    profile = host.load_profile("qianyi")
    env_file = tmp_path / "worker.env"
    env_file.write_text(
        _valid_env(profile) + "LOOM_WORKER_TOKEN=loom_w_leak\n",
        encoding="utf-8",
    )

    with pytest.raises(host.LinkHostError, match="raw secrets"):
        host.validate_worker_env(profile, SHA, env_file)


def test_worker_env_rejects_shared_filesystem_secret_reference(
    tmp_path: Path,
) -> None:
    profile = host.load_profile("qianyi")
    env_file = tmp_path / "worker.env"
    env_file.write_text(
        _valid_env(profile).replace(
            str(host.CLIENT_ROOT / "qianyi" / SHA / "worker-token"),
            f"/shared_work/loom/runtime/sandboxes/qianyi/{SHA}/worker-token",
        ),
        encoding="utf-8",
    )

    with pytest.raises(host.LinkHostError, match="candidate-bound"):
        host.validate_worker_env(profile, SHA, env_file)


def test_mutation_commands_are_plan_only_by_default(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        host.main(
            [
                "prepare-rotation",
                "--sandbox",
                "qianyi",
                "--candidate-sha",
                SHA,
            ],
        )
        == 0
    )

    output = capsys.readouterr().out
    assert '"execute": false' in output
    assert "worker-token" not in output


def test_remote_link_host_has_no_fleet_or_ssh_command_surface() -> None:
    source = Path(host.__file__).read_text(encoding="utf-8")
    assert "fleet-check" not in source
    assert "--ssh" not in source
    assert "BatchMode" not in source

    with pytest.raises(SystemExit):
        host.build_parser().parse_args(
            [
                "fleet-check",
                "--sandbox",
                "qianyi",
                "--candidate-sha",
                SHA,
            ],
        )


def test_prepare_and_install_candidate_credentials_are_exact_and_host_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issuance_root = tmp_path / "issuance"
    client_root = tmp_path / "clients"
    installed_host = tmp_path / "bin" / "host"

    def local_private_dir(path: Path) -> None:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.chmod(0o700)

    def local_parent(path: Path) -> None:
        path.mkdir(parents=True, mode=0o755, exist_ok=True)

    monkeypatch.setattr(host, "ISSUANCE_ROOT", issuance_root)
    monkeypatch.setattr(host, "CLIENT_ROOT", client_root)
    monkeypatch.setattr(host, "INSTALLED_HOST", installed_host)
    monkeypatch.setattr(host, "_link_transaction", _no_link_transaction)
    monkeypatch.setattr(host, "_ensure_root_dir", local_private_dir)
    monkeypatch.setattr(host, "_ensure_root_owned_parent", local_parent)
    monkeypatch.setattr(os, "chown", lambda *_: None)
    monkeypatch.setattr(host, "_validate_existing_issuance", lambda *_args: None)
    profile = host.load_profile("qianyi")

    issuance = host.prepare_rotation(profile, SHA)

    assert (issuance / "server" / "server-key.pem").stat().st_mode & 0o777 == 0o600
    assert len(tuple((issuance / "clients").iterdir())) == len(
        host.INFRASTRUCTURE_LINK_NODES,
    )
    node_source = issuance / "clients" / "oldlab-1"
    cert_text = host._certificate_text(node_source / "client.pem")
    assert profile.client_uri(SHA) in cert_text
    assert "DNS:oldlab-1" in cert_text
    token_source = tmp_path / "worker-token"
    token_source.write_text("loom_w_candidate_secret\n", encoding="utf-8")
    token_source.chmod(0o600)
    access_source = tmp_path / "minio-access-key"
    secret_source = tmp_path / "minio-secret-key"
    access_source.write_text("candidate-access\n", encoding="utf-8")
    secret_source.write_text("candidate-secret\n", encoding="utf-8")
    access_source.chmod(0o600)
    secret_source.chmod(0o600)

    installed = host.install_client(
        profile,
        SHA,
        "oldlab-1",
        node_source,
        token_source,
        access_source,
        secret_source,
    )

    assert installed == client_root / "qianyi" / SHA
    assert (installed / "worker-token").read_text(encoding="utf-8") == ("loom_w_candidate_secret\n")
    assert (installed / "client-key.pem").stat().st_mode & 0o777 == 0o600
    assert (installed / "minio-access-key").read_text(encoding="utf-8") == ("candidate-access\n")
    assert "/shared_work" not in str(installed)


def test_prepare_rotation_reuses_a_verified_complete_same_sha_issuance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    issuance = tmp_path / profile.sandbox / SHA
    issuance.mkdir(parents=True)
    verified: list[Path] = []
    monkeypatch.setattr(host, "ISSUANCE_ROOT", tmp_path)
    monkeypatch.setattr(host, "_link_transaction", _no_link_transaction)
    monkeypatch.setattr(host, "_recover_activation", lambda *_args: None)
    monkeypatch.setattr(host, "_invalidate_activation_proofs", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_validate_existing_issuance",
        lambda _profile, _sha, path: verified.append(path),
    )

    assert host.prepare_rotation(profile, SHA) == issuance
    assert verified == [issuance]


def _valid_attestation(profile: host.Profile) -> dict[str, object]:
    generated = datetime.now(UTC).replace(microsecond=0)
    fingerprint = "sha256:" + "1" * 64
    server_fingerprint = "sha256:" + "2" * 64
    client_fingerprint = "sha256:" + "3" * 64
    secret_files = {
        name: {"present": True, "uid": 0, "gid": 0, "mode": "0600"}
        for name in (
            "worker-token",
            "minio-access-key",
            "minio-secret-key",
            "client-key.pem",
        )
    }
    server_services = {
        service.name: {
            "listener_port": service.server_port,
            "target_host": "127.0.0.1",
            "target_port": service.target_port,
            "health_path": service.health_path,
            "tls_version": "TLSv1.3",
            "status": "active",
        }
        for service in profile.services
    }
    nodes = {
        node: {
            "node": node,
            "candidate_sha": SHA,
            "route": {"destination": profile.server_address, "status": "ok"},
            "tls_version": "TLSv1.3",
            "client_uri_san": profile.client_uri(SHA),
            "ca_fingerprint": fingerprint,
            "client_cert_fingerprint": client_fingerprint,
            "secret_files": secret_files,
            "services": {
                service.name: {
                    "listener_port": service.server_port,
                    "health": "ok",
                }
                for service in profile.services
            },
        }
        for node in host.INFRASTRUCTURE_LINK_NODES
    }
    payload: dict[str, object] = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "env_id": profile.env_id,
        "resource_generation": profile.resource_generation,
        "registry_generation": profile.registry_generation,
        "registry_payload_sha256": profile.registry_payload_sha256,
        "candidate_sha": SHA,
        "candidate_tree": profile.candidate_tree,
        "generated_at": generated.isoformat().replace("+00:00", "Z"),
        "expires_at": (generated + timedelta(seconds=host.ATTESTATION_TTL_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "eligible_nodes": list(host.INFRASTRUCTURE_LINK_NODES),
        "bundle_generation": {
            "candidate_sha": SHA,
            "ca_fingerprint": fingerprint,
            "client_uri_san": profile.client_uri(SHA),
        },
        "server": {
            "node": "oldlab-2",
            "address": profile.server_address,
            "unit": "loom-developer-sandbox-link@qianyi.service",
            "unit_active": True,
            "active_candidate_sha": SHA,
            "ca_fingerprint": fingerprint,
            "server_cert_fingerprint": server_fingerprint,
            "client_uri_san": profile.client_uri(SHA),
            "services": server_services,
        },
        "nodes": nodes,
    }
    payload["payload_sha256"] = host._attestation_digest(payload)
    return payload


def test_fleet_attestation_persists_only_complete_fresh_closed_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    monkeypatch.setattr(host, "ATTESTATION_ROOT", tmp_path / "attestations")
    monkeypatch.setattr(host, "_link_transaction", _no_link_transaction)
    monkeypatch.setattr(
        host,
        "_ensure_root_dir",
        lambda path: path.mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(os, "chown", lambda *_: None)
    payload = _valid_attestation(profile)

    destination = host.persist_attestation(profile, SHA, payload)

    assert destination.stat().st_mode & 0o777 == 0o600
    assert destination.name == "fleet.json"
    persisted = json.loads(destination.read_text(encoding="utf-8"))
    assert "trt-gb10-7" in persisted["eligible_nodes"]
    assert persisted["nodes"]["trt-gb10-7"]["node"] == "trt-gb10-7"


def test_fleet_attestation_rejects_missing_node_or_stale_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    monkeypatch.setattr(host, "_link_transaction", _no_link_transaction)
    missing = _valid_attestation(profile)
    missing["nodes"].pop("trt-gb10-15")  # type: ignore[union-attr]
    missing["payload_sha256"] = host._attestation_digest(missing)
    with pytest.raises(host.LinkHostError, match="closed schema"):
        host.persist_attestation(profile, SHA, missing)

    stale = _valid_attestation(profile)
    stale_time = datetime.now(UTC) - timedelta(minutes=5)
    stale["generated_at"] = stale_time.isoformat().replace("+00:00", "Z")
    stale["expires_at"] = (
        (stale_time + timedelta(seconds=host.ATTESTATION_TTL_SECONDS))
        .isoformat()
        .replace("+00:00", "Z")
    )
    stale["payload_sha256"] = host._attestation_digest(stale)
    with pytest.raises(host.LinkHostError, match="freshness"):
        host.persist_attestation(profile, SHA, stale)


def test_activation_restart_failure_restores_previous_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    server_root = tmp_path / "server"
    (server_root / profile.sandbox / "candidates" / SHA).mkdir(parents=True)
    events: list[str] = []
    monkeypatch.setattr(host, "SERVER_ROOT", server_root)
    monkeypatch.setattr(host, "_validate_installed_server_config", lambda *_args: None)
    monkeypatch.setattr(host, "_current_server_sha", lambda _profile: "b" * 40)
    monkeypatch.setattr(
        host,
        "_write_journal",
        lambda _profile, _sha, _previous, phase: events.append(f"journal:{phase}"),
    )
    monkeypatch.setattr(
        host,
        "_atomic_symlink",
        lambda target, _link: events.append(f"switch:{target}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_activation",
        lambda _profile, previous: events.append(f"restore:{previous}"),
    )
    monkeypatch.setattr(host, "_remove_journal", lambda _profile: events.append("remove"))

    def run(argv: tuple[str, ...], **_kwargs: object) -> object:
        if argv[:2] == ("systemctl", "restart"):
            raise host.LinkHostError("injected restart failure")
        return type("Completed", (), {"stdout": ""})()

    monkeypatch.setattr(host, "_run", run)

    with pytest.raises(host.LinkHostError, match="injected"):
        host._activate_server_locked(profile, SHA)

    assert events == [
        "journal:switching",
        f"switch:candidates/{SHA}",
        "journal:switched",
        "journal:restarting",
        f"restore:{'b' * 40}",
        "remove",
    ]


def test_activation_orphan_recovery_restores_previous_before_next_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    events: list[str] = []
    monkeypatch.setattr(
        host,
        "_read_journal",
        lambda _profile: {
            "candidate_sha": SHA,
            "previous_sha": "b" * 40,
            "phase": "switched",
        },
    )
    monkeypatch.setattr(
        host,
        "_restore_activation",
        lambda _profile, previous: events.append(f"restore:{previous}"),
    )
    monkeypatch.setattr(host, "_remove_journal", lambda _profile: events.append("remove"))

    host._recover_activation(profile)

    assert events == [f"restore:{'b' * 40}", "remove"]


def test_same_sha_mutation_invalidates_fleet_and_combined_proofs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = host.load_profile("qianyi")
    fleet_root = tmp_path / "fleet"
    combined_root = tmp_path / "combined"
    fleet = fleet_root / profile.sandbox / SHA / "fleet.json"
    combined = combined_root / profile.sandbox / SHA / "combined.json"
    fleet.parent.mkdir(parents=True)
    combined.parent.mkdir(parents=True)
    fleet.write_text("fleet\n", encoding="utf-8")
    combined.write_text("combined\n", encoding="utf-8")
    fsynced: list[Path] = []
    monkeypatch.setattr(host, "ATTESTATION_ROOT", fleet_root)
    monkeypatch.setattr(host, "COMBINED_RECEIPT_ROOT", combined_root)
    monkeypatch.setattr(host, "_fsync_directory", fsynced.append)

    host._invalidate_activation_proofs(profile, SHA)

    assert not fleet.exists()
    assert not combined.exists()
    assert fsynced == [fleet.parent, combined.parent]
