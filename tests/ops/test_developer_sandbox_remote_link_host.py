from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import scripts.ops.developer_sandbox_remote_link_host as host

SHA = "a" * 40


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
            f"LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM=https://{profile.server_address}:{gateway.server_port}",
            f"LOOM_SANDBOX_LINK_MINIO_UPSTREAM=https://{profile.server_address}:{minio.server_port}",
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
    profiles = [host.load_profile(name) for name in host.SANDBOXES]

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
    assert len(host.ELIGIBLE_NODES) == 19
    assert "trt-gb10-7" not in host.ELIGIBLE_NODES


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


def test_fleet_check_requires_every_node(monkeypatch: pytest.MonkeyPatch) -> None:
    profile = host.load_profile("qianyi")

    def fake_run(
        argv: tuple[str, ...],
        **_: object,
    ) -> object:
        node = argv[6]
        return type(
            "Completed",
            (),
            {
                "returncode": 1 if node == "trt-gb10-15" else 0,
                "stdout": "{}",
            },
        )()

    monkeypatch.setattr(host, "_run", fake_run)

    with pytest.raises(host.LinkHostError, match="trt-gb10-15"):
        host.fleet_check(profile, SHA, ssh_path="ssh")


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
    monkeypatch.setattr(host, "_ensure_root_dir", local_private_dir)
    monkeypatch.setattr(host, "_ensure_root_owned_parent", local_parent)
    monkeypatch.setattr(os, "chown", lambda *_: None)
    profile = host.load_profile("qianyi")

    issuance = host.prepare_rotation(profile, SHA)

    assert (issuance / "server" / "server-key.pem").stat().st_mode & 0o777 == 0o600
    assert len(tuple((issuance / "clients").iterdir())) == len(
        host.ELIGIBLE_NODES,
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
        for node in host.ELIGIBLE_NODES
    }
    payload: dict[str, object] = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": SHA,
        "generated_at": generated.isoformat().replace("+00:00", "Z"),
        "expires_at": (generated + timedelta(seconds=host.ATTESTATION_TTL_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "eligible_nodes": list(host.ELIGIBLE_NODES),
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
    assert destination.read_text(encoding="utf-8").count("trt-gb10-7") == 0


def test_fleet_attestation_rejects_missing_node_or_stale_payload() -> None:
    profile = host.load_profile("qianyi")
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
