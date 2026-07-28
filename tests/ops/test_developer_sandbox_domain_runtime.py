from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "ops" / "developer_sandbox_domain_runtime.py"
_CONFIG = _ROOT / "deploy" / "developer-sandboxes" / "runtime-domains.toml"
_SPEC = importlib.util.spec_from_file_location("developer_sandbox_domain_runtime", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
runtime = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = runtime
_SPEC.loader.exec_module(runtime)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(repo), *args),
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Runtime Test")
    _git(repo, "config", "user.email", "runtime@example.invalid")
    (repo / "README.md").write_text("exact candidate\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-qm", "candidate")
    return repo, _git(repo, "rev-parse", "HEAD")


def _env_text(sandbox: str, sha: str) -> str:
    root = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{sha}"
    return "\n".join(
        (
            "LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080",
            "LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100",
            "LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000",
            f"LOOM_WORKER_SANDBOX_IDENTITY={sandbox}",
            f"LOOM_WORKER_CANDIDATE_SHA={sha}",
            f"LOOM_WORKER_TOKEN_FILE_HOST={root}/worker-token",
            f"LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST={root}/minio-access-key",
            f"LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST={root}/minio-secret-key",
            f"LOOM_WORKER_CP_TLS_CA_FILE_HOST={root}/ca.pem",
            f"LOOM_WORKER_CP_TLS_CERT_FILE_HOST={root}/client.pem",
            f"LOOM_WORKER_CP_TLS_KEY_FILE_HOST={root}/client-key.pem",
            "",
        ),
    )


def test_checked_in_contract_has_two_independent_domains_and_stable_groups() -> None:
    config = runtime.load_config(_CONFIG)

    assert set(config.domains) == {"oldlab", "gb10"}
    assert (
        config.domains["oldlab"].candidate_root
        == config.domains["gb10"].candidate_root
        == Path("/shared_work/loom/candidates/sandboxes")
    )
    assert (
        config.domains["oldlab"].runtime_root
        == config.domains["gb10"].runtime_root
        == Path("/shared_work/loom/runtime/sandboxes")
    )
    assert config.domains["oldlab"].worker_env_name == "worker-oldlab.env"
    assert config.domains["gb10"].worker_env_name == "worker-gb10.env"
    assert {group.gid for group in config.sandbox_groups.values()} == {
        31021,
        31022,
        31023,
    }
    assert config.sandbox_groups["devansh"].member == "devansh"
    assert runtime._listener_ports(config.sandbox_groups["qianyi"]) == {
        "control-plane": 26080,
        "gateway": 26100,
        "minio": 26900,
    }
    assert "trt-gb10-7" not in {peer.ssh_target for peer in config.domains["gb10"].peers}


def test_paths_bind_domain_specific_env_to_same_exact_candidate_sha() -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40

    oldlab = runtime.runtime_paths(config.domains["oldlab"], "devansh", sha)
    gb10 = runtime.runtime_paths(config.domains["gb10"], "devansh", sha)

    assert oldlab.candidate == gb10.candidate
    assert oldlab.env == Path(
        f"/shared_work/loom/runtime/sandboxes/devansh/{sha}/worker-oldlab.env",
    )
    assert gb10.env == Path(
        f"/shared_work/loom/runtime/sandboxes/devansh/{sha}/worker-gb10.env",
    )


def test_publish_plan_is_exact_sha_and_secret_safe(tmp_path: Path) -> None:
    config = runtime.load_config(_CONFIG)
    repo, sha = _repository(tmp_path)
    seed = tmp_path / "worker.env"
    seed.write_text(_env_text("qianyi", sha), encoding="utf-8")
    seed.chmod(0o600)
    identity = runtime._candidate_identity(repo, sha)

    report = runtime.publish_plan(
        config,
        config.domains["oldlab"],
        "qianyi",
        identity,
        seed,
    )
    serialized = json.dumps(report)

    assert report["candidate_sha"] == sha
    assert report["candidate_tree"] == _git(repo, "rev-parse", "HEAD^{tree}")
    assert report["env_values"] == "redacted"
    assert "worker-token" not in serialized
    assert report["peer_readback"] == [
        "oldlab-1",
        "oldlab-2",
        "oldlab-3",
        "oldlab-4",
        "oldlab-5",
    ]


def test_secret_seed_rejects_group_or_world_access(tmp_path: Path) -> None:
    seed = tmp_path / "worker.env"
    seed.write_text("TOKEN=private\n", encoding="utf-8")
    seed.chmod(0o640)

    with pytest.raises(runtime.ConvergenceError, match="private regular"):
        runtime._secure_seed(seed)


def test_env_references_reject_tls_material_or_wrong_candidate(tmp_path: Path) -> None:
    seed = tmp_path / "worker.env"
    seed.write_text(
        "\n".join(
            (
                _env_text("qianyi", "a" * 40),
                "LOOM_WORKER_TOKEN=raw-material-forbidden",
            ),
        ),
        encoding="utf-8",
    )

    with pytest.raises(runtime.ConvergenceError, match="forbidden raw secret"):
        runtime._parse_env_references(
            seed,
            sandbox="qianyi",
            sha="a" * 40,
        )


@pytest.mark.parametrize(
    "raw_line",
    (
        "LOOM_WORKER_TOKEN=raw",
        "LOOM_WORKER_MINIO_ACCESS_KEY=raw",
        "LOOM_WORKER_MINIO_SECRET_KEY=raw",
        "MODEL_PROVIDER_API_KEY=raw",
        "DATABASE_PASSWORD=raw",
        "TLS_PRIVATE_KEY=raw",
    ),
)
def test_env_references_reject_every_raw_secret_class(
    tmp_path: Path,
    raw_line: str,
) -> None:
    sha = "a" * 40
    seed = tmp_path / "worker.env"
    seed.write_text(_env_text("qianyi", sha) + raw_line + "\n", encoding="utf-8")

    with pytest.raises(runtime.ConvergenceError, match="forbidden raw secret"):
        runtime._parse_env_references(seed, sandbox="qianyi", sha=sha)


def test_env_references_reject_unknown_nonsecret_field(tmp_path: Path) -> None:
    sha = "a" * 40
    seed = tmp_path / "worker.env"
    seed.write_text(_env_text("qianyi", sha) + "UNDECLARED_FLAG=1\n", encoding="utf-8")

    with pytest.raises(runtime.ConvergenceError, match="exact closed schema"):
        runtime._parse_env_references(seed, sandbox="qianyi", sha=sha)


def test_stable_group_membership_does_not_depend_on_user_uid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=2501, pw_gid=2501),
    )
    monkeypatch.setattr(
        runtime.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=31023, gr_mem=["devansh"]),
    )
    monkeypatch.setattr(runtime.os, "getgrouplist", lambda _name, _gid: [2501, 31023])

    assert runtime._group_status("loom-sandbox-devansh", 31023, "devansh") == "ok"


def test_stable_group_rejects_numeric_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=2011, pw_gid=2011),
    )

    def missing_name(_name: str) -> object:
        raise KeyError

    monkeypatch.setattr(runtime.grp, "getgrnam", missing_name)
    monkeypatch.setattr(
        runtime.grp,
        "getgrgid",
        lambda _gid: SimpleNamespace(gr_name="unrelated-service"),
    )

    with pytest.raises(runtime.ConvergenceError, match="already owned"):
        runtime._group_status("loom-sandbox-devansh", 31023, "devansh")


def test_unprivileged_execution_has_no_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime.os, "geteuid", lambda: 501)

    with pytest.raises(runtime.ConvergenceError, match="requires root"):
        runtime._require_root()


def test_config_rejects_drifting_logical_path(tmp_path: Path) -> None:
    drifted = tmp_path / "runtime-domains.toml"
    drifted.write_text(
        _CONFIG.read_text(encoding="utf-8").replace(
            'runtime_root = "/shared_work/loom/runtime/sandboxes"',
            'runtime_root = "/shared_work-other/loom/runtime/sandboxes"',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(runtime.ConvergenceError, match="logical paths"):
        runtime.load_config(drifted)


def test_config_rejects_drifting_remote_link_port(tmp_path: Path) -> None:
    drifted = tmp_path / "runtime-domains.toml"
    drifted.write_text(
        _CONFIG.read_text(encoding="utf-8").replace(
            "upstream_gateway_port = 26100",
            "upstream_gateway_port = 26101",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(runtime.ConvergenceError, match="identity is invalid"):
        runtime.load_config(drifted)


def _fleet_payload(
    config: object,
    sandbox: str,
    sha: str,
    now: datetime,
) -> dict[str, object]:
    group = config.sandbox_groups[sandbox]
    listener_ports = runtime._listener_ports(group)
    ca_fingerprint = "sha256:" + "c" * 64
    client_uri = f"spiffe://loom/developer-sandbox/{sandbox}/candidate/{sha}/worker"
    service_rows = {
        name: {"listener_port": port, "health": "ok"} for name, port in listener_ports.items()
    }
    nodes = {
        node: {
            "node": node,
            "candidate_sha": sha,
            "route": {"destination": "192.168.50.14", "status": "ok"},
            "tls_version": "TLSv1.3",
            "client_uri_san": client_uri,
            "ca_fingerprint": ca_fingerprint,
            "client_cert_fingerprint": "sha256:" + "d" * 64,
            "secret_files": {
                name: {"present": True, "uid": 0, "gid": 0, "mode": "0600"}
                for name in (
                    "worker-token",
                    "minio-access-key",
                    "minio-secret-key",
                    "client-key.pem",
                )
            },
            "services": service_rows,
        }
        for node in runtime._ELIGIBLE_LINK_NODES
    }
    payload: dict[str, object] = {
        "schema_version": 1,
        "sandbox": sandbox,
        "candidate_sha": sha,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": (now + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "eligible_nodes": list(runtime._ELIGIBLE_LINK_NODES),
        "bundle_generation": {
            "candidate_sha": sha,
            "ca_fingerprint": ca_fingerprint,
            "client_uri_san": client_uri,
        },
        "server": {
            "node": "oldlab-2",
            "address": "192.168.50.14",
            "unit": f"loom-developer-sandbox-link@{sandbox}.service",
            "unit_active": True,
            "active_candidate_sha": sha,
            "ca_fingerprint": ca_fingerprint,
            "server_cert_fingerprint": "sha256:" + "e" * 64,
            "client_uri_san": client_uri,
            "services": {
                name: {
                    "listener_port": listener_ports[name],
                    "target_host": "127.0.0.1",
                    "target_port": runtime._SANDBOX_TARGET_PORTS[sandbox][name],
                    "health_path": runtime._SERVICE_HEALTH_PATHS[name],
                    "tls_version": "TLSv1.3",
                    "status": "active",
                }
                for name in runtime._SERVICE_HEALTH_PATHS
            },
        },
        "nodes": nodes,
    }
    payload["payload_sha256"] = (
        "sha256:"
        + runtime.hashlib.sha256(
            runtime._canonical_json(payload),
        ).hexdigest()
    )
    return payload


def _fleet_reference(
    sandbox: str,
    sha: str,
    now: datetime,
) -> dict[str, object]:
    return {
        "path": str(runtime._fleet_attestation_path(sandbox, sha)),
        "payload_sha256": "sha256:" + "f" * 64,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": (now + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def test_fleet_attestation_requires_full_19_node_three_relay_contract() -> None:
    config = runtime.load_config(_CONFIG)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    payload = _fleet_payload(config, "qianyi", sha, now)

    verified, reference = runtime._verify_fleet_attestation(
        config,
        "qianyi",
        sha,
        runtime._canonical_json(payload) + b"\n",
        now=now + timedelta(seconds=30),
    )

    assert len(verified["nodes"]) == 19
    assert set(verified["server"]["services"]) == {
        "control-plane",
        "gateway",
        "minio",
    }
    assert reference["payload_sha256"].startswith("sha256:")


def test_fleet_attestation_rejects_missing_worker_node() -> None:
    config = runtime.load_config(_CONFIG)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    payload = _fleet_payload(config, "qianyi", sha, now)
    payload["nodes"].pop("trt-gb10-15")
    unsigned = dict(payload)
    unsigned.pop("payload_sha256")
    payload["payload_sha256"] = (
        "sha256:"
        + runtime.hashlib.sha256(
            runtime._canonical_json(unsigned),
        ).hexdigest()
    )

    with pytest.raises(runtime.ConvergenceError, match="node set is incomplete"):
        runtime._verify_fleet_attestation(
            config,
            "qianyi",
            sha,
            runtime._canonical_json(payload) + b"\n",
            now=now + timedelta(seconds=30),
        )


def test_fleet_attestation_rejects_digest_tamper_and_stale_generation() -> None:
    config = runtime.load_config(_CONFIG)
    generated = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    tampered = _fleet_payload(config, "qianyi", sha, generated)
    tampered["server"]["services"]["gateway"]["status"] = "inactive"

    with pytest.raises(runtime.ConvergenceError, match="payload digest is invalid"):
        runtime._verify_fleet_attestation(
            config,
            "qianyi",
            sha,
            runtime._canonical_json(tampered) + b"\n",
            now=generated + timedelta(seconds=30),
        )

    stale = _fleet_payload(config, "qianyi", sha, generated)
    with pytest.raises(runtime.ConvergenceError, match="stale or expired"):
        runtime._verify_fleet_attestation(
            config,
            "qianyi",
            sha,
            runtime._canonical_json(stale) + b"\n",
            now=generated + timedelta(seconds=61),
        )


def _domain_manifest(
    config: object,
    domain_name: str,
    sandbox: str,
    sha: str,
    tree: str,
    now: datetime,
    fleet_reference: dict[str, object],
) -> bytes:
    domain = config.domains[domain_name]
    group = config.sandbox_groups[sandbox]
    paths = runtime.runtime_paths(domain, sandbox, sha)
    bundle = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{sha}"
    payload = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-domain-attestation",
        "domain": domain_name,
        "sandbox": sandbox,
        "candidate": {
            "sha": sha,
            "tree": tree,
            "path": str(paths.candidate),
            "uid": 0,
            "gid": config.shared_gid,
            "group": config.shared_group,
            "mode": "2750",
        },
        "runtime_env": {
            "schema_version": 1,
            "path": str(paths.env),
            "uid": 0,
            "gid": group.gid,
            "group": group.name,
            "mode": "0640",
            "candidate_sha": sha,
            "local_urls": {
                "control-plane": "http://sandbox-link:8080",
                "gateway": "http://sandbox-link:9100",
                "minio": "http://sandbox-link:9000",
            },
            "oldlab2_upstreams": {
                service: f"https://192.168.50.14:{port}"
                for service, port in runtime._listener_ports(group).items()
            },
            "host_references": {
                "worker-token": f"{bundle}/worker-token",
                "minio-access-key": f"{bundle}/minio-access-key",
                "minio-secret-key": f"{bundle}/minio-secret-key",
                "ca": f"{bundle}/ca.pem",
                "cert": f"{bundle}/client.pem",
                "key": f"{bundle}/client-key.pem",
            },
        },
        "fleet_attestation": fleet_reference,
        "publisher": {
            "hostname": domain.publisher_hostname,
            "generation": 7,
            "published_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=15)).isoformat(),
            "signature_algorithm": "ed25519",
            "key_id": "f" * 64,
        },
        "eligible_peers": [
            {
                "hostname": peer.hostname,
                "candidate_inode": 101,
                "env_inode": 202,
                "result": "verified",
            }
            for peer in domain.peers
        ],
    }
    payload["payload_sha256"] = runtime.hashlib.sha256(
        runtime._canonical_json(payload),
    ).hexdigest()
    return runtime._canonical_json(payload) + b"\n"


def test_domain_attestation_schema_is_strict_fresh_and_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    tree = "b" * 40
    fleet_reference = _fleet_reference("qianyi", sha, now)
    content = _domain_manifest(
        config,
        "oldlab",
        "qianyi",
        sha,
        tree,
        now,
        fleet_reference,
    )
    monkeypatch.setattr(runtime, "_key_id", lambda _path: "f" * 64)
    monkeypatch.setattr(runtime, "_verify_signature", lambda *_args: None)

    manifest = runtime._verify_domain_attestation(
        config,
        config.domains["oldlab"],
        "qianyi",
        sha,
        content,
        b"c2lnbmF0dXJl",
        now=now + timedelta(minutes=1),
        fleet_reference=fleet_reference,
    )

    assert manifest["candidate"]["tree"] == tree
    assert manifest["publisher"]["generation"] == 7
    assert manifest["_verified_signature_sha256"]


def test_domain_attestation_rejects_incomplete_peer_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    fleet_reference = _fleet_reference("qianyi", sha, now)
    payload = json.loads(
        _domain_manifest(
            config,
            "oldlab",
            "qianyi",
            sha,
            "b" * 40,
            now,
            fleet_reference,
        ),
    )
    payload["eligible_peers"].pop()
    payload_without_digest = dict(payload)
    payload_without_digest.pop("payload_sha256")
    payload["payload_sha256"] = runtime.hashlib.sha256(
        runtime._canonical_json(payload_without_digest),
    ).hexdigest()
    content = runtime._canonical_json(payload) + b"\n"
    monkeypatch.setattr(runtime, "_key_id", lambda _path: "f" * 64)
    monkeypatch.setattr(runtime, "_verify_signature", lambda *_args: None)

    with pytest.raises(runtime.ConvergenceError, match="incomplete"):
        runtime._verify_domain_attestation(
            config,
            config.domains["oldlab"],
            "qianyi",
            sha,
            content,
            b"c2lnbmF0dXJl",
            now=now + timedelta(minutes=1),
            fleet_reference=fleet_reference,
        )


def test_domain_attestation_rejects_expired_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    published = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    fleet_reference = _fleet_reference("hongjian", sha, published)
    content = _domain_manifest(
        config,
        "gb10",
        "hongjian",
        sha,
        "b" * 40,
        published,
        fleet_reference,
    )
    monkeypatch.setattr(runtime, "_key_id", lambda _path: "f" * 64)
    monkeypatch.setattr(runtime, "_verify_signature", lambda *_args: None)

    with pytest.raises(runtime.ConvergenceError, match="stale"):
        runtime._verify_domain_attestation(
            config,
            config.domains["gb10"],
            "hongjian",
            sha,
            content,
            b"c2lnbmF0dXJl",
            now=published + timedelta(minutes=16),
            fleet_reference=fleet_reference,
        )


def test_domain_attestation_rejects_different_fleet_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=UTC)
    sha = "a" * 40
    fleet_reference = _fleet_reference("qianyi", sha, now)
    different_reference = dict(fleet_reference)
    different_reference["payload_sha256"] = "sha256:" + "0" * 64
    content = _domain_manifest(
        config,
        "oldlab",
        "qianyi",
        sha,
        "b" * 40,
        now,
        different_reference,
    )
    monkeypatch.setattr(runtime, "_key_id", lambda _path: "f" * 64)
    monkeypatch.setattr(runtime, "_verify_signature", lambda *_args: None)

    with pytest.raises(runtime.ConvergenceError, match="fleet binding is invalid"):
        runtime._verify_domain_attestation(
            config,
            config.domains["oldlab"],
            "qianyi",
            sha,
            content,
            b"c2lnbmF0dXJl",
            now=now + timedelta(minutes=1),
            fleet_reference=fleet_reference,
        )


def test_collector_requires_matching_two_domain_trees_before_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: "trt-eai-oldlab-2")
    monkeypatch.setattr(runtime, "_COMBINED_ROOT", tmp_path / "combined")
    monkeypatch.setattr(
        runtime,
        "_read_and_verify_fleet",
        lambda _config, sandbox, candidate_sha, *, now: (
            {},
            _fleet_reference(sandbox, candidate_sha, now),
        ),
    )
    monkeypatch.setattr(
        runtime,
        "_remote_attestation",
        lambda _config, domain, _sandbox, _sha: (
            domain.name.encode(),
            b"signature",
            f"/remote/{domain.name}.json",
            f"/remote/{domain.name}.sig",
        ),
    )

    def verified(
        _config: object,
        domain: object,
        _sandbox: str,
        _sha: str,
        _content: bytes,
        _signature: bytes,
        *,
        now: datetime,
        fleet_reference: object,
    ) -> dict[str, object]:
        return {
            "candidate": {"tree": "b" * 40 if domain.name == "oldlab" else "c" * 40},
            "publisher": {
                "key_id": "d" * 64,
                "generation": 1,
                "published_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=15)).isoformat(),
            },
            "payload_sha256": "e" * 64,
            "_verified_signature_sha256": "f" * 64,
        }

    monkeypatch.setattr(runtime, "_verify_domain_attestation", verified)

    with pytest.raises(runtime.ConvergenceError, match="trees do not match"):
        runtime.collect_attestations(config, "qianyi", sha, execute=False)


def test_collector_plan_has_closed_combined_receipt_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: "trt-eai-oldlab-2")
    monkeypatch.setattr(runtime, "_COMBINED_ROOT", tmp_path / "combined")
    monkeypatch.setattr(
        runtime,
        "_read_and_verify_fleet",
        lambda _config, sandbox, candidate_sha, *, now: (
            {},
            _fleet_reference(sandbox, candidate_sha, now),
        ),
    )
    monkeypatch.setattr(
        runtime,
        "_remote_attestation",
        lambda _config, domain, _sandbox, _sha: (
            domain.name.encode(),
            b"signature",
            f"/remote/{domain.name}.json",
            f"/remote/{domain.name}.sig",
        ),
    )

    def verified(
        _config: object,
        domain: object,
        _sandbox: str,
        _sha: str,
        _content: bytes,
        _signature: bytes,
        *,
        now: datetime,
        fleet_reference: object,
    ) -> dict[str, object]:
        return {
            "candidate": {"tree": "b" * 40},
            "publisher": {
                "key_id": f"{domain.name[0]}" * 64,
                "generation": 3,
                "published_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=15)).isoformat(),
            },
            "payload_sha256": f"{domain.name[-1]}" * 64,
            "_verified_signature_sha256": "f" * 64,
        }

    monkeypatch.setattr(runtime, "_verify_domain_attestation", verified)

    report = runtime.collect_attestations(config, "qianyi", sha, execute=False)
    combined = report["combined"]

    assert report["mode"] == "plan"
    assert report["target"].endswith(f"/qianyi/{sha}/combined.json")
    assert set(combined) == {
        "schema_version",
        "kind",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "collector",
        "fleet_attestation",
        "domains",
        "payload_sha256",
    }
    assert set(combined["domains"]) == {"oldlab", "gb10"}
    assert "signature_sha256" in combined["domains"]["oldlab"]
    digest = combined["payload_sha256"]
    unsigned = dict(combined)
    unsigned.pop("payload_sha256")
    assert runtime.hashlib.sha256(runtime._canonical_json(unsigned)).hexdigest() == digest
    collected = datetime.fromisoformat(combined["collector"]["collected_at"])
    expires = datetime.fromisoformat(combined["collector"]["expires_at"])
    assert timedelta(0) < expires - collected <= timedelta(minutes=15)
    assert not (tmp_path / "combined").exists()


def test_failed_execute_revokes_prior_receipt_while_lock_is_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    combined_root = tmp_path / "runtime-attestations"
    target = combined_root / "qianyi" / sha / "combined.json"
    target.parent.mkdir(parents=True)
    target.write_text("previous activation\n", encoding="utf-8")
    events: list[str] = []
    lock_active = False

    @contextmanager
    def fake_lock() -> object:
        nonlocal lock_active
        lock_active = True
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")
            lock_active = False

    def invalidate(receipt: Path) -> None:
        assert lock_active
        events.append("invalidate")
        receipt.unlink()

    def fail_checks(
        _config: object,
        _sandbox: str,
        _sha: str,
        *,
        execute: bool,
    ) -> dict[str, object]:
        assert execute is True
        assert lock_active
        assert not target.exists()
        events.append("checks")
        raise runtime.ConvergenceError("injected SSH failure")

    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: "trt-eai-oldlab-2")
    monkeypatch.setattr(runtime, "_require_capacity_root", lambda: None)
    monkeypatch.setattr(runtime, "_COMBINED_ROOT", combined_root)
    monkeypatch.setattr(runtime, "_collector_lock", fake_lock)
    monkeypatch.setattr(runtime, "_invalidate_combined_receipt", invalidate)
    monkeypatch.setattr(runtime, "_collect_attestation_checks", fail_checks)

    with pytest.raises(runtime.ConvergenceError, match="injected SSH failure"):
        runtime.collect_attestations(config, "qianyi", sha, execute=True)

    assert not target.exists()
    assert events == ["lock-enter", "invalidate", "checks", "lock-exit"]


def test_receipt_invalidation_fsyncs_parent_after_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "runtime-attestations" / "qianyi" / ("a" * 40) / "combined.json"
    target.parent.mkdir(parents=True)
    target.write_text("previous activation\n", encoding="utf-8")
    fsynced: list[Path] = []
    monkeypatch.setattr(runtime, "_ensure_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_fsync_directory", fsynced.append)

    runtime._invalidate_combined_receipt(target)

    assert not target.exists()
    assert fsynced == [target.parent]


def test_rollback_uses_the_same_transaction_lock_as_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    receipt_path = Path("/var/lib/loom-developer-domain-runtime/receipt.json")
    receipt = {
        "domain": "oldlab",
        "sandbox": "qianyi",
        "candidate_sha": "a" * 40,
    }
    events: list[str] = []

    @contextmanager
    def lock(_config: object, domain: str, sandbox: str, sha: str) -> object:
        events.append(f"lock:{domain}:{sandbox}:{sha}")
        yield

    monkeypatch.setattr(runtime, "_read_rollback_receipt", lambda *_args: receipt)
    monkeypatch.setattr(runtime, "_transaction_lock", lock)
    monkeypatch.setattr(
        runtime,
        "_rollback_locked",
        lambda *_args, **_kwargs: events.append("rollback") or {"status": "rolled-back"},
    )

    result = runtime._rollback(config, receipt_path, allow_committed=True)

    assert result["status"] == "rolled-back"
    assert events == [f"lock:oldlab:qianyi:{'a' * 40}", "rollback"]


def test_old_receipt_cannot_mutate_a_newer_attestation_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    tree = "b" * 40
    current_env = tmp_path / "worker.env"
    current_env.write_text("new-env\n", encoding="utf-8")
    (tmp_path / "previous.env").write_bytes(b"old-env\n")
    manifest = tmp_path / "oldlab.json"
    signature = tmp_path / "oldlab.sig"
    raw_signature = b"newer-signature"
    manifest.write_text(
        json.dumps(
            {
                "payload_sha256": "9" * 64,
                "publisher": {"generation": 8},
            },
        ),
        encoding="utf-8",
    )
    signature.write_bytes(runtime.base64.b64encode(raw_signature) + b"\n")
    receipt = {
        "schema_version": 1,
        "status": "committed",
        "domain": "oldlab",
        "sandbox": "qianyi",
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_created": False,
        "env_previously_existed": True,
        "previous_env_sha256": runtime.hashlib.sha256(b"old-env\n").hexdigest(),
        "published_env_sha256": runtime.hashlib.sha256(b"new-env\n").hexdigest(),
        "attestation_previously_existed": False,
        "previous_attestation_payload_sha256": None,
        "previous_attestation_signature_sha256": None,
        "attestation": {
            "payload_sha256": "8" * 64,
            "signature_sha256": runtime.hashlib.sha256(b"older-signature").hexdigest(),
            "generation": 7,
        },
    }
    mutations: list[str] = []
    monkeypatch.setattr(runtime, "_read_rollback_receipt", lambda *_args: receipt)
    monkeypatch.setattr(
        runtime,
        "runtime_paths",
        lambda *_args: SimpleNamespace(env=current_env, candidate=tmp_path / "candidate"),
    )
    monkeypatch.setattr(runtime, "_attestation_paths", lambda *_args: (manifest, signature))
    monkeypatch.setattr(runtime, "_secure_snapshot", lambda *_args: b"old-env\n")
    monkeypatch.setattr(
        runtime,
        "_atomic_env_write",
        lambda *_args, **_kwargs: mutations.append("env"),
    )
    monkeypatch.setattr(runtime, "_write_json", lambda *_args, **_kwargs: mutations.append("receipt"))

    with pytest.raises(runtime.ConvergenceError, match="no longer matches"):
        runtime._rollback_locked(config, tmp_path / "receipt.json", allow_committed=True)

    assert mutations == []
    assert current_env.read_text(encoding="utf-8") == "new-env\n"


def test_rollback_resumes_after_attestation_restore_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    current_env = tmp_path / "worker.env"
    current_env.write_text("new-env\n", encoding="utf-8")
    (tmp_path / "previous.env").write_bytes(b"old-env\n")
    receipt = {
        "schema_version": 1,
        "status": "rolling-back",
        "rollback_phase": "attestation-restored",
        "domain": "oldlab",
        "sandbox": "qianyi",
        "candidate_sha": sha,
        "candidate_tree": "b" * 40,
        "candidate_created": False,
        "env_previously_existed": True,
        "previous_env_sha256": runtime.hashlib.sha256(b"old-env\n").hexdigest(),
        "published_env_sha256": runtime.hashlib.sha256(b"new-env\n").hexdigest(),
        "attestation_previously_existed": False,
        "previous_attestation_payload_sha256": None,
        "previous_attestation_signature_sha256": None,
        "attestation": {
            "payload_sha256": "8" * 64,
            "signature_sha256": "9" * 64,
            "generation": 7,
        },
    }
    restored: list[bytes] = []
    monkeypatch.setattr(runtime, "_read_rollback_receipt", lambda *_args: receipt)
    monkeypatch.setattr(
        runtime,
        "runtime_paths",
        lambda *_args: SimpleNamespace(env=current_env, candidate=tmp_path / "candidate"),
    )
    monkeypatch.setattr(
        runtime,
        "_attestation_paths",
        lambda *_args: (tmp_path / "manifest", tmp_path / "signature"),
    )
    monkeypatch.setattr(runtime, "_secure_snapshot", lambda *_args: b"old-env\n")
    monkeypatch.setattr(
        runtime,
        "_atomic_env_write",
        lambda source, *_args, **_kwargs: restored.append(source.read_bytes()),
    )
    monkeypatch.setattr(runtime, "_write_json", lambda *_args, **_kwargs: None)

    result = runtime._rollback_locked(
        config,
        tmp_path / "receipt.json",
        allow_committed=True,
    )

    assert restored == [b"old-env\n"]
    assert result["status"] == "rolled-back"
    assert "rollback_phase" not in result
