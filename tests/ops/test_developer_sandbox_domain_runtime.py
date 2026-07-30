from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import developer_environment_registry as environment_registry
from scripts.ops import developer_sandbox_node_authority as authority

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "ops" / "developer_sandbox_domain_runtime.py"
_CONFIG = _ROOT / "deploy" / "developer-sandboxes" / "runtime-domains.toml"
_SPEC = importlib.util.spec_from_file_location("developer_sandbox_domain_runtime", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
runtime = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = runtime
_SPEC.loader.exec_module(runtime)


def _registry_snapshot() -> dict[str, object]:
    environments: list[dict[str, object]] = []
    candidates: list[dict[str, object]] = []
    deployments: list[dict[str, object]] = []
    finalizations: list[dict[str, object]] = []
    for index, sandbox in enumerate(("qianyi", "hongjian", "devansh")):
        env_id = f"denv-legacy-{index:016x}"
        candidate_id = f"cand-{'a' * 40}"
        principal = f"unix-uid:{31021 + index}"
        environments.append(
            {
                "env_id": env_id,
                "principal_id": principal,
                "runtime_id": sandbox,
                "state": "active",
                "resource_generation": 2,
                "service_user": f"loom-sandbox-{sandbox}",
                "service_group": f"loom-sandbox-{sandbox}",
                "uid": 31021 + index,
                "gid": 31021 + index,
                "ports": {
                    "control_plane": 20080 + index * 1000,
                    "llm_gateway": 20100 + index * 1000,
                    "minio": 20900 + index * 1000,
                    "relay_control_plane": 26080 + index * 1000,
                    "relay_gateway": 26100 + index * 1000,
                    "relay_minio": 26900 + index * 1000,
                },
                "current_candidate_id": candidate_id,
                "candidate_root": f"/shared_work/loom/candidates/sandboxes/{sandbox}",
                "runtime_root": f"/shared_work/loom/runtime/sandboxes/{sandbox}",
                "compose_project": f"loom-sandbox-{sandbox}",
                "slurm_account": f"loom-dev-{sandbox}",
                "slurm_qos": f"loom-dev-{sandbox}",
                "slurm_user": f"loom-sandbox-{sandbox}",
            },
        )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "env_id": env_id,
                "principal_id": principal,
                "candidate_sha": "a" * 40,
                "candidate_tree": "b" * 40,
            },
        )
        deployment_id = f"dep-{index:032x}"
        finalization = {
            "deployment_id": deployment_id,
            "env_id": env_id,
            "principal_id": principal,
            "candidate_id": candidate_id,
            "candidate_sha": "a" * 40,
            "candidate_tree": "b" * 40,
            "applied_resource_generation": 2,
            "applied_registry_generation": 8,
            "applied_registry_payload_sha256": "e" * 64,
            "capacity_finalize_receipt_sha256": "1" * 64,
            "capacity_finalize_check_receipt_sha256": "2" * 64,
            "runtime_reconcile_receipt_sha256": "3" * 64,
            "runtime_prepare_check_receipt_sha256": "4" * 64,
            "acceptance_probe_receipt_sha256": "5" * 64,
            "created_at": "2026-07-29T12:00:00Z",
        }
        finalization_digest = hashlib.sha256(authority._canonical(finalization)).hexdigest()
        finalizations.append({**finalization, "payload_sha256": finalization_digest})
        deployments.append(
            {
                "deployment_id": deployment_id,
                "env_id": env_id,
                "principal_id": principal,
                "candidate_id": candidate_id,
                "expected_resource_generation": 1,
                "applied_resource_generation": 2,
                "applied_registry_generation": 8,
                "applied_registry_payload_sha256": "e" * 64,
                "finalization_payload_sha256": finalization_digest,
                "phase": "committed",
            },
        )
    return {
        "generation": 9,
        "payload_sha256": "f" * 64,
        "environments": environments,
        "candidates": candidates,
        "deployments": deployments,
        "deployment_finalizations": finalizations,
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "_load_registry_snapshot", lambda _path: _registry_snapshot())
    monkeypatch.setattr(authority, "_load_registry_snapshot", _registry_snapshot)


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


def _env_text(
    sandbox: str,
    sha: str,
    *,
    domain: str = "oldlab",
    candidate_tree: str = "b" * 40,
) -> str:
    root = f"/etc/loom/developer-sandbox-links/clients/{sandbox}/{sha}"
    concurrency = {"oldlab": 4, "gb10": 8}[domain]
    index = ("qianyi", "hongjian", "devansh").index(sandbox)
    control_plane_port = 26080 + index * 1000
    gateway_port = 26100 + index * 1000
    minio_port = 26900 + index * 1000
    return "\n".join(
        (
            "LOOM_WORKER_CONTROL_PLANE_URL=http://sandbox-link:8080",
            "LOOM_WORKER_GATEWAY_URL=http://sandbox-link:9100",
            "LOOM_WORKER_MINIO_ENDPOINT=http://sandbox-link:9000",
            f"LOOM_WORKER_SANDBOX_IDENTITY={sandbox}",
            f"LOOM_WORKER_CANDIDATE_SHA={sha}",
            f"LOOM_SANDBOX_LINK_CP_UPSTREAM=https://192.168.50.14:{control_plane_port}",
            f"LOOM_SANDBOX_LINK_CP_EXPECTED_PORT={control_plane_port}",
            f"LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM=https://192.168.50.14:{gateway_port}",
            f"LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT={gateway_port}",
            f"LOOM_SANDBOX_LINK_MINIO_UPSTREAM=https://192.168.50.14:{minio_port}",
            f"LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT={minio_port}",
            f"LOOM_WORKER_COMPOSE_PROJECT=loom-sandbox-{sandbox}",
            f"LOOM_WORKER_ENV_ID=denv-legacy-{index:016x}",
            "LOOM_WORKER_RESOURCE_GENERATION=2",
            f"LOOM_WORKER_CANDIDATE_ID=cand-{'a' * 40}",
            f"LOOM_WORKER_CANDIDATE_TREE={candidate_tree}",
            "LOOM_WORKER_REGISTRY_GENERATION=9",
            f"LOOM_WORKER_REGISTRY_PAYLOAD_SHA256={'f' * 64}",
            f"LOOM_WORKER_POOL_NAME={domain}",
            f"LOOM_WORKER_MAX_CONCURRENT={concurrency}",
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
    assert (
        config.domains["oldlab"].worker_pool_name,
        config.domains["oldlab"].worker_max_concurrent,
    ) == ("oldlab", 4)
    assert (
        config.domains["gb10"].worker_pool_name,
        config.domains["gb10"].worker_max_concurrent,
    ) == ("gb10", 8)
    assert {group.gid for group in config.sandbox_groups.values()} == {
        31021,
        31022,
        31023,
    }
    assert {group.uid for group in config.sandbox_groups.values()} == {
        31021,
        31022,
        31023,
    }
    assert config.sandbox_groups["devansh"].member == "loom-sandbox-devansh"
    assert runtime._listener_ports(config.sandbox_groups["qianyi"]) == {
        "control-plane": 26080,
        "gateway": 26100,
        "minio": 26900,
    }
    gb10_peers = {peer.ssh_target: peer.hostname for peer in config.domains["gb10"].peers}
    assert len(gb10_peers) == 15
    assert gb10_peers["trt-gb10-7"] == "gx10-0faf"


def test_fourth_environment_uses_registry_allocated_identity_and_paths(
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
            "service_user": "loom-e-fourth",
            "service_group": "loom-e-fourth",
            "uid": 31444,
            "gid": 31444,
            "ports": {
                "control_plane": 34080,
                "llm_gateway": 34100,
                "minio": 34900,
                "relay_control_plane": 36080,
                "relay_gateway": 36100,
                "relay_minio": 36900,
            },
            "current_candidate_id": "cand-" + "4" * 40,
            "candidate_root": "/shared_work/loom/candidates/environments/denv-dynamic-44444444",
            "runtime_root": "/shared_work/loom/runtime/environments/denv-dynamic-44444444",
            "compose_project": "loom-env-fourth",
            "slurm_account": "lda-fourth",
            "slurm_qos": "ldq-fourth",
            "slurm_user": "loom-e-fourth",
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
            "applied_registry_generation": 8,
            "applied_registry_payload_sha256": "e" * 64,
            "phase": "committed",
        },
    )
    monkeypatch.setattr(runtime, "_load_registry_snapshot", lambda _path: snapshot)

    config = runtime.load_config(_CONFIG)
    paths = runtime.runtime_paths(config.domains["gb10"], "e-fourth", "4" * 40)
    assert config.sandbox_groups["e-fourth"].slurm_account == "lda-fourth"
    assert str(paths.candidate).startswith(environment["candidate_root"])


def test_first_committed_registry_generation_is_an_active_runtime_member(
    tmp_path: Path,
) -> None:
    registry = environment_registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = registry.register(
        {
            "schema_version": 1,
            "kind": environment_registry.REGISTER_KIND,
            "principal_id": "oidc:example:runtime-binding",
            "idempotency_key": "runtime-register-0001",
            "display_name": "Runtime binding",
        }
    )
    candidate = registry.import_candidate(
        {
            "schema_version": 1,
            "kind": environment_registry.CANDIDATE_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "runtime-candidate-0001",
            "env_id": environment.env_id,
            "candidate_sha": "4" * 40,
            "candidate_tree": "5" * 40,
            "bundle_sha256": "6" * 64,
            "bundle_size": 1024,
            "image_digests": {
                "amd64": "sha256:" + "7" * 64,
                "arm64": "sha256:" + "8" * 64,
            },
        }
    )
    deployment = registry.begin_deployment(
        {
            "schema_version": 1,
            "kind": environment_registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "runtime-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
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
    environment_row = next(
        item for item in snapshot["environments"] if item["env_id"] == environment.env_id
    )

    selected_candidate, selected_deployment = runtime._candidate_for_environment(
        snapshot,
        environment_row,
    )

    assert selected_candidate == next(
        item for item in snapshot["candidates"] if item["candidate_id"] == candidate.candidate_id
    )
    assert selected_deployment is not None
    assert (
        selected_deployment["applied_resource_generation"] == environment_row["resource_generation"]
    )


def test_peer_check_envelope_is_canonical_for_the_fixed_node_authority() -> None:
    identity = runtime.CandidateIdentity("a" * 40, "b" * 40)
    config = runtime.load_config(_CONFIG)
    raw = runtime._authority_envelope(
        config=config,
        action="inspect-local",
        node="oldlab-3",
        domain="oldlab",
        sandbox="qianyi",
        identity=identity,
    ).encode("ascii")
    policy = authority.AuthorityPolicy(
        source_sha="c" * 40,
        source_tree=identity.tree,
        node="oldlab-3",
        asset_sha256={str(path): "d" * 64 for path in authority.SOURCE_ASSETS},
    )

    parsed = authority._parse_request(raw, verb="check", policy=policy)

    assert parsed.action == "inspect-local"
    assert raw == authority._canonical(parsed.payload)


def test_remote_attestation_uses_bounded_authority_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    domain = config.domains["gb10"]
    sha = "a" * 40
    tree = "b" * 40
    manifest = b'{"proof":"manifest"}\n'
    signature = b"c2lnbmF0dXJl\n"
    manifest_path, signature_path = runtime._attestation_paths(
        "gb10",
        "qianyi",
        sha,
    )
    calls: list[dict[str, object]] = []

    def check(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {
            "operation": "export-domain-attestation",
            "domain": "gb10",
            "hostname": domain.publisher_hostname,
            "sandbox": "qianyi",
            "candidate_sha": sha,
            "candidate_tree": tree,
            "manifest_path": str(manifest_path),
            "signature_path": str(signature_path),
            "manifest_base64": base64.b64encode(manifest).decode("ascii"),
            "signature_base64": base64.b64encode(signature).decode("ascii"),
            "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
            "signature_sha256": hashlib.sha256(signature).hexdigest(),
        }

    monkeypatch.setattr(runtime, "_authority_check", check)

    result = runtime._remote_attestation(config, domain, "qianyi", sha, tree)

    assert result == (manifest, signature.strip(), str(manifest_path), str(signature_path))
    assert calls == [
        {
            "config": config,
            "action": "export-domain-attestation",
            "node": "trt-gb10-1",
            "domain": "gb10",
            "sandbox": "qianyi",
            "identity": runtime.CandidateIdentity(sha, tree),
        },
    ]


def test_runtime_proof_export_binds_closed_artifact_node_and_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    domain = config.domains["oldlab"]
    identity = runtime.CandidateIdentity("a" * 40, "b" * 40)
    artifact_id = f"runtime-proof/v1/qianyi/{identity.sha}/{identity.tree}/artifact/combined.json"
    combined: dict[str, object] = {
        "kind": "loom.developer-runtime-combined-activation",
        "sandbox": "qianyi",
        "candidate_sha": identity.sha,
        "candidate_tree": identity.tree,
        "collector": {"hostname": "trt-eai-oldlab-2"},
    }
    content = runtime._canonical_json(combined) + b"\n"
    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: "trt-eai-oldlab-2")
    monkeypatch.setattr(
        runtime,
        "_read_runtime_proof_artifact",
        lambda _path, *, mode: content if mode == 0o600 else b"",
    )

    report = runtime.export_runtime_proof_artifact(
        config,
        domain,
        "qianyi",
        identity,
        artifact_id,
    )

    assert report["artifact_id"] == artifact_id
    assert report["node"] == "oldlab-2"
    assert report["hostname"] == "trt-eai-oldlab-2"
    assert base64.b64decode(str(report["content_base64"]), validate=True) == content
    with pytest.raises(runtime.ConvergenceError, match="artifact binding"):
        runtime.export_runtime_proof_artifact(
            config,
            config.domains["gb10"],
            "qianyi",
            identity,
            artifact_id,
        )


def test_domain_runtime_has_no_raw_remote_ssh_cat_or_stat_path() -> None:
    source = _SCRIPT.read_text(encoding="utf-8")

    assert '"ssh"' not in source
    assert '"sudo"' not in source
    assert '"cat"' not in source
    assert "remote_metadata" not in source


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


def test_host_converge_creates_empty_candidate_namespace_before_materialize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    shared = tmp_path / "shared_work"
    shared.mkdir()
    domain = replace(
        config.domains["oldlab"],
        candidate_root=shared / "loom/candidates/sandboxes",
        publisher_hostname="publisher",
    )
    config = replace(config, domains={**config.domains, "oldlab": domain})
    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: "publisher")
    monkeypatch.setattr(
        runtime,
        "identity_plan",
        lambda *_args: {
            "groups": {sandbox: "ok" for sandbox in config.sandbox_groups},
        },
    )
    monkeypatch.setattr(runtime, "_service_identity_status", lambda *_args: "ok")
    monkeypatch.setattr(runtime, "_user_has_group", lambda *_args: True)
    monkeypatch.setattr(runtime, "_atomic_install", lambda *_args: None)
    monkeypatch.setattr(runtime, "_ensure_attestation_key", lambda *_args: "a" * 64)
    monkeypatch.setattr(os, "chown", lambda *_args: None)

    runtime.converge_host(_CONFIG, config, domain)

    for path in (
        shared / "loom",
        shared / "loom/candidates",
        shared / "loom/candidates/sandboxes",
    ):
        assert path.is_dir()
        assert stat.S_IMODE(path.stat().st_mode) == 0o2750


def test_publish_plan_is_exact_sha_and_secret_safe(tmp_path: Path) -> None:
    config = runtime.load_config(_CONFIG)
    repo, sha = _repository(tmp_path)
    identity = runtime._candidate_identity(repo, sha)
    group = config.sandbox_groups["qianyi"]
    config = replace(
        config,
        sandbox_groups={
            **config.sandbox_groups,
            "qianyi": replace(
                group,
                candidate_sha=sha,
                candidate_tree=identity.tree,
            ),
        },
    )
    seed = tmp_path / "worker.env"
    seed.write_text(
        _env_text("qianyi", sha, candidate_tree=identity.tree),
        encoding="utf-8",
    )
    seed.chmod(0o600)

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


def test_raw_candidate_verifier_rejects_skip_worktree_byte_drift(tmp_path: Path) -> None:
    repo, _sha = _repository(tmp_path)
    runtime._verify_candidate_raw_tree(repo)
    _git(repo, "update-index", "--skip-worktree", "README.md")
    (repo / "README.md").write_text("hidden drift\n", encoding="utf-8")

    with pytest.raises(runtime.ConvergenceError, match="hidden worktree flags"):
        runtime._verify_candidate_raw_tree(repo)


def test_materialize_is_independent_of_fleet_and_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    domain = config.domains["oldlab"]
    identity = runtime.CandidateIdentity("a" * 40, "b" * 40)
    candidate = tmp_path / "candidates" / "qianyi" / identity.sha
    reports = [
        {
            "hostname": peer.hostname,
            "candidate_inode": 41,
        }
        for peer in domain.peers
    ]
    monkeypatch.setattr(runtime, "_require_root", lambda: None)
    monkeypatch.setattr(runtime, "_hostname", lambda: domain.publisher_hostname)
    monkeypatch.setattr(runtime, "_transaction_lock", lambda *_args: _no_lock())
    monkeypatch.setattr(
        runtime,
        "_materialization_path",
        lambda *_args: tmp_path / "materialization.json",
    )
    monkeypatch.setattr(
        runtime,
        "runtime_paths",
        lambda *_args: SimpleNamespace(candidate=candidate, env=tmp_path / "never.env"),
    )
    monkeypatch.setattr(runtime, "_require_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_publish_candidate", lambda *_args: True)
    monkeypatch.setattr(runtime, "_peer_candidate_readback", lambda *_args: reports)
    monkeypatch.setattr(os, "chown", lambda *_args: None)
    monkeypatch.setattr(
        runtime,
        "_read_and_verify_fleet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fleet was read")),
    )

    report = runtime.converge_materialize(
        config,
        domain,
        "qianyi",
        tmp_path / "candidate.bundle",
        identity,
    )

    assert report["fleet_attestation"] == "not-read"
    assert report["runtime_env"] == "not-written"
    assert report["domain_attestation"] == "not-written"


@contextmanager
def _no_lock() -> object:
    yield


def test_attest_requires_fresh_fleet_before_any_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    domain = config.domains["oldlab"]
    identity = runtime.CandidateIdentity("a" * 40, "b" * 40)
    mutations: list[str] = []
    monkeypatch.setattr(runtime, "_recover_orphaned_transactions", lambda *_args: None)
    monkeypatch.setattr(
        runtime,
        "_read_and_verify_fleet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            runtime.ConvergenceError("remote-link fleet attestation is unavailable"),
        ),
    )
    monkeypatch.setattr(
        runtime,
        "attest_plan",
        lambda *_args: mutations.append("plan") or {},
    )

    with pytest.raises(runtime.ConvergenceError, match="fleet attestation is unavailable"):
        runtime._converge_attest_locked(
            config,
            domain,
            "qianyi",
            Path("/run/worker.env"),
            identity,
        )

    assert mutations == []


def test_transaction_json_replace_is_fsynced_to_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "state" / "phase.json"
    fsynced: list[Path] = []
    monkeypatch.setattr(os, "chown", lambda *_args: None)
    monkeypatch.setattr(runtime, "_fsync_directory", fsynced.append)

    runtime._write_json(target, {"status": "published"})

    assert json.loads(target.read_text(encoding="utf-8")) == {"status": "published"}
    assert fsynced == [target.parent]


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
        config = runtime.load_config(_CONFIG)
        runtime._parse_env_references(
            seed,
            config=config,
            domain=config.domains["oldlab"],
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
        config = runtime.load_config(_CONFIG)
        runtime._parse_env_references(
            seed,
            config=config,
            domain=config.domains["oldlab"],
            sandbox="qianyi",
            sha=sha,
        )


def test_env_references_reject_unknown_nonsecret_field(tmp_path: Path) -> None:
    sha = "a" * 40
    seed = tmp_path / "worker.env"
    seed.write_text(_env_text("qianyi", sha) + "UNDECLARED_FLAG=1\n", encoding="utf-8")

    with pytest.raises(runtime.ConvergenceError, match="exact closed schema"):
        config = runtime.load_config(_CONFIG)
        runtime._parse_env_references(
            seed,
            config=config,
            domain=config.domains["oldlab"],
            sandbox="qianyi",
            sha=sha,
        )


def test_env_references_reject_cross_domain_pool_binding(tmp_path: Path) -> None:
    sha = "a" * 40
    seed = tmp_path / "worker.env"
    seed.write_text(_env_text("qianyi", sha, domain="oldlab"), encoding="utf-8")
    config = runtime.load_config(_CONFIG)

    with pytest.raises(runtime.ConvergenceError, match="LOOM_WORKER_POOL_NAME"):
        runtime._parse_env_references(
            seed,
            config=config,
            domain=config.domains["gb10"],
            sandbox="qianyi",
            sha=sha,
        )


def test_stable_service_identity_requires_fixed_nonlogin_uid_gid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    group = config.sandbox_groups["devansh"]
    monkeypatch.setattr(
        runtime.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(
            pw_uid=31023,
            pw_gid=31023,
            pw_dir="/nonexistent",
            pw_shell="/usr/sbin/nologin",
        ),
    )
    monkeypatch.setattr(
        runtime.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=31023, gr_mem=[]),
    )

    assert runtime._service_identity_status(group) == "ok"


def test_stable_group_rejects_numeric_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    group = config.sandbox_groups["devansh"]
    monkeypatch.setattr(
        runtime.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(
            pw_uid=31023,
            pw_gid=31023,
            pw_dir="/nonexistent",
            pw_shell="/usr/sbin/nologin",
        ),
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
        runtime._service_identity_status(group)


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


def test_config_rejects_drifting_remote_link_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _registry_snapshot()
    snapshot["environments"][0]["ports"]["relay_gateway"] = snapshot["environments"][0]["ports"][
        "relay_control_plane"
    ]
    monkeypatch.setattr(runtime, "_load_registry_snapshot", lambda _path: snapshot)

    with pytest.raises(runtime.ConvergenceError, match="runtime identity is invalid"):
        runtime.load_config(_CONFIG)


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
        for node in runtime._INFRASTRUCTURE_LINK_NODES
    }
    payload: dict[str, object] = {
        "schema_version": 1,
        "sandbox": sandbox,
        "env_id": group.env_id,
        "resource_generation": group.resource_generation,
        "registry_generation": config.registry_generation,
        "registry_payload_sha256": config.registry_payload_sha256,
        "candidate_sha": sha,
        "candidate_tree": group.candidate_tree,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": (now + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "eligible_nodes": list(runtime._INFRASTRUCTURE_LINK_NODES),
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
                    "target_port": runtime._target_ports(config.sandbox_groups[sandbox])[name],
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


def test_fleet_attestation_requires_full_20_node_three_relay_contract() -> None:
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

    assert len(verified["nodes"]) == 20
    assert "trt-gb10-7" in verified["nodes"]
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
            "uid": group.uid,
            "gid": group.gid,
            "user": group.member,
            "mode": "0600",
            "candidate_sha": sha,
            "worker_pool_name": domain.worker_pool_name,
            "worker_max_concurrent": domain.worker_max_concurrent,
            "capacity_policy_source": domain.capacity_policy_source,
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
                "env_uid": group.uid,
                "env_gid": group.gid,
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
        lambda _config, domain, _sandbox, _sha, _tree: (
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
        runtime.collect_attestations(config, "qianyi", sha, "b" * 40, execute=False)


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
        lambda _config, domain, _sandbox, _sha, _tree: (
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

    report = runtime.collect_attestations(
        config,
        "qianyi",
        sha,
        "b" * 40,
        execute=False,
    )
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
        _tree: str,
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
        runtime.collect_attestations(config, "qianyi", sha, "b" * 40, execute=True)

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
    monkeypatch.setattr(
        runtime, "_write_json", lambda *_args, **_kwargs: mutations.append("receipt")
    )

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


def test_pending_reattest_rollback_restores_prior_attestation_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = runtime.load_config(_CONFIG)
    sha = "a" * 40
    old_manifest = b'{"old":true}\n'
    old_signature = b"b2xkLXNpZw==\n"
    current_env = tmp_path / "worker.env"
    current_env.write_bytes(b"new-env\n")
    manifest = tmp_path / "oldlab.json"
    signature = tmp_path / "oldlab.sig"
    manifest.write_bytes(b'{"partial":"new"}\n')
    signature.write_bytes(b"cGFydGlhbA==\n")
    receipt = {
        "schema_version": 1,
        "status": "mutating",
        "domain": "oldlab",
        "sandbox": "qianyi",
        "candidate_sha": sha,
        "candidate_tree": "b" * 40,
        "candidate_created": False,
        "env_previously_existed": False,
        "previous_env_sha256": None,
        "published_env_sha256": runtime.hashlib.sha256(b"new-env\n").hexdigest(),
        "attestation_previously_existed": True,
        "previous_attestation_payload_sha256": runtime.hashlib.sha256(
            old_manifest,
        ).hexdigest(),
        "previous_attestation_signature_sha256": runtime.hashlib.sha256(
            old_signature,
        ).hexdigest(),
        "attestation_pending": True,
    }
    monkeypatch.setattr(runtime, "_read_rollback_receipt", lambda *_args: receipt)
    monkeypatch.setattr(
        runtime,
        "runtime_paths",
        lambda *_args: SimpleNamespace(env=current_env, candidate=tmp_path / "candidate"),
    )
    monkeypatch.setattr(runtime, "_attestation_paths", lambda *_args: (manifest, signature))

    def snapshot(_path: Path, _digest: object, label: str) -> bytes:
        return old_signature if "signature" in label else old_manifest

    monkeypatch.setattr(runtime, "_secure_snapshot", snapshot)
    monkeypatch.setattr(
        runtime,
        "_atomic_bytes",
        lambda target, content, **_kwargs: target.write_bytes(content),
    )
    monkeypatch.setattr(runtime, "_write_json", lambda *_args, **_kwargs: None)

    result = runtime._rollback_locked(
        config,
        tmp_path / "receipt.json",
        allow_committed=False,
    )

    assert manifest.read_bytes() == old_manifest
    assert signature.read_bytes() == old_signature
    assert result["status"] == "rolled-back"
