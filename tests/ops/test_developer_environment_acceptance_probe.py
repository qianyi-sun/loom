from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_acceptance_probe as probe
from scripts.ops import developer_environment_registry as registry


@dataclass(frozen=True)
class PreparedProbe:
    deployment_id: str
    runtime_root: Path
    snapshot_path: Path
    request_path: Path


def _write_canonical(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(probe._canonical(payload))
    path.chmod(0o600)


def _signed(unsigned: dict[str, Any]) -> dict[str, Any]:
    return {**unsigned, "payload_sha256": probe._digest(unsigned)}


def _prepared_probe(tmp_path: Path) -> PreparedProbe:
    registry_root = tmp_path / "registry"
    authority = registry.DeveloperEnvironmentRegistry(
        registry_root / "registry.sqlite3",
    )
    principal_id = "oidc:example:probe-owner"
    environment = authority.register(
        {
            "schema_version": registry.SCHEMA_VERSION,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal_id,
            "idempotency_key": "acceptance-probe-registration",
            "display_name": "Probe Owner",
        }
    )
    candidate = authority.import_candidate(
        {
            "schema_version": registry.SCHEMA_VERSION,
            "kind": registry.CANDIDATE_KIND,
            "principal_id": principal_id,
            "idempotency_key": "acceptance-probe-candidate",
            "env_id": environment.env_id,
            "candidate_sha": "a" * 40,
            "candidate_tree": "b" * 40,
            "bundle_sha256": "c" * 64,
            "bundle_size": 1024,
            "image_digests": {
                "amd64": "sha256:" + "d" * 64,
                "arm64": "sha256:" + "e" * 64,
            },
            "image_archives": {
                "amd64": {
                    "sha256": "f" * 64,
                    "size": 2048,
                    "config_digest": "sha256:" + "d" * 64,
                    "index_digest": "sha256:" + "1" * 64,
                    "manifest_digest": "sha256:" + "2" * 64,
                    "manifest_media_type": "application/vnd.oci.image.manifest.v1+json",
                    "load_descriptor_digest": "sha256:" + "2" * 64,
                    "load_descriptor_media_type": ("application/vnd.oci.image.manifest.v1+json"),
                },
                "arm64": {
                    "sha256": "0" * 64,
                    "size": 4096,
                    "config_digest": "sha256:" + "e" * 64,
                    "index_digest": "sha256:" + "3" * 64,
                    "manifest_digest": "sha256:" + "4" * 64,
                    "manifest_media_type": "application/vnd.oci.image.manifest.v1+json",
                    "load_descriptor_digest": "sha256:" + "4" * 64,
                    "load_descriptor_media_type": ("application/vnd.oci.image.manifest.v1+json"),
                },
            },
        }
    )
    deployment = authority.begin_deployment(
        {
            "schema_version": registry.SCHEMA_VERSION,
            "kind": registry.DEPLOY_KIND,
            "principal_id": principal_id,
            "idempotency_key": "acceptance-probe-deployment",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    nodes: dict[str, dict[str, Any]] = {}
    domains: dict[str, dict[str, Any]] = {}
    for domain, architecture in registry.WORKER_RUNTIME_BINDING_DOMAINS.items():
        archive_binding = candidate.image_archives[architecture]
        backend = "containerd-snapshotter-v1" if domain == "oldlab" else "classic-overlay2"
        driver = registry.WORKER_RUNTIME_BACKENDS[backend]
        runtime_image_id = (
            archive_binding["load_descriptor_digest"]
            if domain == "oldlab"
            else archive_binding["config_digest"]
        )
        domain_binding = {
            "architecture": architecture,
            "docker_driver": driver,
            "docker_backend": backend,
            "config_digest": archive_binding["config_digest"],
            "load_descriptor_digest": archive_binding["load_descriptor_digest"],
            "load_descriptor_media_type": archive_binding["load_descriptor_media_type"],
            "runtime_image_id": runtime_image_id,
        }
        domains[domain] = domain_binding
        for node in registry.FLEET_NODES:
            if ("oldlab" if node.startswith("oldlab-") else "gb10") != domain:
                continue
            nodes[node] = {
                "domain": domain,
                **domain_binding,
                "docker_descriptor_digest": (
                    archive_binding["load_descriptor_digest"] if domain == "oldlab" else None
                ),
                "docker_descriptor_media_type": (
                    archive_binding["load_descriptor_media_type"] if domain == "oldlab" else None
                ),
                "receipt_sha256": hashlib.sha256(node.encode("ascii")).hexdigest(),
            }
    deployment = authority.record_worker_runtime_bindings(
        deployment.deployment_id,
        principal_id=principal_id,
        expected_resource_generation=environment.resource_generation,
        bindings={"nodes": nodes, "domains": domains},
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:-2],
        registry.DEPLOY_PHASES[1:-1],
        strict=True,
    ):
        deployment = authority.advance_deployment(
            deployment.deployment_id,
            principal_id=principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
    assert deployment.phase == "verified"
    deployment = authority.prepare_deployment_finalization(
        deployment.deployment_id,
        principal_id=principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    snapshot = authority.snapshot()
    assert deployment.applied_resource_generation == environment.resource_generation + 1
    runtime_root = tmp_path / "runtime"
    request = _signed(
        {
            "schema_version": 1,
            "kind": probe.REQUEST_KIND,
            "action": probe.ACTION,
            "deployment_id": deployment.deployment_id,
            "env_id": environment.env_id,
            "principal_id": principal_id,
            "runtime_id": environment.runtime_id,
            "candidate_id": candidate.candidate_id,
            "candidate_sha": candidate.candidate_sha,
            "candidate_tree": candidate.candidate_tree,
            "resource_generation": deployment.applied_resource_generation,
            "registry_generation": snapshot["generation"],
            "registry_snapshot_sha256": snapshot["payload_sha256"],
        }
    )
    request_path = runtime_root / "requests" / f"{deployment.deployment_id}-{probe.ACTION}.json"
    _write_canonical(request_path, request)
    return PreparedProbe(
        deployment_id=deployment.deployment_id,
        runtime_root=runtime_root,
        snapshot_path=registry_root / "current-snapshot.json",
        request_path=request_path,
    )


def _transport(
    calls: list[dict[str, Any]],
    *,
    job_user: str | None = None,
) -> Any:
    def run(
        args: tuple[str, ...],
        *,
        input: bytes,
        check: bool,
        capture_output: bool,
        timeout: int,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        assert check is False
        assert capture_output is True
        assert timeout == probe.PROBE_TIME_LIMIT_SECONDS + 120
        assert env["PATH"].startswith("/usr/local/sbin:")
        envelope = json.loads(input)
        assert set(envelope) == {
            "schema_version",
            "action",
            "node",
            "domain",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "payload_kind",
            "payload_sha256",
            "payload_base64",
            "prior_request_id",
            "request_id",
        }
        domain_request_raw = base64.b64decode(
            envelope["payload_base64"],
            validate=True,
        )
        assert envelope["payload_sha256"] == probe.hashlib.sha256(domain_request_raw).hexdigest()
        domain_request = json.loads(domain_request_raw)
        assert domain_request_raw == probe._canonical(domain_request)
        assert set(domain_request) == probe.DOMAIN_REQUEST_FIELDS
        assert domain_request["general_admission_authorized"] is False
        assert domain_request["foreign_job_action"] == "observe-only"
        assert domain_request["time_limit_seconds"] == 300
        calls.append({"args": args, "envelope": envelope, "request": domain_request})
        route = probe.ROUTES[domain_request["domain"]]
        completed_at = "2026-07-29T16:00:00Z"
        unsigned_result = {
            "schema_version": 1,
            "kind": probe.DOMAIN_RECEIPT_KIND,
            "status": "passed",
            "action": probe.ACTION,
            "domain": route.domain,
            "cluster": route.cluster,
            "submit_host": route.submit_host,
            "controller": route.controller,
            "deployment_id": domain_request["deployment_id"],
            "env_id": domain_request["env_id"],
            "principal_id": domain_request["principal_id"],
            "runtime_id": domain_request["runtime_id"],
            "candidate_id": domain_request["candidate_id"],
            "candidate_sha": domain_request["candidate_sha"],
            "candidate_tree": domain_request["candidate_tree"],
            "worker_image_id": domain_request["worker_image_id"],
            "applied_resource_generation": domain_request["applied_resource_generation"],
            "registry_generation": domain_request["registry_generation"],
            "registry_snapshot_sha256": domain_request["registry_snapshot_sha256"],
            "probe_request_sha256": domain_request["payload_sha256"],
            "transport_request_id": envelope["request_id"],
            "submission_count": 1,
            "job": {
                "job_id": "12345",
                "job_name": domain_request["job_name"],
                "user": job_user or domain_request["service_user"],
                "account": domain_request["slurm_account"],
                "qos": domain_request["slurm_qos"],
                "submit_host": route.submit_host,
                "controller": route.controller,
                "allocation_nodes": [route.allowed_nodes[0]],
                "time_limit_seconds": 300,
            },
            "health": {
                service: {
                    "service": service,
                    "status": "healthy",
                    "http_status": 200,
                    "candidate_binding_sha256": "1" * 64,
                    "response_sha256": "2" * 64,
                }
                for service in probe.SERVICE_NAMES
            },
            "terminal": {
                "state": "COMPLETED",
                "exit_code": "0:0",
                "natural_exit": True,
                "cancel_requested": False,
                "timed_out": False,
            },
            "job_output_sha256": "3" * 64,
            "authority_receipt_sha256": "4" * 64,
            "completed_at": completed_at,
        }
        result = _signed(unsigned_result)
        unsigned_response = {
            "schema_version": 1,
            "request_id": envelope["request_id"],
            "status": "succeeded",
            "action": probe.TRANSPORT_ACTION,
            "node": route.transport_node,
            "domain": route.domain,
            "sandbox": domain_request["runtime_id"],
            "candidate_sha": domain_request["candidate_sha"],
            "candidate_tree": domain_request["candidate_tree"],
            "payload_sha256": envelope["payload_sha256"],
            "result": result,
            "result_sha256": probe._digest(result),
            "completed_at": completed_at,
        }
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=probe._canonical(unsigned_response),
            stderr=b"",
        )

    return run


def _execute(prepared: PreparedProbe) -> dict[str, Any]:
    return probe.execute(
        prepared.deployment_id,
        runtime_root=prepared.runtime_root,
        registry_snapshot=prepared.snapshot_path,
        transport_program=Path("/fixed/node-transport"),
        require_root_ownership=False,
    )


def test_probe_persists_both_domains_and_replays_without_resubmission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_probe(tmp_path)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(probe.subprocess, "run", _transport(calls))

    first = _execute(prepared)
    second = _execute(prepared)
    assert first == second
    assert set(first["domains"]) == {"oldlab", "gb10"}
    assert first["worker_image_ids"] == {
        "oldlab": "sha256:" + "2" * 64,
        "gb10": "sha256:" + "e" * 64,
    }
    assert [call["request"]["domain"] for call in calls] == ["oldlab", "gb10"]
    assert "trt-gb10-7" in probe.ROUTES["gb10"].allowed_nodes

    combined = (
        prepared.runtime_root / "acceptance-probes" / prepared.deployment_id / "combined.json"
    )
    combined.unlink()
    assert _execute(prepared) == first
    assert len(calls) == 2


def test_probe_resumes_after_first_domain_without_resubmitting_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_probe(tmp_path)
    calls: list[dict[str, Any]] = []
    stable = _transport(calls)
    failed_gb10 = False

    def fail_second_domain_once(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_gb10
        completed = stable(*args, **kwargs)
        if calls[-1]["request"]["domain"] == "gb10" and not failed_gb10:
            failed_gb10 = True
            return subprocess.CompletedProcess(
                args=completed.args,
                returncode=1,
                stdout=b"",
                stderr=b"",
            )
        return completed

    monkeypatch.setattr(probe.subprocess, "run", fail_second_domain_once)
    with pytest.raises(probe.AcceptanceProbeError, match="failed safely"):
        _execute(prepared)
    assert (
        prepared.runtime_root / "acceptance-probes" / prepared.deployment_id / "oldlab.json"
    ).is_file()

    receipt = _execute(prepared)
    assert set(receipt["domains"]) == {"oldlab", "gb10"}
    assert [call["request"]["domain"] for call in calls] == [
        "oldlab",
        "gb10",
        "gb10",
    ]


def test_probe_rejects_false_registry_candidate_binding(tmp_path: Path) -> None:
    prepared = _prepared_probe(tmp_path)
    request = json.loads(prepared.request_path.read_bytes())
    request["candidate_sha"] = "f" * 40
    unsigned = {key: value for key, value in request.items() if key != "payload_sha256"}
    _write_canonical(prepared.request_path, _signed(unsigned))

    with pytest.raises(probe.AcceptanceProbeError, match="registry binding is stale"):
        _execute(prepared)


def test_probe_rejects_symlink_request(tmp_path: Path) -> None:
    prepared = _prepared_probe(tmp_path)
    target = prepared.request_path.with_suffix(".safe")
    prepared.request_path.rename(target)
    prepared.request_path.symlink_to(target)

    with pytest.raises(probe.AcceptanceProbeError, match="request is unavailable"):
        _execute(prepared)


def test_probe_rejects_tampered_independent_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_probe(tmp_path)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(probe.subprocess, "run", _transport(calls))
    _execute(prepared)
    receipt_dir = prepared.runtime_root / "acceptance-probes" / prepared.deployment_id
    (receipt_dir / "combined.json").unlink()
    oldlab_path = receipt_dir / "oldlab.json"
    oldlab = json.loads(oldlab_path.read_bytes())
    oldlab["job"]["user"] = "foreign-user"
    _write_canonical(oldlab_path, oldlab)

    with pytest.raises(probe.AcceptanceProbeError, match="receipt is invalid"):
        _execute(prepared)
    assert len(calls) == 2


def test_probe_request_cannot_inject_candidate_source_bundle(tmp_path: Path) -> None:
    prepared = _prepared_probe(tmp_path)
    request = json.loads(prepared.request_path.read_bytes())
    request["source_bundle"] = "/tmp/untrusted-candidate"
    unsigned = {key: value for key, value in request.items() if key != "payload_sha256"}
    _write_canonical(prepared.request_path, _signed(unsigned))

    with pytest.raises(probe.AcceptanceProbeError, match="request binding is invalid"):
        _execute(prepared)


def test_probe_rejects_one_domain_combined_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_probe(tmp_path)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(probe.subprocess, "run", _transport(calls))
    _execute(prepared)
    combined_path = (
        prepared.runtime_root / "acceptance-probes" / prepared.deployment_id / "combined.json"
    )
    combined = json.loads(combined_path.read_bytes())
    combined["domains"].pop("gb10")
    unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
    _write_canonical(combined_path, _signed(unsigned))

    with pytest.raises(
        probe.AcceptanceProbeError,
        match="combined acceptance probe receipt is invalid",
    ):
        _execute(prepared)


def test_probe_rejects_foreign_job_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_probe(tmp_path)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        probe.subprocess,
        "run",
        _transport(calls, job_user="foreign-user"),
    )

    with pytest.raises(probe.AcceptanceProbeError, match="receipt is invalid"):
        _execute(prepared)
    assert len(calls) == 1
