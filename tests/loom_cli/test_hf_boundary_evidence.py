from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli import datasets_cmd
from loom_cli.hf_boundary_evidence import (
    HfBoundaryEvidenceError,
    collect_worker_boundary_from_gb10,
    compose_boundary_evidence,
    write_secret_safe_json,
)


def _audit_report() -> dict[str, object]:
    return {
        "items": [
            {
                "id": "skilllearnbench",
                "readiness_state": "runnable",
                "valid_task_config_count": 100,
            }
        ],
        "bundle_presence": {
            "s3_tasks": 100,
            "verified": 100,
            "missing": 0,
            "missing_sources": [],
        },
    }


def _source_summary() -> dict[str, object]:
    return {
        "benchmark": {
            "id": "skilllearnbench",
            "upstream_kind": "git",
            "upstream_locator": "https://github.com/cxcscmu/SkillLearnBench.git",
            "upstream_revision": "2d714f28b4f14bcaf93bccd5d11fbd3bd524fc46",
        },
        "source_counts": {
            "total_task_sources": 100,
            "internal_s3_sources": 100,
            "non_internal_sources": 0,
            "sample_s3_source": (
                "s3://loom-benchmarks/skilllearnbench/"
                "PRHW__loom-benchmark-skilllearnbench/rev123/task/checksum/"
            ),
        },
        "non_internal_sources": [],
        "sample_task": {
            "id": "skilllearnbench/example/example-1",
            "source": (
                "s3://loom-benchmarks/skilllearnbench/"
                "PRHW__loom-benchmark-skilllearnbench/rev123/task/checksum/"
            ),
            "config": {"environment": {"cpu_arch": "any"}},
            "tags": {
                "hf_repo_id": "PRHW/loom-benchmark-skilllearnbench",
                "hf_revision": "rev123",
                "hf_path": "task/",
                "hf_checksum": "checksum",
                "runtime_source_kind": "internal_object_store",
            },
        },
    }


def _canary_summary() -> dict[str, object]:
    return {
        "batch_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "canary_started": True,
        "terminal_state": "succeeded",
        "task_filter": {
            "task_ids": ["skilllearnbench/example/example-1"],
        },
        "worker_pools": {"terminal": {"gb10-arm64": 2}},
        "expected_trial_count": 2,
        "succeeded_trials": 2,
    }


def _worker_boundary() -> dict[str, object]:
    return {
        "summary": {
            "checked_hosts": 15,
            "ssh_failed_hosts": [],
            "env_file_missing_hosts": [],
            "env_file_hf_token_present_hosts": [],
            "hosts_with_container_hf_token_present": [],
            "inspect_failed": [],
        }
    }


def test_compose_uses_task_mirror_provenance_not_adapter_origin() -> None:
    evidence = compose_boundary_evidence(
        benchmark_id="skilllearnbench",
        environment="staging",
        audit_report=_audit_report(),
        source_summary=_source_summary(),
        canary_summary=_canary_summary(),
        worker_boundary=_worker_boundary(),
    )

    assert evidence["environment"] == "staging"
    assert evidence["benchmark_id"] == "skilllearnbench"
    assert evidence["catalog"]["runnable_tasks"] == 100
    assert evidence["catalog"]["requires_caps"]["cpu_arch"] == "any"
    assert evidence["runtime_sources"]["total_task_sources"] == 100
    assert evidence["runtime_sources"]["internal_s3_sources"] == 100
    assert evidence["runtime_sources"]["non_internal_sources"] == []
    assert evidence["hf_provenance"] == {
        "upstream_kind": "huggingface",
        "upstream_locator": "PRHW/loom-benchmark-skilllearnbench",
        "upstream_revision": "rev123",
        "sample_hf_path": "task/",
        "sample_hf_checksum": "checksum",
        "source": "task.tags",
        "benchmark_adapter_origin": _source_summary()["benchmark"],
    }
    assert evidence["worker_boundary"]["hf_token_present"] is False
    assert evidence["worker_boundary"]["direct_hf_egress_required"] is False
    assert evidence["worker_boundary"]["materialized_from_internal_source"] is True


def test_write_secret_safe_json_rejects_raw_hf_token(tmp_path: Path) -> None:
    evidence = compose_boundary_evidence(
        benchmark_id="skilllearnbench",
        environment="staging",
        audit_report=_audit_report(),
        source_summary=_source_summary(),
        canary_summary=_canary_summary(),
        worker_boundary=_worker_boundary(),
    )
    evidence["operator_note"] = "token hf_abcdefghijklmnopqrstuvwxyz123456"

    with pytest.raises(HfBoundaryEvidenceError, match="secret-looking"):
        write_secret_safe_json(evidence, tmp_path / "hf-boundary.json")


def test_write_secret_safe_json_writes_valid_artifact(tmp_path: Path) -> None:
    evidence = compose_boundary_evidence(
        benchmark_id="skilllearnbench",
        environment="staging",
        audit_report=_audit_report(),
        source_summary=_source_summary(),
        canary_summary=_canary_summary(),
        worker_boundary=_worker_boundary(),
    )
    output = tmp_path / "hf-boundary.json"

    write_secret_safe_json(evidence, output)

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["secret_scan"] == {"raw_secret_values_present": False}
    assert written["hf_provenance"]["upstream_kind"] == "huggingface"


def test_datasets_cli_generates_hf_boundary_evidence_from_json_inputs(
    tmp_path: Path,
) -> None:
    audit = tmp_path / "audit.json"
    source = tmp_path / "source.json"
    canary = tmp_path / "canary.json"
    worker = tmp_path / "worker.json"
    status = tmp_path / "gb10-status.json"
    output = tmp_path / "hf-boundary.json"
    audit.write_text(json.dumps(_audit_report()), encoding="utf-8")
    source.write_text(json.dumps(_source_summary()), encoding="utf-8")
    canary.write_text(json.dumps(_canary_summary()), encoding="utf-8")
    worker.write_text(json.dumps(_worker_boundary()), encoding="utf-8")
    status.write_text('{"nodes":[]}\n', encoding="utf-8")

    rc = datasets_cmd.dispatch(
        [
            "hf-boundary-evidence",
            "skilllearnbench",
            "--environment",
            "staging",
            "--audit-json",
            str(audit),
            "--source-summary-json",
            str(source),
            "--canary-summary-json",
            str(canary),
            "--worker-boundary-json",
            str(worker),
            "--gb10-workers-status",
            str(status),
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["runtime_sources"]["internal_s3_sources"] == 100
    assert written["evidence_inputs"]["gb10_workers_status"] == str(status)


def test_worker_boundary_uses_host_repo_path_from_cluster_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ssh_config = tmp_path / "ssh_config"
    ssh_config.write_text("Host trt-gb10-1\n  HostName 127.0.0.1\n", encoding="utf-8")
    cluster_config = tmp_path / "cluster.toml"
    cluster_config.write_text(
        f"""
[gb10_pool]
ssh_config = "{ssh_config.name}"
hosts = [
  {{ ssh_target = "trt-gb10-1", repo_path = "/srv/loom-prod-worker" }},
]
""",
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    class FakeProc:
        returncode = 0
        stdout = json.dumps(
            {
                "env_file_exists": True,
                "env_file_hf_token_present": False,
                "env_file_key_count": 4,
                "docker_ps_ok": True,
                "containers": [],
            }
        )
        stderr = ""

    def fake_run(argv: list[str], **_kwargs: object) -> FakeProc:
        calls.append(list(argv))
        return FakeProc()

    monkeypatch.setattr("loom_cli.hf_boundary_evidence.subprocess.run", fake_run)

    evidence = collect_worker_boundary_from_gb10(
        cluster_config_path=cluster_config,
        timeout_sec=1,
    )

    assert evidence["summary"]["checked_hosts"] == 1
    assert calls[0][-1] == "/srv/loom-prod-worker"
