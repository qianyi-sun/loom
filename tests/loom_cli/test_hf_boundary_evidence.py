from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli import datasets_cmd, hf_boundary_evidence
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
        "task_provenance": {
            "trial_count": 2,
            "target_benchmark_trial_count": 2,
            "non_target_trial_count": 0,
            "task_set_trial_count": 0,
            "benchmark_ids": ["skilllearnbench"],
            "worker_ids": ["worker-current-1", "worker-current-2"],
        },
    }


def _worker_boundary() -> dict[str, object]:
    return {
        "summary": {
            "checked_hosts": 14,
            "checked_host_names": [f"trt-gb10-{number}" for number in range(1, 16) if number != 7],
            "ssh_failed_hosts": [],
            "docker_ps_failed_hosts": [],
            "hosts_without_containers": [],
            "env_file_missing_hosts": [],
            "env_file_hf_token_present_hosts": [],
            "hosts_with_container_hf_token_present": [],
            "containers_checked": 14,
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
    assert evidence["worker_boundary"]["canary_task_provenance"]["worker_ids"] == [
        "worker-current-1",
        "worker-current-2",
    ]


def test_compose_preserves_explicit_zero_valid_task_count() -> None:
    audit = _audit_report()
    audit["items"][0]["valid_task_config_count"] = 0
    audit["items"][0]["raw_task_count"] = 100

    evidence = compose_boundary_evidence(
        benchmark_id="skilllearnbench",
        environment="staging",
        audit_report=audit,
        source_summary=_source_summary(),
        canary_summary=_canary_summary(),
        worker_boundary=_worker_boundary(),
    )

    assert evidence["catalog"]["runnable_tasks"] == 0


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, True),
        ({"result_status": "failed"}, False),
        ({"state": "running"}, False),
        ({"task_filter": {"benchmark_id": "other"}}, False),
        ({"task_filter": {"benchmark_ids": ["skilllearnbench"]}}, True),
        (
            {"task_filter": {"benchmark_ids": ["other", "skilllearnbench"]}},
            False,
        ),
        (
            {
                "task_filter": {
                    "task_ids": [
                        "skilllearnbench/example/example-1",
                        "skilllearnbench/example/example-2",
                    ],
                },
            },
            True,
        ),
        (
            {
                "task_filter": {
                    "task_ids": [
                        "other/task-1",
                        "skilllearnbench/example/example-1",
                    ],
                },
            },
            False,
        ),
        (
            {
                "task_filter": {
                    "benchmark_id": "skilllearnbench",
                    "task_set_id": "other-set",
                },
            },
            False,
        ),
        (
            {
                "task_filter": {
                    "benchmark_id": "skilllearnbench",
                    "future_source_selector": "other",
                },
            },
            False,
        ),
        ({"required_worker_pools": ["oldlab"]}, False),
    ],
)
def test_canary_row_requires_success_benchmark_and_gb10_pool(
    overrides: dict[str, object],
    expected: bool,
) -> None:
    row: dict[str, object] = {
        "state": "finished",
        "result_status": "succeeded",
        "task_filter": {"benchmark_id": "skilllearnbench"},
        "required_worker_pools": ["gb10-arm64"],
    }
    row.update(overrides)

    assert (
        hf_boundary_evidence._is_successful_canary_row(
            row,
            benchmark_id="skilllearnbench",
            worker_pool="gb10-arm64",
        )
        is expected
    )


def test_canary_trial_summary_records_actual_task_provenance() -> None:
    summary = hf_boundary_evidence._summarize_canary_trials(
        [
            {
                "state": "succeeded",
                "pool_name": "gb10-arm64",
                "task_benchmark_id": "skilllearnbench",
                "task_set_id": None,
                "worker_id": "worker-current-1",
            },
            {
                "state": "succeeded",
                "pool_name": "gb10-arm64",
                "task_benchmark_id": "other",
                "task_set_id": None,
                "worker_id": "worker-current-2",
            },
            {
                "state": "succeeded",
                "pool_name": "gb10-arm64",
                "task_benchmark_id": None,
                "task_set_id": "other-set",
                "worker_id": None,
            },
        ],
        benchmark_id="skilllearnbench",
    )

    assert summary == {
        "worker_pools": {"active": {}, "terminal": {"gb10-arm64": 3}},
        "succeeded_trials": 3,
        "task_provenance": {
            "trial_count": 3,
            "target_benchmark_trial_count": 1,
            "non_target_trial_count": 2,
            "task_set_trial_count": 1,
            "benchmark_ids": ["other", "skilllearnbench"],
            "worker_ids": ["worker-current-1", "worker-current-2", None],
        },
    }


def test_collect_explicit_canary_batch_persists_trial_worker_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    class FakeResult:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def mappings(self) -> FakeResult:
            return self

        def all(self) -> list[dict[str, object]]:
            return self._rows

    class FakeConnection:
        def __init__(self) -> None:
            self.calls = 0

        async def __aenter__(self) -> FakeConnection:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def execute(self, *_args: object, **_kwargs: object) -> FakeResult:
            self.calls += 1
            if self.calls == 1:
                return FakeResult(
                    [
                        {
                            "id": batch_id,
                            "name": "explicit historical canary",
                            "task_filter": {"benchmark_id": "skilllearnbench"},
                            "state": "finished",
                            "result_status": "succeeded",
                            "created_at": "2026-01-01T00:00:00Z",
                            "finished_at": "2026-01-01T00:01:00Z",
                            "expected_trial_count": 2,
                            "required_worker_pools": ["gb10-arm64"],
                        },
                    ],
                )
            return FakeResult(
                [
                    {
                        "state": "succeeded",
                        "worker_id": "worker-old-1",
                        "pool_name": "gb10-arm64",
                        "task_benchmark_id": "skilllearnbench",
                        "task_set_id": None,
                    },
                    {
                        "state": "succeeded",
                        "worker_id": "worker-old-2",
                        "pool_name": "gb10-arm64",
                        "task_benchmark_id": "skilllearnbench",
                        "task_set_id": None,
                    },
                ],
            )

    class FakeEngine:
        def __init__(self) -> None:
            self.connection = FakeConnection()

        def connect(self) -> FakeConnection:
            return self.connection

        async def dispose(self) -> None:
            return None

    engine = FakeEngine()
    monkeypatch.setattr(hf_boundary_evidence, "create_async_engine", lambda _url: engine)

    summary = asyncio.run(
        hf_boundary_evidence.collect_canary_summary_from_db(
            db_url="postgresql://loom:test@localhost/loom",
            benchmark_id="skilllearnbench",
            worker_pool="gb10-arm64",
            canary_batch_id=batch_id,
        ),
    )

    assert summary["batch_id"] == batch_id
    assert summary["task_provenance"]["worker_ids"] == [
        "worker-old-1",
        "worker-old-2",
    ]


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
    status.write_text(
        json.dumps(
            {
                "desired_states": [
                    {
                        "environment": "staging",
                        "image_tag": "staging-abc123",
                        "source_git_commit": "a" * 40,
                    }
                ],
                "nodes": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

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
    assert written["candidate_binding"]["release_image_tag"] == "staging-abc123"
    assert written["candidate_binding"]["release_git_sha"] == "a" * 40


def test_worker_boundary_uses_host_repo_path_from_cluster_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ssh_config = tmp_path / "ssh_config"
    ssh_config.write_text("Host trt-gb10-1\n  HostName 127.0.0.1\n", encoding="utf-8")
    ssh_identity = tmp_path / "gb10_ed25519"
    ssh_identity.write_text("fake identity\n", encoding="utf-8")
    ssh_identity.chmod(0o600)
    ssh_certificate = tmp_path / "gb10_ed25519-cert.pub"
    ssh_certificate.write_text("fake cert\n", encoding="utf-8")
    cluster_config = tmp_path / "cluster.toml"
    cluster_config.write_text(
        f"""
[gb10_pool]
ssh_config = "{ssh_config.name}"
ssh_identity_file = "{ssh_identity.name}"
ssh_certificate_file = "{ssh_certificate.name}"
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
    assert evidence["summary"]["checked_host_names"] == ["trt-gb10-1"]
    assert evidence["summary"]["docker_ps_failed_hosts"] == []
    assert evidence["summary"]["hosts_without_containers"] == ["trt-gb10-1"]
    argv = calls[0]
    assert argv[-1] == "/srv/loom-prod-worker"
    assert argv[0:3] == ["ssh", "-F", str(ssh_config)]
    assert ["-i", str(ssh_identity)] == argv[3:5]
    assert ["-o", "IdentitiesOnly=yes"] == argv[5:7]
    assert ["-o", f"CertificateFile={ssh_certificate}"] == argv[7:9]


def test_worker_boundary_sends_multiline_probe_over_stdin(
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
  {{ ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging-worker" }},
]
""",
        encoding="utf-8",
    )
    calls: list[tuple[list[str], dict[str, object]]] = []

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

    def fake_run(argv: list[str], **kwargs: object) -> FakeProc:
        calls.append((list(argv), dict(kwargs)))
        return FakeProc()

    monkeypatch.setattr("loom_cli.hf_boundary_evidence.subprocess.run", fake_run)

    evidence = collect_worker_boundary_from_gb10(
        cluster_config_path=cluster_config,
        timeout_sec=1,
    )

    assert evidence["summary"]["checked_hosts"] == 1
    argv, kwargs = calls[0]
    assert "-c" not in argv
    assert argv[-4:] == ["trt-gb10-1", "python3", "-", "/srv/loom-staging-worker"]
    assert kwargs["input"] == hf_boundary_evidence._REMOTE_WORKER_ENV_SCRIPT
    assert '["docker", "ps", "-a"' not in str(kwargs["input"])
    assert '["docker", "ps", "--filter", "name=worker"' in str(kwargs["input"])
    assert "\n" not in " ".join(argv)


def test_worker_boundary_summary_tracks_exact_hosts_and_container_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ssh_config = tmp_path / "ssh_config"
    ssh_config.write_text("Host trt-gb10-*\n  HostName 127.0.0.1\n", encoding="utf-8")
    cluster_config = tmp_path / "cluster.toml"
    cluster_config.write_text(
        f"""
[gb10_pool]
ssh_config = "{ssh_config.name}"
hosts = [
  {{ ssh_target = "trt-gb10-3", repo_path = "/srv/loom-staging-worker" }},
  {{ ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging-worker" }},
  {{ ssh_target = "trt-gb10-2", repo_path = "/srv/loom-staging-worker" }},
]
""",
        encoding="utf-8",
    )

    class FakeProc:
        returncode = 0
        stderr = ""

        def __init__(self, host: str) -> None:
            containers = []
            docker_ps_ok = host != "trt-gb10-3"
            if host == "trt-gb10-1":
                containers = [
                    {
                        "container": "loom-worker-1",
                        "inspect_ok": True,
                        "hf_token_present": False,
                    }
                ]
            self.stdout = json.dumps(
                {
                    "env_file_exists": True,
                    "env_file_hf_token_present": False,
                    "env_file_key_count": 4,
                    "docker_ps_ok": docker_ps_ok,
                    "containers": containers,
                }
            )

    def fake_run(argv: list[str], **_kwargs: object) -> FakeProc:
        return FakeProc(argv[-4])

    monkeypatch.setattr("loom_cli.hf_boundary_evidence.subprocess.run", fake_run)

    evidence = collect_worker_boundary_from_gb10(
        cluster_config_path=cluster_config,
        timeout_sec=1,
    )

    assert evidence["summary"] == {
        "checked_hosts": 3,
        "checked_host_names": ["trt-gb10-1", "trt-gb10-2", "trt-gb10-3"],
        "ssh_failed_hosts": [],
        "docker_ps_failed_hosts": ["trt-gb10-3"],
        "hosts_without_containers": ["trt-gb10-2"],
        "env_file_missing_hosts": [],
        "env_file_hf_token_present_hosts": [],
        "containers_checked": 1,
        "hosts_with_container_hf_token_present": [],
        "inspect_failed": [],
    }


def test_hf_boundary_db_audit_branch_uses_readiness_audit_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_readiness_audit(**kwargs: object) -> list[str]:
        assert kwargs == {
            "db_url": "postgresql://loom/test",
            "benchmark": "skilllearnbench",
        }
        return ["readiness-item"]

    async def fake_run_bundle_presence_audit(**kwargs: object) -> SimpleNamespace:
        assert set(kwargs) == {"db_url", "benchmark", "object_store"}
        assert kwargs["db_url"] == "postgresql://loom/test"
        assert kwargs["benchmark"] == "skilllearnbench"
        return SimpleNamespace(
            s3_tasks=2,
            verified=2,
            missing=0,
            missing_sources=[],
        )

    def fake_render_readiness_json(items: list[str]) -> str:
        assert items == ["readiness-item"]
        return json.dumps({"count": 1, "items": [{"id": "skilllearnbench"}]})

    class FakeObjectStore:
        def __init__(self, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(
        "loom_cli.benchmark_readiness.run_readiness_audit",
        fake_run_readiness_audit,
    )
    monkeypatch.setattr(
        "loom_cli.benchmark_readiness.run_bundle_presence_audit",
        fake_run_bundle_presence_audit,
    )
    monkeypatch.setattr(
        "loom_cli.benchmark_readiness.render_readiness_json",
        fake_render_readiness_json,
    )
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)

    audit = hf_boundary_evidence._load_or_collect_audit(
        SimpleNamespace(
            audit_json=None,
            namespace=None,
            db_url="postgresql://loom/test",
            benchmark="skilllearnbench",
            minio_endpoint="http://minio:9000",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
        ),
        target=None,
    )

    assert audit["bundle_presence"] == {
        "s3_tasks": 2,
        "verified": 2,
        "missing": 0,
        "missing_sources": [],
    }


def test_hf_boundary_module_import_does_not_require_cluster_config() -> None:
    script = """
import builtins

original_import = builtins.__import__

def guarded_import(name, *args, **kwargs):
    if name == "loom_cli.cluster_config":
        raise RuntimeError("cluster config unavailable in service pod")
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import

import loom_cli.hf_boundary_evidence as evidence

assert evidence.collect_source_summary_from_db
"""

    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
