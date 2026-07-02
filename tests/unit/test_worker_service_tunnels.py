from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ops" / "worker_service_tunnels.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("worker_service_tunnels", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_default_tunnels_cover_remote_worker_private_dependencies() -> None:
    tunnels = {spec.name: spec for spec in _load_module().DEFAULT_TUNNELS}

    assert tunnels["control-plane"].local_port == 18081
    assert tunnels["control-plane"].service_name == "loom-control-plane"
    assert tunnels["control-plane"].service_port == 8080
    assert tunnels["control-plane"].health_path == "/healthz"

    assert tunnels["gateway"].local_port == 19100
    assert tunnels["gateway"].service_name == "loom-llm-gateway"
    assert tunnels["gateway"].service_port == 9100
    assert tunnels["gateway"].health_path == "/healthz"

    assert tunnels["minio"].local_port == 19000
    assert tunnels["minio"].service_name == "loom-minio"
    assert tunnels["minio"].service_port == 9000
    assert tunnels["minio"].health_path == "/minio/health/live"


def test_systemd_unit_restarts_kubectl_port_forward_for_service() -> None:
    module = _load_module()
    spec = next(t for t in module.DEFAULT_TUNNELS if t.name == "control-plane")

    rendered = module.render_systemd_unit(
        spec,
        namespace="loom-public-beta",
        kubectl="/tmp/loom-kubectl",
        kubeconfig="/tmp/loom-public-beta-kubeconfig",
        address="0.0.0.0",
    )

    assert "Description=Loom remote-worker tunnel: control-plane" in rendered
    assert "Restart=always" in rendered
    assert "RestartSec=5" in rendered
    assert "--kubeconfig /tmp/loom-public-beta-kubeconfig" in rendered
    assert "-n loom-public-beta" in rendered
    assert "port-forward --address 0.0.0.0 svc/loom-control-plane 18081:8080" in rendered
    assert "WantedBy=default.target" in rendered


def test_systemd_unit_rendering_can_override_gateway_local_port(tmp_path: Path) -> None:
    module = _load_module()

    module.write_systemd_units(
        tmp_path,
        namespace="loom-public-beta",
        kubectl="/usr/local/bin/kubectl",
        kubeconfig="/secure/public-beta.kubeconfig",
        address="0.0.0.0",
        local_port_overrides={"gateway": 30444},
    )

    gateway = (
        tmp_path / "loom-remote-worker-tunnel-gateway.service"
    ).read_text(encoding="utf-8")
    control_plane = (
        tmp_path / "loom-remote-worker-tunnel-control-plane.service"
    ).read_text(encoding="utf-8")

    assert "svc/loom-llm-gateway 30444:9100" in gateway
    assert "svc/loom-control-plane 18081:8080" in control_plane


def test_systemd_unit_rendering_can_add_subprocess_gateway_tunnel(
    tmp_path: Path,
) -> None:
    module = _load_module()

    module.write_systemd_units(
        tmp_path,
        namespace="loom-public-beta",
        kubectl="/usr/local/bin/kubectl",
        kubeconfig="/secure/public-beta.kubeconfig",
        address="0.0.0.0",
        subprocess_gateway_local_port=30444,
    )

    gateway = (
        tmp_path / "loom-remote-worker-tunnel-gateway.service"
    ).read_text(encoding="utf-8")
    subprocess_gateway = (
        tmp_path / "loom-remote-worker-tunnel-subprocess-gateway.service"
    ).read_text(encoding="utf-8")

    assert "svc/loom-llm-gateway 19100:9100" in gateway
    assert "svc/loom-llm-gateway 30444:9100" in subprocess_gateway
    assert "Description=Loom remote-worker tunnel: subprocess-gateway" in subprocess_gateway


def test_worker_health_urls_are_derived_from_remote_worker_env_file(tmp_path: Path) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
                "LOOM_WORKER_GATEWAY_URL=http://control-node:19100/openai/v1",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
                "LOOM_WORKER_TOKEN=loom_w_should_not_be_used_by_healthcheck",
                "",
            ]
        ),
        encoding="utf-8",
    )

    env = module.load_env_file(env_file)
    urls = module.worker_health_urls(env)

    assert urls == [
        ("control-plane", "http://control-node:18081/healthz"),
        ("gateway", "http://control-node:19100/healthz"),
        ("minio", "http://control-node:19000/minio/health/live"),
    ]


def test_worker_health_urls_include_optional_subprocess_facade_probe(tmp_path: Path) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
                "LOOM_WORKER_GATEWAY_URL=http://control-node:19100/openai/v1",
                "LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
                "",
            ]
        ),
        encoding="utf-8",
    )

    env = module.load_env_file(env_file)
    urls = module.worker_health_urls(env)

    assert urls == [
        ("control-plane", "http://control-node:18081/healthz"),
        ("gateway", "http://control-node:19100/healthz"),
        ("subprocess-gateway", "http://127.0.0.1:30444/healthz"),
        ("minio", "http://control-node:19000/minio/health/live"),
    ]


def test_remote_check_script_uses_exact_worker_urls_without_secrets(tmp_path: Path) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
                "LOOM_WORKER_GATEWAY_URL=http://control-node:19100",
                "LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
                "LOOM_WORKER_TOKEN=loom_w_should_not_print",
                "LOOM_WORKER_MINIO_SECRET_KEY=secret_should_not_print",
                "",
            ]
        ),
        encoding="utf-8",
    )

    snippet = module.render_remote_healthcheck_script(module.load_env_file(env_file))

    assert "curl -fsS --max-time" in snippet
    assert "http://control-node:18081/healthz" in snippet
    assert "http://control-node:19100/healthz" in snippet
    assert "http://127.0.0.1:30444/healthz" in snippet
    assert "http://control-node:19000/minio/health/live" in snippet
    assert "loom_w_should_not_print" not in snippet
    assert "secret_should_not_print" not in snippet


def test_print_check_script_command_outputs_srun_compatible_script(
    tmp_path: Path,
    capsys,
) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
                "LOOM_WORKER_GATEWAY_URL=http://control-node:19100",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
                "",
            ]
        ),
        encoding="utf-8",
    )

    rc = module.main(["print-check-script", "--env-file", str(env_file)])

    assert rc == 0
    out = capsys.readouterr().out
    assert out.startswith("#!/usr/bin/env bash")
    assert "probe control-plane http://control-node:18081/healthz" in out
    assert "probe gateway http://control-node:19100/healthz" in out
    assert "probe minio http://control-node:19000/minio/health/live" in out


def test_install_systemd_rejects_volatile_kubectl_paths_by_default() -> None:
    module = _load_module()

    with pytest.raises(ValueError, match="durable path"):
        module.validate_install_paths(
            kubectl="/tmp/loom-kubectl",
            kubeconfig="/secure/public-beta.kubeconfig",
            allow_volatile_paths=False,
        )

    with pytest.raises(ValueError, match="durable path"):
        module.validate_install_paths(
            kubectl="/usr/local/bin/kubectl",
            kubeconfig="/tmp/public-beta.kubeconfig",
            allow_volatile_paths=False,
        )

    module.validate_install_paths(
        kubectl="/usr/local/bin/kubectl",
        kubeconfig="/secure/public-beta.kubeconfig",
        allow_volatile_paths=False,
    )


def test_install_systemd_restarts_existing_units_after_enable(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    calls: list[list[str]] = []

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    rc = module._run_install_systemd(
        Namespace(
            output_dir=str(tmp_path),
            namespace="loom-public-beta",
            kubectl="/usr/local/bin/kubectl",
            kubeconfig="/secure/public-beta.kubeconfig",
            address="0.0.0.0",
            allow_volatile_paths=False,
        )
    )

    unit_names = [module.unit_name(spec) for spec in module.DEFAULT_TUNNELS]
    assert rc == 0
    assert calls == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", *unit_names],
        ["systemctl", "--user", "restart", *unit_names],
    ]


def test_watchdog_restarts_only_tunnel_after_repeated_probe_failures(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module()
    state_file = tmp_path / "watchdog.json"
    calls: list[list[str]] = []
    results = [
        module.ProbeResult(
            name="control-plane",
            url="http://control-node:18081/healthz",
            ok=False,
            detail="ConnectionResetError",
        ),
        module.ProbeResult(
            name="gateway",
            url="http://control-node:19100/healthz",
            ok=True,
            detail="http_status=200",
        ),
        module.ProbeResult(
            name="minio",
            url="http://control-node:19000/minio/health/live",
            ok=True,
            detail="http_status=200",
        ),
    ]

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    for _ in range(2):
        restarted = module.apply_watchdog_results(
            results,
            state_file=state_file,
            failure_threshold=3,
        )
        assert restarted == []
        assert calls == []

    restarted = module.apply_watchdog_results(
        results,
        state_file=state_file,
        failure_threshold=3,
    )

    assert restarted == ["loom-remote-worker-tunnel-control-plane.service"]
    assert calls == [
        [
            "systemctl",
            "--user",
            "restart",
            "loom-remote-worker-tunnel-control-plane.service",
        ],
    ]
    assert '"control-plane": 0' in state_file.read_text(encoding="utf-8")


def test_watchdog_restarts_gateway_tunnel_after_subprocess_facade_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module()
    calls: list[list[str]] = []
    results = [
        module.ProbeResult(
            name="subprocess-gateway",
            url="http://127.0.0.1:30444/healthz",
            ok=False,
            detail="ConnectionResetError",
        ),
    ]

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    restarted = module.apply_watchdog_results(
        results,
        state_file=tmp_path / "watchdog.json",
        failure_threshold=1,
    )

    assert restarted == ["loom-remote-worker-tunnel-gateway.service"]
    assert calls == [
        [
            "systemctl",
            "--user",
            "restart",
            "loom-remote-worker-tunnel-gateway.service",
        ],
    ]


def test_watchdog_restarts_dedicated_subprocess_gateway_tunnel_when_ports_differ(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join([
            "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
            "LOOM_WORKER_GATEWAY_URL=http://control-node:19100",
            "LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1",
            "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
            "",
        ]),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_probe(urls, *, timeout_sec):  # type: ignore[no-untyped-def]
        assert ("subprocess-gateway", "http://127.0.0.1:30444/healthz") in urls
        return [
            module.ProbeResult(
                name="subprocess-gateway",
                url="http://127.0.0.1:30444/healthz",
                ok=False,
                detail="ConnectionResetError",
            ),
        ]

    def fake_run(command: list[str], check: bool) -> None:
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module, "probe_health_urls", fake_probe)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    rc = module._run_watchdog(Namespace(
        env_file=str(env_file),
        state_file=str(tmp_path / "watchdog.json"),
        timeout_sec=5,
        failure_threshold=1,
    ))

    assert rc == 0
    assert calls == [
        [
            "systemctl",
            "--user",
            "restart",
            "loom-remote-worker-tunnel-subprocess-gateway.service",
        ],
    ]
    assert "restarted=loom-remote-worker-tunnel-subprocess-gateway.service" in (
        capsys.readouterr().out
    )


def test_watchdog_command_records_failures_without_failing_oneshot(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join([
            "LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:18081",
            "LOOM_WORKER_GATEWAY_URL=http://control-node:19100",
            "LOOM_WORKER_MINIO_ENDPOINT=http://control-node:19000",
            "",
        ]),
        encoding="utf-8",
    )

    def fake_probe(urls, *, timeout_sec):  # type: ignore[no-untyped-def]
        assert timeout_sec == 5
        assert urls[0] == ("control-plane", "http://control-node:18081/healthz")
        return [
            module.ProbeResult(
                name="control-plane",
                url="http://control-node:18081/healthz",
                ok=False,
                detail="ConnectionResetError",
            ),
        ]

    monkeypatch.setattr(module, "probe_health_urls", fake_probe)
    rc = module._run_watchdog(Namespace(
        env_file=str(env_file),
        state_file=str(tmp_path / "watchdog.json"),
        timeout_sec=5,
        failure_threshold=3,
    ))

    assert rc == 0
    assert "control-plane=failed" in capsys.readouterr().out


def test_watchdog_systemd_timer_runs_healthcheck_periodically() -> None:
    module = _load_module()

    service = module.render_watchdog_systemd_service(
        script_path="/opt/loom/scripts/ops/worker_service_tunnels.py",
        env_file="/secure/.env.remote-worker",
        state_file="/var/lib/loom/tunnel-watchdog.json",
        timeout_sec=5,
        failure_threshold=3,
    )
    timer = module.render_watchdog_systemd_timer(interval_sec=30)

    assert "Description=Loom remote-worker tunnel watchdog" in service
    assert "Type=oneshot" in service
    assert (
        "ExecStart=/opt/loom/scripts/ops/worker_service_tunnels.py watchdog "
        "--env-file /secure/.env.remote-worker "
        "--state-file /var/lib/loom/tunnel-watchdog.json "
        "--timeout-sec 5 --failure-threshold 3"
    ) in service
    assert "OnBootSec=30" in timer
    assert "OnUnitActiveSec=30" in timer
    assert "Unit=loom-remote-worker-tunnel-watchdog.service" in timer
    assert "WantedBy=timers.target" in timer


def test_watchdog_evidence_discovers_env_file_without_reading_secrets(
    tmp_path: Path,
) -> None:
    module = _load_module()
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join([
            "LOOM_WORKER_TOKEN=loom_w_should_not_print",
            "LOOM_WORKER_MINIO_SECRET_KEY=secret_should_not_print",
            "",
        ]),
        encoding="utf-8",
    )
    (unit_dir / "loom-remote-worker-tunnel-watchdog.service").write_text(
        "\n".join([
            "[Service]",
            "ExecStart=/srv/loom-current/scripts/ops/worker_service_tunnels.py "
            "watchdog "
            f"--env-file {env_file} "
            "--state-file /var/lib/loom/watchdog.json "
            "--timeout-sec 5 "
            "--failure-threshold 3",
            "",
        ]),
        encoding="utf-8",
    )
    (unit_dir / "loom-remote-worker-tunnel-watchdog.timer").write_text(
        module.render_watchdog_systemd_timer(interval_sec=30),
        encoding="utf-8",
    )

    evidence = module.collect_watchdog_evidence(
        unit_dir=unit_dir,
        expected_script_path="/srv/loom-current/scripts/ops/worker_service_tunnels.py",
        timer_active="active",
    )

    rendered = json.dumps(evidence)
    assert evidence["ok"] is True
    assert evidence["env_file"] == {
        "path": str(env_file),
        "exists": True,
    }
    assert evidence["service"]["script_path"] == (
        "/srv/loom-current/scripts/ops/worker_service_tunnels.py"
    )
    assert evidence["timer"]["active_state"] == "active"
    assert "loom_w_should_not_print" not in rendered
    assert "secret_should_not_print" not in rendered


def test_watchdog_evidence_reports_structured_path_drift(tmp_path: Path) -> None:
    module = _load_module()
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "loom-remote-worker-tunnel-watchdog.service").write_text(
        "\n".join([
            "[Service]",
            "ExecStart=/srv/loom-old/scripts/ops/worker_service_tunnels.py "
            "watchdog --env-file /secure/.env.remote-worker",
            "",
        ]),
        encoding="utf-8",
    )

    evidence = module.collect_watchdog_evidence(
        unit_dir=unit_dir,
        expected_script_path="/srv/loom-current/scripts/ops/worker_service_tunnels.py",
        timer_active="inactive",
    )

    assert evidence["ok"] is False
    assert {
        "code": "watchdog_script_path_drift",
        "message": "Watchdog unit ExecStart points at a different script path.",
        "expected_script_path": "/srv/loom-current/scripts/ops/worker_service_tunnels.py",
        "actual_script_path": "/srv/loom-old/scripts/ops/worker_service_tunnels.py",
    } in evidence["diagnostics"]
    assert {
        "code": "watchdog_timer_inactive",
        "message": "Watchdog timer is not active.",
        "active_state": "inactive",
    } in evidence["diagnostics"]


def test_watchdog_evidence_command_outputs_secret_free_json(
    tmp_path: Path,
    capsys,
) -> None:
    module = _load_module()
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "\n".join([
            "LOOM_WORKER_TOKEN=loom_w_should_not_print",
            "",
        ]),
        encoding="utf-8",
    )
    (unit_dir / "loom-remote-worker-tunnel-watchdog.service").write_text(
        "\n".join([
            "[Service]",
            "ExecStart=/srv/loom-current/scripts/ops/worker_service_tunnels.py "
            f"watchdog --env-file {env_file}",
            "",
        ]),
        encoding="utf-8",
    )
    (unit_dir / "loom-remote-worker-tunnel-watchdog.timer").write_text(
        module.render_watchdog_systemd_timer(interval_sec=30),
        encoding="utf-8",
    )

    rc = module.main([
        "watchdog-evidence",
        "--unit-dir",
        str(unit_dir),
        "--expected-script-path",
        "/srv/loom-current/scripts/ops/worker_service_tunnels.py",
        "--timer-active",
        "active",
    ])

    out = capsys.readouterr().out
    assert rc == 0
    body = json.loads(out)
    assert body["ok"] is True
    assert body["env_file"]["path"] == str(env_file)
    assert "loom_w_should_not_print" not in out


def test_tunnel_script_has_no_environment_specific_hosts() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    forbidden = (
        "OLD" + "LAB",
        "192" + ".168.",
        "10" + ".",
        "172" + ".16.",
        "platform" + "-dev",
    )
    assert not any(marker in text for marker in forbidden)
