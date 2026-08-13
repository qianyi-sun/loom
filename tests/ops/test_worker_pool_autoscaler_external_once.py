from __future__ import annotations

import asyncio
import base64
import contextlib
import importlib.util
import json
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ops" / "worker_pool_autoscaler_external_once.py"


@pytest.fixture
def module():
    name = "worker_pool_autoscaler_external_once_test"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)
    try:
        yield loaded
    finally:
        sys.modules.pop(name, None)


def _args(module: Any, *extra: str):
    return module._parser().parse_args(
        [
            "--environment",
            "staging",
            "--pool-name",
            "gb10",
            "--expected-slurm-cluster-name",
            "trt-gb10",
            "--expected-slurm-controller-host",
            "gx10-01c7",
            "--namespace",
            "loom-staging",
            "--kubeconfig",
            "/etc/loom/kubeconfig/staging.yaml",
            "--global-execution-witness-json",
            "/run/loom/global-execution-witness.json",
            "--manager-public-key",
            "/run/loom/global-execution-manager.pub",
            "--expected-manager-public-key-sha256",
            "a" * 64,
            *extra,
        ]
    )


def _authority(module: Any) -> Any:
    return module.SlurmAuthority(
        cluster_name="trt-gb10",
        controller_host="gx10-01c7",
        local_hostname="gx10-01c7",
    )


class _EnvironmentFilteringSession:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.executed_environment: str | None = None

    async def execute(self, statement: Any) -> Any:
        compiled = statement.compile()
        environment_values = [
            value for key, value in compiled.params.items() if key.startswith("environment_")
        ]
        assert len(environment_values) == 1
        environment = environment_values[0]
        assert isinstance(environment, str)
        self.executed_environment = environment
        selected = [row for row in self.rows if row.environment == environment]
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: selected))


def test_parser_requires_exact_environment_authority(module: Any) -> None:
    with pytest.raises(SystemExit):
        module._parser().parse_args(["--pool-name", "gb10"])

    with pytest.raises(
        module.ExternalAutoscalerConfigurationError,
        match="exact non-empty",
    ):
        module._scoped_environment(" staging ")


def test_parser_defaults_follow_service_home_and_concrete_database_service(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.setenv("HOME", "/var/lib/loom-staging-rollout")

    args = module._parser().parse_args(
        [
            "--environment",
            "staging",
            "--pool-name",
            "oldlab",
            "--expected-slurm-cluster-name",
            "trt-oldlab",
            "--expected-slurm-controller-host",
            "TRT-EAI-OLDLAB-1",
            "--global-execution-witness-json",
            "/run/loom/global-execution-witness.json",
            "--manager-public-key",
            "/run/loom/global-execution-manager.pub",
            "--expected-manager-public-key-sha256",
            "a" * 64,
        ]
    )

    assert args.kubeconfig == "/var/lib/loom-staging-rollout/.kube/config"
    assert args.db_service == "service/loom-postgres-rw"


def test_slurm_authority_probe_accepts_exact_local_cluster_and_controller(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _run(argv: list[str], **kwargs: Any) -> Any:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "Configuration data as of 2026-08-07T17:13:56\n"
                "ClusterName             = trt-gb10\n"
                "SlurmctldHost[0]        = gx10-01c7(192.168.20.11)\n"
            ),
        )

    monkeypatch.setattr(module.subprocess, "run", _run)
    monkeypatch.setattr(module.socket, "gethostname", lambda: "gx10-01c7")

    authority = module._validate_local_slurm_authority(_args(module))

    assert authority == module.SlurmAuthority(
        cluster_name="trt-gb10",
        controller_host="gx10-01c7",
        local_hostname="gx10-01c7",
    )
    assert captured["argv"] == ["/usr/bin/scontrol", "show", "config"]
    assert captured["kwargs"] == {
        "capture_output": True,
        "check": False,
        "text": True,
        "timeout": 10.0,
    }


@pytest.mark.parametrize(
    ("cluster_name", "controller_host", "local_hostname"),
    [
        ("foreign-cluster", "gx10-01c7", "gx10-01c7"),
        ("trt-gb10", "foreign-controller", "gx10-01c7"),
        ("trt-gb10", "gx10-01c7", "foreign-host"),
    ],
)
def test_slurm_authority_probe_rejects_foreign_or_non_controller_host(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    cluster_name: str,
    controller_host: str,
    local_hostname: str,
) -> None:
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=(
                f"ClusterName = {cluster_name}\nSlurmctldHost[0] = {controller_host}(192.0.2.10)\n"
            ),
        ),
    )
    monkeypatch.setattr(module.socket, "gethostname", lambda: local_hostname)

    with pytest.raises(module.SlurmAuthorityValidationError, match="authority"):
        module._validate_local_slurm_authority(_args(module))


@pytest.mark.parametrize(
    ("extra", "match"),
    [
        (("--db-local-host", "0.0.0.0"), "loopback"),
        (("--db-local-host", "localhost"), "loopback"),
        (("--db-local-port", "0"), "1..65535"),
        (("--db-remote-port", "65536"), "1..65535"),
        (("--db-port-forward-ready-timeout-sec", "0"), "finite positive"),
        (("--db-port-forward-ready-timeout-sec", "61"), "finite positive"),
        (("--db-port-forward-stop-timeout-sec", "nan"), "finite positive"),
        (("--namespace", "Unsafe_Name"), "DNS label"),
        (("--db-service", "deployment/loom-postgres"), "service/<Kubernetes DNS label>"),
    ],
)
def test_database_port_forward_config_rejects_unsafe_values(
    module: Any,
    extra: tuple[str, str],
    match: str,
) -> None:
    with pytest.raises(module.ExternalAutoscalerConfigurationError, match=match):
        module._database_port_forward_config(_args(module, *extra))


@pytest.mark.parametrize(
    "query",
    [
        "host=attacker.invalid",
        "HOST=attacker.invalid",
        "h%6fst=attacker.invalid",
        "hostaddr=203.0.113.8",
        "port=6543",
        "service=attacker",
        "service%66ile=%2Ftmp%2Fattacker.conf",
        "user=attacker",
        "PASSWORD=attacker-secret",
        "pass%77ord=attacker-secret",
        "database=attacker",
        "dbname=attacker",
        "application_name=not-authority-but-unsupported",
    ],
)
def test_database_url_preflight_rejects_all_query_options(
    module: Any,
    query: str,
) -> None:
    secret = "database-password-must-stay-redacted"
    config = module._database_port_forward_config(_args(module, "--db-local-port", "15451"))

    with pytest.raises(
        module.ExternalAutoscalerConfigurationError,
        match="query options are not permitted",
    ) as caught:
        module._preflight_database_url(
            f"postgresql+psycopg://loom:{secret}@loom-postgres:5432/loom?{query}",
            port_forward=config,
        )

    assert secret not in str(caught.value)
    assert "attacker" not in str(caught.value)


def test_database_url_preflight_binds_queryless_url_to_owned_tunnel(
    module: Any,
) -> None:
    config = module._database_port_forward_config(_args(module, "--db-local-port", "15451"))

    url = module._preflight_database_url(
        "postgresql+psycopg://loom:secret@loom-postgres:5432/loom",
        port_forward=config,
    )

    assert url.host == "127.0.0.1"
    assert url.port == 15451
    assert not url.query


def test_db_secret_lookup_is_bounded_and_does_not_put_dsn_in_command(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    dsn = "postgresql+psycopg://loom:secret@loom-postgres:5432/loom"

    def _check_output(argv: list[str], **kwargs: Any) -> str:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return base64.b64encode(dsn.encode()).decode()

    monkeypatch.setattr(module.subprocess, "check_output", _check_output)

    result = module._load_cp_db_url(_args(module), timeout_sec=7.5)

    assert result == dsn
    assert captured["argv"] == [
        "/usr/local/bin/kubectl",
        "--kubeconfig",
        "/etc/loom/kubeconfig/staging.yaml",
        "-n",
        "loom-staging",
        "get",
        "secret",
        "loom-secrets",
        "-o",
        "jsonpath={.data.cp-db-url}",
    ]
    assert captured["kwargs"] == {"text": True, "timeout": 7.5}
    assert dsn not in " ".join(captured["argv"])


def test_database_port_forward_owns_loopback_child_and_cleans_up(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    captured: dict[str, Any] = {}

    class _Process:
        returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            events.append("terminate")
            self.returncode = 0

        def kill(self) -> None:
            raise AssertionError("responsive child must not be killed")

        def wait(self, timeout: float | None = None) -> int:
            events.append(f"wait:{timeout}")
            assert self.returncode is not None
            return self.returncode

    process = _Process()

    def _popen(argv: list[str], **kwargs: Any) -> _Process:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        events.append("spawn")
        return process

    def _ready(child: Any, config: Any, output: Any) -> None:
        assert child is process
        assert config.local_host == "127.0.0.1"
        assert output.fileno() >= 0
        captured["output"] = output
        events.append("ready")

    monkeypatch.setattr(module, "_assert_local_port_available", lambda _config: None)
    monkeypatch.setattr(module, "_wait_for_database_port_forward", _ready)
    monkeypatch.setattr(module.subprocess, "Popen", _popen)

    config = module._database_port_forward_config(_args(module, "--db-local-port", "15451"))
    with module._database_port_forward(config):
        events.append("body")

    assert captured["argv"] == [
        "/usr/local/bin/kubectl",
        "--kubeconfig",
        "/etc/loom/kubeconfig/staging.yaml",
        "-n",
        "loom-staging",
        "port-forward",
        "--address",
        "127.0.0.1",
        "service/loom-postgres-rw",
        "15451:5432",
    ]
    assert captured["kwargs"]["stdin"] is subprocess.DEVNULL
    assert captured["kwargs"]["stdout"] is captured["output"]
    assert captured["kwargs"]["stderr"] is subprocess.STDOUT
    assert captured["kwargs"]["start_new_session"] is True
    assert captured["output"].closed is True
    assert events == ["spawn", "ready", "body", "terminate", "wait:5.0"]


def test_database_port_forward_readiness_probes_exact_loopback_endpoint(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, int], float]] = []

    class _Process:
        def poll(self) -> None:
            return None

    def _connect(address: tuple[str, int], *, timeout: float):
        calls.append((address, timeout))
        return contextlib.nullcontext()

    monkeypatch.setattr(module.socket, "create_connection", _connect)
    config = module._database_port_forward_config(_args(module, "--db-local-port", "15451"))

    with tempfile.TemporaryFile(mode="w+b") as output:
        output.write(b"Forwarding from 127.0.0.1:15451 -> 5432\n")
        output.flush()
        module._wait_for_database_port_forward(_Process(), config, output)

    assert calls
    assert calls[0][0] == ("127.0.0.1", 15451)
    assert 0 < calls[0][1] <= 0.2


def test_database_port_forward_early_exit_is_generic_and_never_probes_socket(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Process:
        def poll(self) -> int:
            return 19

    monkeypatch.setattr(
        module.socket,
        "create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an exited child must not reach the socket probe")
        ),
    )
    config = module._database_port_forward_config(_args(module))

    with pytest.raises(
        module.DatabasePortForwardError,
        match="exited before readiness",
    ) as caught:
        with tempfile.TemporaryFile(mode="w+b") as output:
            module._wait_for_database_port_forward(_Process(), config, output)

    assert "/etc/loom" not in str(caught.value)


def test_database_port_forward_never_probes_unowned_listener_after_bind_race(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Process:
        def poll(self) -> None:
            return None

    ticks = iter((0.0, 0.1, 100.0))
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        module.socket,
        "create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an unproven listener must never receive database credentials")
        ),
    )
    config = module._database_port_forward_config(_args(module))

    with tempfile.TemporaryFile(mode="w+b") as output:
        output.write(b"Unable to listen on port 15451: address already in use\n")
        output.flush()
        with pytest.raises(
            module.DatabasePortForwardError,
            match="readiness timed out",
        ):
            module._wait_for_database_port_forward(_Process(), config, output)


def test_database_port_forward_kills_child_that_ignores_terminate(module: Any) -> None:
    events: list[str] = []

    class _StubbornProcess:
        killed = False

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            events.append("terminate")

        def kill(self) -> None:
            events.append("kill")
            self.killed = True

        def wait(self, timeout: float | None = None) -> int:
            events.append(f"wait:{timeout}")
            if not self.killed:
                raise subprocess.TimeoutExpired("kubectl", timeout)
            return -9

    module._stop_database_port_forward(_StubbornProcess(), timeout_sec=0.25)

    assert events == ["terminate", "wait:0.25", "kill", "wait:0.25"]


def test_database_port_forward_kills_child_when_terminate_itself_fails(module: Any) -> None:
    events: list[str] = []

    class _TerminateFailureProcess:
        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            events.append("terminate")
            raise OSError("synthetic terminate failure")

        def kill(self) -> None:
            events.append("kill")

        def wait(self, timeout: float | None = None) -> int:
            events.append(f"wait:{timeout}")
            return -9

    module._stop_database_port_forward(_TerminateFailureProcess(), timeout_sec=0.25)

    assert events == ["terminate", "kill", "wait:0.25"]


def test_readiness_failure_always_reaps_started_child(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Process:
        returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            events.append("terminate")
            self.returncode = 0

        def kill(self) -> None:
            raise AssertionError("responsive child must not be killed")

        def wait(self, timeout: float | None = None) -> int:
            events.append(f"wait:{timeout}")
            return 0

    monkeypatch.setattr(module, "_assert_local_port_available", lambda _config: None)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: _Process())
    monkeypatch.setattr(
        module,
        "_wait_for_database_port_forward",
        lambda *_args: (_ for _ in ()).throw(
            module.DatabasePortForwardError("database port-forward readiness timed out")
        ),
    )

    with pytest.raises(module.DatabasePortForwardError, match="readiness timed out"):
        module._start_database_port_forward(module._database_port_forward_config(_args(module)))

    assert events == ["terminate", "wait:5.0"]


def test_normal_reconcile_validates_db_before_actuation_and_owns_tunnel(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[str] = []
    tunnel_active = False

    class _Scalars:
        def all(self) -> list[Any]:
            return [
                SimpleNamespace(
                    pool_name="gb10",
                    actuator_config={
                        "external_runner": True,
                        "slurm_cluster_name": "trt-gb10",
                        "slurm_controller_host": "gx10-01c7",
                    },
                )
            ]

    class _Result:
        def scalars(self) -> _Scalars:
            return _Scalars()

    class _Session:
        async def __aenter__(self) -> _Session:
            events.append("session-enter")
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("session-exit")

        async def execute(self, _statement: object) -> _Result:
            assert tunnel_active
            events.append("policy-query")
            return _Result()

        async def rollback(self) -> None:
            events.append("rollback")

        async def commit(self) -> None:
            events.append("commit")

    class _Engine:
        async def dispose(self) -> None:
            events.append("dispose")

    @contextlib.contextmanager
    def _tunnel(_config: Any):
        nonlocal tunnel_active
        events.append("tunnel-start")
        tunnel_active = True
        try:
            yield
        finally:
            tunnel_active = False
            events.append("tunnel-stop")

    async def _reconcile(_session: Any, **kwargs: object) -> list[Any]:
        assert tunnel_active
        assert kwargs == {
            "environment": "staging",
            "freshness_sec": 120,
            "include_external_policies": True,
            "external_only": True,
            "pool_names": ("gb10",),
            "global_execution_witness": None,
        }
        events.append("reconcile")
        return [SimpleNamespace(action="noop")]

    monkeypatch.setattr(
        module,
        "_load_cp_db_url",
        lambda _args, *, timeout_sec: "postgresql+psycopg://loom:secret@loom-postgres:5432/loom",
    )
    monkeypatch.setattr(module, "_database_port_forward", _tunnel)
    monkeypatch.setattr(
        module,
        "_validate_local_slurm_authority",
        lambda _args: events.append("slurm-authority") or _authority(module),
    )
    monkeypatch.setattr(module, "create_async_engine", lambda *_args, **_kwargs: _Engine())
    monkeypatch.setattr(
        module,
        "async_sessionmaker",
        lambda _engine, *, expire_on_commit: lambda: _Session(),
    )
    monkeypatch.setattr(module, "reconcile_worker_pool_autoscaler_once", _reconcile)
    monkeypatch.setattr(module, "load_global_execution_witness", lambda *_args, **_kwargs: None)

    asyncio.run(module._main_async(_args(module, "--db-local-port", "15451")))

    assert json.loads(capsys.readouterr().out) == [{"action": "noop"}]
    assert events == [
        "slurm-authority",
        "tunnel-start",
        "session-enter",
        "policy-query",
        "rollback",
        "reconcile",
        "commit",
        "session-exit",
        "dispose",
        "tunnel-stop",
    ]


def test_validate_only_uses_exact_tunnel_and_read_only_policy_query(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[str] = []
    tunnel_active = False
    secret = "db-password-must-not-appear"

    class _Scalars:
        def all(self) -> list[Any]:
            return [
                SimpleNamespace(
                    pool_name="gb10",
                    actuator_config={
                        "external_runner": True,
                        "slurm_cluster_name": "trt-gb10",
                        "slurm_controller_host": "gx10-01c7",
                    },
                )
            ]

    class _Result:
        def scalars(self) -> _Scalars:
            return _Scalars()

    class _Session:
        async def __aenter__(self) -> _Session:
            events.append("session-enter")
            return self

        async def __aexit__(self, *_args: object) -> None:
            events.append("session-exit")

        async def execute(self, _statement: object) -> _Result:
            assert tunnel_active
            events.append("policy-query")
            return _Result()

        async def rollback(self) -> None:
            events.append("rollback")

        async def commit(self) -> None:
            events.append("commit")
            raise AssertionError("validate-only must not commit")

    class _Engine:
        async def dispose(self) -> None:
            events.append("dispose")

    @contextlib.contextmanager
    def _tunnel(config: Any):
        nonlocal tunnel_active
        assert config.local_host == "127.0.0.1"
        assert config.local_port == 15451
        events.append("tunnel-start")
        tunnel_active = True
        try:
            yield
        finally:
            tunnel_active = False
            events.append("tunnel-stop")

    def _engine(url: Any, *, pool_pre_ping: bool) -> _Engine:
        assert tunnel_active
        assert pool_pre_ping is True
        assert url.host == "127.0.0.1"
        assert url.port == 15451
        events.append("engine")
        return _Engine()

    async def _unexpected_reconcile(*_args: object, **_kwargs: object) -> None:
        events.append("reconcile")
        raise AssertionError("validate-only must not reconcile")

    monkeypatch.setattr(
        module,
        "_load_cp_db_url",
        lambda _args, *, timeout_sec: f"postgresql+psycopg://loom:{secret}@loom-postgres:5432/loom",
    )
    monkeypatch.setattr(module, "_database_port_forward", _tunnel)
    monkeypatch.setattr(
        module,
        "_validate_local_slurm_authority",
        lambda _args: events.append("slurm-authority") or _authority(module),
    )
    monkeypatch.setattr(module, "create_async_engine", _engine)
    monkeypatch.setattr(
        module,
        "async_sessionmaker",
        lambda _engine, *, expire_on_commit: lambda: _Session(),
    )
    monkeypatch.setattr(module, "reconcile_worker_pool_autoscaler_once", _unexpected_reconcile)
    monkeypatch.setattr(module, "load_global_execution_witness", lambda *_args, **_kwargs: None)

    asyncio.run(module._main_async(_args(module, "--db-local-port", "15451", "--validate-only")))

    output = capsys.readouterr()
    payload = json.loads(output.out)
    assert payload == {
        "database_reachable": True,
        "mode": "validate-only",
        "slurm_authority": {
            "cluster_name": "trt-gb10",
            "controller_host": "gx10-01c7",
            "local_hostname": "gx10-01c7",
        },
        "pools": [
            {
                "enabled_external_policy_count": 1,
                "environment": "staging",
                "pool_name": "gb10",
            }
        ],
    }
    assert secret not in output.out + output.err
    assert events == [
        "slurm-authority",
        "tunnel-start",
        "engine",
        "session-enter",
        "policy-query",
        "rollback",
        "session-exit",
        "dispose",
        "tunnel-stop",
    ]


def test_validate_only_fails_closed_when_policy_is_missing(module: Any) -> None:
    class _Scalars:
        def all(self) -> list[Any]:
            return []

    class _Result:
        def scalars(self) -> _Scalars:
            return _Scalars()

    class _Session:
        async def execute(self, _statement: object) -> _Result:
            return _Result()

    with pytest.raises(module.ExternalPolicyValidationError, match="gb10"):
        asyncio.run(
            module._validate_requested_external_policies(
                _Session(),
                environment="staging",
                pool_names=("gb10",),
                authority=_authority(module),
            )
        )


def test_validate_only_foreign_same_pool_is_missing_without_authority_leak(module: Any) -> None:
    session = _EnvironmentFilteringSession(
        [
            SimpleNamespace(
                environment="production",
                pool_name="gb10",
                actuator_config={
                    "external_runner": True,
                    "slurm_cluster_name": "trt-gb10",
                    "slurm_controller_host": "gx10-01c7",
                },
            )
        ]
    )

    with pytest.raises(module.ExternalPolicyValidationError) as caught:
        asyncio.run(
            module._validate_requested_external_policies(
                session,
                environment="staging",
                pool_names=("gb10",),
                authority=_authority(module),
            )
        )

    assert session.executed_environment == "staging"
    assert "gb10" in str(caught.value)
    assert "production" not in str(caught.value)


def test_validate_only_mixed_same_pool_counts_only_exact_environment(module: Any) -> None:
    session = _EnvironmentFilteringSession(
        [
            SimpleNamespace(
                environment=environment,
                pool_name="gb10",
                actuator_config={
                    "external_runner": True,
                    "slurm_cluster_name": "trt-gb10",
                    "slurm_controller_host": "gx10-01c7",
                },
            )
            for environment in ("production", "staging")
        ]
    )

    validation = asyncio.run(
        module._validate_requested_external_policies(
            session,
            environment="staging",
            pool_names=("gb10",),
            authority=_authority(module),
        )
    )

    assert session.executed_environment == "staging"
    assert validation == [
        {
            "enabled_external_policy_count": 1,
            "environment": "staging",
            "pool_name": "gb10",
        }
    ]


def test_policy_validation_rejects_foreign_slurm_authority(module: Any) -> None:
    session = _EnvironmentFilteringSession(
        [
            SimpleNamespace(
                environment="staging",
                pool_name="gb10",
                actuator_config={
                    "external_runner": True,
                    "slurm_cluster_name": "foreign-cluster",
                    "slurm_controller_host": "gx10-01c7",
                },
            )
        ]
    )

    with pytest.raises(module.ExternalPolicyValidationError, match="authority"):
        asyncio.run(
            module._validate_requested_external_policies(
                session,
                environment="staging",
                pool_names=("gb10",),
                authority=_authority(module),
            )
        )


@pytest.mark.parametrize("validate_only", [False, True])
@pytest.mark.parametrize(
    "query",
    [
        "h%6fst=attacker.invalid",
        "USER=attacker",
        "pass%77ord=attacker-secret",
        "DATABASE=attacker",
        "dbname=attacker",
    ],
)
def test_routing_override_fails_before_tunnel_engine_session_or_reconcile(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    validate_only: bool,
    query: str,
) -> None:
    secret = "malicious-dsn-secret-must-not-appear"
    args = _args(module, *(["--validate-only"] if validate_only else []))
    events: list[str] = []

    def _unexpected(name: str) -> None:
        events.append(name)
        raise AssertionError(f"unsafe database URL reached {name}")

    class _Parser:
        def parse_args(self) -> Any:
            return args

    monkeypatch.setattr(module, "_parser", _Parser)
    monkeypatch.setattr(
        module,
        "_validate_local_slurm_authority",
        lambda _args: _authority(module),
    )
    monkeypatch.setattr(
        module,
        "_load_cp_db_url",
        lambda _args, *, timeout_sec: (
            f"postgresql+psycopg://loom:{secret}@loom-postgres:5432/loom?{query}"
        ),
    )
    monkeypatch.setattr(module, "_database_port_forward", lambda _config: _unexpected("tunnel"))
    monkeypatch.setattr(
        module,
        "create_async_engine",
        lambda *_args, **_kwargs: _unexpected("engine"),
    )
    monkeypatch.setattr(
        module,
        "async_sessionmaker",
        lambda *_args, **_kwargs: _unexpected("session"),
    )
    monkeypatch.setattr(
        module,
        "reconcile_worker_pool_autoscaler_once",
        lambda *_args, **_kwargs: _unexpected("reconcile"),
    )

    with pytest.raises(SystemExit) as caught:
        module.main()

    output = capsys.readouterr()
    assert caught.value.code == 1
    assert output.out == ""
    assert output.err == "error: database URL query options are not permitted\n"
    assert secret not in output.err
    assert "attacker.invalid" not in output.err
    assert events == []


def test_main_redacts_unexpected_database_errors(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "postgresql://loom:do-not-print@127.0.0.1:15451/loom"

    class _Parser:
        def parse_args(self) -> object:
            return object()

    async def _fail(_args: object) -> None:
        raise RuntimeError(secret)

    monkeypatch.setattr(module, "_parser", lambda: _Parser())
    monkeypatch.setattr(module, "_main_async", _fail)

    with pytest.raises(SystemExit) as caught:
        module.main()

    output = capsys.readouterr()
    assert caught.value.code == 1
    assert output.out == ""
    assert output.err == "error: external autoscaler reconcile failed safely\n"
    assert secret not in output.err


def test_local_port_preflight_rejects_an_existing_listener(module: Any) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        config = module._database_port_forward_config(_args(module, "--db-local-port", str(port)))

        with pytest.raises(module.DatabasePortForwardError, match="endpoint is unavailable"):
            module._assert_local_port_available(config)
