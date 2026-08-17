from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

import loom_cli.rollout.external_supervisor_readiness as readiness
from loom_cli.rollout.external_supervisor_readiness import (
    PROFILE_PATH,
    REHEARSAL_KUBECONFIG,
    SCRIPT_PATH,
    STAGING_KUBECONFIG,
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
    staging_python_path,
    staging_runtime_root,
    staging_script_path,
    staging_working_directory,
    verify_external_supervisor_artifact,
)

_SHA = "a" * 40
_TREE = "b" * 40
_TAG = "staging-aaaaaaa"
_RUNTIME_ROOT = staging_runtime_root(_SHA)
_WORKING_DIRECTORY = staging_working_directory(_SHA)
_PYTHON_PATH = staging_python_path(_SHA)
_SCRIPT_PATH = staging_script_path(_SHA)
_TASK_IMAGE_BUILDER_SCRIPT = "scripts/ops/task_image_builder_autoscaler_external_once.py"
_TASK_IMAGE_BUILDER_SCRIPT_PATH = f"{_WORKING_DIRECTORY}/{_TASK_IMAGE_BUILDER_SCRIPT}"


@pytest.fixture(autouse=True)
def _isolate_external_acceptance_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """These tests cover artifact mechanics, not activation authority."""

    monkeypatch.setattr(
        "loom_cli.environment_state.staging_gb10_external_activation_blockers",
        lambda **_kwargs: (),
    )


@dataclass(frozen=True)
class _Result:
    returncode: int


def test_supervisor_script_paths_follow_protected_unit_families() -> None:
    assert readiness.protected_external_supervisor_script_paths_for_units(
        {
            "loom-autoscaler-oldlab-staging.service",
            "loom-autoscaler-oldlab-staging.timer",
            "loom-task-image-builder-oldlab-staging.service",
            "loom-task-image-builder-oldlab-staging.timer",
        }
    ) == frozenset({SCRIPT_PATH, _TASK_IMAGE_BUILDER_SCRIPT})


def test_supervisor_script_paths_reject_units_outside_protected_families() -> None:
    with pytest.raises(ValueError, match="unit name is invalid"):
        readiness.protected_external_supervisor_script_paths_for_units(
            {"loom-unbounded-oldlab-staging.service"}
        )


def _args(
    *,
    port: int = 15451,
    environment: str = "staging",
    pool_name: str = "gb10",
) -> list[str]:
    cluster_name, controller_host = {
        "gb10": ("trt-gb10", "gx10-01c7"),
        "oldlab": ("trt-oldlab", "TRT-EAI-OLDLAB-1"),
    }[pool_name]
    args = [
        "--environment",
        environment,
        "--pool-name",
        pool_name,
        "--expected-slurm-cluster-name",
        cluster_name,
        "--expected-slurm-controller-host",
        controller_host,
        "--namespace",
        "loom-staging",
        "--kubeconfig",
        STAGING_KUBECONFIG,
        "--db-local-host",
        "127.0.0.1",
        "--db-local-port",
        str(port),
        "--db-service",
        "service/loom-postgres-rw",
        "--db-remote-port",
        "5432",
        "--db-port-forward-ready-timeout-sec",
        "10.0",
        "--db-port-forward-stop-timeout-sec",
        "5",
        "--db-connect-timeout-sec",
        "10",
        "--freshness-sec",
        "120",
        "--global-execution-witness-json",
        f"/etc/loom/credentials/global-execution/{pool_name}-witness.json",
        "--manager-public-key",
        "/etc/loom/credentials/global-execution/manager-ed25519.pub",
        "--expected-manager-public-key-sha256-file",
        "/etc/loom/credentials/global-execution/manager-ed25519.pub.sha256",
    ]
    if pool_name == "gb10":
        args.extend(["--db-secret-name", "loom-external-slurm-autoscaler-db"])
    return args


def _task_image_builder_args(*, port: int = 15453) -> list[str]:
    return [
        "--environment",
        "staging",
        "--pool-name",
        "task-image-builder-oldlab",
        "--profile",
        f"{_WORKING_DIRECTORY}/{PROFILE_PATH}",
        "--image-tag",
        _TAG,
        "--env-config-version",
        _TAG,
        "--git-sha",
        _SHA,
        "--expected-slurm-cluster-name",
        "trt-oldlab",
        "--expected-slurm-controller-host",
        "TRT-EAI-OLDLAB-1",
        "--namespace",
        "loom-staging",
        "--kubeconfig",
        STAGING_KUBECONFIG,
        "--db-local-host",
        "127.0.0.1",
        "--db-local-port",
        str(port),
        "--db-service",
        "service/loom-postgres-rw",
        "--db-remote-port",
        "5432",
        "--db-port-forward-ready-timeout-sec",
        "10",
        "--db-port-forward-stop-timeout-sec",
        "5",
        "--db-connect-timeout-sec",
        "10",
        "--freshness-sec",
        "120",
        "--global-execution-witness-json",
        "/etc/loom/credentials/global-execution/oldlab-witness.json",
        "--manager-public-key",
        "/etc/loom/credentials/global-execution/manager-ed25519.pub",
        "--expected-manager-public-key-sha256-file",
        "/etc/loom/credentials/global-execution/manager-ed25519.pub.sha256",
    ]


def _toml_string(value: str) -> str:
    return json.dumps(value)


def _supervisor(
    *,
    name: str = "gb10-staging",
    service_name: str = "loom-autoscaler-gb10-staging.service",
    timer_name: str = "loom-autoscaler-gb10-staging.timer",
    pool_name: str = "gb10",
    execution_host: str | None = None,
    port: int = 15451,
    enabled: bool = True,
    active: bool = True,
    args: list[str] | None = None,
    working_directory: str = _WORKING_DIRECTORY,
    python_path: str = _PYTHON_PATH,
    script_path: str = _SCRIPT_PATH,
) -> str:
    if execution_host is None:
        execution_host = {
            "gb10": "gx10-01c7",
            "oldlab": "TRT-EAI-OLDLAB-1",
        }[pool_name]
    rendered_args = ", ".join(
        _toml_string(item) for item in (args or _args(port=port, pool_name=pool_name))
    )
    return f"""
[[external_slurm_autoscaler_supervisors]]
name = {_toml_string(name)}
pool_name = {_toml_string(pool_name)}
execution_host = {_toml_string(execution_host)}
service_name = {_toml_string(service_name)}
timer_name = {_toml_string(timer_name)}
working_directory = {_toml_string(working_directory)}
python_path = {_toml_string(python_path)}
script_path = {_toml_string(script_path)}
args = [{rendered_args}]
enabled = {str(enabled).lower()}
active = {str(active).lower()}
""".strip()


def _candidate(
    tmp_path: Path,
    *,
    supervisors: list[str] | None = None,
    control_plane_environment: str | None = None,
    protected_pools: tuple[str, ...] = (),
) -> Path:
    root = tmp_path / "candidate"
    profile = root / PROFILE_PATH
    script = root / SCRIPT_PATH
    profile.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    profile_header = 'environment = "staging"\n'
    if control_plane_environment is not None:
        profile_header += f'control_plane_environment = "{control_plane_environment}"\n'
    if protected_pools:
        profile_header += (
            "\n[external_slurm_runner_prerequisites]\n"
            f"pools = {json.dumps(list(protected_pools))}\n"
        )
    profile.write_text(
        profile_header + "\n" + "\n\n".join(supervisors or [_supervisor()]) + "\n",
        encoding="utf-8",
    )
    script.write_text("#!/usr/bin/env python3\nprint('exact candidate')\n", encoding="utf-8")
    profile.chmod(0o644)
    script.chmod(0o755)
    return root


def _build(root: Path) -> ExternalSupervisorArtifact:
    return build_external_supervisor_artifact(
        root,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        image_tag=_TAG,
        environment="staging",
    )


def test_artifact_can_bind_one_physical_controller(tmp_path: Path) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[
            _supervisor(),
            _supervisor(
                name="oldlab-staging",
                service_name="loom-autoscaler-oldlab-staging.service",
                timer_name="loom-autoscaler-oldlab-staging.timer",
                pool_name="oldlab",
                port=15448,
            ),
        ],
    )

    artifact = build_external_supervisor_artifact(
        root,
        candidate_sha=_SHA,
        candidate_tree=_TREE,
        image_tag=_TAG,
        environment="staging",
        execution_host="TRT-EAI-OLDLAB-1",
    )

    assert [item.pool_name for item in artifact.supervisors] == ["oldlab"]
    assert ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) == artifact


def _canonical_bytes(raw: dict[str, Any]) -> bytes:
    return (
        json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"
    )


def test_artifact_binds_sources_exact_units_and_round_trips(tmp_path: Path) -> None:
    root = _candidate(tmp_path)

    artifact = _build(root)

    assert artifact.candidate_sha == _SHA
    assert artifact.candidate_tree == _TREE
    assert artifact.environment == "staging"
    assert artifact.image_tag == _TAG
    assert artifact.runtime_root == _RUNTIME_ROOT
    assert artifact.profile_sha256 == hashlib.sha256((root / PROFILE_PATH).read_bytes()).hexdigest()
    assert artifact.script_sha256 == {
        SCRIPT_PATH: hashlib.sha256((root / SCRIPT_PATH).read_bytes()).hexdigest()
    }
    assert len(artifact.supervisors) == 1
    supervisor = artifact.supervisors[0]
    assert supervisor.execution_host == "gx10-01c7"
    assert supervisor.control_plane_environment == "staging"
    assert supervisor.db_local_host == "127.0.0.1"
    assert supervisor.db_local_port == 15451
    assert supervisor.db_port_forward_ready_timeout_sec == "10"
    assert supervisor.service_unit.endswith("\n")
    assert not supervisor.service_unit.endswith("\n\n")
    assert supervisor.timer_unit.endswith("\n")
    assert artifact.unit_sha256 == {
        supervisor.service_name: hashlib.sha256(supervisor.service_unit.encode()).hexdigest(),
        supervisor.timer_name: hashlib.sha256(supervisor.timer_unit.encode()).hexdigest(),
    }
    assert ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) == artifact


def test_artifact_binds_each_supervisor_kind_to_its_exact_executable(
    tmp_path: Path,
) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[
            _supervisor(
                name="oldlab-staging",
                service_name="loom-autoscaler-oldlab-staging.service",
                timer_name="loom-autoscaler-oldlab-staging.timer",
                pool_name="oldlab",
                port=15448,
            ),
            _supervisor(
                name="task-image-builder-oldlab-staging",
                service_name="loom-task-image-builder-oldlab-staging.service",
                timer_name="loom-task-image-builder-oldlab-staging.timer",
                pool_name="task-image-builder-oldlab",
                execution_host="TRT-EAI-OLDLAB-1",
                port=15453,
                args=_task_image_builder_args(),
                script_path=_TASK_IMAGE_BUILDER_SCRIPT_PATH,
            ),
        ],
    )
    builder_script = root / _TASK_IMAGE_BUILDER_SCRIPT
    builder_script.write_text(
        "#!/usr/bin/env python3\nprint('exact task-image builder candidate')\n",
        encoding="utf-8",
    )
    builder_script.chmod(0o755)

    artifact = _build(root)

    assert [item.pool_name for item in artifact.supervisors] == [
        "oldlab",
        "task-image-builder-oldlab",
    ]
    assert artifact.script_sha256 == {
        SCRIPT_PATH: hashlib.sha256((root / SCRIPT_PATH).read_bytes()).hexdigest(),
        _TASK_IMAGE_BUILDER_SCRIPT: hashlib.sha256(builder_script.read_bytes()).hexdigest(),
    }
    commands = artifact.validation_argv(
        "loom-rehearsal-abc123",
        REHEARSAL_KUBECONFIG,
    )
    assert commands["task-image-builder-oldlab-staging"][:2] == (
        _PYTHON_PATH,
        _TASK_IMAGE_BUILDER_SCRIPT_PATH,
    )
    assert ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) == artifact


def test_builder_rejects_supervisor_on_foreign_execution_host(tmp_path: Path) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[_supervisor(execution_host="TRT-EAI-OLDLAB-1")],
    )

    with pytest.raises(ValueError, match="execution host"):
        _build(root)


def test_artifact_binds_control_plane_environment_alias_in_exact_unit_args(
    tmp_path: Path,
) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[_supervisor(args=_args(environment="production"))],
        control_plane_environment="production",
    )

    artifact = _build(root)

    assert artifact.environment == "staging"
    supervisor = artifact.supervisors[0]
    assert supervisor.environment == "staging"
    assert supervisor.control_plane_environment == "production"
    assert supervisor.args[:2] == ("--environment", "production")
    assert "--environment production" in supervisor.service_unit
    assert ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) == artifact


def test_artifact_omits_only_fully_disabled_supervisors(tmp_path: Path) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[
            _supervisor(),
            _supervisor(
                name="disabled",
                service_name="loom-autoscaler-disabled.service",
                timer_name="loom-autoscaler-disabled.timer",
                pool_name="oldlab",
                port=15452,
                enabled=False,
                active=False,
            ),
        ],
    )

    artifact = _build(root)

    assert [item.name for item in artifact.supervisors] == ["gb10-staging"]


def test_artifact_retains_disabled_protected_pool_without_validation_command(
    tmp_path: Path,
) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[_supervisor(enabled=False, active=False)],
        protected_pools=("gb10",),
    )

    artifact = _build(root)

    assert len(artifact.supervisors) == 1
    supervisor = artifact.supervisors[0]
    assert not supervisor.enabled
    assert not supervisor.active
    assert "# LoomDesiredState=disabled" in supervisor.timer_unit
    assert artifact.validation_argv("loom-rehearsal-abc123", REHEARSAL_KUBECONFIG) == {}
    assert ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) == artifact


@pytest.mark.parametrize(("enabled", "active"), [(True, False), (False, True)])
def test_builder_rejects_split_enablement_state(
    tmp_path: Path,
    enabled: bool,
    active: bool,
) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[_supervisor(enabled=enabled, active=active)],
    )

    with pytest.raises(ValueError, match="must converge together"):
        _build(root)


def test_validation_argv_rewrites_isolated_authority_and_port_and_appends_mode(
    tmp_path: Path,
) -> None:
    artifact = _build(
        _candidate(
            tmp_path,
            supervisors=[
                _supervisor(
                    name="oldlab-staging",
                    service_name="loom-autoscaler-oldlab-staging.service",
                    timer_name="loom-autoscaler-oldlab-staging.timer",
                    pool_name="oldlab",
                    port=15448,
                )
            ],
        )
    )
    live = artifact.supervisors[0]

    commands = artifact.validation_argv(
        "loom-rehearsal-abc123",
        REHEARSAL_KUBECONFIG,
    )
    command = commands[live.name]

    assert command[:2] == (live.python_path, live.script_path)
    assert command[-1] == "--validate-only"
    live_arguments = list(live.args)
    expected = list(live.args)
    expected[expected.index("--namespace") + 1] = "loom-rehearsal-abc123"
    expected[expected.index("--kubeconfig") + 1] = REHEARSAL_KUBECONFIG
    expected[expected.index("--db-local-port") + 1] = "25448"
    assert list(command[2:-1]) == expected
    for flag in (
        "--pool-name",
        "--db-local-host",
        "--db-service",
        "--db-remote-port",
        "--db-port-forward-ready-timeout-sec",
        "--db-port-forward-stop-timeout-sec",
        "--db-connect-timeout-sec",
        "--freshness-sec",
    ):
        assert command[command.index(flag) + 1] == live_arguments[live_arguments.index(flag) + 1]

    with pytest.raises(ValueError, match="not isolated"):
        artifact.validation_argv("loom-staging", REHEARSAL_KUBECONFIG)
    with pytest.raises(ValueError, match="not canonical"):
        artifact.validation_argv("loom-rehearsal-abc123", STAGING_KUBECONFIG)


def test_validation_ports_are_unique_and_disjoint_from_live_ports(tmp_path: Path) -> None:
    artifact = _build(
        _candidate(
            tmp_path,
            supervisors=[
                _supervisor(),
                _supervisor(
                    name="oldlab-staging",
                    service_name="loom-autoscaler-oldlab-staging.service",
                    timer_name="loom-autoscaler-oldlab-staging.timer",
                    pool_name="oldlab",
                    port=15448,
                ),
            ],
        )
    )

    commands = artifact.validation_argv(
        "loom-rehearsal-abc123",
        REHEARSAL_KUBECONFIG,
    )
    live_ports = {supervisor.db_local_port for supervisor in artifact.supervisors}
    validation_ports = {
        int(command[command.index("--db-local-port") + 1]) for command in commands.values()
    }

    assert live_ports == {15448, 15451}
    assert validation_ports == {25448, 25451}
    assert live_ports.isdisjoint(validation_ports)


def test_rehearsal_validation_routes_remote_policy_without_local_slurm_probe(
    tmp_path: Path,
) -> None:
    artifact = _build(
        _candidate(
            tmp_path,
            supervisors=[
                _supervisor(),
                _supervisor(
                    name="oldlab-staging",
                    service_name="loom-autoscaler-oldlab-staging.service",
                    timer_name="loom-autoscaler-oldlab-staging.timer",
                    pool_name="oldlab",
                    port=15448,
                ),
            ],
        )
    )

    commands = artifact.validation_argv(
        "loom-rehearsal-abc123",
        REHEARSAL_KUBECONFIG,
    )
    gb10 = commands["gb10-staging"]
    oldlab = commands["oldlab-staging"]

    assert gb10[:3] == (
        _PYTHON_PATH,
        "-m",
        "loom_cli.rollout.rehearsal_external_supervisor_policy_probe",
    )
    assert gb10[-1] == "--validate-only"
    assert gb10[gb10.index("--pool-name") + 1] == "gb10"
    assert gb10[gb10.index("--expected-slurm-cluster-name") + 1] == "trt-gb10"
    assert gb10[gb10.index("--expected-slurm-controller-host") + 1] == "gx10-01c7"
    assert gb10[gb10.index("--db-secret-name") + 1] == "loom-secrets"
    assert "loom-external-slurm-autoscaler-db" not in gb10
    assert _SCRIPT_PATH not in gb10

    assert oldlab[:2] == (_PYTHON_PATH, _SCRIPT_PATH)
    assert oldlab[-1] == "--validate-only"


def test_artifact_selects_one_controller_without_losing_candidate_binding(
    tmp_path: Path,
) -> None:
    artifact = _build(
        _candidate(
            tmp_path,
            supervisors=[
                _supervisor(),
                _supervisor(
                    name="oldlab-staging",
                    service_name="loom-autoscaler-oldlab-staging.service",
                    timer_name="loom-autoscaler-oldlab-staging.timer",
                    pool_name="oldlab",
                    port=15448,
                ),
            ],
        )
    )

    selected = artifact.for_execution_host("gx10-01c7")

    assert selected.candidate_sha == artifact.candidate_sha
    assert selected.candidate_tree == artifact.candidate_tree
    assert selected.profile_sha256 == artifact.profile_sha256
    assert selected.script_sha256 == artifact.script_sha256
    assert [item.name for item in selected.supervisors] == ["gb10-staging"]
    assert selected.artifact_digest != artifact.artifact_digest
    assert ExternalSupervisorArtifact.from_bytes(selected.to_bytes()) == selected

    with pytest.raises(ValueError, match="execution host"):
        artifact.for_execution_host("foreign-controller")


def test_artifact_rejects_live_port_without_rehearsal_offset_capacity(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="rehearsal DB local port is out of bounds"):
        _build(_candidate(tmp_path, supervisors=[_supervisor(port=60000)]))


def test_verify_uses_only_temporary_exact_units_and_fails_closed(tmp_path: Path) -> None:
    artifact = _build(_candidate(tmp_path))
    observed_paths: list[Path] = []

    def _pass(command: Any) -> _Result:
        assert tuple(command[:2]) == ("systemd-analyze", "verify")
        expected = {item.service_name: item.service_unit.encode() for item in artifact.supervisors}
        expected.update(
            {item.timer_name: item.timer_unit.encode() for item in artifact.supervisors}
        )
        for raw_path in command[2:]:
            path = Path(raw_path)
            observed_paths.append(path)
            assert path.read_bytes() == expected[path.name]
            assert path.stat().st_mode & 0o777 == 0o600
        return _Result(0)

    verified = verify_external_supervisor_artifact(artifact, _pass)

    assert verified.ready
    assert verified.artifact_digest == artifact.artifact_digest
    assert verified.unit_sha256 == artifact.unit_sha256
    assert not verified.failed_units
    assert observed_paths and all(not path.exists() for path in observed_paths)

    failed = verify_external_supervisor_artifact(artifact, lambda _command: _Result(1))
    assert not failed.ready
    assert not failed.unit_sha256
    assert failed.failed_units == dict.fromkeys(artifact.unit_sha256, "systemd-analyze")

    def _raise(_command: Any) -> _Result:
        raise OSError("missing systemd-analyze")

    errored = verify_external_supervisor_artifact(artifact, _raise)
    assert not errored.ready
    assert not errored.unit_sha256


def test_artifact_parser_rejects_tamper_duplicates_types_and_noncanonical_json(
    tmp_path: Path,
) -> None:
    artifact = _build(_candidate(tmp_path))
    raw = json.loads(artifact.to_bytes())
    raw["supervisors"][0]["service_unit"] += "# drift\n"
    with pytest.raises(ValueError):
        ExternalSupervisorArtifact.from_bytes(_canonical_bytes(raw))

    raw = json.loads(artifact.to_bytes())
    raw["extra"] = "unauthorized"
    with pytest.raises(ValueError, match="fields"):
        ExternalSupervisorArtifact.from_bytes(_canonical_bytes(raw))

    duplicate = artifact.to_bytes().replace(
        b'"schema_version":3',
        b'"schema_version":3,"schema_version":3',
        1,
    )
    with pytest.raises(ValueError, match="invalid"):
        ExternalSupervisorArtifact.from_bytes(duplicate)

    raw = json.loads(artifact.to_bytes())
    raw["supervisors"][0]["enabled"] = 1
    with pytest.raises(ValueError, match="boolean"):
        ExternalSupervisorArtifact.from_bytes(_canonical_bytes(raw))

    with pytest.raises(ValueError, match="encoding is not canonical"):
        ExternalSupervisorArtifact.from_bytes(json.dumps(json.loads(artifact.to_bytes())).encode())

    with pytest.raises(ValueError, match="identity is invalid"):
        replace(artifact, supervisors=(artifact.supervisors[0], artifact.supervisors[0]))


@pytest.mark.parametrize(
    ("mode", "path_name"),
    [
        (0o666, PROFILE_PATH),
        (0o777, SCRIPT_PATH),
    ],
)
def test_builder_rejects_group_or_world_writable_sources(
    tmp_path: Path,
    mode: int,
    path_name: str,
) -> None:
    root = _candidate(tmp_path)
    (root / path_name).chmod(mode)

    with pytest.raises(ValueError, match="metadata is unsafe"):
        _build(root)


def test_builder_rejects_symlink_and_nonexecutable_script(tmp_path: Path) -> None:
    root = _candidate(tmp_path)
    script = root / SCRIPT_PATH
    target = script.with_name("alternate.py")
    target.write_bytes(script.read_bytes())
    target.chmod(0o755)
    script.unlink()
    script.symlink_to(target.name)

    with pytest.raises(ValueError, match="traversal is unsafe"):
        _build(root)

    script.unlink()
    script.write_bytes(target.read_bytes())
    script.chmod(0o644)
    with pytest.raises(ValueError, match="owner-executable"):
        _build(root)


def test_builder_rejects_source_drift_during_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _candidate(tmp_path)
    profile_path = root / PROFILE_PATH
    original = readiness.read_trusted_file
    profile_reads = 0

    def _drifting_read(path: Path, **kwargs: Any):
        nonlocal profile_reads
        result = original(path, **kwargs)
        if path == profile_path:
            profile_reads += 1
            if profile_reads == 2:
                return replace(result, metadata_fingerprint="f" * 64)
        return result

    monkeypatch.setattr(readiness, "read_trusted_file", _drifting_read)

    with pytest.raises(ValueError, match="profile changed"):
        _build(root)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('"127.0.0.1"', '"10.0.0.8"', "loopback"),
        ('"loom-staging"', '"loom-prod"', "namespace"),
        (_toml_string(STAGING_KUBECONFIG), '"/tmp/kubeconfig"', "kubeconfig"),
        (_toml_string(_WORKING_DIRECTORY), '"relative/repo"', "absolute path"),
        (
            _toml_string(_PYTHON_PATH),
            '"/usr/bin/python3"',
            "not canonical",
        ),
        ('"10.0"', '"61"', "out of bounds"),
        ('"5432"', '"5433"', "remote port drifted"),
        (
            '"/etc/loom/credentials/global-execution/gb10-witness.json"',
            '"/etc/loom/credentials/global-execution/oldlab-witness.json"',
            "global execution authority",
        ),
        (
            '"/etc/loom/credentials/global-execution/manager-ed25519.pub"',
            '"/tmp/manager-ed25519.pub"',
            "global execution authority",
        ),
        (
            '"/etc/loom/credentials/global-execution/manager-ed25519.pub.sha256"',
            '"/tmp/manager-ed25519.pub.sha256"',
            "global execution authority",
        ),
    ],
)
def test_builder_rejects_unsafe_or_noncanonical_identity(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    supervisor = _supervisor().replace(old, new, 1)
    root = _candidate(tmp_path, supervisors=[supervisor])

    with pytest.raises(ValueError, match=message):
        _build(root)


def test_builder_rejects_missing_duplicate_or_validate_only_arguments(tmp_path: Path) -> None:
    args = _args()
    flag_index = args.index("--db-connect-timeout-sec")
    del args[flag_index : flag_index + 2]
    with pytest.raises(ValueError, match="bounded tunnel arguments are missing"):
        _build(_candidate(tmp_path / "missing", supervisors=[_supervisor(args=args)]))

    duplicate_args = [*_args(), "--db-local-port", "15452"]
    with pytest.raises(ValueError, match="unauthorized"):
        _build(_candidate(tmp_path / "duplicate", supervisors=[_supervisor(args=duplicate_args)]))

    validate_args = [*_args(), "--validate-only"]
    with pytest.raises(ValueError, match="validate-only"):
        _build(_candidate(tmp_path / "validate", supervisors=[_supervisor(args=validate_args)]))


@pytest.mark.parametrize(
    "flag",
    [
        "--global-execution-witness-json",
        "--manager-public-key",
        "--expected-manager-public-key-sha256-file",
    ],
)
def test_builder_requires_global_execution_fence_arguments(
    tmp_path: Path,
    flag: str,
) -> None:
    args = _args()
    flag_index = args.index(flag)
    del args[flag_index : flag_index + 2]

    with pytest.raises(ValueError, match="arguments are missing"):
        _build(_candidate(tmp_path, supervisors=[_supervisor(args=args)]))


def test_builder_rejects_foreign_slurm_authority_arguments(tmp_path: Path) -> None:
    args = _args()
    cluster_index = args.index("--expected-slurm-cluster-name") + 1
    args[cluster_index] = "foreign-cluster"

    with pytest.raises(ValueError, match="Slurm authority"):
        _build(_candidate(tmp_path, supervisors=[_supervisor(args=args)]))


def test_builder_requires_dedicated_gb10_database_secret(tmp_path: Path) -> None:
    args = _args()
    secret_index = args.index("--db-secret-name")
    del args[secret_index : secret_index + 2]

    with pytest.raises(ValueError, match="optional authority"):
        _build(_candidate(tmp_path, supervisors=[_supervisor(args=args)]))


def test_builder_rejects_timer_slower_than_freshness_bound(tmp_path: Path) -> None:
    args = _args()
    freshness_index = args.index("--freshness-sec") + 1
    args[freshness_index] = "10"

    with pytest.raises(ValueError, match="freshness"):
        _build(_candidate(tmp_path, supervisors=[_supervisor(args=args)]))


@pytest.mark.parametrize(
    ("service_name", "timer_name", "message"),
    [
        ("other.service", "other.timer", "protected unit name"),
        (
            "loom-autoscaler-gb10-staging.service",
            "loom-autoscaler-other.timer",
            "unit pair",
        ),
    ],
)
def test_builder_rejects_transport_incompatible_unit_identity(
    tmp_path: Path,
    service_name: str,
    timer_name: str,
    message: str,
) -> None:
    root = _candidate(
        tmp_path,
        supervisors=[
            _supervisor(service_name=service_name, timer_name=timer_name),
        ],
    )

    with pytest.raises(ValueError, match=message):
        _build(root)


@pytest.mark.parametrize("collision", ["unit", "port"])
def test_builder_rejects_cross_supervisor_collisions(tmp_path: Path, collision: str) -> None:
    stem = "loom-autoscaler-gb10-staging" if collision == "unit" else "loom-autoscaler-other"
    service_name = f"{stem}.service"
    timer_name = f"{stem}.timer"
    port = 15451 if collision == "port" else 15452
    root = _candidate(
        tmp_path,
        supervisors=[
            _supervisor(),
            _supervisor(
                name="other",
                service_name=service_name,
                timer_name=timer_name,
                port=port,
            ),
        ],
    )

    with pytest.raises(ValueError, match=r"duplicate|identity is invalid"):
        _build(root)
