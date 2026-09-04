from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tarfile
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import scripts.ops.install_capacity_executor as installer_module
from scripts.ops.capacity_executor_release import record_release
from scripts.ops.install_capacity_executor import (
    CapacityExecutorInstallError,
    CommandResult,
    ControllerInstaller,
    InstallContext,
    _controller_discovery_operation,
    _controller_prerequisite_operation,
    _extract_release_tar,
    _pool_credential_operation,
    _validate_image_reference,
)

from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_cli.capacity_control_plane import (
    CapacityPoolExecutorBinding,
    CapacityPoolExecutorProfile,
    load_capacity_pool_executor_profile,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
    render_capacity_pool_inventory_policies,
)
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    PreparedControllerRequest,
    prepared_executor_profile_sha256,
)
from loom_cli.rollout.operator.protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    ControllerDiscoveryRequest,
    controller_job_visibility_evidence_sha256,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
    ControllerPrerequisiteRequest,
    controller_local_authority_sha256,
)
from loom_cli.rollout.operator.protected_pool_credential_transport import (
    PoolExecutionCredentialEvidence,
    PoolExecutionCredentialPayload,
)

_DIGEST = "a" * 64
_IMAGE = f"ghcr.io/qianyi-sun/loom-capacity-executor@sha256:{_DIGEST}"
_SOURCE_SHA = "1" * 40
_REPO_ROOT = Path(__file__).resolve().parents[2]
_UNITS = (
    "loom-capacity-pool-executor.service",
    "loom-capacity-pool-executor-prepared.service",
    "loom-capacity-pool-executor-prepared.timer",
    "loom-capacity-pool-executor-active.service",
    "loom-capacity-pool-executor-active.timer",
)
_TMPFILES = b"d /run/loom-capacity-executor 0700 loom_capacity_executor loom_capacity_executor -\n"


def _tar(entries: tuple[tuple[str, bytes | None, int, str], ...]) -> io.BytesIO:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        for name, payload, mode, kind in entries:
            member = tarfile.TarInfo(name)
            member.mode = mode
            if kind == "file":
                assert payload is not None
                member.size = len(payload)
                bundle.addfile(member, io.BytesIO(payload))
            elif kind == "dir":
                member.type = tarfile.DIRTYPE
                bundle.addfile(member)
            else:
                member.type = tarfile.SYMTYPE
                member.linkname = "release-manifest.json"
                bundle.addfile(member)
    archive.seek(0)
    return archive


def test_image_reference_requires_the_exact_executor_repository_and_digest() -> None:
    assert _validate_image_reference(_IMAGE) == _DIGEST
    assert (
        _validate_image_reference(f"192.168.50.13:5000/loom-capacity-executor@sha256:{_DIGEST}")
        == _DIGEST
    )

    for invalid in (
        "ghcr.io/qianyi-sun/loom-capacity-executor:latest",
        f"ghcr.io/qianyi-sun/loom-capacity-manager@sha256:{_DIGEST}",
        f"192.168.50.13:5000/other/loom-capacity-executor@sha256:{'0' * 64}",
        f"ghcr.io/qianyi-sun/loom-capacity-executor@sha256:{'A' * 64}",
    ):
        with pytest.raises(CapacityExecutorInstallError, match="digest reference"):
            _validate_image_reference(invalid)


def test_release_tar_extraction_preserves_exact_regular_file_bytes_and_modes(
    tmp_path: Path,
) -> None:
    stream = _tar(
        (
            ("payload", None, 0o555, "dir"),
            ("payload/wheelhouse", None, 0o555, "dir"),
            ("payload/wheelhouse/loom.whl", b"wheel", 0o444, "file"),
            ("release-manifest.json", b"{}\n", 0o444, "file"),
        )
    )

    _extract_release_tar(stream, tmp_path)

    wheel = tmp_path / "payload/wheelhouse/loom.whl"
    assert wheel.read_bytes() == b"wheel"
    assert wheel.stat().st_mode & 0o777 == 0o444
    assert (tmp_path / "payload").stat().st_mode & 0o777 == 0o555


@pytest.mark.parametrize(
    "entries",
    (
        (("../outside", b"escape", 0o444, "file"),),
        (("payload/link", None, 0o777, "symlink"),),
        (
            ("payload", None, 0o555, "dir"),
            ("payload/value", b"one", 0o444, "file"),
            ("payload/value", b"two", 0o444, "file"),
        ),
    ),
)
def test_release_tar_extraction_rejects_unsafe_or_duplicate_members(
    tmp_path: Path,
    entries: tuple[tuple[str, bytes | None, int, str], ...],
) -> None:
    with pytest.raises(CapacityExecutorInstallError, match="archive"):
        _extract_release_tar(_tar(entries), tmp_path)


class FakeHostRunner:
    def __init__(
        self,
        root: Path,
        *,
        image_architecture: str = "amd64",
        image_revision: str = _SOURCE_SHA,
        repo_digests: tuple[str, ...] = (_IMAGE,),
        active_units: tuple[str, ...] = (),
        enabled_units: tuple[str, ...] = (),
        unit_file_states: dict[str, str] | None = None,
        supplementary_gids: tuple[int, ...] = (),
        slurm_cluster: str = "trt-oldlab",
        slurm_partition: str = "loom-staging",
        slurm_nodes: tuple[str, ...] = (
            "trt-eai-oldlab-3",
            "trt-eai-oldlab-4",
            "trt-eai-oldlab-5",
        ),
        manager_route_source: str = "192.168.50.103",
        slurm_version: tuple[int, int, int] = (23, 11, 4),
        data_parser: str = "data_parser/v0.0.40",
        slurm_metadata_cluster: str | None = None,
        slurm_metadata_nodes: tuple[str, ...] | None = None,
        slurm_metadata_version: tuple[int, int, int] | None = None,
        rollout_group_present: bool = True,
        rollout_group_member: bool = True,
        partition_allow_groups: str = "loom-rollout",
        partition_allow_accounts: str = "ALL",
        partition_allow_qos: str = "ALL",
        slurm_association: str = "trt-gb10|loom-staging|loom_capacity_executor|loom-staging|loom-staging|loom-staging|",
    ) -> None:
        self.root = root
        self.image_architecture = image_architecture
        self.image_revision = image_revision
        self.repo_digests = repo_digests
        self.active_units = set(active_units)
        self.enabled_units = set(enabled_units)
        self.unit_file_states = unit_file_states or {}
        self.supplementary_gids = supplementary_gids
        self.slurm_cluster = slurm_cluster
        self.slurm_partition = slurm_partition
        self.slurm_nodes = slurm_nodes
        self.manager_route_source = manager_route_source
        self.slurm_version = slurm_version
        self.data_parser = data_parser
        self.slurm_metadata_cluster = (
            slurm_cluster if slurm_metadata_cluster is None else slurm_metadata_cluster
        )
        self.slurm_metadata_nodes = (
            slurm_nodes if slurm_metadata_nodes is None else slurm_metadata_nodes
        )
        self.slurm_metadata_version = (
            slurm_version if slurm_metadata_version is None else slurm_metadata_version
        )
        self.rollout_group_present = rollout_group_present
        self.rollout_group_member = rollout_group_member
        self.rollout_group_queried = False
        self.rollout_gid = os.getegid() + 1000
        self.partition_allow_groups = partition_allow_groups
        self.partition_allow_accounts = partition_allow_accounts
        self.partition_allow_qos = partition_allow_qos
        self.slurm_association = slurm_association
        self.group_present = False
        self.user_present = False
        self.units_verified = False
        self.runtime_probe_fails = False
        self.prepared_service_fails = False
        self.prepared_ticks = 0
        self.calls: list[tuple[str, ...]] = []

    def _path(self, absolute: str) -> Path:
        path = Path(absolute)
        assert path.is_absolute()
        return self.root.joinpath(*path.parts[1:])

    def run(
        self,
        argv: tuple[str, ...] | list[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        del env
        call = tuple(argv)
        self.calls.append(call)
        command = Path(call[0]).name
        service_call = call
        if command == "runuser":
            assert call[1:4] == ("--user", "loom_capacity_executor", "--")
            service_call = call[4:]
            command = Path(service_call[0]).name
        result: CommandResult
        if command == "systemctl" and call[1] == "is-active":
            unit = call[2]
            active = unit in self.active_units
            result = CommandResult(0 if active else 3, "active\n" if active else "inactive\n")
        elif command == "systemctl" and call[1] == "is-enabled":
            unit = call[2]
            state = self.unit_file_states.get(
                unit,
                (
                    "enabled"
                    if unit in self.enabled_units
                    else "disabled"
                    if unit.endswith(".timer")
                    else "static"
                ),
            )
            result = CommandResult(0 if state == "static" else 1, f"{state}\n")
        elif command == "systemctl" and call[1:] == ("daemon-reload",):
            result = CommandResult(0)
        elif command == "systemctl" and call[1:] == (
            "enable",
            "--now",
            "loom-capacity-pool-executor-prepared.timer",
        ):
            self.enabled_units.add("loom-capacity-pool-executor-prepared.timer")
            self.active_units.add("loom-capacity-pool-executor-prepared.timer")
            result = CommandResult(0)
        elif command == "systemctl" and call[1:] == (
            "disable",
            "--now",
            "loom-capacity-pool-executor-prepared.timer",
        ):
            self.enabled_units.discard("loom-capacity-pool-executor-prepared.timer")
            self.active_units.discard("loom-capacity-pool-executor-prepared.timer")
            result = CommandResult(0)
        elif command == "systemctl" and call[1:] == (
            "start",
            "loom-capacity-pool-executor-prepared.service",
        ):
            if self.prepared_service_fails:
                result = CommandResult(1)
            else:
                self.prepared_ticks += 1
                result = CommandResult(0)
        elif command == "systemctl" and call[1:] == (
            "stop",
            "loom-capacity-pool-executor-prepared.service",
        ):
            self.active_units.discard("loom-capacity-pool-executor-prepared.service")
            result = CommandResult(0)
        elif command == "systemd-analyze" and call[1] == "verify":
            assert {Path(path).name for path in call[2:]} == set(_UNITS)
            current = self.root / "opt/loom-capacity-executor"
            assert current.is_symlink()
            self.units_verified = True
            result = CommandResult(0)
        elif command == "docker" and call[1:3] == ("pull", "--quiet"):
            assert call[3] == _IMAGE
            result = CommandResult(0, f"{_IMAGE}\n")
        elif command == "docker" and call[1:3] == ("image", "inspect"):
            assert call[3] == _IMAGE
            result = CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "Architecture": self.image_architecture,
                            "Os": "linux",
                            "RepoDigests": list(self.repo_digests),
                            "Config": {
                                "Labels": {"org.opencontainers.image.revision": self.image_revision}
                            },
                        }
                    ]
                ),
            )
        elif command == "getent" and call[1:] == ("group", "loom_capacity_executor"):
            result = CommandResult(
                0 if self.group_present else 2,
                (f"loom_capacity_executor:x:{os.getegid()}:\n" if self.group_present else ""),
            )
        elif command == "getent" and call[1:] == ("passwd", "loom_capacity_executor"):
            result = CommandResult(
                0 if self.user_present else 2,
                (
                    "loom_capacity_executor:x:"
                    f"{os.geteuid()}:{os.getegid()}::/var/lib/loom-capacity-executor:"
                    "/usr/sbin/nologin\n"
                    if self.user_present
                    else ""
                ),
            )
        elif command == "getent" and call[1:] == ("group", "loom-rollout"):
            self.rollout_group_queried = True
            members = "loom_capacity_executor" if self.rollout_group_member else ""
            result = CommandResult(
                0 if self.rollout_group_present else 2,
                (
                    f"loom-rollout:x:{self.rollout_gid}:{members}\n"
                    if self.rollout_group_present
                    else ""
                ),
            )
        elif command == "id" and call[1:] == ("-u", "loom_capacity_executor"):
            result = CommandResult(0 if self.user_present else 1, f"{os.geteuid()}\n")
        elif command == "id" and call[1:] == ("-g", "loom_capacity_executor"):
            result = CommandResult(0 if self.user_present else 1, f"{os.getegid()}\n")
        elif command == "id" and call[1:] == ("-G", "loom_capacity_executor"):
            rollout_gids = (
                (self.rollout_gid,)
                if self.rollout_group_queried and self.rollout_group_member
                else ()
            )
            gids = (os.getegid(), *rollout_gids, *self.supplementary_gids)
            result = CommandResult(
                0 if self.user_present else 1,
                " ".join(str(gid) for gid in gids) + "\n",
            )
        elif command == "groupadd":
            self.group_present = True
            result = CommandResult(0)
        elif command == "useradd":
            assert self.group_present
            self.user_present = True
            result = CommandResult(0)
        elif command == "usermod":
            assert call[1:] == (
                "--append",
                "--groups",
                "loom-rollout",
                "loom_capacity_executor",
            )
            self.rollout_group_member = True
            result = CommandResult(0)
        elif command == "python3.12" and call[1:4] == ("-m", "venv", "--copies"):
            venv = self._path(call[4])
            (venv / "bin").mkdir(parents=True)
            (venv / "bin/python").write_bytes(b"python-copy")
            (venv / "bin/python").chmod(0o755)
            (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
            result = CommandResult(0)
        elif command == "python" and "/venv/bin/" in call[0]:
            if "loom-0.0.0-py3-none-any.whl" in " ".join(call):
                launcher = self._path(
                    str(Path(call[0]).with_name("loom-capacity-trusted-launcher"))
                )
                launcher.write_bytes(b"launcher")
                launcher.chmod(0o755)
            is_runtime_probe = "-I" in call
            result = CommandResult(1 if self.runtime_probe_fails and is_runtime_probe else 0)
        elif command == "loom-capacity-trusted-launcher":
            result = CommandResult(1 if self.runtime_probe_fails else 0)
        elif command == "systemd-tmpfiles":
            runtime = self._path("/run/loom-capacity-executor")
            runtime.mkdir(parents=True, exist_ok=True)
            runtime.chmod(0o700)
            result = CommandResult(0)
        elif command == "scontrol" and service_call[1:] == ("show", "config"):
            result = CommandResult(0, f"ClusterName = {self.slurm_cluster}\n")
        elif command == "scontrol" and service_call[1:] == ("--version",):
            result = CommandResult(0, "slurm-wlm " + ".".join(map(str, self.slurm_version)) + "\n")
        elif command == "scontrol" and service_call[1:] == (
            "show",
            "nodes",
            "fixture-targets",
            "--json",
        ):
            major, minor, micro = self.slurm_metadata_version
            result = CommandResult(
                0,
                json.dumps(
                    {
                        "nodes": [{"name": node} for node in self.slurm_metadata_nodes],
                        "meta": {
                            "plugin": {"data_parser": self.data_parser},
                            "slurm": {
                                "cluster": self.slurm_metadata_cluster,
                                "version": {
                                    "major": str(major),
                                    "minor": str(minor),
                                    "micro": str(micro),
                                },
                            },
                        },
                    }
                ),
            )
        elif command == "ip" and service_call[1:] == (
            "-json",
            "route",
            "get",
            "192.168.50.103",
        ):
            result = CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "dst": "192.168.50.103",
                            "dev": "fixture0",
                            "prefsrc": self.manager_route_source,
                        }
                    ]
                ),
            )
        elif command == "scontrol" and service_call[1:3] == ("show", "partition"):
            result = CommandResult(
                0,
                f"PartitionName={self.slurm_partition} Nodes=fixture-targets State=UP "
                f"AllowGroups={self.partition_allow_groups} "
                f"AllowAccounts={self.partition_allow_accounts} "
                f"AllowQos={self.partition_allow_qos}\n",
            )
        elif command == "sacctmgr":
            result = CommandResult(0, f"{self.slurm_association}\n")
        elif command == "scontrol" and service_call[1:] == (
            "show",
            "hostnames",
            "fixture-targets",
        ):
            result = CommandResult(0, "".join(f"{node}\n" for node in self.slurm_nodes))
        else:
            raise AssertionError(f"unexpected command: {call}")
        if check and result.returncode != 0:
            raise CapacityExecutorInstallError(f"command failed safely: {command}")
        return result


def _fake_release_extractor(
    image: str,
    destination: Path,
    runner: Any,
    context: InstallContext,
) -> None:
    del runner, context
    assert image == _IMAGE
    payload = destination / "payload"
    wheelhouse = payload / "wheelhouse"
    units = payload / "units"
    tmpfiles = payload / "tmpfiles"
    wheelhouse.mkdir(parents=True)
    units.mkdir()
    tmpfiles.mkdir()
    files = {
        payload / "requirements.lock": b"dependency==1 --hash=sha256:" + b"b" * 64 + b"\n",
        wheelhouse / "dependency-1-py3-none-any.whl": b"dependency",
        wheelhouse / "loom-0.0.0-py3-none-any.whl": b"loom",
        # Docker COPY preserves the checked-in source filename in the release payload.
        tmpfiles / "loom-capacity-executor.tmpfiles": _TMPFILES,
    }
    for unit in _UNITS:
        files[units / unit] = (_REPO_ROOT / "deploy/dev-fleet" / unit).read_bytes()
    for path, value in files.items():
        path.write_bytes(value)
        path.chmod(0o444)
    record_release(destination, source_sha=_SOURCE_SHA, architecture="amd64")


def _context(tmp_path: Path) -> InstallContext:
    return InstallContext(
        root=tmp_path,
        command_prefix=(),
        authority_uid=os.geteuid(),
        authority_gid=os.getegid(),
    )


def _controller_request(tmp_path: Path) -> ControllerPrerequisiteRequest:
    executable_paths = {
        name: f"/usr/bin/{name}"
        for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
    }
    executable_sha256: dict[str, str] = {}
    for name, absolute in executable_paths.items():
        path = tmp_path.joinpath(*Path(absolute).parts[1:])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{name}-fixture\n".encode("ascii"))
        path.chmod(0o755)
        executable_sha256[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    slurm_conf = tmp_path / "etc/slurm/slurm.conf"
    slurm_conf.parent.mkdir(parents=True)
    slurm_conf.write_text("ClusterName=trt-oldlab\n", encoding="ascii")
    slurm_conf.chmod(0o644)
    for directory in (
        tmp_path / "usr",
        tmp_path / "usr/bin",
        tmp_path / "etc",
        tmp_path / "etc/slurm",
    ):
        directory.chmod(0o755)
    configuration_sha256 = {"slurm.conf": hashlib.sha256(slurm_conf.read_bytes()).hexdigest()}
    profile = load_capacity_pool_executor_profile(
        _REPO_ROOT / "deploy/dev-fleet/capacity-pool-executor.toml.example"
    )
    template = profile.pools[1].model_dump(mode="python")
    targets = tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6))
    exemplar = template["inventory"]["nodes"][0]
    template.update(
        {
            "controller_authority_sha256": "5" * 64,
            "controller_host": "TRT-EAI-OLDLAB-1",
            "local_uid": os.geteuid(),
            "partition": "loom-staging",
            "slurm_cluster": "trt-oldlab",
            "slurm_executables": executable_paths,
        }
    )
    template["inventory"].update(
        {
            "controller_cluster": "trt-oldlab",
            "nodes": [
                {
                    **exemplar,
                    "node_id": node_id,
                    "pool_id": "oldlab",
                    "features": ("amd64",),
                }
                for node_id in targets
            ],
            "query_uid": os.geteuid(),
            "relevant_partitions": ("loom-staging",),
            "scontrol_sha256": executable_sha256["scontrol"],
            "squeue_sha256": executable_sha256["squeue"],
            "slurm_conf_sha256": configuration_sha256["slurm.conf"],
            "job_visibility_evidence_sha256": (
                controller_job_visibility_evidence_sha256(
                    pool_id="oldlab",
                    partition_fields={"AllowGroups": "loom-rollout"},
                    association_fields=(),
                )
            ),
        }
    )
    template["local_authority_sha256"] = controller_local_authority_sha256(
        pool_id="oldlab",
        architecture="amd64",
        controller_hostname="TRT-EAI-OLDLAB-1",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        slurm_cluster="trt-oldlab",
        partition="loom-staging",
        target_nodes=targets,
        executable_sha256=executable_sha256,
        configuration_sha256=configuration_sha256,
        job_visibility_evidence_sha256=template["inventory"]["job_visibility_evidence_sha256"],
    )
    binding = CapacityPoolExecutorBinding.model_validate(template)
    return ControllerPrerequisiteRequest(
        pool_id="oldlab",
        source_sha=_SOURCE_SHA,
        architecture="amd64",
        image=_IMAGE,
        service_user="loom_capacity_executor",
        binding=binding,
        credential_metadata_sha256={
            "pool-executor-oldlab": "6" * 64,
            "pool-ownership-oldlab": "7" * 64,
        },
        transport_authority_sha256="8" * 64,
    )


def _pool_credential_payload(pool_id: str = "oldlab") -> PoolExecutionCredentialPayload:
    return PoolExecutionCredentialPayload(
        pool_id=pool_id,
        files={
            "bearer-token": f"{pool_id}-bearer-token".encode("ascii"),
            "client-certificate.pem": f"{pool_id}-certificate".encode("ascii"),
            "client-private-key.pem": f"{pool_id}-private-key".encode("ascii"),
            "manager-ca.pem": f"{pool_id}-manager-ca".encode("ascii"),
            "ownership-private-key": f"{pool_id}-ownership-key".encode("ascii"),
        },
        credential_metadata_sha256={
            f"pool-executor-{pool_id}": "6" * 64,
            f"pool-ownership-{pool_id}": "7" * 64,
        },
    )


def _prepare_runtime_root(tmp_path: Path) -> Path:
    root = tmp_path / "run/loom-capacity-executor"
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    return root


def _prepared_request(prerequisite: ControllerPrerequisiteRequest) -> PreparedControllerRequest:
    execution = ExecutionContextV2(
        authority_incarnation=UUID("11111111-1111-4111-8111-111111111111"),
        writer_epoch=11,
        configuration_epoch=7,
        execution_epoch=3,
        execution_manifest_sha256="3" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256="4" * 64,
    )
    value = load_capacity_pool_executor_profile(
        _REPO_ROOT / "deploy/dev-fleet/capacity-pool-executor.toml.example"
    ).model_dump(mode="python")
    value.update(
        {
            "authority_incarnation": str(execution.authority_incarnation),
            "configuration_epoch": execution.configuration_epoch,
            "execution_epoch": execution.execution_epoch,
            "execution_manifest_sha256": execution.execution_manifest_sha256,
            "executor_image": prerequisite.image,
            "trusted_fleet_release_sha256": execution.trusted_fleet_release_sha256,
            "writer_epoch": execution.writer_epoch,
        }
    )
    pools = list(value["pools"])
    pools[0]["controller_authority_sha256"] = "9" * 64
    pools[1] = prerequisite.binding.model_dump(mode="python")
    value["pools"] = pools
    profile = CapacityPoolExecutorProfile.model_validate(value)
    configs = render_capacity_pool_executor_configs(profile)
    policies = render_capacity_pool_inventory_policies(profile)
    pool_id = prerequisite.pool_id
    return PreparedControllerRequest(
        schema_version=1,
        pool_id=pool_id,
        transport_authority_sha256=prerequisite.transport_authority_sha256,
        prerequisite=prerequisite,
        execution=execution,
        profile_sha256=prepared_executor_profile_sha256(profile),
        files={
            prerequisite.binding.config_file: configs[pool_id].encode("ascii"),
            str(
                Path(prerequisite.binding.config_file).with_name(f"{pool_id}-inventory-policy.json")
            ): policies[pool_id].encode("ascii"),
            "/etc/loom-capacity-executor/service.env": (
                render_capacity_pool_executor_service_environment(profile, pool_id).encode("ascii")
            ),
        },
    )


def test_controller_converges_only_exact_prepared_files_while_all_units_remain_inert(
    tmp_path: Path,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)

    assert installer.observe_prepared(request) is None
    evidence = installer.converge_prepared_files(request)

    assert evidence.pool_id == "oldlab"
    assert evidence.request_sha256 == request.request_sha256
    assert evidence.successful_tick is False
    assert evidence.tick_evidence_sha256 is None
    assert set(evidence.file_sha256) == set(request.files)
    for absolute, payload in request.files.items():
        installed = tmp_path.joinpath(*Path(absolute).parts[1:])
        assert installed.read_bytes() == payload
        assert stat.S_IMODE(installed.stat().st_mode) == 0o600
    assert not runner.active_units
    assert not runner.enabled_units
    assert not any(Path(call[0]).name in {"sbatch", "scancel"} for call in runner.calls)


def test_controller_revalidates_prerequisite_immediately_before_enabling_prepared_timer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    installer.converge_prepared_files(request)
    observe = installer.observe_prepared
    prerequisite_path = tmp_path.joinpath(*Path(prerequisite.prerequisite_input_path).parts[1:])

    def observe_then_drift(value: PreparedControllerRequest):
        evidence = observe(value)
        prerequisite_path.write_bytes(b"{}\n")
        return evidence

    monkeypatch.setattr(installer, "observe_prepared", observe_then_drift)

    with pytest.raises(CapacityExecutorInstallError, match="prerequisite changed"):
        installer.enable_prepared_timer(request)

    assert "loom-capacity-pool-executor-prepared.timer" not in runner.enabled_units
    assert not any(call[1:3] == ("enable", "--now") for call in runner.calls)


def test_controller_revalidates_prerequisite_immediately_before_running_prepared_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    installer.converge_prepared_files(request)
    installer.enable_prepared_timer(request)
    observe = installer.observe_prepared
    prerequisite_path = tmp_path.joinpath(*Path(prerequisite.prerequisite_input_path).parts[1:])

    def observe_then_drift(value: PreparedControllerRequest):
        evidence = observe(value)
        prerequisite_path.write_bytes(b"{}\n")
        return evidence

    monkeypatch.setattr(installer, "observe_prepared", observe_then_drift)

    with pytest.raises(CapacityExecutorInstallError, match="prerequisite changed"):
        installer.run_prepared_tick(request)

    assert runner.prepared_ticks == 0
    assert not any(
        call[1:] == ("start", "loom-capacity-pool-executor-prepared.service")
        for call in runner.calls
    )


def test_controller_enables_ticks_and_disables_only_the_prepared_timer(
    tmp_path: Path,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    installer.converge_prepared_files(request)

    enabled = installer.enable_prepared_timer(request)
    assert enabled.unit_active_state["loom-capacity-pool-executor-prepared.timer"] == ("active")
    assert enabled.unit_file_state["loom-capacity-pool-executor-prepared.timer"] == ("enabled")
    assert enabled.unit_active_state["loom-capacity-pool-executor-active.timer"] == ("inactive")
    assert enabled.unit_file_state["loom-capacity-pool-executor-active.timer"] == ("disabled")

    ticked = installer.run_prepared_tick(request)
    assert runner.prepared_ticks == 1
    assert ticked.successful_tick is True
    assert ticked.tick_evidence_sha256 is not None
    tick_receipts = list(
        (tmp_path / "var/lib/loom-capacity-executor/oldlab").glob(".prepared-tick-*.json")
    )
    assert len(tick_receipts) == 1
    assert stat.S_IMODE(tick_receipts[0].stat().st_mode) == 0o600

    disabled = installer.disable_prepared_timer(request)
    assert disabled.successful_tick is False
    assert disabled.unit_active_state["loom-capacity-pool-executor-prepared.timer"] == ("inactive")
    assert disabled.unit_file_state["loom-capacity-pool-executor-prepared.timer"] == ("disabled")
    assert "loom-capacity-pool-executor-active.timer" not in runner.enabled_units
    assert not any(Path(call[0]).name in {"sbatch", "scancel"} for call in runner.calls)
    active_units = {
        "loom-capacity-pool-executor-active.service",
        "loom-capacity-pool-executor-active.timer",
    }
    assert not any(
        Path(call[0]).name == "systemctl"
        and call[1] in {"enable", "disable", "start", "stop"}
        and call[-1] in active_units
        for call in runner.calls
    )


def test_failed_prepared_service_creates_no_success_receipt(tmp_path: Path) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    installer.converge_prepared_files(request)
    installer.enable_prepared_timer(request)
    runner.prepared_service_fails = True

    with pytest.raises(CapacityExecutorInstallError, match="command failed safely"):
        installer.run_prepared_tick(request)

    assert runner.prepared_ticks == 0
    assert (
        list((tmp_path / "var/lib/loom-capacity-executor/oldlab").glob(".prepared-tick-*.json"))
        == []
    )


def test_disable_prepared_timer_still_stops_units_when_file_readback_drifts(
    tmp_path: Path,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    installer.converge_prepared_files(request)
    installer.enable_prepared_timer(request)
    config_path = tmp_path.joinpath(*Path(prerequisite.binding.config_file).parts[1:])
    config_path.write_bytes(b"{}\n")

    with pytest.raises(CapacityExecutorInstallError, match="disable did not converge"):
        installer.disable_prepared_timer(request)

    assert "loom-capacity-pool-executor-prepared.timer" not in runner.active_units
    assert "loom-capacity-pool-executor-prepared.timer" not in runner.enabled_units
    assert any(
        call[1:] == ("disable", "--now", "loom-capacity-pool-executor-prepared.timer")
        for call in runner.calls
    )
    assert any(
        call[1:] == ("stop", "loom-capacity-pool-executor-prepared.service")
        for call in runner.calls
    )


def test_disable_prepared_timer_succeeds_when_prepared_files_are_absent(
    tmp_path: Path,
) -> None:
    """Catch turning safe partial-controller compensation into an unresolved failure."""

    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)

    assert installer.disable_prepared_timer(request) is None
    assert (
        installer_module._prepared_controller_operation(
            installer,
            "disable-prepared-timer",
            request.to_bytes(),
        )
        == b"null\n"
    )
    assert "loom-capacity-pool-executor-prepared.timer" not in runner.active_units
    assert "loom-capacity-pool-executor-prepared.timer" not in runner.enabled_units


def test_prepared_controller_wire_operations_are_strict_canonical_and_bounded(
    tmp_path: Path,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)

    assert (
        installer_module._prepared_controller_operation(
            installer,
            "observe-prepared",
            request.to_bytes(),
        )
        == b"null\n"
    )
    operations = (
        "converge-prepared-files",
        "enable-prepared-timer",
        "run-prepared-tick",
        "disable-prepared-timer",
    )
    for operation in operations:
        encoded = installer_module._prepared_controller_operation(
            installer,
            operation,
            request.to_bytes(),
        )
        evidence = installer_module.PreparedControllerEvidence.from_bytes(encoded)
        assert evidence.request_sha256 == request.request_sha256

    with pytest.raises(CapacityExecutorInstallError, match="request"):
        installer_module._prepared_controller_operation(
            installer,
            "observe-prepared",
            request.to_bytes() + b" ",
        )


def test_prepared_controller_wire_rejects_cross_pool_or_transport_authority_drift(
    tmp_path: Path,
) -> None:
    prerequisite = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(prerequisite)
    request = _prepared_request(prerequisite)
    original = json.loads(request.to_bytes())
    foreign_pool = json.loads(json.dumps(original))
    foreign_pool["pool_id"] = "gb10"
    foreign_authority = json.loads(json.dumps(original))
    foreign_authority["transport_authority_sha256"] = "9" * 64
    call_count = len(runner.calls)

    for invalid in (foreign_pool, foreign_authority):
        with pytest.raises(CapacityExecutorInstallError, match="request is invalid"):
            installer_module._prepared_controller_operation(
                installer,
                "observe-prepared",
                installer_module._canonical_json_bytes(invalid),
            )

    assert len(runner.calls) == call_count


@pytest.mark.parametrize(
    "operation",
    (
        "observe-prepared",
        "converge-prepared-files",
        "enable-prepared-timer",
        "run-prepared-tick",
        "disable-prepared-timer",
    ),
)
def test_prepared_controller_cli_bounds_stdin_for_every_operation(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    stdin = io.BytesIO(b"x" * (installer_module._MAX_PREPARED_REQUEST_BYTES + 1))
    monkeypatch.setattr(installer_module, "_validate_host_root", lambda _root: None)
    monkeypatch.setattr(installer_module, "ControllerInstaller", lambda **_kwargs: object())
    monkeypatch.setattr(installer_module.sys, "stdin", type("_Input", (), {"buffer": stdin})())

    with pytest.raises(SystemExit) as failure:
        installer_module.main(["--operation", operation])

    assert failure.value.code == 2


def test_installer_publishes_an_immutable_release_but_leaves_every_unit_inert(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    runner = FakeHostRunner(tmp_path)

    result = ControllerInstaller(
        context=context,
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    ).install(image=_IMAGE, source_sha=_SOURCE_SHA)

    expected_release = Path(f"/opt/loom-capacity-executor-releases/{_SOURCE_SHA}-amd64-{_DIGEST}")
    assert result.release_root == expected_release
    current = tmp_path / "opt/loom-capacity-executor"
    assert current.is_symlink()
    assert os.readlink(current) == str(expected_release)
    for unit in _UNITS:
        installed = tmp_path / "etc/systemd/system" / unit
        assert installed.read_bytes() == (_REPO_ROOT / "deploy/dev-fleet" / unit).read_bytes()
        assert installed.stat().st_mode & 0o777 == 0o644
        assert unit not in runner.active_units
        assert unit not in runner.enabled_units
    tmpfiles = tmp_path / "etc/tmpfiles.d/loom-capacity-executor.conf"
    assert tmpfiles.read_bytes() == _TMPFILES
    assert tmpfiles.stat().st_mode & 0o777 == 0o644
    for directory in (
        tmp_path / "etc/loom-capacity-executor",
        tmp_path / "run/loom-capacity-executor",
        tmp_path / "var/lib/loom-capacity-executor",
    ):
        assert directory.is_dir()
        assert directory.stat().st_mode & 0o777 == 0o700
    assert list((tmp_path / "etc/loom-capacity-executor").iterdir()) == []
    assert runner.units_verified is True
    release = tmp_path.joinpath(*expected_release.parts[1:])
    assert not (release / ".installing").exists()
    for path in release.rglob("*"):
        if not path.is_symlink():
            assert path.stat().st_mode & 0o022 == 0


def test_oldlab_inert_installer_converges_partition_admission_membership(
    tmp_path: Path,
) -> None:
    """Catch installing an executor that the OLDLAB partition cannot admit."""
    runner = FakeHostRunner(tmp_path, rollout_group_member=False)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert runner.rollout_group_member is True
    assert (
        "/usr/sbin/usermod",
        "--append",
        "--groups",
        "loom-rollout",
        "loom_capacity_executor",
    ) in runner.calls
    assert not any(
        Path(call[0]).name == "systemctl" and call[1] in {"start", "enable"}
        for call in runner.calls
    )


@pytest.mark.parametrize(
    ("active_units", "enabled_units"),
    (((_UNITS[1],), ()), ((), (_UNITS[2],))),
)
def test_installer_refuses_existing_active_or_enabled_units_before_extraction(
    tmp_path: Path,
    active_units: tuple[str, ...],
    enabled_units: tuple[str, ...],
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    runner = FakeHostRunner(
        tmp_path,
        active_units=active_units,
        enabled_units=enabled_units,
    )
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="active or enabled"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False
    assert not (tmp_path / "opt/loom-capacity-executor-releases").exists()
    assert not (tmp_path / "etc/systemd/system").exists()


def test_installer_refuses_indirect_unit_enablement_before_extraction(
    tmp_path: Path,
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    runner = FakeHostRunner(
        tmp_path,
        unit_file_states={_UNITS[2]: "indirect"},
    )
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="active or enabled"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False


@pytest.mark.parametrize(
    ("runner_kwargs", "message"),
    (
        ({"image_architecture": "arm64"}, "architecture"),
        ({"image_revision": "2" * 40}, "revision"),
        ({"repo_digests": ()}, "digest"),
    ),
)
def test_installer_rejects_oci_identity_drift_before_extraction(
    tmp_path: Path,
    runner_kwargs: dict[str, object],
    message: str,
) -> None:
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path, **runner_kwargs),
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match=message):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False
    assert not (tmp_path / "opt/loom-capacity-executor-releases").exists()


def test_installer_refuses_an_intermediate_authority_symlink(
    tmp_path: Path,
) -> None:
    redirected = tmp_path / "redirected-etc"
    redirected.mkdir()
    (tmp_path / "etc").symlink_to(redirected, target_is_directory=True)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match=r"parent|directory"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert not (redirected / "systemd/system/loom-capacity-pool-executor.service").exists()


def test_installer_refuses_a_service_identity_with_supplementary_groups(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path, supplementary_gids=(4242,))
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="service identity"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert not (tmp_path / "opt/loom-capacity-executor").is_symlink()


def test_installer_reuses_only_the_same_complete_immutable_release(
    tmp_path: Path,
) -> None:
    extractions = 0

    def counted_extractor(
        image: str,
        destination: Path,
        runner: Any,
        context: InstallContext,
    ) -> None:
        nonlocal extractions
        extractions += 1
        _fake_release_extractor(image, destination, runner, context)

    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=counted_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    first = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    second = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert second == first
    assert extractions == 1


def test_installer_reprobes_an_existing_immutable_release(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )
    installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    runner.runtime_probe_fails = True

    with pytest.raises(CapacityExecutorInstallError, match="command failed safely"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)


def test_installer_rejects_drift_anywhere_in_an_existing_runtime_tree(
    tmp_path: Path,
) -> None:
    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        effective_uid=0,
    )
    result = installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)
    drifted = tmp_path.joinpath(*result.release_root.parts[1:]) / "venv/pyvenv.cfg"
    drifted.chmod(0o666)

    with pytest.raises(CapacityExecutorInstallError, match="runtime authority"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)


def test_installer_rejects_a_foreign_current_release_symlink_before_extraction(
    tmp_path: Path,
) -> None:
    (tmp_path / "opt").mkdir()
    (tmp_path / "opt/loom-capacity-executor").symlink_to("/tmp/foreign-release")
    extracted = False

    def forbidden_extractor(*args: object, **kwargs: object) -> None:
        nonlocal extracted
        del args, kwargs
        extracted = True

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=forbidden_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="foreign"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    assert extracted is False


def test_installer_removes_a_manifest_drifted_incomplete_release(
    tmp_path: Path,
) -> None:
    def tampered_extractor(
        image: str,
        destination: Path,
        runner: Any,
        context: InstallContext,
    ) -> None:
        _fake_release_extractor(image, destination, runner, context)
        wheel = destination / "payload/wheelhouse/loom-0.0.0-py3-none-any.whl"
        wheel.chmod(0o644)
        wheel.write_bytes(b"tampered")
        wheel.chmod(0o444)

    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        extractor=tampered_extractor,
        machine="x86_64",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="release verification"):
        installer.install(image=_IMAGE, source_sha=_SOURCE_SHA)

    releases = tmp_path / "opt/loom-capacity-executor-releases"
    assert releases.is_dir()
    assert list(releases.iterdir()) == []
    assert not (tmp_path / "opt/loom-capacity-executor").exists()


def test_controller_prerequisite_converges_exact_inert_evidence_and_input(
    tmp_path: Path,
) -> None:
    request = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    assert installer.observe_prerequisite(request) is None

    evidence = installer.converge_prerequisite(request)

    assert evidence.pool_id == "oldlab"
    assert evidence.target_nodes == tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6))
    assert evidence.local_authority_sha256 == request.binding.local_authority_sha256
    assert evidence.controller_authority_sha256 == request.binding.controller_authority_sha256
    assert evidence.prerequisite_input_sha256 == request.prerequisite_input_sha256
    assert installer.observe_prerequisite(request) == evidence
    expected_input = (
        json.dumps(
            request.prerequisite_input_value(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")
    installed_input = tmp_path / "etc/loom-capacity-executor/oldlab-prerequisite.json"
    assert installed_input.read_bytes() == expected_input
    assert installed_input.stat().st_mode & 0o777 == 0o600
    for relative in (
        "run/loom-capacity-executor/oldlab",
        "var/lib/loom-capacity-executor/oldlab",
    ):
        directory = tmp_path / relative
        assert directory.stat().st_mode & 0o777 == 0o700
        assert directory.stat().st_uid == os.geteuid()
        assert directory.stat().st_gid == os.getegid()
    assert evidence.unit_active_state == {unit: "inactive" for unit in _UNITS}
    assert evidence.unit_file_state == {
        unit: "disabled" if unit.endswith(".timer") else "static" for unit in _UNITS
    }
    assert not any(
        call[1] in {"start", "enable"} for call in runner.calls if call[0].endswith("systemctl")
    )


def test_controller_discovery_returns_stable_read_only_local_authority(
    tmp_path: Path,
) -> None:
    """Catch requiring a completed prerequisite artifact before controller discovery."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="oldlab",
        transport_authority_sha256="8" * 64,
    )

    evidence = installer.discover_controller(request)

    assert evidence.pool_id == "oldlab"
    assert evidence.manager_client_cidr == "192.168.50.103/32"
    assert evidence.slurm_version == (23, 11, 4)
    assert evidence.data_parser == "data_parser/v0.0.40"
    assert evidence.service_uid == os.geteuid()
    assert evidence.local_authority_sha256 == controller_local_authority_sha256(
        pool_id="oldlab",
        architecture="amd64",
        controller_hostname="TRT-EAI-OLDLAB-1",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        slurm_cluster="trt-oldlab",
        partition="loom-staging",
        target_nodes=tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
        executable_sha256=evidence.executable_sha256,
        configuration_sha256=evidence.configuration_sha256,
        job_visibility_evidence_sha256=evidence.job_visibility_evidence_sha256,
    )
    assert not any(
        Path(call[0]).name in {"docker", "groupadd", "useradd"}
        or (Path(call[0]).name == "systemctl" and call[1] in {"start", "enable"})
        for call in runner.calls
    )


def test_controller_discovery_queries_slurm_and_route_as_the_service_principal(
    tmp_path: Path,
) -> None:
    """Catch claiming the service principal while collecting facts as root."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    installer.discover_controller(
        ControllerDiscoveryRequest(
            schema_version=1,
            pool_id="oldlab",
            transport_authority_sha256="8" * 64,
        )
    )

    observed_queries = [call for call in runner.calls if Path(call[0]).name == "runuser"]
    assert observed_queries == [
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/bin/scontrol",
            "show",
            "config",
        ),
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/bin/scontrol",
            "show",
            "partition",
            "loom-staging",
            "-o",
        ),
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/bin/scontrol",
            "show",
            "hostnames",
            "fixture-targets",
        ),
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/bin/scontrol",
            "--version",
        ),
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/bin/scontrol",
            "show",
            "nodes",
            "fixture-targets",
            "--json",
        ),
        (
            "/usr/sbin/runuser",
            "--user",
            "loom_capacity_executor",
            "--",
            "/usr/sbin/ip",
            "-json",
            "route",
            "get",
            "192.168.50.103",
        ),
    ]


@pytest.mark.parametrize(
    ("hostname", "runner_arguments"),
    (
        ("wrong-controller", {}),
        ("TRT-EAI-OLDLAB-1", {"slurm_cluster": "wrong-cluster"}),
        ("TRT-EAI-OLDLAB-1", {"slurm_partition": "foreign"}),
        (
            "TRT-EAI-OLDLAB-1",
            {"slurm_nodes": ("trt-eai-oldlab-2", "trt-eai-oldlab-3")},
        ),
        ("TRT-EAI-OLDLAB-1", {"slurm_version": (24, 1, 0)}),
        ("TRT-EAI-OLDLAB-1", {"data_parser": "foreign-parser"}),
        ("TRT-EAI-OLDLAB-1", {"manager_route_source": "8.8.8.8"}),
        ("TRT-EAI-OLDLAB-1", {"slurm_metadata_cluster": "foreign-cluster"}),
        (
            "TRT-EAI-OLDLAB-1",
            {
                "slurm_metadata_nodes": (
                    "trt-eai-oldlab-2",
                    "trt-eai-oldlab-3",
                    "trt-eai-oldlab-4",
                    "trt-eai-oldlab-5",
                )
            },
        ),
        ("TRT-EAI-OLDLAB-1", {"slurm_metadata_version": (23, 11, 5)}),
    ),
)
def test_controller_discovery_rejects_controller_authority_drift_without_mutation(
    tmp_path: Path,
    hostname: str,
    runner_arguments: dict[str, object],
) -> None:
    """Catch publishing evidence after controller identity or inventory drift."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path, **runner_arguments)  # type: ignore[arg-type]
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname=hostname,
        effective_uid=0,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="oldlab",
        transport_authority_sha256="8" * 64,
    )

    with pytest.raises(CapacityExecutorInstallError, match="discovery"):
        installer.discover_controller(request)

    assert not any(
        Path(call[0]).name in {"docker", "groupadd", "useradd"}
        or (Path(call[0]).name == "systemctl" and call[1] in {"start", "enable"})
        for call in runner.calls
    )


def test_controller_discovery_requires_an_existing_safe_service_identity(
    tmp_path: Path,
) -> None:
    """Catch discovery creating the executor identity or observing as root instead."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="identity authority"):
        installer.discover_controller(
            ControllerDiscoveryRequest(
                schema_version=1,
                pool_id="oldlab",
                transport_authority_sha256="8" * 64,
            )
        )
    assert not any(Path(call[0]).name in {"groupadd", "useradd"} for call in runner.calls)


def test_oldlab_discovery_requires_executor_membership_in_partition_admission_group(
    tmp_path: Path,
) -> None:
    """Catch claiming OLDLAB submission authority without the allowed Unix group."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path, rollout_group_member=False)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="identity authority"):
        installer.discover_controller(
            ControllerDiscoveryRequest(
                schema_version=1,
                pool_id="oldlab",
                transport_authority_sha256="8" * 64,
            )
        )
    assert not any(Path(call[0]).name == "usermod" for call in runner.calls)


def test_oldlab_discovery_rejects_partition_without_exact_group_admission(
    tmp_path: Path,
) -> None:
    """Catch a membership proof that is not enforced by the target partition."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path, partition_allow_groups="ALL")
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="Slurm admission"):
        installer.discover_controller(
            ControllerDiscoveryRequest(
                schema_version=1,
                pool_id="oldlab",
                transport_authority_sha256="8" * 64,
            )
        )


def test_gb10_discovery_accepts_exact_executor_account_partition_and_qos(
    tmp_path: Path,
) -> None:
    """Catch rejecting the dedicated executor's exact GB10 Slurm admission."""
    _controller_request(tmp_path)
    targets = tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16)))
    association = (
        "trt-gb10|loom-staging|loom_capacity_executor|loom-staging|loom-staging|loom-staging|"
    )
    runner = FakeHostRunner(
        tmp_path,
        image_architecture="arm64",
        slurm_cluster="trt-gb10",
        slurm_nodes=targets,
        slurm_metadata_cluster="trt-gb10",
        slurm_metadata_nodes=targets,
        manager_route_source="192.168.60.11",
        partition_allow_groups="ALL",
        partition_allow_accounts="loom-staging",
        partition_allow_qos="loom-staging",
        slurm_association=association,
    )
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="aarch64",
        hostname="gx10-01c7",
        effective_uid=0,
    )

    evidence = installer.discover_controller(
        ControllerDiscoveryRequest(
            schema_version=1,
            pool_id="gb10",
            transport_authority_sha256="8" * 64,
        )
    )

    assert evidence.target_nodes == targets
    assert evidence.manager_client_cidr == "192.168.60.11/32"
    assert evidence.job_visibility_evidence_sha256 == (
        controller_job_visibility_evidence_sha256(
            pool_id="gb10",
            partition_fields={
                "AllowAccounts": "loom-staging",
                "AllowQos": "loom-staging",
            },
            association_fields=tuple(association.removesuffix("|").split("|")),
        )
    )
    assert not any(Path(call[0]).name == "usermod" for call in runner.calls)


def test_gb10_discovery_requires_exact_executor_account_partition_and_qos(
    tmp_path: Path,
) -> None:
    """Catch reusing the legacy user's association as executor admission evidence."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(
        tmp_path,
        image_architecture="arm64",
        slurm_cluster="trt-gb10",
        slurm_nodes=tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
        slurm_metadata_cluster="trt-gb10",
        slurm_metadata_nodes=tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
        manager_route_source="192.168.60.11",
        partition_allow_groups="ALL",
        partition_allow_accounts="loom-staging",
        partition_allow_qos="loom-staging",
        slurm_association=(
            "trt-gb10|loom-staging|loom-rollout|loom-staging|loom-staging|loom-staging|"
        ),
    )
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="aarch64",
        hostname="gx10-01c7",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="Slurm admission"):
        installer.discover_controller(
            ControllerDiscoveryRequest(
                schema_version=1,
                pool_id="gb10",
                transport_authority_sha256="8" * 64,
            )
        )


def test_controller_discovery_wire_operation_is_strict_and_canonical(tmp_path: Path) -> None:
    """Catch bypassing the typed discovery request at the root helper boundary."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="oldlab",
        transport_authority_sha256="8" * 64,
    )

    encoded = _controller_discovery_operation(installer, request.to_bytes())
    assert ControllerDiscoveryEvidence.from_bytes(encoded).pool_id == "oldlab"
    with pytest.raises(CapacityExecutorInstallError, match="discovery"):
        _controller_discovery_operation(installer, request.to_bytes() + b" ")


def test_controller_discovery_rejects_untyped_request_before_field_access(tmp_path: Path) -> None:
    """Catch an AttributeError bypassing the installer's closed error contract."""
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=FakeHostRunner(tmp_path),
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="discovery"):
        installer.discover_controller(object())  # type: ignore[arg-type]


def test_controller_discovery_cli_dispatches_bounded_stdin_without_install_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catch omitting the read-only discovery operation from the installed CLI."""
    _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="oldlab",
        transport_authority_sha256="8" * 64,
    )
    stdin = io.BytesIO(request.to_bytes())
    stdout = io.BytesIO()
    monkeypatch.setattr(installer_module, "_validate_host_root", lambda _root: None)
    monkeypatch.setattr(installer_module, "ControllerInstaller", lambda **_kwargs: installer)
    monkeypatch.setattr(installer_module.sys, "stdin", type("_Input", (), {"buffer": stdin})())
    monkeypatch.setattr(installer_module.sys, "stdout", type("_Output", (), {"buffer": stdout})())

    assert installer_module.main(["--operation", "discover-controller"]) == 0
    assert ControllerDiscoveryEvidence.from_bytes(stdout.getvalue()).pool_id == "oldlab"


@pytest.mark.parametrize(
    ("hostname", "cluster", "nodes"),
    (
        ("wrong-controller", "trt-oldlab", tuple(f"trt-eai-oldlab-{i}" for i in range(3, 6))),
        ("TRT-EAI-OLDLAB-1", "wrong-cluster", tuple(f"trt-eai-oldlab-{i}" for i in range(3, 6))),
        ("TRT-EAI-OLDLAB-1", "trt-oldlab", ("trt-eai-oldlab-2",)),
    ),
)
def test_controller_prerequisite_rejects_host_or_slurm_drift_before_install(
    tmp_path: Path,
    hostname: str,
    cluster: str,
    nodes: tuple[str, ...],
) -> None:
    request = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path, slurm_cluster=cluster, slurm_nodes=nodes)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname=hostname,
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match="authority"):
        installer.converge_prerequisite(request)

    assert not any(call[0].endswith("docker") and call[1] == "pull" for call in runner.calls)
    assert not (tmp_path / "opt/loom-capacity-executor").exists()


def test_controller_prerequisite_wire_operation_is_canonical_and_bounded(
    tmp_path: Path,
) -> None:
    request = _controller_request(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    assert (
        _controller_prerequisite_operation(installer, "observe-prerequisite", request.to_bytes())
        == b"null\n"
    )
    encoded = _controller_prerequisite_operation(
        installer, "converge-prerequisite", request.to_bytes()
    )

    from loom_cli.rollout.operator.protected_controller_prerequisite_component import (
        ControllerPrerequisiteEvidence,
    )

    assert ControllerPrerequisiteEvidence.from_bytes(encoded).pool_id == "oldlab"
    with pytest.raises(CapacityExecutorInstallError, match="request"):
        _controller_prerequisite_operation(
            installer,
            "observe-prerequisite",
            request.to_bytes() + b" ",
        )


def test_controller_credential_operation_recovers_partial_private_publication(
    tmp_path: Path,
) -> None:
    payload = _pool_credential_payload()
    runtime_root = _prepare_runtime_root(tmp_path)
    target = runtime_root / "oldlab"
    target.mkdir(mode=0o700)
    first = target / "bearer-token"
    first.write_bytes(payload.files[first.name])
    first.chmod(0o600)
    original_inode = first.stat().st_ino
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    assert installer.observe_credential(payload) is None
    evidence = installer.publish_credential(payload)

    assert first.stat().st_ino == original_inode
    assert set(path.name for path in target.iterdir()) == set(payload.files)
    assert installer.observe_credential(payload) == evidence
    assert evidence.pool_id == "oldlab"
    assert evidence.uid == os.geteuid()
    assert evidence.gid == os.getegid()
    assert not any(Path(call[0]).name in {"groupadd", "useradd"} for call in runner.calls)
    assert not any(
        Path(call[0]).name == "systemctl" and call[1] in {"enable", "start"}
        for call in runner.calls
    )


@pytest.mark.parametrize(
    ("pool_id", "hostname", "active_units", "enabled_units"),
    (
        ("oldlab", "wrong-controller", (), ()),
        ("gb10", "TRT-EAI-OLDLAB-1", (), ()),
        ("oldlab", "TRT-EAI-OLDLAB-1", (_UNITS[0],), ()),
        ("oldlab", "TRT-EAI-OLDLAB-1", (), (_UNITS[-1],)),
    ),
)
def test_controller_credential_operation_rejects_wrong_host_pool_or_noninert_units(
    tmp_path: Path,
    pool_id: str,
    hostname: str,
    active_units: tuple[str, ...],
    enabled_units: tuple[str, ...],
) -> None:
    payload = _pool_credential_payload(pool_id)
    _prepare_runtime_root(tmp_path)
    runner = FakeHostRunner(
        tmp_path,
        active_units=active_units,
        enabled_units=enabled_units,
    )
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        hostname=hostname,
        effective_uid=0,
    )

    with pytest.raises(CapacityExecutorInstallError, match=r"credential|units"):
        installer.publish_credential(payload)

    assert not (tmp_path / "run/loom-capacity-executor" / pool_id).exists()
    assert not any(Path(call[0]).name in {"groupadd", "useradd"} for call in runner.calls)


def test_controller_credential_wire_operation_is_canonical_bounded_and_secret_safe(
    tmp_path: Path,
) -> None:
    payload = _pool_credential_payload()
    _prepare_runtime_root(tmp_path)
    runner = FakeHostRunner(tmp_path)
    runner.group_present = True
    runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )

    assert (
        _pool_credential_operation(installer, "observe-credential", payload.to_bytes()) == b"null\n"
    )
    encoded = _pool_credential_operation(installer, "publish-credential", payload.to_bytes())

    evidence = PoolExecutionCredentialEvidence.from_bytes(encoded)
    assert evidence.pool_id == "oldlab"
    with pytest.raises(CapacityExecutorInstallError, match="credential") as malformed:
        _pool_credential_operation(
            installer,
            "observe-credential",
            payload.to_bytes() + b" ",
        )
    message = str(malformed.value)
    assert all(value.decode("ascii") not in message for value in payload.files.values())
