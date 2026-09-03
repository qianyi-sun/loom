from __future__ import annotations

from pathlib import Path, PurePosixPath
from uuid import UUID

import pytest

from loom_task_image_builder_guard.bpf import (
    BpfAttachment,
    Endpoint,
    NetworkPolicy,
    ScopeNetworkPolicy,
    TrafficLimits,
)
from loom_task_image_builder_guard.containment import (
    BpftoolDeviceProbe,
    CgroupNode,
    ContainmentManager,
    DeviceProgram,
    GuardPolicy,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import BatchCgroup
from loom_task_image_builder_guard.models import CommandIdentity, IoLimit
from loom_task_image_builder_guard.slurm import CommandResult

GRANT = UUID("11111111-1111-4111-8111-111111111111")
PID = 4242
BATCH_AUTHORITY = "/sys/fs/cgroup/slurm/job_123/step_batch/user/task_0"


def _limits(value: int) -> TrafficLimits:
    return TrafficLimits(value, value, value, value, value, value, value)


def _network() -> NetworkPolicy:
    trusted = Endpoint("203.0.113.10", 443, "tcp")
    build = Endpoint("198.51.100.20", 443, "tcp")
    return NetworkPolicy(
        containment_policy_sha256="c" * 64,
        resource_profile_sha256="d" * 64,
        bpf_program_sha256="e" * 64,
        bpf_map_schema_sha256="f" * 64,
        scopes=(
            ScopeNetworkPolicy("root", (build, trusted), (), _limits(300)),
            ScopeNetworkPolicy("trusted-service", (trusted,), (), _limits(100)),
            ScopeNetworkPolicy("build-egress", (build,), (), _limits(200)),
        ),
    )


def _policy() -> GuardPolicy:
    return GuardPolicy(
        cpus=8,
        memory_mib=32768,
        pids_max=512,
        io_limits=(IoLimit("8:1", 104857600, 52428800, 20000, 10000),),
        network=_network(),
    )


class _Peer:
    def __init__(self, events: list[tuple[object, ...]]) -> None:
        self.pid = PID
        self.events = events
        self.adopted = False

    def assert_unchanged(self) -> None:
        self.events.append(("peer_assert", self.adopted))

    def adopt_trusted_service_cgroup(self) -> None:
        self.events.append(("peer_adopt",))
        self.adopted = True


class _Filesystem:
    def __init__(
        self,
        tmp_path: Path,
        events: list[tuple[object, ...]],
        *,
        memory_max: str = str(32768 * 1024 * 1024),
        pids_readback: str | None = None,
    ) -> None:
        self.events = events
        self.next_fd = 30
        self.next_inode = 699
        self.nodes: dict[int, CgroupNode] = {}
        self.files: dict[int, dict[str, str]] = {}
        self.pids_readback = pids_readback
        batch_path = tmp_path / "cgroup/slurm/job_123/step_batch/user/task_0"
        batch_path.mkdir(parents=True)
        self.batch = self._new_node(batch_path, BATCH_AUTHORITY, inode=600)
        self.files[self.batch.descriptor] = {
            "cgroup.type": "domain\n",
            "cgroup.procs": f"{PID}\n",
            "cgroup.controllers": "cpu cpuset io memory pids\n",
            "cgroup.subtree_control": "io pids\n",
            "cpuset.cpus.effective": "0-7\n",
            "cpu.max": "800000 100000\n",
            "memory.max": f"{memory_max}\n",
            "memory.swap.max": "0\n",
        }

    def _new_node(self, path: Path, authority_path: str, *, inode: int | None = None) -> CgroupNode:
        self.next_fd += 1
        self.next_inode += 1
        node = CgroupNode(
            path=path,
            authority_path=authority_path,
            descriptor=self.next_fd,
            device=55,
            inode=self.next_inode if inode is None else inode,
        )
        self.nodes[node.descriptor] = node
        return node

    def open_batch(self, batch: BatchCgroup) -> CgroupNode:
        self.events.append(("open_batch", batch.path, batch.inode))
        assert batch.inode == self.batch.inode
        return self.batch

    def create_child(self, parent: CgroupNode, name: str) -> CgroupNode:
        self.events.append(("create", parent.authority_path, name))
        assert name in {"loom-builder", "trusted-service", "build-egress"}
        node = self._new_node(parent.path / name, f"{parent.authority_path}/{name}")
        self.files[node.descriptor] = {
            "cgroup.type": "domain\n",
            "cgroup.procs": "",
            "cgroup.controllers": "io pids\n",
            "cgroup.subtree_control": "\n",
            "pids.max": "max\n",
            "io.max": "\n",
        }
        return node

    def assert_stable(self, node: CgroupNode) -> None:
        self.events.append(("stable", node.authority_path))

    def read(self, node: CgroupNode, name: str) -> str:
        self.events.append(("read", node.authority_path, name))
        if name == "pids.max" and self.pids_readback is not None:
            return self.pids_readback
        return self.files[node.descriptor][name]

    def write(self, node: CgroupNode, name: str, value: str) -> None:
        self.events.append(("write", node.authority_path, name, value))
        assert node.authority_path.startswith(f"{BATCH_AUTHORITY}/loom-builder")
        if name == "cgroup.procs":
            assert node.authority_path.endswith("/trusted-service")
            assert value == str(PID)
            self.files[self.batch.descriptor]["cgroup.procs"] = ""
            self.files[node.descriptor]["cgroup.procs"] = f"{PID}\n"
        elif name == "cgroup.subtree_control":
            assert value == "+io +pids"
            self.files[node.descriptor][name] = "io pids\n"
        elif name == "io.max":
            self.files[node.descriptor][name] = value + "\n"
        else:
            self.files[node.descriptor][name] = value + "\n"

    def close(self, descriptor: int) -> None:
        self.events.append(("close", descriptor))


class _Bpf:
    def __init__(self, events: list[tuple[object, ...]], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def attach(
        self,
        tree: object,
        policy: NetworkPolicy,
        grant_id: UUID,
    ) -> BpfAttachment:
        self.events.append(("bpf_attach", grant_id, policy.containment_policy_sha256))
        if self.fail:
            raise GuardError("bpf_link_readback_invalid")
        targets = tree.bpf_scope_targets()  # type: ignore[attr-defined]
        assert tuple(item.cgroup_id for item in targets) == (701, 702, 703)
        return BpfAttachment(
            pin_path=Path(f"/sys/fs/bpf/loom/{grant_id}"),
            link_ids=tuple(range(101, 125)),
            program_ids=tuple(range(201, 225)),
            map_ids=tuple(range(301, 319)),
        )


class _Devices:
    def __init__(self, events: list[tuple[object, ...]], *, valid: bool = True) -> None:
        self.events = events
        self.valid = valid

    def inspect(self, batch: CgroupNode) -> tuple[DeviceProgram, ...]:
        self.events.append(("device_probe", batch.authority_path))
        if not self.valid:
            return ()
        return (DeviceProgram(19, "cgroup_device", "multi", "loom_devices"),)


def _manager(
    tmp_path: Path,
    *,
    memory_max: str = str(32768 * 1024 * 1024),
    pids_readback: str | None = None,
    bpf_fail: bool = False,
    devices_valid: bool = True,
) -> tuple[ContainmentManager, _Filesystem, _Peer, list[tuple[object, ...]]]:
    events: list[tuple[object, ...]] = []
    filesystem = _Filesystem(
        tmp_path,
        events,
        memory_max=memory_max,
        pids_readback=pids_readback,
    )
    peer = _Peer(events)
    manager = ContainmentManager(
        filesystem=filesystem,
        bpf_loader=_Bpf(events, fail=bpf_fail),
        device_probe=_Devices(events, valid=devices_valid),
    )
    return manager, filesystem, peer, events


def _batch(filesystem: _Filesystem) -> BatchCgroup:
    return BatchCgroup(
        path=filesystem.batch.path,
        relative_path=PurePosixPath("/slurm/job_123/step_batch/user/task_0"),
        inode=filesystem.batch.inode,
        peer_pid=PID,
    )


def test_prepare_attaches_while_empty_then_moves_only_peer_and_reads_every_limit(
    tmp_path: Path,
) -> None:
    manager, filesystem, peer, events = _manager(tmp_path)

    attachment = manager.prepare(_batch(filesystem), peer, _policy(), GRANT)

    attach_index = next(index for index, event in enumerate(events) if event[0] == "bpf_attach")
    move_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("write", f"{BATCH_AUTHORITY}/loom-builder/trusted-service", "cgroup.procs")
    )
    delegation_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("write", f"{BATCH_AUTHORITY}/loom-builder", "cgroup.subtree_control")
    )
    assert attach_index < move_index < delegation_index
    assert peer.adopted is True
    assert attachment.containment_root == f"{BATCH_AUTHORITY}/loom-builder"
    assert attachment.trusted_service_cgroup.endswith("/trusted-service")
    assert attachment.build_egress_cgroup.endswith("/build-egress")
    assert attachment.link_ids == tuple(range(101, 125))
    assert len(attachment.probe_sha256) == 64
    assert isinstance(attachment.probe, bytes)
    writes = [event for event in events if event[0] == "write"]
    assert ("write", f"{BATCH_AUTHORITY}/loom-builder", "pids.max", "512") in writes
    assert (
        "write",
        f"{BATCH_AUTHORITY}/loom-builder",
        "io.max",
        "8:1 rbps=104857600 wbps=52428800 riops=20000 wiops=10000",
    ) in writes
    assert all(event[1].startswith(f"{BATCH_AUTHORITY}/loom-builder") for event in writes)


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"memory_max": "max"}, "containment_inherited_resources_invalid"),
        ({"devices_valid": False}, "containment_device_authority_invalid"),
        ({"pids_readback": "511\n"}, "containment_limit_readback_invalid"),
    ],
)
def test_prepare_rejects_unbounded_inheritance_missing_device_or_changed_readback(
    tmp_path: Path,
    changes: dict[str, object],
    code: str,
) -> None:
    manager, filesystem, peer, events = _manager(tmp_path, **changes)  # type: ignore[arg-type]

    with pytest.raises(GuardError) as caught:
        manager.prepare(_batch(filesystem), peer, _policy(), GRANT)

    assert caught.value.code == code
    if code != "containment_limit_readback_invalid":
        assert not any(event[0] == "bpf_attach" for event in events)


def test_bpf_failure_never_moves_peer_and_preserves_fail_closed_error(tmp_path: Path) -> None:
    manager, filesystem, peer, events = _manager(tmp_path, bpf_fail=True)

    with pytest.raises(GuardError) as caught:
        manager.prepare(_batch(filesystem), peer, _policy(), GRANT)

    assert caught.value.code == "bpf_link_readback_invalid"
    assert peer.adopted is False
    assert not any(event[0] == "write" and event[2] == "cgroup.procs" for event in events)


class _DeviceRunner:
    def __init__(self, output: str) -> None:
        self.output = output
        self.calls: list[tuple[Path, tuple[str, ...]]] = []

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append((command.path, argv))
        return CommandResult(0, self.output, "")


def test_bpftool_device_probe_accepts_only_complete_direct_attachment_readback(
    tmp_path: Path,
) -> None:
    events: list[tuple[object, ...]] = []
    filesystem = _Filesystem(tmp_path, events)
    output = (
        '[{"id":19,"attach_type":"cgroup_device","attach_flags":"multi",'
        '"name":"loom_devices","attach_btf_name":"","attach_btf_obj_id":0,'
        '"attach_btf_id":0}]\n'
    )
    runner = _DeviceRunner(output)
    command = CommandIdentity(Path("/opt/release/bpftool"), "a" * 64)

    programs = BpftoolDeviceProbe(runner, command).inspect(filesystem.batch)

    assert programs == (DeviceProgram(19, "cgroup_device", "multi", "loom_devices"),)
    assert runner.calls == [
        (
            command.path,
            ("-j", "cgroup", "show", str(filesystem.batch.path)),
        )
    ]

    changed = _DeviceRunner(output.replace('"attach_btf_id":0', '"attach_btf_id":1'))
    with pytest.raises(GuardError) as caught:
        BpftoolDeviceProbe(changed, command).inspect(filesystem.batch)
    assert caught.value.code == "containment_device_authority_invalid"
