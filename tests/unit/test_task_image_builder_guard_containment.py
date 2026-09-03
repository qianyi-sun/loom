from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
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
    CgroupFilesystem,
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
        device_program_tags=("0123456789abcdef",),
        pids_max=512,
        io_limits=(IoLimit("8:1", 104857600, 52428800, 20000, 10000),),
        network=_network(),
    )


class _Peer:
    def __init__(self, events: list[tuple[object, ...]]) -> None:
        self.pid = PID
        self.uid = 993
        self.gid = 980
        self.events = events
        self.adopted = False
        self.held = False

    def assert_unchanged(self) -> None:
        self.events.append(("peer_assert", self.adopted))

    def adopt_trusted_service_cgroup(self) -> None:
        self.events.append(("peer_adopt",))
        self.adopted = True

    @contextmanager
    def containment_hold(self) -> Iterator[None]:
        self.events.append(("peer_hold",))
        self.held = True
        try:
            yield
        finally:
            self.held = False
            self.events.append(("peer_resume",))


class _Filesystem:
    def __init__(
        self,
        tmp_path: Path,
        events: list[tuple[object, ...]],
        *,
        memory_max: str = str(32768 * 1024 * 1024),
        pids_readback: str | None = None,
        hidden_descendants: int = 0,
    ) -> None:
        self.events = events
        self.next_fd = 30
        self.next_inode = 699
        self.nodes: dict[int, CgroupNode] = {}
        self.files: dict[int, dict[str, str]] = {}
        self.pids_readback = pids_readback
        self.hidden_descendants = hidden_descendants
        batch_path = tmp_path / "cgroup/slurm/job_123/step_batch/user/task_0"
        batch_path.mkdir(parents=True)
        self.batch = self._new_node(batch_path, BATCH_AUTHORITY, inode=600)
        self.files[self.batch.descriptor] = {
            "cgroup.type": "domain\n",
            "cgroup.procs": f"{PID}\n",
            "cgroup.controllers": "cpu cpuset io memory pids\n",
            "cgroup.subtree_control": "\n",
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
        if name == "cgroup.stat":
            descendants = sum(
                other.authority_path.startswith(f"{node.authority_path}/")
                for other in self.nodes.values()
            )
            if node == self.batch:
                descendants += self.hidden_descendants
            return f"nr_descendants {descendants}\nnr_dying_descendants 0\n"
        if name == "pids.max" and self.pids_readback is not None:
            return self.pids_readback
        return self.files[node.descriptor][name]

    def write(self, node: CgroupNode, name: str, value: str) -> None:
        self.events.append(("write", node.authority_path, name, value))
        assert node.authority_path.startswith(f"{BATCH_AUTHORITY}/loom-builder") or (
            node.authority_path == BATCH_AUTHORITY and name == "cgroup.subtree_control"
        )
        if name == "cgroup.procs":
            assert node.authority_path.endswith("/trusted-service")
            assert value == str(PID)
            self.files[self.batch.descriptor]["cgroup.procs"] = ""
            self.files[node.descriptor]["cgroup.procs"] = f"{PID}\n"
        elif name == "cgroup.subtree_control":
            assert value == "+io +pids"
            assert not self.files[node.descriptor]["cgroup.procs"].strip()
            self.files[node.descriptor][name] = "io pids\n"
        elif name == "io.max":
            self.files[node.descriptor][name] = value + "\n"
        else:
            self.files[node.descriptor][name] = value + "\n"

    def delegate_process_migration(
        self,
        common_ancestor: CgroupNode,
        destination: CgroupNode,
        *,
        uid: int,
        gid: int,
    ) -> dict[str, object]:
        self.events.append(
            (
                "delegate_process_migration",
                common_ancestor.authority_path,
                destination.authority_path,
                uid,
                gid,
            )
        )
        return {
            "common_ancestor": {
                "path": f"{common_ancestor.authority_path}/cgroup.procs",
                "uid": uid,
                "gid": gid,
                "mode": 0o644,
            },
            "destination": {
                "path": f"{destination.authority_path}/cgroup.procs",
                "uid": uid,
                "gid": gid,
                "mode": 0o644,
            },
        }

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
    def __init__(
        self,
        events: list[tuple[object, ...]],
        *,
        valid: bool = True,
        tag: str = "0123456789abcdef",
    ) -> None:
        self.events = events
        self.valid = valid
        self.tag = tag

    def inspect(self, batch: CgroupNode) -> tuple[DeviceProgram, ...]:
        self.events.append(("device_probe", batch.authority_path))
        if not self.valid:
            return ()
        return (
            DeviceProgram(
                19,
                self.tag,
                "cgroup_device",
                "multi",
                "loom_devices",
            ),
        )


def _manager(
    tmp_path: Path,
    *,
    memory_max: str = str(32768 * 1024 * 1024),
    pids_readback: str | None = None,
    bpf_fail: bool = False,
    devices_valid: bool = True,
    device_tag: str = "0123456789abcdef",
    hidden_descendants: int = 0,
) -> tuple[ContainmentManager, _Filesystem, _Peer, list[tuple[object, ...]]]:
    events: list[tuple[object, ...]] = []
    filesystem = _Filesystem(
        tmp_path,
        events,
        memory_max=memory_max,
        pids_readback=pids_readback,
        hidden_descendants=hidden_descendants,
    )
    peer = _Peer(events)
    manager = ContainmentManager(
        filesystem=filesystem,
        bpf_loader=_Bpf(events, fail=bpf_fail),
        device_probe=_Devices(events, valid=devices_valid, tag=device_tag),
    )
    return manager, filesystem, peer, events


def _batch(filesystem: _Filesystem) -> BatchCgroup:
    return BatchCgroup(
        path=filesystem.batch.path,
        relative_path=PurePosixPath("/slurm/job_123/step_batch/user/task_0"),
        inode=filesystem.batch.inode,
        peer_pid=PID,
    )


@pytest.mark.parametrize("unsafe_mode", (0o775, 0o757))
def test_open_batch_rejects_group_or_other_write_authority(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    batch_path = tmp_path / "batch"
    batch_path.mkdir()
    batch_path.chmod(unsafe_mode)
    batch = BatchCgroup(
        path=batch_path,
        relative_path=PurePosixPath("/slurm/job_123/step_batch/user/task_0"),
        inode=batch_path.stat().st_ino,
        peer_pid=PID,
    )

    with pytest.raises(GuardError) as caught:
        CgroupFilesystem(trusted_uid=os.geteuid()).open_batch(batch)

    assert caught.value.code == "containment_batch_invalid"


def test_cgroup_process_delegation_changes_only_exact_migration_files(
    tmp_path: Path,
) -> None:
    root_path = tmp_path / "loom-builder"
    build_path = root_path / "build-egress"
    build_path.mkdir(parents=True)
    for path in (root_path, build_path):
        control = path / "cgroup.procs"
        control.write_bytes(b"")
        control.chmod(0o644)
    root_fd = os.open(root_path, os.O_RDONLY | os.O_DIRECTORY)
    build_fd = os.open(build_path, os.O_RDONLY | os.O_DIRECTORY)
    root_stat = os.fstat(root_fd)
    build_stat = os.fstat(build_fd)
    root = CgroupNode(root_path, "/batch/loom-builder", root_fd, root_stat.st_dev, root_stat.st_ino)
    build = CgroupNode(
        build_path,
        "/batch/loom-builder/build-egress",
        build_fd,
        build_stat.st_dev,
        build_stat.st_ino,
    )
    filesystem = CgroupFilesystem(trusted_uid=os.geteuid())
    try:
        evidence = filesystem.delegate_process_migration(
            root,
            build,
            uid=os.geteuid(),
            gid=os.getegid(),
        )
    finally:
        os.close(build_fd)
        os.close(root_fd)

    assert evidence == {
        "common_ancestor": {
            "path": "/batch/loom-builder/cgroup.procs",
            "uid": os.geteuid(),
            "gid": os.getegid(),
            "mode": 0o644,
        },
        "destination": {
            "path": "/batch/loom-builder/build-egress/cgroup.procs",
            "uid": os.geteuid(),
            "gid": os.getegid(),
            "mode": 0o644,
        },
    }


def test_prepare_attaches_then_moves_peer_before_delegating_domain_controllers(
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
    batch_delegation_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("write", BATCH_AUTHORITY, "cgroup.subtree_control")
    )
    root_delegation_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("write", f"{BATCH_AUTHORITY}/loom-builder", "cgroup.subtree_control")
    )
    hold_index = events.index(("peer_hold",))
    resume_index = events.index(("peer_resume",))
    final_write_index = max(index for index, event in enumerate(events) if event[0] == "write")
    final_peer_assert_index = max(
        index for index, event in enumerate(events) if event[0] == "peer_assert"
    )
    assert (
        hold_index
        < attach_index
        < move_index
        < batch_delegation_index
        < root_delegation_index
        <= final_write_index
        < final_peer_assert_index
        < resume_index
    )
    assert peer.adopted is True
    assert attachment.containment_root == f"{BATCH_AUTHORITY}/loom-builder"
    assert attachment.trusted_service_cgroup.endswith("/trusted-service")
    assert attachment.build_egress_cgroup.endswith("/build-egress")
    assert attachment.link_ids == tuple(range(101, 125))
    assert len(attachment.probe_sha256) == 64
    assert isinstance(attachment.probe, bytes)
    assert json.loads(attachment.probe)["descendants"] == {
        "batch": 3,
        "build_egress": 0,
        "root": 2,
        "trusted_service": 0,
    }
    writes = [event for event in events if event[0] == "write"]
    assert ("write", f"{BATCH_AUTHORITY}/loom-builder", "pids.max", "512") in writes
    assert (
        "write",
        f"{BATCH_AUTHORITY}/loom-builder",
        "io.max",
        "8:1 rbps=104857600 wbps=52428800 riops=20000 wiops=10000",
    ) in writes
    assert all(
        event[1].startswith(f"{BATCH_AUTHORITY}/loom-builder")
        or event[:3] == ("write", BATCH_AUTHORITY, "cgroup.subtree_control")
        for event in writes
    )


def test_prepare_leaves_build_egress_launchable_and_delegates_only_migration_files(
    tmp_path: Path,
) -> None:
    manager, filesystem, peer, events = _manager(tmp_path)

    attachment = manager.prepare(_batch(filesystem), peer, _policy(), GRANT)

    build_path = f"{BATCH_AUTHORITY}/loom-builder/build-egress"
    root_path = f"{BATCH_AUTHORITY}/loom-builder"
    assert (
        "delegate_process_migration",
        root_path,
        build_path,
        peer.uid,
        peer.gid,
    ) in events
    assert not any(
        event[:3] == ("write", build_path, "cgroup.subtree_control")
        for event in events
    )
    assert filesystem.files[attachment.tree.build_egress.descriptor][
        "cgroup.subtree_control"
    ] == "\n"
    assert json.loads(attachment.probe)["process_migration"] == {
        "common_ancestor": {
            "path": f"{root_path}/cgroup.procs",
            "uid": peer.uid,
            "gid": peer.gid,
            "mode": 0o644,
        },
        "destination": {
            "path": f"{build_path}/cgroup.procs",
            "uid": peer.uid,
            "gid": peer.gid,
            "mode": 0o644,
        },
    }


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"memory_max": "max"}, "containment_inherited_resources_invalid"),
        ({"devices_valid": False}, "containment_device_authority_invalid"),
        ({"device_tag": "fedcba9876543210"}, "containment_device_authority_invalid"),
        ({"hidden_descendants": 1}, "containment_descendants_invalid"),
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
    assert events.count(("peer_hold",)) == 1
    assert events.count(("peer_resume",)) == 1


def test_child_creation_failure_closes_every_opened_cgroup_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, filesystem, peer, events = _manager(tmp_path)
    create_child = filesystem.create_child

    def fail_build_child(parent: CgroupNode, name: str) -> CgroupNode:
        if name == "build-egress":
            raise GuardError("containment_child_invalid")
        return create_child(parent, name)

    monkeypatch.setattr(filesystem, "create_child", fail_build_child)

    with pytest.raises(GuardError) as caught:
        manager.prepare(_batch(filesystem), peer, _policy(), GRANT)

    assert caught.value.code == "containment_child_invalid"
    opened = set(filesystem.nodes)
    closed = {event[1] for event in events if event[0] == "close"}
    assert closed == opened


class _DeviceRunner:
    def __init__(self, output: str, *, program_output: str | None = None) -> None:
        self.output = output
        self.program_output = program_output or (
            '{"id":19,"type":"cgroup_device","name":"loom_devices",'
            '"tag":"0123456789abcdef","gpl_compatible":true,'
            '"loaded_at":"2026-09-02T16:00:00+0000","uid":0,'
            '"orphaned":false,"bytes_xlated":64}\n'
        )
        self.calls: list[tuple[Path, tuple[str, ...]]] = []

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append((command.path, argv))
        output = self.program_output if argv[:3] == ("-j", "prog", "show") else self.output
        return CommandResult(0, output, "")


def test_bpftool_device_probe_accepts_only_complete_effective_attachment_readback(
    tmp_path: Path,
) -> None:
    events: list[tuple[object, ...]] = []
    filesystem = _Filesystem(tmp_path, events)
    output = (
        '[{"id":7,"attach_type":"cgroup_inet_ingress",'
        '"name":"ancestor_net","attach_btf_obj_id":0,"attach_btf_id":0},'
        '{"id":19,"attach_type":"cgroup_device",'
        '"name":"loom_devices","attach_btf_obj_id":0,'
        '"attach_btf_id":0}]\n'
    )
    runner = _DeviceRunner(output)
    command = CommandIdentity(Path("/opt/release/bpftool"), "a" * 64)

    programs = BpftoolDeviceProbe(runner, command).inspect(filesystem.batch)

    assert programs == (
        DeviceProgram(
            19,
            "0123456789abcdef",
            "cgroup_device",
            "",
            "loom_devices",
        ),
    )
    assert runner.calls == [
        (
            command.path,
            ("-j", "cgroup", "show", str(filesystem.batch.path), "effective"),
        ),
        (command.path, ("-j", "prog", "show", "id", "19")),
    ]

    changed = _DeviceRunner(
        output.replace('"attach_btf_id":0}]', '"attach_btf_id":1}]')
    )
    with pytest.raises(GuardError) as caught:
        BpftoolDeviceProbe(changed, command).inspect(filesystem.batch)
    assert caught.value.code == "containment_device_authority_invalid"

    broadened = _DeviceRunner(
        output,
        program_output=(
            '{"id":19,"type":"cgroup_device","name":"loom_devices",'
            '"tag":"fedcba9876543210","gpl_compatible":true,'
            '"loaded_at":"2026-09-02T16:00:00+0000","uid":0,'
            '"orphaned":false,"bytes_xlated":64}\n'
        ),
    )
    assert BpftoolDeviceProbe(broadened, command).inspect(filesystem.batch)[0].tag == (
        "fedcba9876543210"
    )

    map_backed = _DeviceRunner(
        output,
        program_output=(
            '{"id":19,"type":"cgroup_device","name":"loom_devices",'
            '"tag":"0123456789abcdef","gpl_compatible":true,'
            '"loaded_at":"2026-09-02T16:00:00+0000","uid":0,'
            '"orphaned":false,"bytes_xlated":64,"map_ids":[31]}\n'
        ),
    )
    with pytest.raises(GuardError) as caught:
        BpftoolDeviceProbe(map_backed, command).inspect(filesystem.batch)
    assert caught.value.code == "containment_device_authority_invalid"
