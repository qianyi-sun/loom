"""Process evidence must not confuse a disabled timer with retired work."""

import hashlib
import subprocess

import pytest

from loom_cli.rollout.operator import protected_legacy_controller_process as module


def _properties():
    return {"Id": "loom-autoscaler-oldlab-staging.service", "LoadState": "loaded",
        "ActiveState": "inactive", "SubState": "dead", "MainPID": "0", "ControlPID": "0",
        "ControlGroup": "", "Job": "", "KillMode": "control-group", "Delegate": "no",
        "ExecMainStartTimestampMonotonic": "12345", "InvocationID": "",
        "NeedDaemonReload": "no", "Transient": "no", "DropInPaths": "",
        "FragmentPath": "/var/lib/loom-staging-rollout/.config/systemd/user/loom-autoscaler-oldlab-staging.service"}


@pytest.mark.parametrize("key,value", [("ActiveState", "active"), ("SubState", "running"),
    ("MainPID", "123"), ("ControlPID", "456"), ("Job", "42"), ("Delegate", "yes"),
    ("KillMode", "process"), ("Transient", "yes"), ("DropInPaths", "/tmp/foreign.conf"),
    ("NeedDaemonReload", "yes")])
def test_process_retirement_refuses_live_or_changed_authority(key, value):
    assert module.legacy_service_processes_retired({**_properties(), key: value}) is False


def test_empty_manager_pids_do_not_hide_remaining_cgroup_children(monkeypatch):
    seen = []
    def populated(group):
        seen.append(group)
        return False
    monkeypatch.setattr(module, "controller_cgroup_empty", populated)
    assert module.legacy_service_processes_retired({**_properties(), "ControlGroup": "/owned/group"}) is False
    assert seen == ["/owned/group"]
    assert module.legacy_service_processes_retired(_properties()) is True


@pytest.mark.parametrize("group", ["/", "relative", "/a/../b", "/a//b", "/a\n"])
def test_cgroup_path_refuses_unbound_or_ambiguous_locations(group):
    with pytest.raises(ValueError):
        module.controller_cgroup_empty(group)


@pytest.mark.parametrize("drift", [None, "process", "source", "duplicate", "failed-command", "fragment"])
def test_process_capture_binds_stable_source_and_actual_manager_readbacks(monkeypatch, drift):
    calls = []
    reads = []
    def run(argv, **kwargs):
        calls.append(argv)
        assert argv[:4] == ["systemctl", "--user", "show", _properties()["Id"]]
        properties = _properties()
        if drift == "process" and len(calls) == 2:
            properties["MainPID"] = "999"
        if drift == "fragment":
            properties["FragmentPath"] = "/tmp/foreign.service"
        output = "\n".join(f"{key}={value}" for key, value in properties.items()) + "\n"
        if drift == "duplicate":
            output += "MainPID=0\n"
        return subprocess.CompletedProcess(argv, 1 if drift == "failed-command" else 0, output.encode(), b"")
    def source(unit):
        reads.append(unit)
        return b"changed" if drift == "source" and len(reads) == 2 else b"owned-unit"
    monkeypatch.setattr(module.subprocess, "run", run)
    arguments = dict(pool="oldlab", expected_unit_sha256=hashlib.sha256(b"owned-unit").hexdigest(),
        read_unit=source, environment={"PATH": "/usr/bin:/bin"})
    if drift is None:
        observed = module.observe_legacy_controller_processes(**arguments)
        assert observed["processes_retired"] is True
        assert observed["properties"] == _properties()
        assert len(observed["evidence_sha256"]) == 64
        assert len(reads) == len(calls) == 2
    else:
        with pytest.raises(RuntimeError):
            module.observe_legacy_controller_processes(**arguments)
