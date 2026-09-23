"""Legacy SNI proof uses a fixed Pod forwarder and cleans it up on all outcomes."""
from __future__ import annotations

import copy
import json
import subprocess
import sys
from uuid import uuid4

import pytest
from tests.ops.test_nebius_ingress_live_binding import live as live
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_ingress_probe import endpoint as endpoint
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize("drift", [None, "before", "after"])
def test_legacy_forwarder_is_bound_to_current_pod_and_always_cleaned(live, endpoint, monkeypatch, drift):
    from scripts.ops.nebius_ingress_operation import OperationError

    api, config = live
    address, _state, paths, _fingerprint = endpoint
    config["public_host"] = "legacy.example.test"
    api.config["data"]["environment.json"] = json.dumps(config)
    pod = {"metadata": {"namespace": api.binding.namespace, "name": "ingress-current", "uid": str(uuid4()),
                        "resourceVersion": "1", "labels": {"app": "loom-shared-ingress"}}}
    reads = []

    def read(namespace, name):
        assert (namespace, name) == (api.binding.namespace, "ingress-current")
        reads.append(name)
        result = copy.deepcopy(pod)
        if drift == "before" or (drift == "after" and len(reads) > 1):
            result["metadata"]["uid"] = str(uuid4())
        return result

    monkeypatch.setattr(api, "get_pod", read)
    processes = []
    original = subprocess.Popen

    def forward(argv, **kwargs):
        assert argv[len(api.prefix):] == ["port-forward", "--address=127.0.0.1", "-n", api.binding.namespace,
                                         "pod/ingress-current", ":8443"]
        assert not kwargs.get("start_new_session", False)
        process = original([sys.executable, "-c", "import time; print('Forwarding from 127.0.0.1:"
                            + str(address["port"]) + " -> 8443', flush=True); time.sleep(60)"], **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(subprocess, "Popen", forward)
    if drift is None:
        api.probe_legacy_pod(pod)
        assert paths == [("/api/v1/health", "legacy.example.test"), ("/loom-frontend-config.json", "legacy.example.test")]
    else:
        with pytest.raises(OperationError):
            api.probe_legacy_pod(pod)
    assert len(processes) == (0 if drift == "before" else 1)
    assert all(process.returncode is not None for process in processes)
