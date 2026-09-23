"""Shared HTTPS must not replace the standalone allocation or TLS state."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import build_platform, validate_environment
from tests.unit.test_nebius_environment_render import rendered
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def test_opt_in_routing_preserves_public_allocation_and_original_tls(platform_inputs):
    config, candidate, profile = platform_inputs
    original = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    enabled = build_platform({**config, "shared_ingress_enabled": True}, candidate, profile,
                             {}, repo_root=ROOT)
    public = enabled["70-public.yaml"][0]
    assert public["metadata"] == original["70-public.yaml"][0]["metadata"]
    assert public["spec"]["type"] == "LoadBalancer"
    assert public["spec"]["ports"] == original["70-public.yaml"][0]["spec"]["ports"]
    assert public["spec"]["selector"] == {"app": "loom-shared-ingress"}
    # Caddy and the RWO TLS PVC survive; only the config revision changes.
    old_web = next(d for d in original["40-services.yaml"] if d["kind"] == "Deployment"
                   and d["metadata"]["name"] == "loom-web")
    new_web = next(d for d in enabled["40-services.yaml"] if d["kind"] == "Deployment"
                   and d["metadata"]["name"] == "loom-web")
    assert new_web["spec"]["template"]["spec"] == old_web["spec"]["template"]["spec"]
    origin = next(d for d in enabled["40-services.yaml"]
                  if d["metadata"]["name"] == "loom-web-origin")
    assert origin["spec"]["selector"] == {"app": "loom-web"}
    assert origin["spec"].get("type", "ClusterIP") == "ClusterIP"
    assert origin["spec"]["ports"][0]["targetPort"] == 8443


@pytest.mark.parametrize("value", ["true", 1, None, {}, []])
def test_shared_routing_mode_rejects_non_booleans(platform_inputs, value):
    with pytest.raises(ValueError, match="shared_ingress_enabled"):
        validate_environment({**platform_inputs[0], "shared_ingress_enabled": value})


def test_children_do_not_inherit_shared_entrypoint_control(platform_inputs):
    config, candidate, profile = deepcopy(platform_inputs)
    config["shared_ingress_enabled"] = True
    child = rendered((config, candidate, profile))
    assert "shared_ingress_enabled" not in child.config
    assert not any(d["metadata"]["name"] == "loom-web-origin"
                   for docs in child.files.values() for d in docs)
    cm = next(d for docs in child.files.values() for d in docs if d["kind"] == "ConfigMap")
    assert "shared_ingress_enabled" not in json.loads(cm["data"]["environment.json"])


def test_managed_persisted_input_rejects_shared_routing_option(platform_inputs):
    child = rendered(platform_inputs)
    with pytest.raises(ValueError, match="shared_ingress_enabled"):
        validate_environment({**child.config, "shared_ingress_enabled": True})
