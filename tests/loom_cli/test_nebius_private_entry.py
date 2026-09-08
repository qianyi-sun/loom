"""Private staging transport renders independently of execution credentials."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from loom_cli.__main__ import main
from loom_cli.cluster_boundary import audit_boundary
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/environments/staging.multinode.cluster.toml"
_NAME = "loom-nebius-private-entry"
_ADDRESS = "10.253.71.2"
_PEER = "10.253.71.1"
_IMAGE = "nginx:1.28-alpine@sha256:" + "a" * 64


def _entry(**overrides: object) -> str:
    values = {
        "enabled": True,
        "node_name": "staging-control-1",
        "wireguard_address": _ADDRESS,
        "peer_address": _PEER,
        "proxy_image": _IMAGE,
    }
    values.update(overrides)
    return "[nebius_private_entry]\n" + "\n".join(
        f"{key} = {json.dumps(value)}" for key, value in values.items()
    )


def _config(tmp_path: Path, extra: str = "", profile: str | None = None) -> Path:
    path = tmp_path / "cluster.toml"
    path.write_text((profile if profile is not None else _PROFILE.read_text()) + "\n" + extra)
    return path


def _render(tmp_path: Path, extra: str | None = None) -> list[dict]:
    path = _config(tmp_path, _entry() if extra is None else extra)
    return [doc for doc in yaml.safe_load_all(render_manifests(load_cluster_config(path))) if doc]


def _object(docs: list[dict], kind: str, name: str = _NAME) -> dict:
    return next(doc for doc in docs if doc["kind"] == kind and doc["metadata"]["name"] == name)


def test_private_entry_is_disabled_by_default_without_route_or_credentials(tmp_path: Path) -> None:
    config = load_cluster_config(_config(tmp_path))
    assert config.nebius_private_entry.enabled is False
    docs = _render(tmp_path, "")
    assert not any(doc["metadata"]["name"] == _NAME for doc in docs)


def test_private_entry_renders_before_execution_attachment_activation(tmp_path: Path) -> None:
    config = load_cluster_config(_config(tmp_path, _entry()))
    assert config.nebius_execution.enabled is False
    docs = _render(tmp_path)
    deployment = _object(docs, "Deployment")
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    pod = deployment["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"kubernetes.io/hostname": "staging-control-1"}
    assert not pod.get("hostNetwork", False)
    assert not any(
        doc["kind"] in {"Service", "Ingress"} and doc["metadata"]["name"] == _NAME for doc in docs
    )
    containers = pod["containers"]
    assert containers[0]["image"] == _IMAGE
    ports = [port for container in containers for port in container.get("ports", [])]
    assert {port["hostPort"] for port in ports} == {15432, 18443, 19443}
    assert len(ports) == 3
    for port in ports:
        assert port["hostIP"] == _ADDRESS
        assert port["containerPort"] == port["hostPort"]
        assert port.get("protocol", "TCP") == "TCP"
    # Provisioning the route must not require or expose new credential payloads.
    assert not any(doc["kind"] == "Secret" and doc["metadata"]["name"] == _NAME for doc in docs)
    assert "loom-nebius-staging-spool" not in str(docs)


def test_proxy_preserves_tcp_and_tracks_renewed_certificate_files(tmp_path: Path) -> None:
    docs = _render(tmp_path)
    data = _object(docs, "ConfigMap")["data"]
    nginx = data["nginx.conf"]
    assert "stream" in nginx
    assert "loom-postgres-rw.loom-staging.svc.cluster.local:5432" in nginx
    assert "loom-control-plane.loom-staging.svc.cluster.local:8080" in nginx
    assert "loom-minio.loom-staging.svc.cluster.local:9000" in nginx
    assert "18443 ssl" in nginx and "19443 ssl" in nginx
    assert "15432 ssl" not in nginx  # PostgreSQL negotiates its own TLS after SSLRequest.
    assert "ssl_certificate " in nginx and "ssl_certificate_key " in nginx
    assert "proxy_pass" in nginx and "rewrite" not in nginx
    script = data["run.sh"]
    assert "nginx" in script
    assert "reload" in script or "HUP" in script
    assert "sha256sum" in script or "cksum" in script
    pod = _object(docs, "Deployment")["spec"]["template"]["spec"]
    secret_volumes = {
        volume["name"]
        for volume in pod["volumes"]
        if volume.get("secret", {}).get("secretName") == "loom-staging-tls"
    }
    assert secret_volumes
    mounts = [
        mount
        for container in pod["containers"]
        for mount in container.get("volumeMounts", [])
        if mount["name"] in secret_volumes
    ]
    assert mounts and all(mount.get("readOnly") and "subPath" not in mount for mount in mounts)


def test_policies_limit_peer_and_allow_actual_backend_pods_bidirectionally(tmp_path: Path) -> None:
    docs = _render(tmp_path)
    policies = [doc["spec"] for doc in docs if doc["kind"] == "NetworkPolicy"]
    selected = [
        policy
        for policy in policies
        if policy["podSelector"].get("matchLabels", {}).get("app") == _NAME
    ]
    assert selected
    ingress = [rule for policy in selected for rule in policy.get("ingress", [])]
    assert ingress
    for rule in ingress:
        assert rule["from"] == [{"ipBlock": {"cidr": f"{_PEER}/32"}}]
    assert {port["port"] for rule in ingress for port in rule["ports"]} == {15432, 18443, 19443}
    egress = [rule for policy in selected for rule in policy.get("egress", [])]
    for selector, port in [
        ({"app": "loom-control-plane"}, 8080),
        ({"app": "loom-minio"}, 9000),
        ({"cnpg.io/cluster": "loom-postgres"}, 5432),
    ]:
        assert any(
            any(
                peer.get("podSelector", {}).get("matchLabels") == selector
                for peer in rule.get("to", [])
            )
            and any(row["port"] == port for row in rule.get("ports", []))
            for rule in egress
        )
        assert any(
            policy["podSelector"].get("matchLabels") == selector
            and any(
                any(
                    peer.get("podSelector", {}).get("matchLabels") == {"app": _NAME}
                    for peer in rule.get("from", [])
                )
                and any(row["port"] == port for row in rule.get("ports", []))
                for rule in policy.get("ingress", [])
            )
            for policy in policies
        )
    dns = [rule for rule in egress if any(row["port"] == 53 for row in rule.get("ports", []))]
    assert dns and all(rule.get("to") for rule in dns)
    assert {row["protocol"] for rule in dns for row in rule["ports"] if row["port"] == 53} == {
        "TCP",
        "UDP",
    }
    assert audit_boundary(yaml.safe_dump_all(docs)) == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"node_name": ""},
        {"node_name": "UPPER"},
        {"node_name": "bad/name"},
        {"node_name": "-node"},
        {"node_name": "a" * 64},
        {"node_name": "a" * 32 + "." + "b" * 32},
        {"wireguard_address": ""},
        {"peer_address": ""},
        {"wireguard_address": "10.1.1.1/32"},
        {"peer_address": "10.1.1.1/32"},
        {"wireguard_address": "0.0.0.0"},
        {"wireguard_address": "127.0.0.1"},
        {"wireguard_address": "8.8.8.8"},
        {"peer_address": "192.0.2.1"},
        {"peer_address": "169.254.1.1"},
        {"peer_address": "100.64.1.1"},
        {"peer_address": "::1"},
        {"peer_address": _ADDRESS},
        {"proxy_image": ""},
        {"proxy_image": "nginx:latest"},
        {"proxy_image": "nginx@sha256:short"},
    ],
)
def test_invalid_private_entry_fails_before_render(tmp_path: Path, overrides: dict) -> None:
    with pytest.raises(ValueError, match="nebius_private_entry"):
        render_manifests(load_cluster_config(_config(tmp_path, _entry(**overrides))))


@pytest.mark.parametrize("address", ["10.12.0.2", "172.16.0.2", "192.168.120.2"])
def test_private_entry_accepts_all_rfc1918_ranges(tmp_path: Path, address: str) -> None:
    config = load_cluster_config(_config(tmp_path, _entry(wireguard_address=address)))
    assert config.nebius_private_entry.wireguard_address == address


def test_enabled_entry_without_any_binding_fails(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nebius_private_entry"):
        render_manifests(
            load_cluster_config(_config(tmp_path, "[nebius_private_entry]\nenabled = true\n"))
        )


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ('runtime_environment = "staging"', 'runtime_environment = "development"'),
        ('namespace = "loom-staging"', 'namespace = "loom-development"'),
        ("multi_node = true", "multi_node = false"),
        ('ingress_host = "yylx.world"', 'ingress_host = "bad/host"'),
        ('ingress_host = "yylx.world"', 'ingress_host = "10.1.2.3"'),
        ('ingress_tls_secret_name = "loom-staging-tls"', 'ingress_tls_secret_name = "bad/secret"'),
    ],
)
def test_private_entry_rejects_invalid_environment_and_tls_identity(
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    profile = _PROFILE.read_text()
    assert before in profile
    with pytest.raises(ValueError):
        render_manifests(
            load_cluster_config(_config(tmp_path, _entry(), profile.replace(before, after)))
        )


@pytest.mark.parametrize(
    "change",
    [
        {"hostIP": "0.0.0.0"},
        {"hostIP": "8.8.8.8"},
        {"hostIP": ""},
        {"hostPort": 5432},
        {"hostPort": 443},
    ],
)
def test_boundary_does_not_blanket_allowlist_private_entry(tmp_path: Path, change: dict) -> None:
    docs = _render(tmp_path)
    pod = _object(docs, "Deployment")["spec"]["template"]["spec"]
    pod["containers"][0]["ports"][0].update(change)
    assert any(row.object_name == _NAME for row in audit_boundary(yaml.safe_dump_all(docs)))


def test_boundary_rejects_missing_private_bind_address(tmp_path: Path) -> None:
    docs = _render(tmp_path)
    pod = _object(docs, "Deployment")["spec"]["template"]["spec"]
    del pod["containers"][0]["ports"][0]["hostIP"]
    assert any(row.object_name == _NAME for row in audit_boundary(yaml.safe_dump_all(docs)))


def test_public_cli_render_accepts_private_entry_and_rejects_partial_binding(
    tmp_path: Path, capsys
) -> None:
    path = _config(tmp_path, _entry())
    assert main(["cluster", "render", "--config", str(path)]) == 0
    result = capsys.readouterr()
    docs = [doc for doc in yaml.safe_load_all(result.out) if doc]
    assert _object(docs, "Deployment")["spec"]["replicas"] == 1
    path = _config(tmp_path, _entry(peer_address=""))
    assert main(["cluster", "render", "--config", str(path)]) != 0
    result = capsys.readouterr()
    assert "nebius_private_entry" in result.err
    assert "apiVersion:" not in result.out
