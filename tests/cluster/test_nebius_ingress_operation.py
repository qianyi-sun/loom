"""Composed disposable staging/TLS/public cutover with the installed SQL guard.

Only cloud metadata and trust roots are test inputs. The controller, Kubernetes
CAS, guard CLI in a Pod, PostgreSQL, TLS sockets and recovery journals are real.
This is not proof of installation on the shared Nebius cluster.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import ssl
import subprocess
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from scripts.ops.nebius_ingress_operation import (
    LiveIngressAPI,
    OperationError,
    install_ingress,
    rollback_ingress,
)
from scripts.ops.nebius_ingress_rollout import build_wheels

from tests.cluster.test_nebius_shared_ingress import (
    PYTHON,
    TRAEFIK,
    _add_failure_diagnostics,
    _backend,
    _certificate,
    _run,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops import test_nebius_certificates as certificate_material
from tests.ops.test_nebius_ingress_gateway import inputs as inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]
POSTGRES = "docker.io/library/postgres@sha256:304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad"
# The gateway's qualified uv binary is glibc, not Alpine/musl. Reuse the pinned
# glibc Python base from Dockerfile.pipeline-core-fixture for the guard installer.
GUARD_PYTHON = "public.ecr.aws/docker/library/python@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2"
pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires an explicitly disposable Kubernetes API")


@pytest.fixture(autouse=True)
def live_certificate_clock(monkeypatch):
    monkeypatch.setattr(certificate_material, "NOW", datetime.now(UTC))


def _guard_documents(namespace):
    schema = """
CREATE TABLE nebius_rollout_guard (id integer PRIMARY KEY, owner text, candidate_sha text);
CREATE TABLE trials (state text);
CREATE TABLE execution_leases (revoked_at timestamptz, cleanup_state text, materialization_state text);
CREATE TABLE task_image_materializations (state text);
CREATE TABLE task_image_materialization_attempts (native_build jsonb);
"""
    def metadata(name):
        return {"name": name, "namespace": namespace}
    return [
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("guard-schema"), "data": {"schema.sql": schema}},
        {"apiVersion": "v1", "kind": "Pod", "metadata": {**metadata("postgres"), "labels": {"app": "postgres"}},
         "spec": {"containers": [{"name": "postgres", "image": POSTGRES,
                   "env": [{"name": "POSTGRES_PASSWORD", "value": "disposable"}],
                   "volumeMounts": [{"name": "schema", "mountPath": "/docker-entrypoint-initdb.d", "readOnly": True}],
                   "readinessProbe": {"exec": {"command": ["pg_isready", "-U", "postgres"]}, "periodSeconds": 1}}],
                  "volumes": [{"name": "schema", "configMap": {"name": "guard-schema"}}]}},
        {"apiVersion": "v1", "kind": "Service", "metadata": metadata("postgres"),
         "spec": {"selector": {"app": "postgres"}, "ports": [{"port": 5432}]}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": metadata("loom-control-plane"),
         "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "guard"}},
                  "template": {"metadata": {"labels": {"app": "guard"}}, "spec": {
                      "volumes": [{"name": "tooling", "hostPath": {"path": "/tmp/loom-ingress-qualification", "type": "Directory"}},
                                  {"name": "runtime", "emptyDir": {}}],
                      "initContainers": [{"name": "install", "image": GUARD_PYTHON, "command": ["sh", "-ec",
                          "/tooling/uv venv --no-config --no-cache --no-python-downloads --python /usr/local/bin/python /runtime; "
                          "/tooling/uv pip sync --no-config --no-cache --python /runtime/bin/python --require-hashes "
                          "--only-binary :all: /tooling/requirements.txt; "
                          "/tooling/uv pip install --no-config --no-cache --python /runtime/bin/python --offline --no-deps /tooling/wheels/*.whl"],
                          "volumeMounts": [{"name": "tooling", "mountPath": "/tooling", "readOnly": True},
                                           {"name": "runtime", "mountPath": "/runtime"}]}],
                      "containers": [{"name": "guard", "image": GUARD_PYTHON,
                          "command": ["/runtime/bin/python", "-c", "import time; time.sleep(3600)"],
                          "env": [{"name": "PATH", "value": "/runtime/bin:/usr/local/bin:/usr/bin:/bin"},
                                  {"name": "LOOM_CP_DB_URL", "value": "postgresql+psycopg://postgres:disposable@postgres:5432/postgres"},
                                  {"name": "LOOM_CP_MINIO_ACCESS_KEY", "value": "fixture"},
                                  {"name": "LOOM_CP_STEP_JWT_SIGNING_KEY", "value": "disposable-guard-fixture-only"},
                                  {"name": "LOOM_CP_MINIO_SECRET_KEY", "value": "fixture"}],
                          "volumeMounts": [{"name": "runtime", "mountPath": "/runtime", "readOnly": True}]}],
                  }}}},
    ]


@pytest.mark.timeout(420)
def test_connected_installation_recovers_then_cuts_over_with_installed_guard(inputs, platform_inputs, tmp_path, monkeypatch):
    config, binding, _fake, roots, _selected, _root = inputs
    platform, candidate, profile = platform_inputs
    namespace, node_name = platform["namespace"], "computeinstance-ingressfixture"
    tools = tmp_path / "tooling"
    tools.mkdir(mode=0o700)
    uv = Path(shutil.which("uv"))
    build_wheels(tools, uv=uv)
    subprocess.run([str(uv), "export", "--locked", "--no-default-groups", "--extra", "cluster", "--group", "nebius-certificates",
                    "--no-emit-workspace", "--format", "requirements-txt", "--no-header", "--quiet",
                    "--output-file", str(tools / "requirements.txt")], cwd=ROOT, check=True, timeout=60)
    shutil.copy2(uv, tools / "uv")
    archives = []
    for index, image in enumerate((TRAEFIK, PYTHON, POSTGRES, GUARD_PYTHON)):
        # Reuse exact local digests; repeated disposable nodes must not fetch
        # the same fixture images from the public registry on every run.
        available = subprocess.run(["docker", "image", "inspect", image], capture_output=True, timeout=15)
        if available.returncode:
            subprocess.run(["docker", "pull", image], check=True, capture_output=True, timeout=180)
        archive = tmp_path / ("image-" + str(index) + ".tar")
        subprocess.run(["docker", "save", "-o", str(archive), image], check=True, capture_output=True, timeout=120)
        archives.append((image, archive))
    container = _start_k3s(node_name=node_name, ephemeral_storage_floor="2Gi")
    try:
        from kubernetes import client
        _, core, _ = _load_client(container)
        ns = core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
        binding = replace(binding, namespace=namespace, namespace_uid=ns.metadata.uid,
                          kube_system_uid=core.read_namespace("kube-system").metadata.uid)
        ident = container.get_wrapped_container().id
        _run(container, "kubectl", "wait", "--for=create", "node/" + node_name, "--timeout=60s")
        _run(container, "kubectl", "wait", "node/" + node_name, "--for=condition=Ready", "--timeout=60s")
        _run(container, "mkdir", "-p", "/tmp/loom-ingress-qualification")
        subprocess.run(["docker", "cp", str(tools) + "/.", ident + ":/tmp/loom-ingress-qualification/"], check=True, timeout=60)
        for image, archive in archives:
            remote = "/tmp/" + archive.name
            subprocess.run(["docker", "cp", str(archive), ident + ":" + remote], check=True, capture_output=True, timeout=60)
            _run(container, "ctr", "images", "import", "--digests", "--base-name", image.split("@", 1)[0], remote)
        image = "cr.eu-north1.nebius.cloud/test/loom-shared-ingress@" + TRAEFIK.split("@", 1)[1]
        _run(container, "ctr", "images", "tag", TRAEFIK, image)
        _run(container, "kubectl", "wait", "--for=create", "node/" + node_name, "--timeout=60s")
        core.patch_node(node_name, {"metadata": {"labels": {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}},
                                    "spec": {"providerID": "nebius://" + node_name}})
        _run(container, "kubectl", "wait", "node/" + node_name, "--for=condition=Ready", "--timeout=60s")
        raw = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        context = yaml.safe_load(raw.output)
        endpoint = "https://127.0.0.1:" + str(container.get_exposed_port(6443))
        context["clusters"][0]["cluster"]["server"] = endpoint
        context["clusters"][0]["name"] = platform["cluster_id"]
        context["contexts"][0]["context"]["cluster"] = platform["cluster_id"]
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(yaml.safe_dump(context))
        kubeconfig.chmod(0o600)
        platform["kubernetes_api_server"] = endpoint
        old_host = platform["public_host"]
        old_tls = _certificate([old_host])
        server = '''import http.server, json, ssl
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        value = {"environment":"development", "apiRouteBase":"https://HOST/api"} if self.path == "/loom-frontend-config.json" else {"buildRevision":"CANDIDATE"} if self.path == "/api/v1/version" else {"status":"ok"}
        body = json.dumps(value).encode()
        self.send_response(200); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*args): pass
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/tls/tls.crt","/tls/tls.key"); context.set_alpn_protocols(["http/1.1","acme-tls/1"])
server=http.server.ThreadingHTTPServer(("0.0.0.0",8443),Handler)
server.socket=context.wrap_socket(server.socket,server_side=True); server.serve_forever()
'''.replace("HOST", old_host).replace("CANDIDATE", candidate["candidate_sha"])
        networks = json.loads(subprocess.check_output(["docker", "inspect", ident]))[0]["NetworkSettings"]["Networks"]
        address = next(value["IPAddress"] for value in networks.values() if value.get("IPAddress"))
        documents = [
            {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "legacy-tls", "namespace": namespace},
             "type": "kubernetes.io/tls", "stringData": old_tls},
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "backend-code", "namespace": namespace}, "data": {"server.py": server}},
            _backend(namespace, "loom-web", 8443, tls=True),
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "loom-platform-config", "namespace": namespace},
             "data": {"environment.json": json.dumps(platform), "profile.json": json.dumps(profile)}},
            {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "loom-web", "namespace": namespace, "annotations": {"foreign": "retained"}},
             "spec": {"type": "LoadBalancer", "externalIPs": [address], "selector": {"app": "loom-web"},
                      "ports": [{"port": 443, "targetPort": 8443}]}},
            *_guard_documents(namespace),
        ]
        _run(container, "kubectl", "apply", "-f", "-", payload=yaml.safe_dump_all(documents))
        core.patch_namespaced_service_status("loom-web", namespace, {"status": {"loadBalancer": {"ingress": [{"ip": address}]}}})
        deadline = time.monotonic() + 120
        while True:
            pods = core.list_namespaced_pod(namespace, label_selector="app=guard").items
            if pods:
                pod = pods[0]
                for status in pod.status.init_container_statuses or []:
                    stopped = status.state.terminated
                    if stopped and stopped.exit_code:
                        pytest.fail("guard init failed: " + core.read_namespaced_pod_log(
                            pod.metadata.name, namespace, container="install", tail_lines=40))
                if any(condition.type == "Ready" and condition.status == "True" for condition in pod.status.conditions or []):
                    break
            assert time.monotonic() < deadline, "guard fixture startup deadline"
            time.sleep(0.25)
        for pod in ("loom-web", "postgres"):
            _run(container, "kubectl", "wait", "pod/" + pod, "-n", namespace, "--for=condition=Ready", "--timeout=60s", timeout=70)
        _run(container, "kubectl", "rollout", "status", "deployment/coredns", "-n", "kube-system", "--timeout=60s", timeout=70)
        public = core.read_namespaced_service("loom-web", namespace)
        legacy = core.read_namespaced_secret("legacy-tls", namespace)

        class InterruptedAPI(LiveIngressAPI):
            def probe_public(self, receipt):
                super().probe_public(receipt)
                if json.loads(self.read()[1]["data"]["environment.json"]).get("shared_ingress_enabled") is True:
                    raise RuntimeError("intentional interruption before guard release")

        kwargs = {"binding": binding, "executable": Path(shutil.which("kubectl")), "candidate": candidate["candidate_sha"],
                  "cluster_id": platform["cluster_id"], "api_server": endpoint, "ingress_class": "loom-shared", "image": image}
        api = InterruptedAPI(kubeconfig, **kwargs)
        original_context = ssl.create_default_context
        trust = original_context(cadata=roots[0].public_bytes(serialization.Encoding.PEM).decode() + old_tls["tls.crt"])
        monkeypatch.setattr(ssl, "create_default_context", lambda: trust)
        state = tmp_path / "ingress-state"
        with pytest.raises(OperationError) as failure:
            install_ingress(api=api, certificate_config=config, state_dir=state, roots=roots)
        record = (json.loads((state / "cutover/cutover.json").read_text())
                  if (state / "cutover/cutover.json").exists() else {})
        if record.get("phase") != "configuration_switched":
            # This disposable fixture has no external credentials. Preserve its
            # exception chain so a preflight failure cannot masquerade as the
            # intended post-write interruption or disappear behind FileNotFound.
            reasons, cause = [], failure.value
            while cause is not None:
                reasons.append(type(cause).__name__ + ": " + str(cause))
                cause = cause.__context__
            pytest.fail("operation never reached interruption (phase=" + str(record.get("phase")) + "): " + " <- ".join(reasons))
        assert record["phase"] == "configuration_switched"
        assert api.guard("observe", record["owner"], api.candidate) == {"status": "held"}
        assert core.read_namespaced_service("loom-web", namespace).spec.selector == {"app": "loom-shared-ingress"}
        assert rollback_ingress(api=api, state_dir=state)["status"] == "rolled_back"
        assert api.guard("observe", record["owner"], api.candidate) == {"status": "open"}
        assert core.read_namespaced_service("loom-web", namespace).spec.selector == {"app": "loom-web"}
        # A new cutover directory is explicit; retain the completed rollback and
        # reuse the immutable stage ownership record, never adopt objects by name.
        (state / "cutover").rename(state / "recovered-cutover")
        api = LiveIngressAPI(kubeconfig, **kwargs)
        result = install_ingress(api=api, certificate_config=config, state_dir=state, roots=roots)
        assert result["status"] == "complete"
        assert install_ingress(api=api, certificate_config=config, state_dir=state, roots=roots) == result
        after = core.read_namespaced_service("loom-web", namespace)
        assert (after.metadata.uid, after.spec.cluster_ip, after.spec.ports, after.spec.external_i_ps) == (
            public.metadata.uid, public.spec.cluster_ip, public.spec.ports, public.spec.external_i_ps)
        assert after.metadata.annotations["foreign"] == "retained"
        assert core.read_namespaced_secret("legacy-tls", namespace).data == legacy.data
        alpn = original_context(cadata=old_tls["tls.crt"])
        alpn.set_alpn_protocols(["acme-tls/1"])
        with socket.create_connection((address, 443), timeout=5) as stream:
            with alpn.wrap_socket(stream, server_hostname=old_host) as secure:
                assert secure.selected_alpn_protocol() == "acme-tls/1"
    except BaseException as error:
        try:
            node = container.get_wrapped_container()
            node.reload()
            if node.status != "running":
                error.add_note(node.logs(tail=1000).decode(errors="replace"))
        except Exception:
            pass
        try:
            # Read-only direct guard diagnostic retains fixture-only traceback;
            # never repeat acquire/release after an uncertain outcome.
            error.add_note(_run(container, "kubectl", "exec", "-n", namespace,
                "deployment/loom-control-plane", "--", "python", "-c",
                "import argparse,asyncio; from loom.nebius_rollout_guard import _run; "
                "print(asyncio.run(_run(argparse.Namespace(action='observe',owner='fixture',candidate='fixture'))))",
                timeout=15))
        except Exception as diagnostic:
            error.add_note(str(diagnostic))
        try:
            inventory = json.loads(_run(container, "kubectl", "get", "pods", "-A", "-o", "json", timeout=10))
            resources = [{"pod": row["metadata"]["namespace"] + "/" + row["metadata"]["name"],
                          "desired": [{key: item[key] for key in ("name", "resources") if key in item}
                                      for key in ("containers", "initContainers") for item in row["spec"].get(key, [])],
                          "observed": [{key: item[key] for key in ("name", "resources", "allocatedResources") if key in item}
                                       for key in ("containerStatuses", "initContainerStatuses") for item in row.get("status", {}).get(key, [])],
                          "pod_spec_resources": row["spec"].get("resources"),
                          "pod_status_resources": row.get("status", {}).get("resources"),
                          "pod_allocated_resources": row.get("status", {}).get("allocatedResources"),
                          "resize": row.get("status", {}).get("resize"),
                          "conditions": row.get("status", {}).get("conditions")}
                         for row in inventory["items"]]
            error.add_note("capacity-only inventory: " + json.dumps(resources))
        except Exception:
            pass
        try:
            error.add_note(_run(container, "kubectl", "logs", "-n", namespace,
                                "deployment/loom-control-plane", "-c", "install", "--tail=40", timeout=10))
        except Exception:
            pass
        _add_failure_diagnostics(container, namespace, error)
        raise
    finally:
        container.stop()
