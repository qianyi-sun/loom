"""Disposable-cluster setup observes actual prerequisites, not discovery alone."""

import ssl
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, ProtocolError, SSLError

from tests.integration import test_execution_actuator_k3s as fixture
from tests.integration.test_execution_actuator_k3s import _wait_for_dns_pods


@pytest.fixture
def loading_client(monkeypatch):
    from kubernetes import client, config

    clock = [0.0]
    monkeypatch.setattr(fixture.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(fixture.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(config, "load_kube_config_from_dict", lambda payload: None)
    core = SimpleNamespace(get_api_resources=lambda **kwargs: object())
    batch = object()
    monkeypatch.setattr(client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(client, "BatchV1Api", lambda: batch)
    container = SimpleNamespace(
        exec=lambda command: SimpleNamespace(exit_code=0, output=b"{}"),
        get_exposed_port=lambda port: "16443",
    )
    return container, core, batch, clock


def test_client_discovery_waits_for_system_namespace(loading_client):
    container, core, batch, clock = loading_client
    reads = []

    def read(name, *, _request_timeout):
        assert name == "kube-system" and 0 < _request_timeout <= 5
        reads.append(name)
        if len(reads) == 1:
            raise ApiException(status=404, reason="namespace bootstrap pending")
        return SimpleNamespace(metadata=SimpleNamespace(uid="cluster-identity"))

    core.read_namespace = read
    assert fixture._load_client(container)[1:] == (core, batch)
    assert len(reads) == 2 and clock[0] > 0


def test_client_missing_system_namespace_has_bounded_wait(loading_client):
    container, core, _, clock = loading_client

    def read(*args, **kwargs):
        raise ApiException(status=404, reason="namespace bootstrap pending")

    core.read_namespace = read
    with pytest.raises(AssertionError, match="kube-system"):
        fixture._load_client(container)
    assert clock[0] == 90


@pytest.mark.parametrize("status", [403, 500])
def test_client_system_namespace_does_not_retry_other_errors(loading_client, status):
    container, core, _, clock = loading_client
    error = ApiException(status=status, reason="not a missing namespace")

    def read(*args, **kwargs):
        raise error

    core.read_namespace = read
    with pytest.raises(ApiException) as raised:
        fixture._load_client(container)
    assert raised.value is error and clock[0] == 0


def _tls_eof():
    return MaxRetryError(None, "/api/v1/namespaces/kube-system/pods", SSLError(
        ssl.SSLEOFError(8, "EOF occurred in violation of protocol"),
    ))


async def test_dns_readiness_survives_tls_eof_then_waits_for_a_real_pod():
    pod = object()
    responses = iter((_tls_eof(), [], [pod]))
    calls = []

    def read(namespace, *, label_selector, **kwargs):
        calls.append((namespace, label_selector))
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return SimpleNamespace(items=response)

    assert await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read)) == [pod]
    assert calls == [("kube-system", "k8s-app=kube-dns")] * 3


async def test_persistent_tls_eof_fails_with_original_cause_by_setup_deadline():
    error = _tls_eof()

    def read(*args, **kwargs):
        raise error

    with pytest.raises(AssertionError, match="CoreDNS") as raised:
        await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read), timeout=0.01)
    assert raised.value.__cause__ is error


@pytest.mark.parametrize("error", [
    ApiException(status=403, reason="Forbidden"),
    MaxRetryError(None, "/pods", SSLError(ssl.SSLCertVerificationError("bad certificate"))),
    MaxRetryError(None, "/pods", ProtocolError("connection lost")),
])
async def test_dns_readiness_does_not_retry_authority_or_other_transport_failures(error):
    calls = []

    def read(*args, **kwargs):
        calls.append(1)
        raise error

    with pytest.raises(type(error)) as raised:
        await _wait_for_dns_pods(SimpleNamespace(list_namespaced_pod=read))
    assert raised.value is error
    assert len(calls) == 1
