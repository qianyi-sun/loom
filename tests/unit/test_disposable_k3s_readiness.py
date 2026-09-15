"""CoreDNS setup tolerates only a transient TLS EOF before checking network policy."""

import ssl
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, ProtocolError, SSLError

from tests.integration.test_execution_actuator_k3s import _wait_for_dns_pods


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
