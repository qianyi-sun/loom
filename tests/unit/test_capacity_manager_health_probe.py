from __future__ import annotations

from pathlib import Path

import pytest

from loom_capacity_manager.health_probe import (
    CapacityHealthProbeError,
    capacity_health_probe_argv,
    parse_capacity_health_response,
    parse_observed_capacity_health_response,
    parse_observed_capacity_manager_identity_response,
    probe_capacity_manager,
)


def test_health_response_accepts_only_ready_with_integer_zero() -> None:
    assert parse_capacity_health_response(
        200,
        b'{"status":"ready","executable_new_capacity_ceiling":0}',
    ) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 0,
    }


@pytest.mark.parametrize(
    ("status_code", "payload", "ceiling"),
    [
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0}', 0),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":3}', 3),
        (503, b'{"status":"not-ready","executable_new_capacity_ceiling":0}', 0),
    ],
)
def test_observed_health_accepts_exact_nonnegative_state(
    status_code: int,
    payload: bytes,
    ceiling: int,
) -> None:
    assert parse_observed_capacity_health_response(status_code, payload) == {
        "status": "ready" if status_code == 200 else "not-ready",
        "executable_new_capacity_ceiling": ceiling,
    }


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (201, b'{"status":"ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (503, b'{"status":"ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":-1}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":false}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0,"extra":1}'),
    ],
)
def test_observed_health_rejects_ambiguous_state(
    status_code: int,
    payload: bytes,
) -> None:
    with pytest.raises(CapacityHealthProbeError):
        parse_observed_capacity_health_response(status_code, payload)


def test_health_probe_argv_keeps_strict_default_and_explicit_observation_mode() -> None:
    strict = capacity_health_probe_argv()
    observed = capacity_health_probe_argv(observe=True)

    assert "--observe" not in strict
    assert observed == (*strict, "--observe")


def test_health_probe_identity_observation_uses_lifecycle_identity_explicitly() -> None:
    assert capacity_health_probe_argv(
        "/run/loom-personal-dev/management/files",
        observe_identity=True,
    ) == (
        "python",
        "-m",
        "loom_capacity_manager.health_probe",
        "--url",
        "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status",
        "--ca-file",
        "/run/loom-personal-dev/management/files/capacity-lifecycle-ca.pem",
        "--certificate-file",
        "/run/loom-personal-dev/management/files/capacity-lifecycle-certificate.pem",
        "--private-key-file",
        "/run/loom-personal-dev/management/files/capacity-lifecycle-private-key.pem",
        "--bearer-token-file",
        "/run/loom-personal-dev/management/files/capacity-lifecycle-token",
        "--observe-identity",
    )


def test_identity_response_returns_only_exact_manager_binding() -> None:
    assert parse_observed_capacity_manager_identity_response(
        200,
        (
            b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
            b'"observer_principal_id":"personal-dev-lifecycle",'
            b'"configuration_epoch":7,"execution_state":"shadow",'
            b'"execution_epoch":0,"executable_new_capacity_ceiling":0,'
            b'"schema_version":1,"account_slots":{}}'
        ),
    ) == {
        "authority_incarnation": "00000000-0000-0000-0000-000000000101",
        "configuration_epoch": 7,
        "executable_new_capacity_ceiling": 0,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "observer_principal_id": "personal-dev-lifecycle",
    }


@pytest.mark.parametrize(
    "payload",
    [
        b"[]",
        b'{"authority_incarnation":"not-a-uuid","observer_principal_id":"reader",'
        b'"configuration_epoch":7,"execution_state":"shadow","execution_epoch":0,'
        b'"executable_new_capacity_ceiling":0}',
        b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
        b'"observer_principal_id":"wrong principal","configuration_epoch":7,'
        b'"execution_state":"shadow","execution_epoch":0,'
        b'"executable_new_capacity_ceiling":0}',
        b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
        b'"observer_principal_id":"reader","configuration_epoch":true,'
        b'"execution_state":"shadow","execution_epoch":0,'
        b'"executable_new_capacity_ceiling":0}',
        b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
        b'"observer_principal_id":"reader","configuration_epoch":7,'
        b'"execution_state":"shadow","execution_epoch":0,'
        b'"executable_new_capacity_ceiling":false}',
        b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
        b'"observer_principal_id":"reader","observer_principal_id":"other",'
        b'"configuration_epoch":7,"execution_state":"shadow","execution_epoch":0,'
        b'"executable_new_capacity_ceiling":0}',
    ],
)
def test_identity_response_rejects_ambiguous_manager_binding(payload: bytes) -> None:
    with pytest.raises(CapacityHealthProbeError):
        parse_observed_capacity_manager_identity_response(200, payload)


def test_identity_probe_authenticates_and_parses_the_status_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "lifecycle-token"
    token_file.write_text("lifecycle-secret", encoding="utf-8")
    token_file.chmod(0o600)

    class _Context:
        def load_cert_chain(self, **_kwargs: object) -> None:
            pass

    class _Response:
        status_code = 200

        def __init__(self) -> None:
            self.headers = {"content-encoding": ""}

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def iter_bytes(self, *, chunk_size: int):
            assert chunk_size == 64 * 1024 + 1
            yield (
                b'{"authority_incarnation":"00000000-0000-0000-0000-000000000101",'
                b'"observer_principal_id":"personal-dev-lifecycle",'
                b'"configuration_epoch":7,"execution_state":"shadow",'
                b'"execution_epoch":0,"executable_new_capacity_ceiling":0}'
            )

    class _Client:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> _Client:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def stream(
            self,
            method: str,
            url: str,
            *,
            headers: dict[str, str] | None,
        ) -> _Response:
            if (
                method != "GET"
                or url != "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status"
                or headers
                != {
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Authorization": "Bearer lifecycle-secret",
                }
            ):
                raise AssertionError("identity observation was not authenticated exactly")
            return _Response()

    monkeypatch.setattr(
        "loom_capacity_manager.health_probe._validate_server_certificate_identities",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "loom_capacity_manager.health_probe.ssl.create_default_context",
        lambda **_kwargs: _Context(),
    )
    monkeypatch.setattr(
        "loom_capacity_manager.health_probe.httpx.Client",
        _Client,
    )

    assert (
        probe_capacity_manager(
            url="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status",
            ca_file=tmp_path / "ca.pem",
            certificate_file=tmp_path / "certificate.pem",
            private_key_file=tmp_path / "private-key.pem",
            bearer_token_file=token_file,
            observe_identity=True,
        )["observer_principal_id"]
        == "personal-dev-lifecycle"
    )


def test_identity_probe_rejects_encoded_response_before_decompression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "lifecycle-token"
    token_file.write_text("lifecycle-secret", encoding="utf-8")
    token_file.chmod(0o600)

    class _Context:
        def load_cert_chain(self, **_kwargs: object) -> None:
            pass

    class _Response:
        status_code = 200

        def __init__(self) -> None:
            self.headers = {"content-encoding": "gzip"}

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def iter_bytes(self, *, chunk_size: int):
            raise AssertionError("encoded response body must not be decompressed")
            yield b""  # pragma: no cover - keep this a generator

    class _Client:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> _Client:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def stream(
            self,
            method: str,
            url: str,
            *,
            headers: dict[str, str] | None,
        ) -> _Response:
            assert method == "GET"
            assert url == (
                "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status"
            )
            assert headers is not None
            assert headers["Accept-Encoding"] == "identity"
            return _Response()

    monkeypatch.setattr(
        "loom_capacity_manager.health_probe.ssl.create_default_context",
        lambda **_kwargs: _Context(),
    )
    monkeypatch.setattr(
        "loom_capacity_manager.health_probe.httpx.Client",
        _Client,
    )

    with pytest.raises(CapacityHealthProbeError, match="response encoding is invalid"):
        probe_capacity_manager(
            url="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status",
            ca_file=tmp_path / "ca.pem",
            certificate_file=tmp_path / "certificate.pem",
            private_key_file=tmp_path / "private-key.pem",
            bearer_token_file=token_file,
            observe_identity=True,
        )


def test_identity_probe_rejects_noncanonical_url_before_reading_credentials(
    tmp_path: Path,
) -> None:
    with pytest.raises(CapacityHealthProbeError, match="URL is invalid"):
        probe_capacity_manager(
            url="https://attacker.example/v1/status",
            ca_file=tmp_path / "missing-ca.pem",
            certificate_file=tmp_path / "missing-certificate.pem",
            private_key_file=tmp_path / "missing-private-key.pem",
            bearer_token_file=tmp_path / "missing-token",
            observe_identity=True,
        )


@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (503, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (200, b"not-json"),
        (200, b'{"status":"ready"}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0,"extra":true}'),
        (
            200,
            b'{"status":"ready","status":"not-ready","executable_new_capacity_ceiling":0}',
        ),
        (200, b'{"status":"not-ready","executable_new_capacity_ceiling":0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":1}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":false}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":0.0}'),
        (200, b'{"status":"ready","executable_new_capacity_ceiling":"0"}'),
        (200, b"{}" + b" " * 4096),
    ],
)
def test_health_response_rejects_every_ambiguous_or_unsafe_shape(
    status_code: int,
    payload: bytes,
) -> None:
    with pytest.raises(CapacityHealthProbeError):
        parse_capacity_health_response(status_code, payload)
