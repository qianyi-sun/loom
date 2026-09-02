from __future__ import annotations

import base64
import json
import os
import ssl
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from loom_task_image_authority import config
from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
    TaskImageSecretStoreKeyring,
    build_uvicorn_kwargs,
    load_secret_store_keyring,
    read_owner_only_bytes,
    read_owner_only_secret,
)


def _owner_only(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _settings_values(tmp_path: Path) -> dict[str, Any]:
    return {
        "principals_file": tmp_path / "principals.json",
        "db_url_file": tmp_path / "database-url",
        "secret_store_keyring_file": tmp_path / "keyring.json",
        "tls_cert_file": tmp_path / "server.pem",
        "tls_key_file": tmp_path / "server-key.pem",
        "tls_client_ca_file": tmp_path / "client-ca.pem",
    }


def _keyring_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "primary": {
            "version": 3,
            "key_base64": base64.b64encode(b"p" * 32).decode("ascii"),
        },
        "fallbacks": [
            {
                "version": 2,
                "key_base64": base64.b64encode(b"f" * 32).decode("ascii"),
            },
            {
                "version": 1,
                "key_base64": base64.b64encode(b"o" * 32).decode("ascii"),
            },
        ],
    }


def _write_keyring(path: Path, document: dict[str, Any]) -> Path:
    return _owner_only(path, json.dumps(document).encode("utf-8"))


def test_settings_are_frozen_strict_and_use_only_the_authority_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in _settings_values(tmp_path).items():
        monkeypatch.setenv(f"LOOM_TASK_IMAGE_AUTHORITY_{name.upper()}", str(value))
    monkeypatch.setenv("LOOM_TASK_IMAGE_AUTHORITY_PORT", "9445")
    monkeypatch.setenv("LOOM_TASK_IMAGE_AUTHORITY_REQUEST_RATE_LIMIT_PER_SECOND", "75")
    monkeypatch.setenv("LOOM_TASK_IMAGE_AUTHORITY_REQUEST_CONCURRENCY_LIMIT", "12")
    monkeypatch.setenv("LOOM_CAPACITY_PORT", "1234")

    settings = TaskImageAuthoritySettings()

    assert settings.port == 9445
    assert settings.request_rate_limit_per_second == 75
    assert settings.request_concurrency_limit == 12
    assert settings.host == "127.0.0.1"
    with pytest.raises(ValidationError):
        settings.port = 8446  # type: ignore[misc]
    with pytest.raises(ValidationError):
        TaskImageAuthoritySettings(**_settings_values(tmp_path), port="8445")
    with pytest.raises(ValidationError):
        TaskImageAuthoritySettings(**_settings_values(tmp_path), unknown=True)
    with pytest.raises(ValidationError):
        TaskImageAuthoritySettings(**_settings_values(tmp_path), host="0.0.0.0")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("port", 0),
        ("port", 65536),
        ("request_rate_limit_per_second", 0),
        ("request_rate_limit_per_second", 10_001),
        ("request_concurrency_limit", 0),
        ("request_concurrency_limit", 1025),
    ],
)
def test_settings_reject_out_of_bounds_values(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    values = _settings_values(tmp_path)
    values[field] = value

    with pytest.raises(ValidationError):
        TaskImageAuthoritySettings(**values)


def test_uvicorn_configuration_always_requires_a_trusted_client_certificate(
    tmp_path: Path,
) -> None:
    settings = TaskImageAuthoritySettings(**_settings_values(tmp_path))

    assert build_uvicorn_kwargs(settings) == {
        "host": "127.0.0.1",
        "port": 8445,
        "ssl_certfile": str(settings.tls_cert_file),
        "ssl_keyfile": str(settings.tls_key_file),
        "ssl_ca_certs": str(settings.tls_client_ca_file),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
        "server_header": False,
    }


def test_owner_only_reader_returns_exact_bounded_bytes(tmp_path: Path) -> None:
    path = _owner_only(tmp_path / "secret", b"exact\x00bytes")

    assert read_owner_only_bytes(path, max_bytes=11) == b"exact\x00bytes"


@pytest.mark.parametrize("max_bytes", [0, -1, True, 1.5])
def test_owner_only_reader_rejects_invalid_bounds(tmp_path: Path, max_bytes: Any) -> None:
    path = _owner_only(tmp_path / "secret", b"value")

    with pytest.raises(ValueError, match="positive integer"):
        read_owner_only_bytes(path, max_bytes=max_bytes)


def test_owner_only_reader_rejects_unsafe_file_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _owner_only(tmp_path / "secret", b"value")
    path.chmod(0o640)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="0600"):
        read_owner_only_bytes(path)

    path.chmod(0o600)
    link = tmp_path / "secret-link"
    link.symlink_to(path)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="nonsymlink"):
        read_owner_only_bytes(link)

    fifo = tmp_path / "secret-fifo"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="nonsymlink"):
        read_owner_only_bytes(fifo)

    current_uid = os.getuid()
    monkeypatch.setattr(config.os, "getuid", lambda: current_uid + 1)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="current-uid"):
        read_owner_only_bytes(path)


def test_owner_only_reader_rejects_oversize_and_metadata_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = _owner_only(tmp_path / "oversized", b"12345")
    with pytest.raises(TaskImageAuthorityConfigurationError, match="maximum byte size"):
        read_owner_only_bytes(oversized, max_bytes=4)

    path = _owner_only(tmp_path / "secret", b"value")
    real_fstat = os.fstat
    calls = 0

    def changed_second_fstat(descriptor: int) -> os.stat_result | SimpleNamespace:
        nonlocal calls
        calls += 1
        metadata = real_fstat(descriptor)
        if calls == 1:
            return metadata
        return SimpleNamespace(
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_mode=metadata.st_mode,
            st_uid=metadata.st_uid,
            st_size=metadata.st_size,
            st_mtime_ns=metadata.st_mtime_ns,
            st_ctime_ns=metadata.st_ctime_ns + 1,
        )

    monkeypatch.setattr(config.os, "fstat", changed_second_fstat)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="changed while reading"):
        read_owner_only_bytes(path)


def test_secret_reader_accepts_one_line_and_rejects_ambiguous_text(tmp_path: Path) -> None:
    assert read_owner_only_secret(_owner_only(tmp_path / "valid", b"value\n")) == "value"

    private = b"TOP_PRIVATE_VALUE"
    invalid_payloads = [b"", private + b"\nsecond", private + b"\r\n", private + b"\x00", b"\xff"]
    for index, payload in enumerate(invalid_payloads):
        path = _owner_only(tmp_path / f"invalid-{index}", payload)
        with pytest.raises(TaskImageAuthorityConfigurationError) as caught:
            read_owner_only_secret(path)
        assert private.decode("ascii") not in str(caught.value)


def test_keyring_loads_exact_keys_into_an_immutable_redacted_value(tmp_path: Path) -> None:
    keyring = load_secret_store_keyring(
        _write_keyring(tmp_path / "keyring.json", _keyring_document())
    )

    assert keyring.primary_key == b"p" * 32
    assert keyring.primary_version == 3
    assert dict(keyring.fallback_keys) == {2: b"f" * 32, 1: b"o" * 32}
    with pytest.raises(TypeError):
        keyring.fallback_keys[1] = b"x" * 32  # type: ignore[index]
    rendered = repr(keyring)
    assert "cHBwc" not in rendered
    assert "b'pppp" not in rendered
    assert "primary_version=3" in rendered


def test_keyring_copies_fallback_mapping_at_its_public_boundary() -> None:
    fallback_keys = {1: b"o" * 32}
    keyring = TaskImageSecretStoreKeyring(
        primary_key=b"p" * 32,
        primary_version=2,
        fallback_keys=fallback_keys,
    )

    fallback_keys[1] = b"x" * 32

    assert dict(keyring.fallback_keys) == {1: b"o" * 32}
    with pytest.raises(TypeError):
        keyring.fallback_keys[1] = b"x" * 32  # type: ignore[index]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.__setitem__("schema_version", 2),
        lambda value: value.__setitem__("unknown", True),
        lambda value: value["primary"].__setitem__("version", 0),
        lambda value: value["primary"].__setitem__("key_base64", "not base64"),
        lambda value: value["primary"].__setitem__(
            "key_base64", base64.b64encode(b"short").decode("ascii")
        ),
        lambda value: value["fallbacks"][1].__setitem__("version", 2),
        lambda value: value["fallbacks"][0].__setitem__("version", 3),
        lambda value: value.__setitem__("fallbacks", list(reversed(value["fallbacks"]))),
        lambda value: value["fallbacks"][0].__setitem__(
            "key_base64", value["primary"]["key_base64"]
        ),
    ],
)
def test_keyring_rejects_noncanonical_or_unsafe_documents(
    tmp_path: Path,
    mutate: Any,
) -> None:
    document = _keyring_document()
    mutate(document)
    path = _write_keyring(tmp_path / "keyring.json", document)

    with pytest.raises(TaskImageAuthorityConfigurationError) as caught:
        load_secret_store_keyring(path)

    message = str(caught.value)
    assert len(message) <= 128
    assert "cHBwc" not in message
    assert "ZmZmZ" not in message


def test_keyring_rejects_oversize_and_invalid_json(tmp_path: Path) -> None:
    invalid = _owner_only(tmp_path / "invalid.json", b"{")
    with pytest.raises(TaskImageAuthorityConfigurationError, match="invalid keyring"):
        load_secret_store_keyring(invalid)

    oversized = _owner_only(tmp_path / "oversized.json", b"{" + b" " * (1024 * 1024))
    with pytest.raises(TaskImageAuthorityConfigurationError, match="maximum byte size"):
        load_secret_store_keyring(oversized)
