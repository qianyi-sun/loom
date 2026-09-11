"""A restarted native worker cannot recover bootstrap authority from its input."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from loom_capacity_executor.native_worker_bootstrap import (
    NativeBootstrapError,
    NativeWorkerBootstrap,
    encode_native_bootstrap,
    native_bootstrap_pipe,
    read_native_bootstrap,
)
from tests.unit.test_capacity_executor_native_launch_profile import native_profile_fixture


def _bootstrap() -> NativeWorkerBootstrap:
    native = native_profile_fixture().native_execution
    assert native is not None
    return NativeWorkerBootstrap(
        native_execution=native,
        worker_credential="disposable-bootstrap-credential-" + "a" * 32,
    )


def test_bootstrap_round_trips_once_through_real_pipe_without_secret_repr() -> None:
    bootstrap = _bootstrap()
    descriptor = native_bootstrap_pipe(bootstrap)
    try:
        assert not os.get_inheritable(descriptor)
        received = read_native_bootstrap(descriptor, timeout_seconds=1)
        assert received == bootstrap
        assert bootstrap.worker_credential not in repr(received)
        with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
            read_native_bootstrap(descriptor, timeout_seconds=1)
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("payload", [b"", b"{}", b"\x00\x00\x00\x00", b"\xff" * 4096])
def test_missing_or_malformed_input_never_yields_bootstrap(payload: bytes) -> None:
    reader, writer = os.pipe()
    try:
        os.write(writer, payload)
    finally:
        os.close(writer)
    try:
        with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
            read_native_bootstrap(reader, timeout_seconds=1)
    finally:
        os.close(reader)


@pytest.mark.parametrize("suffix", [b"\n", b" ", b"{}", b"\x00"])
def test_trailing_or_truncated_bootstrap_is_rejected(suffix: bytes) -> None:
    wire = encode_native_bootstrap(_bootstrap())
    for payload in (wire + suffix, wire[:-1]):
        reader, writer = os.pipe()
        try:
            os.write(writer, payload)
        finally:
            os.close(writer)
        try:
            with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
                read_native_bootstrap(reader, timeout_seconds=1)
        finally:
            os.close(reader)


def test_open_unwritten_stdin_times_out_instead_of_hanging_restarted_worker() -> None:
    reader, writer = os.pipe()
    started = time.monotonic()
    try:
        with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
            read_native_bootstrap(reader, timeout_seconds=0.02)
        assert time.monotonic() - started < 1
    finally:
        os.close(reader)
        os.close(writer)


def test_complete_frame_without_eof_is_not_accepted() -> None:
    reader, writer = os.pipe()
    try:
        os.write(writer, encode_native_bootstrap(_bootstrap()))
        with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
            read_native_bootstrap(reader, timeout_seconds=0.02)
    finally:
        os.close(reader)
        os.close(writer)


def test_regular_file_is_not_a_one_use_bootstrap_transport(tmp_path: Path) -> None:
    path = tmp_path / "retained-bootstrap"
    path.write_bytes(encode_native_bootstrap(_bootstrap()))
    with path.open("rb") as stream:
        with pytest.raises(NativeBootstrapError, match="unavailable or malformed"):
            read_native_bootstrap(stream.fileno(), timeout_seconds=1)


def test_invalid_credential_error_does_not_echo_secret() -> None:
    original = _bootstrap()
    secret = "private\n" + "a" * 60
    with pytest.raises(NativeBootstrapError) as caught:
        encode_native_bootstrap(NativeWorkerBootstrap(
            native_execution=original.native_execution,
            worker_credential=secret,
        ))
    assert secret not in str(caught.value)
