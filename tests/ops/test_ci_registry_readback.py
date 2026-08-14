from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest
from scripts import ci_registry_readback as readback

EXPECTED = "sha256:" + "a" * 64
REFERENCE = "ghcr.io/qianyi-sun/loom-worker:dev"


def _completed(
    stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.CompletedProcess((), returncode, stdout=stdout, stderr=stderr)


def test_digest_readback_retries_transport_and_stale_tag() -> None:
    results = iter(
        (
            _completed(stderr=b"temporary registry error", returncode=255),
            _completed(stdout=b"Name: x\nDigest: sha256:" + b"b" * 64 + b"\n"),
            _completed(stdout=f"Name: x\nDigest: {EXPECTED}\n".encode()),
        )
    )
    sleeps: list[float] = []

    observed = readback.read_digest(
        reference=REFERENCE,
        expected_digest=EXPECTED,
        attempts=3,
        delay_seconds=0.25,
        runner=lambda _command: next(results),
        sleeper=sleeps.append,
    )

    assert observed == EXPECTED
    assert sleeps == [0.25, 0.25]


def test_digest_readback_is_bounded_and_fail_closed() -> None:
    calls: list[Sequence[str]] = []

    def runner(command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
        calls.append(command)
        return _completed(stderr=b"registry unavailable", returncode=1)

    with pytest.raises(readback.RegistryReadbackError, match="did not converge"):
        readback.read_digest(
            reference=REFERENCE,
            expected_digest=EXPECTED,
            attempts=2,
            delay_seconds=0,
            runner=runner,
            sleeper=lambda _delay: None,
        )

    assert len(calls) == 2


def test_raw_readback_retries_digest_drift_and_writes_atomically(tmp_path: Path) -> None:
    payload = b'{"schemaVersion":2}\n'
    expected = "sha256:" + hashlib.sha256(payload).hexdigest()
    results = iter((_completed(stdout=b"stale"), _completed(stdout=payload)))
    output = tmp_path / "manifest.json"

    assert (
        readback.read_raw(
            reference="ghcr.io/qianyi-sun/loom-worker@" + expected,
            expected_digest=expected,
            output=output,
            attempts=2,
            delay_seconds=0,
            runner=lambda _command: next(results),
            sleeper=lambda _delay: None,
        )
        == output
    )
    assert output.read_bytes() == payload
    assert list(tmp_path.glob(".*.tmp")) == []


@pytest.mark.parametrize(
    ("reference", "digest", "attempts", "delay"),
    [
        ("docker.io/library/alpine:latest", EXPECTED, 1, 0),
        (REFERENCE, "sha256:" + "A" * 64, 1, 0),
        (REFERENCE, EXPECTED, 0, 0),
        (REFERENCE, EXPECTED, 1, 31),
    ],
)
def test_readback_rejects_open_or_unbounded_inputs(
    reference: str, digest: str, attempts: int, delay: float
) -> None:
    with pytest.raises(readback.RegistryReadbackError):
        readback.read_digest(
            reference=reference,
            expected_digest=digest,
            attempts=attempts,
            delay_seconds=delay,
        )
