from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from loom.personal_dev_scanner_cache import (
    PersonalDevScannerCacheError,
    load_personal_dev_scanner_cache_lock,
)

_ROOT = Path(__file__).resolve().parents[2]
_LOCK = _ROOT / "deploy/dev-fleet/personal-dev-scanner-cache-lock.json"


def _value() -> dict[str, Any]:
    return json.loads(_LOCK.read_bytes())


def _write(tmp_path: Path, value: object) -> Path:
    path = tmp_path / "scanner-cache-lock.json"
    path.write_bytes(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    )
    return path


def test_checked_in_scanner_cache_lock_is_exact() -> None:
    assert not _LOCK.read_bytes().endswith(b"\n")
    lock = load_personal_dev_scanner_cache_lock(_LOCK)

    assert lock.schema_version == 1
    assert lock.trivy_version == "v0.70.0"
    assert lock.binary_sha256 == {
        "linux/amd64": "379d59f24a4a828c55de5f0b91b6805cc35d13580180b658820e648611256166",
        "linux/arm64": "5bf6066f08c972e0575660eaeb87b4f1bac0e527076dcbf88184bc9baa353f65",
    }
    assert lock.database.image == (
        "ghcr.io/aquasecurity/trivy-db@sha256:"
        "01edd081af12fd613776b0db66ac23ce62c9d25802d8ee57671394c10ca3530b"
    )
    assert lock.database.layer_sha256 == (
        "cafb664d1c10b65e06b317f86171d65ed1f17b1f4de594a7232e16c0848f3590"
    )
    assert lock.java_database.image == (
        "ghcr.io/aquasecurity/trivy-java-db@sha256:"
        "58ef30d104106166d34f36c9861f2c5eb88d3279341fd4838bb5694d8998c436"
    )
    assert lock.java_database.layer_sha256 == (
        "bcc9ee0a8aa79524502cf892eda69e2180b54a3c7bd54c874b564201d2bdfc10"
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: {**value, "unknown": True},
        lambda value: {key: item for key, item in value.items() if key != "database"},
        lambda value: {**value, "schema_version": 2},
        lambda value: {**value, "trivy_version": "v0.73.0"},
        lambda value: {
            **value,
            "binary_sha256": {"linux/amd64": "1" * 64},
        },
        lambda value: {
            **value,
            "binary_sha256": {**value["binary_sha256"], "linux/s390x": "1" * 64},
        },
        lambda value: {
            **value,
            "binary_sha256": {**value["binary_sha256"], "linux/amd64": "0" * 64},
        },
        lambda value: {
            **value,
            "database": {**value["database"], "image": "ghcr.io/aquasecurity/trivy-db:2"},
        },
        lambda value: {
            **value,
            "database": {
                **value["database"],
                "image": "ghcr.io/example/trivy-db@sha256:" + "1" * 64,
            },
        },
        lambda value: {
            **value,
            "database": {**value["database"], "layer_sha256": "A" * 64},
        },
        lambda value: {
            **value,
            "java_database": {**value["java_database"], "layer_sha256": "1" * 63},
        },
        lambda value: {
            **value,
            "java_database": {**value["java_database"], "extra": "1" * 64},
        },
    ],
)
def test_scanner_cache_lock_rejects_schema_or_digest_drift(
    tmp_path: Path,
    mutate: Any,
) -> None:
    path = _write(tmp_path, mutate(_value()))

    with pytest.raises(PersonalDevScannerCacheError, match="lock is invalid"):
        load_personal_dev_scanner_cache_lock(path)


@pytest.mark.parametrize("variant", ["pretty", "duplicate", "trailing-newline"])
def test_scanner_cache_lock_rejects_noncanonical_json(tmp_path: Path, variant: str) -> None:
    value = _value()
    path = tmp_path / "scanner-cache-lock.json"
    if variant == "pretty":
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="ascii")
    elif variant == "duplicate":
        path.write_bytes(_LOCK.read_bytes().replace(b'{"binary_sha256":', b'{"x":1,"x":2,"binary_sha256":'))
    else:
        path.write_bytes(_LOCK.read_bytes() + b"\n")

    with pytest.raises(PersonalDevScannerCacheError, match="lock is invalid"):
        load_personal_dev_scanner_cache_lock(path)


@pytest.mark.parametrize("variant", ["symlink", "hardlink", "empty", "oversize"])
def test_scanner_cache_lock_rejects_unstable_or_unbounded_file(
    tmp_path: Path,
    variant: str,
) -> None:
    source = tmp_path / "source.json"
    source.write_bytes(_LOCK.read_bytes())
    path = source
    if variant == "symlink":
        path = tmp_path / "linked.json"
        path.symlink_to(source)
    elif variant == "hardlink":
        path = tmp_path / "hardlinked.json"
        os.link(source, path)
    elif variant == "empty":
        source.write_bytes(b"")
    else:
        source.write_bytes(b"x" * (1024 * 1024 + 1))

    with pytest.raises(PersonalDevScannerCacheError, match="lock is invalid"):
        load_personal_dev_scanner_cache_lock(path)


def test_scanner_cache_lock_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    path = tmp_path / "scanner-cache-lock.json"
    os.mkfifo(path)
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path\n"
            "from loom.personal_dev_scanner_cache import (\n"
            "    PersonalDevScannerCacheError, load_personal_dev_scanner_cache_lock,\n"
            ")\n"
            "try:\n"
            "    load_personal_dev_scanner_cache_lock(Path(__import__('sys').argv[1]))\n"
            "except PersonalDevScannerCacheError:\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
        str(path),
    ]
    environment = {
        **os.environ,
        "PYTHONPATH": f"{_ROOT / 'src'}:{_ROOT}",
    }

    try:
        result = subprocess.run(command, env=environment, timeout=2, check=False)
    except subprocess.TimeoutExpired:
        pytest.fail("scanner cache lock FIFO blocked before type validation")

    assert result.returncode == 0
