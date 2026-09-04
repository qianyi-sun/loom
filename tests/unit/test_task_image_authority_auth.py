from __future__ import annotations

import hashlib
import json
import os
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_task_image_authority import auth
from loom_task_image_authority.auth import (
    TaskImageAuthorityAuthorizationError,
    TaskImagePrincipalRegistryError,
    TaskImagePrincipalVerifier,
)

_BEARER = "phase2b1-test-node-bearer"
_BEARER_SHA256 = "49544425f5a2f7a5789fa74760173ba2db8476019ebf3ba4d20b3cf7ad775839"


def _registry() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "principals": [
            {
                "principal_id": "gb10-trt-gb10-1",
                "token_sha256": _BEARER_SHA256,
                "slurm_cluster_id": "gb10",
                "node_name": "trt-gb10-1",
                "scopes": ["task-image:attest", "task-image:project"],
            }
        ],
    }


def _write_registry(path: Path, value: dict[str, Any]) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_verifier_returns_exact_node_principal_without_retaining_plaintext(tmp_path: Path) -> None:
    path = _write_registry(tmp_path / "principals.json", _registry())

    verifier = TaskImagePrincipalVerifier.from_file(path)
    principal = verifier.verify_bearer(f"Bearer {_BEARER}")

    assert principal.model_dump(mode="json") == {
        "schema_version": 1,
        "principal_id": "gb10-trt-gb10-1",
        "slurm_cluster_id": "gb10",
        "node_name": "trt-gb10-1",
        "scopes": ["task-image:attest", "task-image:project"],
    }
    assert _BEARER not in repr(verifier)
    assert _BEARER not in repr(principal)


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        _BEARER,
        f"bearer {_BEARER}",
        f"Bearer  {_BEARER}",
        f"Bearer {_BEARER} ",
        "Bearer wrong-node-bearer",
        "Bearer token\ncontinued",
        "Bearer " + "x" * 4097,
    ],
)
def test_verifier_rejects_every_invalid_bearer_indistinguishably(
    tmp_path: Path,
    header: str | None,
) -> None:
    verifier = TaskImagePrincipalVerifier.from_file(
        _write_registry(tmp_path / "principals.json", _registry())
    )

    with pytest.raises(
        TaskImageAuthorityAuthorizationError,
        match=r"^invalid task-image authority credentials$",
    ) as caught:
        verifier.verify_bearer(header)

    assert _BEARER not in str(caught.value)
    assert "wrong-node-bearer" not in str(caught.value)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.__setitem__("unknown", True), "unknown or invalid"),
        (lambda value: value.__setitem__("schema_version", 2), "unknown or invalid"),
        (
            lambda value: value["principals"][0].__setitem__("token_sha256", "0" * 64),
            "invalid token digest",
        ),
        (
            lambda value: value["principals"][0].__setitem__(
                "token_sha256", _BEARER_SHA256.upper()
            ),
            "invalid token digest",
        ),
        (
            lambda value: value["principals"][0].__setitem__(
                "scopes", ["task-image:project", "task-image:project"]
            ),
            "invalid principal scope",
        ),
        (
            lambda value: value["principals"][0].__setitem__(
                "scopes", ["task-image:project", "task-image:attest"]
            ),
            "invalid principal scope",
        ),
        (
            lambda value: value["principals"][0].__setitem__("node_name", "trt-eai-oldlab-3"),
            "cluster and node disagree",
        ),
        (lambda value: value.__setitem__("principals", []), "unknown or invalid"),
    ],
)
def test_registry_rejects_malformed_authority(
    tmp_path: Path,
    mutate: Any,
    message: str,
) -> None:
    value = _registry()
    mutate(value)
    path = _write_registry(tmp_path / "principals.json", value)

    with pytest.raises(TaskImagePrincipalRegistryError, match=message):
        TaskImagePrincipalVerifier.from_file(path)


@pytest.mark.parametrize("duplicate", ["principal_id", "token_sha256", "node"])
def test_registry_rejects_duplicate_identity_bindings(tmp_path: Path, duplicate: str) -> None:
    value = _registry()
    second = {
        "principal_id": "oldlab-trt-eai-oldlab-3",
        "token_sha256": hashlib.sha256(b"second-test-bearer").hexdigest(),
        "slurm_cluster_id": "oldlab",
        "node_name": "trt-eai-oldlab-3",
        "scopes": ["task-image:project"],
    }
    if duplicate == "principal_id":
        second["principal_id"] = value["principals"][0]["principal_id"]
    elif duplicate == "token_sha256":
        second["token_sha256"] = value["principals"][0]["token_sha256"]
    else:
        second["slurm_cluster_id"] = value["principals"][0]["slurm_cluster_id"]
        second["node_name"] = value["principals"][0]["node_name"]
    value["principals"].append(second)

    with pytest.raises(TaskImagePrincipalRegistryError, match=f"duplicate {duplicate}"):
        TaskImagePrincipalVerifier.from_file(
            _write_registry(tmp_path / "principals.json", value)
        )


def test_registry_requires_owner_only_regular_nonsymlink_file(tmp_path: Path) -> None:
    path = _write_registry(tmp_path / "principals.json", _registry())
    path.chmod(0o640)
    with pytest.raises(TaskImagePrincipalRegistryError, match="mode must be exactly 0600"):
        TaskImagePrincipalVerifier.from_file(path)

    path.chmod(0o600)
    link = tmp_path / "principals-link.json"
    link.symlink_to(path)
    with pytest.raises(TaskImagePrincipalRegistryError, match="regular nonsymlink"):
        TaskImagePrincipalVerifier.from_file(link)

    fifo = tmp_path / "principals.fifo"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(TaskImagePrincipalRegistryError, match="regular nonsymlink"):
        TaskImagePrincipalVerifier.from_file(fifo)


def test_registry_rejects_file_not_owned_by_current_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_registry(tmp_path / "principals.json", _registry())
    current_uid = os.getuid()
    monkeypatch.setattr(auth.os, "getuid", lambda: current_uid + 1)

    with pytest.raises(TaskImagePrincipalRegistryError, match="owned by the current uid"):
        TaskImagePrincipalVerifier.from_file(path)


def test_registry_rejects_metadata_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_registry(tmp_path / "principals.json", _registry())
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
            st_mtime_ns=metadata.st_mtime_ns + 1,
            st_ctime_ns=metadata.st_ctime_ns,
        )

    monkeypatch.setattr(auth.os, "fstat", changed_second_fstat)

    with pytest.raises(TaskImagePrincipalRegistryError, match="changed while reading"):
        TaskImagePrincipalVerifier.from_file(path)


def test_registry_rejects_invalid_json_and_oversize_payload(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_bytes(b"{")
    invalid.chmod(0o600)
    with pytest.raises(TaskImagePrincipalRegistryError, match="unknown or invalid"):
        TaskImagePrincipalVerifier.from_file(invalid)

    oversize = tmp_path / "oversize.json"
    oversize.write_bytes(b"{" + b" " * (1024 * 1024))
    oversize.chmod(0o600)
    with pytest.raises(TaskImagePrincipalRegistryError, match="exceeds maximum byte size"):
        TaskImagePrincipalVerifier.from_file(oversize)


def test_registry_validation_traceback_never_echoes_unknown_secret_input(
    tmp_path: Path,
) -> None:
    document = _registry()
    document["raw_bearer"] = _BEARER

    with pytest.raises(TaskImagePrincipalRegistryError) as caught:
        TaskImagePrincipalVerifier.from_file(
            _write_registry(tmp_path / "principals.json", document)
        )

    rendered = "".join(
        traceback.format_exception(
            caught.type,
            caught.value,
            caught.tb,
        )
    )
    assert _BEARER not in rendered
