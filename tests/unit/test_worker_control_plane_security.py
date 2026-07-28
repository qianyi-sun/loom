from __future__ import annotations

from pathlib import Path

import pytest

from loom_worker.config import WorkerSettings
from loom_worker.control_plane_security import (
    WorkerSecurityError,
    resolve_worker_minio_credentials,
    resolve_worker_token,
)


def _settings(**overrides: object) -> WorkerSettings:
    values: dict[str, object] = {
        "_env_file": None,
        "control_plane_url": "https://192.168.50.14:26080",
        "gateway_url": "http://gateway:9100",
        "minio_access_key": "access",
        "minio_secret_key": "secret",
    }
    values.update(overrides)
    return WorkerSettings(**values)


def test_worker_token_file_is_loaded_without_inline_secret(tmp_path: Path) -> None:
    token_file = tmp_path / "worker-token"
    token_file.write_text("loom_w_candidate_secret\n", encoding="utf-8")
    token_file.chmod(0o600)

    assert resolve_worker_token(_settings(token_file=token_file)) == "loom_w_candidate_secret"


def test_worker_token_sources_are_exclusive(tmp_path: Path) -> None:
    token_file = tmp_path / "worker-token"
    token_file.write_text("loom_w_candidate_secret\n", encoding="utf-8")
    token_file.chmod(0o600)

    with pytest.raises(WorkerSecurityError, match="ambiguous"):
        resolve_worker_token(
            _settings(token="loom_w_inline", token_file=token_file),
        )


@pytest.mark.parametrize(
    "content,mode",
    [
        ("not-a-worker-token\n", 0o600),
        ("loom_w_secret\nextra\n", 0o600),
        ("loom_w_secret\n\n", 0o600),
        ("loom_w_secret\n", 0o640),
    ],
)
def test_worker_token_file_rejects_malformed_or_readable_secret(
    tmp_path: Path,
    content: str,
    mode: int,
) -> None:
    token_file = tmp_path / "worker-token"
    token_file.write_text(content, encoding="utf-8")
    token_file.chmod(mode)

    with pytest.raises(WorkerSecurityError):
        resolve_worker_token(_settings(token_file=token_file))


def test_worker_minio_files_are_loaded_without_inline_secrets(tmp_path: Path) -> None:
    access_file = tmp_path / "minio-access-key"
    secret_file = tmp_path / "minio-secret-key"
    access_file.write_text("candidate-access\n", encoding="utf-8")
    secret_file.write_text("candidate-secret\n", encoding="utf-8")
    access_file.chmod(0o600)
    secret_file.chmod(0o600)

    assert resolve_worker_minio_credentials(
        _settings(
            minio_access_key="",
            minio_secret_key="",
            minio_access_key_file=access_file,
            minio_secret_key_file=secret_file,
        ),
    ) == ("candidate-access", "candidate-secret")


def test_worker_minio_sources_are_exclusive(tmp_path: Path) -> None:
    access_file = tmp_path / "minio-access-key"
    secret_file = tmp_path / "minio-secret-key"
    access_file.write_text("candidate-access\n", encoding="utf-8")
    secret_file.write_text("candidate-secret\n", encoding="utf-8")
    access_file.chmod(0o600)
    secret_file.chmod(0o600)

    with pytest.raises(WorkerSecurityError, match="ambiguous"):
        resolve_worker_minio_credentials(
            _settings(
                minio_access_key_file=access_file,
                minio_secret_key_file=secret_file,
            ),
        )


def test_worker_minio_file_pair_is_required(tmp_path: Path) -> None:
    access_file = tmp_path / "minio-access-key"
    access_file.write_text("candidate-access\n", encoding="utf-8")
    access_file.chmod(0o600)

    with pytest.raises(WorkerSecurityError, match="configured together"):
        resolve_worker_minio_credentials(
            _settings(
                minio_access_key="",
                minio_secret_key="",
                minio_access_key_file=access_file,
            ),
        )
