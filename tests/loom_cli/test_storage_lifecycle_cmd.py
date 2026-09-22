"""Exercise lifecycle policy validation through the public CLI dispatcher."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.cluster_cmd import dispatch


@pytest.mark.parametrize("backend", ["s3", "minio", "r2", "b2", "wasabi"])
def test_lifecycle_dry_run_needs_no_credentials(tmp_path: Path, capsys, backend: str) -> None:
    policy = tmp_path / "policy.toml"
    policy.write_text(f'backend = "{backend}"\n[[retention]]\nbucket = "artifacts"\nstrategy = "expire_after_days"\ndays = 30\n')
    assert dispatch(["bootstrap-storage-lifecycle", "--config", str(policy), "--dry-run"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["backend"] == backend
    assert result["lifecycle"]["artifacts"]["Rules"][0]["Expiration"] == {"Days": 30}


@pytest.mark.parametrize("body", [
    'backend = "gcs"',
    'backend = "s3"\nretentoin = []',
    'backend = "s3"\n[[retention]]\nbucket = "artifacts"',
    'backend = "s3"\n[[retention]]\nbucket = "artifacts"\nstrategy = "expire_after_day"',
])
def test_invalid_policy_returns_cli_error(tmp_path: Path, capsys, body: str) -> None:
    policy = tmp_path / "policy.toml"
    policy.write_text(body)
    assert dispatch(["bootstrap-storage-lifecycle", "--config", str(policy), "--dry-run"]) == 2
    assert "error:" in capsys.readouterr().err


def test_live_apply_uses_configured_client_and_closes_it(tmp_path: Path, monkeypatch, capsys) -> None:
    policy = tmp_path / "policy.toml"
    policy.write_text('backend = "s3"\n[[retention]]\nbucket = "artifacts"\nstrategy = "expire_after_days"\ndays = 30\n')
    monkeypatch.setenv("LOOM_SVC_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("LOOM_SVC_STORAGE_AUTH_KIND", "ambient")
    calls = []
    class Client:
        closed = False
        def put_bucket_lifecycle_configuration(self, **kwargs):
            calls.append(kwargs)
        def close(self):
            self.closed = True
    client = Client()
    monkeypatch.setattr("loom.storage_credentials.build_s3_client", lambda **kwargs: client)
    assert dispatch(["bootstrap-storage-lifecycle", "--config", str(policy)]) == 0
    assert calls[0]["Bucket"] == "artifacts"
    assert client.closed
    assert "1 bucket(s)" in capsys.readouterr().out
