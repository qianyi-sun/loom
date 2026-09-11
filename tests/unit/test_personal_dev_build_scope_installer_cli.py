"""Pinned installer entrypoint exposes only non-secret publication metadata."""

import json
from hashlib import sha256

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from loom_service.personal_dev_build_management import BuildManagementServiceConfigV1
from tests.integration.test_personal_dev_build_scope_installer import installer_config
from tests.unit.test_capacity_agent_client import _owner_file


@pytest.mark.parametrize("boundary", ["exact", "hash", "noncanonical", "run-failure"])
def test_installer_cli_requires_pinned_canonical_config_and_redacts_failure(tmp_path, monkeypatch, capsys, boundary):
    module, config = installer_config(tmp_path, "private_build_owner")
    wire = canonical_bytes(config)
    if boundary == "noncanonical":
        wire += b"\n"
    path = _owner_file(tmp_path / "installer.json", wire)
    digest = "e" * 64 if boundary == "hash" else sha256(wire).hexdigest()
    ran = []

    async def run(prepared):
        ran.append(prepared)
        if boundary == "run-failure":
            raise ValueError("postgresql://DO-NOT-PRINT-DATABASE-PASSWORD test-only-private-membership-manager")
        return BuildManagementServiceConfigV1(mode="recovery-only", scopes=(prepared.scope,))

    monkeypatch.setattr(module, "_run", run)
    result = module.main(["--config-file", str(path), "--config-sha256", digest])
    captured = capsys.readouterr()
    assert "DO-NOT-PRINT" not in captured.err + captured.out
    assert "test-only" not in captured.err + captured.out
    if boundary == "exact":
        assert result == 0 and len(ran) == 1
        payload = json.loads(captured.out)
        assert payload["mode"] == "recovery-only"
        assert payload["management_config_file"] == str(tmp_path / "registry" / f'management-{payload["management_config_sha256"]}.json')
        assert payload["management_config_sha256"] == sha256(canonical_bytes(
            BuildManagementServiceConfigV1(mode="recovery-only", scopes=(ran[0].scope,)))).hexdigest()
    else:
        assert result == 1 and not captured.out
        assert "unconfirmed" in captured.err
        assert len(ran) == int(boundary == "run-failure")
