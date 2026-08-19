from __future__ import annotations

import base64
import hashlib
import importlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from stat import S_IMODE
from types import SimpleNamespace
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import ValidationError

from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.global_execution_witness import (
    build_current_global_execution_witness_export,
    build_global_execution_witness_export,
    load_global_execution_signing_key,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionWitness,
    assert_legacy_scale_up_allowed,
)


def _settings(**changes: object) -> CapacityManagerSettings:
    values: dict[str, object] = {
        "principals_file": Path("/run/loom/principals.json"),
        "db_url_file": Path("/run/loom/database-url"),
        "expected_authority_incarnation": UUID(
            "00000000-0000-4000-8000-000000000123"
        ),
        "tls_cert_file": Path("/run/loom/server.pem"),
        "tls_key_file": Path("/run/loom/server-key.pem"),
        "tls_client_ca_file": Path("/run/loom/client-ca.pem"),
    }
    values.update(changes)
    return CapacityManagerSettings(**values)  # type: ignore[arg-type]


def test_signing_key_settings_are_an_exact_validated_pair() -> None:
    disabled = _settings()
    enabled = _settings(
        global_execution_signing_key_file=Path("/run/loom/global-execution-signing-key"),
        global_execution_signing_key_id="global-capacity-manager-2026-08",
    )

    assert disabled.global_execution_signing_key_file is None
    assert disabled.global_execution_signing_key_id is None
    assert enabled.global_execution_signing_key_file == Path(
        "/run/loom/global-execution-signing-key"
    )
    assert enabled.global_execution_signing_key_id == "global-capacity-manager-2026-08"

    with pytest.raises(ValidationError, match="configured together"):
        _settings(
            global_execution_signing_key_file=Path(
                "/run/loom/global-execution-signing-key"
            )
        )
    with pytest.raises(ValidationError, match="signing key id"):
        _settings(
            global_execution_signing_key_file=Path(
                "/run/loom/global-execution-signing-key"
            ),
            global_execution_signing_key_id="INVALID KEY ID",
        )


def test_manager_export_is_signed_pool_bound_and_consumer_verifiable() -> None:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([31]) * 32)
    expected_public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    expected_public_key_sha256 = hashlib.sha256(expected_public_key).hexdigest()

    encoded = build_global_execution_witness_export(
        private_key=private_key,
        signing_key_id="global-capacity-manager-2026-08",
        pool_id="oldlab",
        execution_epoch=0,
        execution_state="shadow",
        executable_new_capacity_ceiling=0,
        expires_at=datetime(2026, 8, 19, 20, 1, tzinfo=UTC),
    )

    assert encoded.endswith(b"\n")
    exported = json.loads(encoded)
    assert set(exported) == {
        "manager_public_key_base64",
        "manager_public_key_sha256",
        "schema_version",
        "witness",
    }
    assert exported["schema_version"] == 1
    assert base64.b64decode(
        exported["manager_public_key_base64"],
        validate=True,
    ) == expected_public_key
    assert exported["manager_public_key_sha256"] == expected_public_key_sha256

    witness = GlobalExecutionWitness.from_mapping(
        exported["witness"],
        public_key=private_key.public_key(),
        expected_public_key_sha256=expected_public_key_sha256,
    )
    assert_legacy_scale_up_allowed(
        witness,
        expected_authority="global-capacity-manager",
        expected_pool_id="oldlab",
        now=datetime(2026, 8, 19, 20, 0, tzinfo=UTC),
    )


def test_signing_key_loader_requires_one_owner_only_raw_ed25519_key(tmp_path: Path) -> None:
    key_path = tmp_path / "manager-ed25519.key"
    key_path.write_bytes(bytes([41]) * 32)
    key_path.chmod(0o600)

    loaded = load_global_execution_signing_key(key_path)

    assert loaded.private_bytes_raw() == bytes([41]) * 32
    assert S_IMODE(key_path.stat().st_mode) == 0o600

    key_path.chmod(0o640)
    with pytest.raises(ValueError, match="owner-only"):
        load_global_execution_signing_key(key_path)


class _AuthorityResult:
    def __init__(self, authority: object, database_now: datetime) -> None:
        self._authority = authority
        self._database_now = database_now

    def one(self) -> tuple[object, datetime]:
        return self._authority, self._database_now


class _AuthoritySession:
    def __init__(self, authority: object, database_now: datetime) -> None:
        self._authority = authority
        self._database_now = database_now
        self.statements: list[object] = []

    async def execute(self, statement: object) -> _AuthorityResult:
        self.statements.append(statement)
        return _AuthorityResult(self._authority, self._database_now)


@pytest.mark.asyncio
async def test_current_export_uses_database_time_and_exact_authority_state() -> None:
    authority_incarnation = UUID("00000000-0000-4000-8000-000000000123")
    database_now = datetime(2026, 8, 19, 20, 0, tzinfo=UTC)
    session = _AuthoritySession(
        SimpleNamespace(
            authority_incarnation=authority_incarnation,
            execution_epoch=0,
            execution_state="shadow",
            executable_new_capacity_ceiling=0,
        ),
        database_now,
    )
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([43]) * 32)

    encoded = await build_current_global_execution_witness_export(
        session,
        private_key=private_key,
        signing_key_id="global-capacity-manager-2026-08",
        expected_authority_incarnation=authority_incarnation,
        pool_id="gb10",
        ttl=timedelta(seconds=30),
    )

    assert len(session.statements) == 1
    exported = json.loads(encoded)
    witness = GlobalExecutionWitness.from_mapping(
        exported["witness"],
        public_key=private_key.public_key(),
        expected_public_key_sha256=exported["manager_public_key_sha256"],
    )
    assert witness.pool_id == "gb10"
    assert witness.execution_epoch == 0
    assert witness.execution_state == "shadow"
    assert witness.executable_new_capacity_ceiling == 0
    assert witness.expires_at == database_now + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_current_export_refuses_a_different_authority_incarnation() -> None:
    session = _AuthoritySession(
        SimpleNamespace(
            authority_incarnation=UUID("00000000-0000-4000-8000-000000000124"),
            execution_epoch=0,
            execution_state="shadow",
            executable_new_capacity_ceiling=0,
        ),
        datetime(2026, 8, 19, 20, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="incarnation"):
        await build_current_global_execution_witness_export(
            session,
            private_key=Ed25519PrivateKey.from_private_bytes(bytes([47]) * 32),
            signing_key_id="global-capacity-manager-2026-08",
            expected_authority_incarnation=UUID(
                "00000000-0000-4000-8000-000000000123"
            ),
            pool_id="oldlab",
            ttl=timedelta(seconds=30),
        )


def test_module_entrypoint_emits_only_the_bounded_export(
    monkeypatch: pytest.MonkeyPatch,
    capsysbinary: pytest.CaptureFixture[bytes],
) -> None:
    module = importlib.import_module("loom_capacity_manager.global_execution_witness")
    expected = b'{"schema_version":1}\n'
    captured: dict[str, object] = {}

    async def export(settings: object, *, pool_id: str) -> bytes:
        captured["settings"] = settings
        captured["pool_id"] = pool_id
        return expected

    sentinel_settings = object()
    monkeypatch.setattr(module, "CapacityManagerSettings", lambda: sentinel_settings)
    monkeypatch.setattr(module, "export_global_execution_witness", export)

    module.main(["--pool-id", "oldlab"])

    output = capsysbinary.readouterr()
    assert output.out == expected
    assert output.err == b""
    assert captured == {"settings": sentinel_settings, "pool_id": "oldlab"}


def test_module_entrypoint_does_not_leak_export_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = importlib.import_module("loom_capacity_manager.global_execution_witness")
    secret = "postgresql://secret-password@database/capacity"

    async def fail(_settings: object, *, pool_id: str) -> bytes:
        raise RuntimeError(f"could not use {secret} for {pool_id}")

    monkeypatch.setattr(module, "CapacityManagerSettings", lambda: object())
    monkeypatch.setattr(module, "export_global_execution_witness", fail)

    with pytest.raises(SystemExit) as caught:
        module.main(["--pool-id", "gb10"])

    output = capsys.readouterr()
    assert caught.value.code == 1
    assert output.out == ""
    assert output.err == "error: global execution witness export failed safely\n"
    assert secret not in output.err
