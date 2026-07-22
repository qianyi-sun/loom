from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from loom_cli.rollout.operator.backup_retirement import (
    BackupPayloadActivator,
    BackupPayloadRetirer,
)
from loom_cli.rollout.operator.deep_preflight_authority import DeepPreflightAuthority
from loom_cli.rollout.operator.installed_detached_preflight import (
    build_installed_detached_preflight_runner,
)
from loom_cli.rollout.operator.store import RequestStore
from tests.loom_cli.rollout.operator.test_backup import make_config


class _Authority:
    def detached_orchestrator(self) -> object:
        return object()


def test_installed_runner_binds_reference_and_retirement_callbacks(tmp_path: Path) -> None:
    store = RequestStore(tmp_path / "request-store")
    runner = build_installed_detached_preflight_runner(
        make_config(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        store=store,
        now=lambda: datetime(2026, 7, 20, tzinfo=UTC),
        authority=cast(DeepPreflightAuthority, _Authority()),
    )

    assert runner.referenced_payload_ids() == frozenset()
    assert isinstance(runner.retire_payload, BackupPayloadRetirer)
    assert isinstance(runner.activate_payload, BackupPayloadActivator)
    assert runner.activate_payload.enforce_freshness is True
    assert runner.activate_payload.allow_metadata_fast_path is False
    assert isinstance(runner.recover_active_payload, BackupPayloadActivator)
    assert runner.recover_active_payload.enforce_freshness is False
    assert runner.recover_active_payload.allow_metadata_fast_path is False
    assert isinstance(runner.confirm_active_payload, BackupPayloadActivator)
    assert runner.confirm_active_payload.enforce_freshness is False
    assert runner.confirm_active_payload.allow_metadata_fast_path is True
