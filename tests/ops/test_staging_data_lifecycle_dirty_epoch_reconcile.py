from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest

from loom.data_lifecycle_dirty_epoch_reconcile import build_dirty_epoch_reconcile_plan
from loom.data_lifecycle_gc import GcScope
from loom.staging_mutation_epoch import (
    MutationEpochState,
    ProtectedMutationClass,
)

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/staging_data_lifecycle_dirty_epoch_reconcile.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dirty_epoch_reconcile_script", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Engine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _plan():
    return build_dirty_epoch_reconcile_plan(
        scope=GcScope(environment="staging", namespace="loom-staging"),
        schema_revision="0075",
        state_fingerprint="a" * 64,
        epoch_count=0,
        epoch_event_count=0,
        gc_authority_count=0,
        gc_item_count=0,
        gc_run_count=0,
        authority_count=1,
        object_count=0,
        capacity_count=1,
        unsafe_authority_count=0,
        unsafe_object_count=0,
        execution_counts=(
            ("artifacts", 0, 0),
            ("batches", 0, 0),
            ("llm_calls", 0, 0),
            ("trial_events", 0, 0),
            ("trials", 1, 0),
        ),
    )


def test_inventory_prints_digest_bound_plan(monkeypatch, capsys) -> None:
    module = _load()
    engine = Engine()
    plan = _plan()

    class Reconciler:
        def __init__(self, exact_engine) -> None:
            assert exact_engine is engine

        def inventory(self, *, scope):
            assert scope == plan.scope
            return plan

    monkeypatch.setattr(module, "load_lifecycle_database_runtime", lambda: object())
    monkeypatch.setattr(module, "build_lifecycle_engine", lambda _runtime: engine)
    monkeypatch.setattr(module, "SqlAlchemyDirtyEpochReconciler", Reconciler)

    assert module.main(["inventory", "--namespace", "loom-staging"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["action"] == "inventory"
    assert payload["inventory_digest"] == plan.inventory_digest
    assert payload["applied"] is False
    assert engine.disposed


def test_apply_echoes_exact_epoch_evidence(monkeypatch, capsys) -> None:
    module = _load()
    engine = Engine()
    plan = _plan()
    state = MutationEpochState(
        environment="staging",
        namespace="loom-staging",
        epoch=1,
        mutation_class=ProtectedMutationClass.OBJECT_REWRITE,
        request_id="req-dirty-epoch-apply",
        evidence_sha256=plan.inventory_digest,
        updated_at=datetime(2026, 8, 7, 21, 0, tzinfo=UTC),
    )

    class Reconciler:
        def __init__(self, exact_engine) -> None:
            assert exact_engine is engine

        def inventory(self, *, scope):
            assert scope == plan.scope
            return plan

        def apply(self, **kwargs):
            assert kwargs == {
                "plan": plan,
                "approved_inventory_digest": plan.inventory_digest,
                "request_id": "req-dirty-epoch-apply",
            }
            return state

    monkeypatch.setattr(module, "load_lifecycle_database_runtime", lambda: object())
    monkeypatch.setattr(module, "build_lifecycle_engine", lambda _runtime: engine)
    monkeypatch.setattr(module, "SqlAlchemyDirtyEpochReconciler", Reconciler)

    assert module.main(
        [
            "apply",
            "--namespace",
            "loom-staging",
            "--requested-by",
            "qianyi",
            "--request-id",
            "req-dirty-epoch-apply",
            "--approved-inventory-digest",
            plan.inventory_digest,
        ]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["applied"] is True
    assert payload["epoch"] == 1
    assert payload["epoch_evidence_sha256"] == plan.inventory_digest
    assert payload["requested_by"] == "qianyi"
    assert engine.disposed


def test_apply_reserves_output_before_opening_database(monkeypatch, tmp_path: Path) -> None:
    module = _load()
    output = tmp_path / "existing.json"
    output.write_text("existing", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "load_lifecycle_database_runtime",
        lambda: (_ for _ in ()).throw(AssertionError("database opened before output reserve")),
    )

    with pytest.raises(FileExistsError):
        module.main(
            [
                "apply",
                "--namespace",
                "loom-staging",
                "--requested-by",
                "qianyi",
                "--request-id",
                "req-dirty-epoch-apply",
                "--approved-inventory-digest",
                "a" * 64,
                "--output",
                str(output),
            ]
        )

    assert output.read_text(encoding="utf-8") == "existing"


def test_apply_reports_post_commit_output_failure(monkeypatch, capsys) -> None:
    module = _load()
    engine = Engine()
    plan = _plan()
    state = MutationEpochState(
        environment="staging",
        namespace="loom-staging",
        epoch=1,
        mutation_class=ProtectedMutationClass.OBJECT_REWRITE,
        request_id="req-dirty-epoch-apply",
        evidence_sha256=plan.inventory_digest,
        updated_at=datetime(2026, 8, 7, 21, 0, tzinfo=UTC),
    )

    class Reconciler:
        def __init__(self, _engine) -> None:  # type: ignore[no-untyped-def]
            pass

        def inventory(self, *, scope):  # type: ignore[no-untyped-def]
            return plan

        def apply(self, **_kwargs):  # type: ignore[no-untyped-def]
            return state

    monkeypatch.setattr(module, "load_lifecycle_database_runtime", lambda: object())
    monkeypatch.setattr(module, "build_lifecycle_engine", lambda _runtime: engine)
    monkeypatch.setattr(module, "SqlAlchemyDirtyEpochReconciler", Reconciler)
    monkeypatch.setattr(
        module,
        "_write",
        lambda _document, _descriptor: (_ for _ in ()).throw(OSError("disk full")),
    )

    assert module.main(
        [
            "apply",
            "--namespace",
            "loom-staging",
            "--requested-by",
            "qianyi",
            "--request-id",
            "req-dirty-epoch-apply",
            "--approved-inventory-digest",
            plan.inventory_digest,
        ]
    ) == 3
    assert "database mutation succeeded" in capsys.readouterr().err
    assert engine.disposed
