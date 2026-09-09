from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_snapshot import (
    ORIGIN,
    _lock,
    _setup,
    snapshot_module,
)


@pytest.mark.parametrize("phase", ["prepare", "recheck"])
async def test_inventory_requires_permanent_insert_retirement_guard(
    registry_authority_session, registry_issuer, phase,
):
    factory = registry_authority_session
    module = snapshot_module()
    _, attempt, _ = await _setup(factory, registry_issuer)
    prepared = await module.prepare_attempt_retirement_inventory(
        factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    async with factory() as writer:
        await writer.execute(text(
            "DROP TRIGGER IF EXISTS task_image_registry_credentials_not_retired "
            "ON public.task_image_registry_credentials"
        ))
        await writer.commit()
    with pytest.raises(module.RetirementInventoryUnavailableError, match="retirement guard"):
        if phase == "prepare":
            await module.prepare_attempt_retirement_inventory(
                factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
            )
        else:
            async with factory() as session:
                await _lock(session, prepared)
                await module.revalidate_retirement_inventory(session, prepared=prepared)


@pytest.mark.parametrize("drift", [
    "disabled", "conditional", "deferrable", "before", "arguments",
    "stable", "invoker", "search_path", "row_security",
])
async def test_inventory_rejects_weakened_insert_guard(
    registry_authority_session, registry_issuer, drift,
):
    factory = registry_authority_session
    module = snapshot_module()
    _, attempt, _ = await _setup(factory, registry_issuer)
    async with factory() as writer:
        table = "public.task_image_registry_credentials"
        trigger = "task_image_registry_credentials_not_retired"
        function = "public.task_image_registry_reject_retired_attempt"
        if drift == "disabled":
            sql = f"ALTER TABLE {table} DISABLE TRIGGER {trigger}"
        elif drift in {"conditional", "deferrable", "before", "arguments"}:
            await writer.execute(text(f"DROP TRIGGER {trigger} ON {table}"))
            constraint = "CONSTRAINT " if drift == "deferrable" else ""
            timing = "BEFORE" if drift == "before" else "AFTER"
            deferred = "DEFERRABLE INITIALLY IMMEDIATE" if drift == "deferrable" else ""
            condition = "WHEN (NEW.generation > 1)" if drift == "conditional" else ""
            argument = "'unexpected'" if drift == "arguments" else ""
            sql = (
                f"CREATE {constraint}TRIGGER {trigger} {timing} INSERT ON {table} "
                f"{deferred} FOR EACH ROW {condition} "
                f"EXECUTE FUNCTION {function}({argument})"
            )
        else:
            alteration = {
                "stable": "STABLE",
                "invoker": "SECURITY INVOKER",
                "search_path": "RESET search_path",
                "row_security": "SET row_security = on",
            }[drift]
            sql = f"ALTER FUNCTION {function}() {alteration}"
        await writer.execute(text(sql))
        await writer.commit()
    with pytest.raises(module.RetirementInventoryUnavailableError, match="retirement guard"):
        await module.prepare_attempt_retirement_inventory(
            factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
        )
