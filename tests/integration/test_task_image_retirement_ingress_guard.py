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
