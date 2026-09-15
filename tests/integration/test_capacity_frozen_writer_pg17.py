"""Exercise protected writer SQL on PostgreSQL 17 as well as the default 16 lane."""

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from loom.application_schema_reference import application_reference_postgres_image
from tests.integration.conftest import capacity_guard_template_database as _guard_template
from tests.integration.test_capacity_frozen_claim import (
    test_authenticated_assigned_claim_survives_trial_writer_freeze as test_claim,
)
from tests.integration.test_capacity_frozen_output import (
    test_frozen_output_preserves_prior_version_and_synchronizes_lineage as test_output_versions,
)
from tests.integration.test_capacity_frozen_output import (
    test_frozen_output_rejection_rolls_back_every_projection as test_output_rejection,
)
from tests.integration.test_capacity_frozen_output import (
    test_protected_output_publishes_artifact_and_lifecycle_atomically as test_output,
)
from tests.integration.test_capacity_frozen_pending_cancel import (
    test_user_pending_cancellation_and_replay_survive_frozen_writer as test_pending_cancel,
)
from tests.integration.test_capacity_frozen_state import (
    test_authenticated_state_report_survives_trial_writer_freeze as test_state,
)
from tests.integration.test_capacity_frozen_state import (
    test_frozen_terminal_report_commits_family_decision as test_family,
)
from tests.integration.test_capacity_frozen_state import (
    test_materializing_terminal_report_closes_exact_claim as test_materializing,
)


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    # The guard template provisions its own application database from template0.
    with PostgresContainer(application_reference_postgres_image(postgres_major=17)) as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")


@pytest.fixture(scope="session")
def capacity_guard_template_database(postgres_url: str):
    # A separate fixture identity prevents caching this PG17 template as the
    # global PG16 template when both modules run in the same pytest process.
    yield from _guard_template.__wrapped__(postgres_url)


@pytest.fixture(autouse=True)
def _require_postgres_17(capacity_guard_database):
    engine = create_engine(capacity_guard_database["admin_url"])
    try:
        with engine.connect() as connection:
            assert int(connection.execute(text("SHOW server_version_num")).scalar_one()) // 10000 == 17
    finally:
        engine.dispose()


__all__ = [
    "test_claim",
    "test_family",
    "test_materializing",
    "test_output",
    "test_output_rejection",
    "test_output_versions",
    "test_pending_cancel",
    "test_state",
]
