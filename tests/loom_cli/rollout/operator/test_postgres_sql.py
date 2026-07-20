from __future__ import annotations

import pytest

from loom_cli.rollout.operator.postgres_sql import single_line_sql


def test_single_line_sql_preserves_tokens_without_newline_argv() -> None:
    assert single_line_sql("""
        SELECT jsonb_build_object(
          'epoch', epoch
        )::text
        FROM staging_mutation_epochs;
    """) == ("SELECT jsonb_build_object( 'epoch', epoch )::text FROM staging_mutation_epochs;")


@pytest.mark.parametrize(
    "statement",
    (
        "",
        "SELECT 1; -- unsafe line scope",
        "SELECT /* unsafe */ 1;",
        "SELECT '\x00';",
    ),
)
def test_single_line_sql_rejects_unsafe_checked_in_statements(statement: str) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        single_line_sql(statement)
