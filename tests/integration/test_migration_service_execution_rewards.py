"""Historical reward projection uses the ordinary Alembic/Postgres path."""

from __future__ import annotations

import copy
import json
from typing import Any
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from loom.terminal_result_semantics import aggregate_reward_scalar


def _result(rewards: Any, **changes: Any) -> dict[str, Any]:
    return {
        "schema_version": "loom.service-execution-trial-result.v1",
        "reward": rewards,
        "runtime_result": {"verifier_rewards": rewards, "status": "succeeded"},
        "usage": {"total_tokens": 61},
        **changes,
    }


def test_0133_repairs_only_missing_valid_matching_scores(
    isolated_migration_postgres_url: str,
) -> None:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url)
    command.downgrade(config, "0132")
    cases = {
        "missing": _result({"artifact_complete": 1.0}),
        "null": _result({"artifact_complete": 1.0}, aggregate_reward=None),
        "zero": _result({"passed": 0.0}),
        "multiple": _result({"a": 0.0, "b": 1.0}),
        "explicit_zero": _result({"passed": 1.0}, aggregate_reward=0.0),
        "explicit_value": _result({"passed": 0.0}, aggregate_reward=0.8),
        "explicit_text": _result({"passed": 1.0}, aggregate_reward="unknown"),
        "explicit_boolean": _result({"passed": 1.0}, aggregate_reward=False),
        "explicit_object": _result({"passed": 1.0}, aggregate_reward={"score": 1.0}),
        "empty": _result({}),
        "none": _result(None),
        "array_rewards": _result([1.0]),
        "boolean": _result({"passed": True}),
        "string": _result({"passed": "1"}),
        "nested": _result({"passed": {"value": 1.0}}),
        "mixed": _result({"a": 1.0, "b": "1"}),
        "nonfinite": _result({"passed": "NaN"}),
        "oversize": _result({"passed": 10**400}),
        "sum_overflow": _result({"a": 1e308, "b": 1e308}),
        "empty_key": _result({"": 1.0}),
        "long_key": _result({"x" * 257: 1.0}),
        "mismatched": _result(
            {"passed": 1.0}, runtime_result={"verifier_rewards": {"passed": 0.0}}
        ),
        "missing_runtime": _result({"passed": 1.0}, runtime_result={}),
        "string_runtime": _result({"passed": 1.0}, runtime_result="invalid"),
        "array_runtime": _result({"passed": 1.0}, runtime_result=[1.0]),
        "legacy": _result({"passed": 1.0}, schema_version="1"),
        "unrelated": {"reward": 1.0},
        "string_result": "invalid",
        "array_result": [1.0],
        "boolean_result": False,
        "no_result": None,
    }
    repaired = {"missing", "null", "zero", "multiple"}
    team_id, task_id = uuid4(), f"reward-migration/{uuid4()}"
    ids = {name: uuid4() for name in cases}
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"reward-migration-{team_id}"},
            )
            connection.execute(
                text("INSERT INTO tasks (id, config, checksum) VALUES (:id, '{}', :checksum)"),
                {"id": task_id, "checksum": "a" * 64},
            )
            for name, result in cases.items():
                connection.execute(
                    text("""
                    INSERT INTO trials (id, team_id, task_id, config, requires_caps, state,
                                        result, trajectory_index)
                    VALUES (:id, :team, :task, '{}', '{}', :state,
                            CAST(:result AS jsonb), '{"artifacts": ["retained"]}')
                """),
                    {
                        "id": ids[name],
                        "team": team_id,
                        "task": task_id,
                        "state": "succeeded" if result is not None else "failed",
                        "result": json.dumps(result) if result is not None else None,
                    },
                )

        def snapshot() -> dict[Any, Any]:
            with engine.connect() as connection:
                return dict(
                    connection.execute(
                        text("SELECT id, to_jsonb(trials) FROM trials WHERE team_id = :team"),
                        {"team": team_id},
                    ).all()
                )

        before = snapshot()
        expected = copy.deepcopy(before)
        for name in repaired:
            original = expected[ids[name]]["result"]
            original["aggregate_reward"] = aggregate_reward_scalar(original["reward"])
        command.upgrade(config, "0133")
        assert snapshot() == expected
        # Downgrading/re-upgrading cannot erase a score or rewrite explicit results.
        command.downgrade(config, "0132")
        assert snapshot() == expected
        command.upgrade(config, "0133")
        assert snapshot() == expected
    finally:
        engine.dispose()
