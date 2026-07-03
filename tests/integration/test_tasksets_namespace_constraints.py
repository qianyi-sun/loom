"""DB CHECK constraints for user TaskSets (#242 sub-plan 1)."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID, uuid4

import pytest
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import IntegrityError

_CREATED_TASK_SET_IDS: set[str] = set()
_CREATED_TEAM_IDS: set[UUID] = set()


@pytest.fixture(autouse=True)
def _cleanup_created_tasksets(postgres_url: str) -> Iterator[None]:
    """Keep the session-scoped integration DB clean for later FK teardowns."""
    task_set_before = set(_CREATED_TASK_SET_IDS)
    team_before = set(_CREATED_TEAM_IDS)
    try:
        yield
    finally:
        task_set_ids = tuple(sorted(_CREATED_TASK_SET_IDS - task_set_before))
        team_ids = tuple(_CREATED_TEAM_IDS - team_before)
        if task_set_ids or team_ids:
            engine = create_engine(postgres_url)
            with engine.begin() as conn:
                if task_set_ids:
                    task_set_param = bindparam("task_set_ids", expanding=True)
                    for statement in (
                        text(
                            "DELETE FROM task_set_materialization_jobs "
                            "WHERE task_set_id IN :task_set_ids",
                        ).bindparams(task_set_param),
                        text(
                            "DELETE FROM task_set_manifests "
                            "WHERE task_set_id IN :task_set_ids",
                        ).bindparams(task_set_param),
                        text(
                            "DELETE FROM task_sets WHERE id IN :task_set_ids",
                        ).bindparams(task_set_param),
                    ):
                        conn.execute(statement, {"task_set_ids": task_set_ids})
                if team_ids:
                    conn.execute(
                        text("DELETE FROM teams WHERE id IN :team_ids").bindparams(
                            bindparam("team_ids", expanding=True),
                        ),
                        {"team_ids": team_ids},
                    )
            engine.dispose()


def _insert_team(conn, team_id: UUID | None = None) -> UUID:
    tid = team_id or uuid4()
    conn.execute(
        text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
        {"id": tid, "name": f"t-{tid}"},
    )
    _CREATED_TEAM_IDS.add(tid)
    return tid


def _insert_task_set(
    conn,
    *,
    team_id: UUID,
    slug: str = "my-tasks",
    task_set_id: str | None = None,
    visibility: str = "private",
    status: str = "materializing",
    intents: list[str] | None = None,
) -> str:
    ts_id = task_set_id or f"ts/{team_id}/{slug}"
    conn.execute(
        text(
            "INSERT INTO task_sets ("
            "id, owning_team_id, slug, display_name, visibility, status, "
            "intents, manifest_blob_uri"
            ") VALUES ("
            ":id, :team, :slug, 'My Tasks', :visibility, :status, "
            ":intents, 's3://bucket/manifest.yaml'"
            ")",
        ),
        {
            "id": ts_id,
            "team": team_id,
            "slug": slug,
            "visibility": visibility,
            "status": status,
            "intents": intents or ["trajectory_generation"],
        },
    )
    _CREATED_TASK_SET_IDS.add(ts_id)
    return ts_id


def test_valid_trajectory_only_task_set(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        team_id = _insert_team(conn)
        ts_id = _insert_task_set(conn, team_id=team_id)
        row = conn.execute(
            text("SELECT id, intents FROM task_sets WHERE id = :id"),
            {"id": ts_id},
        ).mappings().one()
    engine.dispose()
    assert row["intents"] == ["trajectory_generation"]


def test_valid_both_intents_task_set(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        team_id = _insert_team(conn, uuid4())
        ts_id = _insert_task_set(
            conn,
            team_id=team_id,
            slug="eval-tasks",
            intents=["trajectory_generation", "evaluation"],
        )
        row = conn.execute(
            text("SELECT intents FROM task_sets WHERE id = :id"),
            {"id": ts_id},
        ).mappings().one()
    engine.dispose()
    assert row["intents"] == ["trajectory_generation", "evaluation"]


def test_rejects_id_namespace_mismatch(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    "INSERT INTO task_sets ("
                    "id, owning_team_id, slug, display_name, status, intents, "
                    "manifest_blob_uri"
                    ") VALUES ("
                    "'ts/wrong-team/my-tasks', :team, 'my-tasks', 'X', "
                    "'materializing', ARRAY['trajectory_generation']::text[], "
                    "'s3://bucket/manifest.yaml'"
                    ")",
                ),
                {"team": team_id},
            )
    engine.dispose()


def test_rejects_path_traversal_slug(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            _insert_task_set(conn, team_id=team_id, slug="../escape")


def test_rejects_invalid_status(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            _insert_task_set(conn, team_id=team_id, status="bogus")


def test_rejects_invalid_visibility(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            _insert_task_set(conn, team_id=team_id, visibility="cluster")


def test_rejects_empty_intents(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    slug = "empty-intents"
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    "INSERT INTO task_sets ("
                    "id, owning_team_id, slug, display_name, status, intents, "
                    "manifest_blob_uri"
                    ") VALUES ("
                    ":id, :team, :slug, 'X', 'materializing', "
                    "ARRAY[]::text[], 's3://bucket/manifest.yaml'"
                    ")",
                ),
                {"id": f"ts/{team_id}/{slug}", "team": team_id, "slug": slug},
            )
    engine.dispose()


def test_rejects_invalid_intent_value(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        with pytest.raises(IntegrityError):
            _insert_task_set(conn, team_id=team_id, intents=["training"])


def test_rejects_duplicate_team_slug(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    with engine.begin() as conn:
        _insert_team(conn, team_id)
        _insert_task_set(conn, team_id=team_id, slug="dup")
        with pytest.raises(IntegrityError):
            _insert_task_set(
                conn,
                team_id=team_id,
                slug="dup",
                task_set_id=f"ts/{team_id}/dup-2",
            )
    engine.dispose()
