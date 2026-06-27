from __future__ import annotations

from sqlalchemy import create_engine, inspect, text


def test_0044_adds_username_password_account_schema(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        inspector = inspect(conn)

        user_columns = {column["name"]: column for column in inspector.get_columns("users")}
        assert user_columns["email"]["nullable"] is True
        assert user_columns["username"]["nullable"] is False
        assert user_columns["username_normalized"]["nullable"] is False
        assert user_columns["password_hash"]["nullable"] is True
        assert user_columns["password_set_at"]["nullable"] is True
        assert user_columns["disabled_at"]["nullable"] is True
        assert user_columns["status"]["nullable"] is False

        assert "user_registration_requests" in inspector.get_table_names()
        assert "account_action_tokens" in inspector.get_table_names()
        assert "password_reset_requests" in inspector.get_table_names()

        index_names = {index["name"] for index in inspector.get_indexes("users")}
        assert "users_username_normalized_uidx" in index_names

        batch_columns = {column["name"]: column for column in inspector.get_columns("batches")}
        trial_columns = {column["name"]: column for column in inspector.get_columns("trials")}
        assert batch_columns["submitted_by_user_id"]["nullable"] is True
        assert trial_columns["submitted_by_user_id"]["nullable"] is True

        admin_rows = conn.execute(
            text(
                """
                SELECT u.username, t.name
                  FROM users u
                  JOIN team_memberships m ON m.user_id = u.id
                  JOIN teams t ON t.id = m.team_id
                 WHERE u.username_normalized IN ('qianyi', 'hongjian')
                 ORDER BY u.username_normalized
                """,
            ),
        ).all()

    engine.dispose()

    assert [row.username for row in admin_rows] == ["Hongjian", "Qianyi"]
    assert {row.name for row in admin_rows} == {"admin"}
