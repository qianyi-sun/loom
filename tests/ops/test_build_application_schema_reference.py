"""Reference generation must agree independently and clean up on refusal."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import scripts.build_application_schema_reference as builder

from loom.application_schema_inventory import ApplicationSchemaInventory, ApplicationSchemaObject
from loom.application_schema_reference import application_schema_reference
from loom.dev_instance import derive_identity


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disagreement", "observation_error", "cancelled"])
async def test_reference_builder_cleans_its_container_on_failure(monkeypatch, failure) -> None:
    container = MagicMock()
    container.with_bind_ports.return_value = container
    container.__enter__.return_value = container
    container.get_connection_url.return_value = "postgresql+psycopg://localhost/isolated"
    factory = MagicMock(return_value=container)
    monkeypatch.setattr(builder, "PostgresContainer", factory)
    first = ApplicationSchemaInventory(16, ())
    second = ApplicationSchemaInventory(
        16, (ApplicationSchemaObject("routine", "unexpected", "f" * 64),)
    )
    if failure == "disagreement":
        effects = [first, second]
        error = RuntimeError
    elif failure == "observation_error":
        effects = [RuntimeError("observation refused")]
        error = RuntimeError
    else:
        effects = [asyncio.CancelledError()]
        error = asyncio.CancelledError
    observer = AsyncMock(side_effect=effects)
    monkeypatch.setattr(builder, "_observe_fresh_database", observer)
    with pytest.raises(error):
        await builder.build_application_schema_reference()
    container.with_bind_ports.assert_called_once_with(5432, ("127.0.0.1", None))
    container.__exit__.assert_called_once()
    assert factory.call_args.args == (builder.application_reference_postgres_image(),)
    assert factory.call_args.kwargs["driver"] == "psycopg"
    if failure == "disagreement":
        first_identity = observer.call_args_list[0].args[1]
        second_identity = observer.call_args_list[1].args[1]
        assert first_identity.database != second_identity.database
        assert first_identity.db_role != second_identity.db_role


@pytest.mark.asyncio
async def test_reference_builder_rejects_wrong_actual_server_major(monkeypatch) -> None:
    container = MagicMock()
    container.with_bind_ports.return_value = container
    container.__enter__.return_value = container
    container.get_connection_url.return_value = "postgresql+psycopg://localhost/isolated"
    monkeypatch.setattr(builder, "PostgresContainer", MagicMock(return_value=container))
    monkeypatch.setattr(
        builder, "_observe_fresh_database", AsyncMock(return_value=ApplicationSchemaInventory(16, ()))
    )
    with pytest.raises(RuntimeError, match="PostgreSQL major"):
        await builder.build_application_schema_reference(postgres_major=17)
    container.__exit__.assert_called_once()


def test_reference_builder_rejects_a_database_address(monkeypatch, capsys) -> None:
    monkeypatch.setattr(builder.sys, "argv", ["builder", "postgresql://live.example/application"])
    with pytest.raises(SystemExit, match="accepts no arguments"):
        builder.main()
    assert capsys.readouterr().out == ""


@pytest.mark.asyncio
async def test_builder_emits_all_fixed_major_profile_pairs(monkeypatch) -> None:
    build = AsyncMock(side_effect=application_schema_reference)
    monkeypatch.setattr(builder, "build_application_schema_reference", build)
    result = await builder._build_profiles()
    assert set(result) == {"16", "17"}
    assert build.await_count == 88
    for major in (16, 17):
        profiles = {"legacy-owner", "sealed-owner", "staging-readonly-legacy-owner", "staging-readonly-sealed-owner", "cnpg-staging-legacy-owner", "cnpg-staging-sealed-owner"}
        assert set(result[str(major)]) == {"0148/guard_0036", "0149/guard_0035", "0148/guard_0035", "0147/guard_0036", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"}
        for revision in ("0148/guard_0036", "0149/guard_0035", "0148/guard_0035", "0147/guard_0036", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"):
            assert set(result[str(major)][revision]) == (profiles | {"cnpg-staging-executor-admission"} if revision in {"0148/guard_0036", "0147/guard_0036"} else profiles)
