from __future__ import annotations

import json
from copy import deepcopy
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    canonical_digest,
)
from loom_cli.rollout.operator.checkpoint_database_authority import (
    DatabaseAuthorityError,
    capture_database_authority,
    parse_database_authority_observation,
)
from tests.capacity_fixtures import (
    fleet_with_development_template,
    subject_configuration,
)


def _observation() -> dict[str, object]:
    fleet = fleet_with_development_template()
    subject = subject_configuration()
    fleet_digest = canonical_digest(fleet)
    subject_digest = canonical_digest(subject)
    refs = (
        ConfigurationGenerationRefV1(
            scope="subject",
            generation=subject.configuration_generation,
            digest=subject_digest,
            subject_id=subject.subject_id,
            subject_incarnation=subject.subject_incarnation,
        ),
    )
    snapshot = ConfigurationSnapshotV1(
        configuration_epoch=9,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=fleet.fleet_generation,
            digest=fleet_digest,
        ),
        subjects=refs,
    )
    return {
        "authority": [
            {
                "authority_incarnation": "00000000-0000-4000-8000-000000000001",
                "executable_new_capacity_ceiling": 0,
                "execution_epoch": 0,
                "execution_manifest_sha256": None,
                "execution_state": "shadow",
                "increase_freeze": True,
                "recovery_state": "shadow",
                "schema_version": 1,
                "singleton_id": 1,
                "writer_epoch": 0,
            }
        ],
        "configuration": [
            {
                "canonical_digest": canonical_digest(snapshot),
                "configuration_epoch": 9,
                "fleet_digest": fleet_digest,
                "fleet_generation": fleet.fleet_generation,
                "subject_generation_manifest": [item.model_dump(mode="json") for item in refs],
            }
        ],
        "generations": [
            {
                "digest": fleet_digest,
                "payload": fleet.model_dump(mode="json"),
                "scope": "fleet",
                "scope_generation": fleet.fleet_generation,
                "state": "active",
                "subject_id": None,
                "subject_incarnation": None,
            },
            {
                "digest": subject_digest,
                "payload": subject.model_dump(mode="json"),
                "scope": "subject",
                "scope_generation": subject.configuration_generation,
                "state": "active",
                "subject_id": str(subject.subject_id),
                "subject_incarnation": str(subject.subject_incarnation),
            },
        ],
        "guard_revisions": ["guard_0028"],
        "guard_table_present": True,
        "public_revisions": ["0067_global_capacity"],
    }


def _payload(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _application_observation() -> dict[str, object]:
    value = _observation()
    return {
        "guard_revisions": value["guard_revisions"],
        "guard_table_present": value["guard_table_present"],
        "public_revisions": value["public_revisions"],
    }


def _manager_observation() -> dict[str, object]:
    value = _observation()
    return {
        "authority": value["authority"],
        "configuration": value["configuration"],
        "generations": value["generations"],
    }


def _rebind_subject_payload(value: dict[str, object], **changes: object) -> None:
    configuration = value["configuration"][0]
    generation = value["generations"][1]
    generation["payload"].update(changes)
    subject = subject_configuration().__class__.model_validate_json(
        json.dumps(generation["payload"])
    )
    subject_digest = canonical_digest(subject)
    generation["digest"] = subject_digest
    configuration["subject_generation_manifest"][0]["digest"] = subject_digest
    subject_refs = tuple(
        ConfigurationGenerationRefV1.model_validate_json(json.dumps(item))
        for item in configuration["subject_generation_manifest"]
    )
    configuration["canonical_digest"] = canonical_digest(
        ConfigurationSnapshotV1(
            configuration_epoch=configuration["configuration_epoch"],
            fleet=ConfigurationGenerationRefV1(
                scope="fleet",
                generation=configuration["fleet_generation"],
                digest=configuration["fleet_digest"],
            ),
            subjects=subject_refs,
        )
    )


def test_authority_parser_returns_only_typed_non_secret_checkpoint_fields() -> None:
    evidence = parse_database_authority_observation(_payload(_observation()))

    assert evidence.public_schema_revision == "0067_global_capacity"
    assert evidence.capacity_guard_schema_revision == "guard_0028"
    assert evidence.configuration_epoch == 9
    assert evidence.authority_incarnation == UUID("00000000-0000-4000-8000-000000000001")
    assert evidence.writer_epoch == 0
    assert json.loads(evidence.payload) == {
        "authority_incarnation": "00000000-0000-4000-8000-000000000001",
        "capacity_guard_schema_revision": "guard_0028",
        "configuration_digest": evidence.configuration_digest,
        "configuration_epoch": 9,
        "executable_new_capacity_ceiling": 0,
        "execution_epoch": 0,
        "execution_manifest_sha256": None,
        "execution_state": "shadow",
        "increase_freeze": True,
        "public_schema_revision": "0067_global_capacity",
        "schema_version": 1,
        "writer_epoch": 0,
    }


def test_authority_parser_records_absent_capacity_guard_revision() -> None:
    value = _observation()
    value["guard_table_present"] = False
    value["guard_revisions"] = []

    evidence = parse_database_authority_observation(_payload(value))

    assert evidence.capacity_guard_schema_revision is None


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: _rebind_subject_payload(
            value,
            subject_id="00000000-0000-4000-8000-0000000000bb",
        ),
        lambda value: _rebind_subject_payload(
            value,
            subject_incarnation="00000000-0000-4000-8000-0000000000cc",
        ),
        lambda value: _rebind_subject_payload(value, configuration_generation=10),
        lambda value: value["authority"][0].update(
            authority_incarnation="00000000-0000-4000-8000-0000000000dd"
        ),
    ],
)
def test_authority_parser_rejects_payload_or_authority_identity_outside_its_binding(
    mutate,
) -> None:
    value = deepcopy(_observation())
    mutate(value)

    with pytest.raises(DatabaseAuthorityError):
        parse_database_authority_observation(_payload(value))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(public_revisions=[]),
        lambda value: value.update(public_revisions=["0067", "0068"]),
        lambda value: value.update(guard_revisions=["guard_0027", "guard_0028"]),
        lambda value: value.update(configuration=[]),
        lambda value: value["configuration"][0].update(canonical_digest="0" * 64),
        lambda value: value["generations"][0].update(digest="1" * 64),
        lambda value: value["generations"][1]["payload"].update(configuration_generation=99),
        lambda value: value.update(authority=[]),
        lambda value: value["authority"][0].update(schema_version=True),
        lambda value: value["authority"][0].update(execution_state="active"),
        lambda value: value["authority"][0].update(execution_epoch=1),
        lambda value: value["authority"][0].update(execution_epoch=False),
        lambda value: value["authority"][0].update(execution_manifest_sha256="2" * 64),
        lambda value: value["authority"][0].update(executable_new_capacity_ceiling=1),
        lambda value: value["authority"][0].update(increase_freeze=False),
    ],
)
def test_authority_parser_rejects_missing_duplicate_contradictory_or_noncanonical_rows(
    mutate,
) -> None:
    value = deepcopy(_observation())
    mutate(value)

    with pytest.raises(DatabaseAuthorityError):
        parse_database_authority_observation(_payload(value))


def test_authority_capture_uses_distinct_read_only_application_and_manager_transactions() -> None:
    class Runner:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[str, ...], bytes]] = []

        def capture_stdout_with_input(
            self,
            argv,
            *,
            env,
            input_payload,
            timeout_seconds,
        ):
            command = tuple(argv)
            self.calls.append((command, input_payload))
            if "loom-staging" in command:
                return _payload(_application_observation())
            if "loom-dev" in command:
                return _payload(_manager_observation())
            raise AssertionError(command)

    runner = Runner()

    evidence = capture_database_authority(
        runner,
        env={"PATH": "/bin"},
        namespace="loom-staging",
    )

    assert evidence.public_schema_revision == "0067_global_capacity"
    assert evidence.configuration_epoch == 9
    assert len(runner.calls) == 2
    application_argv, application_sql = runner.calls[0]
    manager_argv, manager_sql = runner.calls[1]
    assert application_argv == (
        "kubectl",
        "--namespace",
        "loom-staging",
        "exec",
        "--stdin=true",
        "service/loom-postgres-rw",
        "--",
        "psql",
        "--no-psqlrc",
        "--quiet",
        "--tuples-only",
        "--no-align",
        "--set=ON_ERROR_STOP=1",
        "--username=postgres",
        "--dbname=loom",
        "--file=-",
    )
    assert manager_argv == (
        "kubectl",
        "--namespace",
        "loom-dev",
        "exec",
        "--stdin=true",
        "service/loom-capacity-postgres",
        "--",
        "sh",
        "-ceu",
        "exec psql --no-psqlrc --quiet --tuples-only --no-align "
        '--set=ON_ERROR_STOP=1 --username="$POSTGRES_USER" '
        '--dbname="$POSTGRES_DB" --file=-',
    )
    rendered_application = application_sql.decode()
    rendered_manager = manager_sql.decode()
    assert "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in (rendered_application)
    assert "FROM public.alembic_version" in rendered_application
    assert "FROM loom_capacity_guard.capacity_guard_alembic_version" in (rendered_application)
    assert "capacity_configuration_epochs" not in rendered_application
    assert "capacity_authority_state" not in rendered_application
    assert "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in rendered_manager
    assert rendered_manager.count("FROM public.capacity_configuration_epochs") == 2
    assert "FROM public.capacity_config_generations" in rendered_manager
    assert "FROM public.capacity_authority_state" in rendered_manager
    assert "loom_capacity_guard" not in rendered_manager


def test_authority_capture_keeps_manager_transport_failures_secret_safe() -> None:
    marker = "postgresql://user:password@database/private-key"

    class Runner:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[str, ...], bytes]] = []

        def capture_stdout_with_input(
            self,
            argv,
            *,
            env,
            input_payload,
            timeout_seconds,
        ):
            command = tuple(argv)
            self.calls.append((command, input_payload))
            if "loom-staging" in command:
                return _payload(_application_observation())
            raise RuntimeError(marker)

    runner = Runner()
    with pytest.raises(DatabaseAuthorityError) as caught:
        capture_database_authority(runner, env={"PATH": "/bin"}, namespace="loom-staging")

    assert len(runner.calls) == 2
    assert "loom-dev" in runner.calls[1][0]
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
