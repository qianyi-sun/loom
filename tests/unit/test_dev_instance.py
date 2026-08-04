"""Unit tests for per-developer dev-instance identity + guardrails."""

from __future__ import annotations

import pytest

from loom.dev_instance import (
    DEV_FLEET_BUDGET,
    PER_INSTANCE_CAP,
    DevInstanceRef,
    InvalidDevInstanceNameError,
    RequestedPolicy,
    derive_identity,
    validate_dev_instance,
    validate_name,
)


def _ok_policy(max_slots: int = 1) -> RequestedPolicy:
    return RequestedPolicy(actuator="slurm", min_slots=0, max_slots=max_slots)


class TestDeriveIdentity:
    def test_all_fields_derived_from_name(self) -> None:
        i = derive_identity("alice")
        assert i.runtime_environment == "dev-alice"
        assert i.namespace == "loom-dev-alice"
        assert i.database == "loom_dev_alice"
        assert i.db_role == "loom_dev_alice"
        assert i.task_bucket == "loom-dev-alice-tasks"
        assert i.trajectories_bucket == "loom-dev-alice-trajectories"
        assert i.artifacts_bucket == "loom-dev-alice-artifacts"
        assert i.route_host == "alice.dev.yylx.world"
        assert i.route_path == "/dev-alice"
        assert i.worker_pool == "dev-alice"
        assert i.provider_connection_namespace == "dev-alice"

    def test_dashes_become_underscores_only_in_db_identifiers(self) -> None:
        i = derive_identity("my-env")
        assert i.namespace == "loom-dev-my-env"  # DNS keeps dashes
        assert i.database == "loom_dev_my_env"  # Postgres uses underscores
        assert i.db_role == "loom_dev_my_env"
        assert i.task_bucket == "loom-dev-my-env-tasks"  # buckets keep dashes

    def test_injective_across_names(self) -> None:
        a = derive_identity("alice")
        b = derive_identity("bob")
        for field in a.__dataclass_fields__:
            if field == "name":
                continue
            assert getattr(a, field) != getattr(b, field), field

    def test_dash_underscore_names_do_not_collide_in_db(self) -> None:
        # names can only contain dashes (never underscores), so `a-b` maps to
        # `a_b` and there is no separate `a_b` name to collide with.
        assert derive_identity("a-b").database == "loom_dev_a_b"


class TestValidateName:
    @pytest.mark.parametrize(
        "name",
        ["alice", "bob", "a", "my-env", "dev1", "x1-y2", "a" * 20],
    )
    def test_valid_names(self, name: str) -> None:
        validate_name(name)  # no raise

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "1alice",  # starts with digit
            "Alice",  # uppercase
            "alice_env",  # underscore
            "-alice",  # leading dash
            "alice-",  # trailing dash
            "a" * 21,  # too long
            "al ice",  # space
            "al.ice",  # dot
        ],
    )
    def test_malformed_names_rejected(self, name: str) -> None:
        with pytest.raises(InvalidDevInstanceNameError):
            validate_name(name)

    @pytest.mark.parametrize(
        "name",
        ["dev", "development", "staging", "production", "prod", "loom", "shared"],
    )
    def test_reserved_names_rejected(self, name: str) -> None:
        with pytest.raises(InvalidDevInstanceNameError):
            validate_name(name)

    def test_derive_identity_rejects_bad_name(self) -> None:
        with pytest.raises(InvalidDevInstanceNameError):
            derive_identity("staging")


class TestValidateDevInstance:
    def test_valid_request_passes(self) -> None:
        assert validate_dev_instance("alice", _ok_policy(1), []) == []

    def test_bad_name_short_circuits(self) -> None:
        errs = validate_dev_instance("Alice", _ok_policy(1), [])
        assert len(errs) == 1 and "invalid dev-instance name" in errs[0]

    def test_reserved_name_rejected(self) -> None:
        errs = validate_dev_instance("prod", _ok_policy(1), [])
        assert errs and "reserved" in errs[0]

    def test_non_slurm_actuator_rejected(self) -> None:
        errs = validate_dev_instance(
            "alice", RequestedPolicy(actuator="k8s", min_slots=0, max_slots=1), []
        )
        assert any("slurm actuator" in e for e in errs)

    def test_max_slots_over_cap_rejected(self) -> None:
        errs = validate_dev_instance("alice", _ok_policy(PER_INSTANCE_CAP + 1), [])
        assert any("PER_INSTANCE_CAP" in e for e in errs)

    def test_min_over_max_rejected(self) -> None:
        errs = validate_dev_instance(
            "alice", RequestedPolicy(actuator="slurm", min_slots=2, max_slots=1), []
        )
        assert any("max_slots must be >= min_slots" in e for e in errs)

    def test_negative_min_rejected(self) -> None:
        errs = validate_dev_instance(
            "alice", RequestedPolicy(actuator="slurm", min_slots=-1, max_slots=1), []
        )
        assert any("min_slots must be >= 0" in e for e in errs)

    def test_fleet_budget_boundary_ok(self) -> None:
        # others commit DEV_FLEET_BUDGET - cap; a new cap-sized instance fits.
        others = [DevInstanceRef(name="bob", max_slots=DEV_FLEET_BUDGET - PER_INSTANCE_CAP)]
        assert validate_dev_instance("alice", _ok_policy(PER_INSTANCE_CAP), others) == []

    def test_fleet_budget_exceeded_rejected(self) -> None:
        others = [DevInstanceRef(name="bob", max_slots=DEV_FLEET_BUDGET)]
        errs = validate_dev_instance("alice", _ok_policy(1), others)
        assert any("fleet budget exceeded" in e for e in errs)

    def test_update_same_name_does_not_count_against_budget_twice(self) -> None:
        # re-validating an existing instance: its own prior max_slots (passed in
        # `other_instances` by a naive caller) is excluded by name.
        others = [DevInstanceRef(name="alice", max_slots=DEV_FLEET_BUDGET)]
        assert validate_dev_instance("alice", _ok_policy(PER_INSTANCE_CAP), others) == []

    def test_distinctness_holds_across_many_instances(self) -> None:
        others = [DevInstanceRef(name=f"dev{i}", max_slots=0) for i in range(3)]
        # distinct names never collide; only the (empty) budget matters
        assert validate_dev_instance("alice", _ok_policy(0), others) == []
