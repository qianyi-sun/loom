from __future__ import annotations

import base64
import importlib
import importlib.util
import json
from pathlib import Path

import pytest

from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
)

MODULE = "loom_cli.rollout.operator.protected_external_supervisor_database_secret_component"
SOURCE_VALUE = base64.b64encode(b"postgresql://derived-source\n").decode("ascii")


def _component_type():
    assert importlib.util.find_spec(MODULE) is not None, "protected Secret component is missing"
    module = importlib.import_module(MODULE)
    component = getattr(module, "KubernetesExternalSupervisorDatabaseSecretComponent", None)
    assert component is not None, "protected Secret component type is missing"
    return component


def _epoch(state: ComponentState = ComponentState.EXACT):
    def classify(plan):
        return ComponentObservation(
            state=state,
            evidence_digest="e" * 64,
            observed_epoch=plan.starting_mutation_epoch + 1,
        )

    return classify


class SecretCluster:
    def __init__(
        self,
        *,
        target_value: str | None = None,
        data_owner: str | None = None,
        extra_data: bool = False,
        immutable: object = False,
        malformed_owner_field: bool = False,
    ) -> None:
        self.source_value = SOURCE_VALUE
        self.target_value = target_value
        self.data_owner = data_owner
        self.extra_data = extra_data
        self.immutable = immutable
        self.malformed_owner_field = malformed_owner_field
        self.resource_version = "10"
        self.uid = "bb36273b-9a83-4ad4-bfaf-992e24e43b99"
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []
        self.race_before_apply = False

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append((command, None))
        if "secret/loom-secrets" in command:
            assert "--output=jsonpath={.data.cp-db-url}" in command
            return self.source_value.encode("ascii")
        if "secret/loom-external-slurm-autoscaler-db" in command:
            assert "--show-managed-fields" in command
            assert "--output=json" in command
            return json.dumps(self._target()).encode()
        raise AssertionError(command)

    def capture_stdout_with_input(
        self,
        argv,
        *,
        env,
        input_payload,
        timeout_seconds,
    ):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append((command, input_payload))
        assert "apply" in command
        assert "--server-side=true" in command
        assert ("--force-conflicts" in command) is (self.data_owner == "kubectl-client-side-apply")
        assert "--field-manager=loom-staging-rollout-supervisor-database" in command
        assert "--show-managed-fields" in command
        assert "--output=json" in command
        if self.race_before_apply:
            self.resource_version = "11"
        payload = json.loads(input_payload)
        metadata = payload["metadata"]
        if metadata["uid"] != self.uid or metadata["resourceVersion"] != self.resource_version:
            raise RuntimeError("protected Secret resource version changed")
        assert set(payload) == {"apiVersion", "data", "kind", "metadata"}
        assert payload["data"] == {"cp-db-url": self.source_value}
        self.target_value = self.source_value
        self.data_owner = "loom-staging-rollout-supervisor-database"
        self.resource_version = str(int(self.resource_version) + 1)
        return json.dumps(self._target()).encode()

    def _target(self) -> dict[str, object]:
        data = {}
        if self.target_value is not None:
            data["cp-db-url"] = self.target_value
        if self.extra_data:
            data["unexpected"] = "dW5zYWZl"
        managed_fields: list[dict[str, object]] = [
            {
                "apiVersion": "v1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {"f:type": {}},
                "manager": "loom-staging-rollout",
                "operation": "Apply",
            }
        ]
        if self.data_owner is not None:
            managed_fields.append(
                {
                    "apiVersion": "v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {
                        "f:data": {"f:cp-db-url": ("invalid" if self.malformed_owner_field else {})}
                    },
                    "manager": self.data_owner,
                    "operation": (
                        "Update" if self.data_owner == "kubectl-client-side-apply" else "Apply"
                    ),
                }
            )
        target = {
            "apiVersion": "v1",
            "data": data,
            "kind": "Secret",
            "metadata": {
                "managedFields": managed_fields,
                "name": "loom-external-slurm-autoscaler-db",
                "namespace": "loom-staging",
                "resourceVersion": self.resource_version,
                "uid": self.uid,
            },
            "type": "Opaque",
        }
        if self.immutable:
            target["immutable"] = self.immutable
        return target


def _authority(cluster: SecretCluster):
    return _component_type()(
        runner=cluster,
        environment={"KUBECONFIG": "/exact"},
        epoch_guard=_epoch(),
    )


def test_secret_component_restores_only_derived_data_under_a_dedicated_manager(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster()
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)
    exact = authority.classify(plan)

    assert exact.state is ComponentState.EXACT
    apply_payloads = [payload for _argv, payload in cluster.calls if payload is not None]
    assert len(apply_payloads) == 1


def test_secret_component_migrates_matching_legacy_client_side_ownership(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(
        target_value=SOURCE_VALUE,
        data_owner="kubectl-client-side-apply",
    )
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)

    assert authority.classify(plan).state is ComponentState.EXACT


def test_secret_component_rejects_unknown_data_ownership_without_mutation(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(
        target_value=SOURCE_VALUE,
        data_owner="another-secret-controller",
    )
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all(payload is None for _argv, payload in cluster.calls)


def test_secret_component_rejects_unknown_data_without_mutation(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(extra_data=True)
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all(payload is None for _argv, payload in cluster.calls)


def test_secret_component_rejects_an_immutable_target_without_mutation(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(immutable=True)
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all(payload is None for _argv, payload in cluster.calls)


def test_secret_component_classifies_malformed_immutable_state_as_drift(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(immutable={"unexpected": True})
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all(payload is None for _argv, payload in cluster.calls)


def test_secret_component_rejects_malformed_managed_field_ownership(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster(
        target_value=SOURCE_VALUE,
        data_owner="loom-staging-rollout-supervisor-database",
        malformed_owner_field=True,
    )
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all(payload is None for _argv, payload in cluster.calls)


def test_secret_component_resource_version_precondition_closes_apply_race(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    cluster = SecretCluster()
    cluster.race_before_apply = True
    authority = _authority(cluster)

    assert authority.classify(plan).state is ComponentState.READY
    with pytest.raises(RuntimeError, match="resource version changed"):
        authority.apply(plan)
    assert cluster.target_value is None
