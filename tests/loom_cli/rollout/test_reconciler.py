from __future__ import annotations

from collections.abc import Mapping

from loom_cli.rollout.reconciler import (
    ReconcileMode,
    reconcile_once,
)


def _deploy(name: str, *, image: str, replicas: int = 1, namespace: str = "loom-staging") -> dict:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "replicas": replicas,
            "template": {"spec": {"containers": [{"name": name, "image": image}]}},
        },
    }


class _FakeCluster:
    """A dict-backed live cluster whose server-side apply upserts whole objects."""

    def __init__(self, objects: list[dict] | None = None) -> None:
        self._by_key: dict[tuple[str, str, str], dict] = {}
        for obj in objects or []:
            self._by_key[self._key(obj)] = obj
        self.applied: list[tuple[str, str, str]] = []

    @staticmethod
    def _key(obj: Mapping[str, object]) -> tuple[str, str, str]:
        meta = obj.get("metadata")
        meta = meta if isinstance(meta, Mapping) else {}
        return (str(obj.get("kind", "")), str(meta.get("namespace", "")), str(meta.get("name", "")))

    def read_live(self) -> list[dict]:
        return list(self._by_key.values())

    def apply(self, obj: Mapping[str, object]) -> None:
        self.applied.append(self._key(obj))
        self._by_key[self._key(obj)] = dict(obj)


def test_shadow_mode_never_writes() -> None:
    cluster = _FakeCluster([_deploy("svc", image="loom:OLD", replicas=1)])
    desired = [_deploy("svc", image="loom:NEW", replicas=3)]

    result = reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=cluster.apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.SHADOW,
    )

    assert cluster.applied == []  # no writes
    assert result.converged is False
    assert result.mode is ReconcileMode.SHADOW
    assert {r.name for r in result.residual} == {"svc"}


def test_apply_mode_converges_modified_and_absent() -> None:
    # svc is drifted (old image), minio is absent from live entirely.
    cluster = _FakeCluster([_deploy("svc", image="loom:OLD", replicas=1)])
    desired = [
        _deploy("svc", image="loom:NEW", replicas=3),
        _deploy("minio", image="minio:1"),
    ]

    result = reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=cluster.apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.APPLY,
    )

    assert set(cluster.applied) == {
        ("Deployment", "loom-staging", "svc"),
        ("Deployment", "loom-staging", "minio"),
    }
    assert result.converged is True
    assert result.residual == ()
    # live now equals desired
    live = {c["metadata"]["name"]: c for c in cluster.read_live()}
    assert live["svc"]["spec"]["replicas"] == 3
    assert live["minio"]["metadata"]["name"] == "minio"


def test_apply_mode_skips_already_in_sync_objects() -> None:
    cluster = _FakeCluster([_deploy("svc", image="loom:NEW", replicas=3)])
    # live already carries extra controller fields; desired is a subset → in sync
    cluster._by_key[("Deployment", "loom-staging", "svc")]["status"] = {"readyReplicas": 3}
    desired = [_deploy("svc", image="loom:NEW", replicas=3)]

    result = reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=cluster.apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.APPLY,
    )

    assert cluster.applied == []  # nothing to do
    assert result.converged is True


def test_apply_mode_reports_residual_when_apply_does_not_take() -> None:
    # An applier that silently no-ops (e.g. a webhook rejects the mutation): the
    # loop must not claim convergence — it re-reads and reports residual drift.
    cluster = _FakeCluster([_deploy("svc", image="loom:OLD", replicas=1)])
    desired = [_deploy("svc", image="loom:NEW", replicas=3)]
    applied_calls: list[tuple[str, str, str]] = []

    def noop_apply(obj: Mapping[str, object]) -> None:
        applied_calls.append(_FakeCluster._key(obj))  # records intent, but doesn't mutate live

    result = reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=noop_apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.APPLY,
    )

    assert applied_calls == [("Deployment", "loom-staging", "svc")]  # it tried
    assert result.converged is False  # but live still drifted
    assert {r.name for r in result.residual} == {"svc"}


def test_apply_mode_does_not_prune_live_only_objects() -> None:
    # An object present live but absent from desired must NOT be deleted/applied —
    # the reconciler does not prune (compute_drift default doesn't even surface it).
    cluster = _FakeCluster([_deploy("orphan", image="x:1")])
    desired = [_deploy("svc", image="loom:NEW")]

    reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=cluster.apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.APPLY,
    )

    # only svc applied; orphan untouched and still present
    assert set(cluster.applied) == {("Deployment", "loom-staging", "svc")}
    live_names = {c["metadata"]["name"] for c in cluster.read_live()}
    assert "orphan" in live_names


def test_result_to_dict_shape() -> None:
    cluster = _FakeCluster()
    desired = [_deploy("svc", image="loom:NEW")]
    result = reconcile_once(
        desired,
        read_live=cluster.read_live,
        apply=cluster.apply,
        environment="staging",
        target="rev1",
        mode=ReconcileMode.APPLY,
    )
    doc = result.to_dict()
    assert doc["mode"] == "apply"
    assert doc["converged"] is True
    assert doc["applied"] == [{"kind": "Deployment", "namespace": "loom-staging", "name": "svc"}]
