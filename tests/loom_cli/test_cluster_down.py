"""`loom cluster down` — teardown orchestration tests (#76 Phase 4A).

The unit under test composes `render_manifests` (Phase 1B),
`delete_manifests` (kubectl subprocess), and the optional
`delete_pvcs` / `delete_namespace_resource` Python-client calls.
We mock subprocess + the k8s clients so each branch can run
without a real cluster.
"""

from __future__ import annotations

import io
import subprocess
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    DeleteResult,
    delete_manifests,
    delete_namespace_resource,
    delete_pvcs,
)

# ──────────────────────────────────────────────────────────────────────
# delete_manifests — subprocess wrapper
# ──────────────────────────────────────────────────────────────────────


def test_delete_manifests_invokes_kubectl_with_ignore_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`down` MUST pass --ignore-not-found so a second teardown after
    a partial delete still exits cleanly."""
    captured: dict[str, Any] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        captured["input"] = kwargs.get("input")
        return subprocess.CompletedProcess(
            args=cmd, returncode=0,
            stdout="deployment.apps/loom-service deleted\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr(
        "shutil.which", lambda _bin: "/usr/local/bin/kubectl",
    )

    result = delete_manifests(
        "apiVersion: v1\nkind: ConfigMap\n", "loom", context="prod",
    )
    assert result.returncode == 0
    assert result.summary_lines == [
        "deployment.apps/loom-service deleted",
    ]
    assert captured["cmd"][:5] == [
        "kubectl", "delete", "-n", "loom", "-f",
    ]
    assert "--ignore-not-found" in captured["cmd"]
    assert "--context" in captured["cmd"]
    assert "prod" in captured["cmd"]
    assert "ConfigMap" in captured["input"]


def test_delete_manifests_omits_context_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    delete_manifests("---", "loom", context=None)
    assert "--context" not in captured["cmd"]


def test_delete_manifests_propagates_nonzero_returncode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(
            args=cmd, returncode=1, stdout="",
            stderr="error: forbidden\n",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    result = delete_manifests("---", "loom", context=None)
    assert result.returncode == 1
    assert "forbidden" in result.stderr


def test_delete_manifests_raises_when_kubectl_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("shutil.which", lambda _bin: None)
    with pytest.raises(RuntimeError, match="kubectl is required"):
        delete_manifests("---", "loom", context=None)


# ──────────────────────────────────────────────────────────────────────
# delete_pvcs — python-client teardown
# ──────────────────────────────────────────────────────────────────────


class _FakePVC:
    def __init__(self, name: str) -> None:
        self.metadata = type("M", (), {"name": name})()


class _FakeCoreV1:
    def __init__(self, pvc_names: list[str]) -> None:
        self._pvcs = [_FakePVC(n) for n in pvc_names]
        self.deleted: list[tuple[str, str]] = []
        self.deleted_namespace: str | None = None

    def list_namespaced_persistent_volume_claim(
        self, *, namespace: str,
    ) -> Any:
        return type("L", (), {"items": list(self._pvcs)})()

    def delete_namespaced_persistent_volume_claim(
        self, *, name: str, namespace: str,
    ) -> None:
        self.deleted.append((namespace, name))

    def delete_namespace(self, *, name: str) -> None:
        self.deleted_namespace = name


def test_delete_pvcs_deletes_every_pvc_in_namespace() -> None:
    core = _FakeCoreV1(
        ["data-loom-postgres-0", "data-loom-minio-0"],
    )
    deleted = delete_pvcs(core, "loom")
    assert deleted == ["data-loom-postgres-0", "data-loom-minio-0"]
    assert core.deleted == [
        ("loom", "data-loom-postgres-0"),
        ("loom", "data-loom-minio-0"),
    ]


def test_delete_pvcs_returns_empty_when_no_pvcs() -> None:
    core = _FakeCoreV1([])
    assert delete_pvcs(core, "loom") == []


# ──────────────────────────────────────────────────────────────────────
# delete_namespace_resource
# ──────────────────────────────────────────────────────────────────────


def test_delete_namespace_resource_invokes_core_v1() -> None:
    core = _FakeCoreV1([])
    delete_namespace_resource(core, "loom-stage")
    assert core.deleted_namespace == "loom-stage"


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch — orchestration
# ──────────────────────────────────────────────────────────────────────


def _patch_full_down_path(
    monkeypatch: pytest.MonkeyPatch, *,
    delete_returncode: int = 0,
    pvc_names: list[str] | None = None,
) -> dict[str, Any]:
    """Stub the k8s client tuple + delete_manifests so the CLI can
    run end-to-end without touching kubectl or a cluster."""
    captures: dict[str, Any] = {}
    core = _FakeCoreV1(pvc_names or [])
    captures["core"] = core

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (object(), object(), core, object()),
    )

    def _delete(yaml_text, ns, *, context, extra_args=()):  # type: ignore[no-untyped-def]
        captures["delete_yaml_len"] = len(yaml_text)
        captures["delete_ns"] = ns
        captures["delete_context"] = context
        return DeleteResult(
            returncode=delete_returncode,
            summary_lines=(
                ["deployment.apps/loom-service deleted"]
                if delete_returncode == 0 else []
            ),
            stderr="error: x" if delete_returncode != 0 else "",
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.delete_manifests", _delete)
    return captures


def test_cli_down_happy_path_with_yes_flag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    rc = main(["cluster", "down", "--yes"])
    assert rc == 0
    assert captures["delete_ns"] == "loom"
    out = capsys.readouterr().out
    assert "loom-service deleted" in out
    assert "Cluster down: complete." in out


def test_cli_down_prompts_and_aborts_on_no(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without --yes, an interactive 'n' reply aborts before any
    delete call."""
    captures = _patch_full_down_path(monkeypatch)
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    rc = main(["cluster", "down"])
    assert rc == 1
    assert "delete_ns" not in captures
    out = capsys.readouterr().out
    assert "aborted" in out.lower()


def test_cli_down_prompts_and_proceeds_on_yes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    monkeypatch.setattr("sys.stdin", io.StringIO("y\n"))
    rc = main(["cluster", "down"])
    assert rc == 0
    assert captures["delete_ns"] == "loom"


def test_cli_down_with_volumes_deletes_pvcs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_down_path(
        monkeypatch, pvc_names=["data-loom-postgres-0"],
    )
    rc = main(["cluster", "down", "--yes", "--with-volumes"])
    assert rc == 0
    assert captures["core"].deleted == [
        ("loom", "data-loom-postgres-0"),
    ]
    out = capsys.readouterr().out
    assert "persistentvolumeclaim/data-loom-postgres-0 deleted" in out


def test_cli_down_with_volumes_handles_empty_pvc_list(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_down_path(monkeypatch, pvc_names=[])
    rc = main(["cluster", "down", "--yes", "--with-volumes"])
    assert rc == 0
    assert "no PVCs found" in capsys.readouterr().out


def test_cli_down_delete_namespace_flag_invokes_namespace_delete(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    rc = main(["cluster", "down", "--yes", "--delete-namespace"])
    assert rc == 0
    assert captures["core"].deleted_namespace == "loom"
    assert "namespace/loom deleted" in capsys.readouterr().out


def test_cli_down_delete_namespace_omitted_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    main(["cluster", "down", "--yes"])
    assert captures["core"].deleted_namespace is None


def test_cli_down_with_volumes_omitted_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_down_path(
        monkeypatch, pvc_names=["data-loom-postgres-0"],
    )
    main(["cluster", "down", "--yes"])
    # PVCs untouched.
    assert captures["core"].deleted == []


def test_cli_down_kubectl_delete_failure_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_down_path(monkeypatch, delete_returncode=1)
    rc = main(["cluster", "down", "--yes"])
    assert rc == 1
    assert "kubectl delete failed" in capsys.readouterr().err


def test_cli_down_cluster_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "down", "--yes"])
    assert rc == 2
    assert "cannot connect to cluster" in capsys.readouterr().err


def test_cli_down_kubectl_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_down_path(monkeypatch)

    def _raise(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError(
            "kubectl is required for `loom cluster down`. install from ...",
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.delete_manifests", _raise,
    )
    rc = main(["cluster", "down", "--yes"])
    assert rc == 2
    assert "kubectl is required" in capsys.readouterr().err


def test_cli_down_namespace_flag_threads_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    main(["cluster", "down", "--yes", "--namespace", "loom-stage"])
    assert captures["delete_ns"] == "loom-stage"


def test_cli_down_context_flag_threads_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_down_path(monkeypatch)
    main(["cluster", "down", "--yes", "--context", "prod-cluster"])
    assert captures["delete_context"] == "prod-cluster"


def test_cli_down_invalid_config_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Any,
) -> None:
    """A bad config path → exit 2 before any kubectl call."""
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (object(), object(), _FakeCoreV1([]), object()),
    )
    rc = main([
        "cluster", "down", "--yes",
        "--config", str(tmp_path / "does-not-exist.toml"),
    ])
    assert rc == 2
    assert "render failed" in capsys.readouterr().err


def test_cli_down_prompt_mentions_volumes_when_flag_set(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The interactive prompt MUST warn about data loss when
    --with-volumes is set, so an operator doesn't accidentally
    wipe Postgres."""
    _patch_full_down_path(monkeypatch)
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    main(["cluster", "down", "--with-volumes"])
    out = capsys.readouterr().out
    assert "PersistentVolumeClaims" in out
    assert "data loss" in out
