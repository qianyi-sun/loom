from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.manifest_readiness import ManifestArtifact
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.installed_manifest_ownership import (
    InstalledManifestOwnershipService,
)
from loom_cli.rollout.operator.model import CandidateBinding


def _config(tmp_path: Path) -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=tmp_path / "runner/repo",
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=tmp_path / "rollout",
        kubeconfig_path=tmp_path / "kubeconfig",
        cluster_config_path=tmp_path / "staging.cluster.toml",
        admin_token_source=f"file:{tmp_path}/admin",
        worker_token_source=f"file:{tmp_path}/worker",
        service_token_source=f"file:{tmp_path}/service",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256="1" * 64,
        source_mode="sealed-cumulative",
        source_commit_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )


def _candidate(*, sealed: bool = True) -> CandidateBinding:
    return CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative" if sealed else "merged-dev",
        resolved_tree="b" * 40 if sealed else None,
        approved_base_sha="c" * 40 if sealed else None,
    )


def _desired() -> list[dict[str, object]]:
    return [
        {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "loom-staging-data-lifecycle",
                "namespace": "loom-staging",
            },
            "spec": {"schedule": "*/5 * * * *", "suspend": False},
        },
        *[
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": name, "namespace": "loom-staging"},
                "spec": {
                    "podSelector": {},
                    "policyTypes": ["Ingress", "Egress"],
                    "ingress": [{"from": [{"podSelector": {}}]}],
                },
            }
            for name in (
                "loom-minio",
                "loom-postgres",
                "loom-staging-data-lifecycle",
            )
        ],
    ]


def _live() -> list[dict[str, object]]:
    resources = copy.deepcopy(_desired())
    for index, resource in enumerate(resources, start=1):
        metadata = resource["metadata"]
        assert isinstance(metadata, dict)
        metadata.update(
            {
                "uid": f"uid-{index}",
                "resourceVersion": str(index),
                "generation": index,
                "managedFields": [
                    {
                        "manager": "loom-lifecycle-bootstrap",
                        "operation": "Apply",
                        "fieldsV1": {"f:spec": {}},
                    }
                ],
            }
        )
    cron = resources[0]["spec"]
    assert isinstance(cron, dict)
    cron["suspend"] = True
    for policy in resources[1:]:
        spec = policy["spec"]
        assert isinstance(spec, dict)
        spec.pop("ingress")
    return resources


def _artifact() -> ManifestArtifact:
    rendered = yaml.safe_dump_all(_desired(), sort_keys=True)
    return ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
        resource_count=4,
        resource_set_digest="d" * 64,
        image_identities={"loom-control-plane": "sha256:" + "e" * 64},
        artifact_digest="f" * 64,
    )


def _identity(resource: dict[str, object]) -> str:
    metadata = resource["metadata"]
    assert isinstance(metadata, dict)
    return f"{resource['kind'].lower()}/{metadata['name']}"


class _Runner:
    def __init__(self) -> None:
        self.live = _live()
        self.calls: list[tuple[str, ...]] = []

    @property
    def environment(self):
        return {"KUBECONFIG": "/var/lib/loom-staging-rollout/kubeconfig"}

    def capture_stdout(self, argv, *, env, timeout_seconds):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        self.calls.append(command)
        target = command[command.index("get") + 1]
        resource = next(item for item in self.live if _identity(item) == target)
        return json.dumps(resource).encode()

    def capture_stdout_with_input(
        self,
        argv,
        *,
        env,
        input_payload,
        timeout_seconds,
    ):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        self.calls.append(command)
        document = yaml.safe_load(input_payload)
        assert isinstance(document, dict)
        identity = _identity(document)
        current = next(item for item in self.live if _identity(item) == identity)
        force = "--force-conflicts" in command
        dry_run = "--dry-run=server" in command
        if force:
            result = copy.deepcopy(current)
        else:
            result = copy.deepcopy(document)
            if not dry_run:
                self.live = [
                    copy.deepcopy(result) if _identity(item) == identity else item
                    for item in self.live
                ]
        return json.dumps(result).encode()


class _Journal:
    def __init__(self) -> None:
        self.inventories: list[dict[str, object]] = []
        self.events: list[dict[str, object]] = []

    def publish_inventory(self, request_id, inventory):  # type: ignore[no-untyped-def]
        self.inventories.append({"request_id": request_id, **dict(inventory)})

    def append(self, request_id, event):  # type: ignore[no-untyped-def]
        self.events.append({"request_id": request_id, **dict(event)})


def test_installed_service_binds_exact_publication_and_fixed_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout.operator import installed_manifest_ownership as module

    runner = _Runner()
    journal = _Journal()

    class _Store:
        def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            pass

        def load_exact(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs["candidate_sha"] == "a" * 40
            assert kwargs["candidate_tree"] == "b" * 40
            assert kwargs["mutation_epoch"] == 2
            return SimpleNamespace(manifests=_artifact())

    monkeypatch.setattr(module, "PreflightArtifactStore", _Store)
    monkeypatch.setattr(
        module,
        "InstalledPreflightCommands",
        lambda *args, **kwargs: SimpleNamespace(image=lambda *args: None),
    )
    monkeypatch.setattr(module, "SubprocessProtectedApplyCommandRunner", lambda **kwargs: runner)
    monkeypatch.setattr(module, "ManifestOwnershipJournal", lambda *args, **kwargs: journal)
    monkeypatch.setattr(
        module,
        "ManifestOwnershipEpochClaimer",
        lambda **kwargs: lambda epoch, request, evidence: epoch + 1,
    )

    service = InstalledManifestOwnershipService(
        config=_config(tmp_path),
        service_uid=max(1, os.geteuid()),
        read_mutation_epoch=lambda: 2,
    )
    inventory = service.inventory(_candidate())
    result = service.apply(
        _candidate(),
        request_id="req-manifest-ownership-12345678",
        approved_inventory_sha256=inventory["inventory_sha256"],  # type: ignore[arg-type]
    )
    assert result["mutation_epoch_after"] == 3
    assert any("--force-conflicts" in command for command in runner.calls)
    assert any(
        "--force-conflicts" not in command and "apply" in command for command in runner.calls
    )
    assert journal.events[-1]["event"] == "completed"
    cron = runner.live[0]["spec"]
    assert isinstance(cron, dict)
    assert cron["suspend"] is True


def test_installed_service_rejects_nonsealed_candidate(tmp_path: Path) -> None:
    service = InstalledManifestOwnershipService(
        config=_config(tmp_path),
        service_uid=max(1, os.geteuid()),
        read_mutation_epoch=lambda: 2,
    )
    with pytest.raises(ValueError, match="exact sealed source"):
        service.inventory(_candidate(sealed=False))
