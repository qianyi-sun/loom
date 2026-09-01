from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

import loom_cli.rollout.preflight_runtime_sources as runtime_sources_module
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ROLLOUT_IMAGES, image_plan_digest
from loom_cli.rollout.manifest_readiness import (
    inspect_rendered_manifests,
    render_checkpoint_guard_field_ownership_payload,
)
from loom_cli.rollout.migration_readiness import DEFAULT_MIGRATION_POLICY
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactPublication,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    CheckOperation,
)
from loom_cli.rollout.preflight_registered_checks import (
    CredentialProbeSource,
    ExternalSupervisorPredecessorSnapshot,
)
from loom_cli.rollout.preflight_runtime_sources import (
    GB10_CANDIDATE_SOURCE_CONCURRENCY,
    GB10_PREFLIGHT_FLEET_CONCURRENCY,
    BackupAdmissionAuthority,
    PreflightRuntimeSources,
)
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config
from tests.loom_cli.rollout.test_manifest_readiness import (
    _rendered_with_lifecycle_cronjob,
)
from tests.loom_cli.rollout.test_preflight_artifact_store import (
    _images,
    _migration,
    _production_defaults,
)


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


def _result(stdout: str = ""):
    return type("Result", (), {"returncode": 0, "stdout": stdout})()


def _runtime_sources(
    tmp_path: Path,
    *,
    render_manifest,
    image_run,
    server_schema_dry_run,
    manifest_post_image_pin=None,
    loaded_artifacts: LoadedPreflightArtifacts | None = None,
    container_registry: str = "",
    container_registry_push: str = "",
    gb10_candidate_source_run=None,
    gb10_run=None,
) -> PreflightRuntimeSources:
    candidate = _candidate()
    token = tmp_path / "state" / "admin"
    return PreflightRuntimeSources(
        config=_config(tmp_path),
        candidate=candidate,
        candidate_root=tmp_path,
        artifact_store=PreflightArtifactStore(tmp_path / "state", service_uid=501),
        service_uid=501,
        service_gid=20,
        runner_install_digest="1" * 64,
        git_run=lambda *_args, **_kwargs: _result(),
        credential_sources=(CredentialProbeSource(label="admin", path=token),),
        executable_lookup=lambda _name: "/fixed/tool",
        docker_runtime_run=lambda *_args, **_kwargs: _result(),
        kubernetes_run=lambda *_args, **_kwargs: _result(),
        kubeconfig_metadata_digest="2" * 64,
        readonly_authority_source=lambda: ReadonlyAuthorityEvidence(
            principal="loom-rollout-readonly",
            environment="staging",
            namespace="loom-staging",
            kubernetes_verbs=("get", "list", "watch"),
            kubernetes_resources=("deployments", "pods", "services"),
            http_methods=("GET", "HEAD"),
            capability_source_digest="8" * 64,
        ),
        capacity_source=lambda: None,  # type: ignore[arg-type,return-value]
        backup_authority=BackupAdmissionAuthority.fresh(
            schema_revision="0066",
            object_inventory_root="3" * 64,
        ),
        database_schema_revision="0074",
        external_supervisor_predecessor_source=lambda _context: (
            ExternalSupervisorPredecessorSnapshot(
                kind="legacy-manifest",
                authority_digest="a" * 64,
                pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                unit_sha256={
                    "loom-autoscaler-gb10-staging.service": "b" * 64,
                    "loom-autoscaler-gb10-staging.timer": "c" * 64,
                },
                live_evidence_digest="d" * 64,
                pending_transition_digest=hashlib.sha256(b"{}").hexdigest(),
                transition_clear=True,
                runtime_ready=True,
                pool_identity_digest="e" * 64,
            )
        ),
        systemd_run=lambda *_args, **_kwargs: _result(),
        gb10_run=gb10_run or (lambda *_args, **_kwargs: _result()),
        gb10_targets=(
            GB10ProbeTarget(
                ssh_target="trt-gb10-1",
                node_agent_service="loom-gb10-node-agent.service",
            ),
        ),
        gb10_ssh_config=tmp_path / "ssh-config",
        gb10_identity=tmp_path / "identity",
        gb10_ssh_config_sha256="4" * 64,
        gb10_identity_metadata_fingerprint="5" * 64,
        gb10_mount_source=lambda: None,  # type: ignore[arg-type,return-value]
        gb10_mount_binding_digest="6" * 64,
        alembic_ini=tmp_path / "alembic.ini",
        migration_policy_path=DEFAULT_MIGRATION_POLICY,
        migration_policy_digest=hashlib.sha256(
            DEFAULT_MIGRATION_POLICY.read_bytes()
        ).hexdigest(),
        systemd_analyze_run=lambda *_args, **_kwargs: _result(),
        image_run=image_run,
        render_manifest=render_manifest,
        manifest_image_names=frozenset(name for name, _path in ROLLOUT_IMAGES),
        server_schema_dry_run=server_schema_dry_run,
        server_dry_run=lambda *_args, **_kwargs: _result(),
        browser_run=lambda *_args, **_kwargs: _result(),
        browser_token_path=token,
        baseline_probe_factory=lambda _epoch: {
            check_id: (lambda: None)  # type: ignore[dict-item,return-value]
            for check_id in (
                "staging.health",
                "staging.auth",
                "staging.catalog-task",
                "staging.storage-db",
                "staging.network",
            )
        },
        route="https://staging.example.invalid",
        baseline_probe_route="https://staging.example.invalid",
        rehearsal_actions=lambda _candidate, _checkpoint, _isolation: {
            check_id: (lambda: None)  # type: ignore[dict-item,return-value]
            for check_id in REHEARSAL_CHECK_IDS
        },
        rehearsal_identity=lambda _candidate, _checkpoint: (
            "rehearsal-exact-checkpoint",
            "7" * 64,
        ),
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
        loaded_artifacts=loaded_artifacts,
        container_registry=container_registry,
        container_registry_push=container_registry_push,
        manifest_post_image_pin=manifest_post_image_pin,
        gb10_candidate_source_run=gb10_candidate_source_run,
    )


def _check(plan, check_id: str):
    return next(check for check in plan.registry.checks if check.spec.check_id == check_id)


def _probe(plan, check_id: str):
    return _check(plan, check_id).operations[CheckOperation.PROBE](plan.context)


def test_fresh_runtime_sources_forwards_post_pin_to_manifest_session_artifact(
    tmp_path: Path,
) -> None:
    """Catch omission in ``_tier1`` or ``build_manifest_preflight_checks`` forwarding."""
    candidate = _candidate()
    registry = "192.168.50.13:5000"
    image_ids = {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }
    manifest_digests = {
        name: f"sha256:{hashlib.sha256((name + '-manifest').encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }
    rendered_primary = _rendered_with_lifecycle_cronjob().replace(
        "staging-1111111",
        candidate.image_tag,
    )
    render_calls: list[object] = []
    callback_payloads: list[str] = []
    server_payloads: list[str] = []

    def image_run(argv, _cwd):
        command = tuple(argv)
        if command[:3] == ("docker", "image", "inspect"):
            name = command[-1].rsplit("/", 1)[-1].split(":", 1)[0]
            entrypoint = (
                ["node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs"]
                if name == "loom-staging-admin-browser-smoke"
                else (["docker-entrypoint.sh"] if name == "loom-rehearsal-postgres" else [])
            )
            return _result(
                json.dumps(
                    [
                        {
                            "Id": image_ids[name],
                            "Os": "linux",
                            "Architecture": "amd64",
                            "Config": {
                                "Entrypoint": entrypoint,
                                "Labels": {
                                    "org.opencontainers.image.revision": candidate.resolved_sha
                                },
                            },
                        }
                    ]
                )
            )
        if command[:3] == ("docker", "manifest", "inspect"):
            name = command[-1].rsplit("/", 1)[-1].split(":", 1)[0]
            return _result(
                json.dumps(
                    {
                        "Descriptor": {"digest": manifest_digests[name]},
                        "SchemaV2Manifest": {"config": {"digest": image_ids[name]}},
                    }
                )
            )
        return _result()

    def render_manifest() -> str:
        render_calls.append(object())
        return rendered_primary

    def manifest_post_image_pin(rendered: str) -> str:
        callback_payloads.append(rendered)
        return rendered + "# gb10-manifest-post-image-pin-sentinel\n"

    def server_schema_dry_run(rendered: str):
        server_payloads.append(rendered)
        return _result()

    sources = _runtime_sources(
        tmp_path,
        render_manifest=render_manifest,
        image_run=image_run,
        server_schema_dry_run=server_schema_dry_run,
        manifest_post_image_pin=manifest_post_image_pin,
        container_registry=registry,
        container_registry_push="localhost:5000",
    )
    plan = sources.build(mutation_epoch=9).prebackup_plan(candidate)

    assert _probe(plan, "images.build").passed
    render_probe = _probe(plan, "manifests.render")
    server_probe = _probe(plan, "manifests.server-schema")

    assert render_probe.passed
    assert server_probe.passed
    assert len(render_calls) == 1
    assert len(callback_payloads) == 1
    expected_artifact_yaml = (
        callback_payloads[0] + "# gb10-manifest-post-image-pin-sentinel\n"
    )
    for name, _path in ROLLOUT_IMAGES:
        assert f"{registry}/{name}@{manifest_digests[name]}" in callback_payloads[0]
    assert server_payloads == [expected_artifact_yaml]
    assert render_probe.evidence["rendered-sha256"] == hashlib.sha256(
        expected_artifact_yaml.encode()
    ).hexdigest()


def test_loaded_runtime_sources_reuse_seeded_manifest_without_rematerializing(
    tmp_path: Path,
) -> None:
    """Catch removal of ``artifact=loaded_artifacts.manifests`` from the session seed."""
    candidate = _candidate()
    images = replace(_images(), plan_digest=image_plan_digest())
    stored_yaml = _rendered_with_lifecycle_cronjob().replace(
        "staging-1111111",
        candidate.image_tag,
    ) + "# gb10-loaded-manifest-sentinel\n"
    manifests = inspect_rendered_manifests(
        stored_yaml,
        image_tag=candidate.image_tag,
        namespace="loom-staging",
        image_digests=images.image_digests,
    )
    migration = _migration(
        images,
        candidate_tree=candidate.resolved_tree or "",
        image_tag=candidate.image_tag,
    )
    production_defaults = _production_defaults(
        candidate_tree=candidate.resolved_tree or "",
    )
    artifact_root = (tmp_path / "loaded-artifacts").resolve()
    loaded = LoadedPreflightArtifacts(
        publication=PreflightArtifactPublication(
            candidate_sha=candidate.resolved_sha,
            candidate_tree=candidate.resolved_tree or "",
            mutation_epoch=9,
            bundle_digest="8" * 64,
            descriptor_path=artifact_root / "artifact.json",
            rendered_manifest_path=artifact_root / "rendered.yaml",
            migration_manifest_path=artifact_root / "migration.yaml",
            production_defaults_path=artifact_root / "production-defaults.json",
            image_artifact_sha256=images.artifact_digest,
            manifest_artifact_sha256=manifests.artifact_digest,
            rendered_manifest_sha256=manifests.rendered_sha256,
            migration_manifest_artifact_sha256=migration.artifact_digest,
            migration_manifest_sha256=migration.rendered_sha256,
            migration_job_name=migration.job_name,
            migration_image_id=migration.image_id,
            migration_plan_sha256=migration.migration_plan_sha256,
            migration_target_revision=migration.migration_target_revision,
            browser_report_schema_sha256="9" * 64,
            production_defaults_sha256=production_defaults.artifact_digest,
        ),
        images=images,
        manifests=manifests,
        migration=migration,
        production_defaults=production_defaults,
    )
    render_calls: list[object] = []
    callback_payloads: list[str] = []
    server_payloads: list[str] = []

    def render_manifest() -> str:
        render_calls.append(object())
        raise AssertionError("loaded manifest session rendered again")

    def manifest_post_image_pin(rendered: str) -> str:
        callback_payloads.append(rendered)
        raise AssertionError("loaded manifest session ran post-image pin again")

    def server_schema_dry_run(rendered: str):
        server_payloads.append(rendered)
        return _result()

    sources = _runtime_sources(
        tmp_path,
        render_manifest=render_manifest,
        image_run=lambda *_args, **_kwargs: _result(),
        server_schema_dry_run=server_schema_dry_run,
        manifest_post_image_pin=manifest_post_image_pin,
        loaded_artifacts=loaded,
    )
    plan = sources.build(mutation_epoch=9).prebackup_plan(candidate)

    render_probe = _probe(plan, "manifests.render")
    server_probe = _probe(plan, "manifests.server-schema")

    assert render_probe.passed
    assert server_probe.passed
    assert render_calls == []
    assert callback_payloads == []
    assert server_payloads == [stored_yaml]
    assert render_probe.evidence["rendered-sha256"] == manifests.rendered_sha256
    assert render_probe.evidence["artifact-digest"] == manifests.artifact_digest


def test_fresh_authority_is_explicit_and_does_not_claim_a_lease() -> None:
    authority = BackupAdmissionAuthority.fresh(
        schema_revision="0066",
        object_inventory_root="d" * 64,
    )

    assert authority.lease_source() is None
    assert authority.expected_lease_digest == "0" * 64
    assert authority.source_request_id == "fresh-checkpoint"
    assert set(authority.component_sha256) == {
        "k8s_secrets",
        "object_inventory",
        "postgres",
    }


def test_nested_gb10_checks_share_a_bounded_fleet_budget() -> None:
    # Mount and host readiness may run in the same DAG wave. Candidate-source
    # walks one shared NFS Git tree, so it is a separately serialized resource
    # class rather than another fleet-wide fan-out.
    assert GB10_PREFLIGHT_FLEET_CONCURRENCY == 4
    assert GB10_CANDIDATE_SOURCE_CONCURRENCY == 1


def test_unavailable_authority_builds_a_fail_closed_registered_check() -> None:
    authority = BackupAdmissionAuthority.unavailable(schema_revision="0067")

    assert authority.source_request_id == "unavailable-authority"
    assert authority.object_inventory_root == "0" * 64
    try:
        authority.lease_source()
    except RuntimeError as exc:
        assert str(exc) == "backup admission authority is unavailable"
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("unavailable backup authority returned a lease")


def test_sources_build_complete_registry_and_checkpoint_manifest_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()

    def command(*_args, **_kwargs):
        return _result()

    gb10_concurrency: dict[str, int] = {}
    candidate_source_options: dict[str, object] = {}
    host_readiness_options: dict[str, object] = {}
    missing_retry = object()
    manifest_retries: list[object] = []
    for builder_name in (
        "build_gb10_ssh_topology_check",
        "build_gb10_candidate_source_check",
        "build_gb10_host_readiness_check",
    ):
        original = getattr(runtime_sources_module, builder_name)

        def record_concurrency(
            *args,
            _builder_name=builder_name,
            _original=original,
            **kwargs,
        ):
            gb10_concurrency[_builder_name] = kwargs["max_concurrency"]
            if _builder_name == "build_gb10_candidate_source_check":
                candidate_source_options.update(
                    run=args[0],
                    settle_attempts=kwargs["settle_attempts"],
                    settle_interval_seconds=kwargs["settle_interval_seconds"],
                )
            if _builder_name == "build_gb10_host_readiness_check":
                host_readiness_options.update(
                    run=args[0],
                    settle_attempts=kwargs["settle_attempts"],
                    settle_interval_seconds=kwargs["settle_interval_seconds"],
                )
            return _original(*args, **kwargs)

        monkeypatch.setattr(runtime_sources_module, builder_name, record_concurrency)

    original_manifest_builder = runtime_sources_module.build_manifest_preflight_checks

    def record_manifest_retry(*args, **kwargs):
        manifest_retries.append(kwargs.get("field_ownership_retry_render", missing_retry))
        return original_manifest_builder(*args, **kwargs)

    monkeypatch.setattr(
        runtime_sources_module,
        "build_manifest_preflight_checks",
        record_manifest_retry,
    )

    def candidate_source_command(*_args, **_kwargs):
        return _result()

    sources = _runtime_sources(
        tmp_path,
        render_manifest=lambda: "",
        image_run=command,
        server_schema_dry_run=command,
        gb10_candidate_source_run=candidate_source_command,
        gb10_run=command,
    )

    runtime = sources.build(mutation_epoch=9)
    plan = runtime.prebackup_plan(candidate)

    assert plan.registry.through_tier == 3
    assert len(plan.registry.checks) == len(
        [check for check in plan.registry.checks if check.spec.tier in {0, 1, 2, 3}]
    )
    assert {check.spec.check_id for check in plan.registry.checks} >= {
        "candidate.identity",
        "readonly.authority",
        "backup.lease-eligibility",
        "external-supervisor.predecessor",
        "images.build",
        "staging.release-baseline",
        "rehearsal.cleanup",
    }
    assert plan.context.bindings["staging.mutation-epoch"] == 9
    assert plan.context.bindings["backup.source-request"] == "fresh-checkpoint"
    assert plan.context.bindings["schema.revision"] == "0066"
    assert plan.context.bindings["database.schema.revision"] == "0074"
    assert plan.context.bindings["candidate.image-tag"] == candidate.image_tag
    assert gb10_concurrency == {
        "build_gb10_candidate_source_check": GB10_CANDIDATE_SOURCE_CONCURRENCY,
        "build_gb10_host_readiness_check": GB10_PREFLIGHT_FLEET_CONCURRENCY,
        "build_gb10_ssh_topology_check": GB10_PREFLIGHT_FLEET_CONCURRENCY,
    }
    assert candidate_source_options == {
        "run": candidate_source_command,
        "settle_attempts": 2,
        "settle_interval_seconds": 1.0,
    }
    assert host_readiness_options == {
        "run": command,
        "settle_attempts": 2,
        "settle_interval_seconds": 1.0,
    }

    checkpoint_runtime = replace(
        sources,
        permit_reserved_rotation_candidate=True,
    ).build(mutation_epoch=9)
    checkpoint_runtime.prebackup_plan(candidate)

    assert manifest_retries[:3] == [None, None, None]
    assert manifest_retries[3:] == [render_checkpoint_guard_field_ownership_payload] * 3

    detached_candidate = replace(candidate, resolved_tree="f" * 40)
    assert detached_candidate.resolved_tree is not None
    images = replace(_images(), plan_digest=image_plan_digest())
    rendered = _rendered_with_lifecycle_cronjob().replace(
        "staging-1111111",
        detached_candidate.image_tag,
    )
    manifests = inspect_rendered_manifests(
        rendered,
        image_tag=detached_candidate.image_tag,
        namespace="loom-staging",
        image_digests=images.image_digests,
    )
    migration = _migration(
        images,
        candidate_tree=detached_candidate.resolved_tree,
        image_tag=detached_candidate.image_tag,
    )
    production_defaults = _production_defaults(
        candidate_tree=detached_candidate.resolved_tree,
    )
    artifact_root = (tmp_path / "loaded-artifacts").resolve()
    publication = PreflightArtifactPublication(
        candidate_sha=detached_candidate.resolved_sha,
        candidate_tree=detached_candidate.resolved_tree,
        mutation_epoch=9,
        bundle_digest="8" * 64,
        descriptor_path=artifact_root / "artifact.json",
        rendered_manifest_path=artifact_root / "rendered.yaml",
        migration_manifest_path=artifact_root / "migration.yaml",
        production_defaults_path=artifact_root / "production-defaults.json",
        image_artifact_sha256=images.artifact_digest,
        manifest_artifact_sha256=manifests.artifact_digest,
        rendered_manifest_sha256=manifests.rendered_sha256,
        migration_manifest_artifact_sha256=migration.artifact_digest,
        migration_manifest_sha256=migration.rendered_sha256,
        migration_job_name=migration.job_name,
        migration_image_id=migration.image_id,
        migration_plan_sha256=migration.migration_plan_sha256,
        migration_target_revision=migration.migration_target_revision,
        browser_report_schema_sha256="9" * 64,
        production_defaults_sha256=production_defaults.artifact_digest,
    )
    loaded = LoadedPreflightArtifacts(
        publication=publication,
        images=images,
        manifests=manifests,
        migration=migration,
        production_defaults=production_defaults,
    )
    ownership_payloads: list[str] = []

    def field_ownership_dry_run(payload: str):
        ownership_payloads.append(payload)
        lifecycle = next(
            document
            for document in yaml.safe_load_all(payload)
            if document.get("kind") == "CronJob"
        )
        return type(
            "Result",
            (),
            {
                "returncode": 0 if lifecycle["spec"]["suspend"] is True else 1,
                "stdout": "",
            },
        )()

    detached_runtime = replace(
        sources,
        candidate=detached_candidate,
        loaded_artifacts=loaded,
        permit_reserved_rotation_candidate=True,
        server_dry_run=field_ownership_dry_run,
    ).build(mutation_epoch=9)
    detached_plan = detached_runtime.prebackup_plan(detached_candidate)
    ownership_check = next(
        check
        for check in detached_plan.registry.checks
        if check.spec.check_id == "manifests.field-ownership"
    )

    probe = ownership_check.operations[CheckOperation.PROBE](detached_plan.context)

    assert probe.passed
    assert [
        next(
            document["spec"]["suspend"]
            for document in yaml.safe_load_all(payload)
            if document.get("kind") == "CronJob"
        )
        for payload in ownership_payloads
    ] == [False, True]
