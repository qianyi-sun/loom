from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_registry as registry
from scripts.ops import developer_sandbox_crossover_probe as probe
from tests.ops.worker_runtime_binding_fixtures import (
    rich_image_archives,
    worker_runtime_bindings,
)


def _register(principal: str, index: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": registry.REGISTER_KIND,
        "principal_id": principal,
        "idempotency_key": f"registration-key-{index:04d}",
        "display_name": f"Developer {index}",
    }


def _candidate(
    environment: registry.EnvironmentRecord,
    index: int,
) -> dict[str, Any]:
    digit = format(index + 1, "x")
    amd64_config = "sha256:" + format(index + 1, "x") * 64
    arm64_config = "sha256:" + format(index + 5, "x") * 64
    return {
        "schema_version": 1,
        "kind": registry.CANDIDATE_KIND,
        "principal_id": environment.principal_id,
        "idempotency_key": f"candidate-key-{index:04d}",
        "env_id": environment.env_id,
        "candidate_sha": digit * 40,
        "candidate_tree": format(index + 5, "x") * 40,
        "bundle_sha256": format(index + 9, "x") * 64,
        "bundle_size": 1024 + index,
        "image_digests": {
            "amd64": amd64_config,
            "arm64": arm64_config,
        },
        "image_archives": rich_image_archives(
            amd64_config=amd64_config,
            arm64_config=arm64_config,
            seed=f"crossover-{index}",
        ),
    }


def _active_snapshot(tmp_path: Path, count: int = 4) -> dict[str, Any]:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    for index in range(count):
        environment = authority.register(
            _register(f"oidc:example:developer-{index}", index),
        )
        candidate = authority.import_candidate(_candidate(environment, index))
        request = {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": f"deployment-key-{index:04d}",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": 1,
        }
        deployment = authority.begin_deployment(request)
        deployment = authority.record_worker_runtime_bindings(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_resource_generation=1,
            bindings=worker_runtime_bindings(candidate),
        )
        for expected, following in zip(
            registry.DEPLOY_PHASES[:-1],
            registry.DEPLOY_PHASES[1:],
            strict=True,
        ):
            if following == "committed":
                deployment = authority.prepare_deployment_finalization(
                    deployment.deployment_id,
                    principal_id=environment.principal_id,
                    expected_resource_generation=1,
                )
                deployment = authority.record_deployment_finalization(
                    deployment.deployment_id,
                    principal_id=environment.principal_id,
                    expected_resource_generation=1,
                    evidence={
                        "capacity_finalize_receipt_sha256": "1" * 64,
                        "capacity_finalize_check_receipt_sha256": "2" * 64,
                        "runtime_reconcile_receipt_sha256": "3" * 64,
                        "runtime_prepare_check_receipt_sha256": "4" * 64,
                        "acceptance_probe_receipt_sha256": "5" * 64,
                    },
                )
            deployment = authority.advance_deployment(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_phase=expected,
                next_phase=following,
                expected_resource_generation=1,
            )
    return authority.snapshot()


def _target(tmp_path: Path, name: str, index: int) -> probe.SandboxTarget:
    worker = tmp_path / f"{name}-environment.env"
    worker.write_text(
        "\n".join(
            (
                f"LOOM_WORKER_TOKEN=loom_w_{str(index) * 40}",
                f"LOOM_DEV_MINIO_ROOT_USER={name}-access",
                f"LOOM_DEV_MINIO_ROOT_PASSWORD={name}-secret",
                "",
            ),
        ),
        encoding="utf-8",
    )
    worker.chmod(0o600)
    admin = tmp_path / f"{name}-admin.toml"
    admin.write_text(
        f'[admin]\ntoken = "loom_admin_{str(index) * 43}"\n',
        encoding="utf-8",
    )
    admin.chmod(0o600)
    return probe.SandboxTarget(
        sandbox=name,
        env_id=f"denv-{name}-00000000",
        owner_uid=worker.stat().st_uid,
        control_plane_url=f"http://127.0.0.1:{20000 + index}",
        worker_token_file=worker,
        admin_secret_file=admin,
        minio_endpoint=f"http://127.0.0.1:{21000 + index}",
        minio_access_key_file=worker,
        minio_secret_key_file=worker,
        own_bucket=f"{name}-artifacts",
    )


def test_registry_projection_supports_four_and_different_candidates(tmp_path: Path) -> None:
    projection = probe._registry_projection(_active_snapshot(tmp_path))
    assert len(projection["environments"]) == 4
    assert len({row["candidate_sha"] for row in projection["environments"]}) == 4
    assert (
        projection["payload_sha256"]
        == hashlib.sha256(
            probe._canonical(
                {key: value for key, value in projection.items() if key != "payload_sha256"},
            ),
        ).hexdigest()
    )


def test_registry_projection_rejects_fewer_than_two_active(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="fewer than two"):
        probe._registry_projection(_active_snapshot(tmp_path, count=1))


def test_registry_projection_rejects_tamper_even_when_outer_digest_is_resigned(
    tmp_path: Path,
) -> None:
    snapshot = _active_snapshot(tmp_path)
    snapshot["environments"][0]["ports"]["control_plane"] += 1
    unsigned = {key: value for key, value in snapshot.items() if key != "payload_sha256"}
    snapshot["payload_sha256"] = registry._digest(unsigned)
    with pytest.raises(ValueError, match="registry snapshot is invalid"):
        probe._registry_projection(snapshot)


def test_registry_projection_rejects_stale_current_candidate(tmp_path: Path) -> None:
    snapshot = _active_snapshot(tmp_path)
    environment = snapshot["environments"][0]
    foreign = next(
        candidate
        for candidate in snapshot["candidates"]
        if candidate["env_id"] != environment["env_id"]
    )
    environment["current_candidate_id"] = foreign["candidate_id"]
    unsigned = {key: value for key, value in snapshot.items() if key != "payload_sha256"}
    snapshot["payload_sha256"] = registry._digest(unsigned)
    with pytest.raises(ValueError, match="registry snapshot is invalid"):
        probe._registry_projection(snapshot)


def test_build_targets_uses_registry_ports_paths_and_buckets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _active_snapshot(tmp_path)
    monkeypatch.setattr(
        probe,
        "_verify_candidate_repository",
        lambda **kwargs: Path(kwargs["candidate_root"]) / str(kwargs["expected_sha"]),
    )
    targets, candidates = probe.build_targets(snapshot, execute=False)
    projection = probe._registry_projection(snapshot)
    by_runtime = {row["runtime_id"]: row for row in projection["environments"]}
    assert len(targets) == 4
    assert len(candidates) == 4
    for runtime_id, target in targets.items():
        environment = by_runtime[runtime_id]
        assert target.env_id == environment["env_id"]
        assert target.control_plane_url.endswith(
            f":{environment['ports']['control_plane']}",
        )
        assert target.minio_endpoint.endswith(f":{environment['ports']['minio']}")
        assert target.worker_token_file == (
            Path(environment["state_root"]) / "secrets" / "environment.env"
        )
        assert target.own_bucket == environment["artifacts_bucket"]


def test_directed_pairs_are_all_ordered_foreign_pairs() -> None:
    names = ["a", "b", "c", "d"]
    pairs = probe.directed_pairs(names)
    assert len(pairs) == 12
    assert len(set(pairs)) == 12
    assert all(source != target for source, target in pairs)


def test_cli_has_no_per_environment_resource_or_secret_selectors() -> None:
    options = {
        option for action in probe.build_parser()._actions for option in action.option_strings
    }
    assert options == {"-h", "--help", "--execute", "--write-evidence", "--json"}
    source = Path(probe.__file__).read_text(encoding="utf-8")
    for name in ("qianyi", "hongjian", "devansh"):
        assert name not in source
    assert "profiles-dir" not in source
    assert "candidate-sha" not in source


def test_probe_matrix_covers_every_foreign_surface_and_same_controls(
    tmp_path: Path,
) -> None:
    targets = {
        name: _target(tmp_path, name, index)
        for index, name in enumerate(("alpha", "bravo", "charlie", "delta"), start=1)
    }
    results = probe.run_probe_matrix(
        targets,
        execute=False,
        include_same_sandbox=True,
    )
    assert len(results) == 3 * 4 + 4 * 4 * 3
    foreign = [row for row in results if row.source != row.target]
    assert len(foreign) == 4 * 3 * 4
    assert {
        (row.source, row.target) for row in foreign if row.surface == "minio_foreign_bucket"
    } == set(probe.directed_pairs(list(targets)))
    assert all(row.passed for row in results)


def test_evidence_embeds_registry_projection_and_is_secret_free(tmp_path: Path) -> None:
    projection = probe._registry_projection(_active_snapshot(tmp_path))
    targets = {
        name: _target(tmp_path, name, index)
        for index, name in enumerate(("alpha", "bravo"), start=1)
    }
    results = probe.run_probe_matrix(
        targets,
        execute=False,
        include_same_sandbox=True,
    )
    evidence = probe.build_evidence(
        results,
        execute=False,
        registry_snapshot=projection,
        candidates=[],
        runtime_activations=[],
    )
    assert evidence["registry_snapshot"] == projection
    assert evidence["summary"]["failed"] == 0
    assert (
        evidence["payload_sha256"]
        == hashlib.sha256(
            probe._canonical(
                {key: value for key, value in evidence.items() if key != "payload_sha256"},
            ),
        ).hexdigest()
    )
    assert probe.assert_evidence_secret_free(evidence) == []


@pytest.mark.parametrize(
    "url",
    (
        "https://127.0.0.1:20080",
        "http://user:pass@127.0.0.1:20080",
        "http://127.0.0.1:20080/path",
        "http://127.0.0.1",
    ),
)
def test_endpoint_rejects_non_exact_url_shapes(url: str) -> None:
    with pytest.raises(ValueError, match="http://host:port"):
        probe._normalize_http_url(url)


def test_secure_secret_file_rejects_symlink_and_open_mode(tmp_path: Path) -> None:
    path = tmp_path / "secret"
    path.write_text("secret\n", encoding="utf-8")
    path.chmod(0o644)
    with pytest.raises(ValueError, match="mode 0600"):
        probe.secure_secret_file(path, label="secret")
    path.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="non-symlink"):
        probe.secure_secret_file(link, label="secret")


def test_refresh_rejects_runtime_activation_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = {"alpha": _target(tmp_path, "alpha", 1), "bravo": _target(tmp_path, "bravo", 2)}
    candidate = probe.CandidateIdentity(
        sandbox="alpha",
        env_id="denv-alpha-00000000",
        candidate_id="cand-" + "a" * 40,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        compose_project="loom-env-alpha",
        source_repo="/candidate",
        state_path="/snapshot",
        state_payload_sha256="c" * 64,
        updated_at=None,
    )
    activation = probe.RuntimeActivationIdentity(
        sandbox="alpha",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        receipt_path="/receipt",
        payload_sha256="d" * 64,
        fleet_payload_sha256="sha256:" + "e" * 64,
        collected_at="2026-07-29T00:00:00Z",
        expires_at="2026-07-29T00:10:00Z",
        domain_generations={"oldlab": 1, "gb10": 1},
    )
    bravo_candidate = replace(
        candidate,
        sandbox="bravo",
        env_id="denv-bravo-00000000",
        candidate_id="cand-" + "f" * 40,
        candidate_sha="f" * 40,
    )
    bravo_activation = replace(
        activation,
        sandbox="bravo",
        candidate_sha="f" * 40,
    )
    targets["alpha"] = replace(
        targets["alpha"],
        candidate=candidate,
        runtime_activation=activation,
    )
    targets["bravo"] = replace(
        targets["bravo"],
        candidate=bravo_candidate,
        runtime_activation=bravo_activation,
    )
    monkeypatch.setattr(
        probe,
        "load_runtime_activation",
        lambda _root, candidate: replace(
            activation if candidate.sandbox == "alpha" else bravo_activation,
            payload_sha256="0" * 64,
        ),
    )
    with pytest.raises(ValueError, match="changed during live probes"):
        probe.refresh_runtime_activations(targets, runtime_root=tmp_path)
