from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from loom.pipeline.keys import digest_bytes
from loom.pipeline.recipes import (
    OfficialRecipeRegistration,
    OfficialRecipeRegistry,
    reject_secret_literals,
)
from loom.pipeline.spec import RecipeIdentityV1, RequestRendererLockV1, RunGraphSpecV1

DIGEST_0 = "sha256:" + "0" * 64
DIGEST_1 = "sha256:" + "1" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "2" * 64


def factory(identity: RecipeIdentityV1, parameters: Mapping[str, Any]) -> RunGraphSpecV1:
    return RunGraphSpecV1.model_validate(
        {
            "schema_version": "loom.run-graph.v1",
            "recipe": identity.model_dump(mode="json"),
            "inputs": [],
            "parameters": dict(parameters),
            "budget": {
                "max_provider_cost_usd": "0",
                "max_gpu_seconds": 0,
                "max_wall_seconds": 60,
                "max_artifact_bytes": 10_000,
                "max_stage_runs": 1,
                "max_attempts_total": 1,
            },
            "nodes": [
                {
                    "node_kind": "container",
                    "node_key": "stage",
                    "image": IMAGE,
                    "argv": ["run"],
                    "workdir": "/workspace",
                    "resource_profile": "cpu@1",
                    "network_profile": "none",
                    "needs": [],
                    "inputs": [],
                    "outputs": [],
                    "request_renderer": None,
                    "checkpoint": None,
                    "fanout": None,
                    "fanout_commit": None,
                    "timeout_seconds": 60,
                    "max_attempts": 1,
                    "failure_policy": "fail_run",
                }
            ],
        }
    )


def registration(
    name: str = "ordinary-recipe", policy: str = "ordinary"
) -> OfficialRecipeRegistration:
    return OfficialRecipeRegistration(
        name=name,
        version=1,
        submission_policy=policy,  # type: ignore[arg-type]
        factory=factory,
        parameter_contract_digest=DIGEST_0,
        source_lock_digest=DIGEST_1,
    )


def test_registry_resolves_only_code_backed_official_recipe() -> None:
    item = registration()
    registry = OfficialRecipeRegistry((item,))

    graph = registry.resolve_ordinary("ordinary-recipe", 1, {"mode": "quick"})

    assert graph.recipe == item.identity
    assert graph.parameters == {"mode": "quick"}
    assert registry.list_identities() == (item.identity,)
    with pytest.raises(KeyError, match="unknown official Recipe"):
        registry.get("caller-graph", 1)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(item)


def test_registry_identity_listing_is_bytewise_name_then_version_order() -> None:
    first_v2 = OfficialRecipeRegistration(
        name="a-recipe",
        version=2,
        submission_policy="ordinary",
        factory=factory,
        parameter_contract_digest=DIGEST_0,
        source_lock_digest=DIGEST_1,
    )
    first_v1 = OfficialRecipeRegistration(
        name="a-recipe",
        version=1,
        submission_policy="ordinary",
        factory=factory,
        parameter_contract_digest=DIGEST_0,
        source_lock_digest=DIGEST_1,
    )
    last = registration("z-recipe")

    registry = OfficialRecipeRegistry((last, first_v2, first_v1))

    assert [(item.name, item.version) for item in registry.list_identities()] == [
        ("a-recipe", 1),
        ("a-recipe", 2),
        ("z-recipe", 1),
    ]


def test_acceptance_policy_is_reserved_to_fixed_preflight_recipe() -> None:
    with pytest.raises(ValueError, match="only the fixed acceptance preflight"):
        registration("another-recipe", "acceptance_authorization_only")
    with pytest.raises(ValueError, match="must use"):
        registration("behavior-recovery-acceptance-preflight", "ordinary")
    with pytest.raises(ValueError, match="invalid Recipe submission policy"):
        registration("another-recipe", "raw_graph")

    acceptance = registration(
        "behavior-recovery-acceptance-preflight", "acceptance_authorization_only"
    )
    ordinary = registration()
    registry = OfficialRecipeRegistry((acceptance, ordinary))

    with pytest.raises(PermissionError, match="not available to ordinary"):
        registry.resolve_ordinary(acceptance.name, acceptance.version, {})
    with pytest.raises(PermissionError, match="matrix authorization is required"):
        registry.resolve_acceptance_preflight(
            name=acceptance.name,
            version=acceptance.version,
            parameters={},
            active_same_team_matrix_authorization=False,
        )
    graph = registry.resolve_acceptance_preflight(
        name=acceptance.name,
        version=acceptance.version,
        parameters={"temperature": 0},
        active_same_team_matrix_authorization=True,
    )
    assert graph.recipe == acceptance.identity
    with pytest.raises(PermissionError, match="not the fixed acceptance preflight"):
        registry.resolve_acceptance_preflight(
            name=ordinary.name,
            version=ordinary.version,
            parameters={},
            active_same_team_matrix_authorization=True,
        )


@pytest.mark.parametrize(
    "value",
    [
        {"api_key": "not-even-a-real-key"},
        {"nested": {"password": "hunter2"}},
        {"header": "Bearer abcdefghijklmnop"},
        {"credential_ref": "plain-text-reference"},
        ["github_pat-abcdefghijklmnop"],
    ],
)
def test_secret_literals_and_non_opaque_references_are_rejected(value: object) -> None:
    with pytest.raises(ValueError):
        reject_secret_literals(value)


@pytest.mark.parametrize(
    "value",
    [
        {"provider_ref": "loom://provider/team/default"},
        {"secret_reference_id": "k8s-secret://team/worker"},
    ],
)
def test_opaque_reference_ids_are_allowed(value: object) -> None:
    reject_secret_literals(value)


def test_resolution_rejects_secret_before_factory_runs() -> None:
    called = False

    def guarded_factory(
        identity: RecipeIdentityV1, parameters: Mapping[str, Any]
    ) -> RunGraphSpecV1:
        nonlocal called
        called = True
        return factory(identity, parameters)

    item = OfficialRecipeRegistration(
        name="guarded",
        version=1,
        submission_policy="ordinary",
        factory=guarded_factory,
        parameter_contract_digest=DIGEST_0,
        source_lock_digest=DIGEST_1,
    )

    with pytest.raises(ValueError, match="secret-looking field name"):
        item.resolve({"token": "value"})
    assert called is False


def test_factory_cannot_change_identity_or_parameters() -> None:
    def wrong_identity(identity: RecipeIdentityV1, parameters: Mapping[str, Any]) -> RunGraphSpecV1:
        graph = factory(identity, parameters)
        value = graph.model_dump(mode="json")
        value["recipe"] = {"name": "other", "version": 1, "digest": DIGEST_0}
        return RunGraphSpecV1.model_validate(value)

    def wrong_parameters(
        identity: RecipeIdentityV1, parameters: Mapping[str, Any]
    ) -> RunGraphSpecV1:
        return factory(identity, {**parameters, "injected": True})

    base = registration()
    with pytest.raises(ValueError, match="wrong immutable identity"):
        OfficialRecipeRegistration(
            name=base.name,
            version=base.version,
            submission_policy=base.submission_policy,
            factory=wrong_identity,
            parameter_contract_digest=base.parameter_contract_digest,
            source_lock_digest=base.source_lock_digest,
        ).resolve({})
    with pytest.raises(ValueError, match="freeze the declared parameters exactly"):
        OfficialRecipeRegistration(
            name=base.name,
            version=base.version,
            submission_policy=base.submission_policy,
            factory=wrong_parameters,
            parameter_contract_digest=base.parameter_contract_digest,
            source_lock_digest=base.source_lock_digest,
        ).resolve({})


def test_registry_startup_and_resolution_rehash_renderer_locks(tmp_path: Path) -> None:
    renderer = tmp_path / "renderer.py"
    renderer.write_bytes(b"def render():\n    return {}\n")
    lock = RequestRendererLockV1.model_validate(
        {
            "name": "stage_request",
            "version": 1,
            "entrypoint": "renderer:render",
            "files": [
                {"repo_path": "renderer.py", "sha256": digest_bytes(renderer.read_bytes())}
            ],
        }
    )
    item = OfficialRecipeRegistration(
        name="locked",
        version=1,
        submission_policy="ordinary",
        factory=factory,
        parameter_contract_digest=DIGEST_0,
        source_lock_digest=DIGEST_1,
        renderer_locks=(lock,),
    )

    with pytest.raises(ValueError, match="repository root at registry startup"):
        OfficialRecipeRegistry((item,))
    registry = OfficialRecipeRegistry((item,), repo_root=tmp_path)
    renderer.write_bytes(b"def render():\n    return {'drift': True}\n")
    with pytest.raises(ValueError, match="renderer lock file drift"):
        registry.resolve_ordinary("locked", 1, {})
