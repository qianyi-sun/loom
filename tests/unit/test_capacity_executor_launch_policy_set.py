"""Separate application/build runtime policies under one exact pool root."""

from dataclasses import replace
from importlib import import_module

import pytest

from loom_capacity_executor.launch_renderer import render_signed_launch
from loom_capacity_executor.runtime_profiles import resolve_runtime_profile
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def _profiles():
    app = launch_context_fixture().profile
    build = app.model_copy(update={
        "profile_id": "personal-build", "profile_digest": "b" * 64,
        "image_digest": "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "c" * 64,
    })
    return app, build


def _policy():
    module = import_module("loom_capacity_executor.launch_policy_set")
    app, build = _profiles()
    policy = module.PoolLaunchPolicyV3(
        pool_id=app.pool_id, pool_generation=app.pool_generation,
        entries=(
            module.PurposeLaunchPolicyV3(purpose="application-worker", profile_sha256=module.full_launch_profile_digest(app)),
            module.PurposeLaunchPolicyV3(purpose="personal-build-worker", profile_sha256=module.full_launch_profile_digest(build)),
        ),
    )
    root = module.canonical_pool_launch_policy_digest(policy)
    profiles = tuple(profile.model_copy(update={"controller_authority_sha256": root}) for profile in (app, build))
    return module, policy, root, profiles


def _resolve(module, policy, root, profiles, *, purpose="personal-build-worker", index=1, binding=None):
    profile = profiles[index]
    if binding is None:
        binding = launch_context_fixture().binding.model_copy(update={
            "profile_id": profile.profile_id, "profile_digest": profile.profile_digest,
        })
    return module.resolve_typed_runtime_profile(
        binding, profiles, policy=policy, purpose=purpose, controller_authority_sha256=root,
    )


def test_both_runtime_images_resolve_under_same_typed_root():
    module, policy, root, profiles = _policy()
    assert _resolve(module, policy, root, profiles) == profiles[1]
    assert _resolve(module, policy, root, profiles, purpose="application-worker", index=0) == profiles[0]
    assert profiles[0].image_digest != profiles[1].image_digest
    assert module.full_launch_profile_digest(profiles[0]) == module.full_launch_profile_digest(_profiles()[0])


def test_legacy_resolver_and_renderer_do_not_accept_policy_set_root():
    module, policy, root, profiles = _policy()
    context = launch_context_fixture()
    with pytest.raises(ValueError):
        resolve_runtime_profile(context.binding, profiles, controller_authority_sha256=root)
    with pytest.raises(ValueError):
        render_signed_launch(replace(context, profile=profiles[0], controller_authority=context.controller_authority.model_copy(
            update={"controller_authority_sha256": root},
        )))


@pytest.mark.parametrize("purpose", ("application-worker", "trial", "", None))
def test_build_profile_cannot_be_selected_with_another_purpose(purpose):
    module, policy, root, profiles = _policy()
    with pytest.raises(ValueError):
        _resolve(module, policy, root, profiles, purpose=purpose)


@pytest.mark.parametrize("changes", (
    {"image_digest": "ghcr.io/foreign/worker@sha256:" + "d" * 64},
    {"profile_id": "foreign"}, {"profile_generation": 2}, {"profile_digest": "e" * 64},
    {"shape_id": "different-shape"}, {"controller_authority_sha256": "f" * 64},
    {"cpus": 99}, {"pool_id": "gb10"}, {"pool_generation": 99},
))
def test_profile_identity_resource_or_image_substitution_is_rejected(changes):
    module, policy, root, profiles = _policy()
    mutated = (profiles[0], profiles[1].model_copy(update=changes))
    with pytest.raises(ValueError):
        _resolve(module, policy, root, mutated)


def test_build_runtime_cannot_borrow_application_profile_identity():
    module, policy, root, profiles = _policy()
    forged = profiles[1].model_copy(update={
        "profile_id": profiles[0].profile_id, "profile_digest": profiles[0].profile_digest,
    })
    with pytest.raises(ValueError):
        _resolve(module, policy, root, (forged,), index=0, binding=launch_context_fixture().binding)


def test_policy_digest_commits_purpose_and_pool_generation():
    module, policy, root, profiles = _policy()
    for changed in (policy.model_copy(update={"pool_generation": 2}),
                    policy.model_copy(update={"entries": tuple(replace_entry.model_copy(update={
                        "purpose": "application-worker" if replace_entry.purpose == "personal-build-worker" else "personal-build-worker",
                    }) for replace_entry in policy.entries)})):
        assert module.canonical_pool_launch_policy_digest(changed) != root
        with pytest.raises(ValueError):
            _resolve(module, changed, root, profiles)


def test_duplicate_missing_and_extra_profiles_fail_closed():
    module, policy, root, profiles = _policy()
    for supplied in (profiles + (profiles[1],), (profiles[1],), profiles + (_profiles()[1],)):
        with pytest.raises(ValueError):
            _resolve(module, policy, root, supplied, index=0, purpose="application-worker")
