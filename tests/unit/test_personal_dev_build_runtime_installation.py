"""Installation facts join approved native publication and complete pool policies."""

from dataclasses import replace
from importlib import import_module

import pytest

from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    PurposeLaunchPolicyV3,
    canonical_pool_launch_policy_digest,
    full_launch_profile_digest,
)
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context
from tests.unit.test_personal_dev_build_runtime_publication import _load


def installation_input(tmp_path):
    publication = _load(tmp_path)
    preparation = build_membership_input().preparation
    preparation = preparation.model_copy(update={"personal_builds": preparation.personal_builds.model_copy(
        update={"runtime_candidate": publication.candidate})})
    sets = []
    for pool, platform in (("gb10", "linux/arm64"), ("oldlab", "linux/amd64")):
        build = typed_context(pool=pool).profiles[0].model_copy(update={
            "image_digest": next(item.agent_image for item in publication.platforms if item.platform == platform)})
        app = typed_context(pool=pool, purpose="application-worker").profiles[0]
        sets.append((PoolLaunchPolicyV3(pool_id=pool, pool_generation=build.pool_generation, entries=(
            PurposeLaunchPolicyV3(purpose="personal-build-worker", profile_sha256=full_launch_profile_digest(build)),
            PurposeLaunchPolicyV3(purpose="application-worker", profile_sha256=full_launch_profile_digest(app)),
        )), (build, app)))
    return publication, *pin_sets(preparation, sets)


def pin_sets(preparation, sets):
    configs = []
    executors = []
    for policy, profiles in sets:
        root = canonical_pool_launch_policy_digest(policy)
        configs.append((policy, tuple(profile.model_copy(update={"controller_authority_sha256": root}) for profile in profiles)))
        executors.append(next(item for item in preparation.executors if item.pool_id == policy.pool_id).model_copy(
            update={"controller_authority_sha256": root}))
    return preparation.model_copy(update={"executors": tuple(executors)}), tuple(configs)


def resolve(publication, preparation, configs):
    return import_module("loom.personal_dev_build_runtime_installation").resolve_personal_build_runtime_installation(
        publication, preparation=preparation, pool_profiles=configs)


def test_join_resolves_both_native_images_without_allocating_workers(tmp_path):
    publication, preparation, configs = installation_input(tmp_path)
    result = resolve(publication, preparation, configs)
    assert result.candidate == publication.candidate
    assert tuple(item.pool_id for item in result.pools) == ("gb10", "oldlab")
    images = {item.platform: item for item in publication.platforms}
    for pool in result.pools:
        assert pool.builder_image == images[pool.platform].builder_image
        assert pool.agent_image == images[pool.platform].agent_image
        assert pool.launch_profile_sha256 == full_launch_profile_digest(next(
            profiles[0] for policy, profiles in configs if policy.pool_id == pool.pool_id))
    assert resolve(publication, preparation, tuple(reversed(configs))) == result


@pytest.mark.parametrize("boundary", ("candidate", "publication", "missing-pool", "duplicate-pool", "missing-profile", "unapproved-root", "duplicate-platform"))
def test_join_rejects_unapproved_or_incomplete_installation(tmp_path, boundary):
    publication, preparation, configs = installation_input(tmp_path)
    if boundary in ("candidate", "publication"):
        publication = replace(publication, candidate=publication.candidate.model_copy(update={
            "identity" if boundary == "candidate" else "publication_sha256": "c" * (40 if boundary == "candidate" else 64)}))
    elif boundary == "missing-pool":
        configs = configs[:1]
    elif boundary == "duplicate-pool":
        configs = (configs[0], configs[0])
    elif boundary == "missing-profile":
        configs = ((configs[0][0], configs[0][1][:1]), configs[1])
    elif boundary == "duplicate-platform":
        publication = replace(publication, platforms=(publication.platforms[0], publication.platforms[0]))
    else:
        executor = preparation.executors[0].model_copy(update={"controller_authority_sha256": "c" * 64})
        preparation = preparation.model_copy(update={"executors": (executor, preparation.executors[1])})
    with pytest.raises(ValueError):
        resolve(publication, preparation, configs)


@pytest.mark.parametrize("pool_index", (0, 1))
@pytest.mark.parametrize("boundary", ("image", "architecture-image", "purpose", "profile", "generation", "shape", "resources", "domain", "launcher"))
def test_even_pinned_policy_cannot_substitute_template_or_publication(tmp_path, pool_index, boundary):
    publication, preparation, configs = installation_input(tmp_path)
    policy, profiles = configs[pool_index]
    build, app = profiles
    fields = {
        "image": {"image_digest": "ghcr.io/foreign/runtime@sha256:" + "c" * 64},
        "architecture-image": {"image_digest": configs[1 - pool_index][1][0].image_digest},
        "profile": {"profile_digest": "c" * 64},
        "generation": {"profile_generation": build.profile_generation + 1},
        "shape": {"shape_id": "foreign-shape"},
        "resources": {"cpus": build.cpus + 1, "resources": build.resources.model_copy(update={"cpu_millicores": build.resources.cpu_millicores + 1000})},
        "domain": {"resource_domains": (build.resource_domains[0].model_copy(update={"domain_id": "foreign-domain"}),)},
        "launcher": {"trusted_launcher_release_sha256": "c" * 64},
        "purpose": {},
    }
    build = build.model_copy(update=fields[boundary])
    policy = policy.model_copy(update={"entries": (
        PurposeLaunchPolicyV3(purpose="application-worker" if boundary == "purpose" else "personal-build-worker", profile_sha256=full_launch_profile_digest(build)),
        PurposeLaunchPolicyV3(purpose="application-worker", profile_sha256=full_launch_profile_digest(app)),
    )})
    sets = list(configs)
    sets[pool_index] = (policy, (build, app))
    preparation, configs = pin_sets(preparation, sets)
    with pytest.raises(ValueError):
        resolve(publication, preparation, configs)
