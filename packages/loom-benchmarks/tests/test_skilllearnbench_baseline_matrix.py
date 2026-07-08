"""SkillLearnBench baseline-matrix + online-mode correctness (#672 PR-4).

Confirms the 25 offline baseline-matrix rows land distinct skill trees
into converted bundles - the baselines really are separate systems
under test - and that the 5 online-mode rows skip the baked-in copy so
the family_run state mount populates the target directory at trial
start (Item 7, Option B).

The upstream `skills/<baseline>/<family>/...` layout is mirrored in
tmp_path against a common fake upstream repo tree; each per-baseline
test picks its own row via `SkillLearnBenchAdapter._params` and asserts
the subdir names + a sha256 content hash of the injected skills tree.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from loom_benchmarks.adapters.skilllearnbench import (
    SkillLearnBenchAdapter,
    SkillLearnBenchB1OneShotClaudeHaiku45Adapter,
    SkillLearnBenchB2SelfFeedbackClaudeHaiku45Adapter,
    SkillLearnBenchB3TeacherFeedbackClaudeHaiku45Adapter,
    SkillLearnBenchB4SkillCreatorClaudeHaiku45Adapter,
    SkillLearnBenchHumanAuthoredAdapter,
    SkillLearnBenchOnlineFromB1OneShotClaudeHaiku45Adapter,
    SkillLearnBenchOnlineFromHumanAuthoredAdapter,
)
from loom_benchmarks.util import sha256_of_dir

# Family used across the fixtures — `anthropic-poster-design` is one of
# the SLB families with published skill dirs for every baseline. The
# skill dir names below were pulled directly from
# `gh api repos/cxcscmu/SkillLearnBench/contents/skills/<baseline>/anthropic-poster-design`.
_FAMILY = "anthropic-poster-design"
_TASK = "anthropic-poster-design-1"
_BASELINE_SKILL_DIRS: dict[str, tuple[str, ...]] = {
    "human_authored": ("brand-guidelines",),
    "b1-one-shot-claude-haiku-4-5": (
        "anthropic-brand-colors",
        "python-pillow-graphics",
        "technical-illustration-design",
    ),
    "b2-self-feedback-claude-haiku-4-5": (
        "run2_advanced-pil-techniques",
        "run2_brand-color-strategy",
        "run2_professional-technical-layout",
    ),
    "b3-teacher-feedback-claude-haiku-4-5": (
        "run3_Generate-Nova-Technical-Exploded-View-Poster",
    ),
    "b4-skill-creator-claude-haiku-4-5": (
        "anthropic-brand-system",
        "technical-poster-generation",
    ),
}


def _write_bundle(root: Path) -> Path:
    bundle = root / "repo" / "tasks" / _FAMILY / _TASK
    (bundle / "environment").mkdir(parents=True)
    (bundle / "tests").mkdir()
    (bundle / "environment" / "Dockerfile").write_text(
        "FROM ubuntu:24.04\n"
        "RUN apt-get update && apt-get install -y python3\n"
        "WORKDIR /root\n"
        "COPY skills /root/.claude/skills\n",
    )
    (bundle / "task.toml").write_text(
        "[task]\n"
        f'id = "{_TASK}"\n'
        'name = "Baseline correctness fixture"\n'
        'category = "Image Generation"\n'
        'difficulty = "Easy"\n'
        "\n"
        "[evaluation]\n"
        'required_files = ["/root/poster.png"]\n',
    )
    (bundle / "instruction.md").write_text(
        "Produce a poster following the mounted skills.\n",
    )
    (bundle / "tests" / "test.sh").write_text(
        "#!/bin/bash\n"
        "mkdir -p /logs/verifier\n"
        "echo 1 > /logs/verifier/reward.txt\n",
    )
    return bundle


def _stage_skills(root: Path) -> None:
    """Populate `<root>/skills/<baseline>/<family>/<skill>/README.md`
    for every baseline; contents differ per skill so the sha256 of the
    resulting skills tree is deterministic and per-baseline distinct."""
    for baseline, skills in _BASELINE_SKILL_DIRS.items():
        for skill in skills:
            skill_dir = root / "skills" / baseline / _FAMILY / skill
            skill_dir.mkdir(parents=True)
            (skill_dir / "README.md").write_text(
                f"# {skill}\nBaseline: {baseline}\nFamily: {_FAMILY}\n",
            )


@pytest.fixture
def staged_upstream(tmp_path: Path) -> Iterator[Path]:
    _write_bundle(tmp_path)
    _stage_skills(tmp_path)
    yield tmp_path


@pytest.mark.parametrize(
    ("adapter_cls", "expected_method"),
    [
        (SkillLearnBenchHumanAuthoredAdapter, "human_authored"),
        (
            SkillLearnBenchB1OneShotClaudeHaiku45Adapter,
            "b1-one-shot-claude-haiku-4-5",
        ),
        (
            SkillLearnBenchB2SelfFeedbackClaudeHaiku45Adapter,
            "b2-self-feedback-claude-haiku-4-5",
        ),
        (
            SkillLearnBenchB3TeacherFeedbackClaudeHaiku45Adapter,
            "b3-teacher-feedback-claude-haiku-4-5",
        ),
        (
            SkillLearnBenchB4SkillCreatorClaudeHaiku45Adapter,
            "b4-skill-creator-claude-haiku-4-5",
        ),
    ],
)
def test_baseline_row_injects_matching_upstream_skill_dir(
    adapter_cls: type[SkillLearnBenchAdapter],
    expected_method: str,
    staged_upstream: Path,
) -> None:
    """Every baseline row picks its own upstream skill directory and
    materialises those subdirs into the converted bundle's ``skills/``
    (the target the SLB Dockerfile's ``COPY skills`` will read)."""
    adapter = adapter_cls()
    assert adapter.skill_method == expected_method

    instance = next(iter(
        adapter.list_instances(source_dir=staged_upstream, split="test"),
    ))
    out_dir = staged_upstream / "out"
    adapter.convert_instance(instance, out_dir=out_dir)

    expected_names = set(_BASELINE_SKILL_DIRS[expected_method])
    got_names = {
        p.name for p in (out_dir / "skills").iterdir() if p.is_dir()
    }
    assert got_names == expected_names, (
        f"baseline {expected_method}: expected {expected_names}, "
        f"got {got_names}"
    )


def test_all_baselines_produce_distinct_skill_tree_hashes(
    staged_upstream: Path,
) -> None:
    """Convert one instance under each baseline row and compute a
    sha256 of the produced ``skills/`` subtree; assert every hash is
    unique. Proves the baselines are not accidentally aliased to the
    same upstream directory."""
    baselines: list[tuple[str, type[SkillLearnBenchAdapter]]] = [
        ("human_authored", SkillLearnBenchHumanAuthoredAdapter),
        (
            "b1-one-shot-claude-haiku-4-5",
            SkillLearnBenchB1OneShotClaudeHaiku45Adapter,
        ),
        (
            "b2-self-feedback-claude-haiku-4-5",
            SkillLearnBenchB2SelfFeedbackClaudeHaiku45Adapter,
        ),
        (
            "b3-teacher-feedback-claude-haiku-4-5",
            SkillLearnBenchB3TeacherFeedbackClaudeHaiku45Adapter,
        ),
        (
            "b4-skill-creator-claude-haiku-4-5",
            SkillLearnBenchB4SkillCreatorClaudeHaiku45Adapter,
        ),
    ]
    hashes: dict[str, str] = {}
    for method, cls in baselines:
        adapter = cls()
        instance = next(iter(
            adapter.list_instances(source_dir=staged_upstream, split="test"),
        ))
        out_dir = staged_upstream / f"out-{method}"
        adapter.convert_instance(instance, out_dir=out_dir)
        hashes[method] = sha256_of_dir(out_dir / "skills")

    assert len(set(hashes.values())) == len(hashes), (
        f"expected 5 distinct baseline signatures, got: {hashes}"
    )


def test_online_mode_skips_baked_in_skill_copy(staged_upstream: Path) -> None:
    """When ``params.online_mode == 'true'`` the SLB adapter must NOT
    copy the seed baseline into ``out_dir/skills`` at convert time -
    the family_run state mount populates it at trial start (Item 7,
    Option B)."""
    adapter = SkillLearnBenchOnlineFromHumanAuthoredAdapter()
    assert adapter.online_mode is True

    instance = next(iter(
        adapter.list_instances(source_dir=staged_upstream, split="test"),
    ))
    out_dir = staged_upstream / "out-online"
    adapter.convert_instance(instance, out_dir=out_dir)

    # The parent SkillFlow converter still writes a `.keep` placeholder
    # so the Dockerfile's `COPY skills ...` succeeds against an empty
    # tree; nothing else should live under skills/.
    skills_tree = out_dir / "skills"
    assert skills_tree.is_dir()
    subdirs = [p.name for p in skills_tree.iterdir() if p.is_dir()]
    assert subdirs == [], (
        f"online_mode=true must not bake seed skills into the bundle; "
        f"found: {subdirs}"
    )


def test_online_mode_offline_counterpart_ships_seed_dirs(
    staged_upstream: Path,
) -> None:
    """Twin of the previous test: with ``online_mode`` off (the
    matching offline row), the seed baseline IS materialised - guards
    against the flag flipping the wrong way."""
    offline = SkillLearnBenchB1OneShotClaudeHaiku45Adapter()
    online = SkillLearnBenchOnlineFromB1OneShotClaudeHaiku45Adapter()
    assert offline.online_mode is False
    assert online.online_mode is True

    inst_offline = next(iter(
        offline.list_instances(source_dir=staged_upstream, split="test"),
    ))
    offline_out = staged_upstream / "out-offline"
    offline.convert_instance(inst_offline, out_dir=offline_out)
    inst_online = next(iter(
        online.list_instances(source_dir=staged_upstream, split="test"),
    ))
    online_out = staged_upstream / "out-online-b1"
    online.convert_instance(inst_online, out_dir=online_out)

    offline_subs = {
        p.name for p in (offline_out / "skills").iterdir() if p.is_dir()
    }
    online_subs = {
        p.name for p in (online_out / "skills").iterdir() if p.is_dir()
    }
    assert offline_subs == set(
        _BASELINE_SKILL_DIRS["b1-one-shot-claude-haiku-4-5"],
    )
    assert online_subs == set()
