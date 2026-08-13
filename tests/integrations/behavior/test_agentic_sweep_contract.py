from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import (
    Draft202012Validator,
)
from jsonschema import (
    ValidationError as JsonSchemaValidationError,
)
from pydantic import ValidationError

from loom.integrations.behavior.canonical_json import canonical_document, digest_bytes
from loom.integrations.behavior.errors import BehaviorContractError
from loom.integrations.behavior.offline_judge_assets import (
    ASSET_FILES,
    CODEX_ENV,
    CONFIG_TOML_LINES,
    INITIAL_ARGV,
    PROVIDER_ROOT,
    RESUME_ARGV,
    BehaviorOfflineRunnerLockV1,
    ProviderAssetBundle,
    bundled_provider_asset_root,
    compose_sweep_prompt,
    resume_prompt,
)

ROOT = Path(__file__).resolve().parents[3]
ASSET_ROOT = bundled_provider_asset_root()
SESSION_ID = UUID("018f65d5-53c2-7d80-b4a8-2b67db937c8a")
ATTEMPT_ID = UUID("019fefc0-8cd8-7bb4-a3b6-e4b5631113a2")


def _copy_assets(tmp_path: Path) -> Path:
    destination = tmp_path / "assets"
    shutil.copytree(ASSET_ROOT, destination)
    return destination


def test_bundled_assets_are_exact_canonical_manifest_inventory() -> None:
    bundle = ProviderAssetBundle.load(ASSET_ROOT)
    assert [item.relative_path for item in bundle.manifest.files] == list(ASSET_FILES)
    assert set(bundle.bytes_by_path) == {*ASSET_FILES, "manifest.json"}
    assert bundle.manifest_sha256 == digest_bytes(bundle.bytes_by_path["manifest.json"])
    assert bundle.manifest_sha256 == (
        "sha256:c09e1d6ac9a7299f354fd47951906722355dfb78fd78c8aed6d7c037494dfd3b"
    )
    assert bundle.bytes_by_path["manifest.json"] == canonical_document(bundle.manifest)
    assert bundle.bytes_by_path["runner-lock.json"] == canonical_document(bundle.runner_lock)
    assert bundle.bytes_by_path["mcp-lock.json"] == canonical_document(bundle.mcp_locks)


def test_assets_reject_authority_drift_extra_files_symlinks_and_changed_bytes(
    tmp_path: Path,
) -> None:
    with pytest.raises(BehaviorContractError, match="digest disagrees"):
        ProviderAssetBundle.load(ASSET_ROOT, expected_manifest_sha256="sha256:" + "0" * 64)

    changed = _copy_assets(tmp_path / "changed")
    (changed / "system.md").write_bytes((changed / "system.md").read_bytes() + b"drift\n")
    with pytest.raises(BehaviorContractError, match="disagree with manifest"):
        ProviderAssetBundle.load(changed)

    extra = _copy_assets(tmp_path / "extra")
    (extra / "build_task_card.md").write_text("forbidden", encoding="utf-8")
    with pytest.raises(BehaviorContractError, match="extra or missing"):
        ProviderAssetBundle.load(extra)

    linked = _copy_assets(tmp_path / "linked")
    (linked / "system.md").unlink()
    os.symlink(linked / "looking.md", linked / "system.md")
    with pytest.raises(BehaviorContractError, match=r"symlink|non-regular"):
        ProviderAssetBundle.load(linked)


def test_historical_assets_are_selectively_ported_with_current_provenance() -> None:
    bundle = ProviderAssetBundle.load(ASSET_ROOT)
    derived = [
        "system.md",
        "looking.md",
        "skill_vocabulary.md",
        "inspect_rollout.md",
        "tools/mosaic.py",
        "validate_outputs.py",
    ]
    for name in derived:
        text = bundle.bytes_by_path[name].decode("utf-8")
        assert "Copyright (c) 2023 Stanford Vision and Learning Group" in text
        assert "MIT" in text
    inspect = bundle.bytes_by_path["inspect_rollout.md"].decode("utf-8")
    assert "{step_idx,arm,old_value,new_value}" in inspect
    assert "{step_idx, arm, object}" not in inspect
    aggregate = b"\n".join(bundle.bytes_by_path.values())
    for forbidden in (
        b"build_task_card.md",
        b"video_only_prompts",
        b"AGENTIC_SWEEP_",
        b"B1K_DATA_ROOT",
    ):
        assert forbidden not in aggregate


def test_runner_lock_is_closed_and_renders_exact_config_and_argv() -> None:
    lock = ProviderAssetBundle.load(ASSET_ROOT).runner_lock
    assert lock.codex.version == "0.146.0"
    assert lock.codex.binary_path == "/opt/behavior/codex/bin/codex"
    assert lock.codex.install_script is None
    assert tuple(lock.argv.initial) == INITIAL_ARGV
    assert tuple(lock.argv.resume) == RESUME_ARGV
    assert not {"--ignore-user-config", "--last", "--all", "--ephemeral", "--profile", "-c"}.intersection(
        (*lock.argv.initial, *lock.argv.resume)
    )
    assert lock.initial_argv() == INITIAL_ARGV
    assert lock.resume_argv(SESSION_ID) == tuple(
        str(SESSION_ID) if item == "<session_id>" else item for item in RESUME_ARGV
    )
    rendered = lock.render_config_toml(task_id=7, shim_port=4444)
    expected = (
        "\n".join(CONFIG_TOML_LINES)
        .replace("<shim_port>", "4444")
        .replace("<task_id:04d>", "0007")
        + "\n"
    ).encode()
    assert rendered == expected
    assert hashlib.sha256(rendered).hexdigest() == (
        "e41a7d08575016dfd48dc9acdbd5192fdea03ba359b7ae8564cf3ff5f44d23a8"
    )


def test_runner_process_environments_and_substitutions_are_separated() -> None:
    lock = ProviderAssetBundle.load(ASSET_ROOT).runner_lock
    assert dict(lock.codex_env) == CODEX_ENV
    assert not any(name.startswith("LOOM_") for name in lock.codex_env)
    shim = lock.shim_env("https://gateway.example.test/v1/responses")
    assert dict(shim) == {
        "LOOM_STEP_JWT_FILE": "/run/loom/step-jwt",
        "LOOM_GATEWAY_RESPONSES_URL": "https://gateway.example.test/v1/responses",
    }
    assert not set(shim).intersection(lock.codex_env)
    assert lock.shim_argv(attempt_id=ATTEMPT_ID, shim_port=4444) == (
        "/opt/behavior/bin/loom-codex-gateway-shim",
        "--listen",
        "127.0.0.1:4444",
        "--jwt-file",
        "/run/loom/step-jwt",
        "--attempt-id",
        str(ATTEMPT_ID),
        "--provider-logical-name",
        "behavior_offline_judge",
    )
    assert lock.event_protocol.required_session_count == 1
    assert lock.event_protocol.max_resume_count == 1
    assert lock.cleanup.term_grace_seconds == 30
    assert lock.mcp_server_locks_sha256 == (
        "sha256:a755a5169b03e039c9d8e221b466b3692046d667bb0a1e3200fe37ad76f2694c"
    )


@pytest.mark.parametrize(
    ("operation", "match"),
    [
        (lambda lock: lock.render_config_toml(task_id=True, shim_port=4444), "strict uint32"),
        (lambda lock: lock.render_config_toml(task_id=7, shim_port=1023), "1024..65535"),
        (lambda lock: lock.resume_argv(str(SESSION_ID).upper()), "canonical lowercase"),
        (lambda lock: lock.shim_env("http://gateway/v1/responses"), "HTTPS"),
        (lambda lock: lock.shim_env("https://gateway/v1/responses?q=x"), "exact"),
    ],
)
def test_runner_rejects_noncanonical_substitutions(
    operation: Callable[[BehaviorOfflineRunnerLockV1], object], match: str
) -> None:
    lock = ProviderAssetBundle.load(ASSET_ROOT).runner_lock
    with pytest.raises(BehaviorContractError, match=match):
        operation(lock)


def test_runner_schema_rejects_every_lock_drift() -> None:
    value = ProviderAssetBundle.load(ASSET_ROOT).runner_lock.model_dump(mode="json")
    cases: list[dict[str, object]] = []
    extra = deepcopy(value)
    extra["raw_api_key"] = "forbidden"
    cases.append(extra)
    wrong_argv = deepcopy(value)
    wrong_argv["argv"]["initial"].insert(2, "--ignore-user-config")
    cases.append(wrong_argv)
    wrong_config = deepcopy(value)
    wrong_config["config_toml_lines"] = list(reversed(CONFIG_TOML_LINES))
    cases.append(wrong_config)
    extra_env = deepcopy(value)
    extra_env["env"]["LOOM_STEP_JWT_FILE"] = "/run/loom/step-jwt"
    cases.append(extra_env)
    third_mcp = deepcopy(value)
    third_mcp["mcp_servers"].append(deepcopy(third_mcp["mcp_servers"][0]))
    cases.append(third_mcp)
    for changed in cases:
        with pytest.raises(ValidationError):
            BehaviorOfflineRunnerLockV1.model_validate(changed)


def test_prompt_composition_has_exact_order_and_closed_run_parameters() -> None:
    bundle = ProviderAssetBundle.load(ASSET_ROOT)
    task_card = b"# Signed task card\n\nUse the washer without changing the declared order.\n"
    prompt = compose_sweep_prompt(bundle, task_card, 7, 70_371)
    separator = b"\n---\n\n"
    static = separator.join(
        bundle.bytes_by_path[name]
        for name in ("system.md", "looking.md", "skill_vocabulary.md", "inspect_rollout.md")
    )
    expected_tail = """\n---\n\n# Run parameters

Work in `/outputs/judge`. Write tool outputs and cache only below `/scratch`; write the two
declared final outputs only to `/outputs/judge`.

Signed read-only inputs:
- Immutable task card: `/inputs/dataset/payload/agentic_sweep/task_cards/task-0007.md`
- BDDL transitions: `/inputs/rollout/payload/meta/episodes/task-0007/episode_00070371_bddl_transitions.json`
- Rollout video root: `/inputs/rollout/payload/videos/task-0007`
- Demo video root: `/inputs/dataset/payload/videos/task-0007`

Write exactly `/outputs/judge/report.md` and `/outputs/judge/seed.json`. Re-read and validate
both files before finishing.

---

# Task card — task-0007

# Signed task card

Use the washer without changing the declared order.
""".encode()
    assert prompt == static + expected_tail
    assert hashlib.sha256(prompt).hexdigest() == (
        "5207397386dd72e460b10d49314f99381916943c702eb8de72237c10759c2450"
    )
    run_block = prompt.split(b"# Run parameters", 1)[1]
    assert PROVIDER_ROOT.encode() not in run_block
    assert b"build_task_card" not in prompt and b"video_only" not in prompt


def test_prompt_and_resume_reject_or_avoid_ambient_authority() -> None:
    bundle = ProviderAssetBundle.load(ASSET_ROOT)
    with pytest.raises(BehaviorContractError, match="strict uint32"):
        compose_sweep_prompt(bundle, b"card\n", True, 1)
    with pytest.raises(BehaviorContractError, match="BOM"):
        compose_sweep_prompt(bundle, b"\xef\xbb\xbfcard\n", 1, 1)
    assert resume_prompt(report_missing=True, seed_missing=True) == (
        b"You ended without writing every output the task asked for. Do not re-investigate \xe2\x80\x94 "
        b"use what you already established. Write `report.md` `seed.json` now under "
        b"`/outputs/judge/`, in the formats the task specified.\n"
    )
    with pytest.raises(BehaviorContractError, match="at least one"):
        resume_prompt(report_missing=False, seed_missing=False)


def test_seed_schema_is_closed_and_rejects_legacy_identity_and_bool_uints() -> None:
    schema_bytes = ProviderAssetBundle.load(ASSET_ROOT).bytes_by_path["seed.schema.json"]
    schema = json.loads(schema_bytes)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    valid = {
        "chunks": [
            {
                "span": [0, 12],
                "learn": [0, 12],
                "seed": None,
                "reason": "The object was visibly clear of its support at frame twelve.",
            }
        ],
        "task_id": 7,
        "episode": 70_371,
        "n_steps": 13,
        "fps": 30,
        "rollout": "artifact:018f65d5-53c2-7d80-b4a8-2b67db937c8a",
    }
    validator.validate(valid)
    for key, value in (
        ("episode", "episode_00070371"),
        ("episode", None),
        ("n_steps", True),
        ("extra", "forbidden"),
        ("chunks", []),
    ):
        changed = deepcopy(valid)
        changed[key] = value
        with pytest.raises(JsonSchemaValidationError):
            validator.validate(changed)


def test_mosaic_entrypoint_has_only_direct_root_cli_and_no_ambient_resolution() -> None:
    source = (ASSET_ROOT / "tools/mosaic.py").read_text(encoding="utf-8")
    for required in ("--video-root", "--episode", "--frames", "--out", "--cache-dir"):
        assert source.count(f'add_argument("{required}"') == 1
    for forbidden in (
        "--rollout",
        "--demo-task",
        "paths.py",
        "AGENTIC_SWEEP_",
        "B1K_DATA_ROOT",
        "glob.glob",
        "os.environ",
    ):
        assert forbidden not in source
    help_result = subprocess.run(
        [sys.executable, str(ASSET_ROOT / "tools/mosaic.py"), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert help_result.returncode == 0
    assert "--video-root VIDEO_ROOT" in help_result.stdout
    assert "--rollout" not in help_result.stdout and "--demo-task" not in help_result.stdout


def test_provider_assets_are_in_wheel_package_data() -> None:
    import tomllib

    package_data = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["setuptools"][
        "package-data"
    ]
    assert package_data["loom.integrations.behavior"] == [
        "provider_assets/offline_judge/*.json",
        "provider_assets/offline_judge/*.md",
        "provider_assets/offline_judge/*.py",
        "provider_assets/offline_judge/tools/*.py",
    ]
