from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import tomli_w
from scripts.ops.prepare_nebius_harbor90 import (
    BENCHMARK_ID,
    NATIVE_TASKS,
    arm_build_references,
    prepare,
    protect_assertions,
    x86_task_toml,
)
from tests.unit.test_service_execution_materialization import _profile

from loom.models.task_checksum import task_checksum
from loom.nebius_platform_render import DEFAULT_TASK_RESOURCE_REQUESTS


def test_architecture_edit_preserves_guest_arch_and_other_sections() -> None:
    body = '[task]\nid="arm64-emulator"\n[environment]\ncpu_arch = "arm64"\n# aarch64 guest is intentional\n[agent]\nname="terminus-2"\n'
    updated = x86_task_toml(body)
    assert updated == body.replace('cpu_arch = "arm64"', 'cpu_arch = "x86_64"')
    assert x86_task_toml(updated) == updated


def test_protects_nested_instructions_and_test_assertions(tmp_path: Path) -> None:
    original, overlay = tmp_path / 'original', tmp_path / 'overlay'
    (original / 'instructions').mkdir(parents=True)
    (original / 'tests').mkdir()
    (original / 'task.toml').write_text('[[steps]]\ninstruction_file="instructions/second.md"\n')
    (original / 'instructions/second.md').write_text('Keep this problem unchanged')
    (original / 'tests/test_outputs.py').write_text('assert result == 42')
    shutil.copytree(original, overlay)
    assert protect_assertions(original, overlay) == 2
    (overlay / 'instructions/second.md').write_text('Different problem')
    with pytest.raises(ValueError, match='protected instruction/test'):
        protect_assertions(original, overlay)


def test_arm_url_requires_review_but_comment_and_mips_guest_are_preserved(tmp_path: Path) -> None:
    (tmp_path / 'environment').mkdir()
    dockerfile = tmp_path / 'environment/Dockerfile'
    dockerfile.write_text('FROM ubuntu:24.04\n# amd64/arm64\nRUN apt-get install gcc-mips-linux-gnu\n')
    assert arm_build_references(tmp_path) == []
    dockerfile.write_text('FROM ubuntu:24.04\nRUN curl https://example.org/tool-aarch64.tar.gz\n')
    assert len(arm_build_references(tmp_path)) == 1


def test_prepare_keeps_90_identities_20_native_and_70_explicit_blockers(
    tmp_path: Path,
) -> None:
    profile = _profile().model_dump(mode='json')
    profile['agent_image_ref'] = profile['task_image_ref']
    profile['default_task_resource_requests'] = DEFAULT_TASK_RESOURCE_REQUESTS
    profile_path = tmp_path / 'profile.json'
    profile_path.write_text(json.dumps(profile))
    source, overlays = tmp_path / 'source', tmp_path / 'overlays'
    source.mkdir()
    overlays.mkdir()
    rows, approved = [], []
    for name in sorted(NATIVE_TASKS | {f'other-{i}' for i in range(70)}):
        directory = source / name
        for subdir in ('environment', 'tests', 'verifier'):
            (directory / subdir).mkdir(parents=True, exist_ok=True)
        config = {
            'schema_version': '1', 'task': {'id': name, 'name': name},
            'environment': {'os': 'linux', 'cpu_arch': 'arm64', 'dockerfile': 'environment/Dockerfile', 'workdir': '/app'},
            'agent': {'name': 'terminus-2'},
            'verifier': {'name': 'script', 'args': {'script_path': '/app/verifier/run.sh'}, 'user': 'root'},
            'steps': [{'name': 'main', 'instruction_file': 'instruction.md'}],
        }
        (directory / 'task.toml').write_text(tomli_w.dumps(config))
        (directory / 'instruction.md').write_text('Do the original task\n')
        (directory / 'tests/test_outputs.py').write_text('assert 1 == 1\n')
        (directory / 'verifier/run.sh').write_text('#!/bin/sh\nexit 0\n')
        (directory / 'verifier/run.sh').chmod(0o755)
        (directory / 'environment/Dockerfile').write_text('FROM ubuntu:24.04\n')
        rows.append({'id': BENCHMARK_ID+'/'+name, 'checksum': task_checksum(directory), 'license': 'Apache-2.0'})
        if name in NATIVE_TASKS:
            target = overlays / name
            shutil.copytree(directory, target)
            config['environment'].update(cpu_arch='x86_64', cpus=1, memory_mb=2048, storage_mb=10240,
                                         user='agent', baseline_network_policy={'kind':'gateway-only'},
                                         network_policies_supported=['gateway-only'])
            config['verifier'] = {'name':'script','env_mode':'shared','args':{'script_path':'verifier/run.sh'}}
            (target / 'task.toml').write_text(tomli_w.dumps(config))
            approved.append({'name':name,'current_task_checksum':task_checksum(target)})
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'benchmark':{'id':BENCHMARK_ID,'display_name':'Original 90','license_spdx':'Apache-2.0'},'tasks':rows}))
    metadata = tmp_path / 'overlays.json'
    metadata.write_text(json.dumps({'tasks':approved}))
    kwargs = dict(source_root=source,catalog_metadata=catalog,overlay_roots=[overlays],
                  overlay_metadata=metadata,profile_path=profile_path,output=tmp_path/'prepared')
    report = prepare(**kwargs)
    assert report['task_count'] == 90
    assert report['native_admission_compatible_count'] == 20
    assert len([r for r in report['tasks'] if r['native_admission_blockers']]) == 70
    assert all(r['cpu_arch']=='x86_64' and r['task_id'].startswith(BENCHMARK_ID+'/') for r in report['tasks'])
    assert all(r['source_relative_files_and_modes_preserved'] for r in report['tasks'])
    for record in report['tasks']:
        if record['native_overlay']:
            assert record['prepared_checksum'] == task_checksum(overlays / record['name'])
    with pytest.raises(ValueError, match='output already exists'):
        prepare(**kwargs)
    bad_dockerfile = source / 'other-0/environment/Dockerfile'
    bad_dockerfile.write_text('FROM ubuntu:24.04\nRUN apt-get update && apt-get install -y curl || true\n')
    rows_by_id = {row['id']: row for row in rows}
    rows_by_id[BENCHMARK_ID+'/other-0']['checksum'] = task_checksum(source / 'other-0')
    catalog.write_text(json.dumps({'benchmark':{'id':BENCHMARK_ID,'display_name':'Original 90','license_spdx':'Apache-2.0'},'tasks':rows}))
    kwargs['output'] = tmp_path/'incompatible'
    with pytest.raises(ValueError, match='publish compatibility failed'):
        prepare(**kwargs)
    assert not kwargs['output'].exists()
    changed = overlays / sorted(NATIVE_TASKS)[0] / 'instruction.md'
    changed.write_text('Changed task')
    kwargs['output'] = tmp_path/'second'
    with pytest.raises(ValueError, match='changed since'):
        prepare(**kwargs)
    assert not kwargs['output'].exists()
