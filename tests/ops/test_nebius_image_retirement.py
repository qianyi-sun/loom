"""Supported image publication has no retired fleet prerequisites."""
import json
import os
import subprocess
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[2]


def test_retired_fleet_images_have_no_build_or_release_jobs():
    workflow = (ROOT / '.github/workflows/images.yml').read_text()
    for name in ('personal-dev-', 'scanner-cache-build', 'capacity-executor'):
        assert name not in workflow
    manifest = tomllib.loads((ROOT / 'config/component-ownership.toml').read_text())
    for component in manifest['components']:
        assert not component['id'].startswith(('personal-dev-', 'capacity-'))


@pytest.mark.parametrize('event', ['pull_request', 'merge_group', 'workflow_dispatch'])
@pytest.mark.parametrize('failed', [None, 'build', 'harness'])
def test_image_gate_requires_each_selected_supported_result(event, failed):
    jobs = yaml.safe_load((ROOT / '.github/workflows/images.yml').read_text())['jobs']
    step = jobs['images-gate']['steps'][0]
    results = {'build': 'success', 'harness': 'success'}
    if failed:
        results[failed] = 'failure'
    env = dict(os.environ, EVENT_NAME=event,
               PLAN_RESULT='success', GATE_MODE='full', REQUIRED='true', HARBOR_REQUIRED='true',
               BUILD_RESULT=results['build'], HARNESS_BUILD_RESULT=results['harness'],
               STANDARD_IMAGES=json.dumps([{'image': 'service'}]))
    result = subprocess.run(['bash'], input=step['run'], text=True, env=env, capture_output=True)
    assert (result.returncode == 0) is (failed is None), result.stderr
