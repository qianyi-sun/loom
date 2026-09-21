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


@pytest.mark.parametrize('event', ['pull_request', 'merge_group', 'push', 'workflow_dispatch'])
@pytest.mark.parametrize('failed', [None, 'build', 'harness', 'publish', 'manifest'])
def test_image_gate_requires_each_selected_supported_result(event, failed):
    jobs = yaml.safe_load((ROOT / '.github/workflows/images.yml').read_text())['jobs']
    step = jobs['images-gate']['steps'][0]
    trusted = event in {'push', 'workflow_dispatch'}
    results = {'build': 'skipped' if trusted else 'success',
               'harness': 'skipped' if trusted else 'success',
               'publish': 'success' if trusted else 'skipped',
               'manifest': 'success' if trusted else 'skipped'}
    if failed:
        results[failed] = 'failure'
    env = dict(os.environ, EVENT_NAME=event, TRUSTED_PUBLISH=str(event == 'workflow_dispatch').lower(),
               PLAN_RESULT='success', GATE_MODE='full', REQUIRED='true',
               BUILD_RESULT=results['build'], HARNESS_BUILD_RESULT=results['harness'],
               PUBLISH_RESULT=results['publish'], MANIFEST_RESULT=results['manifest'],
               STANDARD_IMAGES=json.dumps([{'image': 'service'}]))
    result = subprocess.run(['bash'], input=step['run'], text=True, env=env, capture_output=True)
    assert (result.returncode == 0) is (failed is None), result.stderr


@pytest.mark.parametrize('event,trusted', [('push', 'false'), ('workflow_dispatch', 'true'),
                                         ('workflow_dispatch', 'false'), ('pull_request', 'false')])
@pytest.mark.parametrize('manifest_result', ['success', 'failure', 'skipped', 'cancelled'])
def test_release_receipt_requires_successful_trusted_publication(event, trusted, manifest_result):
    from tests.ops.test_ci_images_parallel_builds import _condition

    jobs = yaml.safe_load((ROOT / '.github/workflows/images.yml').read_text())['jobs']
    record, upload = jobs['images-gate']['steps'][1:]
    assert record['if'] == upload['if']
    values = {'github.event_name': event, 'github.ref': 'refs/heads/dev',
              'needs.plan.outputs.required': 'true', 'needs.plan.outputs.trusted_publish': trusted,
              'needs.publish.result': 'success', 'needs.publish-manifest.result': manifest_result}
    assert _condition(record['if'], values) is (
        manifest_result == 'success' and (event == 'push' or (event == 'workflow_dispatch' and trusted == 'true')))
    assert 'github.run_id' in upload['with']['name']
    assert 'github.run_attempt' in upload['with']['name']
    assert upload['with']['if-no-files-found'] == 'error'
