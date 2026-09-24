"""Packaged Compose services must survive intake as explicit unmet requirements."""
from pathlib import Path

import pytest

from tests.loom_cli.test_local_compatibility_report import _report, _write_bundle


@pytest.mark.parametrize('name', ['docker-compose.yaml', 'docker-compose.yml', 'compose.yaml', 'compose.yml'])
def test_profile_blocks_packaged_compose_with_exact_source_without_editing_input(tmp_path, capsys, name):
    bundle = _write_bundle(tmp_path, 'service-fixture')
    compose = bundle / 'environment' / name
    compose.write_text('services:\n  main:\n    depends_on: [fixture]\n  fixture:\n    image: python:3.11-slim\n')
    before = {p.relative_to(bundle): p.read_bytes() for p in bundle.rglob('*') if p.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    (report,) = payload['compatibility_report']['tasks']
    assert report['status'] == 'blocked'
    (diagnostic,) = [d for d in report['diagnostics'] if d['code'] == 'compose_environment_unsupported']
    assert diagnostic['source_location'] == str(compose)
    assert diagnostic['category'] == 'unsupported_conversion'
    assert report['admission_passed'] is None
    assert {p.relative_to(bundle): p.read_bytes() for p in bundle.rglob('*') if p.is_file()} == before


def test_schema_report_does_not_claim_compose_runtime_support(tmp_path, capsys):
    bundle = _write_bundle(tmp_path, 'schema')
    (bundle / 'environment/docker-compose.yaml').write_text('services:\n  main:\n    environment: {MODE: fixture}\n')

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    assert payload['compatibility_report']['tasks'][0]['status'] == 'schema_valid'
    assert payload['compatibility_report']['runtime_verified'] is False


@pytest.mark.parametrize('compose', [
    'services:\n  fixture:\n    image: python:3.11-slim\n',
    'services:\n  main:\n    environment: {MODE: fixture}\n',
])
def test_ordinary_adapter_rejects_compose_before_generating_any_derived_files(tmp_path: Path, compose: str):
    from loom.nebius_terminus_ingest import adapt_bundle_for_nebius_terminus
    from tests.unit.test_nebius_terminus_ingest import _harbor_shaped_config, _write_runtime_inputs

    _write_runtime_inputs(tmp_path)
    (tmp_path / 'environment/docker-compose.yaml').write_text(compose)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}

    with pytest.raises(ValueError, match='Compose'):
        adapt_bundle_for_nebius_terminus(tmp_path, _harbor_shaped_config())

    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before
