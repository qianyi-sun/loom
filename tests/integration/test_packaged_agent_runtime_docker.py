"""Build the retained task stage without a foreign agent's unavailable cache."""
from uuid import uuid4

import pytest

from loom.nebius_terminus_image import _without_packaged_openhands_runtime

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def test_known_foreign_agent_packaging_keeps_task_image_behavior(tmp_path):
    import docker

    marker = uuid4().hex
    task_stage = (
        'FROM python:3.11-slim\n'
        f'RUN mkdir /task-input && printf {marker} > /task-input/marker\n'
        'WORKDIR /task-input\n'
        'CMD ["cat", "/task-input/marker"]\n'
    )
    original = (
        'FROM terminalworld-openhands-sdk-cache:1.34.0-py312-musl-v3 AS terminalworld_openhands_runtime_cache\n'
        + task_stage + ''.join(
            f'COPY --from=terminalworld_openhands_runtime_cache {path} {path}\n'
            for path in ('/opt/openhands-python', '/opt/openhands-sdk-venv', '/opt/openhands-musl-loader')
        )
    )
    (tmp_path / 'Dockerfile').write_text(_without_packaged_openhands_runtime(original))
    client = docker.from_env()
    image = None
    try:
        image, _ = client.images.build(path=str(tmp_path), network_mode='none', pull=False, rm=True)
        output = client.containers.run(image.id, remove=True, network_mode='none', cap_drop=['ALL'],
                                       security_opt=['no-new-privileges'])
        assert output == marker.encode()
        assert image.attrs['Config']['WorkingDir'] == '/task-input'
        assert image.attrs['Config']['Cmd'] == ['cat', '/task-input/marker']
    finally:
        if image is not None:
            client.images.remove(image.id)
        client.close()
