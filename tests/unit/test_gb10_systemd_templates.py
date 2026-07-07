from __future__ import annotations

from pathlib import Path


def test_gb10_node_agent_service_exposes_qianyi_staging_paths() -> None:
    service = Path("deploy/worker-pools/gb10/loom-gb10-node-agent.service")
    text = service.read_text(encoding="utf-8")

    assert (
        "Environment=PATH=/home/qianyi/.local/bin:/usr/local/bin:/usr/bin:/bin"
        in text
    )
    assert "WorkingDirectory=/home/qianyi/loom-worker-build-staging" in text
    assert "--env-file /home/qianyi/loom-worker-build-staging/.env" in text
    assert "ExecStart=/usr/bin/env uv run loom worker gb10-agent apply" in text
