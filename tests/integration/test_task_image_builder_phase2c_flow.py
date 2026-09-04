from __future__ import annotations

import subprocess
from pathlib import Path


def test_go_supervisor_local_socket_flow_passes_with_redacted_boundaries() -> None:
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{repo}:/src:ro",
            "-w",
            "/src",
            "golang:1.23.4-bookworm",
            "go",
            "test",
            "./cmd/loom-task-image-builder-supervisor",
            "-run",
            "TestSupervisorCrossLanguageLocalSocketFlow",
            "-count=1",
            "-timeout=10s",
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert result.returncode == 0, result.stdout


def test_production_main_installs_disabled_publication_handoff_only() -> None:
    repo = Path(__file__).resolve().parents[2]
    main_source = (repo / "cmd/loom-task-image-builder-supervisor/main.go").read_text()

    assert "DisabledPublicationHandoff{}" in main_source
    assert "AcceptingPublicationHandoff" not in main_source
