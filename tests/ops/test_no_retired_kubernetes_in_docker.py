from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEXT_SUFFIXES = {".md", ".py", ".sh", ".toml", ".yaml", ".yml", ".service", ".txt"}


def _tracked_text_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        ROOT / relative
        for relative in result.stdout.splitlines()
        if Path(relative).suffix in TEXT_SUFFIXES
        and relative != "tests/ops/test_no_retired_kubernetes_in_docker.py"
    ]


def test_retired_kubernetes_in_docker_surface_is_absent() -> None:
    retired_name = "ki" + "nd"
    forbidden_paths = (
        ".github/workflows/cluster-deploy-spikes.yml",
        "deploy/k8s/ingress-nginx-" + retired_name + ".yaml",
        "src/loom_cli/cluster_load_images.py",
        "src/loom_cli/rollout/steps/s03_" + retired_name + "_cluster.py",
        "src/loom_cli/rollout/steps/s03_" + retired_name + "_load_images.py",
    )
    assert all(not (ROOT / path).exists() for path in forbidden_paths)

    forbidden_fragments = (
        retired_name + " cluster",
        retired_name + "-cluster",
        retired_name + " load",
        retired_name + "-load",
        retired_name + " create",
        retired_name + " delete",
        retired_name + " get clusters",
        retired_name + "est/node",
        retired_name + "-loom",
        retired_name + "-to-k3s",
        retired_name + "/containerd",
        "cluster-deploy-spikes",
        "ingress-nginx-" + retired_name,
    )
    findings: list[str] = []
    for path in _tracked_text_files():
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace").lower()
        for fragment in forbidden_fragments:
            if fragment in text:
                findings.append(f"{path.relative_to(ROOT)}: {fragment}")
    assert findings == []
