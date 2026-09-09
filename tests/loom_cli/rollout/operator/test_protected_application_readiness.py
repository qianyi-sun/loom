"""Candidate-serving readiness must precede protected apply journal success."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from tests.loom_cli.rollout.operator.test_protected_manifest_component import Runner, _authority
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan


def test_exact_manifest_cannot_hide_failed_replacement_behind_old_ready_pods(
    tmp_path: Path,
) -> None:
    plan = _published_plan(tmp_path)
    documents = list(yaml.safe_load_all(Path(plan.rendered_manifest_path).read_text()))
    deployments = {}
    for name in ("loom-control-plane", "loom-service", "loom-llm-gateway", "loom-web"):
        desired = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": "loom-staging"},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name}},
                    "spec": {"containers": [{"name": name, "image": f"{name}@sha256:{'a' * 64}"}]},
                },
            },
        }
        documents.append(desired)
        live = copy.deepcopy(desired)
        live["metadata"].update({"generation": 88, "uid": f"uid-{name}", "resourceVersion": "123"})
        live["status"] = {
            "observedGeneration": 88,
            "replicas": 2,
            "updatedReplicas": 2,
            "readyReplicas": 2,
            "availableReplicas": 2,
        }
        deployments[name] = live
    # Exact shape from failed rollout req-8c0940dfe9d54a88: two old ready
    # replicas, one updated replica stuck in its non-root credential init.
    deployments["loom-control-plane"]["status"].update(
        {
            "replicas": 3,
            "updatedReplicas": 1,
            "unavailableReplicas": 1,
            "conditions": [
                {"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}
            ],
        }
    )
    rendered = yaml.safe_dump_all(documents).encode()
    manifest = Path(plan.rendered_manifest_path)
    manifest.write_bytes(rendered)
    manifest.chmod(0o600)
    plan = replace(
        plan,
        rendered_manifest_path=str(manifest),
        rendered_manifest_sha256=hashlib.sha256(rendered).hexdigest(),
    )

    class ApplicationRunner(Runner):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            assert env == {"KUBECONFIG": "/exact"}
            assert 0 < timeout_seconds <= 30
            assert "get" in argv
            name = next((name for name in deployments if name in " ".join(argv)), None)
            result = deployments[name] if name else {"items": list(deployments.values())}
            return json.dumps(result).encode()

    runner = ApplicationRunner(status=0)
    with pytest.raises(RuntimeError, match="readiness"):
        _authority(runner).classify(plan)
    assert all("apply" not in argv for argv, _payload in runner.calls)
