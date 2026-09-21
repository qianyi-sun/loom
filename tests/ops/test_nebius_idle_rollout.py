from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as rollout


def publication_api(path, payload=None):
    if path.endswith("/artifacts?per_page=100"):
        return {"artifacts": [{"name": "nebius-candidate-" + "a" * 40 + "-42-2", "expired": False}]}
    return {"conclusion": "success", "head_branch": "dev", "head_repository": {"full_name": rollout.REPOSITORY},
            "path": ".github/workflows/nebius-candidate.yml", "event": "push", "head_sha": "a" * 40, "run_attempt": 2}


def test_selects_exact_successful_attempt_not_workflow_default_sha(monkeypatch):
    monkeypatch.setattr(rollout, "github", publication_api)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0))
    result = rollout.select_publication("42")
    assert result == {"status": "ready", "sha": "a" * 40, "run_id": "42",
                      "artifact": "nebius-candidate-" + "a" * 40 + "-42-2"}


def test_harness_only_does_not_rollout(monkeypatch):
    def api(path, payload=None):
        if "/artifacts?" in path:
            return {"artifacts": [{"name": "nebius-agent-runtime-" + "a" * 40 + "-42-2", "expired": False}]}
        return publication_api(path)
    monkeypatch.setattr(rollout, "github", api)
    assert rollout.select_publication("42")["status"] == "skipped_no_platform_candidate"


@pytest.mark.parametrize("field,value", [("conclusion", "failure"), ("head_branch", "feature"),
                                          ("head_repository", {"full_name": "someone/fork"})])
def test_rejects_ineligible_publication(monkeypatch, field, value):
    monkeypatch.setattr(rollout, "github", lambda *a, **kw: {**publication_api("run"), field: value})
    with pytest.raises(rollout.DeploymentError, match="same-repository"):
        rollout.select_publication("42")


def test_older_candidate_is_not_deployed(monkeypatch):
    def command(argv, **kwargs):
        return SimpleNamespace(returncode=1 if "merge-base" in argv else 0)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    assert rollout.candidate_follows("b" * 40, "a" * 40) is False
