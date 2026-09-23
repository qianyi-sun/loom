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


def test_busy_cli_explains_activity_without_exposing_raw_evidence(monkeypatch, tmp_path, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("sys.argv", [
        "rollout", "run", "--publication-dir", str(tmp_path), "--candidate", "a" * 40,
        "--kubeconfig", "unused", "--expected-cluster-id", "unused",
        "--evidence-dir", str(tmp_path),
    ])
    monkeypatch.setattr(rollout, "rollout", lambda args: {
        "status": "skipped_busy", "candidate_sha": "a" * 40,
        "guard": {"active": {"trials": 1, "executions": 1, "builds": 0, "build_cleanup": 0}},
        "private_diagnostic": "must-not-be-published",
    })
    assert rollout.main() == 0
    body = summary.read_text()
    log = capsys.readouterr().out
    assert "1 claimed/running trial(s)" in body
    assert "1 claimed/running trial(s)" in log
    assert "No deployment was applied" in body
    assert "| Image builds awaiting cleanup | 0 |" in body
    assert "must-not-be-published" not in body + log
