from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ADR = REPO_ROOT / "docs/architecture/adr/v1-workload-trust-contract.md"


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    return " ".join(text.split())


def test_v1_workload_trust_adr_records_the_only_accepted_tuple() -> None:
    text = ADR.read_text(encoding="utf-8")
    normalized = _normalized(text)

    for fragment in [
        'workload_trust_mode = "internal_trusted"',
        "taskset_transforms_enabled = false",
        "taskset_transform_network_isolated = false",
        "untrusted_workload_isolation = false",
        "transform_unavailable_in_internal_trusted",
        "before any transform blob, source blob, verifier blob, or subprocess is fetched or run",
        "raw invalid profile, manifest, or live env values",
        "A staging or production namespace is authoritative protected-target evidence.",
        "Manual rollout validates protected cluster and namespace identity before evidence or Kind work.",
        "#758 owns any later untrusted arbitrary-code execution design and implementation.",
    ]:
        assert fragment in normalized

    assert "[`src/loom/workload_trust.py`](../../../src/loom/workload_trust.py)" in text


def test_v1_workload_trust_docs_preserve_the_fail_closed_release_boundary() -> None:
    tasksets = _read("docs/architecture/user-brought-tasksets.md")
    sandbox = _read("docs/architecture/sandbox-isolation.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    first_prod_runbook = _read("docs/runbooks/first-prod-release-runbook.md")

    assert "transform_unavailable_in_internal_trusted" in tasksets
    assert "best-effort `os.unshare`" in tasksets
    assert "not untrusted-workload isolation" in sandbox
    assert "`--skip-preflight` does not bypass the contract" in _normalized(
        operator_runbook,
    )
    assert "workload-trust-contract" in operator_runbook
    assert "raw invalid profile, manifest, or live env values" in _normalized(
        operator_runbook,
    )
    assert "protected namespace is authoritative" in _normalized(operator_runbook)
    assert (
        "manual rollout validates protected cluster and namespace identity before evidence or kind work"
        in _normalized(operator_runbook).lower()
    )
    assert "workload profile" in first_prod_runbook
    assert "transform_unavailable_in_internal_trusted" in first_prod_runbook
    assert "protected namespace is authoritative" in _normalized(first_prod_runbook)
    assert (
        "manual rollout validates protected cluster and namespace identity before evidence or kind work"
        in _normalized(first_prod_runbook).lower()
    )


def test_v1_workload_trust_docs_are_discoverable_from_architecture_and_release_indexes() -> None:
    adr_index = _read("docs/architecture/adr/README.md")
    architecture_index = _read("docs/architecture/README.md")
    release_program = _read("docs/architecture/v1-release-ready-program.md")
    domain_model = _read("docs/agent/domain-model.md")
    docs_index = _read("docs/index.md")

    assert "v1-workload-trust-contract.md" in adr_index
    assert "v1-workload-trust-contract.md" in architecture_index
    assert "v1-workload-trust-contract.md" in release_program
    assert "Workload Trust Mode" in domain_model
    assert "domain-model.md" in docs_index
