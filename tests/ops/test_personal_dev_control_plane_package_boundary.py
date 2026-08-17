from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_manifest import _config as _builder_config

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import personal_dev_preparation_manifest_documents
from loom.personal_dev_builder_manifest import personal_dev_builder_manifest_documents

_ROOT = Path(__file__).resolve().parents[2]


def _read(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def test_shadow_package_is_pure_render_only_and_has_no_legacy_extension() -> None:
    source = (_ROOT / "src/loom/personal_dev_control_plane_render.py").read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "import kubernetes" not in source
    assert "slurm" not in source.casefold()
    assert "def apply" not in source
    assert "def activate" not in source
    assert "loom-dev-shared" not in source
    assert "0.0.0.0/0" not in source


def test_dynamic_personal_and_builder_namespaces_bind_read_authority_locally() -> None:
    personal = personal_dev_preparation_manifest_documents(
        derive_identity("alice"),
        _immutable_config(),
    )
    builder = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=_builder_config(),
    )

    for documents in (personal, builder):
        binding = next(
            item
            for item in documents
            if item["kind"] == "RoleBinding"
            and item["metadata"]["name"] == "loom-personal-dev-management"
        )
        assert binding["roleRef"]["name"] == "loom-personal-dev-managed-namespace"
        assert binding["subjects"] == [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ]


def test_dev_fleet_package_never_creates_a_second_shared_namespace() -> None:
    for path in (_ROOT / "deploy/dev-fleet").glob("**/*"):
        if path.is_file():
            assert "loom-dev-shared" not in path.read_text(encoding="utf-8", errors="ignore")


def test_personal_management_shadow_runbook_has_exact_bounded_rehearsal() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    normalized = " ".join(runbook.split())

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "management-shadow/<approved-window-id>" in runbook
    assert 'test ! -e "$shadow_render"' in runbook
    assert 'test ! -e "$render_evidence"' in runbook
    assert 'install -d -m 0700 "$evidence_dir"' in runbook
    assert 'test "$(stat -c %u "$trusted_release")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %a "$trusted_release")" = 600' in runbook
    assert 'test "$(stat -c %h "$trusted_release")" = 1' in runbook
    assert 'test "$(stat -c %u "$kubeconfig")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %a "$kubeconfig")" = 600' in runbook
    assert 'test "$(stat -c %h "$kubeconfig")" = 1' in runbook
    assert 'test -z "$(git status --porcelain=v1 --untracked-files=all)"' in runbook
    assert 'repository_source_sha="$(git rev-parse HEAD)"' in runbook
    assert 'repository_source_tree="$(git rev-parse HEAD^{tree})"' in runbook
    assert "export PYTHONPATH=src:." in runbook
    assert '$(jq -r .source_sha "$trusted_release")' in runbook
    assert '$(jq -r .source_tree "$trusted_release")' in runbook
    assert 'test "$(stat -c %u "$profile")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %h "$profile")" = 1' in runbook
    assert "loom admin personal-dev-control-plane render" in normalized
    assert '--file "$profile"' in normalized
    assert '--trusted-release-file "$trusted_release"' in normalized
    assert '--trusted-release-sha256 "$trusted_release_sha256"' in normalized
    assert 'sha256sum "$shadow_render"' in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert "diff_status=0" in runbook
    assert "|| diff_status=$?" in runbook
    assert 'test "$diff_status" -eq 0 || test "$diff_status" -eq 1' in runbook
    assert '"$evidence_dir/server-side-diff.txt"' in runbook
    assert "--field-manager=loom-personal-dev-control-plane" in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "issue #1280 shadow window" in runbook.casefold()
    assert "rollout status statefulset/loom-dev-postgres" in normalized
    assert "rollout status statefulset/loom-dev-minio" in normalized
    assert "--for=condition=complete" in normalized
    assert "rollout status deployment/loom-personal-dev-management" in normalized
    assert "loom admin personal-dev-control-plane status" in normalized
    assert '"schema":"loom-personal-dev-control-plane-status-v1"' in runbook
    assert '"manager_ceiling":0' in runbook
    assert '"ready":true' in runbook
    assert 'previous_shadow_render="$evidence_dir/previous-reviewed-shadow.yaml"' in runbook
    assert "previous_shadow_sha256='<previous-reviewed-64-lowercase-hex>'" in runbook
    assert (
        'test "$(sha256sum "$previous_shadow_render" | awk \'{print $1}\')" = '
        '"$previous_shadow_sha256"'
    ) in normalized
    assert "reapply the previous reviewed shadow" in runbook.casefold()
    assert "stop if any `loom-dev-<owner>` namespace exists" in runbook.casefold()
    assert "stop if any `loom-build-*` namespace exists" in runbook.casefold()
    assert "schema-compatible" in runbook.casefold()
    assert 'rollback_status_evidence="$evidence_dir/rollback-shadow.status.json"' in runbook
    assert '--file "$previous_profile"' in normalized
    assert '--trusted-release-file "$previous_trusted_release"' in normalized
    assert '--trusted-release-sha256 "$previous_trusted_release_sha256"' in normalized
    assert 'cmp -s "$previous_shadow_render_tmp" "$previous_shadow_render"' in runbook
    assert "yaml.safe_load_all" in runbook
    assert "render_identity_set" in runbook
    assert "rollback-current-identities" in runbook
    assert "rollback-previous-identities" in runbook
    assert 'kind == "Job" and migration_name.fullmatch(name)' in runbook
    assert "live_identity_set" in runbook
    assert 'test ! -s "$live_identities"' in runbook
    assert 'cmp -s "$live_identities" "$previous_identities"' in runbook
    assert 'cmp -s "$current_identities_tmp" "$previous_identities_tmp"' in runbook
    assert "rollback_diff_status=0" in runbook
    assert "|| rollback_diff_status=$?" in runbook
    assert 'test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1' in runbook
    assert 'sha256sum "$rollback_status_evidence"' in runbook


def test_personal_management_shadow_runbook_preserves_authority_boundaries() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    lowered = runbook.casefold()

    assert "loom-dev-shared" not in runbook
    assert "kubectl create secret" not in lowered
    assert "--from-literal" not in lowered
    assert "loom_svc_dev_instances_enabled=true" not in lowered
    assert "loom_svc_personal_dev_builder_enabled=true" not in lowered
    assert "kubectl scale" not in lowered
    assert "kubectl delete pvc" not in lowered
    assert "loom service up" not in lowered
    assert "loom admin capacity-control-plane render" not in lowered
    assert "systemctl" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered
    assert "approved secret channel" in lowered
    assert "never delete pvcs" in lowered
    assert "physical capacity unchanged" in lowered
    assert "flattened and self-contained" in lowered


def test_personal_management_shadow_runbook_rechecks_interlocks_at_each_apply() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")

    forward_diff = runbook.index('test "$diff_status" -eq 0 || test "$diff_status" -eq 1')
    forward_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        forward_diff,
    )
    forward_interlock = runbook[forward_diff:forward_apply]
    rollback_diff = runbook.index(
        'test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1'
    )
    rollback_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        rollback_diff,
    )
    rollback_interlock = runbook[rollback_diff:rollback_apply]

    for interlock in (forward_interlock, rollback_interlock):
        assert "assert_reviewed_kubeconfig" in interlock
        assert "assert_forward_identity_contract" in interlock
        assert "assert_no_dynamic_namespaces" in interlock
        assert "assert_zero_capacity" in interlock
    assert "assert_current_shadow_artifacts" in forward_interlock
    assert "assert_previous_shadow_artifacts" in rollback_interlock


def test_personal_management_shadow_embedded_identity_tools_execute(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    programs = re.findall(r"<<'PY'\n(.*?)\nPY\n", runbook, flags=re.DOTALL)

    assert len(programs) == 2
    for program in programs:
        compile(program, "personal-dev-management-plane-shadow.md", "exec")

    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """\
apiVersion: v1
kind: Namespace
metadata:
  name: loom-dev
---
apiVersion: batch/v1
kind: Job
metadata:
  name: loom-personal-dev-migrate-1111111111111111-2222222222222222
  namespace: loom-dev
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-personal-dev-management
  namespace: loom-dev
""",
        encoding="utf-8",
    )
    rendered = subprocess.run(
        [sys.executable, "-", str(manifest)],
        input=programs[0],
        text=True,
        capture_output=True,
        check=True,
    )
    assert rendered.stdout.splitlines() == [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-management"]'
    ]

    managed_labels = {"app.kubernetes.io/managed-by": "loom-personal-dev-control-plane"}
    namespaced = tmp_path / "namespaced.json"
    namespaced.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "loom-personal-dev-management",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "metadata": {
                            "name": ("loom-personal-dev-migrate-1111111111111111-2222222222222222"),
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "PersistentVolumeClaim",
                        "metadata": {
                            "name": "data-loom-dev-postgres-0",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "derived-pod",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                            "ownerReferences": [{"controller": True, "kind": "ReplicaSet"}],
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "unexpected-top-level-pod",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    cluster = tmp_path / "cluster.json"
    cluster.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    {
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "ClusterRole",
                        "metadata": {
                            "name": "loom-personal-dev-management-mutation",
                            "labels": managed_labels,
                        },
                    }
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    live = subprocess.run(
        [sys.executable, "-", str(namespaced), str(cluster)],
        input=programs[1],
        text=True,
        capture_output=True,
        check=True,
    )
    assert live.stdout.splitlines() == [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-management"]',
        '["rbac.authorization.k8s.io/v1","ClusterRole","","loom-personal-dev-management-mutation"]',
        '["v1","Pod","loom-dev","unexpected-top-level-pod"]',
    ]


def test_dev_fleet_readme_coordinates_both_shadow_readiness_gates() -> None:
    readme = _read("deploy/dev-fleet/README.md")
    normalized = " ".join(readme.split())

    assert "personal-dev-management-plane-shadow.md" in readme
    assert "executable-global-capacity-bridge-rehearsal.md" in readme
    assert (
        "Both the personal-management shadow and the global-capacity zero-ceiling "
        "shadow must report ready before the later acceptance interlock"
    ) in normalized


def test_personal_shadow_runbook_is_indexed_and_architecture_linked() -> None:
    runbook_index = _read("docs/runbooks/README.md")
    architecture = _read("docs/architecture/multi-dev-environments.md")

    assert "personal-dev-management-plane-shadow.md" in runbook_index
    assert "personal-dev-management-plane-shadow.md" in architecture
    assert "personal-dev-management-plane-deployment.md" in architecture
