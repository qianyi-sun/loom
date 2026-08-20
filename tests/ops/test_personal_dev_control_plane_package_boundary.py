from __future__ import annotations

import json
import re
import runpy
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_manifest import _config as _builder_config

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import personal_dev_preparation_manifest_documents
from loom.personal_dev_builder_manifest import personal_dev_builder_manifest_documents

_ROOT = Path(__file__).resolve().parents[2]


def _read(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def _sidecar_status(**changes: str) -> bytes:
    fields = {
        "Uid": "1000\t1000\t1000\t1000",
        "Gid": "1000\t1000\t1000\t1000",
        "CapEff": "0000000000000000",
        "CapBnd": "00000000000000c0",
        "NoNewPrivs": "0",
        "Seccomp": "0",
    }
    fields.update(changes)
    return "".join(f"{name}:\t{value}\n" for name, value in fields.items()).encode(
        "ascii"
    )


def _sidecar_launcher() -> dict[str, object]:
    return runpy.run_path(
        str(_ROOT / "deploy/personal-dev-builder/loom-personal-dev-buildkitd"),
        run_name="loom_personal_dev_buildkitd_test",
    )


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


def test_personal_dev_builder_image_binds_rootless_sidecar_prerequisites() -> None:
    dockerfile = _read("deploy/Dockerfile.personal-dev-builder")
    ownership = _read("config/component-ownership.toml")

    assert "rootlesskit version 3.0.1" in dockerfile
    assert (
        "79e43c95bb160488b6cb839da16750f7c590fb307b9c2e2d0421dd73fdc557cc"
        in dockerfile
    )
    assert (
        "27dfdece833e7ababf64ac5ac37b55b631d614e51e23d2f3505b2881f22c1fce"
        in dockerfile
    )
    assert "ARG TARGETARCH" in dockerfile
    assert "/usr/bin/buildctl" in dockerfile
    assert "/bin/setpriv" in dockerfile
    assert "/usr/bin/newuidmap" in dockerfile
    assert "0100000280000000000000000000000000000000" in dockerfile
    assert "/usr/bin/newgidmap" in dockerfile
    assert "0100000240000000000000000000000000000000" in dockerfile
    assert (
        "COPY --chown=0:0 --chmod=0555 "
        "deploy/personal-dev-builder/loom-personal-dev-buildkitd" in dockerfile
    )
    assert '"deploy/personal-dev-builder/loom-personal-dev-buildkitd"' in ownership


def test_rootless_sidecar_launcher_execs_only_the_fixed_buildkit_command(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    recorded: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], environment: dict[str, str]) -> None:
        recorded.append((path, argv, environment))

    def accept_private_directory(path: Path, *, uid: int, gid: int) -> None:
        assert path in {state / "home", tmp_path / "runtime"}
        assert (uid, gid) == (1000, 1000)

    launcher["_main"].__globals__["_ensure_private_directory"] = (
        accept_private_directory
    )
    with pytest.raises(RuntimeError, match="returned"):
        launcher["_main"](
            gvisor_marker=marker,
            status_file=status,
            uid=1000,
            gid=1000,
            home=state / "home",
            runtime_directory=tmp_path / "runtime",
            execve=execve,
        )

    assert recorded == [
        (
            "/usr/bin/rootlesskit",
            [
                "/usr/bin/rootlesskit",
                "/bin/setpriv",
                "--nnp",
                "/usr/bin/buildkitd",
                "--addr=unix:///var/run/loom-buildkit/buildkitd.sock",
                "--oci-worker-no-process-sandbox",
                "--oci-worker-snapshotter=native",
            ],
            {
                "HOME": str(state / "home"),
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TMPDIR": "/tmp",
                "USER": "user",
                "XDG_RUNTIME_DIR": str(tmp_path / "runtime"),
            },
        )
    ]


@pytest.mark.parametrize(
    ("field", "value", "uid", "gid"),
    [
        ("Uid", "1001\t1001\t1001\t1001", 1000, 1000),
        ("Gid", "1001\t1001\t1001\t1001", 1000, 1000),
        ("CapEff", "00000000000000c0", 1000, 1000),
        ("CapBnd", "0000000000000000", 1000, 1000),
        ("NoNewPrivs", "1", 1000, 1000),
        ("Seccomp", "2", 1000, 1000),
        ("Uid", "1000\t1000\t1000\t1000", 1001, 1000),
        ("Gid", "1000\t1000\t1000\t1000", 1000, 1001),
    ],
)
def test_rootless_sidecar_launcher_rejects_each_identity_drift(
    tmp_path: Path,
    field: str,
    value: str,
    uid: int,
    gid: int,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status(**{field: value}))

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_verify_preflight"](
            gvisor_marker=marker,
            status_file=status,
            uid=uid,
            gid=gid,
        )


def test_rootless_sidecar_launcher_requires_gvisor_marker(tmp_path: Path) -> None:
    launcher = _sidecar_launcher()
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_verify_preflight"](
            gvisor_marker=tmp_path / "missing",
            status_file=status,
            uid=1000,
            gid=1000,
        )


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


def test_zero_capacity_acceptance_runbook_has_exact_two_owner_workflow() -> None:
    runbook = _read("docs/runbooks/personal-dev-zero-capacity-acceptance.md")
    normalized = " ".join(runbook.split())

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "personal-dev-trusted-release-run-<run>-attempt-<attempt>" in runbook
    assert 'trusted_release="$trusted_release_artifact/trusted-release.json"' in runbook
    assert (
        'scanner_cache_lock="$(pwd -P)/deploy/dev-fleet/'
        'personal-dev-scanner-cache-lock.json"'
    ) in runbook
    assert 'scanner_cache_lock_sha256="$(sha256sum "$scanner_cache_lock"' in runbook
    assert '$(jq -r .scanner.lock_sha256 "$trusted_release")' in runbook
    assert '.images.personal_dev_scanner_cache "$trusted_release"' in runbook
    assert '.release.images.personal_dev_scanner_cache "$acceptance_plan"' in runbook
    assert '.scanner.cache_identity_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_cache_identity_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.java_database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_java_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert 'acceptance_plan="<absolute-owner-only-acceptance-plan.json>"' in runbook
    assert 'test "$(stat -c %a "$acceptance_plan")" = 600' in runbook
    assert 'test "$(stat -c %h "$acceptance_plan")" = 1' in runbook
    assert 'acceptance_plan_sha256="$(sha256sum "$acceptance_plan"' in runbook
    assert "personal-dev-control-plane render-acceptance" in normalized
    assert "personal-dev-control-plane status-acceptance" in normalized
    assert runbook.count("personal-dev-control-plane status-acceptance") >= 3
    assert '"application_ready":true' in runbook
    assert '"capacity_publication_ready":true' in runbook
    assert '"worker_available":false' in runbook
    assert '"manager_ceiling":0' in runbook

    assert 'owner_a_xdg="<absolute-mode-0700-owner-a-xdg-config-root>"' in runbook
    assert 'owner_b_xdg="<absolute-mode-0700-owner-b-xdg-config-root>"' in runbook
    assert 'owner_a_name="<owner-a-personal-name>"' in runbook
    assert 'owner_b_name="<owner-b-personal-name>"' in runbook
    assert 'XDG_CONFIG_HOME="$owner_a_xdg"' in runbook
    assert 'XDG_CONFIG_HOME="$owner_b_xdg"' in runbook
    assert runbook.count("loom auth whoami") == 2
    assert "owner_a_deploy_pid=$!" in runbook
    assert "owner_b_deploy_pid=$!" in runbook
    assert "wait \"$owner_a_deploy_pid\"" in runbook
    assert "wait \"$owner_b_deploy_pid\"" in runbook
    assert "owner_a_update_pid=$!" in runbook
    assert "owner_b_update_pid=$!" in runbook
    assert runbook.count("--min-slots 0") >= 5
    assert '--source-root "$owner_a_source_v1"' in normalized
    assert '--source-root "$owner_b_source_v1"' in normalized
    assert '--source-root "$owner_a_source_v2"' in normalized
    assert '--source-root "$owner_b_source_v2"' in normalized
    assert "expect_owner_rejection" in runbook
    assert 'loom dev status "$owner_b_name"' in normalized
    assert 'loom dev status "$owner_a_name"' in normalized
    assert 'loom dev destroy "$owner_b_name" --no-wait' in normalized
    assert 'loom dev destroy "$owner_a_name" --no-wait' in normalized
    assert 'loom dev destroy "$owner_a_name" --format json' in normalized
    assert 'loom dev destroy "$owner_b_name" --keep-data --format json' in normalized
    assert 'loom service up --environment "dev-$owner_b_name"' in normalized
    assert 'jq -e --arg previous "$retained_subject"' in normalized
    assert ".subject_id != $previous" in runbook

    assert 'shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"' in runbook
    assert '.release.shadow_manifest_sha256' in runbook
    assert 'cmp -s "$shadow_render_recheck" "$shadow_render"' in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "--field-manager=loom-personal-dev-control-plane" in normalized
    assert runbook.count(
        "rollout status deployment/loom-personal-dev-management"
    ) >= 2
    assert "capture_scanner_cache_init_status" in runbook
    assert runbook.count("personal-dev-scanner-cache-init") >= 2


def test_zero_capacity_acceptance_runbook_preserves_stop_and_authority_boundaries() -> None:
    runbook = _read("docs/runbooks/personal-dev-zero-capacity-acceptance.md")
    lowered = runbook.casefold()

    for phrase in (
        "credential or kubeconfig drift",
        "acceptance window",
        "runtimeclass or scanner drift",
        "secret key inventory drift",
        "migration or storage drift",
        "manager identity, configuration-epoch regression, execution epoch/state, or",
        "namespace ownership drift",
        "personal worker deployment",
    ):
        assert phrase in lowered
    assert "assert_pre_apply_interlocks" in runbook
    assert "assert_live_acceptance" in runbook
    assert "assert_rollback_interlocks" in runbook
    assert "backup_restore_evidence_sha256" in runbook
    assert "runtime_profile_sha256" in runbook
    assert "scanner_finding_policy_sha256" in runbook
    assert "approved secret channel" in lowered
    assert "exact key inventory" in lowered
    assert "two distinct authenticated owner sessions" in lowered
    assert "arbitrary committed, modified, and untracked source" in lowered
    assert "architecture-specific and architecture-neutral tasks are out of scope" in lowered
    assert "issue #906" in lowered
    assert "never delete pvcs" in lowered
    assert "never delete a namespace directly" in lowered
    assert "physical capacity remains unchanged" in lowered

    for legacy_input in (
        "scanner_binary=",
        "scanner_database=",
        "scanner_java_database=",
    ):
        assert legacy_input not in runbook
    for legacy_reference in (
        '"$scanner_binary"',
        '"$scanner_database"',
        '"$scanner_java_database"',
    ):
        assert legacy_reference not in runbook
    assert "kubectl cp" not in lowered
    assert "--download-db-only" not in lowered
    assert "--download-java-db-only" not in lowered
    assert "hostpath" not in lowered
    assert "temporary egress" not in lowered

    assert "loom-dev-shared" not in runbook
    assert "kubectl create secret" not in lowered
    assert "--from-literal" not in lowered
    assert "kubectl delete" not in lowered
    assert "kubectl scale" not in lowered
    assert "loom admin capacity-control-plane" not in lowered
    assert "systemctl" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered

    forward_diff = runbook.index("acceptance_diff_status=0")
    forward_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        forward_diff,
    )
    assert "assert_pre_apply_interlocks" in runbook[forward_diff:forward_apply]
    rollback_diff = runbook.index("rollback_diff_status=0")
    rollback_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        rollback_diff,
    )
    assert "assert_rollback_interlocks" in runbook[rollback_diff:rollback_apply]


def test_zero_capacity_acceptance_runbook_is_indexed_and_current() -> None:
    runbook_index = _read("docs/runbooks/README.md")
    fleet = _read("deploy/dev-fleet/README.md")
    architecture = _read("docs/architecture/multi-dev-environments.md")

    for document in (runbook_index, fleet, architecture):
        assert "personal-dev-zero-capacity-acceptance.md" in document
    assert "render-acceptance" in fleet
    assert "status-acceptance" in fleet
    assert "two concurrent owners" in architecture.casefold()
    assert "worker_available=false" in architecture


def test_personal_dev_builder_runtime_runbook_is_exact_and_inert() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    normalized = " ".join(runbook.split())
    lowered = runbook.casefold()

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert 'test "$(id -u)" != 0' in runbook
    assert "&& test ! -L" not in runbook
    assert 'test ! -e "$archive" &&' not in runbook
    assert "< <(" not in runbook
    assert "assert_pod_continuity()" in runbook
    assert runbook.count("assert_pod_continuity") >= 4
    assert "capture_runtime_node_state()" in runbook
    assert runbook.count("capture_runtime_node_state") >= 3
    assert "assert_node_cordon_state()" in runbook
    assert runbook.count("assert_node_cordon_state") >= 8
    assert (
        'get "node/$node" -o json > "$evidence_dir/$node.node-after.json"'
        not in normalized
    )
    assert 'test -z "$(git status' not in runbook
    assert "<absolute-existing-owner-only-evidence-root-outside-repository>" in runbook
    assert "artifacts/personal-dev" not in runbook
    assert 'test "$(stat -c %a "$evidence_root")" = 700' in runbook
    assert "deploy/dev-fleet/personal-dev-builder-runtime-profile.json" in runbook
    assert "deploy/dev-fleet/personal-dev-builder-runtime-class.yaml" in runbook
    assert "scripts/ops/install_personal_dev_builder_runtime.py" in runbook
    assert "release-20260810.0" in runbook
    assert (
        "3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe"
        "33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62"
    ) in runbook
    assert "trt-eai-oldlab-2" in runbook
    assert "trt-eai-oldlab-3" in runbook
    assert "trt-eai-oldlab-4" in runbook
    assert "trt-eai-oldlab-5" in runbook
    assert "control-plane node is never eligible" in lowered
    assert '"node-role.kubernetes.io/control-plane"' in runbook
    assert "expected_agent_names" in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" cordon "$node"' in normalized
    assert normalized.count('kubectl --kubeconfig "$kubeconfig" cordon "$node"') >= 3
    assert "do not drain" in lowered
    assert "a failure on one node stops the fleet rollout" in lowered
    assert "verify-staged" in runbook
    assert "verify-active" in runbook
    assert " remove " in normalized
    assert "systemctl restart k3s-agent" in normalized
    assert (
        "loom.dev/personal-dev-runtime-profile-a="
        "880b7c79013e38b016046c732209574d"
    ) in runbook
    assert (
        "loom.dev/personal-dev-runtime-profile-b="
        "48d6ae5a008164906f9951ba27765b76"
    ) in runbook
    assert (
        "loom.dev/personal-dev-runtime-profile-sha256="
        "880b7c79013e38b016046c732209574d48d6ae5a008164906f9951ba27765b76"
    ) in runbook
    assert "/proc/gvisor/kernel_is_gvisor" in runbook
    assert "rootless buildkit" in lowered
    assert "buildkit-qemu-aarch64" in runbook
    assert "linux/amd64" in runbook
    assert "linux/arm64" in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "executable-new-capacity ceiling remains exactly `0`" in runbook
    assert "no `loom-dev-<owner>` namespace" in runbook
    assert "no `loom-build-*` namespace" in runbook
    assert "loom-runtime-smoke" in runbook
    assert 'runtimeClassName: "loom-personal-dev-builder"' in runbook
    assert "automountServiceAccountToken: false" in runbook
    assert "build-egress" in runbook
    assert "_load_safe_kubeconfig" in runbook
    assert "render_runtime_class" in runbook
    assert "sudo -n --" in runbook
    assert "personal-dev-runtime\\.[A-Za-z0-9]{8}" in runbook
    assert "/root/loom-personal-dev-builder-runtime-rollout" in runbook
    assert "installer_sha256" in runbook
    assert "profile_module_sha256" in runbook
    assert (
        "/usr/bin/install -o root -g root -m 0600 "
        "'$remote_stage/gvisor-release-20260810.0.tar.bz2'"
    ) in normalized
    assert '"regular file:0:0:600:1"' in runbook
    assert "PYTHONPATH='$remote_stage'" not in runbook
    assert "cd '$root_stage' && sudo" not in runbook
    assert "assert_no_runtimeclass_consumers" in runbook
    assert runbook.count("assert_no_runtimeclass_consumers") >= 5
    assert "assert_runtimeclass_absent" in runbook
    assert runbook.count("assert_runtimeclass_absent") >= 6
    runtimeclass_absence_check = runbook.split(
        "assert_runtimeclass_absent() {", 1
    )[1].split("\n}", 1)[0]
    assert "--ignore-not-found -o name" in " ".join(
        runtimeclass_absence_check.split()
    )
    assert "> /dev/null 2>&1" not in runtimeclass_absence_check
    assert "assert_node_staging()" in runbook
    assert runbook.count('assert_node_staging "$node"') >= 4
    assert "assert_remote_staging()" in runbook
    assert runbook.count('assert_remote_staging "$node"') >= 2
    assert "assert_runtime_receipt()" in runbook
    assert runbook.count("assert_runtime_receipt") >= 10
    assert 'test ! -e "$root_stage_base"' in runbook
    assert "InvocationID" in runbook
    assert "await_fresh_node_lease()" in runbook
    assert runbook.count("await_fresh_node_lease") >= 4
    assert "kube-node-lease" in runbook
    assert "/usr/bin/env LC_ALL=C /usr/bin/stat" in normalized
    assert runbook.count("k3s-invocation.before.txt") >= 3
    assert runbook.count("k3s-invocation.after.txt") >= 3
    assert "runtime_class_sha256" in runbook
    assert "remove_node_runtime_identity" in runbook
    assert runbook.count('remove_node_runtime_identity "$node"') >= 2
    assert 'loom.dev/personal-dev-runtime-profile-a-' not in runbook
    assert 'loom.dev/personal-dev-runtime-profile-b-' not in runbook
    assert 'loom.dev/personal-dev-runtime-profile-sha256-' not in runbook
    assert "--type=merge" in runbook
    assert "reviewed_runtime_diff_sha256='<reviewed-runtime-diff-sha256>'" in runbook
    assert 'test "$runtime_diff_status" -eq 1' in runbook
    assert "<ssh-user>@$node" not in runbook
    assert "assert_smoke_namespace_owned" in runbook
    assert runbook.count("assert_smoke_namespace_owned") >= 3
    assert 'delete "pod/$pod"' not in runbook
    assert "assert_smoke_namespace_absent" in runbook
    assert runbook.count("assert_smoke_namespace_absent") >= 4
    assert 'create -f "$smoke_namespace_manifest"' in normalized
    assert "--field-manager=loom-personal-dev-runtime-smoke" not in runbook
    assert 'test -z "$(kubectl' not in runbook
    assert "loom.dev/runtime-rollout-source-sha" in runbook
    assert runbook.count("loom-personal-dev-runtime-smoke") >= 8
    assert runbook.count(".spec.nodeSelector == {") >= 2
    assert runbook.count(".scheduling.nodeSelector == {") >= 3
    assert '"pod-security.kubernetes.io/enforce": "privileged"' in runbook
    assert '"pod-security.kubernetes.io/enforce-version": "v1.36"' in runbook
    assert '"pod-security.kubernetes.io/audit": "restricted"' in runbook
    assert 'restartPolicy: "Always"' in runbook
    assert 'command: ["/usr/local/bin/loom-personal-dev-buildkitd"]' in runbook
    assert 'capabilities: {drop: ["ALL"], add: ["SETGID", "SETUID"]}' in runbook
    assert 'seccompProfile: {type: "Unconfined"}' in runbook
    assert "shareProcessNamespace: false" in runbook
    assert "/usr/bin/buildctl" in runbook
    assert "/usr/bin/buildctl-daemonless.sh" not in runbook
    assert "loom-nnp=1" in runbook
    assert 'configMap: {name: "buildkit-conformance", defaultMode: 292}' in runbook
    assert "get secrets -o name" in normalized
    assert "services,secrets" not in normalized
    assert "apply --server-side --force-conflicts" not in normalized
    assert "|| true" not in runbook
    assert "PYTHONPATH='$root_stage'" in runbook
    assert runbook.count(" remove --profile ") >= 2

    assert "loom-dev-shared" not in runbook
    assert "kubectl drain" not in lowered
    assert "kubectl create namespace" not in lowered
    assert "loom service up" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered


def test_personal_dev_builder_runtime_embedded_programs_parse() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    bash_blocks = re.findall(r"^```bash\n(.*?)^```$", runbook, flags=re.DOTALL | re.MULTILINE)
    python_programs = re.findall(r"<<'PY'\n(.*?)\nPY\n", runbook, flags=re.DOTALL)
    jq_filters = re.findall(r"\bjq\b[^']*'(.*?)'", runbook, flags=re.DOTALL)

    assert bash_blocks
    bash = subprocess.run(
        ["bash", "-n"],
        input="\n".join(bash_blocks),
        text=True,
        capture_output=True,
        check=False,
    )
    assert bash.returncode == 0, bash.stderr
    assert python_programs
    for program in python_programs:
        compile(program, "personal-dev-builder-runtime.md", "exec")
    assert jq_filters
    for jq_filter in jq_filters:
        jq = subprocess.run(
            ["jq", "-n", f"def candidate: {jq_filter}; empty"],
            text=True,
            capture_output=True,
            check=False,
        )
        assert jq.returncode == 0, jq.stderr

    node_filters = [
        value for value in jq_filters if "node-role.kubernetes.io/control-plane" in value
    ]
    assert len(node_filters) == 1
    agent_names = [f"trt-eai-oldlab-{number}" for number in range(2, 6)]

    def node(name: str, *, control_plane: bool = False) -> dict[str, object]:
        labels = {"node-role.kubernetes.io/control-plane": ""} if control_plane else {}
        return {
            "metadata": {"annotations": {}, "labels": labels, "name": name},
            "status": {
                "conditions": [
                    {"status": "True", "type": "Ready"},
                    {"status": "False", "type": "DiskPressure"},
                ]
            },
        }

    healthy_nodes = {
        "items": [node("control", control_plane=True)]
        + [node(name) for name in agent_names]
    }

    def evaluate_nodes(payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "jq",
                "-e",
                "--argjson",
                "agents",
                json.dumps(agent_names),
                node_filters[0],
            ],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )

    healthy = evaluate_nodes(healthy_nodes)
    assert healthy.returncode == 0, healthy.stderr
    drifted_nodes = json.loads(json.dumps(healthy_nodes))
    drifted_control_labels = drifted_nodes["items"][0]["metadata"]["labels"]
    drifted_control_labels["loom.dev/personal-dev-runtime-profile-a"] = "unexpected"
    assert evaluate_nodes(drifted_nodes).returncode != 0

    smoke_filters = [value for value in jq_filters if "kernel_is_gvisor" in value]
    assert len(smoke_filters) == 1
    rendered = subprocess.run(
        [
            "jq",
            "-n",
            "--arg",
            "namespace",
            "loom-runtime-smoke",
            "--arg",
            "node",
            "trt-eai-oldlab-2",
            "--arg",
            "pod",
            "gvisor-smoke-2",
            "--arg",
            "image",
            "example.invalid/smoke@sha256:" + "1" * 64,
            "--arg",
            "source",
            "2" * 40,
            smoke_filters[0],
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    command = json.loads(rendered.stdout)["spec"]["containers"][0]["args"][0]
    shell = subprocess.run(
        ["/bin/sh", "-n", "-c", command],
        text=True,
        capture_output=True,
        check=False,
    )
    assert shell.returncode == 0, shell.stderr

    buildkit_filters = [
        value
        for value in jq_filters
        if 'restartPolicy: "Always"' in value and 'name: "buildkitd"' in value
    ]
    assert len(buildkit_filters) == 1
    buildkit_pod = json.loads(
        subprocess.run(
            [
                "jq",
                "-n",
                "--arg",
                "namespace",
                "loom-runtime-smoke",
                "--arg",
                "image",
                "example.invalid/builder@sha256:" + "3" * 64,
                "--arg",
                "base",
                "example.invalid/base@sha256:" + "4" * 64,
                "--arg",
                "source",
                "5" * 40,
                buildkit_filters[0],
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    spec = buildkit_pod["spec"]
    assert spec["shareProcessNamespace"] is False
    assert len(spec["containers"]) == 1
    assert len(spec["initContainers"]) == 1
    client = spec["containers"][0]
    sidecar = spec["initContainers"][0]
    assert client["name"] == "conformance"
    assert sidecar["name"] == "buildkitd"
    client_mounts = {mount["name"] for mount in client["volumeMounts"]}
    sidecar_mounts = {mount["name"] for mount in sidecar["volumeMounts"]}
    assert client_mounts == {"buildkit-run", "script", "tmp-client", "workspace"}
    assert sidecar_mounts == {"buildkit-run", "buildkit-state", "tmp-buildkit"}
    assert not {"attempt-capability", "contract", "script", "workspace"} & (
        sidecar_mounts
    )
    assert not {"buildkit-state", "tmp-buildkit"} & client_mounts


def test_personal_dev_builder_runtime_configmap_rendering_executes(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    start = runbook.index(
        'buildkit_configmap="$evidence_dir/buildkit-conformance.configmap.json"'
    )
    end_marker = 'chmod 0600 "$buildkit_configmap"'
    end = runbook.index(end_marker, start) + len(end_marker)
    rendering = runbook[start:end]

    buildkit_script = tmp_path / "run.sh"
    buildkit_script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    merged_source_sha = "0123456789abcdef0123456789abcdef01234567"
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "kubeconfig=/dev/null\n"
            "smoke_namespace=loom-runtime-smoke\n"
            f"buildkit_script={shlex.quote(str(buildkit_script))}\n"
            f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
            f"merged_source_sha={merged_source_sha}\n"
            f"{rendering}\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr

    rendered = json.loads(
        (evidence_dir / "buildkit-conformance.configmap.json").read_text(
            encoding="utf-8"
        )
    )
    assert rendered["metadata"]["namespace"] == "loom-runtime-smoke"
    assert rendered["metadata"]["labels"] == {
        "app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"
    }
    assert rendered["metadata"]["annotations"] == {
        "loom.dev/runtime-rollout-source-sha": merged_source_sha
    }
    assert rendered["data"] == {"run.sh": "#!/bin/sh\nexit 0\n"}


def test_personal_dev_builder_runtime_hostname_identity_uses_dns_case_rules() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    marker = "assert_dns_hostname_identity() {"
    start = runbook.index(marker)
    function = runbook[start:].split("\n}", 1)[0] + "\n}"
    assert (
        runbook.count(
            'assert_dns_hostname_identity "$observed_hostname" "$node"'
        )
        == 2
    )

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            function
            + "\n"
            + "assert_dns_hostname_identity "
            + "trt-EAI-OLDLAB-2 trt-eai-oldlab-2\n"
            + "if assert_dns_hostname_identity "
            + "trt-eai-oldlab-3 trt-eai-oldlab-2; then exit 1; fi\n"
            + "if assert_dns_hostname_identity "
            + "trt-eai-oldlab-2. trt-eai-oldlab-2; then exit 1; fi\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_separates_ssh_stdin_from_scp_options() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    options = re.search(
        r"^ssh_options=\([^\n]+\)\nssh_run_options=\([^\n]+\)$",
        runbook,
        flags=re.MULTILINE,
    )
    assert options is not None

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            options.group(0)
            + "\n"
            + "ssh() {\n"
            + '  test "$1" = -n\n'
            + "}\n"
            + "scp() {\n"
            + '  for argument in "$@"; do test "$argument" != -n; done\n'
            + "}\n"
            + 'ssh "${ssh_run_options[@]}" target /bin/true\n'
            + 'scp "${ssh_options[@]}" -- source target:/tmp/\n'
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr
    assert 'ssh "${ssh_options[@]}"' not in runbook
    assert 'scp "${ssh_run_options[@]}"' not in runbook


def test_personal_dev_builder_runtime_remote_staging_directories_are_private(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    command = re.search(
        r'^ssh "\$\{ssh_run_options\[@\]\}" "\$target" \\\n'
        r'  "/usr/bin/install -d -m 0700 [^"]+"$',
        runbook,
        flags=re.MULTILINE,
    )
    assert command is not None

    remote_stage = tmp_path / "remote-stage"
    remote_stage.mkdir(mode=0o700)
    remote_stage.chmod(0o700)
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "umask 022\n"
            f"remote_stage={shlex.quote(str(remote_stage))}\n"
            "ssh_run_options=()\n"
            "target=target\n"
            "ssh() {\n"
            '  test "$#" -eq 2\n'
            '  test "$1" = target\n'
            '  /bin/bash -ceu -- "$2"\n'
            "}\n"
            + command.group(0)
            + "\n"
            + 'for directory in "$remote_stage" '
            '"$remote_stage/scripts" "$remote_stage/scripts/ops"; do\n'
            + '  mode="$(stat -c %a "$directory")"\n'
            + '  test "$mode" = 700 || { '
            'printf "%s mode=%s\\n" "$directory" "$mode" >&2; exit 1; }\n'
            + "done\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_root_staging_directories_are_private(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    root_stage_evidence = (
        'printf \'%s\\n\' "$root_stage" > '
        '"$evidence_dir/$node.root-stage.txt"'
    )
    command_start = runbook.index(
        'ssh "${ssh_run_options[@]}" "$target" \\\n',
        runbook.index(root_stage_evidence),
    )
    command_end = runbook.index("\nassert_node_staging", command_start)
    command = runbook[command_start:command_end]

    remote_stage = tmp_path / "remote-stage"
    remote_ops = remote_stage / "scripts" / "ops"
    remote_ops.mkdir(parents=True)
    for relative in (
        "personal-dev-builder-runtime-profile.json",
        "gvisor-release-20260810.0.tar.bz2",
        "scripts/ops/install_personal_dev_builder_runtime.py",
        "scripts/ops/personal_dev_builder_runtime_profile.py",
    ):
        (remote_stage / relative).write_bytes(b"test fixture\n")
    root_stage_base = tmp_path / "root-stage"
    root_stage_parent = root_stage_base / "source-sha"
    root_stage = root_stage_parent / "trt-eai-oldlab-2"
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "umask 022\n"
            f"remote_stage={shlex.quote(str(remote_stage))}\n"
            f"root_stage_base={shlex.quote(str(root_stage_base))}\n"
            f"root_stage_parent={shlex.quote(str(root_stage_parent))}\n"
            f"root_stage={shlex.quote(str(root_stage))}\n"
            "ssh_run_options=()\n"
            "target=target\n"
            "sudo() {\n"
            '  test "$1" = -n\n'
            '  test "$2" = --\n'
            "  shift 2\n"
            '  if test "$1" != /usr/bin/install; then "$@"; return; fi\n'
            "  shift\n"
            "  install_arguments=()\n"
            '  while test "$#" -gt 0; do\n'
            '    case "$1" in\n'
            "      -o|-g) shift 2 ;;\n"
            '      *) install_arguments+=("$1"); shift ;;\n'
            "    esac\n"
            "  done\n"
            '  /usr/bin/install "${install_arguments[@]}"\n'
            "}\n"
            "ssh() {\n"
            '  test "$#" -eq 2\n'
            '  test "$1" = target\n'
            '  eval "$2"\n'
            "}\n"
            + command
            + "\n"
            + 'for directory in "$root_stage_base" '
            '"${root_stage%/*}" "$root_stage" '
            '"$root_stage/scripts" "$root_stage/scripts/ops"; do\n'
            + '  mode="$(stat -c %a "$directory")"\n'
            + '  test "$mode" = 700 || { '
            'printf "%s mode=%s\\n" "$directory" "$mode" >&2; exit 1; }\n'
            + "done\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_longhorn_health_requires_live_readiness() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    jq_filters = re.findall(r"\bjq\b[^']*'(.*?)'", runbook, flags=re.DOTALL)
    controller_filters = [
        value
        for value in jq_filters
        if 'select(.kind == "Deployment")' in value
        and "desiredNumberScheduled" in value
    ]
    pod_filters = [
        value
        for value in jq_filters
        if '.status.phase != "Succeeded"' in value
        and '.status.phase != "Failed"' in value
        and "containerStatuses" in value
    ]
    assert len(controller_filters) == 1
    assert len(pod_filters) == 1

    def evaluate(
        jq_filter: str, payload: dict[str, object]
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["jq", "-e", jq_filter],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )

    healthy_controllers: dict[str, object] = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"generation": 4},
                "spec": {"replicas": 3},
                "status": {
                    "availableReplicas": 3,
                    "observedGeneration": 4,
                    "readyReplicas": 3,
                    "updatedReplicas": 3,
                },
            },
            {
                "kind": "DaemonSet",
                "metadata": {"generation": 7},
                "status": {
                    "desiredNumberScheduled": 5,
                    "numberAvailable": 5,
                    "numberReady": 5,
                    "observedGeneration": 7,
                    "updatedNumberScheduled": 5,
                },
            },
        ]
    }
    assert evaluate(controller_filters[0], healthy_controllers).returncode == 0
    deployment_only = {"items": healthy_controllers["items"][:1]}
    assert evaluate(controller_filters[0], deployment_only).returncode != 0
    daemon_set_only = {"items": healthy_controllers["items"][1:]}
    assert evaluate(controller_filters[0], daemon_set_only).returncode != 0
    degraded_controllers = json.loads(json.dumps(healthy_controllers))
    degraded_controllers["items"][0]["status"]["readyReplicas"] = 2
    assert evaluate(controller_filters[0], degraded_controllers).returncode != 0
    degraded_daemon_set = json.loads(json.dumps(healthy_controllers))
    degraded_daemon_set["items"][1]["status"]["numberReady"] = 4
    assert evaluate(controller_filters[0], degraded_daemon_set).returncode != 0

    ready_with_history: dict[str, object] = {
        "items": [
            {
                "status": {
                    "containerStatuses": [{"ready": True}],
                    "phase": "Running",
                }
            },
            {"status": {"phase": "Failed", "reason": "Evicted"}},
            {"status": {"phase": "Succeeded"}},
        ]
    }
    assert evaluate(pod_filters[0], ready_with_history).returncode == 0
    not_ready = json.loads(json.dumps(ready_with_history))
    not_ready["items"][0]["status"]["containerStatuses"][0]["ready"] = False
    assert evaluate(pod_filters[0], not_ready).returncode != 0
    pending_with_ready = json.loads(json.dumps(ready_with_history))
    pending_with_ready["items"].append({"status": {"phase": "Pending"}})
    assert evaluate(pod_filters[0], pending_with_ready).returncode != 0
    terminal_only = {"items": ready_with_history["items"][1:]}
    assert evaluate(pod_filters[0], terminal_only).returncode != 0


def test_personal_dev_builder_runtime_runbook_is_indexed_and_architecture_linked() -> None:
    for document in (
        _read("docs/runbooks/README.md"),
        _read("deploy/dev-fleet/README.md"),
        _read("docs/architecture/personal-dev-management-plane-deployment.md"),
    ):
        assert "personal-dev-builder-runtime.md" in document


def test_release_bound_scanner_cache_is_architecture_linked_and_inert() -> None:
    management = _read(
        "docs/architecture/personal-dev-management-plane-deployment.md"
    )
    environments = _read("docs/architecture/multi-dev-environments.md")
    fleet = _read("deploy/dev-fleet/README.md")

    for document in (management, environments, fleet):
        lowered = document.casefold()
        assert "release-bound scanner cache" in lowered
        assert "personal_dev_scanner_cache" in document
        assert "ceiling" in lowered and "zero" in lowered
    for document in (management, environments):
        assert "personal-dev-scanner-cache-preparation.md" in document
    assert "remain separate operational gates" in " ".join(
        management.casefold().split()
    )
    assert "remain separate operational gates" in " ".join(fleet.casefold().split())


def test_scanner_cache_design_states_the_pod_scoped_network_boundary() -> None:
    design = _read("docs/architecture/personal-dev-scanner-cache-preparation.md")
    normalized = " ".join(design.split())

    assert "Kubernetes NetworkPolicy is Pod-scoped" in normalized
    assert "does not have a separate network namespace" in normalized
    assert "performs no runtime network operation" in normalized
    assert "no network authority" not in design
