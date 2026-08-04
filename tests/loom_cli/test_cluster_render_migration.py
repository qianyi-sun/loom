"""`loom cluster render-migration` unit tests (#332).

Renders the one-off Alembic migration Job manifest with the sanctioned
`app: loom-migration` label so the loom-postgres NetworkPolicy grants it
port 5432 without operators building an ad-hoc NetworkPolicy per rollout.
"""

from __future__ import annotations

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.__main__ import main
from loom_cli.cluster_migration import render_migration_manifest


class TestRenderMigrationManifest:
    """Pure render function (no argparse)."""

    def test_yaml_parses_and_produces_a_job(self) -> None:
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom-staging",
            job_suffix="20260702t172540z",
        )
        docs = [d for d in yaml.safe_load_all(text) if d]
        assert len(docs) == 1
        job = docs[0]
        assert job["apiVersion"] == "batch/v1"
        assert job["kind"] == "Job"
        assert job["metadata"]["namespace"] == "loom-staging"
        # Deterministic job name — critical for idempotence.
        assert "loom-migrate-staging-05ab776" in job["metadata"]["name"]
        assert "20260702t172540z" in job["metadata"]["name"]

    def test_pod_template_carries_the_sanctioned_migration_label(self) -> None:
        """#332: `app: loom-migration` grants postgres netpol ingress."""
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom-staging",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        # Both the Job's own labels AND the pod template labels must
        # match — the podSelector in the NetworkPolicy targets pods.
        assert job["metadata"]["labels"]["app"] == "loom-migration"
        pod_labels = job["spec"]["template"]["metadata"]["labels"]
        assert pod_labels["app"] == "loom-migration"

    def test_container_uses_release_image_and_alembic_upgrade(self) -> None:
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom-staging",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        container = job["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == "loom-control-plane:staging-05ab776"
        assert container["command"] == [
            "alembic",
            "-c",
            "migrations/alembic.ini",
            "upgrade",
            "head",
        ]

    def test_db_url_is_pulled_from_the_control_plane_secret_key(self) -> None:
        """Alembic reads LOOM_DB_URL per `migrations/env.py`; the
        secret key we source it from is still `cp-db-url` because
        Alembic needs the same DB perms as the control plane and a
        separate migration credential would just be another rotation
        liability. See #364."""
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom-staging",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        env_by_name = {
            e["name"]: e for e in job["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        assert "LOOM_DB_URL" in env_by_name, (
            "Alembic requires LOOM_DB_URL; the migration Job must not "
            "set LOOM_CP_DB_URL (that's what the control-plane reads)."
        )
        assert "LOOM_CP_DB_URL" not in env_by_name
        secret_ref = env_by_name["LOOM_DB_URL"]["valueFrom"]["secretKeyRef"]
        assert secret_ref["name"] == "loom-secrets"
        assert secret_ref["key"] == "cp-db-url"

    def test_ttl_and_active_deadline_keep_the_job_self_cleaning(self) -> None:
        """A migration Job that hangs around forever after finishing
        blocks the next rollout's `kubectl apply -f -` because the
        Job's `spec.template` is immutable."""
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom-staging",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        assert isinstance(job["spec"]["ttlSecondsAfterFinished"], int)
        assert job["spec"]["ttlSecondsAfterFinished"] > 0
        assert isinstance(job["spec"]["activeDeadlineSeconds"], int)
        assert job["spec"]["activeDeadlineSeconds"] > 0
        assert job["spec"]["template"]["spec"]["restartPolicy"] == "Never"
        assert job["spec"]["backoffLimit"] == 1

    def test_default_namespace(self) -> None:
        text = render_migration_manifest(
            image_tag="staging-05ab776",
            namespace="loom",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        assert job["metadata"]["namespace"] == "loom"

    def test_image_ref_preserves_literal_tag_while_name_is_normalised(self) -> None:
        """The DNS-1123 normalisation for the Job *name* must never leak
        into the pulled *image* tag or the `loom.image-tag` label.

        A release tag like ``0.7`` (single-node local/dev) or
        ``Staging.05ab776`` names a real pushed image and is a valid
        Docker tag + valid k8s label value. Rewriting dots/uppercase in
        the image reference made the migration Job ImagePullBackOff
        against a tag that was never built — the exact failure seen
        bringing up a single-node dev cluster whose images are tagged
        ``0.7``.
        """
        text = render_migration_manifest(
            image_tag="0.7",
            namespace="loom-local",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        container = job["spec"]["template"]["spec"]["containers"][0]
        # Image keeps the literal tag...
        assert container["image"] == "loom-control-plane:0.7"
        assert job["metadata"]["labels"]["loom.image-tag"] == "0.7"
        # ...while the Job's object name is still RFC 1123 safe.
        assert job["metadata"]["name"] == "loom-migrate-0-7-a"

    def test_job_name_normalizes_uppercase_and_dots(self) -> None:
        """RFC 1123 name convention: dashes + lowercase alphanumerics."""
        text = render_migration_manifest(
            image_tag="Staging.05ab776",
            namespace="loom",
            job_suffix="a",
        )
        job = next(d for d in yaml.safe_load_all(text) if d)
        assert (
            job["metadata"]["name"] == "loom-migrate-staging-05ab776-a"
        )  # DNS-normalization stress


class TestPostgresNetPolIncludesMigrationSelector:
    """The postgres NetworkPolicy template must permit app=loom-migration
    ingress so the Job can connect. Static template inspection — no
    cluster-config plumbing needed."""

    def test_network_policy_template_lists_migration_app(self) -> None:
        from importlib import resources

        pkg = resources.files("loom_cli.templates.k8s")
        text = (pkg / "network-policies.yaml.j2").read_text()
        # Find the postgres NetworkPolicy block. Simple substring check
        # is enough: the file is small, the ingress list is compact, and
        # a regression here is a substring regression by definition.
        postgres_start = text.find("name: loom-postgres")
        assert postgres_start != -1, "postgres NetworkPolicy missing from template"
        # Look for `app: loom-migration` inside the postgres ingress
        # section (bounded by `---` to the next document).
        next_doc = text.find("\n---", postgres_start)
        postgres_section = text[postgres_start : next_doc if next_doc != -1 else None]
        assert "app: loom-migration" in postgres_section, (
            "loom-postgres NetworkPolicy must permit app=loom-migration "
            "ingress so sanctioned migration Jobs can connect (#332)"
        )


class TestCLIDispatch:
    """End-to-end CLI: `loom cluster render-migration ...`."""

    def test_prints_valid_yaml_by_default(self, capsys: pytest.CaptureFixture[str]) -> None:
        rc = main(
            [
                "cluster",
                "render-migration",
                "--image-tag",
                "staging-05ab776",
                "--namespace",
                "loom-staging",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        # The rendered output must be parseable YAML.
        docs = [d for d in yaml.safe_load_all(out) if d]
        assert len(docs) == 1
        assert docs[0]["kind"] == "Job"
        assert docs[0]["metadata"]["namespace"] == "loom-staging"

    def test_requires_image_tag(self, capsys: pytest.CaptureFixture[str]) -> None:
        # argparse's `required=True` exits via SystemExit(2) before the
        # handler runs; catch that rather than checking the return code.
        with pytest.raises(SystemExit) as exc:
            main(
                [
                    "cluster",
                    "render-migration",
                    "--namespace",
                    "loom-staging",
                ]
            )
        assert exc.value.code == 2
        err = capsys.readouterr().err
        assert "image-tag" in err

    def test_default_namespace_is_loom(self, capsys: pytest.CaptureFixture[str]) -> None:
        rc = main(
            [
                "cluster",
                "render-migration",
                "--image-tag",
                "staging-05ab776",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        job = next(d for d in yaml.safe_load_all(out) if d)
        assert job["metadata"]["namespace"] == "loom"

    def test_custom_job_suffix(self, capsys: pytest.CaptureFixture[str]) -> None:
        rc = main(
            [
                "cluster",
                "render-migration",
                "--image-tag",
                "staging-05ab776",
                "--job-suffix",
                "20260702t172540z",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        job = next(d for d in yaml.safe_load_all(out) if d)
        assert "20260702t172540z" in job["metadata"]["name"]
