"""One-off Alembic migration Job renderer (#332).

The sanctioned way to run Alembic during a staging rollout.

The problem this solves: the standing `loom-postgres` NetworkPolicy only
permits ingress from `app=loom-control-plane`, `app=loom-service`, and
`app=loom-llm-gateway` (plus, per #332, `app=loom-migration`). A generic
migration Job labeled anything else is denied 5432. Labeling the Job as
one of the standing service labels would pollute the corresponding
Service selector and route real traffic to the transient Job pod.

This renderer emits a Job with:

* `app=loom-migration` on both the Job and its pod template so the
  Postgres NetworkPolicy grants ingress (see network-policies.yaml.j2).
* The release image tag (`loom-control-plane:<tag>`) so alembic sees
  the same migrations directory the release rolls out.
* `LOOM_DB_URL` (the env var `database/migrations/env.py` reads) sourced from
  `secretKeyRef: {name: loom-secrets, key: cp-db-url}` — reuses the
  control-plane's credential because Alembic needs the same perms; the
  var name matches the tool, not the consumer (#364).
* `ttlSecondsAfterFinished` + `activeDeadlineSeconds` + `backoffLimit=1`
  so a completed or hung Job cleans itself up and doesn't block the
  next rollout's `kubectl apply -f -`.

The renderer is a plain string emission (Jinja2 template lives beside
the cluster manifests); the CLI shim is a thin argparse wrapper.

The protected operator can instead select the fixed separated staging owner.
That profile uses a dedicated per-Job credential/CA Secret, explicit SET ROLE,
no service-account token, and lifecycle-controlled Job cleanup. Rendering does
not create the role or Secret and does not authorize migration execution.
"""

from __future__ import annotations

import re
from importlib import resources

from loom_cli.application_migration_contract import (
    APPLICATION_OWNER_ROLE,
    application_migration_authority,
)

_DEFAULT_JOB_SUFFIX = "0"


def _normalise_dns_component(text: str) -> str:
    """Force a string into RFC 1123 DNS-label shape.

    Kubernetes object names must match `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
    per RFC 1123. Uppercase, dots, and other punctuation are common in
    image tags (e.g. `Staging.05ab776`) so normalise here rather
    than push the burden onto the operator.
    """
    text = text.lower()
    text = re.sub(r"[^a-z0-9-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def render_migration_manifest(
    *,
    image_tag: str,
    namespace: str = "loom",
    job_suffix: str = _DEFAULT_JOB_SUFFIX,
    container_registry: str = "",
    registry_digest: str = "",
    application_owner_role: str = "",
) -> str:
    """Render the migration Job manifest to a YAML string.

    Args:
        image_tag: Release image tag (e.g. ``staging-05ab776``). The
            Job runs the ``loom-control-plane:<image_tag>`` image.
        namespace: Kubernetes namespace. Defaults to ``loom``.
        job_suffix: Uniqueness token appended to the Job name so a
            re-run against the same image tag doesn't collide with the
            previous Job (which sticks around for ``ttlSecondsAfterFinished``).
            Common choice: a UTC timestamp like ``20260702t172540z``.
        container_registry: Optional registry prefix (no trailing slash)
            applied to the loom-control-plane image reference. Empty
            (default) keeps the historical unprefixed shape. Mirrors
            ``ClusterConfig.container_registry``.

    Returns:
        YAML text ready to pipe to ``kubectl apply -f -``.
    """
    if application_owner_role and (application_owner_role != APPLICATION_OWNER_ROLE or namespace != "loom-staging"):
        raise ValueError("application migration owner scope is invalid")
    try:
        from jinja2 import Environment, FileSystemLoader, StrictUndefined
    except ModuleNotFoundError as exc:  # pragma: no cover — dep gate
        raise RuntimeError(
            "the 'jinja2' package is required for `loom cluster "
            "render-migration`. Run `uv sync` or `pip install -e .` "
            "to pick up dependencies."
        ) from exc

    pkg_path = resources.files("loom_cli.templates.k8s")
    env = Environment(
        loader=FileSystemLoader(str(pkg_path)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("migration-job.yaml.j2")
    prefix = f"{container_registry}/" if container_registry else ""
    suffix = f"@{registry_digest}" if registry_digest else f":{image_tag}"
    job_name = f"loom-migrate-{_normalise_dns_component(image_tag)}-{_normalise_dns_component(job_suffix)}"
    return template.render(
        # The image reference and the `loom.image-tag` label must carry the
        # *literal* release tag — that names a real pushed image. Dotted or
        # uppercase tags (e.g. `0.7` for local dev, `Staging.05ab776`) are
        # valid Docker tags and valid k8s label values, so pass them raw.
        # Only the k8s *object name* needs RFC 1123 normalisation, done
        # separately as `job_name_tag` so it never rewrites the pulled tag.
        image_tag=image_tag,
        job_name_tag=_normalise_dns_component(image_tag),
        namespace=namespace,
        job_suffix=_normalise_dns_component(job_suffix),
        container_registry=container_registry,
        registry_digest=registry_digest,
        image_reference=f"{prefix}loom-control-plane{suffix}",
        application_owner_role=application_owner_role,
        application_authority=application_migration_authority(job_name) if application_owner_role else None,
    )
