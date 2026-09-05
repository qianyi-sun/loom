# Task-image builder Phase 2D1 registry publication runbook

Phase 2D1 is a merged but inactive publication increment. It adds the
authority, guard, and supervisor code paths needed to mint short-lived
repository-scoped OCI Distribution credentials, upload a bounded OCI layout,
and record an immutable publication candidate. It does not activate builder
production publication, does not mark materializations ready, and does not
grant task execution from registry state.

## Inert production boundary

Keep these conditions true until a later protected activation explicitly
changes them:

- The task-image authority Deployment remains `replicas: 0` and default-deny.
- Rootless provider policies remain `enabled = false`.
- `production_certification_allowed` remains `false` and `certified_nodes`
  remains empty.
- No live manifest contains a registry signing Secret, registry signing key
  mount, registry signing environment variable, registry endpoint, provider
  activation, reservation, node feature, systemd enablement, or `current`
  symlink for Phase 2D1.
- Phase 1 builder credential, reservation, rollback, and retention procedures
  remain byte-identical to the Phase 1 accepted baseline.

## Credential invariants

Future Phase 2D2/Phase 4 operators may configure registry publication only
after the protected acceptance gates name the exact values. Phase 2D1 requires
all of the following when that future ceremony occurs:

- A dedicated RSA signing key of at least 3072 bits.
- RS256 tokens with a `kid` equal to the RFC 7638 SHA-256 JWK thumbprint.
- Claims limited to `iss`, `sub`, `aud`, `exp`, `nbf`, `iat`, `jti`, and one
  sorted `access` entry.
- One `repository` access entry with actions exactly `pull,push`.
- A repository derived only by the authority:
  `loom-task-image-attempts/<architecture>/<attempt-id>/<component-segment>`.
- No caller-supplied registry origin, service, issuer, subject, repository,
  scope, action, token lifetime, or generation.
- A credential lifetime of at most 45 seconds and no later than the current
  grant, session, containment attestation, or materialization lease.
- Renewal by new generation only; no token is extended in place.

Raw tokens may exist only in the authority signer, encrypted `SecretStore`, a
sealed memfd, locked supervisor memory, and bounded TLS request buffers. They
must not appear in paths, command arguments, environment, Docker config,
BuildKit auth, build arguments, logs, metrics, ledgers, database columns, or
persistent allocation storage.

## Publication candidate invariants

A publication candidate is evidence, not readiness. It is immutable,
attempt-bound, component-bound, credential-generation-bound, and replay-safe.
It may record exact registry upload receipts, OCI digests, sizes, media types,
platform evidence, and component identity. It must not update
`task_image_materializations.registry_images`, set a materialization to
`ready`, produce an execution grant, or certify a node.

Registry `HEAD`, successful blob upload, manifest upload, or a recorded
candidate remains insufficient for task-image execution. Phase 2D2 is the next
increment allowed to validate registry bytes and sign publication readiness;
protected shadow acceptance and Phase 4 activation are still required after
that.

## Future registry/JWKS ceremony

This repository intentionally contains no deployable registry endpoint, signing
key, JWKS URL, token issuer value, or service audience. During a later accepted
ceremony, operators must provide the full optional registry configuration as
owner-only runtime state:

- registry HTTPS origin;
- registry service audience;
- registry issuer;
- registry signing key file;
- the registry-side JWKS trust relationship for the corresponding public key.

Document placeholders in prose only. Do not commit example secret material,
real endpoints, copied human tokens, static registry tokens, or activation
commands that imply Phase 2D1 completed the credential ceremony.

## Failure handling

Treat registry and publication transport failures as retryable infrastructure
work. They do not consume deterministic task failure budget and are not
containment loss by themselves. If optional registry configuration is absent or
incomplete, the authority publication routes must fail closed with HTTP 503.
