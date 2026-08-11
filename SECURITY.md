# Security

## Reporting

Report security issues privately to the repository owner or platform owner. Do
not open public GitHub issues for credentials, access control bugs, sandbox
escape concerns, private endpoint leaks, or sensitive data exposure.

If GitHub private vulnerability reporting is enabled, use it for reports that
include exploit details or sensitive reproduction steps. Otherwise contact a
maintainer directly and share only the minimum detail needed to triage the
issue.

## Repository Rules

- Do not commit secrets, API keys, cloud credentials, SSH keys, database dumps,
  private endpoint inventories, or model provider tokens.
- Keep environment-specific values in secret stores or local ignored files.
- Use `.env.example` for required variable names only.
- Treat sandbox execution, artifact storage, and evaluator outputs as potentially
  sensitive until data classification rules are defined.
- Pull request workflows must run with read-only default `GITHUB_TOKEN`
  permissions and must not receive deployment, publishing, model-provider, or
  infrastructure secrets.
- Workflows with side effects, such as benchmark publishing or deployment, must
  use protected GitHub Environments with branch restrictions and maintainer
  approval before secrets are exposed.

## Public Repository Operations

For public repository operation:

- Run full Git-history secret scans before visibility or release-boundary
  changes and rotate any credential that ever appears in history, even if it has
  is absent from the current tree.
- Keep GitHub Secret Scanning and Push Protection enabled.
- Keep non-GitHub Actions explicitly allowed by policy instead of relying on
  unrestricted workflow sources.
- Pin every remote workflow Action to an upstream-verified full commit SHA.
  Keep the approved SHA and human-readable upstream version in
  [`config/ci-actions-lock.json`](config/ci-actions-lock.json), retain the
  version comment beside each `uses:` reference, and run
  `uv run --no-sync python scripts/check_ci_action_pins.py` when changing the lock.
  New, mismatched, tag-based, expression-based, or stale Action references fail
  the repository CI gate.
- Verify the exact uv installer archive against the official per-platform
  SHA256 authority in [`config/uv-toolchain.toml`](config/uv-toolchain.toml);
  a version string without a matching checksum is not an immutable tool input.
- Scope environment secrets such as `HF_TOKEN` to protected environments, not
  broad repository secrets.
- Keep external pull request workflows untrusted: no deployment, publishing,
  model-provider, or infrastructure secrets may be exposed to PR code.

## Public Platform Auth Model

- Browser users authenticate through HttpOnly SameSite session cookies backed by
  hashed `user_sessions` rows. In production, those cookies must be Secure by
  running the service with `LOOM_ENV=production` behind HTTPS.
- Unsafe browser-session requests must include the configured CSRF header. The
  CSRF token is returned by auth responses and held in frontend memory, but
  only its hash is stored server-side; the session cookie itself is never
  readable by frontend JavaScript.
- The singleton admin secret is an operator/bootstrap credential, not a normal
  browser identity. Public users use persisted user sessions and team
  memberships.
- Staging browser acceptance has one narrow exception: a singleton admin bearer
  may call `/api/v1/auth/staging-admin-browser-session` only when
  `LOOM_ENV=staging`, targeting an existing enabled platform-admin owner of the
  enabled `admin` team. The audited exchange changes no authority, issues a
  distinct Secure HttpOnly SameSite=Lax cookie for at most 900 seconds, cannot
  refresh, and is invalid outside staging. The cookie may only read product
  state or call the exact logout endpoint; every other unsafe method fails
  closed before route handling. The exchange does not update the target's normal
  login timestamp and also returns and audits the immutable service build SHA.
  It is not a production or normal user login mechanism. Ephemeral kind CI
  renders the service as `development` and proves this endpoint remains `404`;
  only a candidate-bound brokered protected-staging rollout may exercise the
  positive exchange.
- Invite onboarding uses one-time revealed `loom_invite_...` links. The
  database stores only invite hashes and safe prefixes; invite list, lookup,
  logs, and audit metadata must never include raw invite codes. Accepting an
  invite creates or reuses the browser user, creates the team membership, and
  sets the HttpOnly session cookie.
- CLI and automation use named, scoped team API tokens with raw `loom_api_...`
  values revealed only on mint/rotate. Token lists, logs, audit metadata, and
  diagnostics must show only safe names, scopes, and hash prefixes.
- Team is the execution, cost, provider credential, member, API-token, and
  quota-policy boundary. Browser `viewer`, `member`,
  and `owner` roles are enforced server-side, with owner-only management of team
  API tokens and provider connections.

## Public Web Origin Policy

- The production web Nginx configuration is authoritative for CSP,
  `X-Content-Type-Options`, `Referrer-Policy`, and `Permissions-Policy` on SPA
  shells, runtime config, static assets, canonical redirects, and web-origin
  errors. Keep `always` semantics and the shared include in cache-header
  locations; a location-local `add_header` otherwise disables inheritance.
- CSP remains self-only for scripts, styles, fonts, connections, forms, and
  manifests; objects and framing are denied. Only tested `data:`/`blob:`
  image, media, worker, and download behavior may widen the relevant directive.
  Do not add wildcards, unsafe script execution, or a third-party font origin.
- ingress-nginx owns HSTS at the TLS boundary. Release evidence validates the
  combined exact singleton headers instead of duplicating the web policy in
  tenant-controlled Ingress snippets.
- `scripts/ops/frontend_security_headers.py` emits only requested URLs,
  statuses, and redacted pass/fail errors. Browser smoke must also remain free
  of CSP console violations.

## Shared Artifact Boundary

- Existing batch, trial, trajectory, ATIF, and artifact routes remain scoped to
  the owner team unless the caller has platform-admin authority.
- Org-wide completed-result sharing must go through explicit Run Library
  visibility/share-state checks. Do not implement cross-team downloads by
  weakening `require_team_or_admin()` on execution/control routes.
- Safe shared artifacts must download through authenticated service-proxied
  Run Library routes, never raw object-store URLs. Reuse must record source
  provenance and must not copy source-team provider credentials.
- Unsafe, secret-like, or policy-blocked artifacts must be denied to other teams
  and surfaced only with a safe blocked reason. Redaction/leak tests for shared
  artifacts are part of the staging security gate.

## Current Security Contracts

- Accounts, teams, sessions, roles, CSRF, and operator authentication:
  [`docs/architecture/auth-and-teams.md`](docs/architecture/auth-and-teams.md)
  and
  [`docs/architecture/auth-threat-model.md`](docs/architecture/auth-threat-model.md).
- Sandbox credentials and network egress:
  [`docs/architecture/sandbox-isolation.md`](docs/architecture/sandbox-isolation.md).
- Artifact visibility and reuse:
  [`docs/architecture/run-library.md`](docs/architecture/run-library.md).
- Staging retention and exact-object deletion:
  [`docs/architecture/staging-data-lifecycle.md`](docs/architecture/staging-data-lifecycle.md).
- Admin mutations covered by service routes write safe attribution to
  `admin_audit_events`; the SPA exposes the corresponding operator audit view.
