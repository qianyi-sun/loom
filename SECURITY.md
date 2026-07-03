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
  since been removed.
- Keep GitHub Secret Scanning and Push Protection enabled.
- Keep non-GitHub Actions explicitly allowed by policy instead of relying on
  unrestricted workflow sources.
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
- Invite onboarding uses one-time revealed `loom_invite_...` links. The
  database stores only invite hashes and safe prefixes; invite list, lookup,
  logs, and audit metadata must never include raw invite codes. Accepting an
  invite creates or reuses the browser user, creates the team membership, and
  sets the HttpOnly session cookie.
- CLI and automation use named, scoped team API tokens with raw `loom_api_...`
  values revealed only on mint/rotate. Token lists, logs, audit metadata, and
  diagnostics must show only safe names, scopes, and hash prefixes.
- Team is the execution, cost, provider credential, member, and API-token
  boundary. Future quota/rate-limit enforcement, if added, must use an explicit
  product policy rather than implicit beta defaults. Browser `viewer`, `member`,
  and `owner` roles are enforced server-side, with owner-only management of team
  API tokens and provider connections.

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

## Platform Security Topics To Resolve

- Team and project-level access model; see
  [`docs/architecture/auth-threat-model.md`](docs/architecture/auth-threat-model.md)
  for the #10 admin/team-registration baseline and
  [`docs/architecture/auth-registration-spec.md`](docs/architecture/auth-registration-spec.md)
  for the user-session, membership, role, and CSRF contract.
- Secret injection into sandboxed jobs.
- Network egress policy for execution environments.
- Artifact retention, deletion, and org-wide Run Library share-state rules.
- Audit log requirements for PM, infra, and research users. The first auth
  backend audit surface now covers team-registration approve/reject,
  invite create/revoke/resend/accept, and `loom_service` token admin
  mint/revoke, with a SPA audit review table for operators. Broader admin
  mutation coverage remains follow-up work.
