# Auth And Team Registration Threat Model

> Archived on 2026-08-11. This version includes implementation gates and
> issue-era residual-risk notes. See the current
> [`auth-threat-model.md`](../../../docs/architecture/auth-threat-model.md).

This document is the security baseline for the auth and team-registration
redesign. It is a threat model, not an implementation spec. The current public
path uses no-email username/password accounts, admin-approved setup/reset
links, HttpOnly browser sessions, and user-owned API tokens.

## Scope

In scope:

- Admin authentication and admin action attribution.
- Username account registration, password setup/reset approval, token issuance,
  disablement, and rotation.
- Team-scoped access to provider connections, model credentials, runs,
  trajectories, evaluator feedback, artifacts, and usage.
- Service, control-plane, gateway, SPA, and CLI paths that accept long-lived
  user or operator credentials.

Out of scope for this document:

- Per-step sandbox JWTs and gateway egress isolation. Those are covered by
  `cluster-deploy.md` and provider-gateway issues.
- SSO/OIDC/SAML and automated email delivery. Loom does not depend on a
  platform mailbox for account setup or password reset.

## Attacker Classes

| Attacker | Capabilities | Primary concern |
| --- | --- | --- |
| Curious tenant | Has a valid user-owned API token and normal platform access | Cross-team run, artifact, provider, usage, and progress visibility |
| Malicious tenant | Has a valid user-owned API token and intentionally submits adversarial payloads | Privilege escalation, endpoint probing, resource abuse, artifact poisoning |
| Legacy automation holder | Has a valid unowned legacy team token | Unattributed submissions, wrong-team CLI configs, compatibility-scope creep |
| Drive-by external | No valid token; can reach public SPA/API endpoints | Token guessing, registration spam, brute force, unauthenticated metadata leaks |
| Targeted external | Can phish, scrape logs, exploit web/API bugs, and replay leaked tokens | Admin takeover, provider-secret theft, durable data exfiltration |
| Supply-chain or CI attacker | Can influence dependency, workflow, image, or PR code paths | Secret exfiltration from CI/deploy, malicious release artifacts |
| Insider operator | Has infrastructure or admin access beyond normal tenant privileges | Unattributed admin mutations, silent team-token minting, audit tampering |

## Assets

- Admin authority: the ability to approve teams, mint/revoke tokens, rotate
  provider secrets, manage execution policy, and see cross-team operational
  state.
- User-owned API tokens and one-time setup/reset links.
- Provider API keys, base URLs, private endpoint decisions, and model lists.
- Run inputs, trajectories, evaluator feedback, final workspaces, artifacts,
  and logs.
- PM/operator dashboards: team progress, failures, usage, queue depth, and cost.
- Audit log integrity for admin and security-sensitive actions.
- Deployment secrets: signing keys, secret-store master keys, database and
  object-store credentials, CI/deploy environment secrets.

## Trust Boundaries

| Boundary | Trusted side | Untrusted side | Required invariant |
| --- | --- | --- | --- |
| Public browser/API | Loom service auth layer | Internet clients | No privileged route executes before auth and scope checks |
| Admin secret file | Service/control-plane process memory | Filesystem readers, logs, env dumps | Admin secret is file-backed, mode-restricted, never echoed or logged |
| User-owned API token | Auth middleware | Tenant clients | Token identifies exactly one team and one submitting user unless admin scope is proven |
| Provider connection | Secret store + gateway | SPA, CLI, sandbox, artifacts | Raw provider keys never leave trusted service/gateway code |
| Team data rows | Repository/query layer | Other teams | Cross-team access is denied or hidden consistently |
| CI/deploy | Protected environment | Public PR code | PR code never receives production, provider, or publishing secrets |
| Audit log | Append-only service path | Admin/operator UI | Admin actions produce durable attribution records before success is returned |

## Blast Radius

| Compromise | Expected blast radius after #10 | Must not allow |
| --- | --- | --- |
| One user-owned API token leaked | That user's allowed submissions and that token's team-scoped reads/actions | Other teams' data, admin actions, provider secrets in plaintext, or attribution to another user |
| Setup/reset link leaked before use | Password setup/reset for the target user until expiry or approval revocation | Email/account enumeration, reuse after consumption, or access to other accounts |
| Admin secret leaked | Full platform administration until rotation | Silent persistence without audit trail after rotation |
| Staging acceptance cookie leaked | At most the remaining fixed 900-second platform-admin session in staging | Refresh, production use, authority repair, or recovery of the singleton bearer |
| Provider API key leaked | The connected upstream provider account until provider-side revocation | Leakage through sandbox env, logs, artifacts, or normal API responses |
| SPA XSS | Can read the in-memory CSRF token and make in-origin requests as the signed-in user until session expiry/logout | HttpOnly session-cookie theft, admin secret file access, cross-team access outside the user's role, or server-side provider secret exfiltration |
| CI workflow compromise | Read-only repo data for ordinary PRs | Deployment/provider secrets or package publishing credentials |
| Database read compromise | Token hashes, metadata, run/artifact rows | Admin plaintext secret or provider plaintext API keys |

## Required Controls For The #10 Design

- **Singleton admin secret:** production admin auth comes from a high-entropy
  process-readable secret file or mounted secret, not database admin-token rows.
  The service derives an in-memory hash and compares with `hmac.compare_digest`.
- **No secret stdout:** admin and team credential generation must print paths,
  IDs, or one-time handles, not raw long-lived secrets, except when an operator
  explicitly invokes a one-time reveal/download flow.
- **Admin actor attribution:** every admin mutation records an `admin_audit`
  entry with action, target, timestamp, request metadata, and the authenticated
  admin user where available. Legacy singleton-admin routes may still require
  an explicit actor string for forensics; the actor string is not
  authentication.
- **Default-closed account registration:** public registration creates a
  pending username request for an existing team. It does not collect email and
  cannot create a new team.
- **Admin-approved setup/reset links:** account approval creates a one-time
  setup link. Forgot-password creates a pending reset request, and admin
  approval creates a one-time reset link. Unknown reset usernames receive the
  same public response as known users.
- **User-owned tokens:** API tokens created by browser users are scoped to one
  team and preserve `created_by_user_id`. Token-authenticated submissions carry
  both team and submitting-user identity into batch/trial rows.
- **Browser sessions:** public SPA users authenticate with HttpOnly SameSite
  cookies backed by hashed `user_sessions` rows. Unsafe browser-session
  mutations require a CSRF header matching the server-side session CSRF hash.
  The SPA receives CSRF tokens from auth responses, keeps them in memory, and
  no longer stores normal bearer-token login state in `localStorage`.
- **Staging admin browser exchange:** only a singleton admin bearer in
  `LOOM_ENV=staging` may exchange into an already-authorized platform-admin
  user. The endpoint requires safe actor/request attribution, changes no
  authority, commits a safe audit row with session creation, sets a distinct
  Secure HttpOnly SameSite=Lax cookie for at most 900 seconds, cannot refresh,
  and is hidden in every other environment.
- **Hard admin rotation:** rotation invalidates the old admin secret immediately
  after the service reload/restart boundary. Operators must not rely on a long
  deprecation window.
- **Production startup guard:** production service startup fails if the admin
  secret file is absent, unreadable, world-readable, or configured through an
  unsafe broad environment injection path.
- **Provider error redaction:** gateway and provider-connection upstream-error
  handling must redact provider API keys, `Authorization: Bearer` values,
  signed object-store URLs, secret refs, cookies, CSRF values, invite/setup/
  reset/API token shapes, and internal service URLs before writing logs or returning
  diagnostics to callers.
- **Audit and frontend redaction:** admin audit metadata rejects secret-like
  values before persistence. SPA error states and raw diagnostics panels render
  redacted copies of JSON/text payloads instead of raw provider keys, signed
  URLs, tokens, or internal endpoints.
- **Artifact share-state scanning:** collected artifacts receive a conservative
  `share_status` decision. `shared` artifacts can become eligible for org-wide
  Run Library download/reuse; `blocked` artifacts retain owner-team diagnostics
  but expose only a safe `blocked_reason` outside the owner boundary.
- **Public repo safety:** external PRs and ordinary CI runs stay read-only and
  do not receive deployment, provider, object-store, or publishing secrets.

## Design Gates Before Implementation

1. Implementation spec describes the admin secret file format, file-mode checks,
   startup behavior, and rotation command behavior.
2. Account-registration spec describes pending/approval modes, setup/reset link
   delivery behavior, and rate-limit boundaries.
3. Migration plan explicitly deletes or disables database admin-token rows and
   documents the operator-visible startup warning.
4. Tests cover missing/unsafe admin secret files, constant-time hash compare
   path, account registration closed mode, admin approval, password reset,
   audit-log writes, and admin rotation invalidation.
5. Operator docs explain first-run setup, rotation, incident response for leaked
   admin/team/provider secrets, and rollback behavior.

## Current Residual Risk

Admin authority is now file-backed rather than DB-backed, and the development
stack uses the same singleton-admin path as production. Browser users now use
HttpOnly session cookies, CSRF protection, username/password login, and
persisted team memberships instead of pasted bearer tokens. Scoped CLI/API
tokens are hash-only at rest with one-time raw reveal and preserve submitting
user attribution. Run Library sharing now uses explicit visibility/share-state
checks and `username / team` owner labels instead of weakening execution
routes. Remaining staging auth risk is concentrated in final staging smoke,
rate-limit tuning for public request endpoints, and operational incident
practice before broad external exposure. The staging-only admin browser
exchange intentionally grants the target's existing platform-admin authority,
so acceptance automation must keep the singleton bearer out of argv and
artifacts, retain only a sanitized report, and prove logout plus `/auth/me`
`401` before it reports success.
