# Auth And Team Registration Threat Model

This document is the security baseline for the #10 auth and team-registration
redesign. It is a threat model, not an implementation spec. Current dev stacks
still use database-backed bearer tokens seeded by local tooling; production
work must not treat that model as sufficient.

## Scope

In scope:

- Admin authentication and admin action attribution.
- Team registration, approval, token issuance, disablement, and rotation.
- Team-scoped access to provider connections, model credentials, runs,
  trajectories, evaluator feedback, artifacts, usage, and quotas.
- Service, control-plane, gateway, SPA, and CLI paths that accept long-lived
  user or operator credentials.

Out of scope for this document:

- Per-step sandbox JWTs and gateway egress isolation. Those are covered by
  `cluster-deploy.md` and provider-gateway issues.
- SSO/OIDC/SAML and per-user RBAC inside a team. Those are later issues after
  the singleton-admin and team-registration base is stable.
- Email delivery automation for team credentials.

## Attacker Classes

| Attacker | Capabilities | Primary concern |
| --- | --- | --- |
| Curious tenant | Has a valid team token and normal platform access | Cross-team run, artifact, provider, usage, and progress visibility |
| Malicious tenant | Has a valid team token and intentionally submits adversarial payloads | Privilege escalation, endpoint probing, quota abuse, artifact poisoning |
| Drive-by external | No valid token; can reach public SPA/API endpoints | Token guessing, registration spam, brute force, unauthenticated metadata leaks |
| Targeted external | Can phish, scrape logs, exploit web/API bugs, and replay leaked tokens | Admin takeover, provider-secret theft, durable data exfiltration |
| Supply-chain or CI attacker | Can influence dependency, workflow, image, or PR code paths | Secret exfiltration from CI/deploy, malicious release artifacts |
| Insider operator | Has infrastructure or admin access beyond normal tenant privileges | Unattributed admin mutations, silent team-token minting, audit tampering |

## Assets

- Admin authority: the ability to approve teams, mint/revoke tokens, rotate
  provider secrets, change quotas, and see cross-team operational state.
- Team tokens and future one-time team credential delivery links.
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
| Team token | Auth middleware | Tenant clients | Token identifies exactly one team unless admin scope is proven |
| Provider connection | Secret store + gateway | SPA, CLI, sandbox, artifacts | Raw provider keys never leave trusted service/gateway code |
| Team data rows | Repository/query layer | Other teams | Cross-team access is denied or hidden consistently |
| CI/deploy | Protected environment | Public PR code | PR code never receives production, provider, or publishing secrets |
| Audit log | Append-only service path | Admin/operator UI | Admin actions produce durable attribution records before success is returned |

## Blast Radius

| Compromise | Expected blast radius after #10 | Must not allow |
| --- | --- | --- |
| One team token leaked | That team's runs, artifacts, provider connections, usage, and submissions | Other teams' data, admin actions, provider secrets in plaintext |
| Admin secret leaked | Full platform administration until rotation | Silent persistence without audit trail after rotation |
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
  entry with action, target, timestamp, request metadata, and operator-provided
  actor string. The actor string is for forensics, not authentication.
- **Default-closed team registration:** public registration creates a pending
  request unless explicitly opened by deployment config. Open mode requires
  rate limiting and a challenge before a token is issued.
- **Team-scoped tokens:** approved teams receive high-entropy tokens scoped to
  one team. Team tokens cannot mint admin credentials or read other teams.
- **Browser sessions:** public SPA users authenticate with HttpOnly SameSite
  cookies backed by hashed `user_sessions` rows. Unsafe browser-session
  mutations require a CSRF header matching the server-side session CSRF hash.
  The SPA receives CSRF tokens from auth responses, keeps them in memory, and
  no longer stores normal bearer-token login state in `localStorage`.
- **Hard admin rotation:** rotation invalidates the old admin secret immediately
  after the service reload/restart boundary. Operators must not rely on a long
  deprecation window.
- **Production startup guard:** production service startup fails if the admin
  secret file is absent, unreadable, world-readable, or configured through an
  unsafe broad environment injection path.
- **Provider error redaction:** gateway upstream-error handling must redact
  provider API keys and `Authorization: Bearer` values before writing logs or
  returning diagnostics to callers.
- **Public repo safety:** external PRs and ordinary CI runs stay read-only and
  do not receive deployment, provider, object-store, or publishing secrets.

## Design Gates Before Implementation

1. Implementation spec describes the admin secret file format, file-mode checks,
   startup behavior, and rotation command behavior.
2. Team-registration spec describes pending/open modes, approval API shape,
   token delivery behavior, and rate-limit boundaries.
3. Migration plan explicitly deletes or disables database admin-token rows and
   documents the operator-visible startup warning.
4. Tests cover missing/unsafe admin secret files, constant-time hash compare
   path, team registration closed/open mode, admin approval, audit-log writes,
   and admin rotation invalidation.
5. Operator docs explain first-run setup, rotation, incident response for leaked
   admin/team/provider secrets, and rollback behavior.

## Current Residual Risk

Admin authority is now file-backed rather than DB-backed, and the development
stack uses the same singleton-admin path as production. The remaining auth risk
for this phase is the bearer-token SPA model: admin and team tokens are still
pasted into the browser and stored client-side for development/pilot use. A
production deployment should move that browser surface to the dedicated
cookie/SSO/RBAC follow-up before broad external exposure.
