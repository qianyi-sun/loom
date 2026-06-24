# Auth And Team Registration Implementation Spec

This spec turns the [auth threat model](auth-threat-model.md) into the shipped
implementation baseline. The original issue #10 scope added singleton admin
secret verification, default-closed team registration, admin review, audit
events, operator secret commands, and DB-admin removal. Issue #326 extends that
baseline with browser users, team memberships, role-derived permissions,
current-team context, HttpOnly session cookies, and CSRF protection for the
invite-only public platform track.

## Goals

- Replace database-backed admin credentials with one singleton admin secret
  loaded from a process-readable file or mounted Kubernetes Secret.
- Keep team and worker tokens database-backed, but prevent team tokens from
  creating admin credentials or escaping their team scope.
- Add a default-closed access-request path with admin approval into a fixed
  internal team.
- Add persisted browser users, team memberships, session rows, and current-team
  context for the SPA.
- Enforce viewer/member/owner/platform-admin semantics server-side while
  preserving existing team bearer tokens for CLI/backward compatibility.
- Protect browser-session mutations with CSRF validation.
- Record durable audit events for admin mutations before returning success.
- Provide first-run and rotation operator commands that do not print secrets by
  default.
- Keep the current development path usable while making production startup fail
  closed when required admin-secret material is absent or unsafe.

## Non-Goals

- SSO, SAML, OIDC, and automated email delivery. Invite links are revealed once
  to an admin/team owner on create or resend; a later mailer can deliver the
  same link without changing the database contract.
- Scoped public CLI/API tokens and org-wide completed-result sharing are
  documented in their own API/UX specs. This document keeps the auth,
  membership, invite, and CSRF contract focused.
- Run Library sharing and artifact reuse must use explicit visibility/
  share-state checks instead of weakening existing team-scoped execution/
  control routes.
- Changing step-scoped sandbox JWTs or gateway provider egress controls.

## Target Flow

```mermaid
flowchart TD
  AdminCLI["Operator CLI"] --> SecretFile["Admin secret file"]
  SecretFile --> ServiceStartup["loom_service startup"]
  ServiceStartup --> AdminAuth["In-memory admin hash"]
  Researcher["Researcher"] --> Register["POST /api/v1/teams/register"]
  Register --> Pending["pending_team_registrations row"]
  AdminSPA["Admin SPA"] --> Teams["Create/update fixed internal teams"]
  AdminSPA --> Approve["Approve or reject request"]
  Teams --> Approve
  Approve --> Audit["admin_audit_events row"]
  Approve --> Team["existing teams row"]
  Approve --> Invite["team_invites hash row"]
  Invite --> Reveal["One-time invite link reveal"]
  Reveal --> Accept["Invite acceptance"]
  Accept --> Membership["users + team_memberships + session"]
```

## Principal Model

| Principal | Auth source | Scope | Notes |
| --- | --- | --- | --- |
| Admin | File-backed singleton secret | Global administration | Compared in memory with `hmac.compare_digest`; not stored in `tokens`. |
| Browser user | `user_sessions` row plus `loom_session` cookie | Current team role | Normal SPA identity. The raw session secret is HttpOnly; unsafe requests must send the CSRF header. |
| Team | `tokens` row with `type='team'` | One team | Can submit and read own resources according to scopes. |
| Worker | `tokens` row with `type='worker'` | Internal worker APIs | Not accepted by `loom_service` user/admin routes. |
| Step session | `loom_step_<jwt>` | One trial step | Gateway-only runtime token, out of scope for #10. |

`AuthContext.type == "admin"` continues to represent admin authority for route
code, but the admin branch is derived from the singleton secret rather than a
database `Token` row. Browser users use `AuthContext.type == "user"`; a
`platform_admin` user role gets the same cross-team inspection/admin wildcard
without making the singleton admin secret a browser identity.

### Browser User Roles

| Role | Scopes | Intended use |
| --- | --- | --- |
| `viewer` | `read:own` | Read-only access to the current team's execution/control resources. |
| `member` | `read:own`, `submit` | Submit team work without managing credentials or tokens. |
| `owner` | `read:own`, `submit`, `tokens:manage`, `providers:manage`, `team:manage` | Manage team API tokens, provider connections, and team-admin surfaces exposed by the service. |
| `platform_admin` | `admin:platform` | Global operator/admin user for inspection and incident response. |

The team boundary remains the execution, cost attribution, credential, member,
and API-token administration boundary. Completed run metadata and safe artifacts
are organization-visible only through the Run Library after explicit
visibility/share-state checks pass.

## Admin Secret File

### File Location

- Operator command default: `~/.config/loom/secrets.toml`.
- Local dev stack default: `loom service up` creates
  `.loom/admin/secrets.toml` and dev compose mounts it read-only into each
  admin-aware service.
- Production: `LOOM_SVC_ADMIN_SECRET_FILE`, `LOOM_CP_ADMIN_SECRET_FILE`, and
  `LOOM_GW_ADMIN_SECRET_FILE` are required and must point to the same mounted
  secret file, not an `envFrom` value.
- `loom_control_plane` reads the same secret for CP-internal admin routes such
  as worker-token bootstrap; the LLM Gateway reads it for rate-card admin
  mutation routes.

### File Shape

```toml
[admin]
token = "loom_admin_<256-bit-url-safe-secret>"
created_at = "2026-06-16T00:00:00Z"
version = 1
```

The file contains the admin bearer credential, so file permissions are part of
the security boundary. Startup must reject files that are group- or
world-readable. On POSIX systems the expected mode is `0600`; on filesystems
that do not expose POSIX modes, production must fail and development may emit a
loud warning only when `LOOM_ENV != production`.

### Startup Behavior

1. Load the admin secret file during `loom_service` lifespan startup.
2. Validate token prefix, entropy length, TOML shape, and file mode.
3. Derive `sha256(token)` in memory and discard the raw token string as soon as
   settings initialization completes.
4. Store an `AdminSecretVerifier` object on `app.state`.
5. `verify_bearer_token(...)` checks this verifier before consulting the
   database-backed token table.
6. `hmac.compare_digest(candidate_hash, admin_hash)` is the only comparison
   path.

If `LOOM_ENV=production`, startup fails when the file is missing, unreadable,
malformed, empty, low entropy, or has unsafe permissions. Development stacks use
the same singleton-admin path by default through `loom service up`; DB-backed
admin rows are not accepted as a fallback.

## Database Changes

Issue #10 adds two admin/onboarding tables and a cleanup migration that revokes
legacy DB-backed admin authority.

### `pending_team_registrations`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Generated by service. |
| `name` | text | Requester/display label. It is not used as the team key. |
| `contact_email` | text | Requester contact. |
| `status` | text | `pending`, `approved`, `rejected`, or `expired`. |
| `requested_at` | timestamptz | Server timestamp. |
| `reviewed_at` | timestamptz nullable | Set on approve/reject. |
| `reviewed_by_actor` | text nullable | `X-Loom-Admin-Actor` value. |
| `approved_team_id` | UUID nullable | Set when approved. |
| `source_ip_hash` | text nullable | Optional abuse/debug signal, never raw IP in v0. |
| `user_agent_hash` | text nullable | Optional abuse/debug signal. |
| `metadata` | JSONB | Future-safe request context. |

Create a partial unique index on lower-case contact email for rows whose status
is `pending` or `approved`, so one requester cannot leave multiple active
approval targets.

### `admin_audit_events`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Generated by service. |
| `created_at` | timestamptz | Server timestamp. |
| `actor` | text | Required `X-Loom-Admin-Actor` header. |
| `action` | text | Stable action id such as `team_registration.approve`. |
| `target_type` | text | `team_registration`, `team`, `token`, `rate_card`, etc. |
| `target_id` | text | UUID or stable identifier. |
| `request_id` | text nullable | From request middleware when available. |
| `source_ip_hash` | text nullable | Hashed request source, not raw IP. |
| `user_agent_hash` | text nullable | Hashed UA string. |
| `metadata` | JSONB | Safe, summary-only context. |

Admin mutations must write the audit row in the same database transaction as the
mutation when both rows live in the same database. If the audit write fails, the
mutation fails.

Issue #326 adds the browser identity tables:

### `users`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Stable user id. |
| `email` | text | Unique case-insensitive identity key. |
| `display_name` | text nullable | UI label when available. |
| `is_platform_admin` | bool | Grants platform-admin session semantics. |
| `created_at`, `updated_at` | timestamptz | Server timestamps. |

### `team_memberships`

| Column | Type | Notes |
| --- | --- | --- |
| `team_id` | UUID | Team boundary for execution, cost attribution, credentials, members, and tokens. |
| `user_id` | UUID | User assigned to the team. |
| `role` | text | `owner`, `member`, or `viewer`. |
| `created_at`, `updated_at` | timestamptz | Server timestamps. |

The primary key is `(team_id, user_id)`. Normal users must have a membership
before selecting a current team. Platform-admin users can inspect across teams
but should still choose a current team for ordinary submission flows.

### `team_invites`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Stable invite id. |
| `team_id` | UUID | Team that will receive the membership. |
| `email` | text | Intended invite email, normalized lower-case. |
| `allowed_domain` | text nullable | Optional explicit domain policy for multi-use invites. |
| `role` | text | `owner`, `member`, or `viewer`. |
| `status` | text | `pending`, `accepted`, `revoked`, or `expired`. |
| `code_hash` | bytea | SHA-256 of the raw `loom_invite_...` code. |
| `code_prefix` | text | Short safe prefix for display and audit. |
| `max_uses`, `accepted_uses` | integer | Use limit and current use count. |
| `created_by_actor`, `created_by_user_id` | text / UUID | Audit attribution. |
| `created_at`, `expires_at`, `last_sent_at` | timestamptz | Lifecycle timestamps. |
| `accepted_at`, `revoked_at`, `revoked_reason` | nullable | Terminal lifecycle metadata. |

The raw invite code/link is returned only from create/resend responses. List,
lookup, audit, and database rows expose only the safe `code_prefix`.

### `user_sessions`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Session row id. |
| `user_id` | UUID | Browser user. |
| `session_hash` | bytea | SHA-256 of the raw `loom_session` value. |
| `csrf_hash` | bytea | SHA-256 of the current CSRF token. |
| `current_team_id` | UUID nullable | Active team context. |
| `expires_at`, `revoked_at` | timestamptz | Expiry and logout state. |
| `created_at`, `updated_at` | timestamptz | Server timestamps. |

Only hashed session and CSRF secrets are persisted. The raw session secret is
returned only as an HttpOnly cookie; the raw CSRF secret is returned in auth
JSON responses and kept in SPA memory so same-site JavaScript can prove
mutation intent without storing that secret in a browser-readable cookie.

### `login_challenges`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID primary key | Challenge row id. |
| `user_id` | UUID | Existing user. |
| `challenge_hash` | bytea | SHA-256 of the one-time login token. |
| `expires_at`, `consumed_at` | timestamptz | Challenge validity. |
| `created_at` | timestamptz | Server timestamp. |

Unknown-email login starts return the same public response as known-email starts
and do not reveal whether a user exists. Until email delivery lands, development
and tests may enable a setting that returns the raw login token in the response.

## API Contract

### Public Registration

`POST /api/v1/teams/register`

Request:

```json
{
  "name": "Mark Li",
  "contact_email": "owner@example.com"
}
```

Closed mode is the default: create a pending row and return `202 Accepted` with
the registration id and status. Open mode is explicitly enabled by
`LOOM_SVC_TEAM_REGISTRATION_OPEN=true`; open mode still requires rate limiting
and a challenge hook before it can auto-approve access. Until that challenge
hook ships, the backend accepts the setting but returns an explicit
`501` for open-registration attempts instead of silently issuing credentials.

### Admin Review

`GET /api/v1/admin/teams`

Requires admin auth. Returns fixed internal teams with quota, token metadata,
and browser-user memberships so admins can choose the right destination during
approval.

`POST /api/v1/admin/teams`

Requires admin auth plus `X-Loom-Admin-Actor`. Creates a fixed internal team
and its default `team_quotas` row. Team names are unique case-insensitively.

`PATCH /api/v1/admin/teams/{team_id}`

Requires admin auth plus `X-Loom-Admin-Actor`. Updates the team display name.
This is for operator cleanup and naming consistency; it does not move existing
runs or memberships.

`GET /api/v1/admin/team-registrations?status=pending`

Returns pending registration summaries. Requires admin auth.

`POST /api/v1/admin/team-registrations/{id}/approve`

Requires admin auth plus `X-Loom-Admin-Actor`. The request body selects the
existing team and membership role:

```json
{
  "team_id": "00000000-0000-0000-0000-000000000000",
  "role": "member"
}
```

Approval creates one invite for the registration contact in that team and role.
The approval response returns the raw invite code/link exactly once; the
database stores only the invite code hash and safe prefix. The contact accepts
the invite link to create or reuse a browser user, create the selected
membership, and receive an HttpOnly browser session. Loom does not email the
link in public beta; the admin copies and shares it manually. If the admin UI
loses the response, use invite resend to rotate and reveal a replacement link.

`POST /api/v1/admin/team-registrations/{id}/reject`

Requires admin auth plus `X-Loom-Admin-Actor`. Marks the request rejected and
records review metadata on the registration row. It does not delete the request
row.

### Invites

`POST /api/v1/invites`

Requires a platform admin or team owner. Creates an invite with `email`,
`team_id` (optional for a team owner using their current team), `role`,
`expires_in_days`, optional `max_uses`, and optional `allowed_domain`. Returns
the invite summary plus `invite_code` and `invite_link` exactly once.

`GET /api/v1/invites?team_id=<team>&status=pending`

Lists invite summaries for team owners/admins. Responses include email, role,
status, counts, timestamps, and `code_prefix`, never raw invite code or link.

`GET /api/v1/invites/lookup?code=loom_invite_...`

Public safe lookup for the SPA invite page. It returns only team name, role,
status, and code prefix so expired/revoked/used states can be explained without
leaking membership data.

`POST /api/v1/invites/accept`

Accepts `{ "code": "loom_invite_...", "email": "user@example.com" }`.
Acceptance succeeds only when the email matches the intended email or an
explicit invite `allowed_domain`. It creates the user if needed, creates the
team membership, records an `invite.accept` audit event with safe metadata, sets
the HttpOnly session cookie, and returns the `/auth/me` shape with a CSRF token.

`POST /api/v1/invites/{id}/revoke`

Revokes a pending/expired invite. The raw code remains unrecoverable.

`POST /api/v1/invites/{id}/resend`

Rotates the invite code hash, extends expiry, and reveals a new raw link exactly
once. The old link fails with `invalid invite`.

### Admin Audit

`GET /api/v1/admin/audit-events?limit=50&cursor=<event-id>`

Returns recent admin audit rows with cursor pagination. The current backend uses
the last event id as the next cursor. Team users never see this endpoint. Raw
tokens, provider secrets, request bodies, and artifact paths must not appear in
audit metadata.

### Browser Session Auth

`POST /api/v1/auth/login/start`

Accepts `{ "email": "user@example.com" }` and always returns
`{ "status": "sent" }`. When `LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN=true`, the
response also includes `login_token` for local development and automated tests.

`POST /api/v1/auth/login/complete`

Accepts `{ "token": "loom_login_..." }`, consumes the one-time challenge, sets
the HttpOnly `loom_session` cookie, and returns the same shape as `/auth/me`
plus `csrf_token`.

`GET /api/v1/auth/me`

Returns the browser user, available teams, current team, current role, scopes,
platform-admin flag, and a freshly rotated `csrf_token`. Bearer tokens are not
accepted as browser identity for this route.

`POST /api/v1/auth/team`

Switches the current team when the user is a member of the target team. This is
a mutating browser-session route and must include the configured CSRF header.
The response includes a freshly rotated `csrf_token`.

`POST /api/v1/auth/refresh` and `POST /api/v1/auth/logout`

Refresh extends the session, rotates both the session cookie and CSRF state,
and returns a fresh `csrf_token`. Logout revokes the session row and clears the
session cookie.

Every unsafe method authenticated by a browser session requires the configured
CSRF header to match the server-side CSRF hash for that session. Bearer token
requests keep their existing Authorization-header behavior and are not subject
to browser CSRF.

## Token Route Changes

`POST /api/v1/tokens` rejects `type='admin'` and any requested `admin:*` scope
for every caller. Admin credentials are created and rotated only by local
operator commands and mounted secret files.

Team API-token behavior stays database-backed:

- tokens are named and reveal a raw `loom_api_...` value only at creation or
  rotation time;
- team callers can mint only same-team tokens and cannot grant scopes they do
  not hold;
- supported public team scopes include `read:own`, `submit`,
  `providers:manage`, and `tokens:manage`;
- admin callers can mint team tokens for approved teams;
- browser users must have the owner-derived `tokens:manage` scope before they
  can mint, rotate, revoke, or list team API tokens through the SPA/API;
- admin callers must send `X-Loom-Admin-Actor` for token mint/rotate/revoke so
  the action can be written to `admin_audit_events`;
- token list/detail responses reveal only names, scopes, metadata, last-used
  timestamps, and hash prefixes.

Provider connection creation, key rotation, provider tests, model refresh,
manual model insertion, hide/unhide, and delete are similarly owner-gated for
browser users and bearer API tokens through `providers:manage`.

Migration `0024_revoke_db_admin_tokens` revokes active legacy
`Token.type='admin'` rows and any DB token carrying `admin:*` scopes. The auth
layer also ignores such rows if they remain in an older database or are inserted
manually.

## Operator Commands

Add commands under `loom service`:

- `loom service init-admin --secret-file PATH`: generate a high-entropy admin
  token, write `secrets.toml` with mode `0600`, and print the file path plus
  next-step instructions. It must not print the raw token by default.
- `loom service reveal-admin --secret-file PATH`: explicitly reveal the raw
  admin token for local operator use. Require an interactive confirmation unless
  `--yes` is supplied.
- `loom service rotate-admin --secret-file PATH`: replace the admin token,
  atomically rewrite the file, and tell the operator which service deployments
  must restart. Old admin tokens become invalid after restart.

These commands operate on local files only. Kubernetes rotation remains an
operator workflow: update the mounted Secret, restart affected deployments, and
verify old tokens fail.

## SPA Contract

The production SPA uses browser user sessions, not pasted bearer tokens in
localStorage. On load, the SPA calls `/api/v1/auth/me`; a `401` means signed
out, and any later `401` clears cached user/team data. The shared API client
always sends `credentials: "include"` and copies the in-memory CSRF token from
auth responses into the configured header for unsafe methods.

Settings is the session and team-settings surface:

- signed-out users open an invite link, submit an access request, or paste a
  one-time login code already issued by an operator or local development
  environment;
- signed-out users also see CLI setup guidance that points them to scoped team
  API tokens after they have joined a team;
- signed-in users see their user, current team, role, platform-admin flag, team
  list, joined browser users, and role-derived capabilities;
- team switching clears cached queries because the current-team context changes
  authorization and result scope;
- owner users can navigate to Team access for invites and scoped CLI/API token
  lifecycle, plus provider setup; platform admins also manage fixed internal
  teams and approve pending access requests into a selected team/role;
  member/viewer users do not get owner-only UI affordances and still receive
  server-side 403s for forbidden mutations.

The invite acceptance page at `/invites/accept?code=...` performs a safe lookup
that displays team name, role, invite status, and code prefix. Pending invites
show an email field and accept action; expired, revoked, and already-used
invites show human-readable terminal states without exposing raw membership
data.

The legacy Admin Access page and singleton admin secret remain operator tools.
They are not normal browser identity for public users.

## Delivery Slices

1. **Spec and docs:** this document plus links from docs index, service-mode,
   operator runbook, and security docs.
2. **Admin secret verifier:** settings, file validation, in-memory verifier,
   `loom_service`, Control Plane, and LLM Gateway admin-route auth integration,
   Kubernetes secret mounts, and production startup guards.
3. **Team registration API:** migration for `pending_team_registrations`, public
   register endpoint, admin list/approve/reject endpoint, and token mint once.
4. **Audit events:** migration for `admin_audit_events`, audit writer helper,
   admin mutation hooks for registration approval/rejection and service token
   mint/revoke, plus the backend audit listing endpoint and SPA audit table.
5. **Operator CLI and runbook:** `init-admin`, `reveal-admin`, `rotate-admin`,
   production runbook update, and rotation smoke.
6. **DB-admin removal:** reject admin token/admin-scope creation, revoke
   existing DB admin rows in migration, remove the fallback, and keep seed data
   on team/worker tokens only.
7. **User sessions/RBAC:** `users`, `team_memberships`, `user_sessions`, and
   `login_challenges`, plus `/auth/*` routes, CSRF checks, current-team context,
   SPA session UX, and route-level owner gates for token/provider management.

## Test Requirements

- Unit tests for admin secret parsing, mode checks, entropy checks, and
  constant-time hash compare behavior.
- Integration tests proving production startup fails when the admin secret file
  is missing, malformed, or permission-unsafe.
- Auth tests proving singleton admin succeeds, wrong admin token fails, team
  tokens still work, and DB admin rows are rejected.
- User-session tests for login start/complete, `/auth/me`, logout, refresh,
  team switch, unknown-email non-disclosure, CSRF denial, role-derived scopes,
  and cross-team denial.
- API tests for closed-mode registration, duplicate pending contact emails,
  approval into an existing team/role, reject, one-time invite reveal, invite
  acceptance, and membership after approval.
- Invite API tests for create/list/revoke/resend/accept, expired and duplicate
  acceptance, explicit domain policy, and raw-code redaction from list/audit.
- Audit tests proving admin mutations fail if audit insertion fails, team users
  cannot read the audit endpoint, admin token mutations require an actor, and
  audit metadata excludes raw secrets.
- CLI tests proving `init-admin` and `rotate-admin` do not print raw token values
  by default and write files with mode `0600`.

## Rollout Rules

- Development compose uses `.loom/admin/secrets.toml`; seeded DB admin tokens
  are no longer created or accepted.
- Production deployment is blocked unless the same singleton admin secret is
  mounted into `loom_service`, Control Plane, and LLM Gateway.
- Backend audit is present for #10 registration review, invite create/revoke/
  resend/accept, and service token admin mutations; broader admin mutation
  coverage should be handled by follow-up issues instead of silently claiming
  complete platform-wide audit.
- Browser sessions must set HttpOnly, SameSite cookies. Production deployments
  must use Secure cookies by running with `LOOM_ENV=production` behind HTTPS.
- Existing batch/trial/artifact execution and control routes remain owner-team
  scoped. Org-wide completed-result sharing is explicit Run Library behavior
  with redaction/share-state enforcement.
