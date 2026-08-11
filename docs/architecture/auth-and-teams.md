# Authentication and Teams

Loom supports username/password browser sessions, session-backed CLI access,
user-owned API tokens, legacy team tokens, worker tokens, and a singleton
operator secret. Teams are the boundary for submissions, provider
connections, usage attribution, members, and API-token administration.

## Identities

| Identity | Credential | Scope |
| --- | --- | --- |
| Browser or CLI user | Username/password followed by a `loom_session` cookie | Current team and membership role |
| User-owned API token | `loom_api_...` bearer token linked to a user | Its user and team |
| Legacy team token | Unowned `loom_team_...` bearer token | Its team, with compatibility restrictions |
| Platform administrator | User with `is_platform_admin=true` | Cross-team administrative access |
| Singleton operator | File-backed `loom_admin_...` bearer token | Operator and service administration |
| Worker | Database-backed worker bearer token | Internal worker APIs |

The singleton operator secret is not a browser account and is not stored in
the database token table. Browser and CLI users select one current team at a
time. Platform administrators still select a current team for ordinary work.

## Account onboarding and recovery

Public onboarding is closed unless an existing, enabled team has public
registration enabled.

1. The client lists eligible teams with `GET /api/v1/auth/public-teams`.
2. The user requests a globally unique, case-insensitive username and chooses
   one of those teams. Loom does not collect an email address.
3. An administrator approves or rejects the request. Approval creates the
   user and membership, then mints a one-time setup link.
4. The administrator reveals and shares the link manually. Loom stores only
   its hash and safe prefix.
5. The user sets a password and can then sign in from the web application or
   CLI.

Password recovery follows the same approval model: a user requests a reset by
username, an administrator approves it, and the administrator manually shares
the one-time reset link. Completing a reset revokes the user's active browser
sessions, user-owned API tokens, and other unconsumed password-reset links.

Invite-code routes remain available for compatibility. New onboarding should
use registration requests and setup links.

## Roles and teams

Membership roles are enforced by service authorization checks:

| Role | Capabilities |
| --- | --- |
| `viewer` | Read the current team's permitted resources |
| `member` | Viewer access plus submission |
| `owner` | Member access plus team, provider-connection, and API-token management |

Changing the current team requires a membership unless the user is a platform
administrator. A team can be disabled, have new submissions paused, or have
public registration enabled or disabled through the admin team routes.

## Sessions and CSRF

Successful password login creates a database-backed session and sets the raw
session secret in an HttpOnly `loom_session` cookie. Loom stores hashes of the
session and CSRF secrets. The CSRF secret is returned in authentication JSON
and kept in application or CLI memory; it is sent as `X-Loom-CSRF` on unsafe
session-authenticated requests.

`GET /api/v1/auth/me` rotates the CSRF token. Switching teams also rotates it.
Refreshing a normal session rotates both the session and CSRF secrets. Logout
revokes the session and clears the authentication cookies. Bearer-token
requests do not use the browser-session CSRF check.

The CLI stores its current session cookie and CSRF token in the selected Loom
profile. These values are credentials and must not be printed or committed.

## CLI workflow

The `loom auth` command group exposes the current account flow:

```text
loom auth register
loom auth setup-password
loom auth login
loom auth status
loom auth whoami
loom auth teams
loom auth forgot-password
loom auth reset-password
loom auth logout
```

`loom auth teams --server URL` lists teams that currently accept public
registration requests; it does not list a signed-in user's memberships or
switch the active team. Signed-in browser users change teams with the Settings
team switcher. The service also exposes the session-authenticated
`POST /api/v1/auth/team` route for clients that implement the session and CSRF
flow. See the [user guide](../user-guide.md#web-sessions-and-teams).

## Singleton operator secret

The operator credential is a TOML file containing an `[admin]` table and a
high-entropy `loom_admin_...` token. Production services read it from the
component-specific admin-secret file settings and fail closed when it is
missing, malformed, low-entropy, or unsafe. POSIX deployments require mode
`0600`. Local `loom service up --environment local` manages a development copy
under `.loom/admin/secrets.toml`.

Manage the local secret without printing it by default:

```text
loom service init-admin
loom service reveal-admin
loom service rotate-admin
```

`reveal-admin` is the explicit secret-display operation. Protect its terminal
and logs accordingly. Rotation replaces the singleton credential used by the
service, Control Plane, and LLM Gateway.

Administrative mutations write durable attribution records to
`admin_audit_events`; mutations that share the service database with their
audit record fail if that record cannot be written.

## Staging browser acceptance

Staging can enable a hidden endpoint that exchanges the singleton operator
bearer for an audited browser session belonging to an existing platform-admin
owner of the `admin` team. The exchange is rejected outside the staging
runtime. Its session lasts exactly 900 seconds, uses a distinct secret prefix,
requires secure cookies, and cannot be refreshed. It exists only for automated
staging browser acceptance, not for user login or production access.

## Persistence and implementation

Authentication and team records live in Postgres. The principal tables are
`users`, `teams`, `team_memberships`, `user_sessions`,
`user_registration_requests`, `password_reset_requests`,
`account_action_tokens`, `team_invites`, `tokens`, and
`admin_audit_events`.

The current schema is defined in
[`src/loom/db/schema.py`](../../src/loom/db/schema.py). Public auth and session
routes are implemented in
[`src/loom_service/routes/auth.py`](../../src/loom_service/routes/auth.py), team
administration in
[`src/loom_service/routes/teams.py`](../../src/loom_service/routes/teams.py),
and compatibility invites in
[`src/loom_service/routes/invites.py`](../../src/loom_service/routes/invites.py).

For attack assumptions and security invariants, see the
[authentication threat model](auth-threat-model.md).
