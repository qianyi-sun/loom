import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import {
  api,
  type AdminAuditEvent,
  type ApiTokenEntry,
  type ApiTokenReveal,
  type InviteEntry,
  type InviteReveal,
  type InviteRole,
  type InviteStatus,
  type TeamRegistrationApproval,
} from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { currentServerOrigin } from "../lib/serverOrigin";

function formatDate(value: string | null): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function downloadInviteLink(link: string, teamName: string | null): void {
  const blob = new Blob([`${link}\n`], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${teamName ?? "loom"}-invite-link.txt`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function AuditRows({ events }: { events: AdminAuditEvent[] }): JSX.Element {
  if (events.length === 0) return <EmptyState label="No admin audit events." />;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-3 py-2 font-semibold">Time</th>
            <th className="px-3 py-2 font-semibold">Actor</th>
            <th className="px-3 py-2 font-semibold">Action</th>
            <th className="px-3 py-2 font-semibold">Target</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">
          {events.map((event) => (
            <tr key={event.id}>
              <td className="whitespace-nowrap px-3 py-2 text-slate-600">{formatDate(event.created_at)}</td>
              <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-800">{event.actor}</td>
              <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-700">{event.action}</td>
              <td className="px-3 py-2 text-slate-600">
                {event.target_type}:{event.target_id}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function statusClass(status: InviteStatus): string {
  switch (status) {
    case "pending":
      return "text-amber-700";
    case "accepted":
      return "text-emerald-700";
    case "expired":
      return "text-slate-600";
    case "revoked":
      return "text-red-700";
  }
}

const TOKEN_SCOPE_OPTIONS = [
  {
    value: "read:own",
    label: "Read own runs",
    description: "Read this team's runs, batches, artifacts, and metadata.",
  },
  {
    value: "submit",
    label: "Submit evaluations",
    description: "Create trials and batches for the current team.",
  },
  {
    value: "providers:manage",
    label: "Manage provider connections",
    description: "Create, update, test, and remove model provider credentials.",
  },
  {
    value: "tokens:manage",
    label: "Manage API tokens",
    description: "Create, rotate, revoke, and list team API tokens.",
  },
] as const;

const TOKEN_SCOPE_LABELS = new Map(
  TOKEN_SCOPE_OPTIONS.map((option) => [option.value, option.label]),
);

function formatTokenScopes(scopes: string[]): string {
  if (scopes.length === 0) return "No scopes";
  return scopes
    .map((scope) => TOKEN_SCOPE_LABELS.get(scope) ?? scope)
    .sort()
    .join(", ");
}

function tokenName(token: ApiTokenEntry): string {
  return token.name?.trim() || token.token_hash_prefix;
}

function tokenStatus(token: ApiTokenEntry): { label: string; className: string } {
  if (token.revoked_at) {
    return { label: "Revoked", className: "text-red-700" };
  }
  if (token.expires_at && Date.parse(token.expires_at) <= Date.now()) {
    return { label: "Expired", className: "text-slate-600" };
  }
  return { label: "Active", className: "text-emerald-700" };
}

function tokenLifetimeDays(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return 30;
  return Math.floor(parsed);
}

function CliSetupCommands({ token }: { token: string }): JSX.Element {
  const commands = [
    `export LOOM_API_TOKEN=${token}`,
    `loom auth login --server ${currentServerOrigin()} --token env:LOOM_API_TOKEN`,
    "loom auth whoami",
  ];
  return (
    <div className="space-y-2">
      <p className="text-sm font-medium text-emerald-950">CLI setup commands</p>
      <div className="space-y-1 rounded-lg border border-emerald-200 bg-white p-3">
        {commands.map((command) => (
          <code
            key={command}
            className="block whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-slate-800"
          >
            {command}
          </code>
        ))}
      </div>
    </div>
  );
}

type RevealedInvite = TeamRegistrationApproval | InviteReveal;

export default function AdminAccess(): JSX.Element {
  const { isAdmin, isLoading, me } = useAuth();
  const [actor, setActor] = useState("");
  const [rejectedReason, setRejectedReason] = useState<Record<string, string>>({});
  const [revealed, setRevealed] = useState<RevealedInvite | null>(null);
  const [inviteStatus, setInviteStatus] = useState<InviteStatus>("pending");
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteTeamId, setInviteTeamId] = useState("");
  const [inviteRole, setInviteRole] = useState<InviteRole>("member");
  const [inviteMaxUses, setInviteMaxUses] = useState("1");
  const [inviteDomain, setInviteDomain] = useState("");
  const [tokenNameInput, setTokenNameInput] = useState("");
  const [tokenExpiresDays, setTokenExpiresDays] = useState("30");
  const [tokenScopes, setTokenScopes] = useState<string[]>(["read:own", "submit"]);
  const [revealedToken, setRevealedToken] = useState<ApiTokenReveal | null>(null);
  const queryClient = useQueryClient();
  const currentRole = me?.current_team?.role ?? null;
  const canManageTeam = isAdmin || currentRole === "owner";

  const registrations = useQuery({
    queryKey: ["admin", "team-registrations", "pending"],
    queryFn: () => api.listTeamRegistrations("pending"),
    enabled: isAdmin,
  });
  const audit = useQuery({
    queryKey: ["admin", "audit-events"],
    queryFn: () => api.listAdminAuditEvents(50),
    enabled: isAdmin,
  });
  const invites = useQuery({
    queryKey: ["invites", inviteStatus],
    queryFn: () => api.listInvites({ status: inviteStatus }),
    enabled: canManageTeam,
  });
  const tokens = useQuery({
    queryKey: ["api-tokens"],
    queryFn: () => api.listTokens(),
    enabled: canManageTeam,
  });

  const approve = useMutation({
    mutationFn: (id: string) => api.approveTeamRegistration(id, actor.trim()),
    onSuccess: (data) => {
      setRevealed(data);
      queryClient.invalidateQueries({ queryKey: ["admin", "team-registrations"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
      queryClient.invalidateQueries({ queryKey: ["invites"] });
    },
  });
  const reject = useMutation({
    mutationFn: (id: string) => api.rejectTeamRegistration(id, actor.trim(), rejectedReason[id]),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "team-registrations"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const createInvite = useMutation({
    mutationFn: () =>
      api.createInvite(
        {
          email: inviteEmail.trim(),
          team_id: inviteTeamId.trim() || undefined,
          role: inviteRole,
          expires_in_days: 7,
          max_uses: inviteMaxUses.trim() ? Number(inviteMaxUses) : null,
          allowed_domain: inviteDomain.trim() || null,
        },
        actor.trim() || undefined,
      ),
    onSuccess: (data) => {
      setRevealed(data);
      setInviteEmail("");
      queryClient.invalidateQueries({ queryKey: ["invites"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const revokeInvite = useMutation({
    mutationFn: (invite: InviteEntry) =>
      api.revokeInvite(invite.id, "revoked from admin access page", actor.trim()),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["invites"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const resendInvite = useMutation({
    mutationFn: (invite: InviteEntry) => api.resendInvite(invite.id, actor.trim()),
    onSuccess: (data) => {
      setRevealed(data);
      queryClient.invalidateQueries({ queryKey: ["invites"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const createToken = useMutation({
    mutationFn: () =>
      api.createToken({
        name: tokenNameInput.trim(),
        type: "team",
        scopes: tokenScopes,
        expires_in_days: tokenLifetimeDays(tokenExpiresDays),
      }),
    onSuccess: (data) => {
      setRevealedToken(data);
      setTokenNameInput("");
      queryClient.invalidateQueries({ queryKey: ["api-tokens"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const rotateToken = useMutation({
    mutationFn: (token: ApiTokenEntry) => api.rotateToken(token.token_hash_prefix),
    onSuccess: (data) => {
      setRevealedToken(data);
      queryClient.invalidateQueries({ queryKey: ["api-tokens"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const revokeToken = useMutation({
    mutationFn: (token: ApiTokenEntry) => api.revokeToken(token.token_hash_prefix),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["api-tokens"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });

  const actorMissing = isAdmin && actor.trim().length === 0;
  const tokenCreateDisabled =
    tokenNameInput.trim().length === 0 ||
    tokenScopes.length === 0 ||
    createToken.isPending;

  function toggleTokenScope(scope: string, checked: boolean): void {
    setTokenScopes((current) => {
      if (checked) return current.includes(scope) ? current : [...current, scope];
      return current.filter((item) => item !== scope);
    });
  }

  if (isLoading) return <LoadingState />;

  if (!canManageTeam) {
    return (
      <Card>
        <Card.Header
          title="Team access"
          description="Team access management requires the owner role."
        />
        <Card.Body>
          <p className="text-sm text-slate-600">
            Ask a team owner to manage invites, members, and CLI tokens.
          </p>
        </Card.Body>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team access</h1>
        <p className="mt-1 text-sm text-slate-500">
          {isAdmin
            ? "Approve pending team registrations, issue invites, and audit access decisions."
            : "Manage team invites and CLI/API tokens for the current team."}
        </p>
      </header>

      {isAdmin ? (
        <Card>
          <Card.Header
            title="Admin actor"
            description="Recorded in audit events for approve, reject, and platform-admin invite actions."
          />
          <Card.Body>
            <label className="block text-sm font-medium text-slate-700" htmlFor="admin-actor">
              Admin actor
            </label>
            <Input
              id="admin-actor"
              className="mt-2 max-w-sm"
              value={actor}
              onChange={(event) => setActor(event.target.value)}
              placeholder="qianyi"
            />
          </Card.Body>
        </Card>
      ) : null}

      {revealed ? (
        <Card className="border-emerald-200">
          <Card.Header
            title="Invite link"
            description="Shown once; copy or download before leaving this page."
          />
          <Card.Body className="space-y-3">
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 font-mono text-sm text-emerald-900">
              {revealed.invite_link}
            </div>
            <div className="flex gap-2">
              <Button
                size="sm"
                onClick={() => navigator.clipboard.writeText(revealed.invite_link)}
              >
                Copy
              </Button>
              <Button
                size="sm"
                onClick={() =>
                  downloadInviteLink(revealed.invite_link, revealed.invite.team_name)
                }
              >
                Download
              </Button>
            </div>
          </Card.Body>
        </Card>
      ) : null}

      {revealedToken ? (
        <Card className="border-emerald-200">
          <Card.Header
            title="New API token"
            description="Shown once; store it in your password manager or CLI secret store before leaving this page."
          />
          <Card.Body className="space-y-3">
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 font-mono text-sm text-emerald-900">
              {revealedToken.token}
            </div>
            <div className="flex flex-wrap gap-2 text-sm text-emerald-900">
              <span>Prefix: {revealedToken.token_hash_prefix}</span>
              <span>Expires: {formatDate(revealedToken.expires_at)}</span>
            </div>
            <Button
              size="sm"
              onClick={() => navigator.clipboard.writeText(revealedToken.token)}
            >
              Copy token
            </Button>
            <CliSetupCommands token={revealedToken.token} />
          </Card.Body>
        </Card>
      ) : null}

      <Card>
        <Card.Header
          title="API tokens"
          description="Create scoped tokens for CLI and automation. Raw token values are shown only once after create or rotate."
        />
        <Card.Body className="space-y-5">
          <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_9rem_auto]">
            <div>
              <label className="block text-sm font-medium text-slate-700" htmlFor="api-token-name">
                Token name
              </label>
              <Input
                id="api-token-name"
                className="mt-2"
                value={tokenNameInput}
                onChange={(event) => setTokenNameInput(event.target.value)}
                placeholder="Nightly CLI"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700" htmlFor="api-token-expires">
                Lifetime days
              </label>
              <Input
                id="api-token-expires"
                className="mt-2"
                type="number"
                min={1}
                value={tokenExpiresDays}
                onChange={(event) => setTokenExpiresDays(event.target.value)}
              />
            </div>
            <div className="flex items-end">
              <Button
                variant="primary"
                disabled={tokenCreateDisabled}
                onClick={() => createToken.mutate()}
              >
                Create API token
              </Button>
            </div>
          </div>

          <fieldset className="grid gap-2 md:grid-cols-2">
            <legend className="mb-1 text-sm font-medium text-slate-700">
              Token scopes
            </legend>
            {TOKEN_SCOPE_OPTIONS.map((option) => (
              <label
                key={option.value}
                className="flex gap-3 rounded-lg border border-slate-200 bg-white p-3 text-sm"
              >
                <input
                  aria-label={option.label}
                  type="checkbox"
                  className="mt-1"
                  checked={tokenScopes.includes(option.value)}
                  onChange={(event) =>
                    toggleTokenScope(option.value, event.currentTarget.checked)
                  }
                />
                <span>
                  <span className="block font-medium text-slate-800">
                    {option.label}
                  </span>
                  <span className="block text-xs text-slate-500">
                    {option.description}
                  </span>
                </span>
              </label>
            ))}
          </fieldset>

          {tokens.isPending ? <LoadingState /> : null}
          {tokens.isError ? <ErrorState error={tokens.error} /> : null}
          {tokens.data ? (
            tokens.data.items.length === 0 ? (
              <EmptyState label="No API tokens." />
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                    <tr>
                      <th className="px-3 py-2 font-semibold">Name</th>
                      <th className="px-3 py-2 font-semibold">Prefix</th>
                      <th className="px-3 py-2 font-semibold">Scopes</th>
                      <th className="px-3 py-2 font-semibold">Last used</th>
                      <th className="px-3 py-2 font-semibold">Expires</th>
                      <th className="px-3 py-2 font-semibold">Status</th>
                      <th className="px-3 py-2 font-semibold">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 bg-white">
                    {tokens.data.items.map((token) => {
                      const status = tokenStatus(token);
                      const label = tokenName(token);
                      const inactive = token.revoked_at !== null;
                      return (
                        <tr key={token.token_hash_prefix}>
                          <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">
                            {label}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-600">
                            {token.token_hash_prefix}
                          </td>
                          <td className="max-w-sm px-3 py-2 text-slate-600">
                            {formatTokenScopes(token.scopes)}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                            {token.last_used_at ? formatDate(token.last_used_at) : "Never"}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                            {formatDate(token.expires_at)}
                          </td>
                          <td className={`whitespace-nowrap px-3 py-2 font-medium ${status.className}`}>
                            {status.label}
                          </td>
                          <td className="px-3 py-2">
                            <div className="flex gap-2">
                              <Button
                                size="sm"
                                aria-label={`Rotate ${label}`}
                                disabled={inactive || rotateToken.isPending}
                                onClick={() => rotateToken.mutate(token)}
                              >
                                Rotate
                              </Button>
                              <Button
                                size="sm"
                                variant="danger"
                                aria-label={`Revoke ${label}`}
                                disabled={inactive || revokeToken.isPending}
                                onClick={() => revokeToken.mutate(token)}
                              >
                                Revoke
                              </Button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )
          ) : null}
          {createToken.isError ? <ErrorState error={createToken.error} /> : null}
          {rotateToken.isError ? <ErrorState error={rotateToken.error} /> : null}
          {revokeToken.isError ? <ErrorState error={revokeToken.error} /> : null}
        </Card.Body>
      </Card>

      {isAdmin ? (
        <Card>
          <Card.Header
            title="Pending registrations"
            description="Approve creates an owner invite link for the requested contact."
          />
          <Card.Body>
            {registrations.isPending ? <LoadingState /> : null}
            {registrations.isError ? <ErrorState error={registrations.error} /> : null}
            {registrations.data ? (
              registrations.data.items.length === 0 ? (
                <EmptyState label="No pending registrations." />
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-slate-200 text-sm">
                    <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                      <tr>
                        <th className="px-3 py-2 font-semibold">Team</th>
                        <th className="px-3 py-2 font-semibold">Contact</th>
                        <th className="px-3 py-2 font-semibold">Requested</th>
                        <th className="px-3 py-2 font-semibold">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 bg-white">
                      {registrations.data.items.map((item) => (
                        <tr key={item.id}>
                          <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">{item.name}</td>
                          <td className="whitespace-nowrap px-3 py-2 text-slate-600">{item.contact_email}</td>
                          <td className="whitespace-nowrap px-3 py-2 text-slate-600">{formatDate(item.requested_at)}</td>
                          <td className="px-3 py-2">
                            <div className="flex flex-wrap items-center gap-2">
                              <Button
                                size="sm"
                                variant="primary"
                                disabled={actorMissing || approve.isPending}
                                onClick={() => approve.mutate(item.id)}
                              >
                                Approve
                              </Button>
                              <Input
                                aria-label={`Reject reason for ${item.name}`}
                                className="w-48"
                                value={rejectedReason[item.id] ?? ""}
                                onChange={(event) =>
                                  setRejectedReason((current) => ({
                                    ...current,
                                    [item.id]: event.target.value,
                                  }))
                                }
                                placeholder="reason"
                              />
                              <Button
                                size="sm"
                                variant="danger"
                                disabled={actorMissing || reject.isPending}
                                onClick={() => reject.mutate(item.id)}
                              >
                                Reject
                              </Button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : null}
            {approve.isError ? <ErrorState error={approve.error} /> : null}
            {reject.isError ? <ErrorState error={reject.error} /> : null}
          </Card.Body>
        </Card>
      ) : null}

      <Card>
        <Card.Header
          title="Create invite"
          description="Create a team invite; the raw link is shown only once."
        />
        <Card.Body className="grid gap-3 md:grid-cols-5">
          <Input
            aria-label="Invite recipient email"
            value={inviteEmail}
            onChange={(event) => setInviteEmail(event.target.value)}
            placeholder="person@example.com"
          />
          <Input
            aria-label="Invite team id"
            value={inviteTeamId}
            onChange={(event) => setInviteTeamId(event.target.value)}
            placeholder="team id"
          />
          <select
            aria-label="Invite role"
            className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
            value={inviteRole}
            onChange={(event) => setInviteRole(event.target.value as InviteRole)}
          >
            <option value="viewer">viewer</option>
            <option value="member">member</option>
            <option value="owner">owner</option>
          </select>
          <Input
            aria-label="Invite max uses"
            type="number"
            min={1}
            value={inviteMaxUses}
            onChange={(event) => setInviteMaxUses(event.target.value)}
            placeholder="max uses"
          />
          <Input
            aria-label="Allowed domain"
            value={inviteDomain}
            onChange={(event) => setInviteDomain(event.target.value)}
            placeholder="allowed domain"
          />
          <div className="md:col-span-5">
            <Button
              variant="primary"
              disabled={actorMissing || !inviteEmail.trim() || createInvite.isPending}
              onClick={() => createInvite.mutate()}
            >
              Create invite
            </Button>
          </div>
          {createInvite.isError ? <ErrorState error={createInvite.error} /> : null}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title={`${inviteStatus[0].toUpperCase()}${inviteStatus.slice(1)} invites`}
          description="Invite links are listed by status without exposing raw codes."
          actions={
            <select
              aria-label="Invite status"
              className="rounded-lg border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
              value={inviteStatus}
              onChange={(event) => setInviteStatus(event.target.value as InviteStatus)}
            >
              <option value="pending">pending</option>
              <option value="accepted">accepted</option>
              <option value="expired">expired</option>
              <option value="revoked">revoked</option>
            </select>
          }
        />
        <Card.Body>
          {invites.isPending ? <LoadingState /> : null}
          {invites.isError ? <ErrorState error={invites.error} /> : null}
          {invites.data ? (
            invites.data.items.length === 0 ? (
              <EmptyState label={`No ${inviteStatus} invites.`} />
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                    <tr>
                      <th className="px-3 py-2 font-semibold">Team</th>
                      <th className="px-3 py-2 font-semibold">Email</th>
                      <th className="px-3 py-2 font-semibold">Role</th>
                      <th className="px-3 py-2 font-semibold">Prefix</th>
                      <th className="px-3 py-2 font-semibold">Status</th>
                      <th className="px-3 py-2 font-semibold">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 bg-white">
                    {invites.data.items.map((invite) => (
                      <tr key={invite.id}>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-700">
                          {invite.team_name ?? invite.team_id}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {invite.email}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {invite.role}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-600">
                          {invite.code_prefix}
                        </td>
                        <td className={`whitespace-nowrap px-3 py-2 font-medium ${statusClass(invite.status)}`}>
                          {invite.status}
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex gap-2">
                            {invite.status === "pending" ? (
                              <Button
                                size="sm"
                                variant="danger"
                                disabled={actorMissing || revokeInvite.isPending}
                                onClick={() => revokeInvite.mutate(invite)}
                              >
                                Revoke
                              </Button>
                            ) : null}
                            {invite.status === "pending" || invite.status === "expired" ? (
                              <Button
                                size="sm"
                                disabled={actorMissing || resendInvite.isPending}
                                onClick={() => resendInvite.mutate(invite)}
                              >
                                Resend
                              </Button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          ) : null}
          {revokeInvite.isError ? <ErrorState error={revokeInvite.error} /> : null}
          {resendInvite.isError ? <ErrorState error={resendInvite.error} /> : null}
        </Card.Body>
      </Card>

      {isAdmin ? (
        <Card>
          <Card.Header
            title="Audit log"
            description="Recent admin access decisions with actor, action, and target."
          />
          <Card.Body>
            {audit.isPending ? <LoadingState /> : null}
            {audit.isError ? <ErrorState error={audit.error} /> : null}
            {audit.data ? <AuditRows events={audit.data.items} /> : null}
          </Card.Body>
        </Card>
      ) : null}
    </div>
  );
}
