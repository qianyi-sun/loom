/** Account onboarding and team settings. */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api, type ApiTokenEntry } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";
import { cn } from "../lib/cn";
import { formatLocalDateTime } from "../lib/dateTime";
import { cliLoginCommands } from "../lib/quickstartSnippets";
import { currentServerOrigin } from "../lib/serverOrigin";

type TeamDetail = Awaited<ReturnType<typeof api.getTeam>>;
type TeamUserMember = NonNullable<TeamDetail["user_members"]>[number];

const LINK_BUTTON =
  "inline-flex items-center justify-center rounded-lg border px-3.5 py-2 text-sm font-medium";

const ROLE_DESCRIPTIONS: Record<string, string> = {
  owner: "Can manage team access, API tokens, provider credentials, and submissions.",
  member: "Can submit and monitor this team's evaluation work.",
  viewer: "Can read this team's completed and in-progress work.",
  platform_admin: "Can administer platform access and shared configuration.",
};

function hasScope(scopes: string[] | undefined, scope: string): boolean {
  return Boolean(scopes?.includes(scope));
}

function formatDate(value: string | null | undefined): string {
  return formatLocalDateTime(value, { fallback: "-" });
}

function roleLabel(role: string | null | undefined): string {
  if (!role) return "No team role";
  return role.replaceAll("_", " ");
}

function tokenStatus(token: ApiTokenEntry): JSX.Element {
  if (token.revoked_at) {
    return <StatusPill variant="cancelled">Revoked</StatusPill>;
  }
  if (token.expires_at && Date.parse(token.expires_at) <= Date.now()) {
    return <StatusPill variant="neutral">Expired</StatusPill>;
  }
  return <StatusPill variant="success">Active</StatusPill>;
}

function TeamMembers({
  members,
}: {
  members: TeamUserMember[] | undefined;
}): JSX.Element {
  if (!members || members.length === 0) {
    return <EmptyState label="No browser users have joined this team yet." />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-4 py-3 font-semibold">Member</th>
            <th className="px-4 py-3 font-semibold">Role</th>
            <th className="px-4 py-3 font-semibold">Joined</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">
          {members.map((member) => (
            <tr key={member.user_id}>
              <td className="px-4 py-3">
                <div className="font-medium text-slate-900">
                  {member.display_name ?? member.email}
                </div>
                <div className="text-xs text-slate-500">{member.email}</div>
              </td>
              <td className="px-4 py-3 text-slate-700">{roleLabel(member.role)}</td>
              <td className="px-4 py-3 text-slate-600">
                {formatDate(member.joined_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Settings(): JSX.Element {
  const {
    me,
    isAuthenticated,
    isAdmin,
    authError,
    currentTeamId,
    teams,
    loginPassword,
    switchTeam,
    logout,
  } = useAuth();
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPasswordValue, setLoginPasswordValue] = useState("");
  const [registerUsername, setRegisterUsername] = useState("");
  const [registerTeamId, setRegisterTeamId] = useState("");
  const [resetUsername, setResetUsername] = useState("");
  const serverOrigin = currentServerOrigin();
  const cliLoginCommand = cliLoginCommands(serverOrigin).join("\n");

  const queryClient = useQueryClient();
  const currentTeam = teams.find((team) => team.id === currentTeamId) ?? null;
  const scopes = me?.scopes ?? [];
  const canSubmit = isAdmin || hasScope(scopes, "submit");
  const canManageProviders = isAdmin || hasScope(scopes, "providers:manage");
  const canManageTokens = isAdmin || hasScope(scopes, "tokens:manage");
  const canManageTeam = isAdmin || currentTeam?.role === "owner";

  const tokens = useQuery({
    queryKey: ["tokens"],
    queryFn: () => api.listTokens(),
    enabled: isAuthenticated && canManageTokens,
    retry: false,
  });
  const teamDetail = useQuery({
    queryKey: ["team", currentTeamId],
    queryFn: () => api.getTeam(currentTeamId ?? ""),
    enabled: isAuthenticated && currentTeamId !== null,
    retry: false,
  });

  const publicTeams = useQuery({
    queryKey: ["public-teams"],
    queryFn: () => api.publicTeams(),
    enabled: !isAuthenticated,
  });
  const signIn = useMutation({
    mutationFn: () => loginPassword(loginUsername.trim(), loginPasswordValue),
  });

  const switchTeamMutation = useMutation({
    mutationFn: (teamId: string) => switchTeam(teamId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["team"] });
      queryClient.invalidateQueries({ queryKey: ["tokens"] });
    },
  });

  const revoke = useMutation({
    mutationFn: (prefix: string) => api.revokeToken(prefix),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["tokens"] }),
  });
  const requestAccess = useMutation({
    mutationFn: () =>
      api.requestRegistration({
        username: registerUsername.trim(),
        team_id: registerTeamId || publicTeams.data?.items[0]?.id || "",
      }),
  });
  const requestReset = useMutation({
    mutationFn: () => api.requestPasswordReset(resetUsername.trim()),
  });

  if (!isAuthenticated) {
    const selectedRegistrationTeamId = registerTeamId || publicTeams.data?.items[0]?.id || "";
    return (
      <div className="space-y-8">
        <header className="max-w-3xl">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-600">
            Loom staging
          </p>
          <h1 className="mt-2 text-3xl font-bold text-slate-950">
            Sign in to run and review evaluations
          </h1>
          <p className="mt-3 text-base text-slate-600">
            Staging account requests are reviewed by an admin. Request a
            username for an existing team, set your password from the approved
            setup link, then use the same account in the browser and CLI.
          </p>
        </header>

        <div className="grid items-start gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(22rem,26rem)]">
          <Card>
            <Card.Body className="space-y-5 p-6 sm:p-7">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">
                  Sign in
                </h2>
                <p className="mt-2 text-sm text-slate-500">
                  Use the username and password you set from an admin-approved
                  account link.
                </p>
              </div>
              <div className="grid gap-3">
                <Input
                  placeholder="Username"
                  value={loginUsername}
                  onChange={(e) => setLoginUsername(e.target.value)}
                  aria-label="Username"
                />
                <Input
                  type="password"
                  placeholder="Password"
                  value={loginPasswordValue}
                  onChange={(e) => setLoginPasswordValue(e.target.value)}
                  aria-label="Password"
                />
                <Button
                  variant="primary"
                  className="w-full"
                  onClick={() => signIn.mutate()}
                  disabled={!loginUsername.trim() || !loginPasswordValue || signIn.isPending}
                >
                  Sign in
                </Button>
              </div>
              {authError || signIn.isError ? (
                <p
                  role="alert"
                  className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
                >
                  {authError ??
                    (signIn.error instanceof Error ? signIn.error.message : null) ??
                    "Sign-in failed."}
                </p>
              ) : null}
            </Card.Body>
          </Card>

          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-1">
            <DocsCallout
              title="First run checklist"
              tone="info"
              className="md:col-span-2 lg:col-span-1"
            >
              <ol className="list-decimal space-y-1 pl-4">
                <li>Submit a username request for an existing team.</li>
                <li>Wait for an admin to approve it and share a password link.</li>
                <li>Set your password, then sign in here or from CLI.</li>
                <li>Create a provider, then launch a one-task smoke batch.</li>
              </ol>
            </DocsCallout>

            <Card>
              <Card.Header
                title="Request account"
                description="Choose an existing team. Ask an admin to create the team first if it is missing."
              />
              <Card.Body className="space-y-3">
                <Input
                  aria-label="Requested username"
                  value={registerUsername}
                  onChange={(event) => setRegisterUsername(event.target.value)}
                  placeholder="Username"
                />
                <select
                  aria-label="Registration team"
                  className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                  value={selectedRegistrationTeamId}
                  onChange={(event) => setRegisterTeamId(event.target.value)}
                >
                  {(publicTeams.data?.items ?? []).map((team) => (
                    <option key={team.id} value={team.id}>{team.name}</option>
                  ))}
                </select>
                <Button
                  variant="secondary"
                  className="w-full"
                  disabled={
                    !registerUsername.trim() ||
                    !selectedRegistrationTeamId ||
                    requestAccess.isPending
                  }
                  onClick={() => requestAccess.mutate()}
                >
                  Request account
                </Button>
                {requestAccess.isSuccess ? (
                  <p className="text-sm text-emerald-700">
                    Request submitted. An admin will review it and share a
                    password setup link manually if approved.
                  </p>
                ) : null}
                {requestAccess.isError ? <ErrorState error={requestAccess.error} /> : null}
              </Card.Body>
            </Card>

            <Card>
              <Card.Header
                title="Forgot password"
                description="Submit a reset request for admin approval."
              />
              <Card.Body className="space-y-3">
                <Input
                  aria-label="Reset username"
                  value={resetUsername}
                  onChange={(event) => setResetUsername(event.target.value)}
                  placeholder="Username"
                />
                <Button
                  variant="secondary"
                  className="w-full"
                  disabled={!resetUsername.trim() || requestReset.isPending}
                  onClick={() => requestReset.mutate()}
                >
                  Request reset
                </Button>
                {requestReset.isSuccess ? (
                  <p className="text-sm text-emerald-700">
                    Reset request submitted. An admin will share a reset link if approved.
                  </p>
                ) : null}
                {requestReset.isError ? <ErrorState error={requestReset.error} /> : null}
              </Card.Body>
            </Card>

            <Card className="md:col-span-2 lg:col-span-1">
              <Card.Header
                title="CLI setup"
                description="Use the same approved username and password with the CLI."
              />
              <Card.Body>
                <CommandSnippet label="CLI login" command={cliLoginCommand} />
              </Card.Body>
            </Card>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team Settings</h1>
        <p className="mt-1 text-sm text-slate-500">
          Manage your browser session, current team, members, provider setup,
          and CLI credentials.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Current team"
          description={
            currentTeam?.role
              ? ROLE_DESCRIPTIONS[currentTeam.role] ?? "This role controls which team actions are available."
              : "Select a team to scope runs, credentials, and artifacts."
          }
          actions={
            <Button
              variant="secondary"
              onClick={() => void logout()}
              title="End this browser session."
            >
              Sign out
            </Button>
          }
        />
        <Card.Body className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_16rem]">
          <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-3">
              <StatusPill variant={isAdmin ? "info" : "success"}>
                {isAdmin ? "Platform admin" : roleLabel(currentTeam?.role)}
              </StatusPill>
              <span className="text-sm text-slate-700">{me?.user.username}</span>
              {currentTeam ? (
                <span className="text-sm font-medium text-slate-900">
                  {currentTeam.name}
                </span>
              ) : null}
            </div>
            <div className="flex flex-wrap gap-2">
              {canSubmit ? <StatusPill variant="success">Can submit</StatusPill> : null}
              {canManageProviders ? (
                <StatusPill variant="info">Can manage providers</StatusPill>
              ) : null}
              {canManageTokens ? (
                <StatusPill variant="info">Can manage CLI tokens</StatusPill>
              ) : null}
              {!canSubmit && !canManageProviders && !canManageTokens ? (
                <StatusPill variant="neutral">Read only</StatusPill>
              ) : null}
            </div>
          </div>

          <div>
            <label
              htmlFor="current-team"
              className="block text-sm font-medium text-slate-700"
            >
              Current team
            </label>
            <select
              id="current-team"
              aria-label="Current team"
              className="mt-2 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
              value={currentTeamId ?? ""}
              disabled={teams.length <= 1 || switchTeamMutation.isPending}
              onChange={(event) => switchTeamMutation.mutate(event.target.value)}
            >
              {teams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name} ({roleLabel(team.role)})
                </option>
              ))}
            </select>
          </div>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Team members"
          description="Browser users who accepted an invite for the current team."
        />
        <Card.Body className="p-0">
          {teamDetail.isPending ? <LoadingState /> : null}
          {teamDetail.isError ? (
            <div className="p-5">
              <ErrorState error={teamDetail.error} />
            </div>
          ) : null}
          {teamDetail.data ? (
            <TeamMembers members={teamDetail.data.user_members ?? []} />
          ) : null}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Setup actions"
          description="Role-aware shortcuts for common team setup tasks."
        />
        <Card.Body className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {canSubmit ? (
            <Link
              to="/batches/new"
              className={cn(LINK_BUTTON, "border-accent bg-accent text-white hover:bg-accent-hover")}
            >
              New batch
            </Link>
          ) : null}
          <Link
            to="/providers"
            className={cn(
              LINK_BUTTON,
              canManageProviders
                ? "border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                : "pointer-events-none border-slate-200 bg-slate-50 text-slate-600",
            )}
            aria-disabled={!canManageProviders}
            title={
              canManageProviders
                ? "Create and test provider connections."
                : "Provider connection management requires an owner role."
            }
          >
            Provider connections
          </Link>
          {canManageTeam ? (
            <Link
              to="/admin/access"
              className={cn(
                LINK_BUTTON,
                "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
              )}
            >
              Team access
            </Link>
          ) : null}
          <Link
            to="/usage"
            className={cn(
              LINK_BUTTON,
              "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
            )}
          >
            Usage
          </Link>
        </Card.Body>
      </Card>

      {canManageTokens ? (
        <Card>
          <Card.Header
            title="API tokens"
            description="Prefixes identify credentials without exposing secrets. Create new CLI tokens from Team access."
            actions={
              <Link
                to="/admin/access"
                className={cn(
                  LINK_BUTTON,
                  "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                )}
              >
                Create CLI token
              </Link>
            }
          />
          <Card.Body className="p-0">
            {tokens.isPending ? <LoadingState /> : null}
            {tokens.isError ? (
              <div className="p-5">
                <ErrorState error={tokens.error} />
              </div>
            ) : null}
            {tokens.data ? (
              tokens.data.items.length === 0 ? (
                <EmptyState label="No API tokens." />
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-slate-200 text-sm">
                    <thead>
                      <tr className="bg-slate-50/50">
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Name</th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Prefix</th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Scopes</th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Issued</th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Expires</th>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Status</th>
                        <th className="px-4 py-3" />
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {tokens.data.items.map((token) => (
                        <tr key={token.token_hash_prefix} className="hover:bg-slate-50">
                          <td className="px-4 py-3 font-medium text-slate-900">
                            {token.name?.trim() || token.token_hash_prefix}
                          </td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-700">{token.token_hash_prefix}</td>
                          <td className="px-4 py-3 text-slate-600">{token.scopes.join(", ")}</td>
                          <td className="px-4 py-3 text-xs text-slate-500">{formatDate(token.issued_at)}</td>
                          <td className="px-4 py-3 text-xs text-slate-500">{formatDate(token.expires_at)}</td>
                          <td className="px-4 py-3">{tokenStatus(token)}</td>
                          <td className="px-4 py-3 text-right">
                            {!token.revoked_at ? (
                              <Button
                                size="sm"
                                variant="danger"
                                onClick={() => revoke.mutate(token.token_hash_prefix)}
                                disabled={revoke.isPending}
                                title="Revoke this token so it can no longer call the API."
                              >
                                Revoke
                              </Button>
                            ) : null}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : null}
          </Card.Body>
        </Card>
      ) : null}
    </div>
  );
}
