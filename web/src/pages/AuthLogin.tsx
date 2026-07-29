/** Signed-out onboarding: sign-in, account request, password-reset request (#775). */

import { useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Navigate } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import { cliLoginCommands } from "../lib/quickstartSnippets";
import { currentServerOrigin } from "../lib/serverOrigin";

const SIGN_IN_FAILURE = new Error(
  "Sign-in failed. Check your username and password, then try again.",
);

const FIELD_LABEL = "block text-sm font-medium text-slate-700";

export default function AuthLogin(): JSX.Element {
  const { isAuthenticated, loginPassword } = useAuth();
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPasswordValue, setLoginPasswordValue] = useState("");
  const [registerUsername, setRegisterUsername] = useState("");
  const [registerTeamId, setRegisterTeamId] = useState("");
  const [resetUsername, setResetUsername] = useState("");

  const publicTeams = useQuery({
    queryKey: ["public-teams"],
    queryFn: () => api.publicTeams(),
    enabled: !isAuthenticated,
  });

  // Clear a stale Team selection when policy refetch removes it — never
  // silently replace with the first returned Team (#775).
  useEffect(() => {
    if (!registerTeamId) return;
    const ids = new Set((publicTeams.data?.items ?? []).map((team) => team.id));
    if (!ids.has(registerTeamId)) {
      setRegisterTeamId("");
    }
  }, [publicTeams.data, registerTeamId]);

  const signIn = useMutation({
    mutationFn: () => loginPassword(loginUsername.trim(), loginPasswordValue),
  });
  const requestAccess = useMutation({
    mutationFn: () =>
      api.requestRegistration({
        username: registerUsername.trim(),
        team_id: registerTeamId,
      }),
  });
  const requestReset = useMutation({
    mutationFn: () => api.requestPasswordReset(resetUsername.trim()),
  });

  if (isAuthenticated) {
    return <Navigate to="/settings" replace />;
  }

  const publicTeamItems = publicTeams.data?.items ?? [];
  const hasPublicTeams = publicTeamItems.length > 0;
  const cliLoginCommand = cliLoginCommands(currentServerOrigin()).join("\n");

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
          username for a publicly available team, set your password from the
          approved setup link, then use the same account in the browser and CLI.
        </p>
      </header>

      <div className="grid items-start gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(22rem,26rem)]">
        <Card>
          <Card.Body className="space-y-5 p-6 sm:p-7">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Sign in</h2>
              <p className="mt-2 text-sm text-slate-500">
                Use the username and password you set from an admin-approved
                account link.
              </p>
            </div>
            <form
              className="grid gap-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (signIn.isPending) return;
                signIn.mutate();
              }}
            >
              <div className="grid gap-1.5">
                <label className={FIELD_LABEL} htmlFor="login-username">
                  Username
                </label>
                <Input
                  id="login-username"
                  name="username"
                  autoComplete="username"
                  value={loginUsername}
                  onChange={(e) => setLoginUsername(e.target.value)}
                />
              </div>
              <div className="grid gap-1.5">
                <label className={FIELD_LABEL} htmlFor="login-password">
                  Password
                </label>
                <Input
                  id="login-password"
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  value={loginPasswordValue}
                  onChange={(e) => setLoginPasswordValue(e.target.value)}
                />
              </div>
              <Button
                type="submit"
                variant="primary"
                className="w-full"
                disabled={
                  !loginUsername.trim() ||
                  !loginPasswordValue ||
                  signIn.isPending
                }
              >
                {signIn.isPending ? "Signing in…" : "Sign in"}
              </Button>
            </form>
            {signIn.isError ? (
              <div role="alert">
                <ErrorState error={SIGN_IN_FAILURE} />
              </div>
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
              <li>Submit a username request for a publicly available team.</li>
              <li>Wait for an admin to approve it and share a password link.</li>
              <li>Set your password, then sign in here or from CLI.</li>
              <li>Create a provider, then launch a one-task smoke batch.</li>
            </ol>
          </DocsCallout>

          <Card>
            <Card.Header
              title="Request account"
              description="Choose a team that allows public registration. Ask an admin if your team is missing."
            />
            <Card.Body className="space-y-3">
              {!publicTeams.isPending && !hasPublicTeams ? (
                <EmptyState label="No teams are currently open for public registration." />
              ) : null}
              <form
                className="space-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (requestAccess.isPending || !registerTeamId) return;
                  requestAccess.mutate();
                }}
              >
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="register-username">
                    Requested username
                  </label>
                  <Input
                    id="register-username"
                    name="username"
                    autoComplete="username"
                    value={registerUsername}
                    onChange={(event) => setRegisterUsername(event.target.value)}
                  />
                </div>
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="register-team">
                    Registration team
                  </label>
                  <select
                    id="register-team"
                    name="team_id"
                    className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                    value={registerTeamId}
                    onChange={(event) => setRegisterTeamId(event.target.value)}
                    disabled={!hasPublicTeams}
                  >
                    <option value="">Select a team…</option>
                    {publicTeamItems.map((team) => (
                      <option key={team.id} value={team.id}>
                        {team.name}
                      </option>
                    ))}
                  </select>
                </div>
                <Button
                  type="submit"
                  variant="secondary"
                  className="w-full"
                  disabled={
                    !registerUsername.trim() ||
                    !registerTeamId ||
                    requestAccess.isPending
                  }
                >
                  {requestAccess.isPending ? "Submitting…" : "Request account"}
                </Button>
              </form>
              {requestAccess.isSuccess ? (
                <p className="text-sm text-emerald-700">
                  Request submitted. An admin will review it and share a
                  password setup link manually if approved.
                </p>
              ) : null}
              {requestAccess.isError ? (
                <ErrorState error={requestAccess.error} />
              ) : null}
            </Card.Body>
          </Card>

          <Card>
            <Card.Header
              title="Forgot password"
              description="Submit a reset request for admin approval."
            />
            <Card.Body className="space-y-3">
              <form
                className="space-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (requestReset.isPending) return;
                  requestReset.mutate();
                }}
              >
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="reset-username">
                    Reset username
                  </label>
                  <Input
                    id="reset-username"
                    name="username"
                    autoComplete="username"
                    value={resetUsername}
                    onChange={(event) => setResetUsername(event.target.value)}
                  />
                </div>
                <Button
                  type="submit"
                  variant="secondary"
                  className="w-full"
                  disabled={!resetUsername.trim() || requestReset.isPending}
                >
                  {requestReset.isPending ? "Submitting…" : "Request reset"}
                </Button>
              </form>
              {requestReset.isSuccess ? (
                <p className="text-sm text-emerald-700">
                  Reset request submitted. An admin will share a reset link if approved.
                </p>
              ) : null}
              {requestReset.isError ? (
                <ErrorState error={requestReset.error} />
              ) : null}
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
