/** Session sign-in and team token management. */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";
import { cn } from "../lib/cn";

const LINK_BUTTON =
  "inline-flex items-center justify-center rounded-lg border px-3.5 py-2 text-sm font-medium";

export default function Settings(): JSX.Element {
  const {
    me,
    isAuthenticated,
    isAdmin,
    authError,
    currentTeamId,
    teams,
    loginStart,
    loginComplete,
    logout,
  } = useAuth();
  const [email, setEmail] = useState("");
  const [loginToken, setLoginToken] = useState("");
  const [loginStarted, setLoginStarted] = useState(false);
  const [requestTeamName, setRequestTeamName] = useState("");
  const [requestEmail, setRequestEmail] = useState("");

  const queryClient = useQueryClient();
  const tokens = useQuery({
    queryKey: ["tokens"],
    queryFn: () => api.listTokens(),
    enabled: isAuthenticated,
    retry: false,
  });

  const start = useMutation({
    mutationFn: (value: string) => loginStart(value),
    onSuccess: (result) => {
      setLoginStarted(true);
      if (result.login_token) setLoginToken(result.login_token);
    },
  });

  const complete = useMutation({
    mutationFn: (value: string) => loginComplete(value),
  });

  const revoke = useMutation({
    mutationFn: (prefix: string) => api.revokeToken(prefix),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["tokens"] }),
  });
  const requestAccess = useMutation({
    mutationFn: () =>
      api.requestTeamRegistration({
        name: requestTeamName.trim(),
        contact_email: requestEmail.trim(),
      }),
  });

  if (!isAuthenticated) {
    return (
      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <Card.Body className="space-y-4">
            <div>
              <p className="text-xs uppercase tracking-wider text-slate-400">
                loom
              </p>
              <h1 className="mt-1 text-2xl font-bold text-slate-900">
                Sign in
              </h1>
              <p className="mt-2 text-sm text-slate-500">
                Use your invited email address. Browser sessions use HttpOnly
                cookies; raw bearer tokens are not stored in this browser.
              </p>
            </div>
            <Input
              placeholder="you@example.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              aria-label="email"
              title="Email address associated with your Loom invite."
            />
            <Button
              variant="primary"
              className="w-full"
              onClick={() => start.mutate(email.trim())}
              disabled={!email.trim() || start.isPending}
              title="Request a one-time login link."
            >
              Continue
            </Button>
            {loginStarted ? (
              <div className="space-y-3 border-t border-slate-100 pt-4">
                <Input
                  placeholder="One-time login token"
                  value={loginToken}
                  onChange={(e) => setLoginToken(e.target.value)}
                  aria-label="login token"
                  title="Paste the one-time login token from your email or local dev response."
                />
                <Button
                  variant="secondary"
                  className="w-full"
                  onClick={() => complete.mutate(loginToken.trim())}
                  disabled={!loginToken.trim() || complete.isPending}
                  title="Complete sign-in and create a browser session."
                >
                  Sign in
                </Button>
              </div>
            ) : null}
            {authError || start.isError || complete.isError ? (
              <p
                role="alert"
                className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
              >
                {authError ??
                  (start.error instanceof Error ? start.error.message : null) ??
                  (complete.error instanceof Error ? complete.error.message : null) ??
                  "Sign-in failed."}
              </p>
            ) : null}
          </Card.Body>
        </Card>

        <div className="space-y-4">
          <Card>
            <Card.Header
              title="Have an invite"
              description="Open an invite link to join a team."
            />
            <Card.Body>
              <Link
                to="/invites/accept"
                className={cn(
                  LINK_BUTTON,
                  "w-full border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                )}
              >
                Open invite page
              </Link>
            </Card.Body>
          </Card>

          <Card>
            <Card.Header
              title="Request access"
              description="Submit a team request for admin review."
            />
            <Card.Body className="space-y-3">
              <Input
                aria-label="Requested team name"
                value={requestTeamName}
                onChange={(event) => setRequestTeamName(event.target.value)}
                placeholder="Team name"
              />
              <Input
                aria-label="Request contact email"
                value={requestEmail}
                onChange={(event) => setRequestEmail(event.target.value)}
                placeholder="you@example.com"
              />
              <Button
                variant="secondary"
                className="w-full"
                disabled={
                  !requestTeamName.trim() ||
                  !requestEmail.trim() ||
                  requestAccess.isPending
                }
                onClick={() => requestAccess.mutate()}
              >
                Request access
              </Button>
              {requestAccess.isSuccess ? (
                <p className="text-sm text-emerald-700">Request submitted.</p>
              ) : null}
              {requestAccess.isError ? <ErrorState error={requestAccess.error} /> : null}
            </Card.Body>
          </Card>
        </div>
      </div>
    );
  }

  const currentTeam = teams.find((team) => team.id === currentTeamId) ?? null;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Settings</h1>
        <p className="mt-1 text-sm text-slate-500">
          Manage your browser session and team API tokens.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Browser session"
          description="Session cookies are HttpOnly; unsafe requests use CSRF protection."
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
        <Card.Body>
          <div className="flex flex-wrap items-center gap-3">
            <StatusPill variant={isAdmin ? "info" : "success"}>
              {isAdmin ? "Platform admin" : currentTeam?.role ?? "User"}
            </StatusPill>
            <span className="text-sm text-slate-700">{me?.user.email}</span>
            {currentTeam ? (
              <span className="text-sm text-slate-500">
                {currentTeam.name}
              </span>
            ) : null}
          </div>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Team API tokens"
          description="Prefixes identify credentials without exposing secrets. Scoped CLI tokens will replace normal bearer-token workflows in the public track."
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
              <EmptyState label="No tokens." />
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead>
                    <tr className="bg-slate-50/50">
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Prefix</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Type</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Scopes</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Issued</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Expires</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Status</th>
                      <th className="px-4 py-3" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {tokens.data.items.map((t) => (
                      <tr key={t.token_hash_prefix} className="hover:bg-slate-50">
                        <td className="px-4 py-3 font-mono text-xs text-slate-700">{t.token_hash_prefix}</td>
                        <td className="px-4 py-3 text-slate-700">{t.type}</td>
                        <td className="px-4 py-3 text-slate-600">{t.scopes.join(", ")}</td>
                        <td className="px-4 py-3 text-xs text-slate-500">{t.issued_at.slice(0, 10)}</td>
                        <td className="px-4 py-3 text-xs text-slate-500">{t.expires_at?.slice(0, 10) ?? "-"}</td>
                        <td className="px-4 py-3">
                          {t.revoked_at ? (
                            <StatusPill variant="cancelled">Revoked</StatusPill>
                          ) : (
                            <StatusPill variant="success">Active</StatusPill>
                          )}
                        </td>
                        <td className="px-4 py-3 text-right">
                          {!t.revoked_at ? (
                            <Button
                              size="sm"
                              variant="danger"
                              onClick={() => revoke.mutate(t.token_hash_prefix)}
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
    </div>
  );
}
