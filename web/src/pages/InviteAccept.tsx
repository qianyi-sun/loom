import { useMutation, useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { api, type InviteLookup, type InviteStatus } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import DocsCallout from "../components/DocsCallout";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatusPill, type StatusVariant } from "../components/StatusPill";
import { cn } from "../lib/cn";

const LINK_BUTTON =
  "inline-flex items-center justify-center rounded-lg border px-3.5 py-2 text-sm font-medium";

function statusLabel(status: InviteStatus): string {
  switch (status) {
    case "pending":
      return "Ready";
    case "expired":
      return "Invite expired";
    case "revoked":
      return "Invite revoked";
    case "accepted":
      return "Invite already used";
  }
}

function statusVariant(status: InviteStatus): StatusVariant {
  switch (status) {
    case "pending":
      return "success";
    case "expired":
      return "warning";
    case "revoked":
      return "cancelled";
    case "accepted":
      return "info";
  }
}

function InviteSummary({ invite }: { invite: InviteLookup }): JSX.Element {
  return (
    <div className="grid gap-3 sm:grid-cols-3">
      <div>
        <p className="text-xs font-medium uppercase tracking-wider text-slate-400">Team</p>
        <p className="mt-1 text-sm font-semibold text-slate-900">{invite.team_name}</p>
      </div>
      <div>
        <p className="text-xs font-medium uppercase tracking-wider text-slate-400">Role</p>
        <p className="mt-1 text-sm font-semibold text-slate-900">{invite.role}</p>
      </div>
      <div>
        <p className="text-xs font-medium uppercase tracking-wider text-slate-400">Invite</p>
        <p className="mt-1 font-mono text-sm text-slate-700">{invite.code_prefix}</p>
      </div>
    </div>
  );
}

export default function InviteAccept(): JSX.Element {
  const [params] = useSearchParams();
  const code = params.get("code")?.trim() ?? "";
  const { acceptInvite } = useAuth();
  const [email, setEmail] = useState("");
  const [joinedTeam, setJoinedTeam] = useState<string | null>(null);

  const invite = useQuery({
    queryKey: ["invite", code],
    queryFn: () => api.lookupInvite(code),
    enabled: code.length > 0,
    retry: false,
  });

  const accept = useMutation({
    mutationFn: () => acceptInvite(code, email.trim()),
    onSuccess: (me) => {
      setJoinedTeam(me.current_team?.name ?? invite.data?.team_name ?? "team");
    },
  });

  const status = invite.data?.status ?? null;
  const canAccept = status === "pending";
  const title = useMemo(() => {
    if (!code) return "Invite link required";
    if (joinedTeam) return `Joined ${joinedTeam}`;
    if (status) return statusLabel(status);
    return "Accept invite";
  }, [code, joinedTeam, status]);

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">{title}</h1>
        <p className="mt-1 text-sm text-slate-500">
          Invite links add a browser user to a team without exposing raw API credentials.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Invite"
          description="Review the target team and role before accepting."
          actions={
            status ? (
              <StatusPill variant={statusVariant(status)}>
                {statusLabel(status)}
              </StatusPill>
            ) : null
          }
        />
        <Card.Body className="space-y-5">
          {!code ? (
            <div className="space-y-3">
              <p className="text-sm text-slate-600">
                Open the invite link you received, or paste the full link in the browser address bar.
              </p>
              <Link
                to="/settings"
                className={cn(
                  LINK_BUTTON,
                  "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                )}
              >
                Back to sign in
              </Link>
            </div>
          ) : null}

          {invite.isPending ? <LoadingState label="Checking invite..." /> : null}
          {invite.isError ? <ErrorState error={invite.error} /> : null}
          {invite.data ? <InviteSummary invite={invite.data} /> : null}

          {canAccept && !joinedTeam ? (
            <div className="space-y-3 border-t border-slate-100 pt-4">
              <label className="block text-sm font-medium text-slate-700" htmlFor="invite-email">
                Invite email
              </label>
              <Input
                id="invite-email"
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@example.com"
                aria-label="Invite email"
                title="Email address allowed by this invite."
              />
              <Button
                variant="primary"
                onClick={() => accept.mutate()}
                disabled={!email.trim() || accept.isPending}
                title="Accept the invite and create a browser session."
              >
                Accept invite
              </Button>
            </div>
          ) : null}

          {joinedTeam ? (
            <div className="space-y-3 border-t border-slate-100 pt-4">
              <DocsCallout title="Next setup steps" tone="success">
                <p>
                  Create or test a model provider first, then launch a one-task
                  batch to verify the team, provider, worker, and artifact path.
                </p>
              </DocsCallout>
              <div className="flex flex-wrap gap-2">
                <Link
                  to="/providers/new"
                  className={cn(
                    LINK_BUTTON,
                    "border-accent bg-accent text-white hover:bg-accent-hover",
                  )}
                >
                  Create provider
                </Link>
                <Link
                  to="/batches/new"
                  className={cn(
                    LINK_BUTTON,
                    "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                  )}
                >
                  Launch first batch
                </Link>
                <Link
                  to="/monitor"
                  className={cn(
                    LINK_BUTTON,
                    "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                  )}
                >
                  Open monitor
                </Link>
                <Link
                  to="/settings"
                  className={cn(
                    LINK_BUTTON,
                    "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                  )}
                >
                  Team settings
                </Link>
              </div>
            </div>
          ) : null}

          {accept.isError ? <ErrorState error={accept.error} /> : null}
        </Card.Body>
      </Card>
    </div>
  );
}
