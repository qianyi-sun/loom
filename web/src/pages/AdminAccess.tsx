import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import {
  api,
  type AdminAuditEvent,
  type InviteEntry,
  type InviteReveal,
  type InviteRole,
  type InviteStatus,
  type TeamRegistrationApproval,
} from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";

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

type RevealedInvite = TeamRegistrationApproval | InviteReveal;

export default function AdminAccess(): JSX.Element {
  const [actor, setActor] = useState("");
  const [rejectedReason, setRejectedReason] = useState<Record<string, string>>({});
  const [revealed, setRevealed] = useState<RevealedInvite | null>(null);
  const [inviteStatus, setInviteStatus] = useState<InviteStatus>("pending");
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteTeamId, setInviteTeamId] = useState("");
  const [inviteRole, setInviteRole] = useState<InviteRole>("member");
  const [inviteMaxUses, setInviteMaxUses] = useState("1");
  const [inviteDomain, setInviteDomain] = useState("");
  const queryClient = useQueryClient();

  const registrations = useQuery({
    queryKey: ["admin", "team-registrations", "pending"],
    queryFn: () => api.listTeamRegistrations("pending"),
  });
  const audit = useQuery({
    queryKey: ["admin", "audit-events"],
    queryFn: () => api.listAdminAuditEvents(50),
  });
  const invites = useQuery({
    queryKey: ["invites", inviteStatus],
    queryFn: () => api.listInvites({ status: inviteStatus }),
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

  const actorMissing = actor.trim().length === 0;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team access</h1>
        <p className="mt-1 text-sm text-slate-500">
          Approve pending team registrations and audit access decisions.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Admin actor"
          description="Recorded in audit events for approve and reject actions."
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
    </div>
  );
}
