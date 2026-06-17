import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api, type AdminAuditEvent, type TeamRegistrationApproval } from "../api/client";
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

function downloadToken(token: string, teamName: string): void {
  const blob = new Blob([`${token}\n`], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${teamName}-loom-token.txt`;
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

export default function AdminAccess(): JSX.Element {
  const [actor, setActor] = useState("");
  const [rejectedReason, setRejectedReason] = useState<Record<string, string>>({});
  const [revealed, setRevealed] = useState<TeamRegistrationApproval | null>(null);
  const queryClient = useQueryClient();

  const registrations = useQuery({
    queryKey: ["admin", "team-registrations", "pending"],
    queryFn: () => api.listTeamRegistrations("pending"),
  });
  const audit = useQuery({
    queryKey: ["admin", "audit-events"],
    queryFn: () => api.listAdminAuditEvents(50),
  });

  const approve = useMutation({
    mutationFn: (id: string) => api.approveTeamRegistration(id, actor.trim()),
    onSuccess: (data) => {
      setRevealed(data);
      queryClient.invalidateQueries({ queryKey: ["admin", "team-registrations"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });
  const reject = useMutation({
    mutationFn: (id: string) => api.rejectTeamRegistration(id, actor.trim(), rejectedReason[id]),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "team-registrations"] });
      queryClient.invalidateQueries({ queryKey: ["admin", "audit-events"] });
    },
  });

  const actorMissing = actor.trim().length === 0;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team access</h1>
      </header>

      <Card>
        <Card.Header title="Admin actor" />
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
          <Card.Header title="Approved team token" />
          <Card.Body className="space-y-3">
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 font-mono text-sm text-emerald-900">
              {revealed.team_token}
            </div>
            <div className="flex gap-2">
              <Button
                size="sm"
                onClick={() => navigator.clipboard.writeText(revealed.team_token)}
              >
                Copy
              </Button>
              <Button
                size="sm"
                onClick={() => downloadToken(revealed.team_token, revealed.team.name)}
              >
                Download
              </Button>
            </div>
          </Card.Body>
        </Card>
      ) : null}

      <Card>
        <Card.Header title="Pending registrations" />
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
        <Card.Header title="Audit log" />
        <Card.Body>
          {audit.isPending ? <LoadingState /> : null}
          {audit.isError ? <ErrorState error={audit.error} /> : null}
          {audit.data ? <AuditRows events={audit.data.items} /> : null}
        </Card.Body>
      </Card>
    </div>
  );
}
