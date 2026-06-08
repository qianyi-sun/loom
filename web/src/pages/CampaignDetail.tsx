import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";

import { api } from "../api/client";
import ErrorState from "../components/ErrorState";
import JsonViewer from "../components/JsonViewer";
import LoadingState from "../components/LoadingState";

const ACTIVE_STATES = new Set(["submitted", "running"]);

export default function CampaignDetail(): JSX.Element {
  const { campaignId } = useParams<{ campaignId: string }>();
  const queryClient = useQueryClient();

  const query = useQuery({
    queryKey: ["campaign", campaignId],
    queryFn: () => api.getCampaign(campaignId!),
    enabled: !!campaignId,
    // Live-poll while the campaign is fanning out; once terminal,
    // stop hitting the API.
    refetchInterval: (q) => {
      const data = q.state.data as
        | { state: string }
        | undefined;
      return data && ACTIVE_STATES.has(data.state) ? 5000 : false;
    },
  });

  const cancel = useMutation({
    mutationFn: () => api.cancelCampaign(campaignId!),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["campaign", campaignId] }),
  });

  if (!campaignId) {
    return <ErrorState error={new Error("missing campaignId")} />;
  }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;
  const c = query.data;

  return (
    <>
      <p>
        <Link to="/campaigns">← All campaigns</Link>
      </p>
      <div className="loom-card">
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
          }}
        >
          <div>
            <h1 style={{ margin: 0 }}>{c.name}</h1>
            {c.description ? (
              <p className="loom-muted">{c.description}</p>
            ) : null}
            <p className="loom-mono loom-muted">id={c.id}</p>
          </div>
          <span className={`loom-state-pill ${c.state}`}>{c.state}</span>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
            gap: "0.6rem",
            marginTop: "1rem",
          }}
        >
          <Stat label="Expected" value={String(c.expected_trial_count)} />
          <Stat
            label="Reward (avg)"
            value={
              c.aggregate_reward != null
                ? c.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <Stat label="Cost" value={`$${c.total_cost_usd.toFixed(4)}`} />
          <Stat
            label="Created"
            value={c.created_at.slice(0, 16).replace("T", " ")}
          />
          <Stat
            label="Finished"
            value={c.finished_at?.slice(0, 16).replace("T", " ") ?? "—"}
          />
        </div>

        {ACTIVE_STATES.has(c.state) ? (
          <div style={{ marginTop: "1rem" }}>
            <button
              onClick={() => cancel.mutate()}
              disabled={cancel.isPending}
            >
              {cancel.isPending ? "Cancelling…" : "Cancel campaign"}
            </button>
            {cancel.isError ? (
              <div style={{ marginTop: "0.6rem" }}>
                <ErrorState error={cancel.error} />
              </div>
            ) : null}
          </div>
        ) : null}
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Trial summary</h2>
        <table>
          <thead>
            <tr>
              <th>State</th>
              <th>Count</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(c.trial_summary).map(([state, n]) => (
              <tr key={state}>
                <td>
                  <span className={`loom-state-pill ${state}`}>{state}</span>
                </td>
                <td>{n}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Filter + config</h2>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "1rem",
          }}
        >
          <div>
            <div className="loom-muted">task_filter</div>
            <JsonViewer data={c.task_filter} />
          </div>
          <div>
            <div className="loom-muted">trial_config</div>
            <JsonViewer data={c.trial_config} />
          </div>
        </div>
      </div>
    </>
  );
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: string;
}): JSX.Element {
  return (
    <div>
      <div className="loom-muted" style={{ fontSize: "0.8em" }}>
        {label}
      </div>
      <div>{value}</div>
    </div>
  );
}
