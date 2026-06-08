/**
 * Renders an array of trajectory events with per-kind summary lines.
 * Click a row to expand its full JSON payload.
 */

import { useState } from "react";

import type { components } from "../api/schema";
import JsonViewer from "./JsonViewer";

type Event = components["schemas"]["TrajectoryEvent"];

function summary(e: Event): string {
  switch (e.kind) {
    case "trial_start":
      return "Trial started";
    case "trial_end": {
      const finalState = (e.final_state as string | undefined) ?? "?";
      return `Trial ended — ${finalState}`;
    }
    case "step_start":
      return `Step ${e.step_id ?? "?"} started`;
    case "step_end": {
      const reward = (e.reward as number | undefined) ?? null;
      return `Step ${e.step_id ?? "?"} ended${
        reward !== null ? ` — reward ${reward}` : ""
      }`;
    }
    case "llm_call": {
      const model = (e.model as string | undefined) ?? "?";
      const inT = (e.input_tokens as number | undefined) ?? 0;
      const outT = (e.output_tokens as number | undefined) ?? 0;
      const cost = (e.cost_usd_snapshot as number | undefined) ?? null;
      return `LLM call — ${model} (${inT} in, ${outT} out${
        cost !== null ? `, $${cost.toFixed(4)}` : ""
      })`;
    }
    case "tool_use":
      return `Tool use — ${(e.tool_name as string | undefined) ?? "?"}`;
    case "agent_thought":
      return `Agent thought (${
        ((e.text as string | undefined) ?? "").slice(0, 60)
      }…)`;
    case "env_exec":
      return `Env exec — ${(e.command as string | undefined) ?? "?"}`;
    default:
      return e.kind;
  }
}

function Row({ event }: { event: Event }): JSX.Element {
  const [open, setOpen] = useState(false);
  return (
    <div className={`loom-event-row ${event.kind}`}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          cursor: "pointer",
        }}
        onClick={() => setOpen((v) => !v)}
      >
        <span>
          <strong>{summary(event)}</strong>
          {event.step_id ? (
            <span className="loom-muted"> · step={event.step_id}</span>
          ) : null}
        </span>
        <span className="loom-muted loom-mono">
          {event.emitted_at?.slice(11, 23) ?? ""}
        </span>
      </div>
      {open ? <JsonViewer data={event} /> : null}
    </div>
  );
}

export default function EventTimeline({
  events,
}: {
  events: Event[];
}): JSX.Element {
  if (events.length === 0) {
    return <div className="loom-empty">No events yet.</div>;
  }
  return (
    <div>
      {events.map((e, i) => (
        <Row key={`${e.seq ?? i}-${e.kind}`} event={e} />
      ))}
    </div>
  );
}
