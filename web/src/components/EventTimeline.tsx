/**
 * Trajectory event timeline. Each event renders as an expandable
 * row with a coloured tag for its kind. Clicking a row reveals the
 * full JSON payload via the shared JsonViewer.
 *
 * The kind-colour mapping borrows the reference design's pattern of
 * action-type pills (shell_cmd / file_edit / etc.). Loom's event
 * kinds are different (llm_call / tool_use / agent_thought / etc.)
 * but the visual treatment is the same — a coloured badge labels
 * the row at a glance.
 */
import { useState } from "react";

import type { components } from "../api/schema";
import { cn } from "../lib/cn";
import { modelLabel } from "../lib/modelLabel";
import EmptyState from "./EmptyState";
import JsonViewer from "./JsonViewer";

type Event = components["schemas"]["TrajectoryEvent"];

const KIND_BADGE: Record<string, string> = {
  trial_start: "bg-emerald-50 text-emerald-700 border-emerald-100",
  trial_end: "bg-emerald-100 text-emerald-800 border-emerald-200",
  step_start: "bg-amber-50 text-amber-700 border-amber-100",
  step_end: "bg-amber-50 text-amber-700 border-amber-100",
  llm_call: "bg-indigo-50 text-indigo-700 border-indigo-100",
  tool_use: "bg-blue-50 text-blue-700 border-blue-100",
  agent_thought: "bg-purple-50 text-purple-700 border-purple-100",
  env_exec: "bg-sky-50 text-sky-700 border-sky-100",
};

const KIND_DEFAULT = "bg-slate-100 text-slate-700 border-slate-200";

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
      const model = modelLabel(e.model);
      const modelText = model === "—" ? "?" : model;
      const inT = (e.input_tokens as number | undefined) ?? 0;
      const outT = (e.output_tokens as number | undefined) ?? 0;
      const cost = (e.cost_usd_snapshot as number | undefined) ?? null;
      return `LLM call — ${modelText} (${inT} in, ${outT} out${
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
  const badge = KIND_BADGE[event.kind] ?? KIND_DEFAULT;
  return (
    <div className="border-b border-slate-100 last:border-b-0">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left transition-colors hover:bg-slate-50"
        aria-expanded={open}
      >
        <div className="flex min-w-0 items-center gap-2">
          <span
            className={cn(
              "shrink-0 rounded-md border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider",
              badge,
            )}
          >
            {event.kind}
          </span>
          <span className="truncate text-sm text-slate-700">
            {summary(event)}
          </span>
          {event.step_id ? (
            <span className="shrink-0 text-xs text-slate-400">
              step={event.step_id}
            </span>
          ) : null}
        </div>
        <span className="shrink-0 font-mono text-xs text-slate-400">
          {event.emitted_at?.slice(11, 23) ?? ""}
        </span>
      </button>
      {open ? (
        <div className="border-t border-slate-100 bg-slate-50/30 px-3 py-3">
          <JsonViewer data={event} expanded />
        </div>
      ) : null}
    </div>
  );
}

export default function EventTimeline({
  events,
}: {
  events: Event[];
}): JSX.Element {
  if (events.length === 0) {
    return <EmptyState label="No events yet." />;
  }
  return (
    <div className="rounded-lg border border-slate-200 bg-white">
      {events.map((e, i) => (
        <Row key={`${e.seq ?? i}-${e.kind}`} event={e} />
      ))}
    </div>
  );
}
