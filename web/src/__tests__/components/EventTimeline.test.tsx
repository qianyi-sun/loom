import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import EventTimeline from "../../components/EventTimeline";

describe("EventTimeline", () => {
  it("renders an empty state when there are no events", () => {
    render(<EventTimeline events={[]} />);
    expect(screen.getByText(/No events yet/i)).toBeInTheDocument();
  });

  it("renders a summary per event with a state-coded class", () => {
    render(
      <EventTimeline
        events={[
          { kind: "trial_start", seq: 0, emitted_at: "2026-06-08T12:00:00Z" },
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: "gpt-4",
            input_tokens: 100,
            output_tokens: 50,
            emitted_at: "2026-06-08T12:00:01Z",
          },
          { kind: "trial_end", seq: 2, emitted_at: "2026-06-08T12:00:02Z" },
        ]}
      />,
    );
    expect(screen.getByText(/Trial started/i)).toBeInTheDocument();
    expect(screen.getByText(/LLM call — gpt-4/)).toBeInTheDocument();
    expect(screen.getByText(/Trial ended/)).toBeInTheDocument();
  });

  it("formats object model specs in LLM call summaries", () => {
    render(
      <EventTimeline
        events={[
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: {
              provider: "openai",
              name: "gpt-4o-mini",
              source: "api",
            },
            input_tokens: 39,
            output_tokens: 97,
            cost_usd_snapshot: 0,
            emitted_at: "2026-06-08T12:00:01Z",
          },
        ]}
      />,
    );
    expect(screen.getByText(/LLM call — openai\/gpt-4o-mini/)).toBeInTheDocument();
    expect(screen.queryByText(/\[object Object\]/)).not.toBeInTheDocument();
  });

  it("expands a row to show the full JSON on click", async () => {
    const user = userEvent.setup();
    render(
      <EventTimeline
        events={[
          {
            kind: "llm_call",
            seq: 1,
            step_id: "main",
            model: "gpt-4",
            input_tokens: 100,
            output_tokens: 50,
            emitted_at: "2026-06-08T12:00:00Z",
          },
        ]}
      />,
    );
    // Click the summary row to expand.
    await user.click(screen.getByText(/LLM call — gpt-4/));
    // The raw JSON contains the model name.
    const all = screen.getAllByText(/gpt-4/);
    // One in summary, one in JSON viewer when expanded.
    expect(all.length).toBeGreaterThanOrEqual(2);
  });
});
