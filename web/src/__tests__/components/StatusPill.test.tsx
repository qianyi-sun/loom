import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { StatusPill } from "../../components/StatusPill";
import { trialStateVariant } from "../../lib/statusVariant";

describe("StatusPill", () => {
  it("renders its children as the visible label", () => {
    render(<StatusPill variant="success">Done</StatusPill>);
    expect(screen.getByText("Done")).toBeInTheDocument();
  });

  it("does NOT set a role — pills decorate cells, they shouldn't be live regions", () => {
    const { container } = render(
      <StatusPill variant="success">Done</StatusPill>,
    );
    const span = container.querySelector("span");
    expect(span).not.toBeNull();
    expect(span!.getAttribute("role")).toBeNull();
  });

  it("applies the variant's color classes", () => {
    const { container } = render(
      <StatusPill variant="failed">Failed</StatusPill>,
    );
    const span = container.querySelector("span")!;
    expect(span.className).toMatch(/bg-red-50/);
    expect(span.className).toMatch(/text-red-700/);
  });

  it("merges caller className alongside the variant classes", () => {
    const { container } = render(
      <StatusPill variant="info" className="custom-extra">
        Info
      </StatusPill>,
    );
    const span = container.querySelector("span")!;
    expect(span.className).toMatch(/custom-extra/);
    expect(span.className).toMatch(/bg-sky-50/);
  });

  it("forwards arbitrary attributes (aria-label, data-*)", () => {
    const { container } = render(
      <StatusPill variant="info" aria-label="state: running" data-state="running">
        Running
      </StatusPill>,
    );
    const span = container.querySelector("span")!;
    expect(span.getAttribute("aria-label")).toBe("state: running");
    expect(span.getAttribute("data-state")).toBe("running");
  });
});

describe("trialStateVariant", () => {
  it("maps known states to their visual variants", () => {
    expect(trialStateVariant("succeeded")).toBe("success");
    expect(trialStateVariant("running")).toBe("running");
    expect(trialStateVariant("claimed")).toBe("running");
    expect(trialStateVariant("queued")).toBe("queued");
    expect(trialStateVariant("submitted")).toBe("queued");
    expect(trialStateVariant("failed")).toBe("failed");
    expect(trialStateVariant("failed_terminal")).toBe("failed");
    expect(trialStateVariant("cancelled")).toBe("cancelled");
  });

  it("falls back to neutral for unknown states so a new server-side state doesn't crash the UI", () => {
    expect(trialStateVariant("warp_drive")).toBe("neutral");
    expect(trialStateVariant("")).toBe("neutral");
  });
});
