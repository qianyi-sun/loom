import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CopyableId } from "../../components/CopyableId";
import PipelineDomainOutcomeSummary from "../../components/pipelines/PipelineDomainOutcomeSummary";

describe("pipeline display utilities", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("shortens, copies, and restores a long identifier", () => {
    vi.useFakeTimers();
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    render(<CopyableId value="artifact-1234567890-abcdefghij" chars={5} className="extra" />);

    const button = screen.getByRole("button", { name: "artif...fghij" });
    expect(button).toHaveClass("extra");
    expect(button).toHaveAttribute("title", "Copy artifact-1234567890-abcdefghij");
    fireEvent.click(button);
    expect(writeText).toHaveBeenCalledWith("artifact-1234567890-abcdefghij");
    expect(button).toHaveAttribute("title", "Copied");

    act(() => vi.advanceTimersByTime(1200));
    expect(button).toHaveAttribute("title", "Copy artifact-1234567890-abcdefghij");
  });

  it("keeps short identifiers intact when clipboard access is unavailable", () => {
    vi.stubGlobal("navigator", {});
    render(<CopyableId value="short-id" />);

    expect(screen.getByRole("button", { name: "short-id" })).toHaveTextContent("short-id");
    fireEvent.click(screen.getByRole("button", { name: "short-id" }));
    expect(screen.getByRole("button", { name: "short-id" })).toHaveAttribute("title", "Copied");
  });

  it("sorts and bounds dense domain outcomes", () => {
    render(<PipelineDomainOutcomeSummary outcomes={{
      zeta: 1,
      alpha: 2,
      epsilon: 3,
      beta: 4,
      delta: 5,
      gamma: 6,
    }} />);

    expect(screen.getByLabelText("Succeeded stage domain outcomes")).toHaveTextContent(
      "alpha × 2, beta × 4, delta × 5, epsilon × 3, gamma × 6 +1 more",
    );
  });
});
