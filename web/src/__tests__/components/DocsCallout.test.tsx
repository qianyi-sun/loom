import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import DocsCallout from "../../components/DocsCallout";

describe("DocsCallout", () => {
  it("renders a compact titled guidance panel", () => {
    render(
      <DocsCallout title="Next steps">
        <p>Run a smoke batch.</p>
      </DocsCallout>,
    );

    expect(
      screen.getByRole("heading", { name: "Next steps" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Run a smoke batch.")).toBeInTheDocument();
  });

  it("supports an informational tone", () => {
    render(
      <DocsCallout title="Provider setup" tone="info">
        <p>Use env-based API keys.</p>
      </DocsCallout>,
    );

    expect(screen.getByText("Provider setup").closest("section")).toHaveClass(
      "border-blue-200",
    );
  });
});
