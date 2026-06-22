import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import ErrorState from "../../components/ErrorState";

describe("ErrorState", () => {
  it("redacts secret-like API error details before rendering", () => {
    const { container } = render(
      <ErrorState
        error={{
          status: 502,
          detail:
            "upstream failed for loom_api_abcdefghijklmnopqrstuvwxyz012345 via http://minio.internal/a?X-Amz-Signature=abc",
        }}
      />,
    );

    expect(screen.getByText("Error 502")).toBeInTheDocument();
    expect(container.textContent).not.toContain("loom_api_");
    expect(container.textContent).not.toContain("minio.internal");
    expect(container.textContent).not.toContain("X-Amz-Signature=abc");
    expect(container.textContent).toContain("[REDACTED]");
  });
});
