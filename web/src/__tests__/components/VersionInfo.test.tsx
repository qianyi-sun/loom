import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import VersionInfo from "../../components/VersionInfo";

const { LOADED_REVISION } = vi.hoisted(() => ({
  LOADED_REVISION: "a".repeat(40),
}));

vi.mock("../../lib/buildInfo", async () => {
  const actual =
    await vi.importActual<typeof import("../../lib/buildInfo")>(
      "../../lib/buildInfo",
    );
  return {
    ...actual,
    LOADED_BUILD_INFO: {
      revision: LOADED_REVISION,
      sourceRef: "refs/heads/dev",
      buildTime: "2026-09-22T09:00:00Z",
    },
  };
});

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function renderVersionInfo() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <VersionInfo environmentLabel="Development" />
    </QueryClientProvider>,
  );
}

describe("VersionInfo (#2009)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows the persistent sidebar entry with environment and short loaded commit", () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({}));
    renderVersionInfo();

    expect(
      screen.getByRole("button", { name: "Deployed version details" }),
    ).toHaveTextContent(`Nebius · Development · ${LOADED_REVISION.slice(0, 12)}`);
  });

  it("opens accessible details with the full commit, copy action and a commit link", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({}));
    renderVersionInfo();

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    expect(
      screen.getByRole("dialog", { name: "Deployed version" }),
    ).toBeInTheDocument();
    // CopyableId's accessible name is its truncated display text; the full
    // value lives in its `title` tooltip instead.
    expect(screen.getByTitle(`Copy ${LOADED_REVISION}`)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View commit" })).toHaveAttribute(
      "href",
      `https://github.com/qianyi-sun/loom/commit/${LOADED_REVISION}`,
    );
    expect(screen.getByText("refs/heads/dev")).toBeInTheDocument();
  });

  it("reports the backend as unknown, never blocking, when its version request fails", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("offline"));
    renderVersionInfo();

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    await waitFor(() =>
      expect(screen.getAllByText("unknown").length).toBeGreaterThan(0),
    );
  });

  it("shows a non-disruptive update notice with an explicit refresh action when the served build differs from the loaded one", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("loom-frontend-config.json")) {
        return Promise.resolve(jsonResponse({ buildRevision: "b".repeat(40) }));
      }
      return Promise.resolve(
        jsonResponse({ buildRevision: null, buildTime: null }),
      );
    });
    renderVersionInfo();

    await waitFor(() =>
      expect(screen.getByTitle("A newer build is available")).toBeInTheDocument(),
    );

    await userEvent.click(
      screen.getByRole("button", { name: "Deployed version details" }),
    );

    expect(
      screen.getByText("A newer frontend build is available."),
    ).toBeInTheDocument();
    // Present, but never auto-invoked — refreshing is the user's call.
    expect(
      screen.getByRole("button", { name: "Refresh" }),
    ).toBeInTheDocument();
  });

  it("shows no update notice when the served build matches the loaded one", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("loom-frontend-config.json")) {
        return Promise.resolve(
          jsonResponse({ buildRevision: LOADED_REVISION }),
        );
      }
      return Promise.resolve(
        jsonResponse({ buildRevision: null, buildTime: null }),
      );
    });
    renderVersionInfo();

    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled());
    expect(screen.queryByTitle("A newer build is available")).toBeNull();
  });
});
