import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api";
import App from "../../App";
import { RootErrorBoundary } from "../../components/RootErrorBoundary";
import { renderWithProviders } from "../../test-utils/renderWithProviders";
import { jsonResponse } from "../../test-utils/fetchMock";

afterEach(() => vi.restoreAllMocks());
function mockSession() {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.includes("/auth/me")) return jsonResponse({
      user: { id: "u", username: "Ada", email: "ada@example.test", display_name: "Ada", is_platform_admin: false },
      scopes: ["read"], role: "member", is_platform_admin: false, csrf_token: "test-csrf",
      teams: [{ id: "t", name: "Research", role: "member" }],
      current_team: { id: "t", name: "Research", role: "member" },
    });
    if (url.includes("/monitor/summary")) return jsonResponse({ detail: "Fixture capacity unavailable" }, 503);
    return jsonResponse({ items: [], next_cursor: null });
  });
}

describe("actual App query navigation", () => {
  it.each([
    ["/monitor?view=trials", /Search trials/],
    ["/library", /Name, description, or ID/],
  ])("preserves sequential typing and focus at %s", async (route, placeholder) => {
    mockSession();
    renderWithProviders(<RootErrorBoundary><App /></RootErrorBoundary>, { route });
    const input = await screen.findByPlaceholderText(placeholder);
    const user = userEvent.setup();
    await user.click(input);
    await user.keyboard("a");
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 350)); });
    expect(input).toHaveFocus();
    await user.keyboard("b");
    await waitFor(() => expect(input).toHaveValue("ab"));
    expect(input).toHaveFocus();
  });

  it.each(["setup", "reset"])("explains a missing %s link without consuming a token", async (mode) => {
    const spy = mockSession();
    renderWithProviders(<RootErrorBoundary><App /></RootErrorBoundary>, { route: `/auth/${mode}` });
    expect(await screen.findByRole("alert")).toHaveTextContent(/link is missing/i);
    expect(screen.queryByText(/\[object Object\]/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Go to sign in/ })).toHaveAttribute("href", "/auth/login");
    expect(spy.mock.calls.some(([url]) => /\/(setup|reset)\/(lookup|complete)/.test(String(url)))).toBe(false);
  });
});

it("keeps signed-in guide connection complete", async () => {
  mockSession();
  renderWithProviders(<App />, { route: "/getting-started" });
  expect(await screen.findByText("Connected as Ada · Research")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: /^Sign in$/ })).not.toBeInTheDocument();
});

it.each(["setup", "reset"] as const)("explains rejected %s links without claiming a specific private state", async (mode) => {
  mockSession();
  vi.spyOn(api, mode === "setup" ? "setupLookup" : "resetLookup").mockRejectedValue({ status: 400, detail: "invalid or expired token" });
  renderWithProviders(<App />, { route: `/auth/${mode}?token=invalid-test-link-credential` });
  expect(await screen.findByRole("alert")).toHaveTextContent("The link is invalid, expired, or already used.");
  expect(screen.queryByLabelText("Password")).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Go to sign in" })).toBeInTheDocument();
});
