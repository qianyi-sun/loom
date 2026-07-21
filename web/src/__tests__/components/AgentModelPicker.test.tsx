import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  AgentModelPicker,
  type AgentModelValue,
} from "../../components/AgentModelPicker";
import type { FetchMock } from "../../test-utils/fetchMock";

const INITIAL_VALUE: AgentModelValue = {
  agentName: "litellm",
  source: "api",
  modelProvider: "",
  modelName: "",
};

function mockPickerEndpoints(
  modelSources: string[] = ["api", "local-server", "hf"],
): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url =
        input instanceof Request
          ? input.url
          : typeof input === "string"
            ? input
            : String(input);
      const json = (body: unknown) =>
        Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      if (url.includes("/api/v1/agents")) {
        return json({
          items: [
            {
              name: "litellm",
              needs_model: true,
              kind: "builtin",
              description: "Multi-provider tool-loop agent.",
              supported_providers: ["*"],
              supported_model_sources: modelSources,
            },
            {
              name: "terminus-2",
              needs_model: true,
              kind: "adapter",
              description: "Terminal-Bench Terminus adapter.",
              supported_providers: ["*"],
              supported_model_sources: ["api"],
            },
          ],
        });
      }
      if (url.includes("/api/v1/provider-connections")) {
        return json({
          items: [
            {
              id: "conn-1",
              name: "Lab vLLM",
              type: "openai-compatible",
              status: "valid",
              rate_card_provider: "openai",
            },
            {
              id: "conn-mz",
              name: "mz_tn_canada_qianyi",
              type: "openai-compatible",
              status: "valid",
              rate_card_provider: "openai",
            },
          ],
        });
      }
      if (url.includes("/api/v1/models")) {
        return json({
          items: [
            {
              provider: "openai",
              name: "deepseek-chat",
              provider_connection_id: "conn-1",
              provider_connection_name: "Lab vLLM",
              provider_connection_type: "openai-compatible",
              source: "discovered",
              agent_capable: true,
              recommended: true,
              visibility: "default",
              hidden_reason: null,
              last_preflight_status: "failed",
              last_preflight_http_status: 403,
              last_preflight_error_code: "access-denied",
              last_preflight_error_message: "HTTP 403 from upstream: [REDACTED]",
            },
            {
              provider: "openai",
              name: "gpt-4o-mini",
              provider_connection_id: "conn-mz",
              provider_connection_name: "mz_tn_canada_qianyi",
              provider_connection_type: "openai-compatible",
              source: "discovered",
              agent_capable: true,
              recommended: true,
              visibility: "default",
              hidden_reason: null,
              last_preflight_status: null,
              last_preflight_http_status: null,
              last_preflight_error_code: null,
              last_preflight_error_message: null,
            },
          ],
        });
      }
      if (url.includes("/api/v1/local-servers")) {
        return json({ items: [] });
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function Harness(): JSX.Element {
  const [value, setValue] = useState<AgentModelValue>(INITIAL_VALUE);
  return <AgentModelPicker value={value} onChange={setValue} />;
}

function renderPicker(): void {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={qc}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <Harness />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("AgentModelPicker copy", () => {
  beforeEach(() => {
    window.localStorage.setItem("loom_token", "t");
    mockPickerEndpoints();
  });
  afterEach(() => vi.restoreAllMocks());

  it("uses operator-readable raw and manual model labels", async () => {
    const user = userEvent.setup();
    renderPicker();

    expect(
      await screen.findByRole("checkbox", {
        name: /Include hidden\/discovered models/i,
      }),
    ).toBeInTheDocument();

    await user.selectOptions(
      screen.getByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );

    expect(
      screen.getByText(/Use this for a model ID that exists on the selected provider/i),
    ).toBeInTheDocument();
  });

  it("uses linked keyboard-operable tabs for model sources", async () => {
    const user = userEvent.setup();
    renderPicker();

    const list = await screen.findByRole("tablist", { name: "Model source" });
    const providerTab = screen.getByRole("tab", { name: "Provider API" });
    const localTab = screen.getByRole("tab", { name: "Local server" });
    const providerPanel = document.getElementById(
      providerTab.getAttribute("aria-controls") ?? "missing",
    );
    expect(list).toContainElement(providerTab);
    expect(providerPanel).toHaveAttribute("aria-labelledby", providerTab.id);
    expect(providerTab).toHaveAttribute("tabindex", "0");

    providerTab.focus();
    await user.keyboard("{ArrowRight}");
    expect(localTab).toHaveFocus();
    expect(localTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveAttribute(
      "aria-labelledby",
      localTab.id,
    );
    expect(screen.getByText(/No local servers are configured/i)).toBeInTheDocument();

    await user.keyboard("{End}");
    expect(screen.getByRole("tab", { name: "HuggingFace" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByText("HuggingFace model id")).toBeInTheDocument();
  });

  it("warns before submit when a selected provider model failed preflight", async () => {
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      await screen.findByRole("option", { name: /deepseek-chat/i }),
    );

    expect(
      screen.getByText(/This model failed its last preflight/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/access-denied/i)).toBeInTheDocument();
  });

  it("keeps mz_tn_canada_qianyi models visible for terminus-2", async () => {
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Agent$/i),
      await screen.findByRole("option", { name: "terminus-2" }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /mz_tn_canada_qianyi/i }),
    );

    expect(
      await screen.findByRole("option", { name: /gpt-4o-mini/i }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/No discovered models match this agent and search/i),
    ).not.toBeInTheDocument();
  });
});
