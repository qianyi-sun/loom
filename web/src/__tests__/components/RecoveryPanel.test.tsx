import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { RecoveryPanel } from "../../components/RecoveryPanel";

describe("RecoveryPanel", () => {
  it("focuses one main landmark and exposes distinct keyboard recovery actions", async () => {
    const onRetry = vi.fn();
    const onReload = vi.fn();
    const user = userEvent.setup();

    render(
      <RecoveryPanel
        title="Loom could not start"
        message="Runtime configuration is temporarily unavailable."
        referenceId="WEB-1234ABCD"
        onRetry={onRetry}
        onReload={onReload}
        homeHref="/prod/"
      />,
    );

    const main = screen.getByRole("main");
    await waitFor(() => expect(main).toHaveFocus());
    expect(main).toHaveAttribute("id", "main-content");
    expect(screen.getAllByRole("main")).toHaveLength(1);
    expect(screen.getByRole("alert")).toHaveTextContent("WEB-1234ABCD");
    expect(
      screen.getByRole("link", { name: "Skip to main content" }),
    ).toHaveAttribute("href", "#main-content");

    const retry = screen.getByRole("button", { name: "Retry" });
    const reload = screen.getByRole("button", { name: "Reload Loom" });
    const home = screen.getByRole("link", { name: "Go to Loom home" });
    for (const action of [retry, reload, home]) {
      expect(action).toHaveClass("min-h-11", "min-w-11");
    }
    expect(retry).toHaveAttribute("type", "button");
    expect(reload).toHaveAttribute("type", "button");
    expect(home).toHaveAttribute("href", "/prod/");

    await user.tab();
    expect(retry).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(onRetry).toHaveBeenCalledTimes(1);

    await user.tab();
    expect(reload).toHaveFocus();
    await user.keyboard(" ");
    expect(onReload).toHaveBeenCalledTimes(1);

    await user.tab();
    expect(home).toHaveFocus();
  });

  it("uses an alert section without nesting document landmarks for route failures", async () => {
    render(
      <main id="main-content">
        <RecoveryPanel
          scope="route"
          title="Loom could not display this section"
          message="This route failed safely."
          referenceId="WEB-ABCDEF12"
          onRetry={() => undefined}
          onReload={() => undefined}
          homeHref="/dev/"
        />
      </main>,
    );

    const alert = screen.getByRole("alert");
    await waitFor(() => expect(alert).toHaveFocus());
    expect(screen.getAllByRole("main")).toHaveLength(1);
    expect(screen.getAllByRole("alert")).toHaveLength(1);
    expect(
      screen.queryByRole("link", { name: "Skip to main content" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Go to Loom home" }),
    ).toHaveAttribute("href", "/dev/");
  });
});
