import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import CommandSnippet from "../../components/CommandSnippet";

describe("CommandSnippet", () => {
  it("renders a labeled command and copies it to the clipboard", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <CommandSnippet
        label="CLI login"
        command="loom auth whoami"
        helperText="Run after login to verify team and scopes."
      />,
    );

    expect(screen.getByText("CLI login")).toBeInTheDocument();
    expect(screen.getByText("loom auth whoami")).toBeInTheDocument();
    expect(
      screen.getByText("Run after login to verify team and scopes."),
    ).toBeInTheDocument();

    await userEvent.click(
      screen.getByRole("button", { name: /copy CLI login/i }),
    );

    expect(writeText).toHaveBeenCalledWith("loom auth whoami");
    expect(await screen.findByText("Copied")).toBeInTheDocument();
  });

  it("wraps long commands inside the command block", () => {
    render(
      <CommandSnippet
        command={
          "loom eval batch create --benchmark humaneval --agent litellm --provider smoke-openai --model gpt-4o-mini"
        }
      />,
    );

    const block = screen.getByText(/loom eval batch create/).closest("pre");
    expect(block).toHaveClass("whitespace-pre-wrap");
    expect(block).toHaveClass("break-words");
  });
});
