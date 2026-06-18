import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import RotateKeyModal from "../../../components/providers/RotateKeyModal";

describe("RotateKeyModal", () => {
  it("submit is disabled until both fields filled + name matches", async () => {
    const user = userEvent.setup();
    render(
      <RotateKeyModal connectionName="prod-anthropic"
        onClose={vi.fn()} onSubmit={vi.fn()} />,
    );
    const submit = screen.getByRole("button", { name: /rotate/i });
    expect(submit).toBeDisabled();
    await user.type(screen.getByLabelText(/new api key/i), "sk-new");
    expect(submit).toBeDisabled();
    await user.type(screen.getByLabelText(/type connection name/i), "prod-anthropic");
    expect(submit).not.toBeDisabled();
  });

  it("submit calls onSubmit with the new key", async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(
      <RotateKeyModal connectionName="x" onClose={vi.fn()} onSubmit={onSubmit} />,
    );
    await user.type(screen.getByLabelText(/new api key/i), "sk-new");
    await user.type(screen.getByLabelText(/type connection name/i), "x");
    await user.click(screen.getByRole("button", { name: /rotate/i }));
    expect(onSubmit).toHaveBeenCalledWith("sk-new");
  });

  it("close calls onClose", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <RotateKeyModal connectionName="x" onClose={onClose} onSubmit={vi.fn()} />,
    );
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(onClose).toHaveBeenCalled();
  });
});
