import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import AddManualModelModal from "../../../components/providers/AddManualModelModal";

describe("AddManualModelModal", () => {
  it("uses the shared labelled dialog and initially focuses model id", () => {
    render(<AddManualModelModal onClose={vi.fn()} onSubmit={vi.fn()} />);
    const dialog = screen.getByRole("dialog", { name: "Add manual model" });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(dialog).toHaveAttribute("aria-describedby");
    expect(screen.getByRole("textbox", { name: /model id/i })).toHaveFocus();
  });

  it("submit calls onSubmit with model_id + optional display_name", async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(<AddManualModelModal onClose={vi.fn()} onSubmit={onSubmit} />);
    await user.type(screen.getByLabelText(/model id/i), "manual/gpt-x");
    await user.type(screen.getByLabelText(/display name/i), "GPT-X");
    await user.click(screen.getByRole("button", { name: /^add/i }));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ model_id: "manual/gpt-x", display_name: "GPT-X" }),
    );
  });

  it("submit disabled until model_id non-empty", async () => {
    const user = userEvent.setup();
    render(<AddManualModelModal onClose={vi.fn()} onSubmit={vi.fn()} />);
    const submit = screen.getByRole("button", { name: /^add/i });
    expect(submit).toBeDisabled();
    await user.type(screen.getByLabelText(/model id/i), "x");
    expect(submit).not.toBeDisabled();
  });

  it("close calls onClose", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(<AddManualModelModal onClose={onClose} onSubmit={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(onClose).toHaveBeenCalled();
  });

  it("keeps submission disabled while pending", async () => {
    const user = userEvent.setup();
    render(<AddManualModelModal pending onClose={vi.fn()} onSubmit={vi.fn()} />);
    await user.type(screen.getByLabelText(/model id/i), "manual/gpt-x");
    expect(screen.getByRole("button", { name: /^add/i })).toBeDisabled();
  });
});
