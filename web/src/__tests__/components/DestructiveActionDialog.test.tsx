import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  DestructiveActionDialog,
  type DestructiveActionDialogProps,
} from "../../components/DestructiveActionDialog";

function props(
  overrides: Partial<DestructiveActionDialogProps> = {},
): DestructiveActionDialogProps {
  return {
    open: true,
    title: "Cancel batch",
    target: "Deterministic batch (batch-user-1)",
    consequence: "Active trials will be cancelled; completed results remain.",
    confirmLabel: "Cancel batch",
    pendingLabel: "Cancelling…",
    confirmation: { type: "simple" },
    pending: false,
    error: null,
    onClose: vi.fn(),
    onConfirm: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

describe("DestructiveActionDialog", () => {
  it("requires an exact case-sensitive typed target", async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn().mockResolvedValue(undefined);
    render(
      <DestructiveActionDialog
        {...props({
          confirmation: {
            type: "typed",
            expected: "prod-anthropic",
            inputLabel: "Type the connection name to confirm",
          },
          onConfirm,
        })}
      />,
    );

    const confirm = screen.getByRole("button", { name: "Cancel batch" });
    const input = screen.getByLabelText(
      "Type the connection name to confirm",
    );
    expect(confirm).toBeDisabled();
    await user.type(input, "Prod-Anthropic");
    expect(confirm).toBeDisabled();
    await user.clear(input);
    await user.type(input, "prod-anthropic");
    expect(confirm).toBeEnabled();
    await user.click(confirm);
    await waitFor(() => expect(onConfirm).toHaveBeenCalledTimes(1));
  });

  it("assigns a unique id to every typed confirmation field", () => {
    render(
      <>
        <DestructiveActionDialog
          {...props({
            target: "provider-a",
            confirmation: {
              type: "typed",
              expected: "provider-a",
              inputLabel: "Confirm provider A",
            },
          })}
        />
        <DestructiveActionDialog
          {...props({
            target: "provider-b",
            confirmation: {
              type: "typed",
              expected: "provider-b",
              inputLabel: "Confirm provider B",
            },
          })}
        />
      </>,
    );
    const ids = screen.getAllByRole("textbox").map((input) => input.id);
    expect(ids.every(Boolean)).toBe(true);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("locks dismissal and duplicate submit before a parent rerender", async () => {
    let release: (() => void) | undefined;
    const request = new Promise<void>((resolve) => {
      release = resolve;
    });
    const onConfirm = vi.fn(() => request);
    const onClose = vi.fn();
    render(
      <DestructiveActionDialog {...props({ onConfirm, onClose })} />,
    );

    const confirm = screen.getByRole("button", { name: "Cancel batch" });
    fireEvent.click(confirm);
    fireEvent.click(confirm);
    fireEvent.keyDown(window, { key: "Escape" });
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onConfirm).toHaveBeenCalledTimes(1);
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Cancelling…" })).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("Cancelling…");
    release?.();
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Cancel" })).toBeEnabled(),
    );
  });

  it.each([400, 403, 409, 500])(
    "keeps a redacted HTTP %s error in the dialog",
    (status) => {
      render(
        <DestructiveActionDialog
          {...props({
            error: {
              status,
              detail:
                "request failed with sk-proj-abcdefghijklmnopqrstuvwxyz0123456789",
            },
          })}
        />,
      );
      expect(screen.getAllByRole("alert")).toHaveLength(1);
      expect(screen.getByRole("alert")).toHaveTextContent(`Error ${status}`);
      expect(screen.getByRole("alert")).toHaveTextContent("[REDACTED]");
      expect(screen.queryByText(/sk-proj-/)).not.toBeInTheDocument();
      expect(screen.getByRole("dialog")).toBeInTheDocument();
    },
  );

  it.each([
    new DOMException("The operation timed out", "TimeoutError"),
    new TypeError("Failed to fetch"),
  ])("renders retryable transport failure", (error) => {
    render(
      <DestructiveActionDialog {...props({ error })} />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent(error.message);
    expect(screen.getByRole("button", { name: "Cancel batch" })).toBeEnabled();
  });

  it("preserves typed context and hides the old alert on retry", async () => {
    let release: (() => void) | undefined;
    const retry = new Promise<void>((resolve) => {
      release = resolve;
    });
    const onConfirm = vi.fn(() => retry);
    const user = userEvent.setup();
    render(
      <DestructiveActionDialog
        {...props({
          confirmation: {
            type: "typed",
            expected: "taskset-1",
            inputLabel: "Type the TaskSet id to confirm",
          },
          error: { status: 409, detail: "conflict" },
          onConfirm,
        })}
      />,
    );

    const input = screen.getByLabelText("Type the TaskSet id to confirm");
    await user.type(input, "taskset-1");
    await user.click(screen.getByRole("button", { name: "Cancel batch" }));
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(input).toHaveValue("taskset-1");
    expect(screen.getByRole("status")).toHaveTextContent("Cancelling…");
    release?.();
    await waitFor(() =>
      expect(screen.queryByRole("status")).not.toBeInTheDocument(),
    );
  });

  it("allows cancel and Escape after a failed confirmation settles", async () => {
    const onClose = vi.fn();
    const onConfirm = vi.fn().mockRejectedValue(new Error("network"));
    const user = userEvent.setup();
    render(
      <DestructiveActionDialog {...props({ onClose, onConfirm })} />,
    );

    await user.click(screen.getByRole("button", { name: "Cancel batch" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Cancel" })).toBeEnabled(),
    );
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
