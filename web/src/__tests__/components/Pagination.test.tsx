import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  default as Pagination,
} from "../../components/Pagination";
import {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../../components/paginationState";

describe("Pagination helpers", () => {
  it("initial state has no cursor and empty stack", () => {
    expect(initialPage).toEqual({ current: null, stack: [] });
  });

  it("nextPage pushes the previous cursor onto the stack", () => {
    let s: PageState = initialPage;
    s = nextPage(s, "c1");
    expect(s).toEqual({ current: "c1", stack: [null] });
    s = nextPage(s, "c2");
    expect(s).toEqual({ current: "c2", stack: [null, "c1"] });
  });

  it("prevPage pops back to the previous cursor", () => {
    let s: PageState = {
      current: "c3",
      stack: [null, "c1", "c2"],
    };
    s = prevPage(s);
    expect(s).toEqual({ current: "c2", stack: [null, "c1"] });
    s = prevPage(s);
    expect(s).toEqual({ current: "c1", stack: [null] });
    s = prevPage(s);
    expect(s).toEqual({ current: null, stack: [] });
  });

  it("prevPage is no-op when the stack is empty", () => {
    const s = prevPage(initialPage);
    expect(s).toEqual(initialPage);
  });

  it("explains previous and next page controls on hover", () => {
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null, "cursor-1"] }}
        hasNext
        onNext={() => undefined}
        onPrev={() => undefined}
      />,
    );

    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "title",
      "Go back to the previous page of results.",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "title",
      "Load the next page of results.",
    );
  });

  it("aria-blocks loading controls without removing them from focus order", () => {
    const onNext = vi.fn();
    const onPrev = vi.fn();
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null] }}
        hasNext
        isLoading
        onNext={onNext}
        onPrev={onPrev}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Loading page 2");
    const previous = screen.getByRole("button", { name: /previous page/i });
    const next = screen.getByRole("button", { name: /next page/i });
    expect(previous).not.toHaveAttribute("disabled");
    expect(next).not.toHaveAttribute("disabled");
    expect(previous).toHaveAttribute("aria-disabled", "true");
    expect(next).toHaveAttribute("aria-disabled", "true");
    fireEvent.click(previous);
    fireEvent.click(next);
    expect(onPrev).not.toHaveBeenCalled();
    expect(onNext).not.toHaveBeenCalled();
  });

  it("keeps previous and retry available after an error while blocking next", () => {
    const retry = vi.fn();
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null] }}
        hasNext
        isError
        onNext={() => undefined}
        onPrev={() => undefined}
        onRetry={retry}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 2 could not be loaded",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "false",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    fireEvent.click(screen.getByRole("button", { name: /retry page/i }));
    expect(retry).toHaveBeenCalledTimes(1);
  });

  it("keeps terminal Next focused and guards keyboard activation", async () => {
    const onNext = vi.fn();
    const user = userEvent.setup();
    render(
      <Pagination
        state={{ current: "cursor-4", stack: [null, "cursor-2", "cursor-3"] }}
        hasNext={false}
        onNext={onNext}
        onPrev={() => undefined}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 4, end of results",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toHaveAttribute(
      "aria-disabled",
      "false",
    );
    const next = screen.getByRole("button", { name: /next page/i });
    expect(next).not.toHaveAttribute("disabled");
    expect(next).toHaveAttribute("aria-disabled", "true");
    next.focus();
    await user.keyboard("{Enter}");
    expect(onNext).not.toHaveBeenCalled();
    expect(next).toHaveFocus();
  });

  it("coalesces concurrent activation before loading state renders", () => {
    const onNext = vi.fn();
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null] }}
        hasNext
        onNext={onNext}
        onPrev={() => undefined}
      />,
    );

    const next = screen.getByRole("button", { name: /next page/i });
    fireEvent.click(next);
    fireEvent.click(next);
    expect(onNext).toHaveBeenCalledTimes(1);
  });
});
