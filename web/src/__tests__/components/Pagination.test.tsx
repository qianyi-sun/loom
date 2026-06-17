import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  default as Pagination,
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../../components/Pagination";

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
});
