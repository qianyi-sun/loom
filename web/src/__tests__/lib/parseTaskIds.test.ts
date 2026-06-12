import { describe, expect, it } from "vitest";

import { parseTaskIds } from "../../lib/parseTaskIds";

const FIVE = [
  "HumanEval/0",
  "HumanEval/1",
  "HumanEval/2",
  "HumanEval/3",
  "HumanEval/4",
];

describe("parseTaskIds", () => {
  it("returns empty without error on empty input", () => {
    const r = parseTaskIds("");
    expect(r.ids).toEqual([]);
    expect(r.error).pilot groupeNull();
  });

  it("parses newline-separated ids", () => {
    const r = parseTaskIds(FIVE.join("\n"));
    expect(r.ids).toEqual(FIVE);
    expect(r.error).pilot groupeNull();
  });

  it("parses comma-separated ids with spaces", () => {
    const r = parseTaskIds(
      "HumanEval/0, HumanEval/1, HumanEval/2, HumanEval/3, HumanEval/4",
    );
    expect(r.ids).toEqual(FIVE);
  });

  it("parses space-separated ids", () => {
    const r = parseTaskIds(
      "HumanEval/0  HumanEval/1  HumanEval/2  HumanEval/3  HumanEval/4",
    );
    expect(r.ids).toEqual(FIVE);
  });

  it("parses tab-separated ids", () => {
    const r = parseTaskIds(FIVE.join("\t"));
    expect(r.ids).toEqual(FIVE);
  });

  it("parses semicolon-separated ids", () => {
    const r = parseTaskIds(FIVE.join(";"));
    expect(r.ids).toEqual(FIVE);
  });

  it("parses pipe-separated ids", () => {
    const r = parseTaskIds(FIVE.join(" | "));
    expect(r.ids).toEqual(FIVE);
  });

  it("parses a JSON array with double quotes", () => {
    const json = JSON.stringify(FIVE);
    const r = parseTaskIds(json);
    expect(r.ids).toEqual(FIVE);
  });

  it("parses a Python list literal with single quotes + trailing comma", () => {
    const r = parseTaskIds(
      "['HumanEval/0','HumanEval/1','HumanEval/2','HumanEval/3','HumanEval/4',]",
    );
    expect(r.ids).toEqual(FIVE);
  });

  it("expands a range shorthand HumanEval/0-4", () => {
    const r = parseTaskIds("HumanEval/0-4");
    expect(r.ids).toEqual(FIVE);
  });

  it("expands a prefix-comma shorthand HumanEval/0,1,2,3,4", () => {
    // After the top-level comma split each piece is just a number,
    // so this case actually exercises the inner numeric-prefix logic
    // only when the segment is preserved (no top-level comma split).
    // We use a bullet line to ensure the inner expansion fires.
    const r = parseTaskIds("- HumanEval/0,1,2,3,4");
    expect(r.ids).toEqual(FIVE);
  });

  it("handles mixed range + list shorthand", () => {
    const r = parseTaskIds("HumanEval/0-2, HumanEval/3, HumanEval/4");
    expect(r.ids).toEqual(FIVE);
  });

  it("strips URL prefixes /api/v1/tasks/ and /tasks/", () => {
    const input = [
      "/api/v1/tasks/HumanEval/0",
      "/api/v1/tasks/HumanEval/1",
      "/tasks/HumanEval/2",
    ].join("\n");
    const r = parseTaskIds(input);
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1", "HumanEval/2"]);
  });

  it("parses markdown bullet list with mixed glyphs", () => {
    const r = parseTaskIds(
      [
        "- HumanEval/0",
        "- HumanEval/1",
        "* HumanEval/2",
        "• HumanEval/3",
        "→ HumanEval/4",
      ].join("\n"),
    );
    expect(r.ids).toEqual(FIVE);
  });

  it("parses a markdown numbered list", () => {
    const r = parseTaskIds(
      [
        "1. HumanEval/0",
        "2. HumanEval/1",
        "3. HumanEval/2",
        "4. HumanEval/3",
        "5. HumanEval/4",
      ].join("\n"),
    );
    expect(r.ids).toEqual(FIVE);
  });

  it("parses a markdown single-column table", () => {
    const r = parseTaskIds(
      [
        "| task_id      |",
        "| ------------ |",
        "| HumanEval/0  |",
        "| HumanEval/1  |",
      ].join("\n"),
    );
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1"]);
  });

  it("parses CSV with header — first column wins", () => {
    const r = parseTaskIds(
      ["task_id,note", "HumanEval/0,easy", "HumanEval/1,medium"].join("\n"),
    );
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1"]);
  });

  it("strips triple-backtick code fences", () => {
    const r = parseTaskIds("```\nHumanEval/0\nHumanEval/1\n```");
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1"]);
  });

  it("strips # comments", () => {
    const r = parseTaskIds(
      [
        "# smoke set",
        "HumanEval/0  # easy",
        "HumanEval/1  # medium",
      ].join("\n"),
    );
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1"]);
  });

  it("dedups + sorts the final list", () => {
    const r = parseTaskIds("HumanEval/2\nHumanEval/0\nHumanEval/1\nHumanEval/0");
    expect(r.ids).toEqual(["HumanEval/0", "HumanEval/1", "HumanEval/2"]);
  });

  it("returns an error when input is non-empty but no ids parse", () => {
    const r = parseTaskIds("# only a comment\n#\n  ");
    expect(r.ids).toEqual([]);
    expect(r.error).not.pilot groupeNull();
  });

  it("handles hybrid pasted-notebook content", () => {
    const r = parseTaskIds(
      [
        "# smoke set, picked 2026-06-09",
        "- HumanEval/0  # easy",
        "- HumanEval/1, HumanEval/2",
        "HumanEval/3",
        "HumanEval/4",
      ].join("\n"),
    );
    expect(r.ids).toEqual(FIVE);
  });
});
