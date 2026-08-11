/**
 * Smart task-id parser for the NewBatch submit form's "Explicit task
 * ids" paste field. Accepts a wide range of formats users tend to
 * paste from (notebooks, spreadsheets, chat messages, ranges, JSON
 * arrays, markdown bullets/tables, CSV with header). Normalizes to a
 * sorted + deduplicated list of task ids.
 *
 * Pure function — no network, no DOM. Returns either:
 *   - `{ ids: [...], error: null }` on success (ids may be empty if
 *     the input was also empty), or
 *   - `{ ids: [], error: "..." }` when the input had content but
 *     nothing parseable came out.
 *
 * Rules (applied in roughly the order spelled out in
 * docs/user-guide.md#quickstart-submit-from-the-web-app):
 *
 *   1. Strip triple-backtick code fences but keep their contents.
 *   2. Drop everything after `#` on a line (treated as a comment).
 *   3. Strip leading markdown bullet noise: `-`, `*`, `•`, `→`, `>`,
 *      `N.` numbered-list prefixes.
 *   4. Strip pipe-table cell delimiters `|` and `| --- |` separator
 *      rows.
 *   5. Strip JSON array brackets, single/double quotes, trailing
 *      commas, Python `(`, `)`, `[`, `]`.
 *   6. Drop empty / blank-only segments.
 *   7. Detect CSV header: if the first non-blank line is comma-
 *      separated AND the first field looks like a column name
 *      (no `/`), drop it and take the first column of subsequent
 *      rows.
 *   8. Split remaining lines on comma / semicolon / pipe / tab /
 *      2+ spaces (single spaces preserved — task ids occasionally
 *      contain them).
 *   9. Expand `<prefix>/<a>-<b>` ranges into individual ids.
 *  10. Expand `<prefix>/<n1>,<n2>,...` numeric prefix-shorthand.
 *  11. Strip `/api/v1/tasks/` and `/tasks/` URL prefixes.
 *  12. Final list = sorted + deduplicated.
 */

export interface ParseTaskIdsResult {
  ids: string[];
  error: string | null;
}

const URL_PREFIXES = ["/api/v1/tasks/", "/tasks/"];

function stripCodeFences(input: string): string {
  // Remove lines that are pure ``` (possibly with a language tag) but
  // keep the contents between fences. We don't try to be smart about
  // unbalanced fences — pasted content rarely has them.
  return input
    .split("\n")
    .filter((line) => !/^\s*```/.test(line))
    .join("\n");
}

function stripLineComment(line: string): string {
  const hashIdx = line.indexOf("#");
  if (hashIdx === -1) return line;
  return line.slice(0, hashIdx);
}

function stripBulletPrefix(line: string): string {
  // Strip leading whitespace + bullet glyph + optional whitespace.
  // Includes ASCII bullets (-, *, >), the unicode bullet (•),
  // an arrow (→), and N.  / N) numbered-list prefixes.
  return line.replace(
    /^\s*(?:[-*•→>]|\d+[.)])\s+/u,
    "",
  );
}

function isTableSeparatorRow(line: string): boolean {
  // A markdown table separator looks like "| --- | --- |" or
  // "|---|---|" or just dashes/colons with pipes.
  const t = line.trim();
  if (!t.startsWith("|") && !t.includes("|")) return false;
  return /^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?$/.test(t);
}

function stripPipeTableDelimiters(line: string): string {
  // Strip leading/trailing pipes from a row like "| HumanEval/0 |".
  // The remaining pipes split the row into cells; downstream split
  // logic handles them.
  let s = line.trim();
  if (s.startsWith("|")) s = s.slice(1);
  if (s.endsWith("|")) s = s.slice(0, -1);
  return s;
}

function stripWrappingPunctuation(line: string): string {
  // Strip JSON array / Python list brackets, quotes (single + double),
  // trailing commas. Applied per-line so a `["foo"]` paste reduces
  // to `foo`. We also strip ( ) for Python tuple-literal pastes.
  let s = line;
  // Strip outer brackets repeatedly (handles `[["a", "b"]]`).
  for (let i = 0; i < 4; i++) {
    const before = s;
    s = s.replace(/^[\s[\](){}]+/g, "").replace(/[\s[\](){}]+$/g, "");
    if (s === before) break;
  }
  return s;
}

function stripSegmentQuotes(seg: string): string {
  let s = seg.trim();
  // Strip trailing comma left over from JSON/Python lists.
  s = s.replace(/,+$/, "").trim();
  // Strip matching outer quotes.
  if (
    (s.startsWith('"') && s.endsWith('"')) ||
    (s.startsWith("'") && s.endsWith("'"))
  ) {
    s = s.slice(1, -1);
  }
  // Strip any inner quote characters that survive after outer stripping
  // (rare — e.g. unquoted Python list with stray ' on one side).
  s = s.replace(/["']/g, "");
  return s.trim();
}

function looksLikeColumnHeader(field: string): boolean {
  const f = field.trim().toLowerCase();
  if (!f) return false;
  // Column names don't contain `/` (the canonical task-id separator).
  // They DO tend to be short alphanumeric/underscore-only words.
  if (f.includes("/")) return false;
  return /^[a-z_][a-z0-9_ ]*$/i.test(f);
}

function splitSegments(line: string): string[] {
  // Split on comma, semicolon, pipe, tab, or 2+ consecutive spaces.
  // Single spaces are preserved (task ids sometimes contain them).
  return line.split(/[,;|\t]|\s{2,}/);
}

function expandRange(seg: string): string[] | null {
  // <prefix>/<a>-<b> where prefix can be any non-empty string that
  // doesn't itself contain `/` adjacent to the dash, and a/b are
  // non-negative integers with a ≤ b. The prefix CAN contain `/`
  // characters earlier (e.g. `humaneval/HumanEval/0-4`) — we anchor
  // on the LAST `/`.
  const slashIdx = seg.lastIndexOf("/");
  if (slashIdx === -1) return null;
  const prefix = seg.slice(0, slashIdx);
  const rest = seg.slice(slashIdx + 1);
  const m = /^(\d+)-(\d+)$/.exec(rest);
  if (!m) return null;
  const a = Number.parseInt(m[1], 10);
  const b = Number.parseInt(m[2], 10);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a > b) return null;
  // Guard absurd ranges so a typo doesn't lock the tab. 100k is well
  // above any single-benchmark task count.
  if (b - a > 100_000) return null;
  const out: string[] = [];
  for (let i = a; i <= b; i++) out.push(`${prefix}/${i}`);
  return out;
}

function expandNumericPrefixList(seg: string): string[] | null {
  // <prefix>/n1,n2,n3 — handled here because the comma split above
  // would have already broken this apart. So we only get here when
  // the segment has a comma INSIDE it (e.g. when an entire line is
  // `HumanEval/0,1,2,3,4` and was treated as one segment because of
  // some other separator on the line). In practice the higher-level
  // splitter handles the comma; this function exists for the case
  // where a single bullet line is `- HumanEval/0,1,2` — after bullet
  // stripping, the comma split runs and we never see the comma here.
  //
  // We still keep this function for symmetry with expandRange, but
  // it only fires when the segment contains a comma. The check below
  // handles that case.
  const slashIdx = seg.lastIndexOf("/");
  if (slashIdx === -1) return null;
  const prefix = seg.slice(0, slashIdx);
  const rest = seg.slice(slashIdx + 1);
  if (!rest.includes(",")) return null;
  const parts = rest.split(",").map((p) => p.trim());
  if (!parts.every((p) => /^\d+$/.test(p))) return null;
  return parts.map((p) => `${prefix}/${p}`);
}

function stripUrlPrefixes(seg: string): string {
  let s = seg;
  for (const prefix of URL_PREFIXES) {
    if (s.startsWith(prefix)) {
      s = s.slice(prefix.length);
      break;
    }
  }
  return s;
}

export function parseTaskIds(input: string): ParseTaskIdsResult {
  const trimmed = input.trim();
  if (trimmed === "") return { ids: [], error: null };

  // Step 1: strip triple-backtick fences.
  let working = stripCodeFences(input);

  // Step 5 (preliminary): strip wrapping JSON/Python brackets so a
  // pasted `["a","b"]` becomes `"a","b"`. We do this on the WHOLE
  // input before line splitting so a multi-line JSON array collapses
  // into pure entries.
  working = working.replace(/^\s*[[(]+/, "").replace(/[\])]+\s*$/, "");

  // Split into lines.
  const rawLines = working.split("\n");

  // Step 2 + 3 + 4: per-line cleanup (comments, bullets, table delim).
  // Also drop markdown table separator rows.
  const cleanedLines: string[] = [];
  for (const raw of rawLines) {
    if (isTableSeparatorRow(raw)) continue;
    let line = stripLineComment(raw);
    line = stripBulletPrefix(line);
    if (line.includes("|")) {
      line = stripPipeTableDelimiters(line);
    }
    line = stripWrappingPunctuation(line);
    if (line.trim() === "") continue;
    cleanedLines.push(line);
  }

  if (cleanedLines.length === 0) {
    return { ids: [], error: "No task ids found in the pasted input." };
  }

  // Step 7: CSV header detection. If the first line has commas AND
  // the first comma-separated field looks like a column name, treat
  // every subsequent line's first column as the id. Also covers the
  // single-column markdown-table case: the header row after `|` strip
  // is a bare column name (e.g. `task_id`).
  let workingLines = cleanedLines;
  const firstLine = cleanedLines[0];
  if (firstLine.includes(",")) {
    const firstFields = firstLine.split(",").map((f) => stripSegmentQuotes(f));
    if (firstFields.length >= 1 && looksLikeColumnHeader(firstFields[0])) {
      workingLines = cleanedLines.slice(1).map((line) => {
        const idx = line.indexOf(",");
        return idx === -1 ? line : line.slice(0, idx);
      });
    }
  } else if (
    cleanedLines.length >= 2 &&
    looksLikeColumnHeader(stripSegmentQuotes(firstLine))
  ) {
    workingLines = cleanedLines.slice(1);
  }

  // Step 8: split each line into segments on any of: comma,
  // semicolon, pipe, tab, or 2+ spaces.
  //
  // Special-case: if the line matches `<prefix>/<n1>,<n2>,...` with
  // numbers-only after the slash, expand BEFORE the comma-split so
  // the bare numbers don't leak through as standalone segments.
  const rawSegments: string[] = [];
  for (const line of workingLines) {
    const expanded = expandNumericPrefixList(line.trim());
    if (expanded) {
      for (const id of expanded) rawSegments.push(id);
      continue;
    }
    for (const seg of splitSegments(line)) {
      const cleaned = stripSegmentQuotes(seg);
      if (cleaned) rawSegments.push(cleaned);
    }
  }

  // Step 9 + 10 + 11: per-segment expansion.
  const ids: string[] = [];
  for (const seg of rawSegments) {
    const stripped = stripUrlPrefixes(seg);
    const range = expandRange(stripped);
    if (range) {
      for (const id of range) ids.push(id);
      continue;
    }
    const numericList = expandNumericPrefixList(stripped);
    if (numericList) {
      for (const id of numericList) ids.push(id);
      continue;
    }
    ids.push(stripped);
  }

  // Step 12: dedup + sort.
  const final = Array.from(new Set(ids.filter((s) => s.length > 0))).sort();

  if (final.length === 0) {
    return { ids: [], error: "No task ids found in the pasted input." };
  }
  return { ids: final, error: null };
}
