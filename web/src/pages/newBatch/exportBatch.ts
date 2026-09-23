import type { CreateBatchBody } from "../../api";

import { shellQuote } from "../../lib/shellQuote";

export function batchExport(body: CreateBatchBody, apiBase: string, origin: string) {
  const server = new URL(apiBase || "/", origin).href.replace(/\/+$/, "");
  const json = JSON.stringify(body, null, 2);
  return {
    json,
    cli: [
      `loom auth login --server ${shellQuote(server)} --username "$LOOM_USERNAME" --password env:LOOM_PASSWORD${body.team_id ? ` --team-id ${shellQuote(body.team_id)}` : ""}`,
      "loom eval batch create --request-json @batch.json",
    ].join(" && \\\n  "),
    api: [
      `curl --fail-with-body --request POST ${shellQuote(`${server}/api/v1/batches`)}`,
      '  --header "Authorization: Bearer ${LOOM_TOKEN:?Set LOOM_TOKEN first}"',
      "  --header 'Content-Type: application/json'",
      `  --data-raw ${shellQuote(json)}`,
    ].join(" \\\n"),
  };
}
