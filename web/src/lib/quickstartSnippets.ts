function cleanOrigin(serverOrigin: string): string {
  const trimmed = serverOrigin.trim().replace(/\/+$/, "");
  return trimmed || "https://loom.example.com";
}

export function cliLoginCommands(serverOrigin: string): string[] {
  const origin = cleanOrigin(serverOrigin);
  return [
    "export LOOM_PASSWORD=...",
    `loom auth login --server ${origin} --username USER --password env:LOOM_PASSWORD`,
    "loom auth whoami",
  ];
}

export function hostedProviderCommands(
  serverOrigin: string,
  providerName = "smoke-openai",
): string[] {
  const origin = cleanOrigin(serverOrigin);
  return [
    ...cliLoginCommands(origin),
    "export PROVIDER_API_KEY=...",
    [
      "loom providers create",
      `  --name ${providerName}`,
      "  --type openai-compatible",
      "  --base-url https://api.example.com/v1",
      "  --api-key env:PROVIDER_API_KEY",
      "  --rate-card-provider openai",
    ].join(" \\\n"),
    `loom providers test ${providerName}`,
    `loom providers models ${providerName} --refresh`,
    `loom providers models ${providerName}`,
  ];
}

export function benchmarkCatalogCommands(serverOrigin: string): string[] {
  const origin = cleanOrigin(serverOrigin);
  return [
    `loom datasets list --remote --server-url ${origin} --token env:LOOM_API_TOKEN`,
    'loom datasets audit --all --db-url "$LOOM_DB_URL"',
    [
      "loom datasets sync-config",
      "  --config config/benchmarks.toml",
      '  --db-url "$LOOM_DB_URL"',
      "  --dry-run",
    ].join(" \\\n"),
  ];
}

export function oracleSmokeBatchCommand(): string {
  return [
    "loom eval batch create",
    "  --name oracle-smoke",
    "  --benchmark humaneval",
    "  --subset first_n",
    "  --n 1",
    "  --agent oracle",
    "  --n-per-task 1",
  ].join(" \\\n");
}

export function providerSmokeBatchCommand(
  providerName = "smoke-openai",
  modelName = "gpt-4o-mini",
): string {
  return [
    "loom eval batch create",
    "  --name provider-smoke",
    "  --benchmark humaneval",
    "  --subset first_n",
    "  --n 1",
    "  --agent litellm",
    `  --provider ${providerName}`,
    `  --model ${modelName}`,
    "  --n-per-task 1",
  ].join(" \\\n");
}

export function batchInspectionCommands(batchId: string): string[] {
  return [
    `loom eval batch show ${batchId}`,
    `loom eval trial list --batch-id ${batchId}`,
  ];
}

export function trialDownloadCommands(
  trialId: string,
  artifactKey?: string | null,
): string[] {
  const commands = [
    `loom eval trial show ${trialId}`,
    `loom eval trial download ${trialId} --kind atif --output atif.json`,
    `loom eval trial download ${trialId} --kind trajectory --output events.jsonl`,
  ];
  if (artifactKey) {
    commands.push(
      `loom eval trial download ${trialId} --kind artifact --artifact-key ${artifactKey} --output artifact.bin`,
    );
  }
  return commands;
}

export function usageCommand(
  start: string,
  end: string,
  teamId?: string,
  includeBatches = false,
): string {
  const trimmedTeamId = teamId?.trim();
  const teamFlag = trimmedTeamId ? ` --team-id ${trimmedTeamId}` : "";
  const batchesFlag = includeBatches ? " --include-batches" : "";
  return `loom eval usage --start ${start} --end ${end}${teamFlag}${batchesFlag}`;
}

export function rateCardExampleJson(): string {
  return JSON.stringify(
    {
      provider: "openai",
      model: "gpt-4o-mini",
      input_per_mtok: 0.15,
      output_per_mtok: 0.6,
      cache_read_per_mtok: 0,
      cache_write_per_mtok: 0,
    },
    null,
    2,
  );
}

export function containsUnsafeSnippetValue(value: string): boolean {
  const patterns = [
    /\bsk-[A-Za-z0-9_-]+/i,
    /\bapi[_-]?key\s*[:=]\s*['"]?[A-Za-z0-9_-]{12,}/i,
    /\bAuthorization:\s*Bearer\s+\S+/i,
    /X-Amz-(Algorithm|Credential|Signature|Security-Token)=/i,
    /([?&]token=|[?&]signature=|[?&]X-Amz-Signature=)/i,
  ];
  return patterns.some((pattern) => pattern.test(value));
}
