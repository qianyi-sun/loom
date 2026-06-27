import {
  benchmarkCatalogCommands,
  batchInspectionCommands,
  cliLoginCommands,
  containsUnsafeSnippetValue,
  hostedProviderCommands,
  oracleSmokeBatchCommand,
  providerSmokeBatchCommand,
  rateCardExampleJson,
  trialDownloadCommands,
  usageCommand,
} from "../../lib/quickstartSnippets";

describe("quickstartSnippets", () => {
  it("builds public CLI login commands from the current server origin", () => {
    expect(cliLoginCommands("https://loom.example.com")).toEqual([
      "export LOOM_PASSWORD=...",
      "loom auth login --server https://loom.example.com --username USER --password env:LOOM_PASSWORD",
      "loom auth whoami",
    ]);
  });

  it("uses env/file based secrets for hosted provider setup", () => {
    const command = hostedProviderCommands(
      "https://loom.example.com",
      "together-prod",
    ).join("\n");

    expect(command).toContain("export PROVIDER_API_KEY=...");
    expect(command).toContain("--api-key env:PROVIDER_API_KEY");
    expect(command).toContain("loom providers test together-prod");
    expect(command).not.toContain("sk-");
  });

  it("builds safe batch and download commands", () => {
    expect(oracleSmokeBatchCommand()).toContain("--agent oracle");
    expect(providerSmokeBatchCommand("smoke-openai", "gpt-4o-mini")).toContain(
      "--provider smoke-openai",
    );
    expect(batchInspectionCommands("batch-1")).toContain(
      "loom eval batch show batch-1",
    );
    expect(trialDownloadCommands("trial-1", "main/report.json")).toContain(
      "loom eval trial download trial-1 --kind artifact --artifact-key main/report.json --output artifact.bin",
    );
  });

  it("builds secret-safe benchmark catalog commands", () => {
    const commands = benchmarkCatalogCommands("https://loom.example.com/");

    expect(commands).toEqual([
      "loom datasets list --remote --server-url https://loom.example.com --token env:LOOM_API_TOKEN",
      'loom datasets audit --all --db-url "$LOOM_DB_URL"',
      [
        "loom datasets sync-config",
        "  --config config/benchmarks.toml",
        '  --db-url "$LOOM_DB_URL"',
        "  --dry-run",
      ].join(" \\\n"),
    ]);
    expect(commands.some((command) => containsUnsafeSnippetValue(command))).toBe(
      false,
    );
  });

  it("builds usage and rate-card examples", () => {
    expect(usageCommand("2026-06-01", "2026-06-30")).toBe(
      "loom eval usage --start 2026-06-01 --end 2026-06-30",
    );
    expect(usageCommand("2026-06-01", "2026-06-30", "team-1")).toBe(
      "loom eval usage --start 2026-06-01 --end 2026-06-30 --team-id team-1",
    );
    expect(JSON.parse(rateCardExampleJson())).toMatchObject({
      provider: "openai",
      model: "gpt-4o-mini",
    });
  });

  it("flags unsafe snippet examples", () => {
    expect(containsUnsafeSnippetValue("sk-live-secret")).toBe(true);
    expect(containsUnsafeSnippetValue("Authorization: Bearer abc")).toBe(true);
    expect(
      containsUnsafeSnippetValue("https://s3.example/object?X-Amz-Signature=abc"),
    ).toBe(true);
    expect(containsUnsafeSnippetValue("--api-key env:PROVIDER_API_KEY")).toBe(
      false,
    );
  });
});
