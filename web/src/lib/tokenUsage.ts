export function formatTokenUsage(
  inputTokens: number | null | undefined,
  outputTokens: number | null | undefined,
): string {
  return `Input ${inputTokens ?? 0} / Output ${outputTokens ?? 0}`;
}
