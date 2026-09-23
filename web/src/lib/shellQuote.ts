/** POSIX shell quoting: JSON, labels and URLs must never become shell syntax. */
export function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

