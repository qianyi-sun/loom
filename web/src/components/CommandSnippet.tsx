import { useState, type ReactNode } from "react";

import { Button } from "./Button";

export interface CommandSnippetProps {
  command: string;
  label?: string;
  helperText?: ReactNode;
}

export default function CommandSnippet({
  command,
  label = "Command",
  helperText,
}: CommandSnippetProps): JSX.Element {
  const [copied, setCopied] = useState(false);

  const copy = async (): Promise<void> => {
    await navigator.clipboard?.writeText(command);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3">
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
          {label}
        </p>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void copy()}
          aria-label={`Copy ${label}`}
          title={`Copy ${label}`}
        >
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="whitespace-pre-wrap break-words rounded-md bg-slate-950 p-3 text-xs leading-relaxed text-slate-50">
        <code>{command}</code>
      </pre>
      {helperText ? (
        <div className="text-xs leading-relaxed text-slate-500">
          {helperText}
        </div>
      ) : null}
    </div>
  );
}
