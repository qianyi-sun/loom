import { useState } from "react";

import { cn } from "../lib/cn";

export interface CopyableIdProps {
  value: string;
  chars?: number;
  className?: string;
}

export function CopyableId({
  value,
  chars = 8,
  className,
}: CopyableIdProps): JSX.Element {
  const [copied, setCopied] = useState(false);
  const short =
    value.length > chars * 2 + 3
      ? `${value.slice(0, chars)}...${value.slice(-chars)}`
      : value;

  return (
    <button
      type="button"
      title={copied ? "Copied" : `Copy ${value}`}
      onClick={() => {
        void navigator.clipboard?.writeText(value);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
      className={cn(
        "rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700 hover:bg-slate-200",
        className,
      )}
    >
      {short}
    </button>
  );
}
