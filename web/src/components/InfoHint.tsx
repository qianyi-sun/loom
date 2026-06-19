import type { ReactNode } from "react";

import { cn } from "../lib/cn";

export interface InfoHintProps {
  children: ReactNode;
  className?: string;
}

export function InfoHint({ children, className }: InfoHintProps): JSX.Element {
  return (
    <p className={cn("mt-1 text-xs leading-relaxed text-slate-500", className)}>
      {children}
    </p>
  );
}
