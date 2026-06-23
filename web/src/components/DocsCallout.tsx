import type { ReactNode } from "react";

import { cn } from "../lib/cn";

export type DocsCalloutTone = "neutral" | "info" | "success";

const TONE_CLASSES: Record<DocsCalloutTone, string> = {
  neutral: "border-slate-200 bg-white",
  info: "border-blue-200 bg-blue-50/70",
  success: "border-emerald-200 bg-emerald-50/70",
};

export interface DocsCalloutProps {
  title: string;
  children: ReactNode;
  tone?: DocsCalloutTone;
  className?: string;
}

export default function DocsCallout({
  title,
  children,
  tone = "neutral",
  className,
}: DocsCalloutProps): JSX.Element {
  return (
    <section
      className={cn(
        "space-y-2 rounded-lg border p-3 text-sm text-slate-700",
        TONE_CLASSES[tone],
        className,
      )}
    >
      <h2 className="text-sm font-semibold text-slate-900">{title}</h2>
      <div className="space-y-2 text-sm leading-relaxed">{children}</div>
    </section>
  );
}
