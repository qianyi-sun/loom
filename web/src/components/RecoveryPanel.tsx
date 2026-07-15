import React from "react";

import { SkipLink } from "./SkipLink";

interface RecoveryPanelProps {
  title: string;
  message: string;
  referenceId: string;
  onRetry: () => void;
  onReload?: () => void;
  homeHref: string;
}

export function RecoveryPanel({
  title,
  message,
  referenceId,
  onRetry,
  onReload,
  homeHref,
}: RecoveryPanelProps): JSX.Element {
  const titleId = React.useId();
  const panelRef = React.useRef<HTMLElement>(null);

  React.useEffect(() => {
    panelRef.current?.focus();
  }, []);

  return (
    <>
      <SkipLink />
      <main
        id="main-content"
        ref={panelRef}
        tabIndex={-1}
        className="mx-auto mt-16 max-w-xl rounded-xl border border-slate-200 bg-white p-6 shadow-sm"
      >
        <div role="alert" aria-labelledby={titleId}>
          <h1 id={titleId} className="text-xl font-semibold text-slate-900">
            {title}
          </h1>
          <p className="mt-2 text-sm text-slate-600">{message}</p>
          <p className="mt-3 font-mono text-xs text-slate-500">
            Support reference: {referenceId}
          </p>
          <div className="mt-6 flex flex-wrap gap-3">
            <button
              type="button"
              onClick={onRetry}
              className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
            >
              Retry
            </button>
            <button
              type="button"
              onClick={onReload ?? (() => window.location.reload())}
              className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Reload Loom
            </button>
            <a
              href={homeHref}
              className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Go to Loom home
            </a>
          </div>
        </div>
      </main>
    </>
  );
}
