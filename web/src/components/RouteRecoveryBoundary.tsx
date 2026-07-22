import React from "react";
import { Outlet, useLocation } from "react-router-dom";

import { frontendHomePath } from "../lib/frontendConfig";
import { BrowserErrorBoundary } from "./BrowserErrorBoundary";
import { RecoveryPanel } from "./RecoveryPanel";

interface RouteRecoveryBoundaryProps {
  children?: React.ReactNode;
  onReload?: () => void;
  /** React.lazy caches rejections, so lazy content must require reload. */
  retryPolicy?: "transient" | "reload-required";
}

function RouteLoadingStatus(): JSX.Element {
  return (
    <div role="status" aria-live="polite" aria-busy="true" tabIndex={-1}>
      <p className="text-sm text-slate-600">Loading this Loom page…</p>
    </div>
  );
}

/** Keep the app shell alive when one routed page or lazy module fails. */
export function RouteRecoveryBoundary({
  children,
  onReload,
  retryPolicy = "transient",
}: RouteRecoveryBoundaryProps): JSX.Element {
  const location = useLocation();

  return (
    <BrowserErrorBoundary
      resetKey={location.key}
      pathname={window.location.pathname}
      renderFallback={({ referenceId, retry }) => (
        <RecoveryPanel
          scope="route"
          title="Loom could not display this section"
          message={
            retryPolicy === "transient"
              ? "This page encountered an unexpected browser error. Retry it, reload Loom, or return to a safe starting point."
              : "This page could not be loaded. Reload Loom or return to a safe starting point."
          }
          referenceId={referenceId}
          onRetry={retryPolicy === "transient" ? retry : undefined}
          onReload={onReload}
          homeHref={frontendHomePath()}
        />
      )}
    >
      <React.Suspense fallback={<RouteLoadingStatus />}>
        {children ?? <Outlet />}
      </React.Suspense>
    </BrowserErrorBoundary>
  );
}
