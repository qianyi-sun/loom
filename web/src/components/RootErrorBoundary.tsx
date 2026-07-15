import React from "react";

import {
  createBrowserFailureId,
  markBrowserFailureForConsoleRedaction,
  reportBrowserFailure,
} from "../lib/errorReporting";
import { frontendHomePath } from "../lib/frontendConfig";
import { RecoveryPanel } from "./RecoveryPanel";

interface RootErrorBoundaryProps {
  children: React.ReactNode;
  onReload?: () => void;
}

interface RootErrorBoundaryState {
  referenceId: string | null;
}

export class RootErrorBoundary extends React.Component<
  RootErrorBoundaryProps,
  RootErrorBoundaryState
> {
  state: RootErrorBoundaryState = { referenceId: null };

  static getDerivedStateFromError(error: unknown): RootErrorBoundaryState {
    markBrowserFailureForConsoleRedaction(error);
    return { referenceId: createBrowserFailureId() };
  }

  componentDidCatch(): void {
    if (this.state.referenceId) {
      reportBrowserFailure("root-render", this.state.referenceId);
    }
  }

  private readonly reload = (): void => {
    if (this.props.onReload) {
      this.props.onReload();
      return;
    }
    window.location.reload();
  };

  render(): React.ReactNode {
    if (!this.state.referenceId) return this.props.children;

    return (
      <RecoveryPanel
        title="Loom could not display this page"
        message="An unexpected browser error occurred. Reload the app or return to a safe starting point."
        referenceId={this.state.referenceId}
        actionLabel="Reload Loom"
        onAction={this.reload}
        homeHref={frontendHomePath()}
      />
    );
  }
}
