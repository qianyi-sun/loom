import React from "react";

import {
  clearBrowserFailureConsoleRedaction,
  prepareBrowserFailureForBoundary,
  reportBrowserFailure,
} from "../lib/errorReporting";
import { frontendHomePath } from "../lib/frontendConfig";
import { RecoveryPanel } from "./RecoveryPanel";

interface RootErrorBoundaryProps {
  children: React.ReactNode;
  onRetry?: () => void;
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
    return { referenceId: prepareBrowserFailureForBoundary(error) };
  }

  componentDidCatch(error: unknown): void {
    try {
      if (this.state.referenceId) {
        reportBrowserFailure("root-render", this.state.referenceId);
      }
    } finally {
      clearBrowserFailureConsoleRedaction(error);
    }
  }

  private readonly reload = (): void => {
    if (this.props.onReload) {
      this.props.onReload();
      return;
    }
    window.location.reload();
  };

  private readonly retry = (): void => {
    this.props.onRetry?.();
    this.setState({ referenceId: null });
  };

  render(): React.ReactNode {
    if (!this.state.referenceId) return this.props.children;

    return (
      <RecoveryPanel
        title="Loom could not display this page"
        message="An unexpected browser error occurred. Reload the app or return to a safe starting point."
        referenceId={this.state.referenceId}
        onRetry={this.retry}
        onReload={this.reload}
        homeHref={frontendHomePath()}
      />
    );
  }
}
