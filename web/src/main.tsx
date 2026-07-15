import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { AuthProvider } from "./auth/AuthContext";
import { useAuth } from "./auth/useAuth";
import { FrontendBootstrap } from "./bootstrap/FrontendBootstrap";
import { RootErrorBoundary } from "./components/RootErrorBoundary";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30_000,
    },
  },
});

// This production entrypoint is not a Fast Refresh module. The wrapper exposes
// a browser-smoke signal only after React commits the application tree.
// eslint-disable-next-line react-refresh/only-export-components
function MountedApp({ rootElement }: { rootElement: HTMLElement }): JSX.Element {
  const { authError, isAuthenticated, isLoading } = useAuth();
  React.useLayoutEffect(() => {
    rootElement.setAttribute("data-loom-mounted", "true");
    if (isLoading) {
      rootElement.removeAttribute("data-loom-auth-settled");
      rootElement.removeAttribute("data-loom-auth-state");
    } else {
      rootElement.setAttribute("data-loom-auth-settled", "true");
      rootElement.setAttribute(
        "data-loom-auth-state",
        authError ? "error" : isAuthenticated ? "authenticated" : "anonymous",
      );
    }
    return () => {
      rootElement.removeAttribute("data-loom-mounted");
      rootElement.removeAttribute("data-loom-auth-settled");
      rootElement.removeAttribute("data-loom-auth-state");
    };
  }, [authError, isAuthenticated, isLoading, rootElement]);
  return <App />;
}

const rootElement = document.getElementById("root")!;
ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <RootErrorBoundary>
      <FrontendBootstrap>
        {(config) => (
          <QueryClientProvider client={queryClient}>
            <BrowserRouter basename={config.routePath || undefined}>
              <AuthProvider>
                <MountedApp rootElement={rootElement} />
              </AuthProvider>
            </BrowserRouter>
          </QueryClientProvider>
        )}
      </FrontendBootstrap>
    </RootErrorBoundary>
  </React.StrictMode>,
);
