import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { AuthProvider } from "./auth/AuthContext";
import { useAuth } from "./auth/useAuth";
import "./index.css";
import { getFrontendConfig, loadFrontendConfig } from "./lib/frontendConfig";

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

function renderApp(): void {
  const routePath = getFrontendConfig().routePath;
  const rootElement = document.getElementById("root")!;
  ReactDOM.createRoot(rootElement).render(
    <React.StrictMode>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename={routePath || undefined}>
          <AuthProvider>
            <MountedApp rootElement={rootElement} />
          </AuthProvider>
        </BrowserRouter>
      </QueryClientProvider>
    </React.StrictMode>,
  );
}

void loadFrontendConfig()
  .then(renderApp)
  .catch((err: unknown) => {
    console.error("Failed to load Loom frontend config", err);
    const rootElement = document.getElementById("root")!;
    const message = document.createElement("div");
    message.className = "frontend-config-error";
    message.setAttribute("role", "alert");
    message.textContent = "Frontend configuration error.";
    rootElement.replaceChildren(message);
  });
