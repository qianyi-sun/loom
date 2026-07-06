import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { AuthProvider } from "./auth/AuthContext";
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

function renderApp(): void {
  const routePath = getFrontendConfig().routePath;
  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename={routePath || undefined}>
          <AuthProvider>
            <App />
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
    document.getElementById("root")!.innerHTML =
      '<div style="padding: 24px; font-family: sans-serif">Frontend configuration error.</div>';
  });
