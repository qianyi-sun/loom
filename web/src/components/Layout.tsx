import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "../auth/useAuth";
import NavBar from "./NavBar";

export default function Layout(): JSX.Element {
  const { token } = useAuth();
  const loc = useLocation();
  const isSettings = loc.pathname.startsWith("/settings");
  // Unauthenticated users land on Settings (the token-paste page).
  if (!token && !isSettings) {
    return <Navigate to="/settings" replace />;
  }
  return (
    <div className="loom-root">
      <NavBar />
      <main className="loom-main">
        <Outlet />
      </main>
    </div>
  );
}
