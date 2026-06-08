import { NavLink } from "react-router-dom";

export default function NavBar(): JSX.Element {
  return (
    <nav className="loom-nav">
      <NavLink
        to="/trials"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Trials
      </NavLink>
      <NavLink
        to="/tasks"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Tasks
      </NavLink>
      <NavLink
        to="/benchmarks"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Benchmarks
      </NavLink>
      <div style={{ flex: 1 }} />
      <NavLink
        to="/settings"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Settings
      </NavLink>
    </nav>
  );
}
