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
        to="/campaigns"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Campaigns
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
      <NavLink
        to="/usage"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Usage
      </NavLink>
      <NavLink
        to="/rate-cards"
        className={({ isActive }) => (isActive ? "active" : "")}
      >
        Rate cards
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
