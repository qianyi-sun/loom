import { Navigate, Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import AdminAccess from "./pages/AdminAccess";
import Benchmarks from "./pages/Benchmarks";
import ProviderCreate from "./pages/ProviderCreate";
import ProviderDetail from "./pages/ProviderDetail";
import ProvidersList from "./pages/ProvidersList";
import BatchDetail from "./pages/BatchDetail";
import InviteAccept from "./pages/InviteAccept";
import Monitor from "./pages/Monitor";
import NewBatch from "./pages/NewBatch";
import NotFound from "./pages/NotFound";
import RateCardsAdmin from "./pages/RateCardsAdmin";
import RunLibrary from "./pages/RunLibrary";
import RunLibraryBatchDetail from "./pages/RunLibraryBatchDetail";
import Settings from "./pages/Settings";
import Tasks from "./pages/Tasks";
import TrialCompare from "./pages/TrialCompare";
import TrialDetail from "./pages/TrialDetail";
import UsageDashboard from "./pages/UsageDashboard";

export default function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Navigate to="/monitor" replace />} />
        <Route path="monitor" element={<Monitor />} />
        <Route path="library" element={<RunLibrary />} />
        <Route path="library/batches/:batchId" element={<RunLibraryBatchDetail />} />
        {/* Legacy redirects — preserve old links from external docs / chats. */}
        <Route path="trials" element={<Navigate to="/monitor?view=trials" replace />} />
        <Route path="batches" element={<Navigate to="/monitor?view=batches" replace />} />
        <Route path="trials/compare" element={<TrialCompare />} />
        <Route path="trials/:trialId" element={<TrialDetail />} />
        <Route path="batches/new" element={<NewBatch />} />
        <Route path="batches/:batchId" element={<BatchDetail />} />
        {/* Tasks / Benchmarks / Usage routes still resolve so power-user
            URLs don't 404 mid-redesign; they're absent from nav per PR-4. */}
        <Route path="tasks" element={<Tasks />} />
        <Route path="benchmarks" element={<Benchmarks />} />
        <Route path="usage" element={<UsageDashboard />} />
        <Route path="providers/new" element={<ProviderCreate />} />
        <Route path="providers/:id" element={<ProviderDetail />} />
        <Route path="providers" element={<ProvidersList />} />
        <Route path="admin/access" element={<AdminAccess />} />
        <Route path="invites/accept" element={<InviteAccept />} />
        <Route path="rate-cards" element={<RateCardsAdmin />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
