import { useEffect } from 'preact/hooks';
import { dashboardStore } from '../store/dashboard.store';
import { fetchBootstrap, normalizeDashboardData } from '../lib/api';
import { Header } from './Header';
import { KpiStrip } from './KpiStrip';
import { MapContainer } from './MapContainer';
import { Sidebar } from './Sidebar';
import { Modal } from './Modal';

export function App() {
  useEffect(() => {
    const store = dashboardStore.getState();
    store.setBootstrapLoading();
    fetchBootstrap()
      .then((raw) => store.setBootstrapReady(normalizeDashboardData(raw)))
      .catch((err: unknown) =>
        store.setBootstrapError(err instanceof Error ? err.message : String(err))
      );
  }, []);

  return (
    <div id="preact-wrapper">
      <Header />
      <KpiStrip />
      <div class="ml">
        <MapContainer />
        <Sidebar />
      </div>
      <Modal />
    </div>
  );
}
